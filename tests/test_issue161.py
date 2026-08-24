"""Regressions for GitHub issue #161.

When a supervised maintained worker dies unexpectedly (SIGKILL/OOM), the
supervisor's crash path must converge every command process group owned by
the exact crashed worker incarnation before any replacement is spawned. The
durable recorded child identity (carrying the exact lifecycle token) is the
blocking recovery obligation: while exact owned-group recovery fails or is
incomplete, the child stays recorded, the restart counter stays untouched,
and ``_ensure_worker`` is never reached. Only after recovery succeeds is the
child cleared exactly once and a replacement permitted on a later tick.

The first tests are deterministic daemon-level tests (no PostgreSQL): the
exact recovery machinery is replaced by a recording double that can fail.
The final test runs a real worker, SIGKILLs it while it owns a long-lived
SIGTERM-ignoring command group, and proves through the real recovery path
that no descendant survives and the replacement only follows recovery.
"""

from __future__ import annotations

import os
import sys
import time
from contextlib import suppress
from dataclasses import replace
from typing import TYPE_CHECKING
from uuid import uuid4

import psycopg
import pytest

from lubko import lifecycle, supervise
from lubko import supervisor as supervisor_module
from lubko import worker as worker_mod
from lubko.supervisor import OwnedGroupRecoveryError, Settings, SupervisorDaemon
from tests.test_issue75 import (
    _db_conf_from_conninfo,
    _insert_pending_job,
    _meta_for_live,
    _read_job_field,
    _spawn_real_worker,
    _wait_for_claim,
)

if TYPE_CHECKING:
    from pathlib import Path

COMMIT: str = "1" * 40


@pytest.fixture
def crash_token() -> str:
    """Return a fresh non-secret lifecycle token for a simulated crashed worker.

    Returns:
        A unique issue161-prefixed incarnation token.
    """
    return f"issue161-{uuid4().hex}"


def _dead_child(token: str) -> supervise.WorkerChild:
    """Return a child identity whose process is certainly gone.

    Args:
        token: Lifecycle token (incarnation) recorded for the crashed worker.

    Returns:
        A dead fake child identity.
    """
    return supervise.WorkerChild(
        pid=9_999_901,
        pgid=9_999_901,
        sid=9_999_901,
        start_time_ticks=7,
        token=token,
        worker_id="issue161-worker",
        spawned_at=time.time(),
    )


def _write_crashed_state(child: supervise.WorkerChild, *, restart_count: int = 0) -> None:
    """Persist durable supervisor state recording a crashed (dead) child.

    Args:
        child: The dead child identity to record.
        restart_count: The consecutive-crash counter to persist.
    """
    desired = supervise.read_desired()
    assert desired is not None
    supervise.write_state(
        replace(
            supervise.fresh_state(),
            applied_generation=desired.generation,
            mode=supervise.MODE_RUN,
            commit=COMMIT,
            child=child,
            intent=supervise.INTENT_RUN,
            restart_count=restart_count,
            next_attempt_at=None,
            last_spawn_at=None,
            ready=False,
            next_readiness_at=None,
            boot_id=supervise.current_boot_id(),
        )
    )


def _daemon(monkeypatch: pytest.MonkeyPatch) -> tuple[SupervisorDaemon, list[str]]:
    """Build a daemon whose spawn side effects are recorded, never real.

    Args:
        monkeypatch: Pytest monkeypatch fixture.

    Returns:
        The daemon and the list of commits handed to ``_ensure_worker``.
    """
    supervise.request_run(COMMIT, repo="", uv_path="uv", worker_id="issue161-worker")
    daemon = SupervisorDaemon(Settings())
    ensured: list[str] = []
    monkeypatch.setattr(daemon, "_ensure_worker", ensured.append)
    monkeypatch.setattr(daemon, "_record_mission_progress", lambda _commit: None)
    monkeypatch.setattr(daemon, "_probe_readiness", lambda _now: None)
    return daemon, ensured


def test_crash_recovery_failure_blocks_replacement_and_preserves_state(
    monkeypatch: pytest.MonkeyPatch, crash_token: str
) -> None:
    """Failed owned-group recovery retains the obligation and spawns nothing."""
    token = crash_token
    daemon, ensured = _daemon(monkeypatch)
    child = _dead_child(token)
    _write_crashed_state(child)
    calls: list[str] = []

    def _failing_recovery(incarnation: str) -> None:
        calls.append(incarnation)
        msg = f"blocked for {incarnation}"
        raise OwnedGroupRecoveryError(msg)

    monkeypatch.setattr(supervisor_module, "recover_owned_groups", _failing_recovery)

    now = 1000.0
    for tick in range(3):
        daemon.reconcile(now + tick)
        state = supervise.read_state()
        assert state.child is not None, "the blocking obligation must stay durable"
        assert state.child.token == token
        assert state.child.pid == child.pid
        assert state.child.start_time_ticks == child.start_time_ticks
        assert state.restart_count == 0, "failed recovery must never bump the counter"
        assert state.ready is False, "a dead retained child is never ready"
        assert state.next_readiness_at is None
        assert ensured == [], "no replacement may be spawned while recovery fails"
    assert calls == [token, token, token], "each tick retries the same exact incarnation"


def test_later_successful_recovery_clears_child_once_and_permits_restart(
    monkeypatch: pytest.MonkeyPatch, crash_token: str
) -> None:
    """After recovery succeeds the child is cleared once and a replacement runs."""
    token = crash_token
    daemon, ensured = _daemon(monkeypatch)
    _write_crashed_state(_dead_child(token))
    calls: list[str] = []
    outcomes: list[bool] = [False, True]

    def _flaky_recovery(incarnation: str) -> None:
        calls.append(incarnation)
        if outcomes.pop(0) is False:
            msg = f"still blocked for {incarnation}"
            raise OwnedGroupRecoveryError(msg)

    monkeypatch.setattr(supervisor_module, "recover_owned_groups", _flaky_recovery)

    now = 2000.0
    daemon.reconcile(now)
    blocked = supervise.read_state()
    assert blocked.child is not None, "a failed recovery keeps the obligation durable"
    assert blocked.restart_count == 0
    assert ensured == []
    assert blocked.next_attempt_at is not None, "the blocked crash schedules a retry"
    # Drive the next tick past the persisted retry deadline.
    assert blocked.next_attempt_at is not None
    daemon.reconcile(blocked.next_attempt_at + 0.01)
    state = supervise.read_state()
    assert state.child is None, "a successful recovery must clear the dead child"
    assert state.restart_count == 1, "the crash itself counts exactly once"
    assert state.next_attempt_at is not None, "success schedules a fresh crash backoff"
    assert ensured == [], "no replacement until the post-success backoff expires"
    assert state.next_attempt_at is not None
    daemon.reconcile(state.next_attempt_at + 0.01)
    assert ensured == [COMMIT]
    assert calls == [token, token]


def test_drained_crash_skips_recovery_and_retires_immediately(
    monkeypatch: pytest.MonkeyPatch, crash_token: str
) -> None:
    """A crashed worker that proved a clean drain needs no emergency recovery."""
    token = crash_token
    daemon, ensured = _daemon(monkeypatch)
    sentinel = worker_mod.drain_sentinel_path(token)
    sentinel.parent.mkdir(parents=True, exist_ok=True)
    sentinel.write_text(token + "\n", encoding="utf-8")
    _write_crashed_state(_dead_child(token))
    calls: list[str] = []

    def _unexpected(_incarnation: str) -> None:
        calls.append(_incarnation)

    monkeypatch.setattr(supervisor_module, "recover_owned_groups", _unexpected)

    now = 3000.0
    daemon.reconcile(now)
    assert calls == [], "a proven clean drain must not trigger emergency recovery"
    state = supervise.read_state()
    assert state.child is None
    assert state.restart_count == 1
    deadline = state.next_attempt_at
    daemon.reconcile((deadline if deadline is not None else now) + 1.0)
    assert ensured == [COMMIT]


def test_sigkilled_worker_group_is_recovered_before_replacement(
    jobs_db: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    crash_token: str,
) -> None:
    """A real SIGKILLed worker leaves no descendant after the crash path.

    A real maintained worker claims a long-lived command whose process group
    ignores SIGTERM; the worker itself is then SIGKILLed so nothing drains the
    group. The daemon crash path must recover that exact owned group by its
    persisted process-group id (SIGKILL and reap it),
    clear the dead child exactly once, and permit a replacement only on a
    later tick.
    """
    db_conf = _db_conf_from_conninfo(jobs_db, tmp_path)
    monkeypatch.setenv("LUBKO_DATABASE_CONFIG", str(db_conf))
    monkeypatch.setenv("LUBKO_SERVER", "alpha-server")
    token = crash_token
    worker = _spawn_real_worker(db_conf, token=token)
    command = (
        'trap "" TERM; exec '
        + sys.executable
        + ' -c "import signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); '
        'time.sleep(120)"'
    )
    job_id = _insert_pending_job(jobs_db, str(tmp_path), command)
    incarnation: object = None
    try:
        child_pgid, incarnation = _wait_for_claim(jobs_db, job_id, tmp_path)
        assert incarnation == token
        assert worker_mod.group_has_members(child_pgid)

        os.kill(worker.pid, 9)
        worker.wait(timeout=10)
        assert worker.poll() is not None
        assert worker_mod.group_has_members(child_pgid), (
            "precondition: the SIGKILLed worker left its command group alive"
        )

        supervise.request_run(COMMIT, repo="", uv_path="uv", worker_id="issue161-worker")
        _write_crashed_state(_dead_child(token))
        daemon = SupervisorDaemon(Settings())
        ensured: list[str] = []
        monkeypatch.setattr(daemon, "_ensure_worker", ensured.append)
        monkeypatch.setattr(daemon, "_record_mission_progress", lambda _commit: None)
        monkeypatch.setattr(daemon, "_probe_readiness", lambda _now: None)

        now = time.monotonic()
        daemon.reconcile(now)
        assert not worker_mod.group_has_members(child_pgid), "no descendant may survive"
        state = supervise.read_state()
        assert state.child is None, "the child clears only after completed recovery"
        assert state.restart_count == 1
        assert ensured == [], "no replacement during the recovery reconcile"
        assert _read_job_field(jobs_db, job_id, "status") == "running", (
            "owned-group recovery proves the OS group; the root row stays "
            "eligible for normal stale-lease recovery"
        )

        _assert_stale_recovery_finalizes(jobs_db, job_id)

        restart_deadline = state.next_attempt_at
        daemon.reconcile(
            (restart_deadline if restart_deadline is not None else now) + 1.0,
        )
        assert ensured == [COMMIT], "replacement only after completed recovery"
    finally:
        if worker.poll() is None:
            lifecycle.stop_worker(_meta_for_live(worker, tmp_path, token), 5.0)
        _cancel_job(jobs_db, job_id)


def _assert_stale_recovery_finalizes(jobs_db: str, job_id: object) -> None:
    """Finalize the root row via ordinary stale-lease recovery and assert terminal.

    Args:
        jobs_db: PostgreSQL connection string.
        job_id: The job identifier.
    """
    with psycopg.connect(jobs_db) as conn:
        recovered = worker_mod.recover_stale_jobs(conn, "alpha-server")
    assert any(str(job_id) == str(row_id) for row_id, _incarnation in recovered)
    deadline = time.monotonic() + 10.0
    status = None
    while time.monotonic() < deadline:
        status = _read_job_field(jobs_db, job_id, "status")
        if status in {"failed", "cancelled"}:
            break
        time.sleep(0.05)
    assert status in {"failed", "cancelled"}


def _cancel_job(jobs_db: str, job_id: object) -> None:
    """Force a claimed job terminal so teardown cannot leave it running.

    Args:
        jobs_db: PostgreSQL connection string.
        job_id: The job identifier.
    """
    with suppress(psycopg.Error, OSError), psycopg.connect(jobs_db) as conn:
        if _read_job_field(jobs_db, job_id, "status") in {"pending", "running"}:
            conn.execute(
                "UPDATE lubko.jobs SET payload = jsonb_set("
                "(payload::jsonb), '{state,status}', '\"cancelled\"'::jsonb)"
                " WHERE id = %s",
                (job_id,),
            )
