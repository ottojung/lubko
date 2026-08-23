"""Process-level regressions for GitHub issue #75.

Planned worker retirement must never force-kill or forget a worker before every
owned command process group is dead/reaped/finalized or safely transferred. The
outer lifecycle authority must observe the worker's explicit safe-to-reap
boundary (a drain acknowledgement) rather than race two equal-duration timers,
and a wedged worker's surviving command groups must be recovered by exact
process-group identity rather than silently orphaned.

The first two tests are pure process-level (no PostgreSQL) and exercise the
drain/stop protocol and the exact-ownership recovery directly. The remaining
tests run the same scenarios through a real maintained worker and through the
external supervisor restart path; they require a local PostgreSQL cluster and
are skipped automatically when one is unavailable.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Final, Self, cast
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from psycopg import connect

from lubko import lifecycle
from lubko import worker as worker_mod
from lubko.supervisor import OwnedGroupRecoveryError, recover_owned_groups
from tests import _process_guard as guard
from tests.test_lifecycle import identity_of

if TYPE_CHECKING:
    from collections.abc import Iterator

SLEEP_BIN: Final = __import__("shutil").which("sleep") or "/bin/sleep"
REPAIR_WORKER_ID: Final = "repair-worker"
REPAIR_TIMINGS: Final = {
    "LUBKO_POLL_INTERVAL_SECONDS": "0.05",
    "LUBKO_PROCESS_POLL_INTERVAL_SECONDS": "0.01",
    "LUBKO_CANCEL_GRACE_SECONDS": "0.5",
    "LUBKO_LEASE_DURATION_SECONDS": "2.0",
    "LUBKO_LEASE_REFRESH_INTERVAL_SECONDS": "0.15",
    "LUBKO_LEASE_RECOVERY_INTERVAL_SECONDS": "0.2",
    "LUBKO_LEASE_SAFETY_MARGIN_SECONDS": "0.3",
    "LUBKO_OUTPUT_PUBLICATION_INTERVAL_SECONDS": "0.1",
    "LUBKO_CLAIM_BATCH_LIMIT": "16",
}

#: A scripted worker that mimics the real worker's drain contract: it owns one
#: command process group (which ignores SIGTERM), and on SIGTERM it drains
#: that group (SIGTERM, wait the cancel grace, SIGKILL), writes the
#: per-incarnation drain sentinel, and exits. This lets the no-PG tests prove
#: the outer lifecycle authority waits for the drain boundary instead of
#: racing it.
WORKER_SCRIPT: Final = """
import os, sys, time, signal, subprocess
from contextlib import suppress

token = sys.argv[1]
child_pgid_file = sys.argv[2]
cancel_grace = float(sys.argv[3])
state = os.environ["XDG_STATE_HOME"]
sentinel = os.path.join(state, "lubko/worker/drain", token + ".drained")


def spawn_child():
    return subprocess.Popen(
        [
            "/bin/sh", "-c",
            'trap "" TERM; exec %s -c "import signal,time; '
            'signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(30)  '
            '# lubko_issue75_child"' % sys.executable,
        ],
        start_new_session=True,
    )


child = spawn_child()
pgid = os.getpgid(child.pid)
with open(child_pgid_file, "w") as fh:
    fh.write(str(pgid))


def handle(signum, frame):
    os.killpg(pgid, signal.SIGTERM)
    time.sleep(cancel_grace)
    os.killpg(pgid, signal.SIGKILL)
    with suppress(OSError):
        os.makedirs(os.path.dirname(sentinel), exist_ok=True)
        with open(sentinel, "w") as fh:
            fh.write(token + "\\n")
    os._exit(0)


signal.signal(signal.SIGTERM, handle)
while True:
    time.sleep(0.2)
"""


def _spawn_scripted_worker(
    tmp_path: Path, token: str, cancel_grace: float, *, env_token: str | None = None
) -> subprocess.Popen[bytes]:
    """Spawn the scripted drain worker, registered with the process guard.

    Args:
        tmp_path: Test temporary directory.
        token: Lifecycle token (incarnation) for the worker.
        cancel_grace: Seconds the worker waits before SIGKILLing its group.
        env_token: The ``LUBKO_LIFECYCLE_TOKEN`` to place in the worker's
            environment. Defaults to ``token`` so the worker is recognized as
            owned; a divergent value simulates a live, unowned (wrong-token)
            process that must not be signalled.

    Returns:
        The spawned worker process.
    """
    script = tmp_path / "scripted_worker.py"
    script.write_text(WORKER_SCRIPT, encoding="utf-8")
    child_pgid_file = tmp_path / "child_pgid"
    env = dict(os.environ)
    env["LUBKO_LIFECYCLE_TOKEN"] = env_token if env_token is not None else token
    proc = subprocess.Popen(
        [sys.executable, str(script), token, str(child_pgid_file), str(cancel_grace)],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        close_fds=True,
        env=env,
    )
    guard.register(proc)
    return proc


def _wait_child_pgid(child_pgid_file: Path, timeout: float = 5.0) -> int:
    """Read the child process group id the scripted worker recorded.

    Args:
        child_pgid_file: File the worker writes the pgid to.
        timeout: Maximum seconds to wait.

    Returns:
        The recorded process group id.

    Raises:
        AssertionError: If the worker never publishes a process group id.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if child_pgid_file.exists():
            with suppress(ValueError, OSError):
                pgid = int(child_pgid_file.read_text(encoding="utf-8").strip())
                _register_owned_group(pgid)
                return pgid
        time.sleep(0.02)
    msg = "scripted worker never published its child process group id"
    raise AssertionError(msg)


def _meta_for_live(
    proc: subprocess.Popen[bytes], tmp_path: Path, token: str
) -> lifecycle.WorkerMeta:
    """Build running metadata for a live worker process.

    Args:
        proc: Live worker process.
        tmp_path: Repository path to record.
        token: Lifecycle token (incarnation) for the worker.

    Returns:
        Running metadata for the process.
    """
    identity = identity_of(proc)
    return lifecycle.WorkerMeta(
        schema_version=1,
        state=lifecycle.STATE_RUNNING,
        pid=identity.pid,
        pgid=identity.pgid,
        sid=identity.sid,
        start_time_ticks=identity.start_time_ticks,
        token=token,
        repo=str(tmp_path),
        git_commit="a" * 40,
        worker_id="test-worker",
        log_path=str(lifecycle.worker_log_path()),
        started_at=time.time(),
        stopped_at=None,
    )


def test_stop_worker_respects_drain_of_sigterm_ignoring_group(tmp_path: Path) -> None:
    """The outer authority must not kill the worker before its group is drained.

    A command whose process group ignores SIGTERM survives until the worker's
    inner cancel grace boundary. The outer ``stop_worker`` must wait for the
    worker's drain acknowledgement and must not SIGKILL the worker while the
    command group is still alive, must leave no descendant process, and must see
    the command reach a terminal (gone) state.
    """
    token = f"issue75-drain-{uuid4().hex}"
    cancel_grace = 1.0
    proc = _spawn_scripted_worker(tmp_path, token, cancel_grace)
    meta = _meta_for_live(proc, tmp_path, token)
    child_pgid = _wait_child_pgid(tmp_path / "child_pgid")
    assert worker_mod.group_has_members(child_pgid)

    # While the command group is still alive, the outer authority must not have
    # force-killed the worker: that would be the forbidden equal-timeout race.
    outer_deadline = time.monotonic() + 15.0
    killed_early = False
    while worker_mod.group_has_members(child_pgid) and time.monotonic() < outer_deadline:
        if proc.poll() is not None:
            killed_early = True
        time.sleep(0.02)
    assert not killed_early, "outer authority force-killed the worker before its group was drained"

    stopped = lifecycle.stop_worker(meta, 5.0, cancel_grace_seconds=cancel_grace)
    assert stopped
    assert proc.poll() is not None
    assert not worker_mod.group_has_members(child_pgid)
    # The drain sentinel proves the worker reached its safe-to-reap boundary.
    sentinel = worker_mod.drain_sentinel_path(token)
    assert sentinel.exists()
    assert sentinel.read_text(encoding="utf-8").strip() == token


def test_stop_worker_equal_timeouts_do_not_race_drain(tmp_path: Path) -> None:
    """Equal outer/inner timeouts must no longer race worker cleanup.

    With ``stop_grace == cancel_grace``, the worker still drains its group
    before the outer authority may treat it as wedged, because the outer wait
    floor is the worker's cancel grace plus finalization slack.
    """
    token = f"issue75-equal-timeout-{uuid4().hex}"
    cancel_grace = 1.0
    proc = _spawn_scripted_worker(tmp_path, token, cancel_grace)
    meta = _meta_for_live(proc, tmp_path, token)
    child_pgid = _wait_child_pgid(tmp_path / "child_pgid")
    stopped = lifecycle.stop_worker(meta, cancel_grace, cancel_grace_seconds=cancel_grace)
    assert stopped
    assert proc.poll() is not None
    assert not worker_mod.group_has_members(child_pgid)
    assert worker_mod.drain_sentinel_path(token).exists()


class _FakeCursor:
    """Minimal cursor double returning a fixed row set for recovery queries."""

    def __init__(self, rows: list[tuple[object, ...]]) -> None:
        self._rows = rows
        self.executed: list[tuple[str, object]] = []

    def __enter__(self) -> Self:
        """Enter the cursor context.

        Returns:
            Self, so the ``with`` target is the cursor itself.
        """
        return self

    def __exit__(self, _exc_type: object, _exc: object, _tb: object) -> None:
        """Exit the cursor context (no-op)."""
        return

    def execute(self, sql: str, params: object = None) -> None:
        """Record the query (ignored)."""
        self.executed.append((sql, params))

    def fetchall(self) -> list[tuple[object, ...]]:
        """Return the fixed rows."""
        return self._rows


class _FakeConn:
    """Minimal connection double for :func:`recover_owned_job_groups`."""

    def __init__(self, rows: list[tuple[object, ...]]) -> None:
        self._rows = rows

    @contextmanager
    def transaction(self) -> Iterator[_FakeConn]:
        """Yield self as the transaction context."""
        yield self

    def cursor(self, **_kwargs: object) -> _FakeCursor:
        """Return a fake cursor."""
        return _FakeCursor(self._rows)


def test_recover_owned_job_groups_kills_exact_groups_only() -> None:
    """Emergency recovery must terminate only exact owned groups, never by name.

    A wedged worker can leave a command group alive after it is SIGKILLed. The
    recovery targets the exact process-group id persisted in the job row for the
    retired incarnation and never touches other groups. It is also PID-reuse
    safe and fail-closed: a persisted group id that has since been recycled by
    an unrelated process (different start-time ticks) is never signalled even
    though it is still alive, and the unverifiable group is reported as
    ``unresolved`` so the orchestrator blocks rather than clearing authority.
    """
    unrelated = subprocess.Popen(
        [SLEEP_BIN, "300"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        close_fds=True,
    )
    guard.register(unrelated)
    owned = subprocess.Popen(
        [
            "/bin/sh",
            "-c",
            'trap "" TERM; exec ' + sys.executable + ' -c "import signal,time; '
            'signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(30)"',
        ],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        close_fds=True,
    )
    guard.register(owned)
    owned_pgid = os.getpgid(owned.pid)
    # A stale, reused group id: it points at the still-alive ``unrelated``
    # process but carries start-time ticks that do NOT match that process, so
    # recovery must treat it as a recycled id, leave it untouched, and report it
    # as unresolved (a durable blocking obligation).
    reused_pgid = os.getpgid(unrelated.pid)
    reused_ticks = (lifecycle.proc_start_ticks(unrelated.pid) or 0) + 1
    try:
        incarnation = "issue75-wedged-incarnation"
        conn = _FakeConn([
            (uuid4(), str(owned_pgid), str(lifecycle.proc_start_ticks(owned.pid))),
            (uuid4(), str(reused_pgid), str(reused_ticks)),
        ])
        result = worker_mod.recover_owned_job_groups(
            cast("worker_mod.JobsConnection", conn), incarnation, 0.5
        )
        assert result.reaped == [owned_pgid]
        assert result.surviving == []
        assert result.unresolved == [reused_pgid]
        assert not worker_mod.group_has_members(owned_pgid)
        # The reused id was NOT acted on: the unrelated process is untouched.
        assert worker_mod.group_has_members(reused_pgid)
    finally:
        if owned.poll() is None:
            with suppress(ProcessLookupError):
                os.killpg(owned_pgid, signal.SIGKILL)
        if unrelated.poll() is None:
            with suppress(ProcessLookupError):
                os.killpg(os.getpgid(unrelated.pid), signal.SIGKILL)
        for p in (owned, unrelated):
            with suppress(Exception):
                p.wait(timeout=5)
            guard.unregister(p)


def _spawn_real_worker(
    db_conf: Path,
    *,
    worker_id: str = REPAIR_WORKER_ID,
    token: str | None = None,
) -> subprocess.Popen[bytes]:
    """Spawn a real queue-consuming worker registered with the process guard.

    Args:
        db_conf: Database configuration file for the worker.
        worker_id: Worker identifier the worker records on claims.
        token: Explicit lifecycle token (incarnation) for the worker, so tests
            can deterministically authorize a later ``stop_worker`` against the
            exact process. ``None`` lets the worker generate its own token.

    Returns:
        The spawned worker process.
    """
    env = dict(os.environ)
    env["LUBKO_DATABASE_CONFIG"] = str(db_conf)
    env["LUBKO_SERVER"] = "alpha-server"
    env["LUBKO_WORKER_ID"] = worker_id
    if token is not None:
        env["LUBKO_LIFECYCLE_TOKEN"] = token
    env.update(REPAIR_TIMINGS)
    proc = subprocess.Popen(
        [sys.executable, "-m", "lubko.worker"],
        cwd=Path(__file__).resolve().parent.parent,
        env=env,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        close_fds=True,
    )
    guard.register(proc)
    return proc


def _db_conf_from_conninfo(conninfo: str, tmp_path: Path) -> Path:
    """Write a database config file the worker can read from a conninfo string.

    Args:
        conninfo: A psycopg-style conninfo string.
        tmp_path: Directory for the config file.

    Returns:
        The config file path.
    """
    host = port = ""
    for part in conninfo.split():
        if part.startswith("host="):
            host = part.split("=", 1)[1]
        elif part.startswith("port="):
            port = part.split("=", 1)[1]
    conf = tmp_path / "database.conf"
    conf.write_text(
        f"host={host}\nport={port}\ndbname=postgres\nuser=postgres\npassword=local-trust\n",
        encoding="utf-8",
    )
    conf.chmod(0o600)
    return conf


def _insert_pending_job(conninfo: str, cwd: str, command: str) -> object:
    """Insert a protocol v4 pending command job running a shell snippet.

    Args:
        conninfo: PostgreSQL connection string.
        cwd: Working directory for the job.
        command: Shell snippet, executed by an explicit ``/bin/sh -c`` argv.

    Returns:
        The job identifier.
    """
    payload = json.dumps({
        "v": 4,
        "type": "command",
        "server": "alpha-server",
        "request": {"cwd": cwd, "process": ["/bin/sh", "-c", command]},
        "state": {"status": "pending"},
    })
    with connect(conninfo) as conn:
        row = conn.execute(
            "INSERT INTO lubko.jobs (payload) VALUES (%s) RETURNING id",
            (payload,),
        ).fetchone()
    assert row is not None
    return row[0]


_JOB_FIELD_QUERIES: Final = {
    "status": "SELECT (payload::jsonb)->'state'->>'status' FROM lubko.jobs WHERE id = %s",
    "pgid": "SELECT (payload::jsonb)->'state'->>'process_pgid' FROM lubko.jobs WHERE id = %s",
    "incarnation": (
        "SELECT (payload::jsonb)->'state'->>'worker_incarnation' FROM lubko.jobs WHERE id = %s"
    ),
}


def _read_job_field(conninfo: str, job_id: object, field: str) -> object:
    """Read one whitelisted JSON field from a job's payload.

    Args:
        conninfo: PostgreSQL connection string.
        job_id: Job identifier.
        field: Key selecting the exact prebuilt query in
            :data:`_JOB_FIELD_QUERIES`.

    Returns:
        The decoded field value, or ``None``.
    """
    with connect(conninfo) as conn:
        row = conn.execute(_JOB_FIELD_QUERIES[field], (job_id,)).fetchone()
    if row is None or row[0] is None:
        return None
    return row[0]


def _wait_for_claim(jobs_db: str, job_id: object, tmp_path: Path) -> tuple[int, str]:
    """Wait for the worker to claim the job and publish its process group.

    Args:
        jobs_db: PostgreSQL connection string.
        job_id: The pending job identifier.
        tmp_path: Repository path recorded in metadata.

    Returns:
        The claimed ``(child_pgid, incarnation)``.
    """
    del tmp_path
    deadline = time.monotonic() + 15.0
    child_pgid: int | None = None
    incarnation: object = None
    while time.monotonic() < deadline:
        status = _read_job_field(jobs_db, job_id, "status")
        if status == "running":
            pgid = _read_job_field(jobs_db, job_id, "pgid")
            incarnation = _read_job_field(jobs_db, job_id, "incarnation")
            if pgid is not None and incarnation is not None:
                child_pgid = int(str(pgid))
                break
        time.sleep(0.05)
    assert child_pgid is not None
    assert incarnation is not None
    assert worker_mod.group_has_members(child_pgid)
    _register_owned_group(child_pgid)
    return child_pgid, str(incarnation)


def _assert_root_terminal(jobs_db: str, job_id: object) -> None:
    """Assert the root job reaches a deterministic terminal state.

    Args:
        jobs_db: PostgreSQL connection string.
        job_id: The job identifier.
    """
    status_deadline = time.monotonic() + 10.0
    status = None
    while time.monotonic() < status_deadline:
        status = _read_job_field(jobs_db, job_id, "status")
        if status in {"succeeded", "failed", "cancelled"}:
            break
        time.sleep(0.05)
    assert status == "cancelled"


def test_planned_replacement_drains_sigterm_ignoring_command(
    jobs_db: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A planned replacement must drain a SIGTERM-ignoring command group.

    Runs a real maintained worker, claims a command whose process group ignores
    SIGTERM, requests an intentional replacement via ``stop_worker``, and proves
    the outer authority does not kill/forget the worker before the command group
    is SIGKILLed and gone, no descendant remains, the root job reaches a terminal
    state, and a replacement worker is the sole consumer only after the old
    execution ownership is safe.
    """
    db_conf = _db_conf_from_conninfo(jobs_db, tmp_path)
    # The readiness roundtrip probe (verify_worker_consumes_queue) runs in this
    # test process and loads the database configuration from the environment,
    # exactly like any production caller of the outer lifecycle authority.
    monkeypatch.setenv("LUBKO_DATABASE_CONFIG", str(db_conf))
    worker_token = f"issue75-worker-{uuid4().hex}"
    replacement_token = f"issue75-replacement-{uuid4().hex}"
    worker = _spawn_real_worker(db_conf, token=worker_token)
    py = sys.executable
    command = (
        'trap "" TERM; exec ' + py + ' -c "import signal,time; '
        'signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(30)"'
    )
    job_id = _insert_pending_job(jobs_db, str(tmp_path), command)
    incarnation: object = None
    try:
        child_pgid, incarnation = _wait_for_claim(jobs_db, job_id, tmp_path)
        meta = _meta_for_live(worker, tmp_path, str(incarnation))

        # While the command group is alive the outer authority must not kill
        # the worker: that is the forbidden equal-timeout race.
        race_deadline = time.monotonic() + 15.0
        killed_early = False
        while worker_mod.group_has_members(child_pgid) and time.monotonic() < race_deadline:
            if worker.poll() is not None:
                killed_early = True
            time.sleep(0.02)
        assert not killed_early

        stopped = lifecycle.stop_worker(meta, 5.0)
        assert stopped
        assert worker.poll() is not None
        assert not worker_mod.group_has_members(child_pgid)

        _assert_root_terminal(jobs_db, job_id)

        # A replacement is the sole consumer only after old execution ownership
        # is safe: no old-incarnation group may be alive, and the replacement
        # genuinely consumes the queue.
        assert not worker_mod.group_has_members(child_pgid)
        replacement = _spawn_real_worker(
            db_conf, worker_id="replacement-worker", token=replacement_token
        )
        try:
            ready = lifecycle.verify_worker_consumes_queue(
                "replacement-worker", str(tmp_path), replacement.pid, 15.0
            )
            assert ready
        finally:
            if replacement.poll() is None:
                lifecycle.stop_worker(_meta_for_live(replacement, tmp_path, replacement_token), 5.0)
    finally:
        if worker.poll() is None:
            lifecycle.stop_worker(_meta_for_live(worker, tmp_path, worker_token), 5.0)
        _recover_incarnation(jobs_db, incarnation)


@pytest.fixture(autouse=True)
def _issue75_server_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    """Give every in-process probe an explicit protocol v4 server identity.

    Protocol v4 has no implicit or default server: queue probes inserted by
    ``verify_worker_consumes_queue`` must be addressed to a configured server.
    """
    monkeypatch.setenv("LUBKO_SERVER", "alpha-server")


@pytest.fixture(autouse=True)
def _issue75_leak_proof() -> Iterator[None]:
    """Deterministically reap every test-owned worker and child group.

    Every #75 focused test spawns a scripted worker (and, transitively, a
    SIGTERM-ignoring command group) that must never reparent to PID 1. Even on
    an assertion/failure path this fixture stops only the exact process groups
    the test explicitly created and recorded at spawn time (group id plus the
    recorded leader's start-time ticks), then asserts none of those owned
    groups survives. No global /proc scan and no marker-name matching is used.
    """
    yield
    try:
        _stop_owned_groups()
        _assert_no_owned_groups_live()
    finally:
        OWNED_GROUPS.clear()


@dataclass(frozen=True)
class _OwnedGroup:
    """A command process group the test explicitly created and recorded.

    Attributes:
        pgid: Exact process group id (the spawned child is its leader).
        leader_start_ticks: Start-time ticks of the leader at spawn time,
            proving a later signal targets the same exact process instance.
    """

    pgid: int
    leader_start_ticks: int


OWNED_GROUPS: dict[int, _OwnedGroup] = {}


def _register_owned_group(pgid: int) -> None:
    """Record an explicitly created child group for exact-identity teardown.

    Args:
        pgid: The exact group id published by the test-spawned child (which
            leads its own dedicated session/process group).
    """
    ticks = lifecycle.proc_start_ticks(pgid)
    assert ticks is not None, "recorded owned group leader vanished before registration"
    OWNED_GROUPS[pgid] = _OwnedGroup(pgid=pgid, leader_start_ticks=ticks)


def _still_ours(group: _OwnedGroup) -> bool:
    """Return whether the recorded group identity is unchanged since spawn.

    Args:
        group: The recorded owned group.

    Returns:
        ``True`` only when the live leader's start-time ticks still match the
        recorded value, so signalling can never hit a recycled group id.
    """
    return lifecycle.proc_start_ticks(group.pgid) == group.leader_start_ticks


def _stop_owned_groups() -> None:
    """Terminate every still-live recorded group by its exact identity."""
    for group in OWNED_GROUPS.values():
        if not _still_ours(group) or not worker_mod.group_has_members(group.pgid):
            continue
        with suppress(ProcessLookupError, OSError):
            os.killpg(group.pgid, signal.SIGTERM)
        deadline = time.monotonic() + 2.0
        while (
            time.monotonic() < deadline
            and _still_ours(group)
            and worker_mod.group_has_members(group.pgid)
        ):
            time.sleep(0.02)
        if _still_ours(group) and worker_mod.group_has_members(group.pgid):
            with suppress(ProcessLookupError, OSError):
                os.killpg(group.pgid, signal.SIGKILL)


def _assert_no_owned_groups_live() -> None:
    """Assert teardown converged for every recorded owned group.

    A recorded group that still has live members is a failure in BOTH cases:
    when its identity is still verifiable as ours (cleanup did not converge),
    and when its leader identity became unavailable/mismatched (unresolved —
    never signalled, but explicitly NOT clean). Only identity-verified groups
    are ever signalled; an unresolved live target must fail loudly instead of
    being silently forgotten.

    Raises:
        AssertionError: If any recorded group has surviving members, whether
            verified-ours (surviving) or identity-unresolved (unresolved).
    """
    survivors = [
        group
        for group in OWNED_GROUPS.values()
        if _still_ours(group) and worker_mod.group_has_members(group.pgid)
    ]
    unresolved = [
        group
        for group in OWNED_GROUPS.values()
        if not _still_ours(group) and worker_mod.group_has_members(group.pgid)
    ]
    if not survivors and not unresolved:
        return
    _stop_owned_groups()
    time.sleep(0.2)
    survivors = [
        group
        for group in OWNED_GROUPS.values()
        if _still_ours(group) and worker_mod.group_has_members(group.pgid)
    ]
    unresolved = [
        group
        for group in OWNED_GROUPS.values()
        if not _still_ours(group) and worker_mod.group_has_members(group.pgid)
    ]
    if survivors or unresolved:
        msg = (
            "issue75-owned command groups not clean after focused tests: "
            + ", ".join(f"pgid={g.pgid} (surviving)" for g in survivors)
            + ", ".join(f"pgid={g.pgid} (unresolved identity)" for g in unresolved)
        )
        raise AssertionError(msg)


def test_stop_worker_rejects_live_unowned_wrong_token(tmp_path: Path) -> None:
    """stop_worker must not signal or report success for a live unowned worker.

    A live process may match the recorded identity (PID/PGID/SID/start ticks)
    yet carry a lifecycle token that is not ours (or none). Such a live,
    unowned process must never be signalled, and stop_worker must report
    failure rather than claiming retirement -- so authority is never handed off.
    """
    token = f"issue75-wrong-token-{uuid4().hex}"
    # The process environment carries a DIFFERENT token, so it is live but not
    # owned by the recorded incarnation.
    proc = _spawn_scripted_worker(
        tmp_path,
        token,
        1.0,
        env_token=f"issue75-other-{uuid4().hex}",
    )
    meta = _meta_for_live(proc, tmp_path, token)
    try:
        assert lifecycle.process_identity(proc.pid) is not None
        assert lifecycle.process_has_token(proc.pid, token) is False
        stopped = lifecycle.stop_worker(meta, 5.0, cancel_grace_seconds=1.0)
        assert stopped is False
        # The worker must not have been signalled: still alive, command group
        # intact.
        assert proc.poll() is None
        assert worker_mod.group_has_members(os.getpgid(proc.pid))
    finally:
        # Deterministic reap so teardown sees nothing live; the worker's own
        # drain handler terminates its child group.
        with suppress(ProcessLookupError, OSError):
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        try:
            proc.wait(timeout=5)
        except subprocess.SubprocessError:
            with suppress(ProcessLookupError, OSError):
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            with suppress(Exception):
                proc.wait(timeout=5)
        guard.unregister(proc)


def test_start_gate_release_runs_user_code(tmp_path: Path) -> None:
    """A gated start runs the user argv only after the gate is released.

    The wrapper is the dedicated session/process-group leader and blocks on the
    gate before ``exec``. While gated it must not have executed any user code;
    once the worker durably persists the identity and releases the gate, the
    exact same PID execs the user program.
    """
    sentinel = tmp_path / "ran"
    proc, stdout_path, stderr_path, pgid, gate_fd, _so_r, _se_r = worker_mod.spawn_job(
        worker_mod.Job(
            id=uuid4(),
            cwd=str(tmp_path),
            process=(
                sys.executable,
                "-c",
                "import sys; open(sys.argv[1], 'w').close()",
                str(sentinel),
            ),
        )
    )
    guard.register(proc)
    try:
        assert pgid == proc.pid
        time.sleep(0.3)
        # Still gated: the user program has not been exec'd.
        assert proc.poll() is None
        assert not sentinel.exists()
        worker_mod.release_gate(gate_fd)
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline and not sentinel.exists():
            time.sleep(0.02)
        assert sentinel.exists()
        assert proc.wait(timeout=5) == 0
        assert worker_mod.read_output(stdout_path) == b""
    finally:
        with suppress(ProcessLookupError, OSError):
            os.killpg(pgid, signal.SIGKILL)
        with suppress(Exception):
            proc.wait(timeout=5)
        guard.unregister(proc)
        stdout_path.unlink(missing_ok=True)
        stderr_path.unlink(missing_ok=True)


def test_start_gate_persist_failure_aborts_cleanly(tmp_path: Path) -> None:
    """A persist failure terminates the gated process with no user side effect.

    This is the exact branch ``Supervisor._start_job`` takes when
    :func:`lubko.worker._persist_process` fails: the gate is closed WITHOUT the
    release byte, the still-gated (childless) process group is terminated and
    reaped, and the capture files are removed. The user program must never run
    and no unowned process may survive.
    """
    sentinel = tmp_path / "ran"
    proc, stdout_path, stderr_path, pgid, gate_fd, _so_r, _se_r = worker_mod.spawn_job(
        worker_mod.Job(
            id=uuid4(),
            cwd=str(tmp_path),
            process=(
                sys.executable,
                "-c",
                "import sys; open(sys.argv[1], 'w').close()",
                str(sentinel),
            ),
        )
    )
    guard.register(proc)
    try:
        time.sleep(0.3)
        assert proc.poll() is None  # gated, user code not run
        # Simulate the persist-failure branch of _start_job.
        worker_mod.abort_gated_start(proc, pgid, stdout_path, stderr_path, gate_fd)
        # No user side effect, no surviving unowned process, no leftover files.
        assert not sentinel.exists()
        assert proc.poll() is not None
        assert not worker_mod.group_has_members(pgid)
        assert not stdout_path.exists()
        assert not stderr_path.exists()
    finally:
        with suppress(ProcessLookupError, OSError):
            os.killpg(pgid, signal.SIGKILL)
        with suppress(Exception):
            proc.wait(timeout=5)
        guard.unregister(proc)
        stdout_path.unlink(missing_ok=True)
        stderr_path.unlink(missing_ok=True)


def test_start_gate_worker_death_before_release_leaves_no_orphan(
    tmp_path: Path,
) -> None:
    """A worker SIGKILLed before persist/release never execs user code.

    The gate write end is the worker's only handle to release the wrapper. If
    the worker dies (simulated here by closing that fd without releasing), the
    kernel closes it, the wrapper reads EOF, and it exits WITHOUT executing the
    user argv. No side effect survives and no unowned process remains, so a
    replacement authority can claim the same job without overlap.
    """
    sentinel = tmp_path / "ran"
    proc, stdout_path, stderr_path, pgid, gate_fd, _so_r, _se_r = worker_mod.spawn_job(
        worker_mod.Job(
            id=uuid4(),
            cwd=str(tmp_path),
            process=(
                sys.executable,
                "-c",
                "import sys; open(sys.argv[1], 'w').close()",
                str(sentinel),
            ),
        )
    )
    guard.register(proc)
    try:
        time.sleep(0.3)
        assert proc.poll() is None  # gated
        # Simulate the worker being force-killed: its gate write end closes.
        with suppress(OSError):
            os.close(gate_fd)
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline and proc.poll() is None:
            time.sleep(0.02)
        # The wrapper exited on EOF without exec'ing the user program.
        assert proc.poll() is not None
        assert not sentinel.exists()
        assert not worker_mod.group_has_members(pgid)
    finally:
        with suppress(ProcessLookupError, OSError):
            os.killpg(pgid, signal.SIGKILL)
        with suppress(Exception):
            proc.wait(timeout=5)
        guard.unregister(proc)
        stdout_path.unlink(missing_ok=True)
        stderr_path.unlink(missing_ok=True)


def test_recover_owned_groups_db_failure_blocks(monkeypatch: pytest.MonkeyPatch) -> None:
    """Recovery DB/config failure is a durable blocking obligation.

    When the database configuration is missing the supervisor's owned-group
    recovery must raise :class:`OwnedGroupRecoveryError` rather than silently
    skipping, so the retired child and sole-consumer authority are preserved
    and no replacement is spawned alongside stale groups.
    """
    monkeypatch.setattr(
        "lubko.supervisor.load_database_config",
        lambda: (_ for _ in ()).throw(ValueError("no database configuration available")),
    )
    with pytest.raises(OwnedGroupRecoveryError):
        recover_owned_groups("issue75-blocked-incarnation")


def test_recover_owned_groups_survivor_blocks(monkeypatch: pytest.MonkeyPatch) -> None:
    """A surviving verified-ours group keeps recovery a blocking obligation.

    When emergency recovery acts on a group it proved is ours but the group
    cannot be reaped within the cancel grace, the supervisor must raise
    :class:`OwnedGroupRecoveryError` rather than reporting success. The caller
    then preserves the retired child and does not spawn a replacement alongside
    a still-live side-effecting group.
    """
    surviving_pgid = 424242
    monkeypatch.setattr(
        worker_mod,
        "recover_owned_job_groups",
        lambda *_, **__: worker_mod.ReclaimedGroups(
            reaped=[], surviving=[surviving_pgid], unresolved=[]
        ),
    )
    fake_db = MagicMock()
    fake_db.conninfo.return_value = ""
    monkeypatch.setattr("lubko.supervisor.load_database_config", lambda: fake_db)
    monkeypatch.setattr("lubko.supervisor.psycopg.connect", lambda *_, **__: MagicMock())
    with pytest.raises(OwnedGroupRecoveryError):
        recover_owned_groups("issue75-survivor-incarnation")


def test_recover_owned_job_groups_reports_survivors(monkeypatch: pytest.MonkeyPatch) -> None:
    """An unreapable verified-ours group is reported as surviving, never hidden.

    When a command group that recovery verified is ours (matching start-time
    ticks, live members) cannot be reaped, the recovery pass must report it as
    ``surviving`` so the orchestrator blocks clearing the retired worker's
    authority. The unreapable outcome is injected through the termination seam
    (``_terminate_one_group`` returning ``False``) so the test is deterministic
    and does not race the kernel's SIGKILL→reap window.
    """
    owned = subprocess.Popen(
        [
            "/bin/sh",
            "-c",
            'trap "" TERM; exec ' + sys.executable + ' -c "import signal,time; '
            'signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(30)"',
        ],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        close_fds=True,
    )
    guard.register(owned)
    owned_pgid = os.getpgid(owned.pid)
    monkeypatch.setattr(worker_mod, "_terminate_one_group", lambda *_, **__: False)
    try:
        conn = _FakeConn([(uuid4(), str(owned_pgid), str(lifecycle.proc_start_ticks(owned.pid)))])
        result = worker_mod.recover_owned_job_groups(
            cast("worker_mod.JobsConnection", conn), "issue75-survivor", 0.5
        )
        assert result.surviving == [owned_pgid]
        assert result.reaped == []
        assert result.unresolved == []
    finally:
        with suppress(ProcessLookupError, OSError):
            os.killpg(owned_pgid, signal.SIGKILL)
        with suppress(Exception):
            owned.wait(timeout=5)
        guard.unregister(owned)


def test_recover_owned_job_groups_missing_ticks_live_unrelated_unresolved() -> None:
    """Legacy/missing ticks over a live unrelated group: no signal, blocks.

    A persisted row whose ``process_start_time_ticks`` is missing (a legacy row
    that never recorded them) but whose ``process_pgid`` points at a still-live
    unrelated process must NOT be signalled. It is nonetheless reported as
    ``unresolved`` so the orchestrator treats it as a durable blocking
    obligation and holds rather than clearing authority or starting a
    replacement.
    """
    unrelated = subprocess.Popen(
        [SLEEP_BIN, "300"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        close_fds=True,
    )
    guard.register(unrelated)
    unrelated_pgid = os.getpgid(unrelated.pid)
    try:
        # Ticks column is ``None`` (missing/legacy) while the group is alive.
        conn = _FakeConn([(uuid4(), str(unrelated_pgid), None)])
        result = worker_mod.recover_owned_job_groups(
            cast("worker_mod.JobsConnection", conn), "issue75-missing-ticks"
        )
        assert result.unresolved == [unrelated_pgid]
        assert result.reaped == []
        assert result.surviving == []
        # The unrelated process is completely untouched.
        assert worker_mod.group_has_members(unrelated_pgid)
        assert unrelated.poll() is None
    finally:
        if unrelated.poll() is None:
            with suppress(ProcessLookupError):
                os.killpg(unrelated_pgid, signal.SIGKILL)
        with suppress(Exception):
            unrelated.wait(timeout=5)
        guard.unregister(unrelated)


def test_recover_owned_job_groups_mismatched_ticks_live_group_unresolved() -> None:
    """Mismatched persisted ticks over a live group: no signal, blocks.

    A persisted row whose ``process_pgid`` points at a live group but whose
    ``process_start_time_ticks`` does not match the live leader's ticks (the id
    was recycled by an unrelated process) must NOT be signalled. It is reported
    as ``unresolved`` so the orchestrator holds rather than risk killing a
    stranger-owned group.
    """
    unrelated = subprocess.Popen(
        [SLEEP_BIN, "300"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        close_fds=True,
    )
    guard.register(unrelated)
    unrelated_pgid = os.getpgid(unrelated.pid)
    # Ticks that are valid/positive but do not match the live leader.
    wrong_ticks = (lifecycle.proc_start_ticks(unrelated.pid) or 0) + 1
    try:
        conn = _FakeConn([(uuid4(), str(unrelated_pgid), str(wrong_ticks))])
        result = worker_mod.recover_owned_job_groups(
            cast("worker_mod.JobsConnection", conn), "issue75-mismatch-ticks"
        )
        assert result.unresolved == [unrelated_pgid]
        assert result.reaped == []
        assert result.surviving == []
        assert worker_mod.group_has_members(unrelated_pgid)
        assert unrelated.poll() is None
    finally:
        if unrelated.poll() is None:
            with suppress(ProcessLookupError):
                os.killpg(unrelated_pgid, signal.SIGKILL)
        with suppress(Exception):
            unrelated.wait(timeout=5)
        guard.unregister(unrelated)


def test_recover_owned_job_groups_missing_ticks_no_members_converges() -> None:
    """Missing ticks over a gone group converges safely (no signal, no block).

    When the persisted group id has no live members the recovery has already
    converged: even with missing/malformed ticks the group is not signalled and
    is not reported as an unresolved blocking obligation.
    """
    dead = subprocess.Popen(
        [SLEEP_BIN, "0.01"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        close_fds=True,
    )
    guard.register(dead)
    dead_pgid = os.getpgid(dead.pid)
    dead.wait(timeout=5)
    guard.unregister(dead)
    # After the process exits the group has no live members.
    assert not worker_mod.group_has_members(dead_pgid)
    conn = _FakeConn([(uuid4(), str(dead_pgid), None)])
    result = worker_mod.recover_owned_job_groups(
        cast("worker_mod.JobsConnection", conn), "issue75-gone-missing-ticks"
    )
    assert result.reaped == []
    assert result.surviving == []
    assert result.unresolved == []


def test_recover_owned_groups_unresolved_blocks(monkeypatch: pytest.MonkeyPatch) -> None:
    """Unresolved (unverifiable) groups keep recovery a blocking obligation.

    When emergency recovery reports a live group whose exact identity could not
    be proven (missing/mismatched ticks) it must raise
    :class:`OwnedGroupRecoveryError` so the orchestrator preserves the retired
    child and does not spawn a replacement alongside a possibly stranger-owned
    group.
    """
    unresolved_pgid = 515151
    monkeypatch.setattr(
        worker_mod,
        "recover_owned_job_groups",
        lambda *_, **__: worker_mod.ReclaimedGroups(
            reaped=[], surviving=[], unresolved=[unresolved_pgid]
        ),
    )
    fake_db = MagicMock()
    fake_db.conninfo.return_value = ""
    monkeypatch.setattr("lubko.supervisor.load_database_config", lambda: fake_db)
    monkeypatch.setattr("lubko.supervisor.psycopg.connect", lambda *_, **__: MagicMock())
    with pytest.raises(OwnedGroupRecoveryError):
        recover_owned_groups("issue75-unresolved-incarnation")


def _recover_incarnation(conninfo: str, incarnation: object) -> None:
    """Recover any surviving command group owned by an incarnation.

    Args:
        conninfo: PostgreSQL connection string.
        incarnation: Worker incarnation whose groups to recover.
    """
    if not incarnation:
        return

    with connect(conninfo) as conn:
        worker_mod.recover_owned_job_groups(conn, str(incarnation), 0.5)


def test_teardown_fails_loudly_on_unresolved_owned_group(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unresolved live owned group is never signalled but fails teardown.

    When the recorded leader's start-time ticks become unavailable while the
    group still has members, exact signal authorization forbids touching it.
    The stop pass must not signal anything, and the leak-proof assertion must
    fail loudly: an unresolved live target is NOT clean and must never be
    silently forgotten.
    """
    proc = subprocess.Popen(
        [SLEEP_BIN, "300"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        close_fds=True,
    )
    guard.register(proc)
    pgid = os.getpgid(proc.pid)
    assert proc.pid == pgid  # dedicated session/group leader
    _register_owned_group(pgid)
    # Simulate the leader identity becoming unreadable AFTER registration.
    monkeypatch.setattr(lifecycle, "proc_start_ticks", lambda _pid: None)
    try:
        _stop_owned_groups()
        # No signal was delivered: the process is untouched and still live.
        assert proc.poll() is None
        assert worker_mod.group_has_members(pgid)
        with pytest.raises(AssertionError, match="unresolved identity"):
            _assert_no_owned_groups_live()
    finally:
        OWNED_GROUPS.pop(pgid, None)
        with suppress(ProcessLookupError, OSError):
            os.killpg(pgid, signal.SIGKILL)
        with suppress(Exception):
            proc.wait(timeout=5)
        guard.unregister(proc)


def test_abort_gated_start_ignores_reused_pgid_after_reap(tmp_path: Path) -> None:
    """After the direct wrapper is reaped, the numeric PGID is never signalled.

    Simulates PGID reuse: the original gated Popen is already terminal and
    reaped while the recorded numeric group id points at a live UNRELATED
    process (the "reuse"). abort_gated_start must treat the abort as converged
    purely from the reaped direct child, must NOT signal the unrelated group,
    and must clean the capture files rather than retain them.
    """
    reused = subprocess.Popen(
        [SLEEP_BIN, "300"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        close_fds=True,
    )
    guard.register(reused)
    # A real, already-terminal-and-reaped Popen stands in for the original
    # unreleased wrapper.
    original = subprocess.Popen(
        [sys.executable, "-c", "pass"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        close_fds=True,
    )
    guard.register(original)
    original.wait(timeout=5)
    guard.unregister(original)
    assert original.poll() is not None
    stdout_path = tmp_path / "out.capture"
    stderr_path = tmp_path / "err.capture"
    stdout_path.write_bytes(b"")
    stderr_path.write_bytes(b"")
    try:
        converged = worker_mod.abort_gated_start(
            original, os.getpgid(reused.pid), stdout_path, stderr_path, -1
        )
        assert converged is True
        # The reused group was NOT signalled: the unrelated process is intact.
        assert reused.poll() is None
        assert worker_mod.group_has_members(os.getpgid(reused.pid))
        # Capture files are cleaned on convergence, never retained.
        assert not stdout_path.exists()
        assert not stderr_path.exists()
    finally:
        with suppress(ProcessLookupError, OSError):
            os.killpg(os.getpgid(reused.pid), signal.SIGKILL)
        with suppress(Exception):
            reused.wait(timeout=5)
        guard.unregister(reused)
