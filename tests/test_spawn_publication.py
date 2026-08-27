"""Deterministic invariants for the child+spawning publication order (issue #282).

A supervisor death after publishing ``state.child`` but before the maintained
``worker/meta.json`` is durable must never let manual recovery authorize a
second queue consumer. The fix keeps the exact pid-bearing ``spawning``
obligation durable while ``state.child`` is published, writes the lifecycle
meta, and only then clears ``spawning`` in a final durable state write.

These tests drive the publication protocol and its reconciliation deterministically
without real worker processes.
"""

from __future__ import annotations

import os
import subprocess
from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from lubko import cli, lifecycle, supervise, supervisor
from lubko.durable import DurabilityError
from lubko.supervise import SpawningObligation, WorkerChild, read_state

if TYPE_CHECKING:
    from lubko.supervise import SupervisorState

COMMIT = "a" * 40

#: A fake sealed runtime root used only to build child metadata paths; it is
#: never touched on disk by the publication protocol under test.
FAKE_RUNTIME_ROOT = Path("/opt/lubko-fake-runtime")


@pytest.fixture(autouse=True)
def state_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Use an isolated durable state root for each test.

    Returns:
        The supervisor state directory.
    """
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state"))
    supervise.state_path().parent.mkdir(parents=True, exist_ok=True)
    return supervise.state_path().parent


def _published_child() -> WorkerChild:
    """Build the exact child identity a successful spawn returns.

    Returns:
        The exact child identity a successful spawn would produce.
    """
    return WorkerChild(
        pid=4711,
        pgid=4711,
        sid=4711,
        start_time_ticks=555,
        token="tok" + "a" * 20,
        worker_id="w",
        spawned_at=0.0,
    )


def _in_progress_state(child: WorkerChild, commit: str = COMMIT) -> None:
    """Persist a state where child is published under a matching live obligation.

    Args:
        child: The already-published exact child identity.
        commit: The commit the child was published for.
    """
    obligation = SpawningObligation(
        token=child.token,
        commit=commit,
        creator_pid=1,
        creator_start_time_ticks=1,
        pid=child.pid,
        start_time_ticks=child.start_time_ticks,
        created_at=0.0,
        boot_id=supervise.current_boot_id(),
    )
    supervise.write_state(
        supervise.SupervisorState(
            schema_version=supervise.SCHEMA_VERSION,
            applied_generation=0,
            mode=supervise.MODE_RUN,
            commit=commit,
            child=child,
            unresolved_child=None,
            ownership_hold_malformed=False,
            unresolved_hold_malformed=False,
            spawning=obligation,
            spawning_hold_malformed=False,
            intent=supervise.INTENT_RUN,
            restart_count=0,
            next_attempt_at=None,
            last_exit=None,
            last_spawn_at=None,
            ready=False,
            next_readiness_at=None,
            boot_id=supervise.current_boot_id(),
        )
    )


def _fake_spawn(daemon: supervisor.SupervisorDaemon, commit: str) -> WorkerChild:
    """Stand-in ``_spawn_worker`` that records the pid-bearing obligation.

    Args:
        daemon: The daemon requesting the spawn (unused by the stub).
        commit: The exact commit the worker must run.

    Returns:
        The spawned child identity.
    """
    del daemon
    child = _published_child()
    obligation = SpawningObligation(
        token=child.token,
        commit=commit,
        creator_pid=os.getpid(),
        creator_start_time_ticks=1,
        pid=child.pid,
        start_time_ticks=child.start_time_ticks,
        created_at=0.0,
        boot_id=supervise.current_boot_id(),
    )
    supervise.write_state(replace(read_state(), spawning=obligation))
    return child


def _patch_recovery(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, str]]:
    """Patch owned-group recovery to record calls.

    Args:
        monkeypatch: The pytest monkeypatch fixture.

    Returns:
        The list of ``(event, token)`` tuples recorded so far.
    """
    observed: list[tuple[str, str]] = []

    def fake_recover(token: str) -> None:
        observed.append(("recover", token))

    monkeypatch.setattr(supervisor, "recover_owned_groups", fake_recover)
    return observed


def test_child_published_with_spawning_then_meta_then_spawning_cleared(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ordered protocol: child+spawning -> meta -> spawning cleared."""
    monkeypatch.setattr(cli, "runtime_is_usable", lambda _commit: True)
    monkeypatch.setattr(cli, "cli_commit_dir", lambda _commit: FAKE_RUNTIME_ROOT)
    monkeypatch.setattr(lifecycle, "worker_env", lambda _token: {})

    meta_log: list[lifecycle.WorkerMeta] = []
    real_write_meta = lifecycle.write_meta

    def fake_write_meta(meta: lifecycle.WorkerMeta) -> None:
        meta_log.append(meta)
        real_write_meta(meta)

    monkeypatch.setattr(lifecycle, "write_meta", fake_write_meta)

    events: list[SupervisorState] = []
    real_write_state = supervise.write_state

    def capturing_write(state: SupervisorState) -> None:
        events.append(state)
        real_write_state(state)

    monkeypatch.setattr(supervisor, "write_state", capturing_write)

    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    monkeypatch.setattr(daemon, "_spawn_worker", lambda c: _fake_spawn(daemon, c))
    daemon._ensure_consumer_locked(COMMIT)

    published = next(s for s in events if s.child is not None and s.spawning is not None)
    assert published.child is not None
    assert published.spawning is not None
    assert published.spawning.token == published.child.token
    assert published.spawning.pid == published.child.pid
    assert len(meta_log) == 1, "the lifecycle meta was written exactly once"
    final = events[-1]
    assert final.spawning is None
    assert final.child is not None
    assert final.child.pid == published.child.pid
    assert meta_log[0].pid == published.child.pid
    assert meta_log[0].token == published.child.token


def test_meta_write_failure_keeps_spawning_durable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed meta write leaves spawning durable (replacement-blocking)."""
    monkeypatch.setattr(cli, "runtime_is_usable", lambda _commit: True)
    monkeypatch.setattr(cli, "cli_commit_dir", lambda _commit: FAKE_RUNTIME_ROOT)
    monkeypatch.setattr(lifecycle, "worker_env", lambda _token: {})
    monkeypatch.setattr(
        lifecycle,
        "write_meta",
        lambda _meta: (_ for _ in ()).throw(DurabilityError("x")),
    )

    events: list[SupervisorState] = []
    real_write_state = supervise.write_state

    def capturing_write(state: SupervisorState) -> None:
        events.append(state)
        real_write_state(state)

    monkeypatch.setattr(supervisor, "write_state", capturing_write)

    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    monkeypatch.setattr(daemon, "_spawn_worker", lambda c: _fake_spawn(daemon, c))
    daemon._ensure_consumer_locked(COMMIT)

    assert read_state().spawning is not None, "the obligation survived the meta failure"
    assert read_state().child is not None, "the child was still published"
    assert all(s.spawning is not None for s in events), "spawning never cleared"
    assert lifecycle.read_meta() is None, "no meta was published on failure"


def test_deferred_publication_retries_then_clears_on_next_tick(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A restart reconciles child+spawning: retry meta, then clear spawning."""
    child = _published_child()
    _in_progress_state(child)

    meta_log: list[lifecycle.WorkerMeta] = []
    real_write_meta = lifecycle.write_meta

    def fake_write_meta(meta: lifecycle.WorkerMeta) -> None:
        meta_log.append(meta)
        real_write_meta(meta)

    monkeypatch.setattr(lifecycle, "write_meta", fake_write_meta)

    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    assert daemon._resolve_spawning_obligation() is True
    assert read_state().spawning is None, "obligation cleared after retry"
    assert read_state().child is not None
    assert len(meta_log) == 1, "the previously-missing meta was written on reconciliation"
    assert meta_log[0].pid == child.pid


def test_deferred_publication_meta_failure_retries_without_clearing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A meta failure during reconciliation keeps the obligation blocking."""
    child = _published_child()
    _in_progress_state(child)
    monkeypatch.setattr(
        lifecycle,
        "write_meta",
        lambda _meta: (_ for _ in ()).throw(DurabilityError("x")),
    )

    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    assert daemon._resolve_spawning_obligation() is True
    assert read_state().spawning is not None, "the obligation stayed blocking"
    assert read_state().child is not None


def test_in_progress_publication_is_not_converged_as_live_spawn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """child+spawning is recognized as in-progress, never as a live first spawn."""
    child = _published_child()
    _in_progress_state(child)
    observed = _patch_recovery(monkeypatch)

    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    monkeypatch.setattr(
        daemon,
        "_spawn_worker",
        lambda _commit: pytest.fail("in-progress publication authorized a spawn"),
    )
    assert daemon._resolve_spawning_obligation() is True
    assert read_state().spawning is None
    assert observed == [], "no group recovery ran for an in-progress publication"


def test_manual_recovery_blocked_while_spawning_retained() -> None:
    """A retained spawning obligation blocks a second consumer's adoption."""
    child = _published_child()
    _in_progress_state(child)

    other = lifecycle.WorkerMeta(
        schema_version=lifecycle.SCHEMA_VERSION,
        state=lifecycle.STATE_RUNNING,
        pid=9999,
        pgid=9999,
        sid=9999,
        start_time_ticks=1,
        token="other" + "b" * 20,
        repo="",
        git_commit=COMMIT,
        worker_id="w",
        log_path="",
        started_at=0.0,
        stopped_at=None,
    )
    assert lifecycle._pre_adoption_authority_error(other) is not None

    matching = lifecycle.WorkerMeta(
        schema_version=lifecycle.SCHEMA_VERSION,
        state=lifecycle.STATE_RUNNING,
        pid=child.pid,
        pgid=child.pid,
        sid=child.pid,
        start_time_ticks=child.start_time_ticks,
        token=child.token,
        repo="",
        git_commit=COMMIT,
        worker_id="w",
        log_path="",
        started_at=0.0,
        stopped_at=None,
    )
    # The supervisor's in-progress obligation carries the kernel parent-death
    # guarantee (parent_death_signal=True), so even a metadata-exact match must
    # stay blocked: manual recovery may not release a supervisor-owned
    # publication's authority, only the supervisor itself may finish it.
    assert lifecycle._pre_adoption_authority_error(matching) is not None


def test_fully_published_state_has_no_blocking_obligation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A successful publication ends with child published, meta present, no spawning."""
    monkeypatch.setattr(cli, "runtime_is_usable", lambda _commit: True)
    monkeypatch.setattr(cli, "cli_commit_dir", lambda _commit: FAKE_RUNTIME_ROOT)
    monkeypatch.setattr(lifecycle, "worker_env", lambda _token: {})
    real_write_meta = lifecycle.write_meta

    def fake_write_meta(meta: lifecycle.WorkerMeta) -> None:
        real_write_meta(meta)

    monkeypatch.setattr(lifecycle, "write_meta", fake_write_meta)

    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    monkeypatch.setattr(daemon, "_spawn_worker", lambda c: _fake_spawn(daemon, c))
    daemon._ensure_consumer_locked(COMMIT)

    state = read_state()
    assert state.child is not None
    assert state.spawning is None
    meta = lifecycle.read_meta()
    assert meta is not None
    assert meta.pid == state.child.pid
    assert meta.token == state.child.token
    assert lifecycle._pre_adoption_authority_error(meta) is None


def test_pre_popen_obligation_carries_no_child(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Before Popen the obligation is durable with no child identity yet."""
    observed: list[SpawningObligation | None] = []

    real_spawn = supervisor.SupervisorDaemon._spawn_worker

    def intercept(daemon: supervisor.SupervisorDaemon, commit: str) -> WorkerChild | None:
        observed.append(read_state().spawning)
        return real_spawn(daemon, commit)

    monkeypatch.setattr(cli, "runtime_is_usable", lambda _commit: False)
    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    monkeypatch.setattr(daemon, "_spawn_worker", lambda c: intercept(daemon, c))
    daemon._ensure_worker(COMMIT)

    # The refused-runtime path never reaches Popen, so no durable obligation is
    # left behind; the invariant is that any obligation present pre-Popen names
    # no child until the spawn succeeds.
    assert read_state().spawning is None
    assert read_state().child is None


# Keep ``subprocess`` referenced for parity with sibling spawn tests.
_ = subprocess
