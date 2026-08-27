"""Shutdown finalization is unconditional across remote DB terminalization failure.

The supervisor's shutdown detaches its *local* ownership (the worker child,
the pre-spawn obligation, the unresolved-child hold) and performs *local*
cleanup (pidfile removal, ``stopped`` status). These steps must complete even
when the *remote* owned-group terminalization -- the only step that touches the
database -- fails with a hard client deadline breach
(:class:`lubko.worker.DbOperationDeadlineError`) or a connectivity loss. Remote
terminalization is best-effort and fail-closed: a failure is logged, but it
never blocks local convergence or cleanup.

The exact drain-sentinel semantics are preserved: when the worker proved a
clean drain, no remote terminalization is attempted at all.
"""

from __future__ import annotations

import json
from dataclasses import replace
from typing import TYPE_CHECKING
from uuid import uuid4

import psycopg
import pytest

from lubko import lifecycle, supervise, supervisor
from lubko import worker as worker_mod
from lubko.config import DatabaseConfig
from lubko.supervise import (
    INTENT_RUN,
    SpawningObligation,
    UnresolvedChild,
    WorkerChild,
    fresh_state,
    read_state,
    write_state,
)

if TYPE_CHECKING:
    from pathlib import Path

COMMIT = "a" * 40
TOKEN = "c" * 32
CHILD_TOKEN = "b" * 32


@pytest.fixture(autouse=True)
def fast_lock_poll(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make consumer-lock contention polling effectively instantaneous."""
    monkeypatch.setattr(supervise, "CONSUMER_LOCK_POLL_SECONDS", 0.001)


class _FakeConn:
    """A database connection stand-in that tolerates attribute assignment."""


def _supervisor_dir() -> Path:
    """Create and return the isolated supervisor state directory.

    Returns:
        The supervisor state directory, created if absent.
    """
    path = supervise.state_path().parent
    path.mkdir(parents=True, exist_ok=True)
    return path


def _seed_full_ownership() -> None:
    """Seed durable state with a child, a pre-spawn obligation, and a hold."""
    child = WorkerChild(
        pid=4_100_000,
        pgid=4_100_000,
        sid=4_100_000,
        start_time_ticks=42,
        token=CHILD_TOKEN,
        worker_id="host",
        spawned_at=0.0,
    )
    obligation = SpawningObligation(
        token=TOKEN,
        commit=COMMIT,
        creator_pid=9_000_000,
        creator_start_time_ticks=0,
        pid=None,
        start_time_ticks=None,
        created_at=0.0,
        boot_id=supervise.current_boot_id(),
        parent_death_signal=True,
    )
    hold = UnresolvedChild(
        pid=4_200_000,
        start_time_ticks=7,
        token=TOKEN,
        spawned_at=0.0,
    )
    write_state(
        replace(
            fresh_state(),
            mode=supervise.MODE_RUN,
            commit=COMMIT,
            child=child,
            intent=INTENT_RUN,
            spawning=obligation,
            unresolved_child=hold,
        )
    )


def _patch_remote_failure(
    monkeypatch: pytest.MonkeyPatch,
    *,
    sentinel_matches: bool,
    failure: type[Exception] | None,
    calls: list[str],
) -> None:
    """Install the shutdown boundary seams for the failure scenarios.

    Args:
        monkeypatch: The pytest monkeypatch fixture.
        sentinel_matches: Whether the drain sentinel proves a clean worker drain.
        failure: Exception the remote terminalization raises, or ``None`` for a
            clean success.
        calls: List appended with ``"recover"`` each time terminalization runs.
    """
    monkeypatch.setattr(lifecycle, "stop_worker", lambda *_a, **_k: True)
    monkeypatch.setattr(lifecycle, "check_postgres", lambda _timeout: False)
    monkeypatch.setattr(worker_mod, "drain_sentinel_matches", lambda _token: sentinel_matches)
    monkeypatch.setattr(
        supervisor.SupervisorDaemon,
        "_converge_unresolved",
        lambda _self, _hold: True,
    )

    breach_message = "simulated remote terminalization failure"

    def failing_recover(_token: str) -> None:
        calls.append(_token)
        if failure is not None:
            raise failure(breach_message)

    monkeypatch.setattr(supervisor, "recover_owned_groups", failing_recover)


def test_shutdown_completes_local_convergence_and_cleanup_on_deadline_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A remote deadline/connectivity failure must not block local shutdown.

    Every local step -- clearing ``child``, ``spawning``, and ``unresolved_child``,
    removing the pidfile, and publishing the ``stopped`` status -- must complete
    even though the remote owned-group terminalization raised.
    """
    _supervisor_dir()
    _seed_full_ownership()
    calls: list[str] = []
    # OwnedGroupRecoveryError is exactly what recover_owned_groups raises after
    # wrapping a DbOperationDeadlineError or a connectivity loss.
    _patch_remote_failure(
        monkeypatch,
        sentinel_matches=False,
        failure=supervisor.OwnedGroupRecoveryError,
        calls=calls,
    )
    supervise.write_supervisor_pid(12345, 0)

    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    daemon._shutdown()

    state = read_state()
    assert state.child is None, "local child ownership was not cleared"
    assert state.spawning is None, "local pre-spawn obligation was not cleared"
    assert state.unresolved_child is None, "local unresolved hold was not cleared"
    assert calls == [CHILD_TOKEN, TOKEN, TOKEN], (
        "remote terminalization must still be attempted for each owned authority"
    )
    assert not supervise.supervisor_pid_path().exists(), "local pidfile was not removed"
    status = json.loads(supervise.status_path().read_text(encoding="utf-8"))
    assert status["message"] == "stopped", "local stopped status was not published"


def test_shutdown_completes_local_convergence_and_cleanup_on_connectivity_loss(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A raised DbOperationDeadlineError during shutdown still converges locally."""
    _supervisor_dir()
    _seed_full_ownership()
    calls: list[str] = []
    _patch_remote_failure(
        monkeypatch,
        sentinel_matches=False,
        failure=worker_mod.DbOperationDeadlineError,
        calls=calls,
    )
    # The production recover_owned_groups wraps DbOperationDeadlineError into
    # OwnedGroupRecoveryError; emulate that single contract boundary here.
    wrapped_message = "wrapped deadline"

    def wrapped_recover(_token: str) -> None:
        calls.append("recover")
        raise supervisor.OwnedGroupRecoveryError(wrapped_message) from None

    monkeypatch.setattr(supervisor, "recover_owned_groups", wrapped_recover)
    supervise.write_supervisor_pid(12345, 0)

    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    daemon._shutdown()

    state = read_state()
    assert state.child is None
    assert state.spawning is None
    assert state.unresolved_child is None
    assert not supervise.supervisor_pid_path().exists()
    status = json.loads(supervise.status_path().read_text(encoding="utf-8"))
    assert status["message"] == "stopped"


def test_shutdown_skips_remote_terminalization_when_drain_sentinel_matches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An exact drain sentinel means the worker already terminated its groups.

    When the sentinel matches, no remote terminalization is attempted, so the
    shutdown must not touch the database at all and must still converge locally.
    """
    _supervisor_dir()
    _seed_full_ownership()
    calls: list[str] = []
    _patch_remote_failure(
        monkeypatch,
        sentinel_matches=True,
        failure=None,
        calls=calls,
    )
    supervise.write_supervisor_pid(12345, 0)

    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    daemon._shutdown()

    assert CHILD_TOKEN not in calls, (
        "remote terminalization must not run for the child when the drain sentinel matches"
    )
    assert calls == [TOKEN, TOKEN], (
        "the separate pre-spawn and unresolved authorities are still terminalized"
    )
    state = read_state()
    assert state.child is None
    assert state.spawning is None
    assert state.unresolved_child is None
    assert not supervise.supervisor_pid_path().exists()


def test_recover_owned_groups_wraps_deadline_error_as_blocking(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A DbOperationDeadlineError from the query layer is fail-closed.

    The remote-terminalization wrapper must classify a hard client deadline
    breach as :class:`OwnedGroupRecoveryError` so every caller -- shutdown
    (best-effort) and the run loop (hold) alike -- shares one failure contract.
    """
    config = DatabaseConfig(host="h", port=1, dbname="d", user="u", password=str(uuid4()))
    monkeypatch.setattr(supervisor, "load_database_config", lambda: config)
    monkeypatch.setattr(psycopg, "connect", lambda *_args, **_kwargs: _FakeConn())
    breach_message = "hung established connection"

    def boom(_conn: object, _incarnation: str, _grace: float) -> object:
        raise worker_mod.DbOperationDeadlineError(breach_message)

    monkeypatch.setattr(worker_mod, "recover_owned_job_groups", boom)

    with pytest.raises(supervisor.OwnedGroupRecoveryError):
        supervisor.recover_owned_groups(TOKEN)


def test_recover_owned_groups_wraps_connectivity_loss_as_blocking(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A connectivity loss (OSError) during recovery is fail-closed."""
    config = DatabaseConfig(host="h", port=1, dbname="d", user="u", password=str(uuid4()))
    monkeypatch.setattr(supervisor, "load_database_config", lambda: config)
    monkeypatch.setattr(psycopg, "connect", lambda *_args, **_kwargs: _FakeConn())
    loss_message = "connection reset"

    def boom(_conn: object, _incarnation: str, _grace: float) -> object:
        raise OSError(loss_message)

    monkeypatch.setattr(worker_mod, "recover_owned_job_groups", boom)

    with pytest.raises(supervisor.OwnedGroupRecoveryError):
        supervisor.recover_owned_groups(TOKEN)
