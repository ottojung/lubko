"""Deterministic tests for the lifecycle authority state machine.

These tests exercise the authority guards and the no-op failpoint seam without
sleeps or a slow integration tier: they monkeypatch the side-effecting boundary
functions and assert that (a) the guards preserve the required invariants and
(b) arming a named failpoint at a real durable/side-effect boundary crashes
exactly there and prevents the subsequent side effect, while leaving the durable
authority in a fail-closed, single-consumer-safe state.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from lubko import deployctl, lifecycle, lifecycle_state, supervise, supervisor
from lubko.lifecycle_state import (
    FailpointError,
    LifecyclePhase,
    current_phase,
    mutation_blocked,
    mutation_blocker_reason,
    refuses_version_change,
)


@dataclass
class _Scenario:
    """Synthetic durable sources for :func:`current_phase` derivation."""

    meta: object | None = None
    meta_alive: bool = False
    mission_status: str | None = None
    mission_error: bool = False
    sup_child_alive: bool = False
    spawning: bool = False
    unresolved: bool = False
    blocking_hold: bool = False


# ---------------------------------------------------------------------------
# Failpoint seam
# ---------------------------------------------------------------------------


def test_failpoint_default_is_noop() -> None:
    """Every named boundary is inert unless a deterministic test arms it."""
    for name in (
        lifecycle_state.FAILPOINT_POPEN,
        lifecycle_state.FAILPOINT_METADATA_PUBLICATION,
        lifecycle_state.FAILPOINT_PROCESS_RETIREMENT,
        lifecycle_state.FAILPOINT_DB_RECOVERY,
        lifecycle_state.FAILPOINT_MISSION_PUBLISH,
        lifecycle_state.FAILPOINT_MISSION_CONFIRM,
        lifecycle_state.FAILPOINT_MISSION_ROLLBACK,
        lifecycle_state.FAILPOINT_SUPERVISOR_SPAWNING_WRITE,
        lifecycle_state.FAILPOINT_SUPERVISOR_PID_UPGRADE,
        lifecycle_state.FAILPOINT_SUPERVISOR_SPAWNING_CLEARANCE,
        lifecycle_state.FAILPOINT_SUPERVISOR_UNRESOLVED_CHILD,
    ):
        lifecycle_state.failpoint(name)  # must not raise


def test_failpoint_arms_and_disarms() -> None:
    """An armed failpoint fires once and then reverts to no-op on disarm."""
    lifecycle_state.arm_failpoint(lifecycle_state.FAILPOINT_POPEN)
    with pytest.raises(FailpointError):
        lifecycle_state.failpoint(lifecycle_state.FAILPOINT_POPEN)
    lifecycle_state.disarm_failpoints()
    lifecycle_state.failpoint(lifecycle_state.FAILPOINT_POPEN)  # no-op again


def test_failpoint_custom_exception() -> None:
    """A caller-supplied exception is raised verbatim at the armed boundary."""
    err = RuntimeError("boom")
    with (
        pytest.raises(RuntimeError),
        lifecycle_state.armed_failpoint(
            lifecycle_state.FAILPOINT_PROCESS_RETIREMENT,
            exc=err,
        ),
    ):
        lifecycle_state.failpoint(lifecycle_state.FAILPOINT_PROCESS_RETIREMENT)


# ---------------------------------------------------------------------------
# Guard: version-change refusal
# ---------------------------------------------------------------------------


def test_refuses_version_change() -> None:
    """An ordinary deploy may not change the commit of a recorded worker."""
    assert refuses_version_change(None, "abc", git_commit=None) is False
    assert refuses_version_change(object(), "abc", git_commit="abc") is False
    assert refuses_version_change(object(), "abc", git_commit="def") is True


# ---------------------------------------------------------------------------
# Guard: supervised-mutation blocker
# ---------------------------------------------------------------------------


def test_mutation_blocker_none_when_no_mission(monkeypatch: pytest.MonkeyPatch) -> None:
    """Without a supervised mission, ordinary mutation is permitted."""
    monkeypatch.setattr(deployctl, "read_rollback_state", lambda: None)
    assert mutation_blocker_reason() is None
    assert mutation_blocked() is False


def test_mutation_blocker_pending(monkeypatch: pytest.MonkeyPatch) -> None:
    """A pending mission blocks mutation until it is confirmed or rolled back."""
    mission = SimpleNamespace(status=deployctl.STATUS_PENDING, commit="c" * 40, generation=3)
    monkeypatch.setattr(deployctl, "read_rollback_state", lambda: mission)
    reason = mutation_blocker_reason()
    assert reason is not None
    assert "pending confirmation" in reason
    assert mutation_blocked() is True


def test_mutation_blocker_unknown_status(monkeypatch: pytest.MonkeyPatch) -> None:
    """An unrecognized mission status blocks mutation as corrupt-ish authority."""
    mission = SimpleNamespace(status="weird", commit="c" * 40, generation=3)
    monkeypatch.setattr(deployctl, "read_rollback_state", lambda: mission)
    reason = mutation_blocker_reason()
    assert reason is not None
    assert "unknown status" in reason


def test_mutation_blocker_corrupt_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    """An unreadable mission state fails closed and blocks mutation."""
    monkeypatch.setattr(
        deployctl,
        "read_rollback_state",
        lambda: (_ for _ in ()).throw(deployctl.DeployCtlError("bad")),
    )
    reason = mutation_blocker_reason()
    assert reason is not None
    assert "corrupt" in reason


# ---------------------------------------------------------------------------
# Derived phase from real durable sources
# ---------------------------------------------------------------------------


def _patch_sources(monkeypatch: pytest.MonkeyPatch, scenario: _Scenario) -> None:
    """Install synthetic durable sources for :func:`current_phase`."""
    if scenario.meta is None:
        monkeypatch.setattr(lifecycle, "read_meta", lambda: None)
    else:
        monkeypatch.setattr(lifecycle, "read_meta", lambda: scenario.meta)
    monkeypatch.setattr(lifecycle, "worker_alive", lambda _m: scenario.meta_alive)
    if scenario.mission_error:
        monkeypatch.setattr(
            deployctl,
            "read_rollback_state",
            lambda: (_ for _ in ()).throw(deployctl.DeployCtlError("bad")),
        )
    elif scenario.mission_status is None:
        monkeypatch.setattr(deployctl, "read_rollback_state", lambda: None)
    else:
        monkeypatch.setattr(
            deployctl,
            "read_rollback_state",
            lambda: SimpleNamespace(
                status=scenario.mission_status,
                commit="c" * 40,
                generation=3,
            ),
        )
    state = SimpleNamespace(
        child=SimpleNamespace() if scenario.sup_child_alive else None,
        spawning=SimpleNamespace() if scenario.spawning else None,
        unresolved_child=SimpleNamespace() if scenario.unresolved else None,
        ownership_hold_malformed=scenario.blocking_hold,
        unresolved_hold_malformed=scenario.blocking_hold,
        spawning_hold_malformed=scenario.blocking_hold,
    )
    monkeypatch.setattr(supervise, "read_state", lambda: state)
    monkeypatch.setattr(supervise, "child_alive", lambda _c: scenario.sup_child_alive)


def test_phase_unmanaged(monkeypatch: pytest.MonkeyPatch) -> None:
    """No durable consumer implies the unmanaged phase."""
    _patch_sources(monkeypatch, _Scenario())
    assert current_phase() == LifecyclePhase.UNMANAGED


def test_phase_ownership_pending_on_corrupt_mission(monkeypatch: pytest.MonkeyPatch) -> None:
    """An unreadable mission forces the fail-closed ownership pending phase."""
    _patch_sources(monkeypatch, _Scenario(mission_error=True))
    assert current_phase() == LifecyclePhase.OWNERSHIP_PENDING


def test_phase_ownership_pending_on_blocking_hold(monkeypatch: pytest.MonkeyPatch) -> None:
    """A malformed authority hold forces the fail-closed ownership pending phase."""
    _patch_sources(monkeypatch, _Scenario(blocking_hold=True))
    assert current_phase() == LifecyclePhase.OWNERSHIP_PENDING


def test_phase_running_from_owned_meta(monkeypatch: pytest.MonkeyPatch) -> None:
    """A live owned worker metadata implies the running phase."""
    meta = SimpleNamespace(pid=1, git_commit="c" * 40)
    _patch_sources(monkeypatch, _Scenario(meta=meta, meta_alive=True))
    assert current_phase() == LifecyclePhase.RUNNING


def test_phase_running_from_supervisor_child(monkeypatch: pytest.MonkeyPatch) -> None:
    """A live supervisor child implies the running phase."""
    _patch_sources(monkeypatch, _Scenario(sup_child_alive=True))
    assert current_phase() == LifecyclePhase.RUNNING


def test_phase_mission_pending(monkeypatch: pytest.MonkeyPatch) -> None:
    """A pending mission implies the mission pending phase."""
    _patch_sources(monkeypatch, _Scenario(mission_status=deployctl.STATUS_PENDING))
    assert current_phase() == LifecyclePhase.MISSION_PENDING


def test_phase_confirmed(monkeypatch: pytest.MonkeyPatch) -> None:
    """A confirmed mission implies the confirmed phase."""
    _patch_sources(monkeypatch, _Scenario(mission_status=deployctl.STATUS_CONFIRMED))
    assert current_phase() == LifecyclePhase.CONFIRMED


def test_phase_rolled_back(monkeypatch: pytest.MonkeyPatch) -> None:
    """A rolled-back mission implies the rolled back phase."""
    _patch_sources(monkeypatch, _Scenario(mission_status=deployctl.STATUS_ROLLED_BACK))
    assert current_phase() == LifecyclePhase.ROLLED_BACK


def test_phase_spawn_obligation(monkeypatch: pytest.MonkeyPatch) -> None:
    """An open spawning obligation implies the spawn obligation phase."""
    _patch_sources(monkeypatch, _Scenario(spawning=True))
    assert current_phase() == LifecyclePhase.SPAWN_OBLIGATION


def test_phase_spawning_unresolved(monkeypatch: pytest.MonkeyPatch) -> None:
    """An unresolved spawned child implies the spawning phase."""
    _patch_sources(monkeypatch, _Scenario(unresolved=True))
    assert current_phase() == LifecyclePhase.SPAWNING


def test_phase_is_single_valued(monkeypatch: pytest.MonkeyPatch) -> None:
    """Two proven consumers collapse to one phase, not two live consumers."""
    meta = SimpleNamespace(pid=1, git_commit="c" * 40)
    _patch_sources(monkeypatch, _Scenario(meta=meta, meta_alive=True, sup_child_alive=True))
    assert current_phase() == LifecyclePhase.RUNNING


# ---------------------------------------------------------------------------
# Failpoint injection at real boundaries
# ---------------------------------------------------------------------------


def test_popen_failpoint_blocks_subprocess(monkeypatch: pytest.MonkeyPatch) -> None:
    """A popen failpoint prevents subprocess creation at the spawn boundary."""
    popen = MagicMock()
    monkeypatch.setattr("subprocess.Popen", popen)
    with (
        pytest.raises(FailpointError),
        lifecycle_state.armed_failpoint(
            lifecycle_state.FAILPOINT_POPEN,
        ),
    ):
        lifecycle.spawn_worker(Path("/repo"), "/uv", Path("/log"), {})
    popen.assert_not_called()


def test_process_retirement_failpoint_blocks_signalling(monkeypatch: pytest.MonkeyPatch) -> None:
    """A process retirement failpoint prevents pinning before any signal."""
    pin = MagicMock()
    monkeypatch.setattr(lifecycle, "_open_exact_pidfd", lambda _p: pin)
    meta = lifecycle.WorkerMeta(
        schema_version=lifecycle.SCHEMA_VERSION,
        state=lifecycle.STATE_RUNNING,
        pid=12345,
        pgid=12345,
        sid=12345,
        start_time_ticks=1,
        token=os.urandom(8).hex(),
        repo="/r",
        git_commit="c" * 40,
        worker_id="w",
        log_path="/l",
        started_at=None,
        stopped_at=None,
    )
    with (
        pytest.raises(FailpointError),
        lifecycle_state.armed_failpoint(
            lifecycle_state.FAILPOINT_PROCESS_RETIREMENT,
        ),
    ):
        lifecycle.stop_worker(meta, 1.0)
    pin.assert_not_called()


def test_metadata_publication_failpoint_blocks_write(monkeypatch: pytest.MonkeyPatch) -> None:
    """A metadata publication failpoint prevents the durable meta write."""
    writer = MagicMock()
    monkeypatch.setattr(lifecycle, "write_json_durable", writer)
    meta = lifecycle.WorkerMeta(
        schema_version=lifecycle.SCHEMA_VERSION,
        state=lifecycle.STATE_RUNNING,
        pid=1,
        pgid=1,
        sid=1,
        start_time_ticks=1,
        token=os.urandom(8).hex(),
        repo="/r",
        git_commit="c" * 40,
        worker_id="w",
        log_path="/l",
        started_at=None,
        stopped_at=None,
    )
    with (
        pytest.raises(FailpointError),
        lifecycle_state.armed_failpoint(
            lifecycle_state.FAILPOINT_METADATA_PUBLICATION,
        ),
    ):
        lifecycle.write_meta(meta)
    writer.assert_not_called()


def _pending_mission() -> deployctl.RollbackState:
    """Build a serializable pending mission for boundary-injection tests.

    Returns:
        A pending :class:`RollbackState` whose metadata fields are JSON
        serializable so the durable write boundary can run without error.
    """
    previous_meta = lifecycle.WorkerMeta(
        schema_version=lifecycle.SCHEMA_VERSION,
        state=lifecycle.STATE_RUNNING,
        pid=1,
        pgid=1,
        sid=1,
        start_time_ticks=1,
        token=os.urandom(8).hex(),
        repo="/r",
        git_commit="d" * 40,
        worker_id="w",
        log_path="/l",
        started_at=None,
        stopped_at=None,
    )
    new_meta = lifecycle.WorkerMeta(
        schema_version=lifecycle.SCHEMA_VERSION,
        state=lifecycle.STATE_RUNNING,
        pid=2,
        pgid=2,
        sid=2,
        start_time_ticks=1,
        token=os.urandom(8).hex(),
        repo="/r",
        git_commit="c" * 40,
        worker_id="w",
        log_path="/l",
        started_at=None,
        stopped_at=None,
    )
    return deployctl.RollbackState(
        schema_version=deployctl.ROLLBACK_SCHEMA_VERSION,
        generation=3,
        status=deployctl.STATUS_PENDING,
        commit="c" * 40,
        previous_commit="d" * 40,
        challenge_hash=None,
        deadline=1e9,
        repo="/r",
        uv_path="/uv",
        stop_grace_seconds=1.0,
        git_timeout_seconds=1.0,
        previous_retiring=False,
        previous_meta=previous_meta,
        new_meta=new_meta,
    )


def test_mission_publish_failpoint_blocks_watchdog(monkeypatch: pytest.MonkeyPatch) -> None:
    """A mission publish failpoint prevents arming the watchdog after write."""
    watchdog = MagicMock()
    monkeypatch.setattr(deployctl, "_fork_watchdog", watchdog)
    monkeypatch.setattr(deployctl, "_write_state", MagicMock())
    with (
        pytest.raises(FailpointError),
        lifecycle_state.armed_failpoint(
            lifecycle_state.FAILPOINT_MISSION_PUBLISH,
        ),
    ):
        deployctl.publish_mission(_pending_mission(), 1.0)
    watchdog.assert_not_called()


def test_mission_confirm_failpoint_blocks_readiness(monkeypatch: pytest.MonkeyPatch) -> None:
    """A mission confirm failpoint prevents awaiting supervisor readiness."""
    monkeypatch.setattr(supervise, "request_run", lambda _commit, **_kw: 7)
    waited = MagicMock()
    monkeypatch.setattr(supervise, "wait_for_generation", waited)
    with (
        pytest.raises(FailpointError),
        lifecycle_state.armed_failpoint(
            lifecycle_state.FAILPOINT_MISSION_CONFIRM,
        ),
    ):
        deployctl.settle_desired("c" * 40, "/r", "/uv")
    waited.assert_not_called()


def test_mission_rollback_failpoint_blocks_settlement(monkeypatch: pytest.MonkeyPatch) -> None:
    """A mission rollback failpoint prevents settling the previous commit."""
    monkeypatch.setattr(supervise, "supervisor_running", lambda: True)
    settled = MagicMock()
    monkeypatch.setattr(deployctl, "settle_desired", settled)
    with (
        pytest.raises(FailpointError),
        lifecycle_state.armed_failpoint(
            lifecycle_state.FAILPOINT_MISSION_ROLLBACK,
        ),
    ):
        deployctl._rollback_locked(_pending_mission())
    settled.assert_not_called()


def test_db_recovery_failpoint_blocks_before_db(monkeypatch: pytest.MonkeyPatch) -> None:
    """A db recovery failpoint prevents touching the database at the boundary."""
    monkeypatch.setattr(
        supervisor,
        "load_database_config",
        lambda: (_ for _ in ()).throw(AssertionError("db must not be touched")),
    )
    with (
        pytest.raises(FailpointError),
        lifecycle_state.armed_failpoint(
            lifecycle_state.FAILPOINT_DB_RECOVERY,
        ),
    ):
        supervisor.recover_owned_groups("incarnation-token")
