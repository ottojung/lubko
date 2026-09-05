"""Prepared deployment handoffs preserve their durable ownership authority."""

from __future__ import annotations

import math
import time
from dataclasses import replace
from pathlib import Path
from unittest.mock import MagicMock

import pytest

import lubko.deployctl as dc
from lubko import lifecycle, supervise
from lubko import supervisor as supervisor_mod

COMMIT = "2" * 40
PREVIOUS_COMMIT = "1" * 40


def _meta(commit: str, pid: int) -> lifecycle.WorkerMeta:
    """Return a minimal valid worker authority record."""
    return lifecycle.WorkerMeta(
        schema_version=lifecycle.SCHEMA_VERSION,
        state=lifecycle.STATE_RUNNING,
        pid=pid,
        pgid=pid,
        sid=pid,
        start_time_ticks=pid,
        token=f"token-{pid}",
        repo="/workspace/Lubko",
        git_commit=commit,
        worker_id="w",
        log_path="",
        started_at=1.0,
        stopped_at=None,
    )


def _state(*, supervisor_owned: bool | None) -> dc.RollbackState:
    """Return a valid pending handoff mission."""
    return dc.RollbackState(
        schema_version=dc.ROLLBACK_SCHEMA_VERSION,
        generation=1,
        status=dc.STATUS_PENDING,
        commit=COMMIT,
        previous_commit=PREVIOUS_COMMIT,
        deadline=10.0,
        repo="/workspace/Lubko",
        uv_path="uv",
        stop_grace_seconds=1.0,
        git_timeout_seconds=1.0,
        previous_retiring=False,
        previous_meta=_meta(PREVIOUS_COMMIT, 100),
        new_meta=_meta(COMMIT, 200),
        supervisor_owned=supervisor_owned,
    )


def _options() -> dc.Options:
    """Return deterministic handoff options."""
    return dc.Options(
        repo=Path("/workspace/Lubko"),
        uv_path="uv",
        confirm_window_seconds=3.0,
        stop_grace_seconds=1.0,
        postgres_timeout_seconds=1.0,
        lock_timeout_seconds=2.0,
        validation_timeout_seconds=1.0,
        git_timeout_seconds=1.0,
        cli_timeout_seconds=1.0,
    )


def _gated(state: dc.RollbackState) -> dc.GatedWorker:
    """Return a harmless gated-candidate test record."""
    assert state.new_meta is not None
    return dc.GatedWorker(proc=MagicMock(), gate_writer=9, meta=state.new_meta)


def test_supervisor_owned_handoff_remains_supervised_when_daemon_is_temporarily_absent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Prepared supervisor authority publishes durably without reclassifying."""
    state = _state(supervisor_owned=True)
    published: list[tuple[dc.RollbackState, float]] = []
    waited: list[tuple[dc.RollbackState, float]] = []
    live = replace(state, deadline=20.0)
    monkeypatch.setattr(
        supervise,
        "supervisor_running",
        lambda: pytest.fail("supervised handoff must not reclassify from liveness"),
    )
    monkeypatch.setattr(dc, "publish_mission", lambda s, t: published.append((s, t)))

    def wait(s: dc.RollbackState, window: float) -> dc.RollbackState:
        waited.append((s, window))
        return live

    monkeypatch.setattr(dc, "_wait_for_supervisor_mission", wait)

    assert dc._complete_handoff(_options(), state, None) == live
    assert published == [(state, 2.0)]
    assert waited == [(state, 3.0)]


def test_legacy_handoff_aborts_gated_candidate_if_supervisor_appears(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A live supervisor cannot silently adopt legacy preparation artifacts."""
    state = _state(supervisor_owned=False)
    gated = _gated(state)
    aborted: list[dc.GatedWorker] = []
    rolled_back: list[dc.RollbackState] = []
    monkeypatch.setattr(supervise, "supervisor_running", lambda: True)
    monkeypatch.setattr(dc, "_abort_gated_candidate", aborted.append)

    def rollback(s: dc.RollbackState) -> bool:
        rolled_back.append(s)
        return True

    monkeypatch.setattr(dc, "_rollback_locked", rollback)
    monkeypatch.setattr(
        dc,
        "stop_worker",
        lambda *_args: pytest.fail("legacy destructive handoff must not begin"),
    )

    with pytest.raises(dc.DeployCtlError, match="became authoritative"):
        dc._complete_handoff(_options(), state, gated)

    assert aborted == [gated]
    assert rolled_back == [state]


def test_legacy_handoff_stays_pending_if_supervisor_takeover_cannot_roll_back(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Failed takeover convergence never crosses the legacy destructive boundary."""
    state = _state(supervisor_owned=False)
    gated = _gated(state)
    monkeypatch.setattr(supervise, "supervisor_running", lambda: True)
    monkeypatch.setattr(dc, "_abort_gated_candidate", lambda _gated: None)
    monkeypatch.setattr(dc, "_rollback_locked", lambda _state: False)

    with pytest.raises(dc.DeployCtlError, match="rollback remains pending"):
        dc._complete_handoff(_options(), state, gated)


def test_unknown_handoff_ownership_fails_closed_and_converges_gated_candidate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unknown ownership is never inferred from current daemon liveness."""
    state = _state(supervisor_owned=None)
    gated = _gated(state)
    aborted: list[dc.GatedWorker] = []
    monkeypatch.setattr(dc, "_abort_gated_candidate", aborted.append)
    monkeypatch.setattr(
        supervise,
        "supervisor_running",
        lambda: pytest.fail("unknown ownership must not be inferred from liveness"),
    )

    with pytest.raises(dc.DeployCtlError, match="unknown supervisor ownership"):
        dc._complete_handoff(_options(), state, gated)

    assert aborted == [gated]


def test_stable_legacy_handoff_keeps_its_prepared_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Explicit legacy authority retains the ordinary gated handoff when safe."""
    state = _state(supervisor_owned=False)
    gated = _gated(state)
    writes: list[dc.RollbackState] = []
    released: list[int] = []
    monkeypatch.setattr(supervise, "supervisor_running", lambda: False)
    monkeypatch.setattr(dc, "_write_state", writes.append)
    monkeypatch.setattr(dc, "stop_worker", lambda *_args: True)
    monkeypatch.setattr(dc, "_release_gate", released.append)
    monkeypatch.setattr(dc, "_wait_for_released_worker", lambda _meta: True)
    monkeypatch.setattr(time, "time", lambda: 50.0)

    live = dc._complete_handoff(_options(), state, gated)

    assert live.previous_retiring is True
    assert math.isclose(live.deadline, 53.0)
    assert released == [9]
    assert writes == [replace(state, previous_retiring=True), live]


def test_supervisor_never_adopts_explicit_legacy_pending_mission() -> None:
    """A durable legacy mission cannot become supervisor authority at startup."""
    daemon = supervisor_mod.SupervisorDaemon(supervisor_mod.Settings())
    mission = _state(supervisor_owned=False)

    assert daemon._derive_with_mission(mission, 0, None) == ("hold", None)
    assert daemon._message == (
        "pending deployment mission is not supervisor-owned; holding without a worker"
    )


def test_supervisor_fails_closed_on_unknown_pending_mission_ownership() -> None:
    """Unknown durable ownership cannot be inferred as supervisor authority."""
    daemon = supervisor_mod.SupervisorDaemon(supervisor_mod.Settings())
    mission = _state(supervisor_owned=None)

    assert daemon._derive_with_mission(mission, 0, None) == ("hold", None)


def test_newer_desired_intent_can_supersede_legacy_pending_mission() -> None:
    """A strictly newer explicit desired generation still owns reconciliation."""
    daemon = supervisor_mod.SupervisorDaemon(supervisor_mod.Settings())
    mission = _state(supervisor_owned=False)

    assert daemon._derive_with_mission(mission, 2, PREVIOUS_COMMIT) == (
        "run",
        PREVIOUS_COMMIT,
    )
