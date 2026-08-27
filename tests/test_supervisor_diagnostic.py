"""Durable, non-live supervisor diagnostics never masquerade as live health."""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING

import pytest

from lubko import supervise

if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture
def isolated_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Provide an isolated supervisor state directory.

    Returns:
        The temporary state root path.
    """
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
    supervise.supervisor_dir().mkdir(parents=True, exist_ok=True)
    return tmp_path


def test_fresh_state_is_reported_holding_not_running(isolated_state: Path) -> None:
    """No supervisor process: diagnostic is non-live and marked holding."""
    assert isolated_state.is_dir()
    diag = supervise.derive_durable_diagnostic()
    assert diag.live is False
    assert diag.source == "durable-state"
    assert diag.supervisor_present is False
    assert diag.supervisor_alive is None
    assert diag.holding is True
    assert diag.child_present is False


def test_ownership_hold_malformed_is_reported_holding(isolated_state: Path) -> None:
    """A durable replacement-blocking hold is surfaced as holding."""
    assert isolated_state.is_dir()
    state = replace(supervise.fresh_state(), ownership_hold_malformed=True)
    supervise.write_state(state)
    diag = supervise.derive_durable_diagnostic()
    assert diag.live is False
    assert diag.holding is True
    assert diag.ownership_hold_malformed is True


def test_running_child_is_not_holding(isolated_state: Path) -> None:
    """A confirmed running child with a run intent is not holding."""
    assert isolated_state.is_dir()
    marker = "t"
    child = supervise.WorkerChild(
        pid=1,
        pgid=1,
        sid=1,
        start_time_ticks=10,
        token=marker,
        worker_id="w",
        spawned_at=1.0,
    )
    state = replace(
        supervise.fresh_state(),
        mode=supervise.MODE_RUN,
        child=child,
        intent=supervise.INTENT_RUN,
        ready=True,
    )
    supervise.write_state(state)
    diag = supervise.derive_durable_diagnostic()
    assert diag.holding is False
    assert diag.child_present is True
    assert diag.ready is True


def test_diagnostic_round_trips(isolated_state: Path) -> None:
    """The durable diagnostic serializes and parses back identically."""
    assert isolated_state.is_dir()
    state = replace(
        supervise.fresh_state(),
        mode=supervise.MODE_RUN,
        intent=supervise.INTENT_RUN,
        restart_count=3,
        next_attempt_at=42.0,
        ownership_hold_malformed=True,
    )
    supervise.write_state(state)
    diag = supervise.derive_durable_diagnostic()
    restored = supervise.SupervisorDiagnostic.from_dict(diag.to_dict())
    assert restored == diag
    assert restored.restart_count == 3
    assert restored.next_attempt_at == pytest.approx(42.0)
    assert restored.ownership_hold_malformed is True


def test_live_status_carries_holding_flag(isolated_state: Path) -> None:
    """A live SupervisorStatus exposes the derived holding state."""
    assert isolated_state.is_dir()
    status = supervise.SupervisorStatus(
        schema_version=supervise.SCHEMA_VERSION,
        supervisor_pid=4242,
        supervisor_start_time_ticks=111,
        started_at=1.0,
        applied_generation=1,
        mode=supervise.MODE_IDLE,
        commit=None,
        child=None,
        intent=supervise.INTENT_RUN,
        restart_count=0,
        next_attempt_at=None,
        last_exit=None,
        mission=None,
        db_ready=None,
        ready=None,
        message=None,
        worker_health=None,
        holding=True,
    )
    assert status.holding is True
    restored = supervise.SupervisorStatus.from_dict(status.to_dict())
    assert restored.holding is True
