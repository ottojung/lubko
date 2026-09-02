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


def _canonical_diagnostic_mapping() -> dict[str, object]:
    """Return one fully populated canonical diagnostic mapping."""
    return supervise.SupervisorDiagnostic(
        live=False,
        source="durable-state",
        supervisor_present=True,
        supervisor_alive=False,
        mode=supervise.MODE_IDLE,
        intent=supervise.INTENT_RUN,
        commit="a" * 40,
        applied_generation=7,
        restart_count=2,
        next_attempt_at=12.5,
        ready=False,
        next_readiness_at=13.5,
        holding=True,
        ownership_hold_malformed=False,
        unresolved_hold_malformed=False,
        spawning_hold_malformed=False,
        child_present=False,
        unresolved_child_present=False,
        spawning_present=False,
        last_exit=supervise.LastExit(returncode=1, at=3.5),
        message="held",
    ).to_dict()


def _assert_diagnostic_rejects(field: str, malformed: object) -> None:
    """Assert one malformed present scalar is rejected.

    Args:
        field: Diagnostic field to corrupt.
        malformed: Invalid JSON-domain value to place there.
    """
    data = _canonical_diagnostic_mapping()
    data[field] = malformed
    with pytest.raises((TypeError, ValueError), match="supervisor diagnostic is malformed"):
        supervise.SupervisorDiagnostic.from_dict(data)


def test_diagnostic_rejects_malformed_present_booleans() -> None:
    """Present diagnostic boolean fields are literal JSON booleans only."""
    cases: tuple[tuple[str, object], ...] = (
        ("live", "false"),
        ("supervisor_present", 1),
        ("supervisor_alive", "true"),
        ("ready", None),
        ("holding", []),
        ("ownership_hold_malformed", {}),
        ("unresolved_hold_malformed", 0),
        ("spawning_hold_malformed", "false"),
        ("child_present", 1.0),
        ("unresolved_child_present", "true"),
        ("spawning_present", None),
    )
    for field, malformed in cases:
        _assert_diagnostic_rejects(field, malformed)


def test_diagnostic_rejects_noncanonical_numbers() -> None:
    """Diagnostic counters and timestamps never use permissive coercion."""
    counter_cases: tuple[tuple[str, object], ...] = (
        ("applied_generation", "7"),
        ("applied_generation", 7.0),
        ("applied_generation", True),
        ("restart_count", "2"),
        ("restart_count", -1),
        ("restart_count", False),
    )
    timestamp_values: tuple[object, ...] = ("1.5", True, float("inf"), float("nan"), [])
    for field, malformed in counter_cases:
        _assert_diagnostic_rejects(field, malformed)
    for field in ("next_attempt_at", "next_readiness_at"):
        for malformed in timestamp_values:
            _assert_diagnostic_rejects(field, malformed)


def test_diagnostic_rejects_malformed_present_strings() -> None:
    """Present diagnostic strings cannot collapse to defaults or null."""
    cases: tuple[tuple[str, object], ...] = (
        ("source", ""),
        ("source", 1),
        ("mode", ""),
        ("mode", "unknown"),
        ("intent", ""),
        ("intent", "unknown"),
        ("commit", 1),
        ("message", []),
    )
    for field, malformed in cases:
        _assert_diagnostic_rejects(field, malformed)


def test_diagnostic_defaults_apply_only_to_absent_legacy_fields() -> None:
    """Legacy absence keeps documented defaults without accepting malformed presence."""
    data = _canonical_diagnostic_mapping()
    for field in (
        "live",
        "source",
        "supervisor_present",
        "mode",
        "intent",
        "applied_generation",
        "restart_count",
        "ready",
        "holding",
    ):
        data.pop(field)
    restored = supervise.SupervisorDiagnostic.from_dict(data)
    assert restored.live is False
    assert restored.source == "durable-state"
    assert restored.supervisor_present is False
    assert restored.mode == supervise.MODE_IDLE
    assert restored.intent == supervise.INTENT_RUN
    assert restored.applied_generation == 0
    assert restored.restart_count == 0
    assert restored.ready is False
    assert restored.holding is False
