"""Durable supervisor ownership-state invariants."""

import json
from pathlib import Path

import pytest

from lubko import supervise, supervisor


@pytest.fixture
def state_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Use an isolated state root for each persistence test.

    Returns:
        The supervisor state path.
    """
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
    path = supervise.state_path()
    path.parent.mkdir(parents=True)
    return path


def test_genuine_child_absence_remains_idle(state_path: Path) -> None:
    """Missing state and an explicit null child remain valid absence."""
    assert supervise.read_state().ownership_hold_malformed is False
    state_path.write_text(
        json.dumps({"schema_version": supervise.SCHEMA_VERSION, "child": None}),
        encoding="utf-8",
    )
    state = supervise.read_state()
    assert state.child is None
    assert state.ownership_hold_malformed is False


@pytest.mark.parametrize(
    "raw",
    [
        "{not json",
        "[]",
        json.dumps({"schema_version": supervise.SCHEMA_VERSION + 1}),
        json.dumps({"schema_version": supervise.SCHEMA_VERSION, "child": {"pid": "unknown"}}),
    ],
)
def test_present_corrupt_authority_is_durable_hold(state_path: Path, raw: str) -> None:
    """Corrupt authority never becomes absence, including after a rewrite."""
    state_path.write_text(raw, encoding="utf-8")
    state = supervise.read_state()
    assert state.ownership_hold_malformed is True

    supervise.write_state(state)
    rewritten = supervise.read_state()
    assert rewritten.ownership_hold_malformed is True


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("ownership_hold_malformed", None),
        ("ownership_hold_malformed", "true"),
        ("ownership_hold_malformed", 1),
        ("unresolved_hold_malformed", None),
        ("unresolved_hold_malformed", "true"),
        ("unresolved_hold_malformed", 1),
    ],
)
def test_present_non_boolean_safety_bit_is_durable_hold(
    state_path: Path, field: str, value: object
) -> None:
    """A malformed persisted safety bit cannot erase its obligation."""
    state_path.write_text(
        json.dumps({"schema_version": supervise.SCHEMA_VERSION, field: value}),
        encoding="utf-8",
    )

    state = supervise.read_state()
    assert getattr(state, field) is True
    supervise.write_state(state)
    assert getattr(supervise.read_state(), field) is True


@pytest.mark.parametrize("field", ["ownership_hold_malformed", "unresolved_hold_malformed"])
@pytest.mark.parametrize("value", [False, True])
def test_boolean_safety_bit_values_are_preserved(
    state_path: Path, field: str, value: object
) -> None:
    """Actual JSON booleans retain their explicit safety-bit values."""
    state_path.write_text(
        json.dumps({"schema_version": supervise.SCHEMA_VERSION, field: value}),
        encoding="utf-8",
    )

    state = supervise.read_state()
    assert getattr(state, field) is value


def test_reconcile_holds_before_worker_spawn_on_ownership_corruption(
    state_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Reconcile returns before any worker path for a corrupt ownership bit."""
    state_path.write_text(
        json.dumps({
            "schema_version": supervise.SCHEMA_VERSION,
            "ownership_hold_malformed": "not-a-boolean",
        }),
        encoding="utf-8",
    )
    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    monkeypatch.setattr(
        daemon,
        "_ensure_worker",
        lambda _commit: pytest.fail("corrupt ownership state reached worker spawn path"),
    )

    daemon.reconcile(0.0)

    assert daemon._message is not None
    assert "malformed" in daemon._message
