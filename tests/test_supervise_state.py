"""Durable supervisor ownership-state invariants."""

import json
from pathlib import Path

import pytest

from lubko import supervise


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
