"""Strict JSON-boolean handling of the ``restart`` flag in desired intents."""

from __future__ import annotations

import json
from dataclasses import replace
from typing import TYPE_CHECKING

import pytest

from lubko import supervise, supervisor

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

COMMIT = "a" * 40


def intent_payload(**overrides: object) -> dict[str, object]:
    """Return a minimal valid desired-intent payload."""
    payload: dict[str, object] = {
        "schema_version": supervise.SCHEMA_VERSION,
        "generation": 7,
        "commit": COMMIT,
        "repo": "/workspace/repo",
        "uv_path": "uv",
        "worker_id": None,
    }
    payload.update(overrides)
    return payload


@pytest.mark.parametrize(("restart", "expected"), [(None, False), (True, True), (False, False)])
def test_missing_and_boolean_restart_values_parse(restart: object, expected: object) -> None:
    """Missing parses as false; only literal JSON booleans are accepted."""
    payload = intent_payload() if restart is None else intent_payload(restart=restart)
    desired = supervise.SupervisorDesired.from_dict(payload)
    assert desired.restart is expected


@pytest.mark.parametrize("malformed", [1, 0, "true", "", {}, [], [True]])
def test_present_non_boolean_restart_fails_closed(malformed: object) -> None:
    """A present non-boolean ``restart`` enters malformed-desired handling."""
    with pytest.raises((TypeError, ValueError), match="malformed"):
        supervise.SupervisorDesired.from_dict(intent_payload(restart=malformed))


def _write_intent(raw: dict[str, object]) -> None:
    supervise.desired_path().parent.mkdir(parents=True, exist_ok=True)
    supervise.desired_path().write_text(json.dumps(raw), encoding="utf-8")


@pytest.fixture
def settled_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Callable[[], supervise.SupervisorState]:
    """Isolate state and seed a durable record of an already-applied generation.

    Returns:
        A callable reading the durable supervisor state.
    """
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
    supervise.write_state(
        replace(
            supervise.fresh_state(),
            mode="run",
            intent="run",
            applied_generation=5,
            commit=COMMIT,
        )
    )
    return supervise.read_state


def test_malformed_restart_does_not_advance_settlement(
    settled_state: Callable[[], supervise.SupervisorState],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A malformed ``restart`` never lets the daemon apply the newer generation."""
    del settled_state
    _write_intent(intent_payload(restart="true"))
    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    monkeypatch.setattr(
        daemon,
        "_ensure_worker",
        lambda _commit: pytest.fail("malformed intent reached worker spawn path"),
    )

    daemon.reconcile(0.0)

    with pytest.raises(supervise.DesiredIntentError):
        supervise.read_desired_strict()
    assert supervise.read_state().applied_generation == 5
    assert supervise.read_state().commit == COMMIT


def test_same_commit_settlement_advances_on_valid_restart_false(
    settled_state: Callable[[], supervise.SupervisorState],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A legacy intent without ``restart`` still records the newer generation."""
    del settled_state
    _write_intent(intent_payload())
    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    monkeypatch.setattr(daemon, "_ensure_worker", lambda _commit: None)
    monkeypatch.setattr(daemon, "_retire_child", lambda: True)

    daemon.reconcile(0.0)

    state = supervise.read_state()
    assert state.applied_generation == 7
