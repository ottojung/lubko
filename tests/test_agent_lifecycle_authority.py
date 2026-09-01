"""Managed-agent lifecycle authority invariants."""

from __future__ import annotations

import time

import pytest

from lubko import agent
from lubko.agent import derive_state


def _running_meta(**overrides: object) -> dict[str, object]:
    meta: dict[str, object] = {
        "state": "running",
        "pid": None,
        "started_at": 100.0,
        "created_at": 90.0,
    }
    meta.update(overrides)
    return meta


def test_derive_state_uses_liveness_for_canonical_pid(monkeypatch: pytest.MonkeyPatch) -> None:
    """Canonical positive PIDs use strict liveness evidence."""
    meta = _running_meta(pid=123)
    monkeypatch.setattr("lubko.agent.is_alive", lambda value: value is meta)
    assert derive_state(meta) == "running"


def test_derive_state_allows_only_genuine_pid_absence_launch_grace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Only genuine PID absence receives bounded launch grace."""
    monkeypatch.setattr(time, "time", lambda: 120.0)
    assert derive_state(_running_meta(pid=None)) == "running"

    malformed_values: tuple[object, ...] = (False, 0, 0.0, "", [], {})
    for malformed in malformed_values:
        assert derive_state(_running_meta(pid=malformed)) == "unknown"


@pytest.mark.parametrize("field", ["started_at", "created_at"])
@pytest.mark.parametrize("malformed", [False, "", [], {}, float("inf"), float("nan")])
def test_derive_state_fails_closed_on_malformed_launch_timestamp(
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    malformed: object,
) -> None:
    """Malformed present launch timestamps fail closed."""
    monkeypatch.setattr(time, "time", lambda: 120.0)
    meta = _running_meta(pid=None)
    if field == "created_at":
        meta["started_at"] = None
    meta[field] = malformed
    assert derive_state(meta) == "unknown"


@pytest.mark.parametrize(
    "malformed",
    [False, 0, 0.0, "", [], {}, ["corrupt"], "bogus", 1, True],
)
def test_derive_state_fails_closed_on_malformed_lifecycle_state(malformed: object) -> None:
    """Malformed present lifecycle state never becomes idle/running/terminal authority."""
    assert derive_state({"id": "a1", "state": malformed}) == "unknown"


def _idle_transition_meta(state: object) -> dict[str, object]:
    return {
        "id": "a1",
        "state": state,
        "active_runner": False,
        "pending_prompt": None,
        "prompt_count": 0,
        "runner_gen": 0,
        "runner_reservation": None,
        "steer_queue": [],
        "steer_seq": 0,
        "native_session_id": None,
    }


@pytest.mark.parametrize("malformed", [False, 0, "", [], {}, ["corrupt"], "bogus"])
def test_locked_transition_does_not_repair_malformed_state_into_runner_authority(
    monkeypatch: pytest.MonkeyPatch, malformed: object
) -> None:
    """Malformed lifecycle state blocks prompt acceptance without mutating durable state."""
    meta = _idle_transition_meta(malformed)
    before = dict(meta)
    decision: dict[str, object] = {}
    monkeypatch.setattr(agent, "is_alive", lambda _m: False)
    monkeypatch.setattr(agent, "runner_alive", lambda _m: False)
    monkeypatch.setattr(agent, "reservation_in_flight", lambda _m: False)

    with pytest.raises(agent.MalformedLifecycleStateError):
        agent._apply_locked_transition(meta, decision, prompt="P", steer=False, mode="new")

    assert decision == {}
    assert meta == before
    assert meta["runner_reservation"] is None


def test_missing_lifecycle_state_retains_legacy_idle_semantics() -> None:
    """Genuine field absence remains distinct from malformed presence."""
    assert derive_state({"id": "a1"}) == "idle"
