"""Managed-agent lifecycle authority invariants."""

from __future__ import annotations

import time
from typing import Any, cast

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


@pytest.mark.parametrize("malformed", [0, 0.0, "", [], {}, 1, "yes", [1], None])
def test_delete_tombstone_rejects_malformed_present_values(malformed: object) -> None:
    """Only literal booleans carry durable deletion authority."""
    assert agent._delete_pending_flag({"delete_pending": malformed}) is None


def test_delete_tombstone_preserves_boolean_and_legacy_absence_semantics() -> None:
    """Canonical booleans remain exact and genuine absence stays non-tombstoned."""
    assert agent._delete_pending_flag({"delete_pending": False}) is False
    assert agent._delete_pending_flag({"delete_pending": True}) is True
    assert agent._delete_pending_flag({}) is False


@pytest.mark.parametrize("malformed", [0, 0.0, "", [], {}, 1, "yes", [1], None])
def test_malformed_delete_tombstone_blocks_prompt_claim(
    monkeypatch: pytest.MonkeyPatch, malformed: object
) -> None:
    """Malformed deletion metadata cannot authorize prompt execution."""
    meta: agent.Meta = {
        "id": "aaaaaaaa",
        "state": "running",
        "active_runner": True,
        "pending_prompt": "P",
        "delete_pending": malformed,
    }

    def update(_aid: str, mutate: object) -> None:
        assert callable(mutate)
        mutate(meta)

    monkeypatch.setattr(agent, "update_meta", update)
    assert agent._claim_pending_prompt("aaaaaaaa", "P") is False
    assert meta["pending_prompt"] == "P"
    assert meta["delete_pending"] == malformed


@pytest.mark.parametrize("malformed", [0, 0.0, "", [], {}, 1, "yes", [1], None])
def test_malformed_delete_tombstone_cannot_establish_convergence(
    monkeypatch: pytest.MonkeyPatch, malformed: object
) -> None:
    """Deletion convergence requires a canonical true tombstone."""
    meta: agent.Meta = {
        "id": "aaaaaaaa",
        "delete_pending": malformed,
        "active_runner": False,
    }
    monkeypatch.setattr(agent, "runner_alive", lambda _meta: False)
    monkeypatch.setattr(agent, "group_alive", lambda _meta: False)
    monkeypatch.setattr(agent, "reservation_in_flight", lambda _meta: False)
    monkeypatch.setattr(agent, "_unresolved_child_state", lambda _meta: "gone")
    assert agent._delete_converged(meta) is False


def test_canonical_delete_tombstone_can_converge_when_execution_is_gone(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Literal true retains ordinary deletion convergence semantics."""
    meta: agent.Meta = {
        "id": "aaaaaaaa",
        "delete_pending": True,
        "active_runner": False,
    }
    monkeypatch.setattr(agent, "runner_alive", lambda _meta: False)
    monkeypatch.setattr(agent, "group_alive", lambda _meta: False)
    monkeypatch.setattr(agent, "reservation_in_flight", lambda _meta: False)
    monkeypatch.setattr(agent, "_unresolved_child_state", lambda _meta: "gone")
    assert agent._delete_converged(meta) is True


@pytest.mark.parametrize("malformed", [0, 0.0, "", [], {}, 1, "yes", [1], None])
def test_delete_transitions_do_not_normalize_malformed_tombstones(
    monkeypatch: pytest.MonkeyPatch, malformed: object
) -> None:
    """Begin/abort deletion preserve malformed authority for explicit repair."""
    meta: agent.Meta = {"id": "aaaaaaaa", "delete_pending": malformed}

    def update(_aid: str, mutate: object) -> None:
        assert callable(mutate)
        mutate(meta)

    monkeypatch.setattr(agent, "update_meta", update)
    assert agent._begin_delete("aaaaaaaa", force=True) is None
    assert meta["delete_pending"] == malformed
    agent._abort_delete("aaaaaaaa")
    assert meta["delete_pending"] == malformed


@pytest.mark.parametrize("malformed", [0, "", 1, "yes"])
def test_malformed_delete_tombstone_blocks_invocation_tracking(malformed: object) -> None:
    """A spawned child is unresolved, never running authority, under malformed tombstones."""

    class ProcessStub:
        pid = 4242

    blocked: dict[str, bool] = {}
    mutate = agent._record_running(cast("Any", ProcessStub()), 77, "inv-1", blocked)
    meta: agent.Meta = {"id": "aaaaaaaa", "delete_pending": malformed}
    mutate(meta)

    assert blocked == {"stopped": True}
    assert "pid" not in meta
    assert meta["unresolved_invocation"] == {
        "pid": 4242,
        "pgid": 4242,
        "start_time": 77,
        "invocation_id": "inv-1",
    }


def test_forced_delete_rechecks_after_signalling_before_convergence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Forced deletion waits for fresh convergence evidence after signalling."""
    meta: agent.Meta = {"id": "aaaaaaaa", "delete_pending": True}
    convergence = iter([False, True])
    signals: list[agent.Meta] = []
    monkeypatch.setattr(agent, "read_meta", lambda _aid: meta)

    def signal(cur: agent.Meta) -> bool:
        signals.append(cur)
        return True

    monkeypatch.setattr(agent, "_signal_delete_execution", signal)
    monkeypatch.setattr(agent, "_delete_converged", lambda _cur: next(convergence))
    monkeypatch.setattr(time, "time", lambda: 0.0)
    monkeypatch.setattr(time, "sleep", lambda _seconds: None)

    assert agent._converge_for_delete("aaaaaaaa", force=True, deadline=1.0) is True
    assert signals == [meta, meta]


@pytest.mark.parametrize(
    "finished_at",
    [False, "", [], {}, -1, float("inf"), float("nan")],
)
def test_dead_running_state_requires_canonical_completion_timestamp(
    monkeypatch: pytest.MonkeyPatch, finished_at: object
) -> None:
    """Malformed completion timestamps cannot preserve running authority."""
    meta = _running_meta(pid=123, finished_at=finished_at)
    monkeypatch.setattr("lubko.agent.is_alive", lambda _value: False)
    assert derive_state(meta) == "unknown"


def test_dead_running_state_preserves_valid_completion_timestamp(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A canonical completion timestamp preserves existing derived-state behavior."""
    meta = _running_meta(pid=123, finished_at=1.0)
    monkeypatch.setattr("lubko.agent.is_alive", lambda _value: False)
    assert derive_state(meta) == "running"
