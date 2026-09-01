"""Strict pending-prompt durable authority regressions."""

import copy
from pathlib import Path

import pytest

from lubko import agent


@pytest.mark.parametrize("bad", [123, True, 1.5, [], {}, ""])
def test_pending_prompt_rejects_malformed_present_values(bad: object) -> None:
    """Reject malformed present durable prompt values."""
    with pytest.raises(agent.MalformedPendingPromptMetadataError):
        agent._pending_prompt({"pending_prompt": bad})


def test_pending_prompt_preserves_none_absence_and_nonempty_string() -> None:
    """Preserve canonical absence and non-empty string semantics."""
    assert agent._pending_prompt({}) is None
    assert agent._pending_prompt({"pending_prompt": None}) is None
    assert agent._pending_prompt({"pending_prompt": "work"}) == "work"


@pytest.mark.parametrize("bad", [123, True, 1.5, [], {}, ""])
def test_reclaim_prompt_fails_closed_before_mutation(
    monkeypatch: pytest.MonkeyPatch, bad: object
) -> None:
    """Reclaim fails closed before mutating malformed prompt state."""
    meta: agent.Meta = {
        "active_runner": True,
        "pending_prompt": bad,
        "steer_queue": [],
        "steer_seq": 0,
    }
    before = copy.deepcopy(meta)

    def fake_update_meta(_aid: str, mutate: object) -> agent.Meta:
        mutate(meta)  # type: ignore[operator]
        return meta

    monkeypatch.setattr(agent, "update_meta", fake_update_meta)
    with pytest.raises(agent.MalformedPendingPromptMetadataError):
        agent._reclaim_prompt("test")
    assert meta == before


@pytest.mark.parametrize("bad", [123, True, 1.5, [], {}, ""])
def test_stale_reservation_recovery_fails_closed_before_mutation(
    monkeypatch: pytest.MonkeyPatch, bad: object
) -> None:
    """Recovery fails closed before mutating malformed prompt state."""
    meta: agent.Meta = {
        "id": "audit",
        "active_runner": True,
        "pending_prompt": bad,
        "runner_gen": 1,
        "runner_reservation": {
            "state": "reserved",
            "gen": 1,
            "owner_pid": 999999,
            "owner_start_ticks": 1,
            "mode": "new",
        },
    }
    before = copy.deepcopy(meta)
    monkeypatch.setattr(agent, "reservation_in_flight", lambda _m: False)
    with pytest.raises(agent.MalformedPendingPromptMetadataError):
        agent._recover_stale_reservation(meta, {}, prompt="new", steer=False)
    assert meta == before


@pytest.mark.parametrize("bad", [0, False, 0.0, [], {}, ""])
def test_malformed_pending_prompt_blocks_quiescence(
    monkeypatch: pytest.MonkeyPatch, bad: object
) -> None:
    """Malformed prompt state blocks quiescence."""
    monkeypatch.setattr(agent, "runner_alive", lambda _m: False)
    monkeypatch.setattr(agent, "reservation_in_flight", lambda _m: False)
    assert agent._no_invocation_owned({"pending_prompt": bad}) is False


@pytest.mark.parametrize("bad", [123, True, 1.5, [], {}, ""])
def test_locked_transition_rejects_malformed_before_mutation(
    monkeypatch: pytest.MonkeyPatch, bad: object
) -> None:
    """Transitions reject malformed prompt state before mutation."""
    meta: agent.Meta = {"pending_prompt": bad}
    before = copy.deepcopy(meta)
    monkeypatch.setattr(agent, "is_alive", lambda _m: False)
    monkeypatch.setattr(agent, "runner_alive", lambda _m: False)
    monkeypatch.setattr(agent, "reservation_in_flight", lambda _m: False)
    with pytest.raises(agent.MalformedPendingPromptMetadataError):
        agent._apply_locked_transition(meta, {}, prompt="new", steer=False, mode="new")
    assert meta == before


@pytest.mark.parametrize("bad", [123, True, 1.5, [], {}, ""])
def test_clear_pending_rejects_malformed_before_mutation(bad: object) -> None:
    """Exact-claim clearing cannot normalize malformed durable prompt state."""
    meta: agent.Meta = {"pending_prompt": bad}
    before = copy.deepcopy(meta)
    with pytest.raises(agent.MalformedPendingPromptMetadataError):
        agent._clear_pending(meta, "work")
    assert meta == before


@pytest.mark.parametrize("bad", [123, True, 1.5, [], {}, ""])
def test_drain_next_rejects_malformed_pending_prompt(
    monkeypatch: pytest.MonkeyPatch, bad: object
) -> None:
    """Drain cannot forward malformed durable prompt values."""
    meta: agent.Meta = {
        "active_runner": True,
        "pending_prompt": bad,
        "steer_queue": [],
        "steer_seq": 0,
    }
    before = copy.deepcopy(meta)

    def fake_update_meta(_aid: str, mutate: object) -> agent.Meta:
        mutate(meta)  # type: ignore[operator]
        return meta

    monkeypatch.setattr(agent, "update_meta", fake_update_meta)
    with pytest.raises(agent.MalformedPendingPromptMetadataError):
        agent._drain_next("test")
    assert meta == before


@pytest.mark.parametrize("bad", [123, True, 1.5, [], {}, ""])
def test_runner_loop_never_forwards_malformed_pending_prompt(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, bad: object
) -> None:
    """Runner consumption validates durable prompt shape before invocation."""
    monkeypatch.setattr(agent, "read_meta", lambda _aid: {"pending_prompt": bad})
    called: list[object] = []
    monkeypatch.setattr(agent, "_run_invocation", lambda *a, **k: called.append((a, k)))
    ctx = agent._RunnerContext(
        aid="audit", log_path=tmp_path / "output.log", cwd=str(tmp_path), env={}
    )
    with pytest.raises(agent.MalformedPendingPromptMetadataError):
        agent._runner_loop(ctx, is_continue=False)
    assert called == []
