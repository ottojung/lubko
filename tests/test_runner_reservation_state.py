"""Strict durable runner-reservation lifecycle authority."""

from __future__ import annotations

import copy

import pytest

from lubko import agent

BAD_STATES: list[object] = [None, "", "other", True, 0, 1.5, [], {}]


@pytest.mark.parametrize("state", ["reserved", "claimed"])
def test_reservation_state_accepts_only_canonical_lifecycle_values(state: str) -> None:
    """Only exact canonical state strings carry lifecycle authority."""
    assert agent._runner_reservation_state({"state": state}) == state


def test_reservation_state_distinguishes_genuine_absence() -> None:
    """Genuine reservation absence remains distinct from corruption."""
    assert agent._runner_reservation_state(None) == "absent"


@pytest.mark.parametrize("bad", BAD_STATES)
def test_reservation_state_rejects_malformed_present_values(bad: object) -> None:
    """Present malformed discriminators are never normalized."""
    assert agent._runner_reservation_state({"state": bad}) == "malformed"


def test_reservation_state_rejects_missing_discriminator() -> None:
    """A reservation object missing its discriminator is malformed."""
    assert agent._runner_reservation_state({}) == "malformed"


@pytest.mark.parametrize("bad", ["other", True, 0, 1.5, [], {}])
def test_malformed_reservation_blocks_in_flight_convergence(
    monkeypatch: pytest.MonkeyPatch, bad: object
) -> None:
    """Malformed reservation authority blocks negative convergence proof."""
    meta: agent.Meta = {
        "id": "audit",
        "active_runner": True,
        "runner_reservation": {"state": bad, "gen": 1, "mode": "new"},
    }
    monkeypatch.setattr(agent, "runner_alive", lambda _m: False)

    assert agent.reservation_in_flight(meta)
    assert not agent.active_runner_justified(meta)


@pytest.mark.parametrize("bad", ["other", True, 0, 1.5, [], {}])
def test_stale_recovery_preserves_malformed_reservation_authority(
    monkeypatch: pytest.MonkeyPatch, bad: object
) -> None:
    """Malformed state cannot be rewritten during stale recovery."""
    meta: agent.Meta = {
        "id": "audit",
        "active_runner": True,
        "pending_prompt": "accepted",
        "runner_gen": 1,
        "runner_reservation": {"state": bad, "gen": 1, "mode": "new"},
    }
    before = copy.deepcopy(meta)
    decision: dict[str, object] = {}
    monkeypatch.setattr(agent, "reservation_in_flight", lambda _m: False)

    assert agent._recover_stale_reservation(meta, decision, prompt="new", steer=False)
    assert decision == {"action": "busy"}
    assert meta == before


@pytest.mark.parametrize("bad", ["other", True, 0, 1.5, [], {}])
def test_runner_refuses_malformed_reservation_state_before_claim(
    monkeypatch: pytest.MonkeyPatch, bad: object
) -> None:
    """A production runner cannot claim malformed reservation state."""
    meta: agent.Meta = {
        "id": "audit",
        "active_runner": True,
        "runner_reservation": {"state": bad, "gen": 1, "mode": "new"},
    }
    before = copy.deepcopy(meta)
    monkeypatch.setenv("LUBKO_RUNNER_GEN", "1")
    monkeypatch.setattr(agent, "read_meta", lambda _aid: meta)

    def fake_update_meta(_aid: str, mutate: object) -> agent.Meta:
        mutate(meta)  # type: ignore[operator]
        return meta

    monkeypatch.setattr(agent, "update_meta", fake_update_meta)
    monkeypatch.setattr(
        agent,
        "_runner_loop",
        lambda *_a, **_kw: pytest.fail("malformed reservation state reached execution"),
    )

    agent.runner("audit", "new")
    assert meta == before


def test_malformed_reservation_cannot_authorize_prompt_reuse(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Malformed reservation authority makes prompt transitions explicitly busy."""
    meta: agent.Meta = {
        "id": "audit",
        "state": "idle",
        "active_runner": True,
        "pending_prompt": "accepted",
        "runner_gen": 1,
        "prompt_count": 1,
        "steer_queue": [],
        "steer_seq": 0,
        "runner_reservation": {"state": [], "gen": 1, "mode": "new"},
    }
    before = copy.deepcopy(meta)
    decision: dict[str, object] = {}
    monkeypatch.setattr(agent, "is_alive", lambda _m: False)
    monkeypatch.setattr(agent, "runner_alive", lambda _m: False)

    agent._apply_locked_transition(meta, decision, prompt="new caller", steer=True, mode="new")

    assert decision == {"action": "busy"}
    assert meta == before
