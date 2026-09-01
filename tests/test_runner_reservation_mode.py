"""Strict durable runner-reservation mode authority."""

from __future__ import annotations

import copy
import subprocess

import pytest

from lubko import agent

BAD_MODES: list[object] = [None, "", "resume", 123, True, 1.5, [], {}]


@pytest.mark.parametrize("bad", BAD_MODES)
def test_runner_reservation_mode_rejects_malformed_values(bad: object) -> None:
    """Only exact canonical native-session modes carry reservation authority."""
    assert agent._runner_reservation_mode({"mode": bad}) is None


def test_runner_reservation_mode_rejects_missing_value() -> None:
    """Missing durable mode cannot silently become fresh-session authority."""
    assert agent._runner_reservation_mode({}) is None


@pytest.mark.parametrize("mode", ["new", "continue"])
def test_stale_reservation_recovery_preserves_canonical_mode(
    monkeypatch: pytest.MonkeyPatch, mode: str
) -> None:
    """Valid stale recovery keeps the exact reserved native-session mode."""
    meta: agent.Meta = {
        "id": "audit",
        "active_runner": True,
        "pending_prompt": "accepted",
        "prompt_count": 1,
        "runner_gen": 7,
        "runner_reservation": {
            "state": "reserved",
            "gen": 7,
            "owner_pid": 999999,
            "owner_start_ticks": 1,
            "mode": mode,
        },
    }
    decision: dict[str, object] = {}
    monkeypatch.setattr(agent, "reservation_in_flight", lambda _m: False)

    assert agent._recover_stale_reservation(meta, decision, prompt="new caller", steer=False)
    assert decision["action"] == "spawn"
    assert decision["mode"] == mode
    reservation = meta["runner_reservation"]
    assert isinstance(reservation, dict)
    assert reservation["mode"] == mode


@pytest.mark.parametrize("bad", BAD_MODES)
def test_stale_reservation_mode_fails_closed_before_mutation(
    monkeypatch: pytest.MonkeyPatch, bad: object
) -> None:
    """Malformed mode cannot be normalized while accepted work is recovered."""
    meta: agent.Meta = {
        "id": "audit",
        "active_runner": True,
        "pending_prompt": "accepted",
        "prompt_count": 1,
        "runner_gen": 7,
        "runner_reservation": {
            "state": "reserved",
            "gen": 7,
            "owner_pid": 999999,
            "owner_start_ticks": 1,
            "mode": bad,
        },
    }
    before = copy.deepcopy(meta)
    decision: dict[str, object] = {}
    monkeypatch.setattr(agent, "reservation_in_flight", lambda _m: False)

    assert agent._recover_stale_reservation(meta, decision, prompt="new caller", steer=False)
    assert decision == {"action": "busy"}
    assert meta == before


def test_missing_stale_reservation_mode_fails_closed_before_mutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Legacy-looking absence is ambiguous and cannot default to ``new``."""
    meta: agent.Meta = {
        "id": "audit",
        "active_runner": True,
        "pending_prompt": "accepted",
        "runner_gen": 7,
        "runner_reservation": {
            "state": "reserved",
            "gen": 7,
            "owner_pid": 999999,
            "owner_start_ticks": 1,
        },
    }
    before = copy.deepcopy(meta)
    decision: dict[str, object] = {}
    monkeypatch.setattr(agent, "reservation_in_flight", lambda _m: False)

    assert agent._recover_stale_reservation(meta, decision, prompt="new caller", steer=False)
    assert decision == {"action": "busy"}
    assert meta == before


@pytest.mark.parametrize("bad", BAD_MODES)
def test_malformed_mode_cannot_justify_reserved_runner_in_flight(
    monkeypatch: pytest.MonkeyPatch, bad: object
) -> None:
    """Live spawner identity cannot bless malformed execution-mode authority."""
    meta: agent.Meta = {
        "id": "audit",
        "active_runner": True,
        "runner_reservation": {
            "state": "reserved",
            "gen": 1,
            "owner_pid": 1234,
            "owner_start_ticks": 55,
            "mode": bad,
        },
    }
    monkeypatch.setattr(agent, "runner_alive", lambda _m: False)
    monkeypatch.setattr(agent, "_owner_alive", lambda *_a: True)
    assert not agent.reservation_in_flight(meta)
    assert not agent.active_runner_justified(meta)


@pytest.mark.parametrize("reservation_mode", [123, [], {}, "resume"])
def test_runner_refuses_malformed_reserved_mode_before_claim(
    monkeypatch: pytest.MonkeyPatch, reservation_mode: object
) -> None:
    """A spawned runner cannot claim malformed durable mode authority."""
    meta: agent.Meta = {
        "id": "audit",
        "active_runner": True,
        "runner_reservation": {
            "state": "reserved",
            "gen": 1,
            "mode": reservation_mode,
        },
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
        lambda *_a, **_kw: pytest.fail("malformed reservation mode reached execution"),
    )

    agent.runner("audit", "new")
    assert meta == before


def test_runner_refuses_mode_mismatch_before_claim(monkeypatch: pytest.MonkeyPatch) -> None:
    """Spawn argv cannot override a different canonical durable mode."""
    meta: agent.Meta = {
        "id": "audit",
        "active_runner": True,
        "runner_reservation": {"state": "reserved", "gen": 1, "mode": "continue"},
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
        lambda *_a, **_kw: pytest.fail("mismatched runner mode reached execution"),
    )

    agent.runner("audit", "new")
    assert meta == before


@pytest.mark.parametrize("bad", ["", "resume", "123"])
def test_spawn_runner_rejects_noncanonical_mode_before_popen(
    monkeypatch: pytest.MonkeyPatch, bad: str
) -> None:
    """The spawn boundary itself accepts only canonical modes."""
    monkeypatch.setattr(
        subprocess,
        "Popen",
        lambda *_a, **_kw: pytest.fail("invalid runner mode reached subprocess"),
    )
    with pytest.raises(ValueError, match="runner mode is malformed"):
        agent.spawn_runner("audit", bad, gen=1)
