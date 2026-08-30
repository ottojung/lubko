"""Managed-agent runner generation authority invariants."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pathlib import Path

from lubko import agent


@pytest.mark.parametrize("generation", [7.9, "7", True, False, -1, None, [], {}])
def test_reservation_generation_must_be_canonical_integer(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    generation: object,
) -> None:
    """Malformed reservation generations grant no liveness authority."""
    meta = agent.idle_meta("audit", str(tmp_path), None)
    meta.update(
        active_runner=True,
        runner_reservation={
            "state": "reserved",
            "gen": generation,
            "owner_pid": 1,
            "owner_start_ticks": 1,
        },
    )
    marker_calls: list[tuple[str, int]] = []
    owner_calls: list[tuple[object, object]] = []

    def owner_alive(pid: object, ticks: object) -> bool:
        owner_calls.append((pid, ticks))
        return True

    def marker_alive(aid: str, gen: int) -> bool:
        marker_calls.append((aid, gen))
        return True

    monkeypatch.setattr(agent, "runner_alive", lambda _meta: False)
    monkeypatch.setattr(agent, "_owner_alive", owner_alive)
    monkeypatch.setattr(agent, "_runner_marker_alive", marker_alive)

    assert not agent.reservation_in_flight(meta)
    assert owner_calls == []
    assert marker_calls == []


def test_canonical_reservation_generation_retains_marker_fallback(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Canonical integer generations retain the marker fallback."""
    meta = agent.idle_meta("audit", str(tmp_path), None)
    meta.update(
        active_runner=True,
        runner_reservation={
            "state": "reserved",
            "gen": 7,
            "owner_pid": 1,
            "owner_start_ticks": 1,
        },
    )
    marker_calls: list[tuple[str, int]] = []

    def marker_alive(aid: str, gen: int) -> bool:
        marker_calls.append((aid, gen))
        return True

    monkeypatch.setattr(agent, "runner_alive", lambda _meta: False)
    monkeypatch.setattr(agent, "_owner_alive", lambda _pid, _ticks: False)
    monkeypatch.setattr(agent, "_runner_marker_alive", marker_alive)

    assert agent.reservation_in_flight(meta)
    assert marker_calls == [("audit", 7)]


@pytest.mark.parametrize("generation", [7.9, "7", True, False, -1, None])
def test_malformed_runner_generation_blocks_allocation(
    tmp_path: Path,
    generation: object,
) -> None:
    """Malformed persisted runner history cannot allocate a new generation."""
    meta = agent.idle_meta("audit", str(tmp_path), None)
    meta["runner_gen"] = generation
    decision: dict[str, object] = {}

    agent._apply_locked_transition(
        meta,
        decision,
        prompt="work",
        steer=False,
        mode="new",
    )

    assert decision == {"action": "busy"}
    assert meta["runner_gen"] is generation
    assert meta["runner_reservation"] is None


@pytest.mark.parametrize("generation", [7.9, "7", True, False, -1, None])
def test_malformed_runner_generation_blocks_stale_recovery(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    generation: object,
) -> None:
    """Malformed persisted runner history cannot allocate during stale recovery."""
    meta = agent.idle_meta("audit", str(tmp_path), None)
    meta.update(
        active_runner=True,
        runner_gen=generation,
        runner_reservation={
            "state": "reserved",
            "gen": 7,
            "owner_pid": 1,
            "owner_start_ticks": 1,
            "mode": "new",
        },
        pending_prompt="original",
    )
    monkeypatch.setattr(agent, "runner_alive", lambda _meta: False)
    monkeypatch.setattr(agent, "_owner_alive", lambda _pid, _ticks: False)
    monkeypatch.setattr(agent, "_runner_marker_alive", lambda _aid, _gen: False)
    decision: dict[str, object] = {}

    agent._apply_locked_transition(
        meta,
        decision,
        prompt="late",
        steer=False,
        mode="new",
    )

    assert decision == {"action": "busy"}
    assert meta["runner_gen"] is generation
    reservation = meta["runner_reservation"]
    assert isinstance(reservation, dict)
    assert reservation["gen"] == 7


def test_valid_stale_recovery_allocates_next_generation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Valid stale recovery remains monotonic."""
    meta = agent.idle_meta("audit", str(tmp_path), None)
    meta.update(
        active_runner=True,
        runner_gen=7,
        runner_reservation={
            "state": "reserved",
            "gen": 7,
            "owner_pid": 1,
            "owner_start_ticks": 1,
            "mode": "new",
        },
        pending_prompt="original",
    )
    monkeypatch.setattr(agent, "runner_alive", lambda _meta: False)
    monkeypatch.setattr(agent, "_owner_alive", lambda _pid, _ticks: False)
    monkeypatch.setattr(agent, "_runner_marker_alive", lambda _aid, _gen: False)
    decision: dict[str, object] = {}

    agent._apply_locked_transition(
        meta,
        decision,
        prompt="late",
        steer=False,
        mode="new",
    )

    assert decision["action"] == "spawn"
    assert decision["gen"] == 8
    assert meta["runner_gen"] == 8
    reservation = meta["runner_reservation"]
    assert isinstance(reservation, dict)
    assert reservation["gen"] == 8
