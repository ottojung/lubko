"""Managed-agent prompt-count durable authority invariants."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pathlib import Path

from lubko import agent

MALFORMED_COUNTS = ["7", 7.0, True, False, -1, None, [], {}]


@pytest.mark.parametrize("count", MALFORMED_COUNTS)
def test_malformed_prompt_count_blocks_fresh_acceptance(tmp_path: Path, count: object) -> None:
    """Fresh acceptance cannot normalize malformed durable prompt counts."""
    meta = agent.idle_meta("audit", str(tmp_path), None)
    meta["prompt_count"] = count
    decision: dict[str, object] = {}

    agent._apply_locked_transition(
        meta,
        decision,
        prompt="work",
        steer=False,
        mode="new",
    )

    assert decision == {"action": "busy"}
    assert meta["prompt_count"] is count
    assert meta.get("pending_prompt") is None
    assert meta["runner_reservation"] is None


@pytest.mark.parametrize("count", MALFORMED_COUNTS)
def test_malformed_prompt_count_blocks_stale_recovery(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    count: object,
) -> None:
    """Stale-reservation recovery cannot normalize malformed prompt counts."""
    meta = agent.idle_meta("audit", str(tmp_path), None)
    meta.update(
        prompt_count=count,
        active_runner=True,
        runner_gen=7,
        runner_reservation={
            "state": "reserved",
            "gen": 7,
            "owner_pid": 1,
            "owner_start_ticks": 1,
            "mode": "new",
        },
        pending_prompt=None,
    )
    monkeypatch.setattr(agent, "runner_alive", lambda _meta: False)
    monkeypatch.setattr(agent, "_owner_alive", lambda _pid, _ticks: False)
    monkeypatch.setattr(agent, "_runner_marker_alive", lambda _aid, _gen: False)
    decision: dict[str, object] = {}

    agent._apply_locked_transition(
        meta,
        decision,
        prompt="recovered",
        steer=False,
        mode="new",
    )

    assert decision == {"action": "busy"}
    assert meta["prompt_count"] is count
    assert meta.get("pending_prompt") is None
    reservation = meta["runner_reservation"]
    assert isinstance(reservation, dict)
    assert reservation["gen"] == 7


@pytest.mark.parametrize("count", MALFORMED_COUNTS)
def test_malformed_prompt_count_blocks_queued_promotion(
    tmp_path: Path,
    count: object,
) -> None:
    """Queued work remains queued when durable prompt_count is malformed."""
    meta = agent.idle_meta("audit", str(tmp_path), None)
    item = {"seq": 1, "prompt": "queued", "queued_at": 1.0}
    meta["prompt_count"] = count
    meta["steer_queue"] = [item.copy()]

    assert agent._pop_into_pending(meta, 2.0) is None
    assert meta["prompt_count"] is count
    assert meta.get("pending_prompt") is None
    assert meta["steer_queue"] == [item]


@pytest.mark.parametrize("initial", [None, 0, 7])
def test_canonical_or_absent_prompt_count_increments(
    tmp_path: Path,
    initial: int | None,
) -> None:
    """Absence means zero; canonical non-negative integers increment exactly."""
    meta = agent.idle_meta("audit", str(tmp_path), None)
    if initial is None:
        del meta["prompt_count"]
        expected = 1
    else:
        meta["prompt_count"] = initial
        expected = initial + 1
    decision: dict[str, object] = {}

    agent._apply_locked_transition(
        meta,
        decision,
        prompt="work",
        steer=False,
        mode="new",
    )

    assert decision["action"] == "spawn"
    assert meta["prompt_count"] == expected
    assert meta["pending_prompt"] == "work"
