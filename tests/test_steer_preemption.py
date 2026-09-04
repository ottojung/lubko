"""Hard-preemptive managed-agent steering invariants."""

from __future__ import annotations

import signal
import subprocess
import time
from typing import TYPE_CHECKING, cast

import pytest

from lubko import agent

if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture(autouse=True)
def state_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Isolate durable managed-agent state for every test.

    Returns:
        The isolated state directory.
    """
    state = tmp_path / "state"
    monkeypatch.setenv("XDG_STATE_HOME", str(state))
    return state


def _steering_meta(aid: str = "a600") -> agent.Meta:
    """Return canonical running metadata with a durable steer intent."""
    return {
        "id": aid,
        "state": "running",
        "intent": "steer",
        "stop_reason": None,
        "pid": 4242,
        "pgid": 4242,
        "start_time": 111,
        "invocation_id": "0123456789abcdef0123456789abcdef",
        "active_runner": True,
    }


def test_steer_preemption_escalates_and_requires_group_death(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Steer uses bounded TERM/KILL escalation and succeeds only after group death."""
    meta = _steering_meta()
    signals: list[int] = []
    timeouts: list[float] = []
    waits = iter((False, True))
    monkeypatch.setattr(agent, "read_meta", lambda _aid: meta)
    monkeypatch.setattr(agent, "group_alive", lambda _meta: True)
    monkeypatch.setattr(agent, "send_signal_group", lambda _meta, sig: signals.append(sig))

    def wait_dead(_meta: agent.Meta, timeout: float) -> bool:
        timeouts.append(timeout)
        return next(waits)

    monkeypatch.setattr(agent, "wait_group_dead", wait_dead)

    assert agent._interrupt_steer_if_needed(meta["id"]) is True
    assert signals == [signal.SIGTERM, signal.SIGKILL]
    assert timeouts == [agent.STOP_WAIT_SECONDS, agent.KILL_WAIT_SECONDS]


def test_steer_preemption_fails_closed_when_group_death_is_unproven(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A signal attempt is not enough authority to start replacement work."""
    meta = _steering_meta()
    signals: list[int] = []
    monkeypatch.setattr(agent, "read_meta", lambda _aid: meta)
    monkeypatch.setattr(agent, "group_alive", lambda _meta: True)
    monkeypatch.setattr(agent, "send_signal_group", lambda _meta, sig: signals.append(sig))
    monkeypatch.setattr(agent, "wait_group_dead", lambda _meta, _timeout: False)

    assert agent._interrupt_steer_if_needed(meta["id"]) is False
    assert signals == [signal.SIGTERM, signal.SIGKILL]


class _TimedProcess:
    """Minimal process double that stays live for one steer-watch tick."""

    def __init__(self) -> None:
        self.wait_calls = 0

    def wait(self, timeout: float | None = None) -> int:
        self.wait_calls += 1
        if self.wait_calls == 1:
            assert timeout == agent.STEER_POLL_SECONDS
            raise subprocess.TimeoutExpired(cmd=["fake-agent"], timeout=timeout)
        return 0


def test_runner_enforces_durable_steer_when_submitting_cli_disappears(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The runner observes steer intent independently of the accepting CLI lifetime."""
    proc = _TimedProcess()
    observed: list[str] = []

    def record_interrupt(aid: str) -> bool:
        observed.append(aid)
        return True

    monkeypatch.setattr(agent, "_interrupt_steer_if_needed", record_interrupt)

    rc = agent._wait_for_invocation_exit(
        cast("subprocess.Popen[bytes]", proc),
        "a600",
        is_continue=True,
    )

    assert rc == 0
    assert observed == ["a600"]


def test_continuation_waits_for_exact_superseded_group(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Leader exit cannot authorize a new invocation while owned descendants survive."""
    current = _steering_meta()
    observed = dict(current)
    group_states = iter((True, False))
    preemptions: list[str] = []
    monkeypatch.setattr(agent, "read_meta", lambda _aid: current)
    monkeypatch.setattr(agent, "group_alive", lambda _meta: next(group_states))

    def record_preemption(aid: str) -> bool:
        preemptions.append(aid)
        return True

    monkeypatch.setattr(agent, "_interrupt_steer_if_needed", record_preemption)

    assert agent._wait_for_steer_group_convergence(current["id"], observed) is True
    assert preemptions == [current["id"]]


def test_runner_holds_execution_authority_until_ambiguous_group_converges(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Failed exact convergence is retried instead of authorizing overlap."""
    current = _steering_meta()
    observed = dict(current)
    group_states = iter((True, False))
    sleeps: list[float] = []
    monkeypatch.setattr(agent, "read_meta", lambda _aid: current)
    monkeypatch.setattr(agent, "group_alive", lambda _meta: next(group_states))
    monkeypatch.setattr(agent, "_interrupt_steer_if_needed", lambda _aid: False)

    def record_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    monkeypatch.setattr(time, "sleep", record_sleep)

    assert agent._wait_for_steer_group_convergence(current["id"], observed) is True
    assert sleeps == [agent.STEER_POLL_SECONDS]


def test_dead_runner_recovers_oldest_accepted_steer_before_new_prompt(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Runner failure preserves FIFO control work ahead of later callers."""
    meta = agent.idle_meta("a600", str(tmp_path), None)
    meta.update(
        state="running",
        active_runner=True,
        runner_gen=1,
        runner_reservation={
            "gen": 1,
            "owner_pid": 999999,
            "owner_start_ticks": 1,
            "state": "claimed",
            "reserved_at": 1.0,
            "mode": "continue",
        },
        pending_prompt=None,
        prompt_count=1,
    )
    agent._queue_steer(meta, "first accepted steer", 1.0)
    agent._queue_steer(meta, "second accepted steer", 2.0)
    decision: dict[str, object] = {}
    monkeypatch.setattr(agent, "reservation_in_flight", lambda _meta: False)
    monkeypatch.setattr(agent, "proc_start_ticks", lambda _pid: 123)

    assert (
        agent._recover_stale_reservation(meta, decision, prompt="new ordinary prompt", steer=False)
        is True
    )
    assert decision["action"] == "spawn"
    assert decision["recover_busy"] is True
    assert meta["pending_prompt"] == "first accepted steer"
    queue = meta["steer_queue"]
    assert isinstance(queue, list)
    assert [item["prompt"] for item in queue] == ["second accepted steer"]


def test_recovery_queues_new_steer_behind_accepted_fifo(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A recovery steer never coalesces or jumps ahead of older accepted steers."""
    meta = agent.idle_meta("a600", str(tmp_path), None)
    meta.update(
        state="running",
        active_runner=True,
        runner_gen=1,
        runner_reservation={
            "gen": 1,
            "owner_pid": 999999,
            "owner_start_ticks": 1,
            "state": "claimed",
            "reserved_at": 1.0,
            "mode": "continue",
        },
        pending_prompt=None,
        prompt_count=1,
    )
    agent._queue_steer(meta, "first accepted steer", 1.0)
    agent._queue_steer(meta, "second accepted steer", 2.0)
    decision: dict[str, object] = {}
    monkeypatch.setattr(agent, "reservation_in_flight", lambda _meta: False)
    monkeypatch.setattr(agent, "proc_start_ticks", lambda _pid: 123)

    assert (
        agent._recover_stale_reservation(meta, decision, prompt="third steer", steer=True) is True
    )
    assert decision["action"] == "spawn"
    assert decision["steer_accepted"] is True
    assert meta["pending_prompt"] == "first accepted steer"
    queue = meta["steer_queue"]
    assert isinstance(queue, list)
    assert [item["prompt"] for item in queue] == [
        "second accepted steer",
        "third steer",
    ]


@pytest.mark.parametrize(
    ("intent", "expected_signals", "expected_timeouts"),
    [
        ("stop", [signal.SIGTERM], [agent.STOP_WAIT_SECONDS]),
        ("kill", [signal.SIGKILL], [agent.KILL_WAIT_SECONDS]),
    ],
)
def test_stop_like_intent_converges_exact_group_after_leader_exit(
    monkeypatch: pytest.MonkeyPatch,
    intent: str,
    expected_signals: list[int],
    expected_timeouts: list[float],
) -> None:
    """Stop/kill cannot terminalize merely because the invocation leader exited."""
    current = _steering_meta()
    current["intent"] = intent
    observed = dict(current)
    group_states = iter((True, False))
    signals: list[int] = []
    timeouts: list[float] = []
    monkeypatch.setattr(agent, "read_meta", lambda _aid: current)
    monkeypatch.setattr(agent, "group_alive", lambda _meta: next(group_states))
    monkeypatch.setattr(
        agent,
        "send_signal_group",
        lambda _meta, sig: signals.append(sig),
    )

    def wait_dead(_meta: agent.Meta, timeout: float) -> bool:
        timeouts.append(timeout)
        return True

    monkeypatch.setattr(agent, "wait_group_dead", wait_dead)

    assert agent._wait_for_steer_group_convergence(current["id"], observed) is True
    assert signals == expected_signals
    assert timeouts == expected_timeouts


def test_stop_after_leader_exit_escalates_before_allowing_terminalization(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A surviving exact descendant forces TERM-to-KILL convergence for stop."""
    current = _steering_meta()
    current["intent"] = "stop"
    observed = dict(current)
    group_states = iter((True, False))
    waits = iter((False, True))
    signals: list[int] = []
    monkeypatch.setattr(agent, "read_meta", lambda _aid: current)
    monkeypatch.setattr(agent, "group_alive", lambda _meta: next(group_states))
    monkeypatch.setattr(
        agent,
        "send_signal_group",
        lambda _meta, sig: signals.append(sig),
    )
    monkeypatch.setattr(agent, "wait_group_dead", lambda _meta, _timeout: next(waits))

    assert agent._wait_for_steer_group_convergence(current["id"], observed) is True
    assert signals == [signal.SIGTERM, signal.SIGKILL]
