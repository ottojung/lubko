"""Regression tests for supervisor crash-backoff stability (#173)."""

from __future__ import annotations

from dataclasses import replace

import pytest

from lubko import supervise
from lubko.supervisor import Settings, SupervisorDaemon

COMMIT = "f" * 40
TEST_TOKEN = "test-incarnation"  # ruff: ignore[hardcoded-password-string] - test token


def _backing_off_without_child(
    now: float,
    *,
    restart_count: int,
    delay: float,
) -> supervise.SupervisorState:
    """Persist a crash-loop state whose retry deadline is still in the future.

    Returns:
        The persisted supervisor state.
    """
    state = replace(
        supervise.fresh_state(),
        mode=supervise.MODE_RUN,
        commit=COMMIT,
        intent=supervise.INTENT_RUN,
        restart_count=restart_count,
        next_attempt_at=now + delay,
        last_exit=supervise.LastExit(returncode=1, at=1.0),
        last_spawn_at=now - supervise.DEFAULT_STABLE_WINDOW_SECONDS,
    )
    supervise.write_state(state)
    return state


@pytest.mark.parametrize(("restart_count", "delay"), [(5, 32.0), (6, 64.0), (7, 120.0)])
def test_reconcile_preserves_future_backoff_without_child(
    monkeypatch: pytest.MonkeyPatch,
    restart_count: int,
    delay: float,
) -> None:
    """A dead child cannot erase any active long crash-backoff deadline."""
    now = 1_000.0
    _backing_off_without_child(now, restart_count=restart_count, delay=delay)
    daemon = SupervisorDaemon(Settings())
    spawned: list[str] = []

    monkeypatch.setattr(daemon, "_derive_action", lambda _state: ("run", COMMIT))
    monkeypatch.setattr(daemon, "_ensure_worker", spawned.append)
    monkeypatch.setattr(daemon, "_record_mission_progress", lambda _commit: None)
    monkeypatch.setattr(daemon, "_probe_readiness", lambda _now: None)

    daemon.reconcile(now)

    state = supervise.read_state()
    assert spawned == []
    assert state.child is None
    assert state.restart_count == restart_count
    assert state.next_attempt_at == now + delay
    assert state.last_exit == supervise.LastExit(returncode=1, at=1.0)


def test_live_child_earns_stability_reset(monkeypatch: pytest.MonkeyPatch) -> None:
    """A continuously live exact child still clears prior crash history."""
    now = 1_000.0
    child = supervise.WorkerChild(
        pid=101,
        pgid=101,
        sid=101,
        start_time_ticks=12345,
        token=TEST_TOKEN,
        worker_id="test-worker",
        spawned_at=1.0,
    )
    state = replace(
        supervise.fresh_state(),
        mode=supervise.MODE_RUN,
        commit=COMMIT,
        child=child,
        intent=supervise.INTENT_RUN,
        restart_count=5,
        next_attempt_at=None,
        last_exit=supervise.LastExit(returncode=1, at=1.0),
        last_spawn_at=now - supervise.DEFAULT_STABLE_WINDOW_SECONDS,
    )
    supervise.write_state(state)
    daemon = SupervisorDaemon(Settings())
    spawned: list[str] = []

    monkeypatch.setattr(daemon, "_derive_action", lambda _state: ("run", COMMIT))
    monkeypatch.setattr(daemon, "_child_alive", lambda _state: True)
    monkeypatch.setattr(daemon, "_ensure_worker", spawned.append)
    monkeypatch.setattr(daemon, "_record_mission_progress", lambda _commit: None)
    monkeypatch.setattr(daemon, "_probe_readiness", lambda _now: None)

    daemon.reconcile(now)

    current = supervise.read_state()
    assert current.child == child
    assert current.restart_count == 0
    assert current.next_attempt_at is None
    assert current.last_exit is None
    assert spawned == [COMMIT]
