"""Regression tests for supervisor crash-backoff stability (#173)."""

from __future__ import annotations

from dataclasses import replace

import pytest

from lubko import supervise
from lubko.supervisor import Settings, SupervisorDaemon

COMMIT = "f" * 40


def _backing_off_without_child(now: float) -> supervise.SupervisorState:
    """Persist a crash-loop state whose retry deadline is still in the future."""
    state = replace(
        supervise.fresh_state(),
        mode=supervise.MODE_RUN,
        commit=COMMIT,
        intent=supervise.INTENT_RUN,
        restart_count=5,
        next_attempt_at=now + 60.0,
        last_exit=supervise.LastExit(returncode=1, at=1.0),
        last_spawn_at=now - supervise.DEFAULT_STABLE_WINDOW_SECONDS,
    )
    supervise.write_state(state)
    return state


def test_reconcile_preserves_future_backoff_without_child(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A dead child cannot erase a still-active crash-backoff deadline."""
    now = 1_000.0
    _backing_off_without_child(now)
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
    assert state.restart_count == 5
    assert state.next_attempt_at == now + 60.0
    assert state.last_exit == supervise.LastExit(returncode=1, at=1.0)


def test_live_child_earns_stability_reset(monkeypatch: pytest.MonkeyPatch) -> None:
    """A continuously live exact child still clears prior crash history."""
    now = 1_000.0
    child = supervise.WorkerChild(
        pid=101,
        pgid=101,
        sid=101,
        start_time_ticks=12345,
        token="test-incarnation",
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
        next_attempt_at=now + 60.0,
        last_exit=supervise.LastExit(returncode=1, at=1.0),
        last_spawn_at=now - supervise.DEFAULT_STABLE_WINDOW_SECONDS,
    )
    supervise.write_state(state)
    daemon = SupervisorDaemon(Settings())
    monkeypatch.setattr(daemon, "_child_alive", lambda _state: True)

    daemon._maybe_reset_backoff(state, now)

    current = supervise.read_state()
    assert current.child == child
    assert current.restart_count == 0
    assert current.next_attempt_at is None
    assert current.last_exit is None


def test_crash_backoff_reaches_configured_cap() -> None:
    """The exponential sequence includes delays beyond the stability window."""
    daemon = SupervisorDaemon(
        Settings(
            backoff_base_seconds=2.0,
            backoff_max_seconds=120.0,
            stable_window_seconds=30.0,
        )
    )

    assert [daemon._backoff_seconds(count) for count in range(1, 8)] == [
        2.0,
        4.0,
        8.0,
        16.0,
        32.0,
        64.0,
        120.0,
    ]
