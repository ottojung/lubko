"""Lifecycle retirement signalling invariants.

Retirement signals (drain SIGTERM and escalation SIGKILL) must each be
authorized by exact worker identity at the moment of delivery under a
kernel-stable pin, so a recycled PID/PGID occupant is never signalled.
"""

import os
import signal
import time
from typing import Final

import pytest

from lubko import agent, lifecycle
from lubko.lifecycle import SCHEMA_VERSION, ProcessIdentity, WorkerMeta

LIVE: Final = ProcessIdentity(pid=42, pgid=42, sid=7, start_time_ticks=1234)
RECYCLED: Final = ProcessIdentity(pid=42, pgid=42, sid=7, start_time_ticks=9999)


class FakeClock:
    """Deterministic monotonic clock advanced by ``sleep``."""

    now: float

    def __init__(self) -> None:
        """Start the clock at zero."""
        self.now = 0.0

    def monotonic(self) -> float:
        """Return the current fake time."""
        return self.now

    def sleep(self, seconds: float) -> None:
        """Advance the fake time instead of blocking."""
        self.now += seconds


class Harness:
    """Injected world for one retirement attempt."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Install deterministic fakes over the lifecycle signalling surface."""
        self.clock = FakeClock()
        self.emissions: list[tuple[int, int]] = []
        self.pin_fd: int | None = 77
        self.closes = 0
        #: Clock time at which the live worker exits (never when ``None``).
        self.exit_at: float | None = None
        #: Clock time at which the numeric PID gets recycled (never if ``None``).
        self.recycle_at: float | None = None
        self.has_token = True

        monkeypatch.setattr(agent, "open_pidfd", lambda _pid: self.pin_fd)

        def observe(_pid: int) -> ProcessIdentity | None:
            killed = any(sig == signal.SIGKILL for _, sig in self.emissions)
            if killed or (self.exit_at is not None and self.clock.now >= self.exit_at):
                return None
            if self.recycle_at is not None and self.clock.now >= self.recycle_at:
                return RECYCLED
            return LIVE

        monkeypatch.setattr(lifecycle, "process_identity", observe)
        monkeypatch.setattr(lifecycle, "process_has_token", lambda _pid, _token: self.has_token)
        monkeypatch.setattr(time, "monotonic", self.clock.monotonic)
        monkeypatch.setattr(time, "sleep", self.clock.sleep)
        monkeypatch.setattr(os, "killpg", self._record_killpg)
        monkeypatch.setattr(lifecycle, "drain_sentinel_matches", lambda _token: False)
        monkeypatch.setattr(os, "close", self._record_close)

    def _record_killpg(self, pgid: int, sig: int) -> None:
        self.emissions.append((pgid, sig))

    def _record_close(self, fd: int) -> None:
        del fd
        self.closes += 1


def run(_h: Harness) -> bool:
    """Run ``stop_worker`` with zero grace windows.

    Args:
        _h: The injected retirement world (fakes already installed).

    Returns:
        The retirement outcome reported by ``stop_worker``.
    """
    return lifecycle.stop_worker(meta(), 0.0, cancel_grace_seconds=0.0)


def meta() -> WorkerMeta:
    """Return recorded worker metadata matching :data:`LIVE`."""
    defaults: dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "state": "running",
        "pid": LIVE.pid,
        "pgid": LIVE.pgid,
        "sid": LIVE.sid,
        "start_time_ticks": LIVE.start_time_ticks,
        "token": "t",
        "repo": "/repo",
        "git_commit": None,
        "worker_id": None,
        "log_path": "/log",
        "started_at": 1.0,
        "stopped_at": None,
    }
    return WorkerMeta.from_dict(defaults)


@pytest.fixture
def h(monkeypatch: pytest.MonkeyPatch) -> Harness:
    """Provide one deterministic retirement world per test.

    Returns:
        A harness with fakes installed over the lifecycle module.
    """
    return Harness(monkeypatch)


def test_recycled_group_is_never_signalled_before_term(h: Harness) -> None:
    """A recycled PID occupying the group before SIGTERM absorbs no signal."""
    h.recycle_at = -1.0
    h.pin_fd = None

    assert run(h) is False
    assert h.emissions == []


def test_recycled_group_never_receives_sigkill_escalation(h: Harness) -> None:
    """After SIGTERM, only the exact proven instance may receive SIGKILL."""
    h.recycle_at = 2.0  # exactly at the kill floor, after the drain wait

    assert run(h) is True
    assert h.emissions == [(LIVE.pgid, signal.SIGTERM)]


def test_exact_worker_that_exits_after_term_retires_with_drain_only(h: Harness) -> None:
    """An exact worker exiting after SIGTERM retires without escalation."""
    h.exit_at = 0.5

    assert run(h) is True
    assert h.emissions == [(LIVE.pgid, signal.SIGTERM)]


def test_wedged_exact_worker_receives_sigkill_escalation(h: Harness) -> None:
    """A wedged exact worker is escalated to SIGKILL and then reaps."""
    assert run(h) is True
    assert h.emissions == [
        (LIVE.pgid, signal.SIGTERM),
        (LIVE.pgid, signal.SIGKILL),
    ]


def test_unpinnable_live_worker_fails_closed(h: Harness) -> None:
    """A live worker that cannot be pinned is never signalled."""
    h.pin_fd = None

    assert run(h) is False
    assert h.emissions == []


def test_worker_gone_before_pin_reports_retired_without_signalling(h: Harness) -> None:
    """An already-gone worker retires successfully with nothing emitted."""
    h.pin_fd = None
    h.exit_at = -1.0

    assert run(h) is True
    assert h.emissions == []


def test_missing_lifecycle_token_fails_closed(h: Harness) -> None:
    """Token ownership is retained: an unowned live instance is not signalled."""
    h.has_token = False

    assert run(h) is False
    assert h.emissions == []


def test_pin_descriptor_is_released_after_retirement(h: Harness) -> None:
    """The pidfd pin never leaks across a retirement attempt."""
    run(h)
    assert h.closes == 1
