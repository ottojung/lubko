"""Deterministic invariants for converging an unresolved worker hold.

Signals must only ever be delivered through a pinned pidfd to a process that
still provably matches the recorded start-time ticks, and the same pin must be
retained across TERM and KILL escalation so a recycled numeric PID can never
be signalled.
"""

from __future__ import annotations

import os
import signal
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import Callable

from lubko import supervisor
from lubko.supervise import UnresolvedChild

type TicksMap = dict[int, int | None]

TERMINATE = signal.SIGTERM
KILL = signal.SIGKILL


class FakePinning:
    """Deterministic pidfd/ticks fake driven by an injected ticks table.

    The fake pidfd descriptor is a plain increasing integer. Each delivery is
    recorded as ``(fd, signal_number)`` so tests can assert that both escalation
    steps address the same kernel-pinned process. A call to ``os.kill`` would
    surface as an unexpected delivery, since it is wired to the same recorder.
    """

    def __init__(self) -> None:
        """Start with an empty process table and no recorded deliveries."""
        self.ticks: TicksMap = {}
        self.delivered: list[tuple[int, int]] = []
        self.pins: list[int] = []
        self.next_fd = 100
        self.fail_pin = False
        self.ticks_on_pin: int | None = None
        self.after_term: Callable[[int], None] = lambda _pidfd: None

    def proc_start_ticks(self, pid: int) -> int | None:
        """Return the injected ticks for ``pid`` (``None`` when absent)."""
        return self.ticks.get(pid)

    def open_pidfd(self, pid: int) -> int:
        """Return a fresh fake pin for ``pid``, or fail when pinning is off.

        Raises:
            OSError: When the fake is configured without pidfd support.
        """
        if self.fail_pin:
            msg = "no pidfd support"
            raise OSError(msg)
        if self.ticks_on_pin is not None:
            # Simulate a numeric PID that was recycled between the liveness
            # check and the pin: the pinned instance shows foreign ticks.
            self.ticks[pid] = self.ticks_on_pin
        self.pins.append(pid)
        self.next_fd += 1
        return self.next_fd

    def send_signal(self, pidfd: int, sig: int) -> None:
        """Record one delivery against the pinned descriptor."""
        self.delivered.append((pidfd, sig))
        if sig == TERMINATE:
            self.after_term(pidfd)

    def close(self, _fd: int) -> None:
        """Accept closes without effect."""


def _hold(pid: int = 4242, ticks: int | None = 777) -> UnresolvedChild:
    lifecycle_token = os.urandom(8).hex()
    return UnresolvedChild(pid=pid, start_time_ticks=ticks, token=lifecycle_token, spawned_at=0.0)


def _alive(hold: UnresolvedChild, fake: FakePinning) -> bool:
    current = fake.proc_start_ticks(hold.pid)
    if hold.start_time_ticks is None:
        return current is not None
    return current == hold.start_time_ticks


def _daemon(fake: FakePinning) -> supervisor.SupervisorDaemon:
    """Build a daemon whose exit-await reads the fake's process table.

    Args:
        fake: The shared deterministic pidfd/ticks fake.

    Returns:
        A daemon wired for deterministic convergence checks.
    """
    daemon = supervisor.SupervisorDaemon(supervisor.Settings(stop_grace_seconds=0.03))
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(daemon, "_await_unresolved_exit", lambda hold: not _alive(hold, fake))
    return daemon


@pytest.fixture
def converge(monkeypatch: pytest.MonkeyPatch) -> FakePinning:
    """Wire every module-level primitive the convergence path uses to one fake.

    Returns:
        The shared fake also installed into :mod:`lubko.supervisor`.
    """
    fake = FakePinning()
    monkeypatch.setattr(supervisor, "proc_start_ticks", fake.proc_start_ticks)
    monkeypatch.setattr(supervisor, "_open_unresolved_pidfd", fake.open_pidfd)
    monkeypatch.setattr(supervisor, "_signal_pinned_unresolved", fake.send_signal)
    monkeypatch.setattr(os, "kill", fake.send_signal)
    return fake


def test_recycled_pid_before_term_is_never_signalled(
    converge: FakePinning,
) -> None:
    """A pinned process whose ticks diverge from the record ends the attempt."""
    fake = converge
    fake.ticks[4242] = 777
    fake.ticks_on_pin = 999_999

    converged = _daemon(fake)._converge_unresolved(_hold())

    assert converged is False
    assert fake.delivered == []


def test_reuse_between_term_and_kill_keeps_one_pin_and_no_numeric_signal(
    converge: FakePinning,
) -> None:
    """TERM and KILL escalation reuse one pidfd; bare numeric kill never runs."""
    fake = converge
    fake.ticks[4242] = 777

    # The child keeps matching its recorded identity until the grace expires;
    # KILL then escalates through the very same pinned descriptor.
    converged = _daemon(fake)._converge_unresolved(_hold())

    assert [sig for _, sig in fake.delivered] == [TERMINATE, KILL]
    fds = {fd for fd, _ in fake.delivered}
    assert fds == {101}
    assert fake.pins == [4242]
    assert converged is False


def test_exact_pinned_delivery_converges_after_term(
    converge: FakePinning,
) -> None:
    """A proven matching instance receives TERM via the pin and converges."""
    fake = converge
    fake.ticks[4242] = 777
    fake.after_term = lambda _pidfd: fake.ticks.update({4242: None})

    converged = _daemon(fake)._converge_unresolved(_hold())

    assert converged is True
    assert [(fd, sig) for fd, sig in fake.delivered] == [(101, TERMINATE)]
    assert all(sig != KILL for _, sig in fake.delivered)


def test_unknown_start_ticks_preserves_hold_without_any_signal(
    converge: FakePinning,
) -> None:
    """Unobservable ticks authorize nothing: no pin, no signal, hold kept."""
    fake = converge
    fake.ticks[4242] = 555

    converged = _daemon(fake)._converge_unresolved(_hold(ticks=None))

    assert converged is False
    assert fake.delivered == []
    assert fake.pins == []


def test_unpinnable_process_preserves_the_hold(converge: FakePinning) -> None:
    """When no pin can be opened, nothing is signalled and the hold remains."""
    fake = converge
    fake.fail_pin = True
    fake.ticks[4242] = 777

    converged = _daemon(fake)._converge_unresolved(_hold())

    assert converged is False
    assert fake.delivered == []
