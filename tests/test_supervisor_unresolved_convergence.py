"""Deterministic invariants for converging an unresolved worker hold.

Signals must only ever be delivered through a pinned pidfd to a process that
still provably matches the recorded start-time ticks, and the same pin must be
retained across TERM and KILL escalation so a recycled numeric PID can never
be signalled.
"""

from __future__ import annotations

import errno
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import Callable

from lubko import supervise, supervisor
from lubko.supervise import UnresolvedChild, proc_start_ticks, read_state

type TicksMap = dict[int, int | None]

TERMINATE = signal.SIGTERM
KILL = signal.SIGKILL


class FakePinning:
    """Deterministic pidfd/ticks fake driven by an injected ticks table.

    The fake pidfd descriptor is a plain increasing integer. Each delivery is
    recorded as ``(fd, signal_number)`` so tests can assert that both escalation
    steps address the same kernel-pinned process. A call to ``os.kill`` would
    surface as an unexpected delivery addressed to the numeric PID, since it is
    wired to the same recorder.

    After a TERM delivery the process table can be scripted: the first
    ``alive_reads_after_term`` observations still report old process A with its
    recorded ticks; after the last of those observations A exits and the numeric
    PID is recycled to an unrelated process B whose ticks differ.
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
        self.alive_reads_after_term = 0
        self.reuse_ticks: int | None = None
        self.kill_esrch = False
        self._term_delivered = False
        self._reads_since_term = 0
        self._a_exited = False

    def proc_start_ticks(self, pid: int) -> int | None:
        """Return the injected ticks for ``pid`` (``None`` when absent)."""
        if self._term_delivered and pid in self.ticks:
            self._reads_since_term += 1
            if self._reads_since_term <= self.alive_reads_after_term:
                if self._reads_since_term == self.alive_reads_after_term:
                    # Reuse happens right AFTER this final grace observation:
                    # old A exits and B takes over the numeric PID.
                    self._a_exited = True
                return self.ticks.get(pid)
            self._a_exited = True
            return self.reuse_ticks
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
        """Record one delivery against its addressed target.

        A pidfd delivery goes only to the kernel-pinned process; KILL against a
        pin whose process has already exited raises ``ESRCH``. A numeric
        ``os.kill`` delivery would instead be addressed to whoever currently
        owns the number — including recycled B — which tests can detect.

        Raises:
            OSError: When KILL targets a pin whose process already exited.
        """
        self.delivered.append((pidfd, sig))
        if sig == TERMINATE:
            self._term_delivered = True
            self._reads_since_term = 0
            self.after_term(pidfd)
        elif sig == KILL and self._a_exited:
            self.kill_esrch = True
            msg = "pinned process already exited"
            raise OSError(errno.ESRCH, msg)

    @property
    def a_exited(self) -> bool:
        """Whether old process A has exited in the current script."""
        return self._a_exited

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
    """KILL at the escalation boundary hits only the pinned A, never recycled B.

    The first post-TERM grace observation still reports old process A alive
    with its recorded ticks. Immediately after that final observation — and
    before KILL is delivered — A exits and the numeric PID 4242 is recycled to
    an unrelated process B. The KILL must still be addressed through the pin
    taken before the reuse (failing with ESRCH because A is already dead), and
    B must receive no signal of any kind.
    """
    fake = converge
    fake.ticks[4242] = 777
    fake.alive_reads_after_term = 1
    fake.reuse_ticks = 888_888

    # A provably survives TERM, then exits and its numeric PID is recycled by
    # B before the KILL escalation fires.
    converged = _daemon(fake)._converge_unresolved(_hold())

    assert [sig for _, sig in fake.delivered] == [TERMINATE, KILL]
    assert all(fd == 101 for fd, _ in fake.delivered), "both signals used one pidfd"
    assert all(fd != 4242 for fd, _ in fake.delivered), "no numeric kill reached recycled B"
    assert fake.pins == [4242], "the single pin predates the reuse"
    assert fake.a_exited, "A was modelled as exited before KILL"
    assert fake.kill_esrch, "pinned KILL surfaced ESRCH against dead A"
    assert converged is True, "recycled B ends the hold without being signalled"


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


# ---------------------------------------------------------------------------
# Owned direct-child zombie reaping
# ---------------------------------------------------------------------------


def _spawn_blocking_child() -> subprocess.Popen[bytes]:
    """Spawn a real child that stays alive until killed.

    Returns:
        The live ``Popen`` child of this test process.
    """
    return subprocess.Popen(
        [sys.executable, "-c", "import sys; sys.stdin.read()"],
        stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def _wait_zombie(pid: int) -> None:
    """Busy-wait without sleeping until ``pid`` is an unreaped zombie.

    Args:
        pid: The PID to observe.

    Raises:
        AssertionError: When ``pid`` never reaches zombie state.
    """
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        try:
            stat = (Path("/proc") / str(pid) / "stat").read_bytes()
        except OSError:
            continue
        close_paren = stat.rfind(b")")
        if close_paren != -1 and stat[close_paren + 2 :].split()[0] == b"Z":
            return
    msg = "child never became a zombie"
    raise AssertionError(msg)


def _owned_daemon(proc: subprocess.Popen[bytes]) -> supervisor.SupervisorDaemon:
    """Build a daemon owning ``proc`` as its direct child.

    Args:
        proc: The direct ``Popen`` child to assign to the daemon.

    Returns:
        A daemon whose ``self.proc`` is exactly ``proc``.
    """
    daemon = supervisor.SupervisorDaemon(supervisor.Settings(stop_grace_seconds=5.0))
    daemon.proc = proc
    return daemon


def test_owned_zombie_child_is_reaped_and_the_hold_clears() -> None:
    """An exited-but-unreaped owned direct child converges instead of blocking.

    After the pinned TERM/KILL kills our own direct Popen child, the child
    remains visible in /proc with its original start ticks until it is
    reaped. Liveness for such an owned hold must be decided by reaping, so
    convergence succeeds immediately rather than treating the zombie as live
    forever.
    """
    proc = _spawn_blocking_child()
    try:
        ticks = proc_start_ticks(proc.pid)
        assert ticks is not None
        os.kill(proc.pid, signal.SIGKILL)
        _wait_zombie(proc.pid)
        # The unreaped zombie still reports its exact recorded start ticks.
        assert proc_start_ticks(proc.pid) == ticks

        converged = _owned_daemon(proc)._converge_unresolved(_hold(pid=proc.pid, ticks=ticks))

        assert converged is True
        assert proc.poll() is not None, "the zombie was positively reaped"
    finally:
        proc.poll()


def test_owned_live_child_converges_through_pinned_term(monkeypatch: pytest.MonkeyPatch) -> None:
    """A live owned child receives pinned TERM and its exit is positively reaped."""
    monkeypatch.setattr(time, "sleep", lambda _seconds: None)
    proc = _spawn_blocking_child()
    try:
        ticks = proc_start_ticks(proc.pid)
        assert ticks is not None

        converged = _owned_daemon(proc)._converge_unresolved(_hold(pid=proc.pid, ticks=ticks))

        assert converged is True
        assert proc.poll() is not None, "the exited child was reaped"
    finally:
        proc.poll()


def test_foreign_zombie_is_never_treated_as_owned(monkeypatch: pytest.MonkeyPatch) -> None:
    """A zombie that is not our still-owned direct child never clears a hold."""
    monkeypatch.setattr(time, "sleep", lambda _seconds: None)
    proc = _spawn_blocking_child()
    try:
        ticks = proc_start_ticks(proc.pid)
        assert ticks is not None
        os.kill(proc.pid, signal.SIGKILL)
        _wait_zombie(proc.pid)

        daemon = supervisor.SupervisorDaemon(supervisor.Settings(stop_grace_seconds=0.05))
        daemon.proc = None
        converged = daemon._converge_unresolved(_hold(pid=proc.pid, ticks=ticks))

        assert converged is False
    finally:
        proc.poll()


def _state_with_hold(hold: UnresolvedChild) -> supervise.SupervisorState:
    """Build a minimal durable state carrying only ``hold``.

    Args:
        hold: The unresolved-child hold to persist.

    Returns:
        A valid durable supervisor state.
    """
    return supervise.SupervisorState(
        schema_version=supervise.SCHEMA_VERSION,
        applied_generation=0,
        mode=supervise.MODE_RUN,
        commit=None,
        child=None,
        unresolved_child=hold,
        ownership_hold_malformed=False,
        unresolved_hold_malformed=False,
        intent=supervise.INTENT_RUN,
        restart_count=0,
        next_attempt_at=None,
        last_exit=None,
        last_spawn_at=None,
        ready=False,
        next_readiness_at=None,
        boot_id=None,
    )


def test_resolve_unresolved_child_clears_the_durable_hold(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Converging an owned zombie clears the persisted hold, not just in memory.

    The zombie is a real unreaped direct child of the test process, so the
    full resolution path — convergence by kernel-proven ownership followed by
    a durable state write — runs without any sleeping.
    """
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state"))
    monkeypatch.setattr(time, "sleep", lambda _seconds: None)
    proc = _spawn_blocking_child()
    try:
        ticks = proc_start_ticks(proc.pid)
        assert ticks is not None
        os.kill(proc.pid, signal.SIGKILL)
        _wait_zombie(proc.pid)
        hold = _hold(pid=proc.pid, ticks=ticks)
        supervise.write_state(_state_with_hold(hold))

        daemon = supervisor.SupervisorDaemon(supervisor.Settings(stop_grace_seconds=5.0))
        daemon.proc = proc

        assert daemon._resolve_unresolved_child() is True
        assert read_state().unresolved_child is None, "the durable hold was cleared"
        assert proc.poll() is not None, "the zombie was positively reaped"
    finally:
        proc.poll()
