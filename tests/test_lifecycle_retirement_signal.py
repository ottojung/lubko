"""Lifecycle retirement signalling invariants.

A holding pidfd does NOT reserve a numeric PID/PGID: the kernel frees the
numeric ID before the pinned reference is released. Every retirement signal
must therefore be delivered through a per-member pidfd to a process re-proven
(group membership + lifecycle token) under its own pin, so a recycled numeric
identity can never absorb a signal meant for the retiring worker.
"""

import os
import signal
import time
from typing import Final

import pytest

from lubko import lifecycle
from lubko.lifecycle import SCHEMA_VERSION, ProcessIdentity, WorkerMeta

LIVE: Final = ProcessIdentity(pid=42, pgid=42, sid=7, start_time_ticks=1234)
RECYCLED: Final = ProcessIdentity(pid=42, pgid=42, sid=7, start_time_ticks=9999)
CHILD: Final = 4242


class FakeClock:
    """Deterministic monotonic clock advanced by ``sleep``."""

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
        #: Delivered signals as ``(pid, sig)`` pairs, resolved from pidfds.
        self.sends: list[tuple[int, int]] = []
        self.closes = 0
        #: Candidate group members returned by each live snapshot.
        self.members: list[int] = []
        #: Per-pid process groups observed under the pins.
        self.pgrps: dict[int, int] = {}
        #: Per-pid lifecycle-token ownership observed under the pins.
        self.tokened: dict[int, bool] = {}
        #: Pids that cannot be pinned (exited, or no pin capability).
        self.unpinnable: set[int] = set()
        #: Observed identity of the recorded worker PID over time.
        self.identity: ProcessIdentity | None = LIVE
        #: Whether a recorded token exists to prove ownership with.
        self.has_recorded_token = True

        monkeypatch.setattr(time, "monotonic", self.clock.monotonic)
        monkeypatch.setattr(time, "sleep", self.clock.sleep)

        monkeypatch.setattr(
            lifecycle,
            "process_identity",
            lambda _pid: self.identity,
        )
        monkeypatch.setattr(
            lifecycle,
            "process_has_token",
            lambda pid, _token: self.tokened.get(pid, False),
        )
        monkeypatch.setattr(lifecycle, "_live_group_member_pids", lambda _pgid: list(self.members))

        def member_pgrp(pid: int) -> int | None:
            return self.pgrps.get(pid)

        monkeypatch.setattr(lifecycle, "process_pgrp", member_pgrp)

        def open_pin(pid: int) -> int:
            if pid in self.unpinnable:
                message = "no such process"
                raise OSError(message)
            return 10000 + pid

        def send(fd: int, sig: int) -> None:
            self.sends.append((fd - 10000, sig))

        def close(_fd: int) -> None:
            self.closes += 1

        monkeypatch.setattr(lifecycle, "_open_exact_pidfd", open_pin)
        monkeypatch.setattr(lifecycle, "pidfd_send_signal", send)
        monkeypatch.setattr(os, "close", close)
        monkeypatch.setattr(lifecycle, "drain_sentinel_matches", lambda _token: False)

        def absence_proven(_pid: int, ticks: int | None) -> bool:
            identity = self.identity
            return identity is None or identity.start_time_ticks != ticks

        monkeypatch.setattr(lifecycle, "process_absence_proven", absence_proven)


def run(h: Harness) -> bool:
    """Run ``stop_worker`` with zero grace windows.

    Args:
        h: The injected retirement world (fakes already installed).

    Returns:
        The retirement outcome reported by ``stop_worker``.
    """
    del h
    return lifecycle.stop_worker(meta(), 0.0, cancel_grace_seconds=0.0)


def meta(**overrides: object) -> WorkerMeta:
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
    return WorkerMeta.from_dict({**defaults, **overrides})


@pytest.fixture
def h(monkeypatch: pytest.MonkeyPatch) -> Harness:
    """Provide one deterministic retirement world per test.

    Returns:
        A harness with fakes installed over the lifecycle module.
    """
    return Harness(monkeypatch)


def test_recycled_leader_absorbs_no_term(h: Harness) -> None:
    """A replacement occupant of the recorded identity proves unowned and is never signalled."""
    h.members = [LIVE.pid]
    h.pgrps = {LIVE.pid: LIVE.pgid}
    h.tokened = {LIVE.pid: False}  # the recycled occupant carries no token
    h.identity = RECYCLED

    assert run(h) is True
    assert h.sends == []


def test_unprovable_member_is_skipped_but_exact_members_are_signalled(h: Harness) -> None:
    """Per-member proof isolates a recycled numeric PID from genuine members."""
    h.members = [CHILD, 7777]
    h.pgrps = {LIVE.pid: LIVE.pgid, CHILD: LIVE.pgid, 7777: LIVE.pgid}
    h.tokened = {LIVE.pid: True, CHILD: True, 7777: False}  # 7777 is recycled

    # The wedged exact leader itself survives, so retirement is not claimed
    # and escalation fires; the invariant under test is that the recycled
    # replacement never receives anything while the exact member gets both.
    assert run(h) is False
    assert sorted(h.sends) == [
        (CHILD, signal.SIGKILL),
        (CHILD, signal.SIGTERM),
    ]


def test_member_exit_between_snapshot_and_delivery_is_benign(h: Harness) -> None:
    """A member that exits after the snapshot simply receives nothing."""
    h.members = [CHILD, CHILD + 1]
    h.pgrps = {LIVE.pid: LIVE.pgid, CHILD: LIVE.pgid}
    h.tokened = {LIVE.pid: True, CHILD: True}
    h.unpinnable = {CHILD + 1}

    # The wedged exact leader itself survives; the exited member is benignly
    # skipped during both passes and never signalled.
    assert run(h) is False
    assert sorted(h.sends) == [
        (CHILD, signal.SIGKILL),
        (CHILD, signal.SIGTERM),
    ]


def test_wedged_exact_worker_receives_sigkill_escalation(
    h: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A wedged exact worker is escalated to SIGKILL and then reaps."""
    h.members = [LIVE.pid]
    h.pgrps = {LIVE.pid: LIVE.pgid}
    h.tokened = {LIVE.pid: True}

    def gone_after_kill(_pid: int) -> ProcessIdentity | None:
        return None if any(sig == signal.SIGKILL for _, sig in h.sends) else LIVE

    monkeypatch.setattr(lifecycle, "process_identity", gone_after_kill)
    monkeypatch.setattr(
        lifecycle,
        "process_absence_proven",
        lambda _pid, _ticks: any(sig == signal.SIGKILL for _, sig in h.sends),
    )

    assert run(h) is True
    assert h.sends == [(LIVE.pid, signal.SIGTERM), (LIVE.pid, signal.SIGKILL)]


def test_escalation_never_hits_recycled_replacement(h: Harness) -> None:
    """Between SIGTERM and SIGKILL a recycled member is re-proofed out.

    The worker itself survives (the wedged instance keeps running), but the
    replacement occupying pid 7777 fails its token re-proof under the pin and
    receives neither signal.
    """
    h.members = [LIVE.pid, 7777]
    h.pgrps = {LIVE.pid: LIVE.pgid, 7777: LIVE.pgid}
    h.tokened = {LIVE.pid: True, 7777: False}

    assert run(h) is False
    assert sorted(h.sends) == [
        (LIVE.pid, signal.SIGKILL),
        (LIVE.pid, signal.SIGTERM),
    ]


def test_exact_worker_that_exits_after_term_retires_with_drain_only(
    h: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An exact worker exiting after SIGTERM retires without escalation."""
    h.members = [LIVE.pid]
    h.pgrps = {LIVE.pid: LIVE.pgid}
    h.tokened = {LIVE.pid: True}

    def gone_after_term(_pid: int) -> ProcessIdentity | None:
        return None if any(sig == signal.SIGTERM for _, sig in h.sends) else LIVE

    monkeypatch.setattr(lifecycle, "process_identity", gone_after_term)
    monkeypatch.setattr(
        lifecycle,
        "process_absence_proven",
        lambda _pid, _ticks: any(sig == signal.SIGTERM for _, sig in h.sends),
    )

    assert run(h) is True
    assert h.sends == [(LIVE.pid, signal.SIGTERM)]


def test_missing_lifecycle_token_fails_closed(h: Harness) -> None:
    """Token ownership is retained: an unowned live instance is not signalled."""
    h.members = [LIVE.pid]
    h.pgrps = {LIVE.pid: LIVE.pgid}
    h.has_recorded_token = False

    assert lifecycle.stop_worker(meta(token=None), 0.0, cancel_grace_seconds=0.0) is False
    assert h.sends == []


def test_worker_gone_before_proof_reports_retired_without_signalling(h: Harness) -> None:
    """An already-gone worker retires successfully with nothing emitted."""
    h.identity = None
    h.members = [LIVE.pid]
    h.unpinnable.add(LIVE.pid)

    assert run(h) is True
    assert h.sends == []


def test_live_worker_without_pin_capability_fails_closed(h: Harness) -> None:
    """No pin capability means no signal may ever be delivered."""
    h.members = [LIVE.pid]
    h.unpinnable.update({LIVE.pid, CHILD})

    assert run(h) is False
    assert h.sends == []


def test_pins_are_released_after_retirement(h: Harness) -> None:
    """Every opened pin descriptor is closed again."""
    h.members = [LIVE.pid, CHILD]
    h.pgrps = {LIVE.pid: LIVE.pgid, CHILD: LIVE.pgid}
    h.tokened = {LIVE.pid: True, CHILD: True}

    run(h)
    # one leader pin + one per signalled member (TERM), plus escalation pass
    assert h.closes >= 3


def test_unpinnable_leader_with_unknown_absence_fails_closed(
    h: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unknown process liveness after pidfd failure must not count as retirement."""
    h.unpinnable = {LIVE.pid}
    calls: list[tuple[int, int | None]] = []

    def absence(pid: int, ticks: int | None) -> bool:
        calls.append((pid, ticks))
        return False

    monkeypatch.setattr(lifecycle, "process_absence_proven", absence)

    assert run(h) is False
    assert calls == [(LIVE.pid, LIVE.start_time_ticks)]
    assert h.sends == []


def test_pinned_leader_with_unknown_identity_fails_closed(
    h: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A successful pidfd pin does not turn an unreadable identity into absence."""
    h.identity = None
    calls: list[tuple[int, int | None]] = []

    def absence(pid: int, ticks: int | None) -> bool:
        calls.append((pid, ticks))
        return False

    monkeypatch.setattr(lifecycle, "process_absence_proven", absence)

    assert run(h) is False
    assert calls == [(LIVE.pid, LIVE.start_time_ticks)]
    assert h.sends == []


def test_pinned_leader_with_proven_absence_succeeds(
    h: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Positive absence proof after a successful pin permits retirement."""
    h.identity = None
    monkeypatch.setattr(lifecycle, "process_absence_proven", lambda _pid, _ticks: True)

    assert run(h) is True
    assert h.sends == []


def test_unpinnable_leader_with_proven_absence_succeeds(
    h: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Conclusive absence or PID reuse after pidfd failure counts as retirement."""
    h.unpinnable = {LIVE.pid}
    monkeypatch.setattr(
        lifecycle,
        "process_absence_proven",
        lambda pid, ticks: (pid, ticks) == (LIVE.pid, LIVE.start_time_ticks),
    )

    assert run(h) is True
    assert h.sends == []


def test_unreadable_retirement_liveness_never_authorizes_handoff(
    h: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Unreadable identity after signalling stays unknown through escalation."""
    h.members = [LIVE.pid]
    h.pgrps = {LIVE.pid: LIVE.pgid}
    h.tokened = {LIVE.pid: True}

    def unreadable_after_term(_pid: int) -> ProcessIdentity | None:
        return None if any(sig == signal.SIGTERM for _, sig in h.sends) else LIVE

    monkeypatch.setattr(lifecycle, "process_identity", unreadable_after_term)
    monkeypatch.setattr(lifecycle, "process_absence_proven", lambda _pid, _ticks: False)

    assert run(h) is False
    assert h.sends == [(LIVE.pid, signal.SIGTERM), (LIVE.pid, signal.SIGKILL)]


@pytest.mark.parametrize(
    "observed",
    [
        ProcessIdentity(
            pid=LIVE.pid, pgid=LIVE.pgid + 1, sid=LIVE.sid, start_time_ticks=LIVE.start_time_ticks
        ),
        ProcessIdentity(
            pid=LIVE.pid, pgid=LIVE.pgid, sid=LIVE.sid + 1, start_time_ticks=LIVE.start_time_ticks
        ),
    ],
)
def test_same_incarnation_identity_disagreement_fails_closed(
    observed: ProcessIdentity, monkeypatch: pytest.MonkeyPatch
) -> None:
    """PGID/SID disagreement cannot prove a still-present PID incarnation retired."""
    monkeypatch.setattr(lifecycle, "process_identity", lambda _pid: observed)

    assert lifecycle._worker_retirement_state(meta()) is None
