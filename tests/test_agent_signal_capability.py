"""Agent exact-signal delivery invariants under missing pidfd capability."""

import os
from collections.abc import Callable

import pytest

from lubko import agent

DELIVERY_FAILURES = [
    pytest.param(
        AttributeError,
        id="attribute-error",
    ),
    pytest.param(
        OSError,
        id="os-error",
    ),
]


def _failing_delivery(exc: type[Exception]) -> Callable[[int, int], None]:
    """Build a ``pidfd_send_signal`` stand-in that always fails with ``exc``.

    Returns:
        A callable raising ``exc`` for any pidfd and signal.
    """

    def fail(_pidfd: int, _sig: int) -> None:
        """Mimic an unsupported or denied pidfd delivery by raising ``exc``."""
        message = "pidfd_send_signal unavailable"
        raise exc(message)

    return fail


def _fake_pin(monkeypatch: pytest.MonkeyPatch, fd: int) -> list[int]:
    """Pin every ``open_pidfd`` call to ``fd`` and record closed descriptors.

    Returns:
        The list that receives each descriptor the agent closes.
    """
    monkeypatch.setattr(agent, "open_pidfd", lambda _pid: fd)
    closed: list[int] = []
    real_close = os.close

    def record_close(descriptor: int) -> None:
        closed.append(descriptor)
        real_close(descriptor)

    monkeypatch.setattr(os, "close", record_close)
    return closed


def _verified_identity(
    monkeypatch: pytest.MonkeyPatch,
    *,
    aid: str = "ab12",
    iid: str = "25e6c9b6be0fb773433c28fb74db32a0",
) -> None:
    """Make every identity proof pass for the fake pinned process."""
    monkeypatch.setattr(agent, "proc_start_ticks", lambda _pid: 1234)
    monkeypatch.setattr(agent, "env_has_marker", lambda _pid, marker: marker == aid)
    monkeypatch.setattr(agent, "env_has_invocation", lambda _pid, marker: marker == iid)


_META: agent.Meta = {
    "id": "ab12",
    "pid": 4242,
    "pgid": 4242,
    "start_time": 1234,
    "invocation_id": "25e6c9b6be0fb773433c28fb74db32a0",
}


def _unsupported_delivery(_pidfd: int, _sig: int) -> None:
    """Mimic a libc binding where ``pidfd_send_signal`` does not exist.

    Raises:
        AttributeError: Always, as the real missing binding would.
    """
    message = "module 'libc' has no attribute 'pidfd_send_signal'"
    raise AttributeError(message)


@pytest.mark.parametrize("exc", DELIVERY_FAILURES)
def test_identity_checked_signal_withholds_when_delivery_unsupported(
    monkeypatch: pytest.MonkeyPatch,
    exc: type[Exception],
) -> None:
    """An unsupported pidfd delivery withholds the signal instead of crashing."""
    monkeypatch.setattr(agent, "pidfd_send_signal", _failing_delivery(exc))
    kills: list[tuple[object, int]] = []
    monkeypatch.setattr(os, "kill", lambda pid, sig: kills.append((pid, sig)))
    monkeypatch.setattr(os, "killpg", lambda pgid, sig: kills.append((pgid, sig)))
    r, w = os.pipe()
    real_close = os.close
    try:
        closed = _fake_pin(monkeypatch, r)
        _verified_identity(monkeypatch)
        agent.signal_identity_checked(4242, 1234, 15, marker_aid="ab12")
    finally:
        real_close(w)

    assert closed == [r]
    assert kills == []


@pytest.mark.parametrize("exc", DELIVERY_FAILURES)
def test_group_signal_fails_closed_when_delivery_unsupported(
    monkeypatch: pytest.MonkeyPatch,
    exc: type[Exception],
) -> None:
    """Unsupported group delivery withholds signals from leader and members."""
    monkeypatch.setattr(agent, "pidfd_send_signal", _failing_delivery(exc))
    kills: list[tuple[object, int]] = []
    monkeypatch.setattr(os, "kill", lambda pid, sig: kills.append((pid, sig)))
    monkeypatch.setattr(os, "killpg", lambda pgid, sig: kills.append((pgid, sig)))

    leader_fd, leader_w = os.pipe()
    member_fd, member_w = os.pipe()
    real_close = os.close
    try:
        monkeypatch.setattr(
            agent,
            "_pinned_invocation_members",
            lambda _pgid, _aid, _iid: [(5555, member_fd)],
        )
        closed = _fake_pin(monkeypatch, leader_fd)
        _verified_identity(monkeypatch)
        agent.send_signal_group(_META, 15)
    finally:
        real_close(leader_w)
        real_close(member_w)

    assert sorted(closed) == sorted([leader_fd, member_fd])
    assert kills == []


def test_group_signal_delivers_through_pins_when_supported(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ordinary supported delivery still reaches every verified pinned process."""
    delivered: list[tuple[int, int]] = []

    def deliver(pidfd: int, sig: int) -> None:
        delivered.append((pidfd, sig))

    monkeypatch.setattr(agent, "pidfd_send_signal", deliver)

    leader_fd, leader_w = os.pipe()
    member_fd, member_w = os.pipe()
    closed: list[int] = []
    real_close = os.close

    def record_close(descriptor: int) -> None:
        closed.append(descriptor)
        real_close(descriptor)

    monkeypatch.setattr(os, "close", record_close)
    try:
        monkeypatch.setattr(
            agent,
            "_pinned_invocation_members",
            lambda _pgid, _aid, _iid: [(5555, member_fd)],
        )
        monkeypatch.setattr(agent, "open_pidfd", lambda _pid: leader_fd)
        _verified_identity(monkeypatch)
        agent.send_signal_group(_META, 15)
    finally:
        real_close(leader_w)
        real_close(member_w)

    assert delivered == [(leader_fd, 15), (member_fd, 15)]
