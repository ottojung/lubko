"""Regression tests for legacy rollback live-child convergence (#182)."""

from __future__ import annotations

import subprocess

import pytest

from lubko import deployctl as dc
from tests.test_deployctl import pending_state


class FakePopen:
    """Deterministic stand-in for the spawned previous-worker ``Popen``."""

    def __init__(self, pid: int, *, mode: str) -> None:
        """Record the fake behaviour mode.

        Args:
            pid: Fake process id.
            mode: One of ``converges``, ``needs_kill``, or ``exited``.
        """
        self.pid = pid
        self.mode = mode
        self.returncode: int | None = 0 if mode == "exited" else None
        self.signals: list[str] = []

    def terminate(self) -> None:
        """Record a SIGTERM; only a converging child then exits."""
        self.signals.append("SIGTERM")
        if self.mode == "converges":
            self.returncode = -15

    def kill(self) -> None:
        """Record a SIGKILL; every covered non-exited child then exits."""
        self.signals.append("SIGKILL")
        if self.mode != "exited":
            self.returncode = -9

    def poll(self) -> int | None:
        """Return the exit status while the child is still alive."""
        return self.returncode

    def wait(self, timeout: float | None = None) -> int:
        """Reap the child unless it has not yet been signalled enough.

        Args:
            timeout: How long a real ``Popen`` would wait.

        Returns:
            The exit status.

        Raises:
            subprocess.TimeoutExpired: When the child refuses to exit yet.
        """
        expired: float = timeout if timeout is not None else 0.0
        if self.mode == "converges" and "SIGTERM" not in self.signals:
            raise subprocess.TimeoutExpired(cmd="worker", timeout=expired)
        if self.mode == "needs_kill" and "SIGKILL" not in self.signals:
            raise subprocess.TimeoutExpired(cmd="worker", timeout=expired)
        assert self.returncode is not None
        return self.returncode


@pytest.fixture
def identity_timeout_state() -> dc.RollbackState:
    """Return a pending rollback mission whose previous worker is retiring.

    Returns:
        A rollback mission in the ``previous_retiring`` phase, so
        ``_restart_previous`` takes the fresh-spawn path.
    """
    return pending_state(previous_retiring=True)


def _install_failing_identity(
    monkeypatch: pytest.MonkeyPatch,
    fake: FakePopen,
    spawned: list[FakePopen],
) -> None:
    """Force the spawn path to produce ``fake`` and never prove its identity.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
        fake: The fake child every spawn returns.
        spawned: List receiving every spawned fake child.
    """
    monkeypatch.setattr(dc, "worker_alive", lambda _meta: False)
    monkeypatch.setattr(dc, "_wait_for_identity", lambda _proc: None)

    def fake_spawn(
        *_args: object,
        **_kwargs: object,
    ) -> FakePopen:
        spawned.append(fake)
        return fake

    monkeypatch.setattr(dc, "spawn_worker", fake_spawn)


def test_identity_timeout_converges_live_child_and_stays_retryable(
    monkeypatch: pytest.MonkeyPatch,
    identity_timeout_state: dc.RollbackState,
) -> None:
    """A live child whose identity timed out is converged before returning."""
    fake = FakePopen(41001, mode="converges")
    spawned: list[FakePopen] = []
    _install_failing_identity(monkeypatch, fake, spawned)

    restored = dc._restart_previous(identity_timeout_state)  # ruff: ignore[private-member-access]

    assert restored is None
    assert spawned == [fake]
    assert fake.signals == ["SIGTERM"]
    assert fake.poll() == -15


def test_identity_timeout_already_exited_child_is_retryable(
    monkeypatch: pytest.MonkeyPatch,
    identity_timeout_state: dc.RollbackState,
) -> None:
    """An already-exited child remains an ordinary retryable failure."""
    fake = FakePopen(41002, mode="exited")
    spawned: list[FakePopen] = []
    _install_failing_identity(monkeypatch, fake, spawned)

    restored = dc._restart_previous(identity_timeout_state)  # ruff: ignore[private-member-access]

    assert restored is None
    assert fake.signals == []


def test_identity_timeout_child_ignoring_sigterm_is_killed_and_reaped(
    monkeypatch: pytest.MonkeyPatch,
    identity_timeout_state: dc.RollbackState,
) -> None:
    """A child that ignores SIGTERM is exact-PID SIGKILLed and reaped."""
    fake = FakePopen(41003, mode="needs_kill")
    spawned: list[FakePopen] = []
    _install_failing_identity(monkeypatch, fake, spawned)

    restored = dc._restart_previous(identity_timeout_state)  # ruff: ignore[private-member-access]

    assert restored is None
    assert fake.signals == ["SIGTERM", "SIGKILL"]
    assert fake.poll() == -9


def test_repeated_rollback_retries_never_leave_a_live_worker_behind(
    monkeypatch: pytest.MonkeyPatch,
    identity_timeout_state: dc.RollbackState,
) -> None:
    """Repeated watchdog retries converge every spawn before the next one."""
    spawned: list[FakePopen] = []
    attempts = iter(range(3))

    def counting_spawn(
        *_args: object,
        **_kwargs: object,
    ) -> FakePopen:
        fake = FakePopen(41004 + len(spawned), mode="converges")
        spawned.append(fake)
        return fake

    monkeypatch.setattr(dc, "worker_alive", lambda _meta: False)
    monkeypatch.setattr(dc, "_wait_for_identity", lambda _proc: None)
    monkeypatch.setattr(dc, "spawn_worker", counting_spawn)

    results = [
        dc._restart_previous(identity_timeout_state)  # ruff: ignore[private-member-access]
        for _ in attempts
    ]

    assert results == [None, None, None]
    # Every abandoned spawn was positively converged: no live worker from any
    # earlier retry can coexist with a later replacement.
    assert all(fake.poll() is not None for fake in spawned)
    assert [fake.signals for fake in spawned] == [["SIGTERM"]] * 3
