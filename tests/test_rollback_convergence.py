"""Rollback spawns must be converged before any retry can start another."""

from __future__ import annotations

import subprocess
import time

import pytest

from lubko import deployctl as dc
from lubko import lifecycle


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


def pending_state(*, previous_retiring: bool = False) -> dc.RollbackState:
    """Return a live pending deployment state.

    Args:
        previous_retiring: Whether the previous worker's retirement has begun.

    Returns:
        A pending rollback state with distinct old/new commits.
    """

    def worker_meta(commit: str, *, pid: int) -> lifecycle.WorkerMeta:
        return lifecycle.WorkerMeta(
            schema_version=lifecycle.SCHEMA_VERSION,
            state=lifecycle.STATE_RUNNING,
            pid=pid,
            pgid=pid,
            sid=pid,
            start_time_ticks=pid * 10,
            token=f"token-{pid}",
            repo="/workspace/Lubko",
            git_commit=commit,
            worker_id="test-worker",
            log_path="/workspace/worker.log",
            started_at=1.0,
            stopped_at=None,
        )

    old = "1" * 40
    new = "2" * 40
    return dc.RollbackState(
        schema_version=dc.ROLLBACK_SCHEMA_VERSION,
        generation=1,
        status=dc.STATUS_PENDING,
        commit=new,
        previous_commit=old,
        challenge_hash=None,
        deadline=time.time() + 60,
        repo="/workspace/Lubko",
        uv_path="uv",
        stop_grace_seconds=1.0,
        git_timeout_seconds=5.0,
        previous_retiring=previous_retiring,
        previous_meta=worker_meta(old, pid=100),
        new_meta=worker_meta(new, pid=200),
        supervisor_owned=False,
    )


@pytest.fixture
def retiring_state() -> dc.RollbackState:
    """Return a rollback mission whose previous worker is retiring.

    Returns:
        A rollback mission in the ``previous_retiring`` phase, so the
        fresh-spawn replacement path is exercised.
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


def test_unproven_live_child_is_converged_and_retry_stays_possible(
    monkeypatch: pytest.MonkeyPatch,
    retiring_state: dc.RollbackState,
) -> None:
    """A live child whose identity timed out is converged before returning."""
    fake = FakePopen(41001, mode="converges")
    spawned: list[FakePopen] = []
    _install_failing_identity(monkeypatch, fake, spawned)

    restored = dc.restart_previous(retiring_state)

    assert restored is None
    assert spawned == [fake]
    assert fake.signals == ["SIGTERM"]
    assert fake.poll() == -15


def test_already_exited_child_needs_no_convergence(
    monkeypatch: pytest.MonkeyPatch,
    retiring_state: dc.RollbackState,
) -> None:
    """An already-exited child remains an ordinary retryable failure."""
    fake = FakePopen(41002, mode="exited")
    spawned: list[FakePopen] = []
    _install_failing_identity(monkeypatch, fake, spawned)

    restored = dc.restart_previous(retiring_state)

    assert restored is None
    assert fake.signals == []


def test_child_ignoring_sigterm_is_killed_and_reaped(
    monkeypatch: pytest.MonkeyPatch,
    retiring_state: dc.RollbackState,
) -> None:
    """A child that ignores SIGTERM is exact-PID SIGKILLed and reaped."""
    fake = FakePopen(41003, mode="needs_kill")
    spawned: list[FakePopen] = []
    _install_failing_identity(monkeypatch, fake, spawned)

    restored = dc.restart_previous(retiring_state)

    assert restored is None
    assert fake.signals == ["SIGTERM", "SIGKILL"]
    assert fake.poll() == -9


def test_repeated_retries_never_leave_a_live_worker_behind(
    monkeypatch: pytest.MonkeyPatch,
    retiring_state: dc.RollbackState,
) -> None:
    """Repeated watchdog retries converge every spawn before the next one."""
    spawned: list[FakePopen] = []

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

    results = [dc.restart_previous(retiring_state) for _ in range(3)]

    assert results == [None, None, None]
    # Every abandoned spawn was positively converged: no live worker from any
    # earlier retry can coexist with a later replacement.
    assert all(fake.poll() is not None for fake in spawned)
    assert [fake.signals for fake in spawned] == [["SIGTERM"]] * 3
