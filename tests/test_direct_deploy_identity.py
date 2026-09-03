"""Direct bootstrap deploys must never forget or misrecord a timed-out child."""

from __future__ import annotations

import signal
import subprocess
import time
from pathlib import Path
from typing import cast

import pytest

from lubko import deployctl as dc
from lubko import lifecycle
from lubko.lifecycle import (
    DeployAbortedError,
    DeployOptions,
    ProcessIdentity,
    WorkerMeta,
)

COMMIT = "a" * 40
PREVIOUS_COMMIT = "b" * 40
PID = 4242
PRIVATE = ProcessIdentity(pid=PID, pgid=PID, sid=PID, start_time_ticks=555)
TRANSITIONED = ProcessIdentity(pid=PID, pgid=1, sid=9000, start_time_ticks=555)
PIN_BASE = 10000


class FakeClock:
    """Deterministic monotonic clock advanced by fake sleeps."""

    def __init__(self, step: float) -> None:
        """Start at fake time zero with one sleep crossing the deadline.

        Args:
            step: Fake time added by each sleep; large enough that exactly
                one sleep moves the clock past any real deadline.
        """
        self.now = 0.0
        self.step = step

    def monotonic(self) -> float:
        """Return the current fake time."""
        return self.now

    def sleep(self, seconds: float) -> None:
        """Advance the fake time instead of blocking.

        Args:
            seconds: How long a real sleep would have blocked (ignored).
        """
        del seconds
        self.now += self.step


class NumericSignalError(AssertionError):
    """Raised when a numeric kill primitive is used instead of a pidfd."""


def _numeric_signal_forbidden() -> None:
    """Fail the test: only pinned ``pidfd_send_signal`` may deliver signals.

    Raises:
        NumericSignalError: Always.
    """
    raise NumericSignalError


def _forbid_os_kill(_pid: int, _sig: int) -> None:
    """Stand-in for ``os.kill`` that must never run."""
    _numeric_signal_forbidden()


def _forbid_popen_terminate(self: object) -> None:
    """Stand-in for the numeric ``Popen`` signal helpers."""
    del self
    _numeric_signal_forbidden()


class FakePopen:
    """Deterministic stand-in for the spawned replacement worker."""

    def __init__(self, pid: int) -> None:
        """Start as a live child with no recorded signals.

        Args:
            pid: Fake process id.
        """
        self.pid = pid
        self.returncode: int | None = None
        self.signals: list[str] = []

    def terminate(self) -> None:
        """Numeric fallback must never be reached."""
        del self
        _numeric_signal_forbidden()

    def kill(self) -> None:
        """Numeric fallback must never be reached."""
        del self
        _numeric_signal_forbidden()

    def poll(self) -> int | None:
        """Return the exit status while the child is still alive."""
        return self.returncode

    def wait(self, timeout: float | None = None) -> int:
        """Reap the child once it has been signalled or given up on its own.

        Args:
            timeout: How long a real ``Popen`` would wait.

        Returns:
            The exit status.

        Raises:
            subprocess.TimeoutExpired: When the child refuses to exit yet.
        """
        if timeout is not None and "SIGTERM" not in self.signals:
            raise subprocess.TimeoutExpired(cmd="worker", timeout=timeout)
        if self.returncode is None:
            self.returncode = -15 if "SIGTERM" in self.signals else -1
        return self.returncode


@pytest.fixture
def forbid_numeric_signals(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fail the test if any numeric kill primitive is used."""
    monkeypatch.setattr("os.kill", _forbid_os_kill)
    monkeypatch.setattr("os.killpg", _forbid_os_kill)


def options() -> DeployOptions:
    """Return minimal direct-spawn deployment options."""
    return DeployOptions(
        repo=Path(),
        uv_path="uv",
        bootstrap=True,
        stop_grace_seconds=0.0,
        postgres_timeout_seconds=1.0,
        lock_timeout_seconds=1.0,
        validation_timeout_seconds=1.0,
        git_timeout_seconds=1.0,
        cli_timeout_seconds=1.0,
    )


def install_spawn(monkeypatch: pytest.MonkeyPatch, fake: FakePopen) -> list[FakePopen]:
    """Make ``spawn_worker`` return ``fake``.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
        fake: The fake child every spawn returns.

    Returns:
        The list receiving every spawned fake child.
    """
    spawned: list[FakePopen] = []

    def spawn(*_args: object, **_kwargs: object) -> FakePopen:
        spawned.append(fake)
        return fake

    monkeypatch.setattr(lifecycle, "spawn_worker", spawn)
    return spawned


def previous_meta() -> WorkerMeta:
    """Return metadata of a live previous worker."""
    return WorkerMeta(
        schema_version=lifecycle.SCHEMA_VERSION,
        state=lifecycle.STATE_RUNNING,
        pid=100,
        pgid=100,
        sid=100,
        start_time_ticks=1000,
        token=f"previous-{PID}",
        repo=".",
        git_commit=PREVIOUS_COMMIT,
        worker_id="previous",
        log_path="worker.log",
        started_at=1.0,
        stopped_at=None,
    )


def install_convergence(
    monkeypatch: pytest.MonkeyPatch,
    fake: FakePopen,
    observed: ProcessIdentity,
    proof: ProcessIdentity | None,
) -> list[tuple[int, str]]:
    """Make pinning, under-pin proof, and pinned signalling deterministic.

    Before the pidfd is opened, ``/proc`` reports ``observed`` (the timeout
    observation); afterwards it reports ``proof``, so an exit-and-reuse of the
    numeric PID between the two operations is expressible.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
        fake: The fake direct child.
        observed: Identity seen while waiting for the private session.
        proof: Occupant identity seen under the pin (or ``None``).

    Returns:
        The ``(pid, signal name)`` pairs delivered through the pin.
    """
    delivered: list[tuple[int, str]] = []
    state = {"pinned": False}

    def open_pin(_pid: int) -> int:
        state["pinned"] = True
        return PIN_BASE + fake.pid

    def send(_pin: int, sig: int) -> None:
        name = signal.Signals(sig).name
        delivered.append((fake.pid, name))
        fake.signals.append(name)
        if name == "SIGTERM":
            fake.returncode = -15

    def close_pin(_pin: int) -> None:
        """Record the pin release without touching real descriptors."""

    monkeypatch.setattr(lifecycle, "_open_exact_pidfd", open_pin)
    monkeypatch.setattr(
        lifecycle, "process_identity", lambda _pid: proof if state["pinned"] else observed
    )
    monkeypatch.setattr(lifecycle, "pidfd_send_signal", send)
    monkeypatch.setattr("os.close", close_pin)
    return delivered


def install_timeout_observation(
    monkeypatch: pytest.MonkeyPatch,
    observed: ProcessIdentity | None,
) -> None:
    """Force ``_wait_for_identity`` to time out observing ``observed``.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
        observed: The last identity ``/proc`` reports for the child.
    """
    monkeypatch.setattr(lifecycle, "SESSION_ESTABLISH_TIMEOUT_SECONDS", 0.0)

    def identity(_pid: int) -> ProcessIdentity | None:
        return observed

    monkeypatch.setattr(lifecycle, "process_identity", identity)


def record_written(monkeypatch: pytest.MonkeyPatch) -> list[WorkerMeta]:
    """Capture every ``write_meta`` call instead of touching real state.

    Args:
        monkeypatch: Pytest monkeypatch fixture.

    Returns:
        The list receiving every written metadata record.
    """
    written: list[WorkerMeta] = []

    def write(meta: WorkerMeta) -> None:
        written.append(meta)

    monkeypatch.setattr(lifecycle, "write_meta", write)
    return written


def test_private_session_child_deploys_with_exact_recorded_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A child that establishes its private session deploys normally."""
    fake = FakePopen(PID)
    install_spawn(monkeypatch, fake)
    install_timeout_observation(monkeypatch, PRIVATE)
    verified: list[WorkerMeta] = []

    def verify(meta: WorkerMeta, _options: DeployOptions) -> bool:
        verified.append(meta)
        return True

    monkeypatch.setattr(lifecycle, "_verify_replacement", verify)
    written = record_written(monkeypatch)

    meta = lifecycle._deploy_direct(options(), None, lifecycle.STATE_UNMANAGED, COMMIT)

    assert (meta.pid, meta.pgid, meta.sid) == (PRIVATE.pid, PRIVATE.pgid, PRIVATE.sid)
    assert meta.start_time_ticks == PRIVATE.start_time_ticks
    assert verified == [meta]
    assert written == [meta]


def test_session_transition_after_timeout_converges_child_and_aborts(
    monkeypatch: pytest.MonkeyPatch,
    forbid_numeric_signals: None,
) -> None:
    """A live child whose PGID/SID transitions after the deadline is converged."""
    del forbid_numeric_signals
    fake = FakePopen(PID)
    install_spawn(monkeypatch, fake)
    install_timeout_observation(monkeypatch, TRANSITIONED)
    delivered = install_convergence(monkeypatch, fake, TRANSITIONED, TRANSITIONED)
    written = record_written(monkeypatch)
    stopped: list[WorkerMeta] = []

    def stop(meta: WorkerMeta, *_args: object, **_kwargs: object) -> bool:
        stopped.append(meta)
        return True

    monkeypatch.setattr(lifecycle, "stop_worker", stop)

    with pytest.raises(DeployAbortedError):
        lifecycle._deploy_direct(options(), previous_meta(), lifecycle.STATE_RUNNING, COMMIT)

    # The unproven child was exactly signalled through its pin and positively
    # reaped; it can neither be forgotten nor coexist with the previous worker.
    assert delivered == [(PID, "SIGTERM")]
    assert fake.poll() == -15
    # The previous worker was never touched and nothing was recorded.
    assert stopped == []
    assert written == []


def test_reused_occupant_after_timeout_is_never_signalled(
    monkeypatch: pytest.MonkeyPatch,
    forbid_numeric_signals: None,
) -> None:
    """A PID reused between the timeout observation and the pin absorbs nothing."""
    del forbid_numeric_signals
    fake = FakePopen(PID)
    install_spawn(monkeypatch, fake)
    install_timeout_observation(monkeypatch, TRANSITIONED)
    recycled = ProcessIdentity(pid=PID, pgid=PID, sid=PID, start_time_ticks=99999)
    delivered = install_convergence(monkeypatch, fake, TRANSITIONED, recycled)
    record_written(monkeypatch)

    with pytest.raises(DeployAbortedError):
        lifecycle._deploy_direct(options(), None, lifecycle.STATE_UNMANAGED, COMMIT)

    assert delivered == []
    # Fail closed: the original direct child is still positively reaped.
    assert fake.poll() is not None


def test_lifecycle_wait_preserves_anchor_through_final_transient_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A transient ``None`` at the deadline does not discard the anchor."""
    clock = FakeClock(step=lifecycle.SESSION_ESTABLISH_TIMEOUT_SECONDS)
    monkeypatch.setattr(time, "monotonic", clock.monotonic)
    monkeypatch.setattr(time, "sleep", clock.sleep)
    observations = iter([TRANSITIONED, None])
    reads: list[ProcessIdentity | None] = []

    def identity(_pid: int) -> ProcessIdentity | None:
        observed = next(observations, None)
        reads.append(observed)
        return observed

    monkeypatch.setattr(lifecycle, "process_identity", identity)

    assert lifecycle._wait_for_identity(PID) == TRANSITIONED
    # Poll 1 observed the anchor before the deadline; poll 2 returned the
    # transient None at/after it. Both polls really happened.
    assert reads == [TRANSITIONED, None]
    assert clock.now >= lifecycle.SESSION_ESTABLISH_TIMEOUT_SECONDS


def test_deployctl_wait_preserves_anchor_through_final_transient_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The rollback candidate waiter keeps its anchor across a final ``None``."""

    class LiveProc:
        """Direct child that stays live for the whole wait."""

        def __init__(self) -> None:
            self.pid = PID

        def poll(self) -> int | None:
            """Report the child as continuously live.

            Returns:
                Always ``None``: not exited.
            """
            del self
            return None

    clock = FakeClock(step=lifecycle.SESSION_ESTABLISH_TIMEOUT_SECONDS)
    monkeypatch.setattr(time, "monotonic", clock.monotonic)
    monkeypatch.setattr(time, "sleep", clock.sleep)
    observations = iter([TRANSITIONED, None])
    reads: list[ProcessIdentity | None] = []

    def identity(_pid: int) -> ProcessIdentity | None:
        observed = next(observations, None)
        reads.append(observed)
        return observed

    monkeypatch.setattr(dc, "process_identity", identity)

    assert dc._wait_for_identity(cast("subprocess.Popen[bytes]", LiveProc())) == TRANSITIONED
    # Poll 1 observed the anchor before the deadline; poll 2 returned the
    # transient None at/after it. Both polls really happened.
    assert reads == [TRANSITIONED, None]
    assert clock.now >= dc.IDENTITY_TIMEOUT_SECONDS


def test_bootstrap_allows_genuine_metadata_absence(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Fresh bootstrap may proceed only when maintained metadata is absent."""
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
    monkeypatch.setattr(lifecycle, "_supervised_mutation_blocker", lambda: None)
    monkeypatch.setattr(lifecycle, "_validate_and_prepare", lambda _options: COMMIT)
    observed: list[tuple[WorkerMeta | None, str]] = []

    def complete(
        _options: DeployOptions,
        _commit: str,
        previous: WorkerMeta | None,
        state: str,
    ) -> int:
        observed.append((previous, state))
        return lifecycle.EXIT_OK

    monkeypatch.setattr(lifecycle, "_complete_deploy_handoff", complete)

    assert lifecycle._deploy_locked(options()) == lifecycle.EXIT_OK
    assert observed == [(None, lifecycle.STATE_UNMANAGED)]


@pytest.mark.parametrize("contents", ["{", "[]", "{}"])
def test_bootstrap_refuses_untrustworthy_maintained_metadata(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    contents: str,
) -> None:
    """Present malformed authority must block bootstrap before preparation."""
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
    path = lifecycle.meta_path()
    path.parent.mkdir(parents=True)
    path.write_text(contents)
    prepared: list[bool] = []

    def prepare(_options: DeployOptions) -> str:
        prepared.append(True)
        return COMMIT

    monkeypatch.setattr(lifecycle, "_validate_and_prepare", prepare)

    with pytest.raises(DeployAbortedError):
        lifecycle._deploy_locked(options())

    assert prepared == []
    assert path.read_text() == contents
