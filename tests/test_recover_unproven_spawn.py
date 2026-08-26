"""The recover path must never drop a live unproven child it spawned."""

from __future__ import annotations

import signal
import subprocess
from pathlib import Path

import pytest

from lubko import lifecycle
from lubko.lifecycle import DeployOptions, ProcessIdentity

COMMIT = "c" * 40
PID = 4242
PRIVATE = ProcessIdentity(pid=PID, pgid=PID, sid=PID, start_time_ticks=555)
NON_PRIVATE = ProcessIdentity(pid=PID, pgid=1, sid=9000, start_time_ticks=555)
PIN_BASE = 20000


class NumericSignalError(AssertionError):
    """Raised when a numeric kill primitive is used instead of a pidfd."""


def _numeric_signal_forbidden() -> None:
    """Fail the test: only pinned ``pidfd_send_signal`` may deliver signals.

    Raises:
        NumericSignalError: Always.
    """
    raise NumericSignalError


class FakePopen:
    """Deterministic stand-in for the spawned recovery worker."""

    def __init__(self, pid: int) -> None:
        """Start as a live child with no recorded signals.

        Args:
            pid: Fake process id.
        """
        self.pid = pid
        self.returncode: int | None = None
        self.signals: list[str] = []

    def poll(self) -> int | None:
        """Return the exit status while the child is still alive."""
        return self.returncode

    def terminate(self) -> None:
        """Numeric fallback that must never be reached."""
        del self
        _numeric_signal_forbidden()

    def kill(self) -> None:
        """Numeric fallback that must never be reached."""
        del self
        _numeric_signal_forbidden()

    def wait(self, timeout: float | None = None) -> int:
        """Reap the child once it has been signalled or reaped without one.

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


def options() -> DeployOptions:
    """Return minimal recover deployment options.

    Returns:
        Deterministic deployment inputs.
    """
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


@pytest.fixture
def recover_env(monkeypatch: pytest.MonkeyPatch) -> list[FakePopen]:
    """Stub preflight, logging, and spawning with deterministic fakes.

    Args:
        monkeypatch: Pytest monkeypatch fixture.

    Returns:
        The list receiving every spawned fake child.
    """
    monkeypatch.setattr(lifecycle, "_recover_preflight", lambda _options: COMMIT)
    monkeypatch.setattr(lifecycle, "append_deploy_log", lambda _line: None)
    spawned: list[FakePopen] = []

    def spawn(*_args: object, **_kwargs: object) -> FakePopen:
        """Return a fresh fake live child."""
        fake = FakePopen(PID)
        spawned.append(fake)
        return fake

    monkeypatch.setattr(lifecycle, "spawn_worker", spawn)
    monkeypatch.setattr("os.kill", lambda *_a: _numeric_signal_forbidden())
    monkeypatch.setattr("os.killpg", lambda *_a: _numeric_signal_forbidden())
    return spawned


def observe(monkeypatch: pytest.MonkeyPatch, observed: ProcessIdentity | None) -> None:
    """Make ``_wait_for_identity`` time out observing ``observed``.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
        observed: The last identity ``/proc`` reports for the child.
    """
    monkeypatch.setattr(lifecycle, "SESSION_ESTABLISH_TIMEOUT_SECONDS", 0.0)
    monkeypatch.setattr(lifecycle, "process_identity", lambda _pid: observed)


def install_convergence(
    monkeypatch: pytest.MonkeyPatch,
    fake: FakePopen,
    proof: ProcessIdentity | None,
) -> list[tuple[int, str]]:
    """Make pidfd pinning and pinned signalling deterministic for ``fake``.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
        fake: The fake direct child being converged.
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

    def identity(_pid: int) -> ProcessIdentity | None:
        return proof if state["pinned"] else NON_PRIVATE

    monkeypatch.setattr(lifecycle, "_open_exact_pidfd", open_pin)
    monkeypatch.setattr(lifecycle, "process_identity", identity)
    monkeypatch.setattr(lifecycle, "pidfd_send_signal", send)
    monkeypatch.setattr("os.close", lambda _fd: None)
    return delivered


def test_recover_converges_live_child_when_identity_is_none(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    recover_env: list[FakePopen],
) -> None:
    """A live child with no observable identity is converged before failing."""
    observe(monkeypatch, None)
    converged: list[tuple[FakePopen, float, ProcessIdentity | None]] = []
    monkeypatch.setattr(
        lifecycle,
        "_converge_unproven_spawn",
        lambda proc, grace, anchor: converged.append((proc, grace, anchor)),
    )

    code = lifecycle._recover_locked(options())

    assert code == lifecycle.EXIT_ERROR
    assert "converging" in capsys.readouterr().err
    assert len(converged) == 1
    proc, _grace, anchor = converged[0]
    assert proc is recover_env[-1]
    assert anchor is None
    assert proc.poll() is None


def test_recover_converges_live_child_with_non_private_identity(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    recover_env: list[FakePopen],
) -> None:
    """A live non-private session child is exactly signalled via its pin."""
    observe(monkeypatch, NON_PRIVATE)
    fake = FakePopen(PID)

    def spawn(*_args: object, **_kwargs: object) -> FakePopen:
        """Return the prebuilt non-private-session fake child."""
        recover_env.append(fake)
        return fake

    monkeypatch.setattr(lifecycle, "spawn_worker", spawn)
    delivered = install_convergence(monkeypatch, fake, NON_PRIVATE)

    code = lifecycle._recover_locked(options())

    assert code == lifecycle.EXIT_ERROR
    assert "converging" in capsys.readouterr().err
    assert delivered == [(PID, "SIGTERM")]
    assert fake.poll() == -15


def test_recover_does_not_report_success_for_dead_child(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    recover_env: list[FakePopen],
) -> None:
    """A child that already exits is never reported as adoptable."""
    observe(monkeypatch, PRIVATE)

    def mark_dead(_pid: int) -> ProcessIdentity | None:
        """Report the private identity while terminating the fake child.

        Returns:
            The private identity observed before the child exited.
        """
        if recover_env:
            recover_env[-1].returncode = 0
        return PRIVATE

    monkeypatch.setattr(lifecycle, "process_identity", mark_dead)

    code = lifecycle._recover_locked(options())
    captured = capsys.readouterr()

    assert code == lifecycle.EXIT_ERROR
    assert "exited before it could be adopted" in captured.err
    assert "adopt it with" not in captured.out
    assert f"pid={PID}" not in captured.out


def test_recover_reports_success_for_live_private_session_child(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    recover_env: list[FakePopen],
) -> None:
    """A live child that establishes its private session is reported."""
    observe(monkeypatch, PRIVATE)

    code = lifecycle._recover_locked(options())

    assert code == lifecycle.EXIT_OK
    assert recover_env[-1].poll() is None
    assert "adopt it with" in capsys.readouterr().out
