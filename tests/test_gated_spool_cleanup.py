"""Aborted gated starts converge without any filesystem use."""

from __future__ import annotations

import errno
import os
import uuid
from typing import TYPE_CHECKING, cast

import psycopg
import pytest

from lubko import worker
from lubko.worker import (
    GatedSpawn,
    OutputStream,
    Supervisor,
    abort_gated_start,
    await_gated_group_gone,
)

if TYPE_CHECKING:
    import subprocess

    from lubko.worker import JobsConnection, Settings


class _TerminalProc:
    """Minimal already-terminal ``Popen`` stand-in."""

    pid = 999999

    def __init__(self) -> None:
        self.returncode = 0
        self.wait_calls = 0

    def poll(self) -> int | None:
        return self.returncode

    def wait(self, timeout: float | None = None) -> int:
        del timeout
        self.wait_calls += 1
        return self.returncode


def _gated() -> tuple[GatedSpawn, _TerminalProc]:
    """Create an already-terminal gated start with in-memory capture buffers.

    Returns:
        The gated-start record and its test process stand-in.
    """
    read_fd, gate_fd = os.pipe()
    os.close(read_fd)
    stdout_read, _stdout_write = os.pipe()
    stderr_read, _stderr_write = os.pipe()
    proc = _TerminalProc()
    gated = GatedSpawn(
        proc=cast("subprocess.Popen[bytes]", proc),
        pgid=proc.pid,
        stdout=OutputStream(data=bytearray(b"stdout")),
        stderr=OutputStream(data=bytearray(b"stderr")),
        gate_fd=gate_fd,
        stdout_read_fd=stdout_read,
        stderr_read_fd=stderr_read,
    )
    return gated, proc


def _assert_gate_closed(gate_fd: int) -> None:
    """Assert that the worker-side start gate was closed without release."""
    with pytest.raises(OSError, match="Bad file descriptor") as caught:
        os.write(gate_fd, b"x")
    assert caught.value.errno == errno.EBADF


def test_abort_converges_without_filesystem_use(monkeypatch: pytest.MonkeyPatch) -> None:
    """Aborting a terminal gated start converges and closes the gate only."""
    gated, proc = _gated()
    monkeypatch.setattr("os.unlink", _forbid_unlink)
    monkeypatch.setattr("pathlib.Path.unlink", _forbid_unlink)

    assert abort_gated_start(gated.proc, gated.pgid, gated.gate_fd)

    assert proc.wait_calls == 1
    _assert_gate_closed(gated.gate_fd)


def _forbid_unlink(*_args: object, **_kwargs: object) -> None:
    """Fail any filesystem unlink attempted during gated abort.

    Raises:
        AssertionError: Always; gated abort must not touch the filesystem.
    """
    msg = "gated abort must not touch the filesystem"
    raise AssertionError(msg)


def test_blocking_convergence_closes_capture_fds() -> None:
    """Blocking convergence closes the capture pipe read ends."""
    gated, proc = _gated()

    await_gated_group_gone(gated)

    assert proc.wait_calls == 1
    for capture_fd in (gated.stdout_read_fd, gated.stderr_read_fd):
        assert capture_fd is not None
        with pytest.raises(OSError, match="Bad file descriptor"):
            os.fstat(capture_fd)
    os.close(gated.gate_fd)


def _supervisor() -> Supervisor:
    """Build the minimal supervisor state needed by the pre-release seam.

    Returns:
        An unstarted supervisor carrying placeholder settings.
    """
    supervisor = object.__new__(Supervisor)
    supervisor.settings = cast("Settings", object())
    return supervisor


def test_failed_identity_persistence_keeps_job_local_outcome(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Convergence cannot replace a normal fail-closed start result."""
    gated, proc = _gated()
    monkeypatch.setattr(worker, "proc_start_ticks", lambda _pid: 123)
    monkeypatch.setattr(worker, "_persist_process", lambda *_args, **_kwargs: False)

    failure, ticks = _supervisor()._pre_release_failure(
        cast("JobsConnection", None), uuid.uuid4(), gated
    )

    assert failure == "unable to record process identity; job not started"
    assert ticks == 0
    assert proc.wait_calls == 1
    _assert_gate_closed(gated.gate_fd)


def test_cleanup_failure_preserves_connectivity_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Convergence cannot mask the database connectivity exception."""
    gated, proc = _gated()
    supervisor = _supervisor()
    monkeypatch.setattr(worker, "proc_start_ticks", lambda _pid: 123)

    db_error = psycopg.OperationalError()

    def fail_persist(*_args: object, **_kwargs: object) -> bool:
        raise db_error

    monkeypatch.setattr(worker, "_persist_process", fail_persist)
    monkeypatch.setattr(supervisor, "_is_connectivity_error", lambda _exc: True)

    with pytest.raises(psycopg.OperationalError) as caught:
        supervisor._pre_release_failure(cast("JobsConnection", None), uuid.uuid4(), gated)

    assert caught.value is db_error
    assert proc.wait_calls == 1
    _assert_gate_closed(gated.gate_fd)
