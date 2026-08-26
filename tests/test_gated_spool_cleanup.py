"""Temporary gated-spool cleanup is best-effort after exact convergence."""

from __future__ import annotations

import errno
import os
import uuid
from typing import TYPE_CHECKING, cast

import psycopg
import pytest

from lubko import worker
from lubko.worker import GatedSpawn, Supervisor, abort_gated_start, await_gated_group_gone

if TYPE_CHECKING:
    import subprocess
    from pathlib import Path

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


def _gated(tmp_path: Path) -> tuple[GatedSpawn, _TerminalProc]:
    """Create an already-terminal gated start with one unremovable spool.

    Returns:
        The gated-start record and its test process stand-in.
    """
    stdout_path = tmp_path / "stdout"
    stdout_path.mkdir()
    stderr_path = tmp_path / "stderr"
    stderr_path.write_bytes(b"stderr")
    read_fd, gate_fd = os.pipe()
    os.close(read_fd)
    proc = _TerminalProc()
    gated = GatedSpawn(
        proc=cast("subprocess.Popen[bytes]", proc),
        pgid=proc.pid,
        stdout_path=stdout_path,
        stderr_path=stderr_path,
        gate_fd=gate_fd,
    )
    return gated, proc


def _assert_gate_closed(gate_fd: int) -> None:
    """Assert that the worker-side start gate was closed without release."""
    with pytest.raises(OSError, match="Bad file descriptor") as caught:
        os.write(gate_fd, b"x")
    assert caught.value.errno == errno.EBADF


def test_abort_cleanup_failure_does_not_mask_convergence(tmp_path: Path) -> None:
    """One failed gated-spool unlink does not block sibling cleanup."""
    gated, proc = _gated(tmp_path)

    assert abort_gated_start(
        gated.proc,
        gated.pgid,
        gated.stdout_path,
        gated.stderr_path,
        gated.gate_fd,
    )

    assert gated.stdout_path.is_dir()
    assert not gated.stderr_path.exists()
    assert proc.wait_calls == 1
    _assert_gate_closed(gated.gate_fd)


def test_blocking_convergence_cleanup_is_best_effort(tmp_path: Path) -> None:
    """Blocking convergence also isolates each temporary-spool cleanup."""
    gated, proc = _gated(tmp_path)

    await_gated_group_gone(gated)

    assert gated.stdout_path.is_dir()
    assert not gated.stderr_path.exists()
    assert proc.wait_calls == 1
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
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cleanup failure cannot replace a normal fail-closed start result."""
    gated, proc = _gated(tmp_path)
    monkeypatch.setattr(worker, "proc_start_ticks", lambda _pid: 123)
    monkeypatch.setattr(worker, "_persist_process", lambda *_args, **_kwargs: False)

    failure, ticks = _supervisor()._pre_release_failure(
        cast("JobsConnection", None), uuid.uuid4(), gated
    )

    assert failure == "unable to record process identity; job not started"
    assert ticks == 0
    assert proc.wait_calls == 1
    assert not gated.stderr_path.exists()
    _assert_gate_closed(gated.gate_fd)


def test_cleanup_failure_preserves_connectivity_exception(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cleanup failure cannot mask the database connectivity exception."""
    gated, proc = _gated(tmp_path)
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
    assert not gated.stderr_path.exists()
    _assert_gate_closed(gated.gate_fd)
