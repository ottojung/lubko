"""Regression tests for the deterministic process-guard infrastructure.

These verify that the shared guard owns every test-created process with an
exact identity, stops leaked processes by their exact process group, never
signals the pytest/shared process group, and fails loudly when a test leaks.
"""

from __future__ import annotations

import os
import shutil
import signal
import subprocess
from pathlib import Path

import pytest

from tests import _process_guard as guard

SLEEP_BIN: str = shutil.which("sleep") or "/bin/sleep"


def spawn_sleep_leader() -> tuple[subprocess.Popen[bytes], int]:
    """Spawn an untracked long-lived session-leader ``sleep``.

    Returns:
        A ``(Popen, pid)`` pair where the process is deliberately NOT
        registered with the guard.
    """
    proc = subprocess.Popen(
        [SLEEP_BIN, "300"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        close_fds=True,
    )
    return proc, proc.pid


def pid_live(pid: int) -> bool:
    """Return whether a process exists and is not a zombie.

    Args:
        pid: Process ID to probe.

    Returns:
        ``True`` when a running (non-zombie) process with that ID exists.
    """
    try:
        stat = (Path("/proc") / str(pid) / "stat").read_bytes()
    except OSError:
        return False
    close_paren = stat.rfind(b")")
    if close_paren == -1:
        return True
    fields = stat[close_paren + 2 :].split()
    if not fields:
        return True
    return fields[0] not in {b"Z", b"X"}


def test_teardown_stops_leaked_registered_process() -> None:
    """A leaked registered process is stopped by exact group and reported."""
    proc, pid = spawn_sleep_leader()
    guard.register(proc)
    try:
        with pytest.raises(AssertionError, match="leaked"):
            guard.teardown_tracked()
        assert not pid_live(pid)
        assert pid not in guard.TRACKED
    finally:
        with pytest.raises(ProcessLookupError):
            os.killpg(pid, 0)


def test_teardown_is_clean_when_process_was_reaped() -> None:
    """A test that owns and reaps its process leaves a clean registry."""
    proc, pid = spawn_sleep_leader()
    guard.register(proc)
    os.killpg(pid, signal.SIGKILL)
    proc.wait(timeout=10)
    guard.unregister(proc)
    assert guard.teardown_tracked() == 0
    assert pid not in guard.TRACKED
    assert not pid_live(pid)


def test_assert_no_live_tracked_detects_leaks() -> None:
    """A live tracked process is detected after teardown."""
    proc, pid = spawn_sleep_leader()
    guard.register(proc)
    try:
        with pytest.raises(AssertionError, match="still live"):
            guard.assert_no_live_tracked()
    finally:
        guard.teardown_tracked(fail_on_leak=False)
        assert not pid_live(pid)


def test_teardown_never_signals_parent_process_group() -> None:
    """A non-leader child is stopped by exact PID, never its shared group.

    A tracked process that is not a session leader shares the pytest process
    group.  Teardown must signal only that exact PID; signalling the shared
    group would kill pytest and any sibling process in the same group.
    """
    sibling = subprocess.Popen(
        [SLEEP_BIN, "60"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    leaked = subprocess.Popen(
        [SLEEP_BIN, "60"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        assert os.getpgid(leaked.pid) == os.getpgrp()
        assert os.getpgid(sibling.pid) == os.getpgrp()
        guard.register(leaked)
        killed = guard.teardown_tracked(fail_on_leak=False)
        assert killed == 1
        assert not pid_live(leaked.pid)
        assert pid_live(sibling.pid)
        assert pid_live(os.getpid())
    finally:
        if sibling.poll() is None:
            sibling.kill()
            sibling.wait(timeout=10)
