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


def test_teardown_never_signals_reused_pid_identity() -> None:
    """A registry entry whose recorded identity no longer matches is never signalled.

    Deterministically simulates kernel PID reuse: a live innocent process
    occupies a PID whose registry entry records different start ticks, as if
    the originally-registered process had died and the kernel reassigned its
    PID.  Teardown must refuse to signal the unverified new occupant and must
    report the stale identity loudly instead.
    """
    innocent = subprocess.Popen(
        [SLEEP_BIN, "60"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        current = guard.proc_start_ticks(innocent.pid)
        assert current is not None
        guard.register(innocent, start_ticks=current - 1)
        with pytest.raises(AssertionError, match="never signalled"):
            guard.teardown_tracked()
        assert pid_live(innocent.pid)
    finally:
        if innocent.poll() is None:
            innocent.kill()
            innocent.wait(timeout=10)
    assert innocent.pid not in guard.TRACKED


def test_register_fails_closed_for_live_process_without_ticks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A live registration whose ticks cannot be read is refused outright."""
    proc = subprocess.Popen(
        [SLEEP_BIN, "300"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    try:
        monkeypatch.setattr(guard, "proc_start_ticks", lambda _pid: None)
        with pytest.raises(AssertionError, match="unverifiable identity"):
            guard.register(proc)
        assert guard.tracked_pids() == ()
        # The live process was never owned and never signalled.
        assert pid_live(proc.pid)
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=10)


def test_register_ignores_already_terminal_process() -> None:
    """An already-terminal (reaped) Popen is harmless and not registered."""
    proc = subprocess.Popen(
        [SLEEP_BIN, "0"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    assert proc.wait(timeout=10) == 0
    guard.register(proc)
    assert guard.tracked_pids() == ()


def test_teardown_never_signals_entries_without_valid_ticks() -> None:
    """A registry entry without valid recorded ticks is never signalled."""
    innocent = subprocess.Popen(
        [SLEEP_BIN, "60"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        guard.register_unverifiable(innocent)
        with pytest.raises(AssertionError, match="never signalled"):
            guard.teardown_tracked()
        assert pid_live(innocent.pid)
    finally:
        if innocent.poll() is None:
            innocent.kill()
            innocent.wait(timeout=10)
    assert innocent.pid not in guard.TRACKED


def test_teardown_stops_exact_identity_after_registration() -> None:
    """A registered process whose ticks still match is signalled exactly."""
    proc, pid = spawn_sleep_leader()
    guard.register(proc)
    assert guard.proc_start_ticks(pid) is not None
    with pytest.raises(AssertionError, match="leaked"):
        guard.teardown_tracked()
    assert not pid_live(pid)
    assert pid not in guard.TRACKED


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
