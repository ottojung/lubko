"""Deterministic process ownership and teardown for process-level tests.

Every process a process-level test spawns must be owned with an exact
identity and stopped deterministically on both success and assertion/error
paths.  This module provides:

- ``TRACKED`` — a session registry of every ``Popen`` the tests create,
  keyed by PID;
- ``register``/``unregister`` — helpers the test helpers call when they
  spawn or reap a process;
- ``teardown_tracked`` — stops any still-live registered process with
  ``SIGTERM`` then ``SIGKILL`` by exact identity (the whole dedicated group
  for a session/process-group leader, the exact PID only for a non-leader
  that shares the pytest orchestrator's group), reaps it, and raises when a
  test leaked a process;
- ``assert_no_live_tracked`` — asserts that no registered process survives.

The container runs under a real reaping PID 1 (tini) which reaps adopted
children, so this guard never installs a reaper or calls ``waitpid(-1)``: it
only ever waits on the exact processes the tests own.  Only exact identities
are signalled; nothing here inspects the production code and nothing performs
a broad process kill.
"""

from __future__ import annotations

import os
import signal
import time
from contextlib import suppress
from typing import TYPE_CHECKING, Final

from lubko.worker import group_has_members

if TYPE_CHECKING:
    import subprocess

KILL_GRACE_SECONDS: Final = 5.0
GROUP_POLL_SECONDS: Final = 0.02

TRACKED: dict[int, subprocess.Popen[bytes]] = {}


def register(proc: subprocess.Popen[bytes]) -> None:
    """Track a spawned process so teardown can stop it deterministically.

    Args:
        proc: The spawned process to own.
    """
    TRACKED[proc.pid] = proc


def unregister(proc: subprocess.Popen[bytes]) -> None:
    """Stop tracking a process that a test has already reaped.

    Args:
        proc: The reaped process to forget.
    """
    TRACKED.pop(proc.pid, None)


def tracked_pids() -> tuple[int, ...]:
    """Return the PIDs currently owned by the registry.

    Returns:
        The tracked process IDs.
    """
    return tuple(TRACKED)


def live_pids() -> list[int]:
    """Return the tracked PIDs whose processes are still running.

    Returns:
        The live tracked process IDs.
    """
    return [pid for pid, proc in TRACKED.items() if proc.poll() is None]


def _process_group_of(pid: int) -> int | None:
    """Return the exact process group of ``pid``, or ``None`` when gone.

    Args:
        pid: Process whose group to resolve.

    Returns:
        The process group ID, or ``None`` when the process is gone.
    """
    try:
        return os.getpgid(pid)
    except ProcessLookupError:
        return None


def _signal_exact(pid: int, pgid: int | None, sig: int) -> None:
    """Signal a tracked process without ever touching a shared group.

    When the process leads its own dedicated group (``pgid == pid``) the
    whole group is signalled; otherwise only the exact PID is signalled, so
    a non-leader child that shares the pytest orchestrator's process group
    can never cause the orchestrator or its siblings to be killed.

    Args:
        pid: Exact process ID to signal.
        pgid: The process's group, or ``None``.
        sig: Signal to deliver.
    """
    if pgid is not None and pgid == pid:
        with suppress(ProcessLookupError):
            os.killpg(pgid, sig)
    else:
        with suppress(ProcessLookupError):
            os.kill(pid, sig)


def _group_clear(pgid: int | None, pid: int) -> bool:
    """Return whether no dedicated group remains to wait for.

    A non-leader child shares an external group (for example the pytest
    orchestrator's own group), which is never treated as owned, so it is
    always considered clear; only a leader's dedicated group is awaited.

    Args:
        pgid: The process's group, or ``None``.
        pid: The tracked process ID.

    Returns:
        ``True`` when teardown need not wait for group members.
    """
    if pgid is None or pgid != pid:
        return True
    return not group_has_members(pgid)


def _still_active(proc: subprocess.Popen[bytes], pgid: int | None) -> bool:
    """Return whether a tracked process or its dedicated group remains live.

    Args:
        proc: The tracked process.
        pgid: The process's group, or ``None``.

    Returns:
        ``True`` when the process is still running, or its dedicated group
        still has members that would otherwise be abandoned.
    """
    if proc.poll() is None:
        return True
    return pgid is not None and pgid == proc.pid and group_has_members(pgid)


def _wait_group_gone(pgid: int) -> None:
    """Wait until a leader's dedicated process group has no live members.

    Args:
        pgid: The dedicated group to await.
    """
    deadline = time.monotonic() + KILL_GRACE_SECONDS
    while time.monotonic() < deadline and group_has_members(pgid):
        time.sleep(GROUP_POLL_SECONDS)


def _stop_one(proc: subprocess.Popen[bytes]) -> None:
    """Stop one tracked process deterministically and reap it.

    A tracked process is signalled with ``SIGTERM``, then ``SIGKILL`` while
    it remains live.  When the process is a session/process-group leader the
    whole dedicated group is signalled so no child of that group is
    abandoned, exactly like the worker cancellation contract.  When the
    process is NOT a group leader it shares an external process group (for
    example the pytest orchestrator's own group), so only the exact PID is
    signalled and the direct child is reaped: signalling that shared group
    would kill unrelated processes.  Only exact identities are ever touched;
    nothing uses process-name matching or broad ``pkill``.

    Args:
        proc: The tracked process to stop.
    """
    pid = proc.pid
    pgid = _process_group_of(pid)
    _signal_exact(pid, pgid, signal.SIGTERM)
    deadline = time.monotonic() + KILL_GRACE_SECONDS
    while time.monotonic() < deadline:
        if proc.poll() is not None and _group_clear(pgid, pid):
            break
        time.sleep(GROUP_POLL_SECONDS)
    if _still_active(proc, pgid):
        _signal_exact(pid, pgid, signal.SIGKILL)
    if proc.poll() is None:
        with suppress(Exception):
            proc.wait(timeout=KILL_GRACE_SECONDS)
    if pgid is not None and pgid == pid:
        _wait_group_gone(pgid)


def teardown_tracked(*, fail_on_leak: bool = True) -> int:
    """Stop every tracked process still live, returning how many leaked.

    Args:
        fail_on_leak: Whether a leftover live process is a hard failure.

    Returns:
        How many tracked processes were still live and had to be stopped.

    Raises:
        AssertionError: If ``fail_on_leak`` and any tracked process was still
            live, meaning a test failed to own and stop its own process.
    """
    procs = list(TRACKED.values())
    live = [proc for proc in procs if proc.poll() is None]
    for proc in live:
        _stop_one(proc)
    for proc in procs:
        TRACKED.pop(proc.pid, None)
    if fail_on_leak and live:
        msg = "test leaked process(es) that teardown had to stop: " + ", ".join(
            str(proc.pid) for proc in live
        )
        raise AssertionError(msg)
    return len(live)


def assert_no_live_tracked() -> None:
    """Assert that no test-created tracked process remains live.

    Raises:
        AssertionError: If any tracked process is still running after
            teardown, which would violate the acceptance criterion that
            repeated test runs do not increase the live process count.
    """
    live = live_pids()
    if live:
        msg = f"test-created processes still live after teardown: {live}"
        raise AssertionError(msg)
