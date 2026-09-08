"""Shared exact-process signalling and identity primitives.

These helpers exist because a holding pidfd does NOT keep a numeric PID
reserved: the kernel frees the numeric ID from the namespace before the final
``struct pid`` reference is released. Any check-then-signal sequence that ends
in a *numeric* syscall (``os.kill``, ``os.killpg``) therefore remains racy no
matter how strong the preceding proof was. The only non-reusable delivery is
``pidfd_send_signal``, which addresses the pinned kernel process itself.

The process-identity observation helpers (``proc_start_ticks``,
``process_state_char``, ``process_is_zombie``, ``process_ppid``,
``proc_cpu_seconds``) own the canonical ``/proc/<pid>/stat`` parsing used by
agent, supervisor, health, and lifecycle subsystems.  Subsystem-specific
authority (agent markers, worker incarnation IDs, supervisor child ownership,
lifecycle obligations) composes with these shared primitives rather than
duplicating them.
"""

from __future__ import annotations

import ctypes
import os
import signal
from pathlib import Path
from typing import Final

STAT_MIN_FIELDS: Final = 20
STAT_STATE_FIELD_INDEX: Final = 0
STAT_PPID_FIELD_INDEX: Final = 1
STAT_PGRP_FIELD_INDEX: Final = 2
STAT_UTIME_FIELD_INDEX: Final = 11
STAT_STIME_FIELD_INDEX: Final = 12
STAT_STARTTIME_FIELD_INDEX: Final = 19


def open_pidfd(pid: int) -> int:
    """Open a pidfd pinning ``pid`` against kernel struct-pid release.

    Args:
        pid: Process id to pin.

    Returns:
        The new pid file descriptor.

    Raises:
        OSError: If the process is gone or the pin fails.
    """
    if hasattr(os, "pidfd_open"):
        return int(os.pidfd_open(pid))
    _LIBC.pidfd_open.argtypes = (ctypes.c_int, ctypes.c_uint)
    _LIBC.pidfd_open.restype = ctypes.c_int
    fd = _LIBC.pidfd_open(pid, 0)
    if fd < 0:
        raise OSError(ctypes.get_errno(), "pidfd_open failed")
    return int(fd)


def pidfd_send_signal(pidfd: int, sig: int) -> None:
    """Deliver ``sig`` to exactly the process pinned by ``pidfd``.

    Args:
        pidfd: Pinned process file descriptor.
        sig: Signal number to deliver.

    Raises:
        OSError: If delivery fails (for example the process already exited).
    """
    if hasattr(signal, "pidfd_send_signal"):
        signal.pidfd_send_signal(pidfd, sig)
        return
    _LIBC.pidfd_send_signal.argtypes = (
        ctypes.c_int,
        ctypes.c_int,
        ctypes.c_void_p,
        ctypes.c_uint,
    )
    _LIBC.pidfd_send_signal.restype = ctypes.c_int
    if _LIBC.pidfd_send_signal(pidfd, sig, None, 0) != 0:
        raise OSError(ctypes.get_errno(), "pidfd_send_signal failed")


def _read_proc_stat(pid: int) -> bytes | None:
    """Read the raw ``/proc/<pid>/stat`` bytes, or ``None`` on any error.

    Args:
        pid: Process ID to inspect.

    Returns:
        The raw stat bytes, or ``None`` when the process is gone or
        unreadable.
    """
    try:
        return (Path("/proc") / str(pid) / "stat").read_bytes()
    except OSError:
        return None


def _split_stat_fields(stat: bytes) -> list[bytes] | None:
    """Split ``/proc/<pid>/stat`` bytes after the closing paren.

    The comm field (between the first ``(`` and last ``)``) may contain
    spaces and parentheses, so field splitting must begin after the last
    ``)``.

    Args:
        stat: Raw stat bytes.

    Returns:
        The space-split fields after the comm, or ``None`` when the line
        is unparseable.
    """
    close_paren = stat.rfind(b")")
    if close_paren == -1:
        return None
    fields = stat[close_paren + 2 :].split()
    if len(fields) < STAT_MIN_FIELDS:
        return None
    return fields


def proc_start_ticks(pid: int) -> int | None:
    """Return a process start time in clock ticks, or ``None`` if unknown.

    The start time is unique per process on a boot and survives PID reuse,
    so it anchors identity checks.

    Args:
        pid: Process ID to inspect.

    Returns:
        The start time in clock ticks, or ``None`` when unreadable.
    """
    stat = _read_proc_stat(pid)
    if stat is None:
        return None
    fields = _split_stat_fields(stat)
    if fields is None:
        return None
    try:
        return int(fields[STAT_STARTTIME_FIELD_INDEX])
    except ValueError:
        return None


def process_state_char(pid: int) -> str | None:
    """Return the single-character process state, or ``None`` if unreadable.

    This is the neutral primitive: it reports observed evidence without
    interpreting it.  Callers adapt the unknown case to their own fail-closed
    or fail-open policy.

    Args:
        pid: Process ID to inspect.

    Returns:
        The state character (e.g. ``"R"``, ``"S"``, ``"Z"``) or ``None``
        when the process is gone or the stat entry is unreadable.
    """
    stat = _read_proc_stat(pid)
    if stat is None:
        return None
    fields = _split_stat_fields(stat)
    if fields is None:
        return None
    try:
        return fields[STAT_STATE_FIELD_INDEX].decode("ascii", "replace")
    except (ValueError, UnicodeDecodeError):
        return None


def process_is_zombie(pid: int) -> bool:
    """Return whether a process is a zombie or dead.

    Fail-closed: an unreadable or unparseable ``/proc`` entry is treated as
    zombie so callers never trust an ambiguous process state.

    Args:
        pid: Process ID to inspect.

    Returns:
        ``True`` when the process is zombie, dead, or unreadable.
    """
    state = process_state_char(pid)
    if state is None:
        return True
    return state in {"Z", "X"}


def process_ppid(pid: int) -> int | None:
    """Return the exact parent process ID, or ``None`` if unknown.

    Args:
        pid: Process whose parent to inspect.

    Returns:
        The parent PID, or ``None`` when the process is gone or unreadable.
    """
    stat = _read_proc_stat(pid)
    if stat is None:
        return None
    fields = _split_stat_fields(stat)
    if fields is None:
        return None
    try:
        return int(fields[STAT_PPID_FIELD_INDEX])
    except ValueError:
        return None


def proc_cpu_seconds(pid: int) -> float | None:
    """Return the total CPU time in seconds used by a process, or ``None``.

    Reads the user and system CPU time from ``/proc/<pid>/stat`` and converts
    clock ticks to seconds.

    Args:
        pid: Process ID to inspect.

    Returns:
        The total CPU time in seconds, or ``None`` when unavailable.
    """
    stat = _read_proc_stat(pid)
    if stat is None:
        return None
    fields = _split_stat_fields(stat)
    if fields is None:
        return None
    try:
        ticks = int(fields[STAT_UTIME_FIELD_INDEX]) + int(
            fields[STAT_STIME_FIELD_INDEX],
        )
    except (ValueError, IndexError):
        return None
    try:
        ticks_per_second = os.sysconf("SC_CLK_TCK")
    except (ValueError, OSError):
        return None
    if not ticks_per_second:
        return None
    return ticks / ticks_per_second


def process_pgrp(pid: int) -> int | None:
    """Return the exact process group of a running process.

    Zombie and dead processes report no group. Unreadable or unparseable
    process table entries are ignored.

    Args:
        pid: Process ID to inspect.

    Returns:
        The process group ID, or ``None`` if the process is dead or unknown.
    """
    state = process_state_char(pid)
    if state is None or state in {"Z", "X"}:
        return None
    stat = _read_proc_stat(pid)
    if stat is None:
        return None
    fields = _split_stat_fields(stat)
    if fields is None:
        return None
    try:
        return int(fields[STAT_PGRP_FIELD_INDEX])
    except ValueError:
        return None


_LIBC = ctypes.CDLL(None, use_errno=True)
