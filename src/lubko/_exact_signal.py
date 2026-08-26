"""Shared exact-process signalling primitives.

These helpers exist because a holding pidfd does NOT keep a numeric PID
reserved: the kernel frees the numeric ID from the namespace before the final
``struct pid`` reference is released. Any check-then-signal sequence that ends
in a *numeric* syscall (``os.kill``, ``os.killpg``) therefore remains racy no
matter how strong the preceding proof was. The only non-reusable delivery is
``pidfd_send_signal``, which addresses the pinned kernel process itself.
"""

from __future__ import annotations

import ctypes
import os
import signal
from pathlib import Path
from typing import Final

STAT_MIN_FIELDS: Final = 20
STAT_STATE_FIELD_INDEX: Final = 0
STAT_PGRP_FIELD_INDEX: Final = 2


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


def process_pgrp(pid: int) -> int | None:
    """Return the exact process group of a running process.

    Zombie and dead processes report no group. Unreadable or unparseable
    process table entries are ignored.

    Args:
        pid: Process ID to inspect.

    Returns:
        The process group ID, or ``None`` if the process is dead or unknown.
    """
    try:
        stat = (Path("/proc") / str(pid) / "stat").read_bytes()
    except OSError:
        return None
    close_paren = stat.rfind(b")")
    if close_paren == -1:
        return None
    fields = stat[close_paren + 2 :].split()
    if len(fields) < STAT_MIN_FIELDS:
        return None
    if fields[STAT_STATE_FIELD_INDEX] in {b"Z", b"X"}:
        return None
    try:
        return int(fields[STAT_PGRP_FIELD_INDEX])
    except ValueError:
        return None


_LIBC = ctypes.CDLL(None, use_errno=True)
