"""Process-group membership queries shared across lifecycle modules.

Kept separate from :mod:`lubko.worker` so lightweight modules such as
:mod:`lubko.agent` can query process-group membership without pulling in
the heavy ``psycopg`` dependency that :mod:`lubko.worker` requires at
import time.
"""

from __future__ import annotations

import os
from pathlib import Path

from lubko._exact_signal import process_pgrp as _shared_process_pgrp


def _process_pgrp(pid: int) -> int | None:
    """Return the exact process group of a running process.

    Args:
        pid: Process ID to inspect.

    Returns:
        The process group ID, or ``None`` if the process is dead or unknown.
    """
    return _shared_process_pgrp(pid)


def group_has_members(pgid: int) -> bool:
    """Return whether any live process still belongs to the exact process group.

    Uses the process table under ``/proc`` when available so membership is
    matched by exact process group, never by process name.  Falls back to
    querying the kernel directly otherwise.

    Args:
        pgid: Process group identifier to inspect.

    Returns:
        ``True`` when at least one running process still belongs to the group.
    """
    proc_dir = Path("/proc")
    if proc_dir.is_dir():
        for entry in proc_dir.iterdir():
            if not entry.name.isdigit():
                continue
            if _process_pgrp(int(entry.name)) == pgid:
                return True
        return False
    try:
        os.getpgid(pgid)
    except ProcessLookupError:
        return False
    return True
