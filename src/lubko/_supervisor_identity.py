"""Supervisor runtime identity: captured once, used for spawn-based handoff.

Every ``lubko-supervisor`` daemon resolves its own code through ``cli/current``
at startup time.  After deployment B, ``cli/current`` points to B while the
still-running supervisor executes from the ``cli/<commit_A>`` runtime it
resolved at startup.  ``cli.current_commit()`` returns B, not A.

This module therefore does NOT call ``cli.current_commit()`` to derive the
supervisor's identity.  Instead, the daemon captures the confirmed commit once
at startup (before the reconcile loop) and stores it durably in ``state.json``
as ``supervisor_runtime_commit``.  The reconcile loop compares the stored
runtime commit against the current ``cli/current`` to detect when the
supervisor's own runtime is outdated.

Activation (spawn-based two-phase handoff)
------------------------------------------

When the reconcile loop detects that ``cli.current_commit()`` differs from the
stored ``supervisor_runtime_commit``, the old supervisor A spawns the new
supervisor B as a child process using ``subprocess.Popen`` with the inherited
lock file descriptor and two dedicated pipes (readiness B→A, transfer A→B).
B starts in handoff preparation mode, initializes, signals READY, and waits.
Only after A confirms B is ready does A send TRANSFER, close the lock fd, and
exit cleanly.

Crash boundaries
----------------

- **Before trigger**: The supervisor runs A's code, holds the lock, owns the
  worker.  State.json records ``supervisor_runtime_commit=A``.
- **Spawn success, B ready**: B signals READY on the readiness pipe.  A
  confirms B has initialized.  No state mutation yet; A remains authoritative.
- **TRANSFER**: A writes TRANSFER on the transfer pipe, closes its lock fd,
  and exits.  B receives TRANSFER and enters normal startup.  No authority
  overlap and no authority gap.
- **Spawn failure** (``OSError``): The old supervisor catches the exception,
  logs an error, and continues its reconcile loop.  No state mutation, no
  lock release.  The old supervisor remains authoritative.
- **B failure before READY**: If B crashes during import/startup before
  signalling READY, A detects EOF/timeout, kills B, and continues with its
  own authority.
- **B failure after READY but before TRANSFER**: A detects the pipe closure,
  kills B, and continues.  A still holds the lock and remains authoritative.
- **Recovery from previous confirmed runtime**: If B's runtime is missing or
  corrupt, the path resolution returns ``None`` and the old supervisor
  continues running A's code.  The operator must fix the runtime before the
  upgrade can proceed.

Garbage collection
------------------

``cli.supervisor_authoritative_commits()`` includes the stored
``supervisor_runtime_commit`` so the old runtime A is never garbage-collected
while the supervisor is still executing from it.  After a successful transfer
to B, the new supervisor's ``supervisor_runtime_commit`` is B, and A is no
longer authoritative — GC may collect it.
"""

from __future__ import annotations

import logging
from typing import Final

from lubko import cli
from lubko import startup_contract as _startup_contract

LOGGER: Final = logging.getLogger(__name__)


def capture_supervisor_runtime_commit() -> str | None:
    """Capture the supervisor daemon's own runtime commit at startup.

    This must be called exactly once, at daemon startup, BEFORE the reconcile
    loop begins.  At this moment ``cli/current`` still points to the commit
    the supervisor is executing from.  After a later deployment changes
    ``cli/current``, this captured value remains correct.

    Returns:
        The 40-character commit hash the supervisor is executing from,
        or ``None`` when no maintained CLI is active.
    """
    return cli.current_commit()


def resolve_new_supervisor_executable(commit: str) -> str | None:
    """Resolve the path to the supervisor executable for a different commit.

    This is used to detect whether a spawn-based handoff is possible.  The
    path resolution goes through the sealed per-commit runtime, never through
    a mutable working tree.

    Args:
        commit: The target commit whose supervisor to resolve.

    Returns:
        The executable path as a string, or ``None`` when the runtime is
        missing, incomplete, or the entry point does not exist.
    """
    if not cli.runtime_is_usable(commit):
        return None
    executable = cli.cli_entry_executable(commit, "lubko-supervisor")
    return str(executable) if executable is not None else None


def contract_schema_version() -> int:
    """Return the startup contract schema version compiled into this code.

    Returns:
        The contract schema version integer.
    """
    return _startup_contract.CONTRACT_SCHEMA_VERSION
