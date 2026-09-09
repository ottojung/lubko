"""Supervisor runtime identity: captured once, exec-activated.

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

Activation (exec-based upgrade)
------------------------------

When the reconcile loop detects that ``cli.current_commit()`` differs from the
stored ``supervisor_runtime_commit``, the daemon resolves the path to the new
supervisor executable through ``cli/cli_entry_executable(commit,
"lubko-supervisor")`` and calls ``os.execv()``.  ``os.execv`` atomically
replaces the process image while preserving:

- the **PID** (worker ``PR_SET_PDEATHSIG`` and parentage remain valid);
- the **open lock file descriptor** (the ownership ``flock`` is never
  released, so no second daemon can start during the handoff);
- the **child process** (the maintained worker is still a live child of the
  same PID).

If ``os.execv`` raises ``OSError`` (missing runtime, permission error), the old
supervisor continues its reconcile loop unchanged: the exec either fully
replaces the process or raises without any partial effect.

Crash boundaries
----------------

- **Before trigger**: The supervisor runs A's code, holds the lock, owns the
  worker.  State.json records ``supervisor_runtime_commit=A``.
- **During preparation**: The supervisor resolves B's executable path.  No
  state mutation occurs; no lock release.
- **Exec success**: ``os.execv`` atomically replaces the process with B's
  code.  B inherits the lock fd, the PID, and the child.  B reads state.json,
  sees the worker is alive, continues reconciliation.
- **Exec failure** (``OSError``): The old supervisor catches the exception,
  logs an error, and continues its reconcile loop with backoff.  No state
  mutation, no lock release.  The old supervisor remains authoritative.
- **Successor startup failure**: If B's code crashes during startup (before
  acquiring the lock — impossible since the lock fd is inherited, but if B
  somehow fails), the kernel would kill the process and release the lock.  Tini
  restarts a fresh supervisor, which resolves ``cli/current`` (now B) and
  starts normally.
- **Recovery from previous confirmed runtime**: If B's runtime is missing or
  corrupt, the exec path resolution returns ``None`` and the old supervisor
  continues running A's code.  The operator must fix the runtime before the
  upgrade can proceed.

Garbage collection
------------------

``cli.supervisor_authoritative_commits()`` includes the stored
``supervisor_runtime_commit`` so the old runtime A is never garbage-collected
while the supervisor is still executing from it.  After a successful exec into
B, the new supervisor's ``supervisor_runtime_commit`` is B, and A is no longer
authoritative — GC may collect it.
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

    This is used to detect whether an exec-based upgrade is possible.  The
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
