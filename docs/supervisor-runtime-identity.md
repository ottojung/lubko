# Supervisor runtime identity and exec-based upgrade

## Problem

After deployment B, `cli/current` points to B while the still-running supervisor executes from the `cli/<commit_A>` runtime it resolved at startup. `cli.current_commit()` returns B, not A. There is no mechanism to ever activate supervisor B, and GC could delete A's runtime.

## Design: exec-based upgrade with lock adoption (Model A)

The supervisor captures its own runtime commit once at startup (the only moment `cli/current_commit()` is correct for the supervisor's own identity) and stores it durably in `state.json` as `supervisor_runtime_commit`. The reconcile loop compares this against `cli.current_commit()` to detect when the supervisor's own runtime is outdated.

When skew is detected, the supervisor resolves the path to the new supervisor executable through the exact confirmed commit's sealed runtime (`cli.cli_entry_executable(confirmed, "lubko-supervisor")`, not the mutable `cli/current` symlink) and calls `os.execve()` to replace itself in-place.

## Lock-adoption protocol

Python PEP 446 makes file descriptors non-inheritable by default, so the ownership `flock` fd would close on exec. The adoption protocol passes the fd explicitly:

1. **Before exec**: The old supervisor sets the lock fd inheritable via `os.set_inheritable(fd, True)`.
2. **Environment variables**: Three env vars are passed to the exec'd process:
   - `LUBKO_SUPERVISOR_HANDOFF_FD`: The fd number holding the ownership flock.
   - `LUBKO_SUPERVISOR_HANDOFF_PATH`: The expected lock file path (for validation).
   - `LUBKO_SUPERVISOR_HANDOFF_PID`: The old supervisor's PID (for diagnostics).
3. **At startup**: The new supervisor checks for the handoff env vars. If present, it calls `adopt_supervisor_lock(fd, path)` which validates:
   - The fd number is within the process's open-fd limit.
   - The fd is open (`os.fstat` succeeds).
   - The fd's path (`/proc/self/fd/<N>`) matches the expected lock path exactly.
   - The flock on the fd is held (non-blocking `flock` does not return `EWOULDBLOCK`).
4. **Adoption**: The validated fd is used directly as the ownership fd. No second `open`/`flock` occurs.
5. **Cleanup**: The handoff env vars are cleared so they are never consumed twice.
6. **Revert on failure**: If `os.execve` raises `OSError`, the inheritable flag is reverted and the old supervisor continues with its existing authority.

## Crash boundaries

| Boundary | State | Lock | Worker | Outcome |
|---|---|---|---|---|
| Before trigger | state.json has `supervisor_runtime_commit=A` | Old holds flock | Worker alive, owned by old PID | Normal operation |
| During preparation | No state mutation | Old holds flock | Worker alive | Resolve new executable path |
| Exec success | `os.execve` atomically replaces process | New inherits fd | Worker survives (same PID) | New code runs, lock never released |
| Exec failure | No state mutation | Old holds flock | Worker alive | Old catches `OSError`, reverts inheritable flag, continues |
| Successor startup (handoff path) | Reads state.json, adopts fd | Validates inherited fd | Worker alive (same PID) | New code runs, lock adopted |
| Successor startup (normal path) | Reads state.json | Opens/acquires new flock | Worker alive (same PID) | New code runs, lock acquired |
| Tini restart (after crash) | Reads state.json | Opens/acquires new flock | Worker may be dead (PDEATHSIG) | Fresh start, resolves cli/current |

## GC semantics

`cli.supervisor_authoritative_commits()` includes the stored `supervisor_runtime_commit` so the old runtime A is never garbage-collected while the supervisor is still executing from it. After a successful exec into B, the new supervisor's `supervisor_runtime_commit` is B, and A is no longer authoritative — GC may collect it.

## Compatibility

The durable schema version (`supervise.SCHEMA_VERSION`) is the authoritative compatibility boundary. The `supervisor_runtime_commit` field is backward-compatible: old state files without the field parse as `None`. The startup contract schema version is an observational aid for operators, not a hard compatibility gate.

## Activation path

1. `lubko-deploy deploy` builds and confirms worker B.
2. After confirmation, `cli/current` points to B.
3. The running supervisor (executing from A) detects skew on the next reconcile tick.
4. The supervisor resolves B's `lubko-supervisor` executable through the sealed runtime.
5. The supervisor sets its lock fd inheritable, builds the handoff env, and calls `os.execve`.
6. B's supervisor code starts, adopts the lock fd, reads state.json, and continues the worker lifecycle.
7. The old runtime A is no longer authoritative and may be GC'd.
