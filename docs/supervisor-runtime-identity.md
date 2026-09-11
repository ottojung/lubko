# Supervisor runtime identity and spawned two-phase handoff

## Problem

After deployment B, `cli/current` points to B while the still-running supervisor executes from the `cli/<commit_A>` runtime it resolved at startup. `cli.current_commit()` returns B, not A. There is no mechanism to ever activate supervisor B, and GC could delete A's runtime.

## Design: spawned two-phase handoff

The supervisor captures its own runtime commit once at startup (the only moment `cli/current_commit()` is correct for the supervisor's own identity) and stores it durably in `state.json` as `supervisor_runtime_commit`. The reconcile loop compares this against `cli.current_commit()` to detect when the supervisor's own runtime is outdated.

When skew is detected, the supervisor resolves the path to the new supervisor executable through the exact confirmed commit's sealed runtime (`cli.cli_entry_executable(confirmed, "lubko-supervisor")`, not the mutable `cli/current` symlink) and spawns it as a subprocess in handoff preparation mode.

## Two-phase READY/TRANSFER protocol

Python PEP 446 makes file descriptors non-inheritable by default, so the ownership `flock` fd would close on exec. The handoff protocol passes the fd explicitly:

1. **Before spawn**: The old supervisor (A) creates two pipes: a readiness pipe (B→A) and a transfer pipe (A→B). The lock fd, ready-write fd, and transfer-read fd are made inheritable.
2. **Spawn**: A spawns B in handoff mode with `HANDOFF_MODE=1` and the relevant fds in the environment.
3. **B initializes**: B reads the lock fd from the environment, validates it via `adopt_supervisor_lock(fd, path)`, and uses it as its ownership fd.
4. **B signals READY**: B writes `R\n` on the readiness pipe to signal it is initialized and ready.
5. **A confirms**: A reads `R\n` from the readiness pipe (with timeout), then writes `T\n` on the transfer pipe, closes the lock fd, and exits. No authority overlap: A holds the lock until TRANSFER is sent.
6. **B receives TRANSFER**: B reads `T\n` from the transfer pipe and becomes the sole lifecycle authority.
7. **Cleanup**: Both sides close pipe fds and clear handoff environment variables.

## Crash boundaries

| Boundary | State | Lock | Worker | Outcome |
|---|---|---|---|---|
| Before trigger | state.json has `supervisor_runtime_commit=A` | Old holds flock | Worker alive, owned by old PID | Normal operation |
| During preparation | No state mutation | Old holds flock | Worker alive | Resolve new executable, create pipes |
| Spawn success | B subprocess starts in handoff mode | Old holds flock | Worker alive | B validates lock fd, signals READY |
| A sends transfer | A writes `T\n`, closes lock fd, exits | New holds flock | Worker alive (same PID) | New code runs, lock never released |
| Spawn failure | No state mutation | Old holds flock | Worker alive | Old catches OSError, continues |
| B startup failure | No state mutation | Old holds flock | Worker alive | Old catches failure, continues |
| Tini restart (after crash) | Reads state.json | Opens/acquires new flock | Worker may be dead (PDEATHSIG) | Fresh start, resolves cli/current |

## GC semantics

`cli.supervisor_authoritative_commits()` includes the stored `supervisor_runtime_commit` so the old runtime A is never garbage-collected while the supervisor is still executing from it. After a successful handoff to B, the new supervisor's `supervisor_runtime_commit` is B, and A is no longer authoritative — GC may collect it.

## Compatibility

The durable schema version (`supervise.SCHEMA_VERSION`) is the authoritative compatibility boundary. The `supervisor_runtime_commit` field is backward-compatible: old state files without the field parse as `None`. The startup contract schema version is an observational aid for operators, not a hard compatibility gate.

## Activation path

1. `lubko-deploy deploy` builds and confirms worker B.
2. After confirmation, `cli/current` points to B.
3. The running supervisor (executing from A) detects skew on the next reconcile tick.
4. The supervisor resolves B's `lubko-supervisor` executable through the sealed runtime.
5. The supervisor creates readiness and transfer pipes, sets the lock fd inheritable, builds the handoff env, and spawns B in handoff mode.
6. B adopts the lock fd, signals READY, and waits for TRANSFER.
7. A confirms B is ready, sends TRANSFER, closes the lock fd, and exits.
8. B receives transfer, continues the worker lifecycle, and becomes the sole authority.
9. The old runtime A is no longer authoritative and may be GC'd.
