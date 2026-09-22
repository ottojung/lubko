# ADR 0003: Zero-free-space storage semantics for Lubko-owned mutable state

- **Status:** Accepted (design; implementation follows in child issues)
- **Date:** 2026-09-22
- **Deciders:** Lubko maintainers
- **Related issue:** #797
- **Supersedes / Superseded by:** none

## Context

Two intent records constrain this design:

- `$id-6724019853147602`: exhausting the host's general-purpose persistent
  filesystem must not prevent an already-deployed supervisor and worker from
  starting, communicating with Supabase, executing jobs, or publishing results.
- `$id-3157892460835174`: correctness must not depend on `/tmp`, `/run`, or
  any other filesystem path being tmpfs or memory-backed.

Lubko currently treats several local files as crash-durable lifecycle
authority and writes them with create/write/fsync/rename sequences
(`src/lubko/durable.py`). With zero free blocks those transitions can fail
with `ENOSPC`; silently ignoring such failures would weaken recovery and
exact-ownership guarantees. This ADR defines the coherent storage model that
the implementation issues in this workstream must follow. It changes no call
sites itself.

The scope is an **already-deployed** Lubko. Installation, first deployment,
and staging of new artifacts happen while the operator still has free space
(or fail loudly to the operator, who can free space and retry). Arbitrary
job/application filesystem writes are out of scope: Lubko must be able to
launch and supervise a job without needing free filesystem bytes itself, but
a user command that explicitly writes new files may naturally receive
`ENOSPC`.

## Inventory and classification

Root of Lubko-owned state is `$XDG_STATE_HOME/lubko` (fallback
`~/.local/state/lubko`, `src/lubko/state.py`), launchers under
`$XDG_BIN_HOME`/`~/.local/bin`, and operator config under `~/.config/lubko`.
All of it lives on the general-purpose persistent filesystem. The only
current exceptions are the Linux abstract control socket (kernel memory, no
pathname) and per-stream job capture spools in `/tmp` (typically
tmpfs/memory, intentionally ephemeral). No `/run` paths exist.

### Class 1 — Immutable / read-only installed and runtime data

Sealed CLI runtimes (`$STATE/cli/<commit>/`, `lubko-runtime.json`), installed
launchers, and operator-supplied config (`database.conf`, `worker.conf`,
read-only by Lubko). Written at install/deploy time only; the steady-state
runtime only reads them. Zero-free-space safe by construction, provided no
steady-state path ever writes here (GC of old CLI roots is an
operator-initiated maintenance action and may fail loudly under `ENOSPC`).

### Class 2 — Mutable correctness / crash-durable recovery authority

Small JSON files (and two symlinks) that are read after a crash and decide
ownership, spawn, retirement, and deployment transitions:

- `worker/meta.json` — maintained-worker identity (primary worker authority).
- `worker/rollback.json` — supervised-deployment mission.
- `supervisor/<token>/desired.json` — explicit run intent.
- `supervisor/<token>/state.json` — applied generation, child identity,
  spawning obligation, unresolved hold.
- `supervisor/<token>/authority.json` — replacement-blocking recovery
  authority overflow.
- `supervisor/<token>/reserved_generation.json` — last allocated generation.
- `supervisor/supervisor.pid` — exact live supervisor identity.
- `cli/current` symlink and `worker/health.json`-style stable-surface
  symlinks — active pointers.
- `toolchain.json`, `deploy/startup-contract.json`,
  `deploy/lubko-startup-definition.json`, `deploy/staging-manifest.json`,
  `deploy/pre-confirmation-startup-artifacts.json`,
  `deploy/startup-confirmation-receipt.json`, `$BIN/lubko-startup` —
  deployment/startup authority (operator-initiated transitions only).
- `supervisor/pending-request.json` and `supervisor/pending-ack/*.json` —
  one-shot install handoff (not steady-state authority).
- Per-destination durable sidecars `.lubko-durable-lock-<name>` — 0-byte
  `flock` rendezvous files; the *holdings* are kernel state, the files
  themselves are created once.

### Class 3 — Ephemeral process-incarnation state

Must never be recovery authority and must be obtainable or recreatable
without persistent-filesystem allocation: the abstract control socket,
`flock` holdings on pre-created rendezvous lock files, pipes, `pidfd` /
`/proc` liveness evidence, and in-memory generation counters. The worker
drain sentinel (`worker/drain/<incarnation>.drained`) is already only a hint,
never authority. In-flight durable temporaries (`.lubko-durable-*`,
`.rewrite.tmp`, `*.tmp`) are staging garbage by definition.

### Class 4 — Diagnostics / observability

Per-incarnation health snapshots, stable-surface symlinks, rotated worker
logs, `supervisor/status.json`, `supervisor/supervisor.log`, and
`worker/deploy.log`. Read after a crash for inspection only; never authority.
Readers already fail closed on dangling or corrupt surfaces.

### Class 5 — Job output capture

Per-stream capture spools (today `/tmp` files via `mkstemp`, appended with
`O_APPEND` without `O_CREAT`, trimmed by rewrite). The durable authority for
job output is the immutable Postgres `output_chunk` rows; the local spool
holds only the unpublished tail and is lost on reboot by design.

## Decision

### Principle 1 — Steady-state operation performs no persistent-filesystem allocation

After deployment, every action on the supervisor/worker hot path — daemon
start, crash recovery, child spawn and supervision, queue polling, job
execution, result publication, status/health publication — must be able to
complete with zero free persistent blocks. Concretely, the implementation
issues must ensure each of these paths does one of the following, in
preference order:

1. **No filesystem write at all.** Prefer kernel or memory mechanisms:
   abstract socket (already used for control), pipes, `pidfd`, in-memory
   counters, `flock` holdings on pre-created rendezvous files opened without
   `O_CREAT`.
2. **Bounded in-place rewrite of pre-reserved capacity.** Where a durable
   transition genuinely needs crash-durable bytes after exhaustion, that
   capacity is reserved *before* exhaustion (see Principle 2) and the
   steady-state write overwrites already-allocated blocks in place
   (fixed-size slot rewrite + `fsync`), requiring no new block allocation.
3. **Best-effort diagnostic write that degrades silently.** Class 4 writes
   that fail with `ENOSPC` are dropped (with an in-memory drop counter
   exposed over the control socket) and must never fail, block, or alter a
   lifecycle decision.
4. **Fail-closed per-job degradation.** If a Class 5 spool cannot be
   created or extended, exactly the offending job fails closed; the worker
   itself stays alive and keeps serving other jobs.

No path may assume any pathname is tmpfs, RAM-backed, on a separate
filesystem, or has spare blocks (per `$id-3157892460835174`). In particular,
no path may require the operator to mount `/tmp`, `/run`, or anything else
as tmpfs.

### Principle 2 — Durable authority uses bounded preallocated slots

For each Class 2 file that can be rewritten during already-deployed
steady-state operation, the implementation must, at install/deploy time
(when free space is an operator precondition, not a runtime assumption):

- preallocate a fixed-size slot (e.g. a 4 KiB record: generation counter +
  bounded payload + checksum) sized to the file's documented maximum;
- perform steady-state transitions as whole-slot in-place rewrites
  (`pwrite` of the full slot + `fsync`), never as create/rename sequences
  that allocate new directory entries and inodes.

Each slot's logical capacity is bounded and documented. When a transition
would exceed its slot (for example a rollback snapshot larger than the
reserved bound), the transition is refused with an explicit error to the
operator *before* any irreversible lifecycle action — it is never silently
truncated and never partially applied. Refusal behavior per file:

- `worker/rollback.json` / deployment snapshots: refuse the deployment or
  confirmation; the previous mission stays authoritative.
- `supervisor/<token>/state.json` / `desired.json` / `reserved_generation` /
  `supervisor.pid`: these have fixed small schemas by construction; any
  growth beyond the slot is a programming error and must fail closed (hold
  the previous value, take no lifecycle action).
- One-shot handoff files (`pending-request.json`, `pending-ack/`) belong to
  installation, not steady state; they keep create/rename semantics and may
  fail loudly to the installer under `ENOSPC`.

If a slot rewrite cannot be confirmed for any reason (including `ENOSPC`
from filesystem metadata/journal pressure despite preallocation), the
existing `durable.py` contract already governs: the value is *not* written,
the previous destination is untouched, and the caller must not advance any
irreversible lifecycle action that depended on it. That contract is
unchanged; this ADR only removes the *need* for allocation on paths where
the intents forbid depending on it.

### Principle 3 — Recovery is read-only plus kernel state

Crash recovery with zero free blocks must work because recovery:

- **reads** Class 1 and Class 2 files (reads need no allocation);
- re-verifies liveness from `pidfd` / `/proc` start-time evidence, never
  from files it must first write;
- re-establishes mutual exclusion by opening *existing* rendezvous lock
  files without `O_CREAT` and taking `flock` (kernel state, no
  allocation);
- rebinds the abstract control socket (kernel memory, no pathname);
- treats a missing drain sentinel as "not drained" (conservative,
  fail-closed) rather than requiring sentinel creation during recovery.

No recovery path may create files, create directories, rotate logs, or
publish health/status as a precondition for resuming supervision. Deferred
maintenance (directory creation for new incarnation artifacts, log rotation,
health publication) happens opportunistically when writes succeed and is
skipped without effect on authority when they do not.

### Principle 4 — Job capture leaves the persistent filesystem

Class 5 spools move off any filesystem path: capture into bounded
anonymous memory (`memfd_create`, which needs no mount, no pathname, and no
tmpfs assumption) sized by the existing `LUBKO_OUTPUT_SPOOL_MAX_BYTES`
bound, with Postgres `output_chunk` rows remaining the only durable
authority. Trimming then frees memory instead of rewriting files. If memory
for a spool cannot be obtained, exactly that job fails closed
(`STOP_REASON_SPOOL` semantics); the worker continues. This simultaneously
satisfies `$id-6724019853147602` (no persistent bytes needed to run jobs)
and `$id-3157892460835174` (no tmpfs assumption for correctness — the
current `/tmp` spool placement is the one existing tmpfs dependence and it
is removed rather than relied upon).

The `agents/<id>/output.log` capture is per-agent-session state outside
supervisor/worker lifecycle authority; agent sessions are operator-invoked
while free space is an operator concern, so it keeps ordinary file
semantics and may fail loudly.

### Principle 5 — Fail-closed lifecycle invariants are preserved exactly

- **No duplicate maintained worker:** worker identity still resolves from
  `worker/meta.json` plus live-process evidence; when the identity write
  cannot be confirmed, no spawn proceeds (unchanged `DurabilityError`
  semantics).
- **No unowned spawned user process:** the pre-spawn spawning obligation in
  `supervisor/<token>/state.json` is a Class 2 slot rewrite *before* spawn;
  if it cannot be confirmed, the spawn does not happen.
- **No unsafe PID/PGID reuse decisions:** reuse decisions still require the
  exact `{pid, start-time-ticks}` match against live kernel evidence; a
  missing or unconfirmable file never reads as "reusable".
- **No false durable transition after a crash:** readers still treat absent,
  torn (checksum/generation mismatch), or superseded slot contents as
  "no value", and every writer still treats an unconfirmed write as "not
  written". Slot checksums/generations replace the current torn-write
  protection lost by moving from rename-atomicity to in-place rewrite:
  a torn slot is detectable and therefore fail-closed.

## Non-goals

This ADR specifies semantics only. It does not change any call site, split
work into child issues, or alter the `durable.py` API. Implementation
issues own: slot layout and per-file bounds, the `memfd` capture rewrite,
opening rendezvous files without `O_CREAT`, Fahrenheit-scale audit of every
steady-state write path, and tests expressing these invariants within the
repository's sub-ten-second deterministic suite.
