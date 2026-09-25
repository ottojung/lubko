# ADR 0003: Zero-free-space storage semantics for Lubko-owned mutable state

- **Status:** Accepted (design; implementation follows in child issues)
- **Date:** 2026-09-22
- **Deciders:** Lubko maintainers
- **Related issue:** #797
- **Supersedes / Superseded by:** none
- **Revision note:** An earlier revision of this ADR made crash-durable
  authority bounded preallocated filesystem slots rewritten in place. That
  design was rejected in review of PR #803: in-place `pwrite` + `fsync` is
  not a portable guarantee at zero free space (metadata/journal/CoW
  allocation can still fail), and the resulting fail-closed hold would block
  job execution — contradicting #797. This revision moves crash-durable
  lifecycle authority off the local persistent filesystem entirely, into the
  already-required Supabase/PostgreSQL protocol, so steady-state progress
  requires no successful persistent-filesystem mutation.

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

### Principle 1 — Steady-state progress requires no successful persistent-filesystem mutation

After deployment, every action on the supervisor/worker hot path — daemon
start, crash recovery, child spawn and authority-independent supervision
(observation, reaping, pipe draining), queue polling, job
execution, result publication, status/health publication — must be able to
complete with zero free persistent blocks **even when every local
filesystem mutation attempted on that path fails**. Authority-gated
supervision actions (ownership-dependent signals, retire, adopt) need a
fresh database row rather than local bytes, which is available exactly
whenever job progress itself is possible (Principle 2). Concretely, no
lifecycle decision and no job-execution step may depend on a local file
write succeeding. Local filesystem writes on these paths are permitted
only as opportunistic caches whose failure is ignored (see Principle 2).
The mechanisms that carry progress are, in preference order:

1. **The Supabase/PostgreSQL protocol** for all crash-durable lifecycle
   authority (Principle 2). The worker already requires the database for
   queue claiming and result publication, so authority over the database
   adds no new availability dependency to the job path: whenever the
   worker can execute and publish jobs, it can also confirm lifecycle
   transitions.
2. **Kernel or memory mechanisms** needing no filesystem bytes: the
   abstract control socket (already used for control), pipes, `pidfd`,
   in-memory counters, `memfd` capture buffers, and `flock` holdings on
   pre-created rendezvous files opened without `O_CREAT`.
3. **Best-effort diagnostic writes that degrade silently.** Class 4 writes
   that fail with `ENOSPC` are dropped (with an in-memory drop counter
   exposed over the control socket) and must never fail, block, or alter a
   lifecycle decision.
4. **Fail-closed per-job degradation.** If a Class 5 capture buffer cannot
   be created or extended, exactly the offending job fails closed; the
   worker itself stays alive and keeps serving other jobs.

No path may assume any pathname is tmpfs, RAM-backed, on a separate
filesystem, or has spare blocks (per `$id-3157892460835174`). In particular,
no path may require the operator to mount `/tmp`, `/run`, or anything else
as tmpfs.

### Principle 2 — Crash-durable lifecycle authority lives in the existing database protocol

All Class 2 state that can change during already-deployed steady-state
operation moves from local files into the existing `lubko.jobs` table as a
new application payload kind (for example `lifecycle_authority`), one
current-state row per execution server. The table's PostgreSQL metadata is
frozen (`docs/SKILL.md`, `docs/protocol_upgrades.md`): `payload` stays
opaque text, and the new kind is a pure application-protocol evolution —
no new tables, columns, indexes, roles, grants, triggers, or functions.
Server isolation reuses the existing exact application-level server
predicates, and the row kind is permanently exempt from worker GC (GC
predicates match only job/output types; the authority row is never
terminal).

The local Class 2 files (`worker/meta.json`, `supervisor/<token>/…`,
`supervisor.pid`, `cli/current`, deployment snapshots, sidecars) become
**read-through caches, never authority**. Writers commit the database
transaction first and then attempt the local cache write, ignoring local
failure entirely (no `fsync`, no error propagation). Readers treat the
database row as the source of truth: a cache entry is usable only when its
embedded generation/epoch matches the row just read; on any disagreement
the row wins. Torn or stale caches are therefore fail-closed by
construction, and a host with zero free blocks operates correctly with an
absent or outdated cache.

**Identity, bootstrap, and discovery.** Uniqueness is mechanical and needs
no new database constraint: the authority row's `id` is a deterministic
UUID derived in application code from the execution-server name (UUIDv5
over a fixed Lubko namespace UUID plus the exact server string), and the
frozen `id uuid primary key` already rejects a second row with that `id`.
Concurrent or retried bootstraps therefore converge: each attempts
`INSERT … ON CONFLICT (id) DO NOTHING` with the neutral initial payload
(no owner, generation and epoch zero), then reads the row; exactly one
insert wins and every contender proceeds on the same row, so two daemons
can never CAS different rows and both believe they own the fencing epoch.
(A UUIDv5 collision between distinct server names is cryptographically
negligible; identical server names denote the same authority domain by
definition.)

Every fresh daemon discovers and verifies the canonical authority with no
mutable local state and no local allocation: it reads its server name from
the read-only operator-installed worker config (Class 1; reads need no
allocation), derives the authority `id` in memory, and `SELECT`s that `id`.
It then verifies, fail-closed, that the row exists (else it runs the
bootstrap insert above and re-reads), that `payload.server` equals its own
server name, that the payload kind is `lifecycle_authority` at a supported
protocol version, and that the schema validates — any mismatch means "no
usable authority": hold, never act. Every ownership-dependent or
destructive child action — signaling, killing, spawning, adopting, or
retiring — additionally requires a fresh row read inside the decision
transaction whose fencing epoch matches the acting incarnation's own
epoch, so a cache or an early read can never authorize such an action.
At zero free blocks this path performs zero local writes: config read,
in-memory UUID derivation, database reads, kernel liveness proof.

**Ordering.** The linearization point of every lifecycle transition is the
commit of a single database transaction against the authority row. Mutations
use compare-and-swap predicates on the row's contents (`UPDATE … WHERE id
= … AND generation/epoch/state = expected`), with `SELECT … FOR UPDATE`
serialization for multi-step transitions — the same transactional
discipline the worker already uses for queue claiming (`FOR UPDATE SKIP
LOCKED`). Generation allocation is an atomic increment inside the row
update; the pre-spawn obligation is a committed row state *before* the
spawn syscall; desired-state and mission transitions are row CAS operations
performed by deployctl through the same database connection it already
requires.

**Crash recovery.** A restarted daemon reads the authority row (reads need
no local allocation), re-proves liveness from `pidfd` / `/proc` start-time
evidence, and reconciles: a committed pre-spawn obligation with no live
child resolves deterministically (complete the adoption or roll it back,
exactly as the current state machine prescribes); a live child matching
the row's exact `{pid, start-time-ticks, incarnation epoch}` is adopted;
anything else is fenced, never adopted. A `boot_id` / incarnation epoch in
the row invalidates all pre-crash claims after a host reboot.

**Network-unavailable behavior.** Losing the database never causes unsafe
action, and never causes more harm than the status quo: without the
database the worker can neither claim jobs nor publish results today, so
holding lifecycle transitions adds no new outage. While disconnected, an
incarnation may perform only authority-independent work: observation
(pipe draining into memory buffers, `waitpid` reaping of naturally exited
children, liveness polling), non-destructive bookkeeping (in-memory
counters, best-effort diagnostics), and bounded buffering of unpublished
results. It must not signal, kill, retire, spawn, or adopt based on stale
authority — a partitioned incarnation that has lost its fencing epoch is
indistinguishable from a superseded one, so every ownership-dependent or
destructive signal or child action requires a fresh canonical row read
matching the local fencing epoch (see above), and without that read the
action does not happen. A naturally exited child is reaped and its
buffered result retained for publication on reconnect; no kill, retire, or
replacement decision follows until fresh authority is available. Retry
uses bounded backoff. On reconnect, the first act is a row read; any local
incarnation whose epoch no longer matches the row stands down immediately
(fencing) without touching any child process, so a partitioned old
supervisor can never signal a child from stale ownership and can never
contradict the new authority.

**Boundedness.** The authority row has a fixed small schema (generations,
epochs, exact-identity tuples, phase flags, bounded mission descriptor);
application code rejects payloads above a documented byte bound, and
transitions that would exceed it are refused before any irreversible
action, exactly as deployments are refused today. No history accumulates:
the row holds current state only (history remains in job outputs and
diagnostics). The bounded in-memory result buffer degrades per-job
fail-closed when full. One-shot install-handoff state keeps local-file
semantics — installation has network, database, and free-space
preconditions and may fail loudly — and performs the bootstrap insert
described above at install time (safe to repeat: it converges by primary
key).

### Principle 3 — Recovery needs reads plus kernel state, never local writes

Crash recovery with zero free blocks works because recovery:

- **reads** the authority row from the database and Class 1 files locally
  (reads need no allocation);
- re-verifies liveness from `pidfd` / `/proc` start-time evidence, never
  from files it must first write;
- re-establishes local mutual exclusion by opening *existing* rendezvous
  lock files without `O_CREAT` and taking `flock` (kernel state, no
  allocation) — kept only as a local fast path; cross-incarnation
  exclusion comes from the row's fencing epoch;
- rebinds the abstract control socket (kernel memory, no pathname);
- treats a missing drain sentinel as "not drained" (conservative,
  fail-closed) rather than requiring sentinel creation during recovery.

Recovery with an unreachable database is observation-only hold: the daemon
may observe and reap, but adopts, spawns, signals, kills, and retires
nothing until the first successful canonical row read plus fencing check.
No recovery path may create files, create directories, rotate logs, or
publish health/status as a precondition for resuming supervision. Deferred
maintenance (directory creation for new incarnation artifacts, log rotation,
health publication, cache rewrite) happens opportunistically when writes
succeed and is skipped without effect on authority when they do not.

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
supervisor/worker lifecycle authority; external jobs are operator-invoked
while free space is an operator concern, so it keeps ordinary file
semantics and may fail loudly.

### Principle 5 — Fail-closed lifecycle invariants are preserved exactly

- **No duplicate maintained worker:** at most one incarnation holds the
  authority row's fencing epoch. A contender commits a CAS epoch bump;
  exactly one commit wins, and the loser observes the mismatch and stands
  down without spawning. Worker identity in the row plus live-process
  proof replaces `worker/meta.json` as the ownership decision.
- **No unowned spawned user process:** the pre-spawn obligation is a
  committed row state *before* the spawn syscall; if the commit fails
  (database unreachable), the spawn does not happen. A crash between
  commit and spawn leaves a deterministic recovery obligation in the row.
- **No unsafe PID/PGID reuse decisions:** reuse decisions still require
  the exact `{pid, start-time-ticks}` match against live kernel evidence,
  now cross-checked with the row's claimed identity and epoch; a missing
  row, an epoch mismatch, or a failed proof never reads as "reusable".
- **No signal from stale ownership:** no incarnation signals, kills,
  retires, spawns, or adopts unless a fresh canonical row read taken
  inside the decision matches its own fencing epoch. A disconnected or
  superseded incarnation is limited to authority-independent
  observation (`waitpid` reaping of naturally exited children, pipe
  draining, liveness polling), non-destructive bookkeeping, and bounded
  result buffering; natural child exit is reaped and buffered for later
  publication, never followed by a kill/retire/replace decision without
  fresh authority.
- **No false durable transition after a crash:** the durable transition
  is the database commit. An uncommitted transaction is invisible to every
  recoverer; a committed one is visible to all of them. Local caches can
  be absent, stale, or torn without effect because readers validate them
  against the row generation/epoch and the row always wins.

## Non-goals

This ADR specifies semantics only. It does not change any call site, split
work into child issues, or alter the `durable.py` API. Implementation
issues own: the authority-row payload schema and CAS helpers, GC exemption
and server predicates for the new row kind, the `memfd` capture rewrite,
demoting local authority files to validated caches, opening rendezvous
files without `O_CREAT`, the disconnect hold/reconnect fencing behavior,
and tests expressing these invariants within the repository's
sub-ten-second deterministic suite.
