# ADR 0001: No application-level cap on active jobs

- **Status:** Accepted
- **Date:** 2026-08-27
- **Deciders:** Lubko maintainers
- **Related issue:** #279
- **Supersedes / Superseded by:** none

## Context

Lubko's worker is a single nonblocking supervisor that owns one PostgreSQL
connection and an in-memory registry of active jobs. Each job is an independent
OS process (its own session/process group), executed directly from its recorded
argv and observed by the supervisor with `poll()`-style checks. There is no
thread, no connection, and no dedicated resource bucket allocated *per job* by
the application.

Against that design, a recurring suggestion is to add an application-level
maximum on the number of simultaneously active jobs: a `max_active_jobs`
constant, a counting semaphore guarding execution, or a fixed pool of
"slots" that jobs must occupy before they may run. The pressure behind the
suggestion is legitimate — unbounded fan-out can exhaust host CPU, memory,
file descriptors, or Postgres connections — but the proposed mechanism
conflicts with Lubko's core contract and solves the wrong layer's problem.

This record settles the question. Lubko must **not** impose an
application-level maximum on active jobs. It records why, what the real
boundary between *bounded interface state and bounded per-job output windows*
and *unbounded execution concurrency with aggregate output that scales* is, which
resource-safety mechanisms remain acceptable, how
resource boundaries are enforced, how overload is made observable, and which
capsule designs were considered and rejected.

## Decision

Lubko will not introduce any application-level upper bound on the number of
concurrently active jobs. It will continue to let any number of claimed jobs
run genuinely in parallel, bounded only by whatever process/resource limits
the surrounding container, host, or OS already enforces. The application
deliberately stays out of the business of being a scheduler.

The codebase already encodes this. `docs/protocol.md` states plainly that the
daemon holds a single connection and an unbounded in-memory registry, and that
there is **no application-level concurrency limit**. `src/lubko/health.py`
documents that "a worker supervises an unbounded number of concurrently active
jobs" and reports aggregates rather than a single job id for exactly that
reason. This ADR makes that stance an explicit, durable architectural decision
rather than an incidental consequence of the current implementation.

## Bounded interface state and per-job output windows vs. unbounded execution concurrency and aggregate output

The decision rests on a sharp separation that must be preserved in every future
change:

- **Execution concurrency** is the count of job OS processes (process groups)
  alive at once. This is intentionally **unbounded by the application**. It is a
  property of the host, not of the queue logic.

- **Output state** separates a bounded local live spool from unbounded-while-active
  history. Each job's local capture spool is capped by
  `LUBKO_OUTPUT_SPOOL_MAX_BYTES` (default 4 MiB), and each live tail / published
  chunk payload is itself bounded (the live tail is recomputed as the newest 4000
  bytes). Immutable historical chunks, however, are appended as output is
  produced, and a long-running or high-output job can accumulate **arbitrarily
  many** immutable history chunks while it stays active; they are only reclaimed
  once the job reaches a terminal state and passes the GC retention window
  (`LUBKO_GC_RETENTION_SECONDS`, `LUBKO_GC_BATCH_LIMIT`). Chunk archiving and
  retention therefore bound the *terminal* history and each *GC pass*, not the
  total per-job history of a still-active job. The aggregate live-tail, spool,
  and retained-history footprint grows both with the number of concurrently
  active jobs and with the lifetime/output of each active job. The real
  distinction is a bounded *per-job local spool* and *per-payload* size versus a
  history chunk *count* that scales with active-job output until termination,
  plus a bounded machine-readable interface surface — while the aggregate output
  and storage cost scales with concurrency.

- **Interface state** has a **bounded, fixed-size surface** and no per-job
  entries. The machine-readable health snapshot
  (`src/lubko/health.py::WorkerHealth`) publishes aggregates
  (`active_jobs`, `stopping_jobs`, `completed_jobs`), a bounded oldest-active-job
  *age* (never a job id), and bounded operational counters. It never embeds a
  job id list, command text, secret, or unbounded payload, so its serialized
  size stays fixed no matter how many jobs run. Producing it still requires
  scanning the active registry, so its *computation* cost grows with active
  count; the bound is on the published *surface* (no per-job data, fixed fields),
  not on the work behind it.

The trap to avoid is conflating the two: although the *interface surface* and
each job's *output window* are bounded per job / in size, the aggregate output,
spool, and active-row work do grow with concurrency. What is already defended is
the representational and per-job boundary — a secret-free snapshot with no
per-job entries, a bounded live tail per job, and per-pass GC bounds — which
hold no matter how many jobs run. Capping *execution* to defend the interface
would be defending a boundary that is already defended at the per-job level; the
aggregate scaling is instead left to the host/OS boundary and made observable
through the health snapshot.

## Acceptable resource-safety mechanisms

Mechanisms that bound *cost* without capping *concurrency* remain in scope and
are encouraged:

- **Fairness, not a cap, on the claim turn.** `LUBKO_CLAIM_BATCH_LIMIT` bounds
  how many pending jobs one supervisor turn will claim at once. This is a
  fairness bound that prevents a flood of pending work from starving
  heartbeats, output publication, cancellation, and finalization of already
  running jobs. It is explicitly *not* a concurrency limit: claimed jobs do not
  count against a ceiling, and an endless pending queue is serviced across many
  turns rather than rejected.

- **Lease-bounded ownership.** Every running job carries a
  `lease_expires_at` deadline refreshed by heartbeat
  (`LUBKO_LEASE_DURATION_SECONDS`, `LUBKO_LEASE_REFRESH_INTERVAL_SECONDS`). A
  crashed worker stops heartbeating; the lease truly expires; a recovery pass
  atomically marks the job failed instead of re-executing it. This bounds how
  long an *unowned* process can run, which is the actual resource-danger window
  during a crash — without capping how many *owned* processes run while healthy.

- **Pre-eviction safety margin.** `LUBKO_LEASE_SAFETY_MARGIN_SECONDS` makes the
  daemon terminate an owned process group *before* its lease can expire during a
  database outage, so there is never a live command process Lubko has knowingly
  allowed to become unowned. This bounds the blast radius of an outage.

- **Bounded per-job local spool and per-payload sizes, not a bounded total.** The
  `LUBKO_OUTPUT_SPOOL_MAX_BYTES` local capture spool and the per-payload 4000-byte
  live tail cap what *one job's live, in-flight output* contributes to disk and
  memory. The GC retention and batch limits bound how much terminal history is
  kept and how much is reclaimed per pass, but they do **not** bound a still-active
  job's accumulating history-chunk count. The aggregate output/spool storage
  therefore scales with both the number of concurrently active jobs and the
  lifetime/output of each; a single long-lived active job is not footprint-bounded.

- **One shared connection, set-based operations, deadlines, and bounded scans
  where they exist — not bounded total work.** The worker holds a *single*
  PostgreSQL connection for its whole lifetime and services every job through that
  one connection — there is no connection or thread allocated per job. Recovery,
  cancellation, GC, and claiming run as bulk, set-based statements guarded by
  `LUBKO_DB_OPERATION_TIMEOUT_SECONDS`, and the supervisor applies explicit
  per-turn batch/scan caps for *claiming*, *cancellation*, *recovery*, and *GC*
  (`LUBKO_CLAIM_BATCH_LIMIT`, `LUBKO_GC_BATCH_LIMIT`, and the scan-interval
  deadlines) where those scans actually exist. Lease heartbeat, by contrast, is
  intentionally unscoped: `bulk_refresh_leases` heartbeats **every eligible active
  root ID** in one statement (`src/lubko/worker.py::bulk_refresh_leases`), so
  heartbeat work scales with the number of active jobs. Aggregate and heartbeat
  database work can therefore scale with active rows; the benefit is one shared
  connection, set-based operations, operation deadlines, and bounded scans on the
  non-heartbeat passes — not a total that is independent of job count.

These bounds (local spool, per-payload size, set-based scans, operation deadlines)
make the parts of the system that *are* bounded predictable, while the parts that
scale — history-chunk accumulation, heartbeat, and aggregate active-row work —
are instead made observable; all are orthogonal to a concurrency ceiling.

## Resource-boundary enforcement

Where a hard ceiling on simultaneous work *is* required, it belongs to the layer
that actually owns the boundary, not to the queue logic:

- **Container / cgroup limits** on CPU, memory, and PIDs are the correct place
  to say "this machine runs at most this much." They apply uniformly to all
  processes, including Lubko's children, and they fail in a way the OS already
  understands (`ENOMEM`, `EAGAIN`, OOM kill) rather than inventing a new,
  application-specific rejection path.

- **OS process limits** (`ulimit -u`, `RLIMIT_NPROC`, `RLIMIT_NOFILE`) bound
  total process and descriptor count per user/container.

- **Per-job process groups** give every job a clean, independently killable
  boundary, so a shutdown or lease-eviction terminates exactly the work it
  owns and nothing else.

Lubko should continue to *cooperate* with these boundaries (clean
process-group termination, per-job spool windows, lease eviction) rather than
*replicate* them with an internal counter. An application counter cannot see other
processes on the host, cannot account for heterogeneous job sizes, and silently
drifts from reality on crash unless it is itself made crash-proof — which is a
harder problem than letting the OS enforce the real limit.

## Overload observability (instead of overload rejection)

Because the system will not reject work for being "too many jobs," it must make
overload *visible* so operators and the orchestrator can react. Lubko already
does this through the bounded health snapshot:

- `active_jobs` and `stopping_jobs` expose the live concurrency level directly.
- `oldest_active_job_age_seconds` exposes whether jobs are progressing or
  stalling under load.
- `min_lease_safety_remaining_seconds` exposes whether the lease-eviction
  boundary is being approached under pressure.
- `spool_held_bytes` and `capture_streams_open` expose output/storage pressure.
- `db_deadline_breach_count` and the `db_deadline_breached_at` / overdue flags
  (`cancellation_scan_overdue`, `recovery_overdue`, `gc_overdue`) expose whether
  the supervisor's periodic work is keeping up.

The design choice is deliberate: **observe and signal overload, do not silently
gate it.** A cap hides overload behind a rejection threshold and converts a
resource condition into a mysterious "job not starting" failure. Observability
preserves the system's honesty — a busy worker is reported as busy, and the
consumer of the snapshot can decide what to do (scale the host, shed load
upstream, alert) with real numbers rather than guessing why work disappeared.

## Rejected alternatives

### `max_active_jobs` constant

A single integer ceiling on concurrently running jobs was rejected.

- It converts a fairness concern into a rejection policy: jobs past the limit
  must be dropped, queued-but-not-started, or errored, each introducing a new
  failure mode the current design does not have.
- It directly contradicts the product contract that submitting several
  independent jobs lets them genuinely run at the same time.
- It hides overload: a worker at the cap looks healthy while work silently
  fails to start, defeating the observability stance above.
- It is a static number that cannot account for heterogeneous job sizes (a few
  heavy jobs vs. many light ones occupy the same "slot" count differently), so
  it both wastes capacity and fails to protect against the heavy case.
- It is crash-fragile: a process that dies while "holding" active-job count must
  reconcile that count on restart, re-introducing exactly the
  ownership/recovery problem the lease protocol already solves at the database
  layer.

### Counting semaphore around execution

A `threading.Semaphore` / `asyncio.Semaphore` guarding job startup was rejected.

- It couples resource policy to the queue/execution path and fails closed poorly:
  a semaphore acquired but never released (crash, unhandled cancellation) leaks
  capacity permanently until restart.
- It is per-process and invisible to the OS and to other tooling, so it cannot
  coordinate with container limits and duplicates a boundary the OS already
  owns.
- Held across the full lifetime of an OS process, it is strictly harder to keep
  correct than the existing lease-based ownership, which already bounds the
  dangerous (unowned) window without any in-memory counter.

### Fixed slot / worker-pool system

A pre-allocated pool of execution "slots" that jobs occupy was rejected.

- Slots impose a static capacity that does not adapt to heterogeneous resource
  needs and routinely both under-utilizes light workloads and fails to guard
  heavy ones.
- A slot map is a new piece of durable, crash-sensitive state: who refills a
  slot when its owning process dies, and how a restarted worker reconstructs the
  map, is the same recovery problem the lease/recovery design already solves at
  the database level — duplicating it inside the application is pure cost.
- Slots re-introduce per-job coupling (claim → acquire slot → run → release)
  that the current "claim and run as its own process group" model deliberately
  avoids.

All three share a root flaw: they make the application pretend to be a scheduler
for a boundary it does not own, while the bounded interface state and bounded
per-job output windows already remove any representational or per-job reason to
do so (the aggregate output and active-row scaling that remains is left to the
host/OS boundary and made observable).

## Consequences

- **Positive:** the parallel-execution contract is preserved; each job's output
  window and the machine-readable interface surface stay bounded per job / in
  size (no per-job entries, no secrets) even as concurrency grows, while the
  aggregate output, spool, and active-row work that scale with concurrency are
  made observable rather than hidden; the application does not duplicate OS-level
  scheduling or crash-recovery responsibilities.
- **Negative / required discipline:** hard ceilings, when needed, must be
  supplied by the deployment (cgroups, ulimits, host sizing). Lubko will not
  save an operator who configures an unbounded host with no OS limits; the
  health snapshot is the operator's early-warning system and must remain
  populated and consulted.
- **Future-proofing:** no future change may reintroduce an application-level
  active-job ceiling, semaphore, or slot pool without explicitly superseding
  this ADR. Rate/size/deadline bounds (claim batch, lease, GC, spool, DB
  timeout) remain the accepted shape of resource safety.

## References

- `docs/protocol.md` — "Lease, heartbeat, recovery, and the supervisor" section;
  the daemon "holds a single PostgreSQL connection and an unbounded in-memory
  registry of active jobs" and "there is no application-level concurrency limit."
- `src/lubko/health.py` — `WorkerHealth` and its bounded, aggregate,
  concurrency-aware snapshot (schema version 2).
- `src/lubko/worker.py` — supervisor loop that claims a bounded batch per turn
  but runs all claimed jobs concurrently as independent process groups.
- Issue #279 — the request to formalize this decision.
