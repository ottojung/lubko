# Transport GC liveness theorem

## Goal

Transport garbage collection must never be a dependency of normal worker progress.
A slow, blocked, broken, or permanently failing GC pass may delay reclamation, but
it must not delay claiming, gated-start activation, lease refresh, cancellation,
output publication, or finalization.

## Construction

1. GC runs only in the daemon transport-gc Python thread.
2. GC owns a PostgreSQL connection that is never shared with the worker thread.
3. The worker event loop does not call GC, wait for GC, join the GC thread, or
   acquire a GC-owned synchronization primitive.
4. GC stop only sets an event; worker shutdown never waits for a GC pass.
5. Each GC pass has a hard client deadline and PostgreSQL statement_timeout.
6. Every exceptional lock wait is bounded by GC_LOCK_TIMEOUT_MS.
7. Row selection uses FOR UPDATE SKIP LOCKED.
8. LUBKO_GC_BATCH_LIMIT is capped at MAX_GC_BATCH_LIMIT = 100. Therefore any
   one GC transaction owns at most 101 row locks: one terminal root plus at
   most 100 chunks. Phase 1 and orphan cleanup own at most 100.
9. GC root locks are restricted to terminal, retention-eligible rows. Normal
   execution owns pending/running roots. GC chunk deletion is restricted to
   chunks of GC-marked or absent roots. Thus the protocol-level live-work and
   GC row-lock domains are disjoint.

## Worker liveness theorem

Assume the OS eventually schedules the worker thread and PostgreSQL eventually
services or rejects each statement on the worker's own connection. Then GC
cannot prevent the worker from completing another event-loop turn.

Proof. The worker has no control-flow edge to the GC thread and shares no DB
connection with it. Therefore a GC computation, Python exception, blocked
socket, or client deadline cannot suspend the worker thread. At the database
level, GC's protocol row-lock domain is disjoint from live worker rows;
SKIP LOCKED removes row-lock waiting within the GC domain, and the finite lock
timeout bounds exceptional table/metadata conflicts. Consequently GC adds no
unbounded wait edge to the worker's wait-for graph. Worker progress therefore
depends only on the worker's pre-existing scheduler/DB assumptions, not on GC.
QED.

## GC progress theorem

Assume PostgreSQL grants the GC connection execution often enough for a bounded
pass to finish before its hard deadline, and the set of eligible garbage is
finite during some interval. Then repeated GC passes eventually reduce that set
to zero.

Proof sketch. A successful saturated pass schedules another pass after the short
saturated cadence. Each successful phase either marks at least one new eligible
terminal root, deletes at least one chunk/orphan, or deletes a fully drained
marked root. Batch cardinalities are finite and positive. Therefore the finite
lexicographic measure (unmarked roots, chunks of marked roots, marked roots,
orphan chunks) strictly decreases across successful reclamation steps until zero.
QED.

The executable premises of these arguments live in tests/test_gc_liveness.py.
