# Lifecycle authority state machine

This document is the authoritative design for Lubko's supervisor / lifecycle /
deployment authority. It was written **before** any refactor so the runtime
could be reshaped around an explicit model instead of accumulating
incident-shaped conditionals.

The implementation lives in `src/lubko/lifecycle_state.py`. The module is the
single source of truth for the authority invariants; the OS / DB / filesystem
code paths call into it and execute the transitions it authorizes. The existing
`lifecycle.py`, `deployctl.py`, `supervise.py` and `supervisor.py` modules keep
their exact-identity, fail-closed, generation, consumer-authority, spawn /
unresolved / recovery, readiness, rollback, and deployment behavior — the
refactor centralizes the *decisions* so those properties can no longer drift
across files.

## 1. Scope of authority

The authority decides, for exactly one server, whether a queue consumer may
exist, who that consumer is, and what durable state must hold before any
destructive side effect. It covers:

- desired generation / applied generation / open deployment mission;
- maintained worker ownership;
- pre-spawn obligations;
- unresolved child authority;
- recovery-worker authority;
- retirement and crash recovery;
- readiness and rollback / confirmation.

## 2. Authoritative state

`LifecyclePhase` is the high-level authority phase. It is derived, never stored;
the durable facts below are what is persisted.

| Phase                | Meaning                                                            |
| -------------------- | ------------------------------------------------------------------ |
| `UNMANAGED`          | No durable authority over any worker exists.                        |
| `OWNERSHIP_PENDING`  | Deciding maintained-worker ownership (reading / proving metadata).  |
| `SPAWN_OBLIGATION`   | A pre-spawn recovery obligation is durable.                        |
| `SPAWNING`           | `Popen` issued; child identity not yet proven.                     |
| `RUNNING`            | Exactly one proven consumer is live.                               |
| `DRAINING`           | Retirement drain requested, awaiting safe-to-reap boundary.        |
| `RETIRING`           | Emergency retirement in progress (SIGKILL escalation).             |
| `STOPPED`            | No live consumer.                                                  |
| `RECOVERING`         | Recovery-worker authority active.                                  |
| `MISSION_PENDING`    | An open deployment mission is pending confirmation / rollback.      |
| `CONFIRMED`          | Mission confirmed; candidate is the single consumer.               |
| `ROLLED_BACK`        | Mission rolled back; previous worker is the single consumer.       |

`AuthorityFacts` is the reconciled durable + observed snapshot the authority
decides on:

- `desired_generation`, `applied_generation`
- `mission_status`, `mission_generation`, `mission_commit`
- `owned_worker_pid`, `owned_worker_commit`, `owned_worker_identity_proven`
- `pre_spawn_obligation`
- `unresolved_child`
- `recovery_authority`
- `candidate_ready`
- `rollback_pending`
- `durable_malformed`

## 3. Transitions

Every transition names: (1) durable preconditions; (2) observed external facts;
(3) the durable state transition; (4) side effects that may occur only **after**
the durable transition; (5) what must remain blocking if any step crashes.

The boundary names below are the exact `failpoint` identifiers used by the
deterministic crash tests.

### 3.1 `durable_authority_write`

- **Preconditions:** the durable write target exists and its parent directory is
  owned by the invoking user; the value being written has passed strict
  shape validation (`from_dict` did not raise).
- **Observed facts:** none required before the write.
- **Durable transition:** `meta.json` / `rollback.json` / `state.json` is
  replaced atomically and confirmed durable by `write_json_durable`.
- **Side effects after:** any dependent lifecycle action that read the prior
  value may now proceed (spawn, stop, handoff).
- **Blocking on crash:** if the write did not confirm, callers must **not**
  advance; the prior durable state remains authoritative (fail closed).

### 3.2 `lock_acquisition`

- **Preconditions:** no other deployment holds `worker/.deploy.lock`.
- **Observed facts:** `fcntl.flock` returned `LOCK_EX`.
- **Durable transition:** none; the lock is an OS serializer, not durable state.
- **Side effects after:** guard-to-mutation steps may run inside the held lock.
- **Blocking on crash:** the lock is released by the kernel on process death; a
  crash mid-locked leaves the next acquisition free and the prior durable state
  intact.

### 3.3 `popen`

- **Preconditions:** a pre-spawn obligation is durable (or a supervised intent
  is applied); no unresolved earlier consumer/process fate exists.
- **Observed facts:** the child `Popen` object exists with a real PID.
- **Durable transition:** none yet — the child is not authoritative until its
  identity is proven and published.
- **Side effects after:** the child is converge-and-reap monitored until it
  establishes a private session.
- **Blocking on crash:** a crash between `Popen` and identity proof must yield a
  child that is either proven and adopted, or converged and reaped — never left
  unresolved.

### 3.4 `child_identity_publication`

- **Preconditions:** the child is in a private session (`pgid == pid == sid`)
  and carries the exact lifecycle token, or it is converged and reaped.
- **Observed facts:** `process_identity` returns a private-session identity, or
  `proc.poll()` is not `None`.
- **Durable transition:** `WorkerMeta` is built from the proven identity.
- **Side effects after:** the replacement may be verified, the previous worker
  may be retired, and metadata may be published.
- **Blocking on crash:** an unproven child is never published; it is converged
  exactly before any other worker starts.

### 3.5 `metadata_publication`

- **Preconditions:** the replacement worker is verified alive and reaches
  PostgreSQL.
- **Observed facts:** `worker_alive(new_meta)` is `True`.
- **Durable transition:** `meta.json` is written with the new proven identity.
- **Side effects after:** the previous worker may be retired.
- **Blocking on crash:** if the write did not confirm, the previous worker stays
  the only consumer (fail closed); no second consumer is published.

### 3.6 `process_retirement`

- **Preconditions:** exact-identity proof (PID + start-time ticks + PGID + SID +
  lifecycle token) for the target; fail closed when the token is missing.
- **Observed facts:** `_worker_process_alive` flips to `False` (exit or proven
  drain sentinel).
- **Durable transition:** none until the exact process is gone; then no consumer
  remains unresolved.
- **Side effects after:** the replacement may be published; the CLI pointer may
  move.
- **Blocking on crash:** retirement is never claimed while the exact process is
  still alive; a crash mid-retire leaves the old consumer authoritative.

### 3.7 `db_recovery`

- **Preconditions:** the database is reachable and the recovery query succeeds.
- **Observed facts:** owned command groups are proven dead before removal.
- **Durable transition:** the retired child and its obligation are cleared only
  after recovery proves each group dead.
- **Side effects after:** the supervisor may clear the retired child.
- **Blocking on crash:** a recovery failure is a durable *blocking* obligation;
  the supervisor must not clear the retired child (fail closed).

### 3.8 `mission_publication`

- **Preconditions:** a strictly newer `mission_generation` was allocated under
  the generation lock; no pending mission already exists.
- **Observed facts:** the supervisor owns or will own the candidate.
- **Durable transition:** `rollback.json` is written `status=pending`.
- **Side effects after:** a watchdog is forked; the candidate is started by the
  supervisor and confirmed.
- **Blocking on crash:** a crash before durable `pending` leaves no mission; a
  crash after durable `pending` leaves a mission the watchdog converges.

### 3.9 `mission_confirmation`

- **Preconditions:** the candidate is proven live and queue-ready at the mission
  generation.
- **Observed facts:** `wait_until_ready` returned `True` for the mission
  generation.
- **Durable transition:** the supervisor applies a newer desired generation for
  `commit`; `rollback.json` is archived `confirmed`.
- **Side effects after:** the CLI pointer switches to `commit`.
- **Blocking on crash:** confirmation settles on a strictly newer generation so
  the terminal mission record can never override the resulting worker.

### 3.10 `mission_rollback`

- **Preconditions:** the candidate is proven dead (or the supervisor settled the
  previous commit at a newer generation).
- **Observed facts:** `stop_worker` returned `True` and the candidate is not
  `worker_alive`, or the supervisor applied the previous commit.
- **Durable transition:** checkout restored, previous worker restarted,
  `rollback.json` archived `rolled_back`.
- **Side effects after:** the candidate CLI root is removed; the CLI pointer
  reconciles to the previous commit.
- **Blocking on crash:** rollback never mutates the checkout, restarts the
  previous worker, or records terminal state while the candidate might be alive.

## 4. Invariants (explicit)

The machine enforces these in `assert_authority_invariants`. Each maps to a
failure code and is exercised by the deterministic failpoint tests.

1. **`SINGLE_CONSUMER`** — at most one authorized queue consumer per server. The
   proven owned worker, an unresolved child, and a ready recovery worker are
   mutually exclusive; their sum is `≤ 1`.
2. **`GENERATION_MONOTONIC`** — generations never move backward and never silently
   reuse authority: `0 ≤ applied_generation ≤ desired_generation`, and a pending
   mission's generation equals the desired generation it was allocated under.
3. **`MALFORMED_NEVER_ERASED`** — malformed durable authority (`durable_malformed`)
   is never implicitly erased or trusted; the authority must fail closed rather
   than grant or remove authority.
4. **`NO_REPLACEMENT_WHILE_UNRESOLVED`** — no replacement starts while an earlier
   consumer / process fate is unresolved: a pre-spawn obligation or an unresolved
   child blocks a new spawn.
5. **`NO_SIGNAL_WITHOUT_PROOF`** — no process is signalled without an exact
   identity proof; retirement must not be claimed when the target identity is not
   proven.
6. **`CRASH_CONVERGES_TO_ONE_OR_ZERO`** — every crash point converges to one
   consumer or zero consumers, never two (same as `SINGLE_CONSUMER` evaluated at
   every crash boundary).
7. **`NO_LIVE_CONSUMER_WITHOUT_AUTHORITY`** — there is never a state with a
   potentially live consumer and no durable replacement-blocking authority: a
   proven owned worker requires non-malformed durable commit authority.

## 5. Runtime integration

`lifecycle.py` and `deployctl.py` call the model:

- `_supervised_mutation_blocker()` routes to
  `mutation_blocker_reason()`, a behavior-preserving re-expression of the
  rollback-state guard.
- `_refuse_version_changing_deploy()` routes to `refuses_version_change()`.
- Each §3 boundary emits a `failpoint(name)` call (no-op by default) immediately
  before its side effect, so deterministic tests can inject a crash and assert
  the §4 invariants hold.
