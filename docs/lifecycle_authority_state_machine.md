# Lifecycle authority state machine

This document is the authoritative design for Lubko's supervisor / lifecycle /
deployment authority. It was written **before** any refactor so the runtime
could be reshaped around an explicit model instead of accumulating
incident-shaped conditionals.

The implementation lives in `src/lubko/lifecycle_state.py`. The module is the
single source of truth for the authority invariants and the transition /
authorization decisions; the OS / DB / filesystem code paths call into it and
execute the decisions it authorizes. The existing `lifecycle.py`, `deployctl.py`,
`supervise.py` and `supervisor.py` modules keep their exact-identity,
fail-closed, generation, consumer-authority, spawn / unresolved / recovery,
readiness, rollback, and deployment behavior — the refactor centralizes the
*decisions* so those properties can no longer drift across files.

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
the durable facts below are what is persisted. The phase is computed by
`phase_from_facts`, which reads a reconciled `AuthorityFacts` snapshot.

| Phase                | Meaning                                                            |
| -------------------- | ------------------------------------------------------------------ |
| `UNMANAGED`          | No durable authority over any worker exists.                       |
| `OWNERSHIP_PENDING`  | Deciding maintained-worker ownership (reading / proving metadata), or durable authority is malformed/blocking. |
| `SPAWN_OBLIGATION`   | A pre-spawn recovery obligation is durable.                        |
| `SPAWNING`           | An unresolved spawned child hold is durable (recovery due).        |
| `RUNNING`            | Exactly one proven consumer is live.                               |
| `RETIRING`           | Emergency retirement in progress (SIGKILL escalation).              |
| `STOPPED`            | No live consumer.                                                  |
| `RECOVERING`         | Recovery-worker authority active.                                  |
| `MISSION_PENDING`    | An open deployment mission is pending confirmation / rollback.      |
| `CONFIRMED`          | Mission confirmed; candidate is the single consumer.               |
| `ROLLED_BACK`        | Mission rolled back; previous worker is the single consumer.        |

`AuthorityFacts` is the reconciled durable + observed snapshot the authority
decides on. It is built by `reconcile_authority_facts()`, which reads the genuine
sources (maintained `meta.json`, the rollback/deploy mission state, the desired
run intent, and the supervisor `SupervisorState`) and fails closed on
unreadable/corrupt state: an exception from any read — including
`supervise.read_state()` — sets `durable_malformed` rather than being treated as
absent authority, so the authority can never fail open. The desired generation is
read from the desired run intent (`supervise.read_desired()`), never aliased to
the applied generation, so `GENERATION_MONOTONIC` can detect desired/applied
skew:

- `desired_generation`, `applied_generation`
- `mission_status`, `mission_generation`, `mission_commit`
- `owned_worker_pid`, `owned_worker_commit`, `owned_worker_identity_proven`
- `pre_spawn_obligation`
- `unresolved_child`
- `candidate_ready`
- `rollback_pending`
- `durable_malformed`
- `supervisor_child_present`
- `ownership_hold_malformed`, `unresolved_hold_malformed`, `spawning_hold_malformed`

## 3. Transitions

Every transition names: (1) durable preconditions; (2) observed external facts;
(3) the durable state transition; (4) side effects that may occur only **after**
the durable transition; (5) what must remain blocking if any step crashes.

The boundary names below are the exact `failpoint` identifiers used by the
deterministic crash tests (see `FAILPOINT_*` in `lifecycle_state.py`). Each is a
no-op unless a test arms it.

### 3.1 `popen`

- **Preconditions:** a pre-spawn obligation is durable (or a supervised intent
  is applied); no unresolved earlier consumer/process fate exists — enforced by
  `authorize_spawn`.
- **Observed facts:** the child `Popen` object exists with a real PID.
- **Durable transition:** none yet — the child is not authoritative until its
  identity is proven and published.
- **Side effects after:** the child is converge-and-reap monitored until it
  establishes a private session.
- **Blocking on crash:** a crash between `Popen` and identity proof must yield a
  child that is either proven and adopted, or converged and reaped — never left
  unresolved.

### 3.2 `metadata_publication`

- **Preconditions:** the replacement worker is verified alive and reaches
  PostgreSQL.
- **Observed facts:** `worker_alive(new_meta)` is `True`.
- **Durable transition:** `meta.json` is written with the new proven identity.
- **Side effects after:** the previous worker may be retired.
- **Blocking on crash:** if the write did not confirm, the previous worker stays
  the only consumer (fail closed); no second consumer is published.

### 3.3 `process_retirement`

- **Preconditions:** exact-identity proof (PID + start-time ticks + PGID + SID +
  lifecycle token) for the target; fail closed when the token is missing —
  enforced by `authorize_retirement`.
- **Observed facts:** the exact process is proven gone (`NO_SIGNAL_WITHOUT_PROOF`).
- **Durable transition:** none until the exact process is gone; then no consumer
  remains unresolved.
- **Side effects after:** the replacement may be published; the CLI pointer may
  move.
- **Blocking on crash:** retirement is never claimed while the exact process is
  still alive.

### 3.4 `db_recovery`

- **Preconditions:** the database is reachable and the recovery query succeeds —
  enforced by `authorize_recovery`.
- **Observed facts:** owned command groups are proven dead before removal.
- **Durable transition:** the retired child and its obligation are cleared only
  after recovery proves each group dead.
- **Side effects after:** the supervisor may clear the retired child.
- **Blocking on crash:** a recovery failure is a durable *blocking* obligation;
  the supervisor must not clear the retired child (fail closed).

### 3.5 `mission_publish`

- **Preconditions:** no pending mission already exists; durable authority is not
  malformed — enforced by `authorize_mission_publish`.
- **Observed facts:** the supervisor owns or will own the candidate.
- **Durable transition:** `rollback.json` is written `status=pending`.
- **Side effects after:** a watchdog is forked; the candidate is started by the
  supervisor and confirmed.
- **Blocking on crash:** a crash before durable `pending` leaves no mission; a
  crash after durable `pending` leaves a mission the watchdog converges.

### 3.6 `mission_confirm`

- **Preconditions:** the candidate is proven live and queue-ready at the mission
  generation — enforced by `authorize_mission_confirm`.
- **Observed facts:** `wait_until_ready` returned `True` for the mission
  generation.
- **Durable transition:** the supervisor applies a newer desired generation for
  `commit`; `rollback.json` is archived `confirmed`.
- **Side effects after:** the CLI pointer switches to `commit`.
- **Blocking on crash:** confirmation settles on a strictly newer generation so
  the terminal mission record can never override the resulting worker.

### 3.7 `mission_rollback`

- **Preconditions:** the candidate is proven dead (or the supervisor settled the
  previous commit at a newer generation) — enforced by `authorize_mission_rollback`.
- **Observed facts:** `stop_worker` returned `True` and the candidate is not
  `worker_alive`, or the supervisor applied the previous commit.
- **Durable transition:** checkout restored, previous worker restarted,
  `rollback.json` archived `rolled_back`.
- **Side effects after:** the candidate CLI root is removed; the CLI pointer
  reconciles to the previous commit.
- **Blocking on crash:** rollback never mutates the checkout, restarts the
  previous worker, or records terminal state while the candidate might be alive.

### 3.8 `supervisor.spawning_write`

- **Preconditions:** `authorize_spawn` holds — no pre-spawn obligation, no
  unresolved child, no live consumer, and durable authority is not malformed.
- **Observed facts:** none required before the write.
- **Durable transition:** the pre-`Popen` spawning obligation is written durably
  and fsync-confirmed.
- **Side effects after:** only after this durable obligation exists may `Popen`
  run, so a supervisor death before identity proof leaves a replacement-blocking
  hold rather than a second consumer.
- **Blocking on crash:** if the write is blocked (failpoint) the spawn is held;
  no child is ever started without the durable obligation.

### 3.9 `supervisor.pid_upgrade_write`

- **Preconditions:** `check_authority_invariants` holds at the pid-upgrade
  boundary.
- **Observed facts:** the child `Popen` exists with a real PID.
- **Durable transition:** the spawning obligation is upgraded in place with the
  child's exact PID and start-time ticks.
- **Side effects after:** the child is monitored for identity proof.
- **Blocking on crash:** if the write is blocked the spawn is held; the
  obligation stays durable and replacement-blocking.

### 3.10 `supervisor.spawning_clearance`

- **Preconditions:** `check_authority_invariants` holds at the clearance boundary.
- **Observed facts:** the child identity proved the private session/group
  invariant.
- **Durable transition:** `state.child` is published and `spawning`/`unresolved_child`
  are cleared.
- **Side effects after:** the previous worker may be retired.
- **Blocking on crash:** if the write is blocked the proven child is not yet
  published; the durable obligation remains replacement-blocking.

### 3.11 `supervisor.unresolved_child_write`

- **Preconditions:** `check_authority_invariants` holds at the unresolved-child
  boundary.
- **Observed facts:** the direct child could not be positively reaped.
- **Durable transition:** an exact-identity `unresolved_child` hold is persisted
  (carrying the child's token) and the spawning obligation is cleared.
- **Side effects after:** a later tick converges the possibly-live child by
  pinned single-PID signals and recovers its command groups.
- **Blocking on crash:** the durable token-bearing hold keeps replacement
  blocked.

## 4. Invariants (explicit)

The machine enforces these in `assert_authority_invariants` (and its non-raising
sibling `check_authority_invariants`). Each maps to a failure code and is
exercised by the deterministic crash tests in
`tests/test_authority_invariants.py`.

1. **`SINGLE_CONSUMER`** — at most one authorized queue consumer per server. The
   proven owned worker, an unresolved child, and a ready recovery worker are
   mutually exclusive; their sum is `≤ 1`.
2. **`GENERATION_MONOTONIC`** — generations never move backward and never silently
   reuse authority: `0 ≤ applied_generation ≤ desired_generation`, and a pending
   mission's generation is not below the applied generation.
3. **`MALFORMED_NEVER_ERASED`** — malformed durable authority (`durable_malformed`)
   is never implicitly erased or trusted; the authority must fail closed rather
   than grant or remove authority. When corrupt with no remaining block, it is
   flagged rather than silently cleared.
4. **`NO_REPLACEMENT_WHILE_UNRESOLVED`** — no replacement starts while an earlier
   consumer / process fate is unresolved: a pre-spawn obligation or a published
   child blocks a new spawn beside an unresolved child.
5. **`NO_SIGNAL_WITHOUT_PROOF`** — no process is signalled without an exact
   identity proof; a published child whose recorded identity is not proven must
   not be signalled.
6. **`CRASH_CONVERGES_TO_ONE_OR_ZERO`** — every crash point converges to one
   consumer or zero consumers, never two (same as `SINGLE_CONSUMER` evaluated at
   every crash boundary).
7. **`NO_LIVE_CONSUMER_WITHOUT_AUTHORITY`** — there is never a state with a
   potentially live consumer and no durable replacement-blocking authority: a
   proven owned worker requires non-malformed durable commit authority.

## 5. Runtime integration

`lifecycle.py`, `deployctl.py`, and `supervisor.py` route the model:

- `_supervised_mutation_blocker()` routes to `mutation_blocker_reason()`, a
  behavior-preserving re-expression of the rollback-state guard.
- `_refuse_version_changing_deploy()` routes to `refuses_version_change()`.
- `supervisor.SupervisorDaemon._spawn_worker` routes the pre-`Popen` obligation
  write through `authorize_spawn`; the `pid_upgrade`, `spawning_clearance`, and
  `unresolved_child` durable writes are gated by `check_authority_invariants`,
  holding (fail closed) when any invariant is violated.
- `current_phase()` derives from `phase_from_facts(reconcile_authority_facts())`
  so the phase is always the faithful projection of the reconciled facts.
- Each §3 boundary emits a `failpoint(name)` call (no-op by default) immediately
  before its side effect, so deterministic tests can inject a crash and assert
  the §4 invariants hold.
