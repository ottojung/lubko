# Lubko transport protocol and database binding specification

Status: authoritative for protocol v1.

## The two-column invariant

The Lubko transport table `lubko.jobs` has **exactly two columns forever**:

| Column    | Type         | Constraint                          | Meaning                                    |
| --------- | ------------ | ----------------------------------- | ------------------------------------------ |
| `id`      | `uuid`       | primary key, `default gen_random_uuid()` | unique random identifier              |
| `payload` | `text`       | `not null`, must hold a JSON object | one string containing a JSON object |

All evolving job/request/result/state/cancellation/process-identity data lives
inside the `payload` JSON object. **Never add a third column.** Schema
evolution happens inside `payload`; incompatible changes bump the protocol
version (`v`).

The payload column is opaque text at rest. SQL casts `payload::jsonb` only
transiently — for predicates and for atomic `jsonb_set` updates — and every
write stores `::text` back. The only enforced constraints are:

```sql
constraint jobs_payload_is_json_object check (jsonb_typeof(payload::jsonb) = 'object')
constraint jobs_payload_has_version     check ((payload::jsonb) ? 'v')
constraint jobs_payload_has_status      check (((payload::jsonb)->'state'->>'status') is not null)
```

## Physical schema

```sql
create table lubko.jobs (
    id uuid primary key default gen_random_uuid(),
    payload text not null
        constraint jobs_payload_is_json_object check (jsonb_typeof(payload::jsonb) = 'object')
        constraint jobs_payload_has_version check ((payload::jsonb) ? 'v')
        constraint jobs_payload_has_status check (((payload::jsonb)->'state'->>'status') is not null)
);

create index jobs_queue_idx
    on lubko.jobs (((payload::jsonb)->'state'->>'status'), ((payload::jsonb)->'state'->>'created_at'));
```

## Worker role access (part of the binding)

`lubko_worker` is the stable role the worker connects as (see the README
database configuration) and must hold the table privileges it needs to claim,
cancel, poll, and finalize jobs:

```sql
grant select, update on table lubko.jobs to lubko_worker;
```

The baseline migration `migrations/0001_two_column_protocol.sql` applies this
`GRANT` (guarded by `to_regrole` so a fresh environment without the role does
not fail). `GRANT` is idempotent, so re-applying the baseline repairs the
access contract.

## Versioning

- `payload.v` is a required integer protocol version.
- Version `1` is the current binding. Within a version, fields may be added
  additively. Breaking changes (renaming or removing fields, changing types or
  semantics) require a new version and a new worker generation.
- A worker rejects any payload whose version it does not understand; the job is
  failed with a diagnostic instead of being stuck in the queue.

## Protocol v1 payload

A v1 payload is a JSON object:

```json
{
  "v": 1,
  "type": "command",
  "request": {
    "cwd": "/workspace/project",
    "command": "git status --short"
  },
  "state": {
    "status": "pending"
  },
  "result": null
}
```

### `type` — job kind

Required. Version 1 defines exactly one kind:

- `command` — run a shell command (`request.command` via `bash -lc`) or an
  argv list (`request.args`, executed directly). Exactly one of `command` or
  `args` must be present; both is a validation error.

### `request` — immutable submission

Required object.

| Field     | Type            | Required | Meaning                                |
| --------- | --------------- | -------- | -------------------------------------- |
| `cwd`     | string          | yes      | absolute working directory for the job |
| `command` | non-empty string | exactly one of `command`/`args` | shell command to run through `bash -lc` |
| `args`    | non-empty array of strings | exactly one of `command`/`args` | argv-style command to exec directly |

### `state` — mutable lifecycle

Required object. Managed by the worker and, for cancellation, by the
orchestrator.

| Field                 | Type     | Meaning                                              |
| --------------------- | -------- | ---------------------------------------------------- |
| `status`              | string   | one of `pending`, `running`, `succeeded`, `failed`, `cancelled` |
| `created_at`          | timestamp | UTC ISO-8601, set on first claim if absent           |
| `updated_at`          | timestamp | UTC ISO-8601, set on every write                     |
| `started_at`          | timestamp | UTC ISO-8601, set when claimed                       |
| `finished_at`         | timestamp | UTC ISO-8601, set when terminal                      |
| `worker_id`           | string   | claiming worker identity                             |
| `worker_incarnation`  | string   | unique per-worker-process identity, recorded at claim |
| `lease_expires_at`    | timestamp | UTC ISO-8601 lease deadline; refreshed by the worker's heartbeat while running |
| `recovered_at`        | timestamp | UTC ISO-8601, set when a recovery pass marks a stale running job failed |
| `process_pid`         | integer  | exact PID of the spawned process while running       |
| `process_pgid`        | integer  | exact process group of the spawned process           |
| `cancel_requested_at` | timestamp | UTC ISO-8601, set by the orchestrator to cancel      |

A submitted job must carry `state.status = "pending"`; the worker only claims
`pending` jobs.

### `result` — terminal data

Optional object, set when the job is terminal.

| Field                | Type   | Meaning                                     |
| -------------------- | ------ | ------------------------------------------- |
| `stdout`             | string | captured standard output (may be truncated) |
| `stderr`             | string | captured standard error                     |
| `exit_code`          | integer | process exit code, or negative on a signal  |
| `cancellation_note`  | string | human-readable cancellation diagnostic      |
| `recovery_note`      | string | human-readable diagnostic when a recovery pass marked a stale running job failed |

### Timestamps

Timestamps are canonical UTC ISO-8601 with microseconds and a trailing `Z`,
for example `2026-08-14T08:40:03.610411Z`. The canonical form is produced by:

```sql
to_char(now() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
```

Because the format is fixed-width, lexicographic ordering of the stored text
equals chronological ordering, which the claim query relies on.

## Concurrency and cancellation without extra columns

All queue mechanics are implemented with the two columns only, using
PostgreSQL compare-and-swap (CAS) predicates plus row locking:

- **Claiming (atomic, multi-worker):** the claim transaction selects the
  oldest `pending` row with `FOR UPDATE SKIP LOCKED`, then flips
  `state.status` to `running`, records `worker_id`, `worker_incarnation`,
  `started_at`, `updated_at`, a fresh `lease_expires_at`, and (if absent)
  `created_at`, all through one atomic `jsonb_set` chain stored back as
  `::text`.
- **Lease heartbeat:** while a job runs, the worker rewrites
  `state.lease_expires_at` (and `state.updated_at`) on an interval, guarded by
  `status = 'running'`. A healthy long-running job therefore always shows a
  fresh lease and is never stolen.
- **Recovering stale running jobs (atomic, multi-worker):** a rate-limited
  recovery pass selects `running` rows whose `state.lease_expires_at` is
  present and in the past with `FOR UPDATE SKIP LOCKED`, then atomically marks
  them `failed`, records `state.recovered_at`, `state.finished_at`, and a
  `result` object whose `recovery_note` names the expired lease and the owning
  worker. Recovery never re-executes a job, so two workers can never execute
  the same job concurrently; a running job without a lease field is never
  selected and is left for manual repair.
- **Cancelling a pending job:** a CAS update flips `state.status` to
  `cancelled` and writes the `result` object, guarded by
  `(payload::jsonb)->'state'->>'status' = 'pending'`.
- **Cancelling a running job:** a CAS update sets `state.cancel_requested_at`
  guarded by `(payload::jsonb)->'state'->>'status' = 'running'`; the worker
  polls that marker and signals the exact recorded `state.process_pgid`.
- **Finalizing:** a CAS update writes the `result` object and the terminal
  status guarded by `(payload::jsonb)->'state'->>'status' = 'running'`.
  Cancellation wins: if `state.cancel_requested_at` is set at finalization the
  status is forced to `cancelled`.

## Lease, heartbeat, and recovery

Every claimed job carries a lease deadline in `state.lease_expires_at`, and the
owning worker refreshes that deadline by heartbeat while the job runs. If a
worker crashes or is restarted, its jobs stop being heartbeated; once their
lease truly expires, any worker's recovery pass atomically marks them `failed`
with a clear `result.recovery_note` instead of re-executing them. Re-executing
an abandoned job is deliberately avoided: a job may have already performed
side effects (git pushes, agent launches, deployments) that must not run twice.

The worker behavior is configurable through environment variables:

| Variable                                | Default | Meaning                                          |
| --------------------------------------- | ------- | ------------------------------------------------ |
| `LUBKO_LEASE_DURATION_SECONDS`          | `30`    | how far in the future a claim or heartbeat pushes the lease deadline |
| `LUBKO_LEASE_REFRESH_INTERVAL_SECONDS`  | `5`     | how often the worker heartbeats its running job  |
| `LUBKO_LEASE_RECOVERY_INTERVAL_SECONDS` | `10`    | how often a worker runs the stale-job recovery pass |

The refresh interval must be smaller than the lease duration so a healthy
worker's lease never expires between heartbeats; the worker refuses to start
with an invalid combination. Recovery never signals process groups it does not
own: a surviving orphan process runs to completion inside the container and its
output is discarded, which keeps the pass safe from recycled process groups and
never lets it steal a live job.

## Fresh-install schema

A fresh installation applies the single baseline migration
`migrations/0001_two_column_protocol.sql`, which creates the canonical
two-column `lubko.jobs` table (with its checks, index, invariant comment, and
the worker role grant). The migration is idempotent and safe to apply more
than once.

There is no legacy schema, no staging table, and no rollback path: the
two-column table is the only supported binding, and the worker refuses to
start against any other shape. After the table exists, submit jobs in
protocol v1 JSON form:

```sql
insert into lubko.jobs (payload)
values ('{"v":1,"type":"command","request":{"cwd":"...","command":"..."},"state":{"status":"pending"}}');
```
