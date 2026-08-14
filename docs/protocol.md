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
database configuration) and must keep the same table privileges it holds on
the legacy table, so it can claim, cancel, poll, and finalize jobs:

```sql
grant select, update on table lubko.jobs to lubko_worker;
```

`migrations/0002_two_column_protocol.sql` grants `SELECT, UPDATE` on
`lubko.jobs_v2` to `lubko_worker` and mirrors every other grant the legacy
`lubko.jobs` carries; `0003` re-asserts the grant after the table is promoted.
Table privileges survive a `RENAME`, so the promoted `lubko.jobs` keeps the
required access. Both `GRANT` statements are idempotent.

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
  `state.status` to `running`, records `worker_id`, `started_at`,
  `updated_at`, and (if absent) `created_at`, all through one atomic
  `jsonb_set` chain stored back as `::text`.
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

## Live migration and cutover plan

The live legacy worker depends on the multi-column `lubko.jobs`, so the
schema is migrated in two steps with a short, coordinated cutover window.

1. **Prepare (additive, no interruption).** Apply
   `migrations/0002_two_column_protocol.sql` while the legacy worker keeps
   running. It creates `lubko.jobs_v2` (the two-column table), its checks,
   index, invariant comment, and backfills every legacy row as a protocol v1
   payload. It never touches `lubko.jobs`.
2. **Pause submissions.** The orchestrator stops submitting new jobs so no job
   is written to a table with no live reader during the cutover.
3. **Stop the legacy worker** (`lubko-deploy stop`, or the manual stop for the
   legacy unmanaged daemon). Pending/running jobs may be drained or cancelled
   first.
4. **Cutover.** Apply `migrations/0003_cutover_two_column_protocol.sql`. It
   performs a final incremental backfill of any rows that changed since step 1,
   renames `lubko.jobs` to `lubko.jobs_legacy` (kept for rollback), and
   promotes `lubko.jobs_v2` to `lubko.jobs`.
5. **Deploy the new worker** with `lubko-deploy deploy` against the same
   checkout. The new worker verifies the two-column invariant on connect and
   refuses to start against any other schema.
6. **Resume submissions** in protocol v1 JSON form:
   `insert into lubko.jobs (payload) values ('{"v":1,"type":"command","request":{"cwd":"...","command":"..."},"state":{"status":"pending"}}')`.

Steps 2–5 are the only window with no live worker; it is kept short and
coordinated. `lubko.jobs_legacy` may be dropped later once rollback is no
longer needed.
