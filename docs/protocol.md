# Lubko transport protocol and database binding specification

Status: authoritative for protocol v4.

## The two-column invariant

The Lubko transport table `lubko.jobs` has **exactly two columns forever**:

| Column    | Type         | Constraint                          | Meaning                                    |
| --------- | ------------ | ----------------------------------- | ------------------------------------------ |
| `id`      | `uuid`       | primary key, `default gen_random_uuid()` | unique random identifier              |
| `payload` | `text`       | `not null`, must hold a JSON object | one string containing a JSON object |

All evolving job/request/result/state/cancellation/process-identity/output
data lives inside the `payload` JSON object. **Never add a third column.**
Schema evolution happens inside `payload`; incompatible changes bump the
protocol version (`v`).

The payload column is opaque text at rest. SQL casts `payload::jsonb` only
transiently — for predicates and for atomic `jsonb_set` updates — and every
write stores `::text` back. Constraints are **type-aware**:

```sql
constraint jobs_payload_is_json_object check (jsonb_typeof(payload::jsonb) = 'object')
constraint jobs_payload_has_version     check ((payload::jsonb) ? 'v')
constraint jobs_payload_type_shape      check (
    case
        when (payload::jsonb)->>'type' = 'command' then
            ((payload::jsonb)->'v') = '4'::jsonb
            and jsonb_typeof((payload::jsonb)->'request') = 'object'
            and (((payload::jsonb)->'state'->>'status') is not null)
            and coalesce(jsonb_typeof((payload::jsonb)->'server'), '') = 'string'
            and coalesce((payload::jsonb)->>'server', '') <> ''
        when (payload::jsonb)->>'type' = 'output_chunk' then
            ((payload::jsonb)->'v') = '4'::jsonb
            and jsonb_typeof((payload::jsonb)->'value') = 'string'
            and (((payload::jsonb)->>'thread') is not null)
            and coalesce(jsonb_typeof((payload::jsonb)->'server'), '') = 'string'
            and coalesce((payload::jsonb)->>'server', '') <> ''
            and (((payload::jsonb)->>'stream') in ('stdout', 'stderr'))
            and (((payload::jsonb)->>'sequence') ~ '^[0-9]+$')
            and (((payload::jsonb)->>'start') ~ '^[0-9]+$')
            and (((payload::jsonb)->>'end') ~ '^[0-9]+$')
        else true
    end
)
```

The SQL constraint is deliberately generic; it does not encode `request.process`
or the legacy-field prohibition. Those rules are enforced by the payload parser
(`src/lubko/protocol.py`): a v4 `command` request must carry a process array,
and carrying the legacy `command`/`args` keys is rejected. The required
non-empty top-level `server` field, however, is enforced by both the parser and
the SQL constraint: there is **no implicit or default server**. Because the v3
to v4 breaking change is content-only, the physical two-column table does not
change between versions.

`command` rows carry a runnable lifecycle; immutable `output_chunk` rows carry
explicit ownership and offset shape. Chunk rows never carry fake command
lifecycle state. Claim and lease-recovery queries operate only on
`type = 'command'` rows.

## Startup schema verification

The v4 worker verifies more than the two-column invariant before starting. It
also requires the type-aware `jobs_payload_type_shape` constraint and the chunk
ownership/ordering indexes to be present, because immutable `output_chunk`
publication is impossible without them. Any table lacking this canonical
protocol v4 shape is refused at startup with a clear diagnostic pointing at
the idempotent baseline `migrations/0001_two_column_protocol.sql`. This keeps
output publication from failing at runtime on a table that cannot represent
immutable chunks.

## Physical schema

```sql
create table lubko.jobs (
    id uuid primary key default gen_random_uuid(),
    payload text not null
        constraint jobs_payload_is_json_object check (jsonb_typeof(payload::jsonb) = 'object')
        constraint jobs_payload_has_version check ((payload::jsonb) ? 'v')
        constraint jobs_payload_type_shape check (...)
);

create index jobs_queue_idx
    on lubko.jobs (
        ((payload::jsonb)->>'server'),
        ((payload::jsonb)->'state'->>'status'),
        ((payload::jsonb)->'state'->>'created_at')
    )
    where ((payload::jsonb)->>'type') = 'command';

create index jobs_chunk_owner_idx
    on lubko.jobs (((payload::jsonb)->>'thread'))
    where ((payload::jsonb)->>'type') = 'output_chunk';

create index jobs_chunk_order_idx
    on lubko.jobs (((payload::jsonb)->>'thread'), (((payload::jsonb)->'sequence')::bigint))
    where ((payload::jsonb)->>'type') = 'output_chunk';
```

## Worker role access (part of the binding)

`lubko_worker` is the stable role the worker connects as (see the README
database configuration) and must hold the privileges it needs to claim, cancel,
poll, publish output (including inserting immutable `output_chunk` rows),
finalize jobs, and collect transport garbage:

```sql
grant usage on schema lubko to lubko_worker;
grant select, insert, update, delete on table lubko.jobs to lubko_worker;
```

The baseline migration `migrations/0001_two_column_protocol.sql` applies these
`GRANT`s (guarded by `to_regrole` so a fresh environment without the role does
not fail). `GRANT` is idempotent, so re-applying the baseline repairs the
access contract.

## Versioning

- `payload.v` is a required integer protocol version.
- Version `4` is the current binding. Within a version, fields may be added
  additively. Breaking changes (renaming or removing fields, changing types or
  semantics) require a new version and a new worker generation.
- **The v3 → v4 cutover is destructive.** Version `3` is unsupported: v4
  parsers, builders, and workers accept only payloads carrying a required
  non-empty top-level `server` string field, which v3 rows lack; there is **no
  compatibility path and no data migration**. The two-column transport schema
  itself does not change between versions — only the payload-shape constraint
  and the routing-aware queue index do — so the cutover replaces the constraint
  via `migrations/0003_protocol_v4_server_routing.sql`. A valid procedure is:
  quiesce new submissions, let any in-flight work become durably terminal,
  `truncate lubko.jobs` while quiescent (the row cutover is destructive: every
  old root `command` row and its `output_chunk` history is discarded, and there
  is no default server), apply the migration (drop + recreate + validate the
  constraint and rebuild the queue index), then start the daemon with its
  configured server identity and prove a fresh v4 round trip. Applying the
  migration against a table that still holds nonconforming rows fails fast with
  an explicit diagnostic instead of leaving the transport half-upgraded;
  truncating first is what makes validation trivially succeed.
- A v4 worker rejects any payload whose version it does not understand; the job
  is failed with a diagnostic instead of being stuck in the queue.

## Server routing

Protocol v4 introduces explicit execution-server routing. Every daemon runs
with exactly one configured non-empty server identity (the `server` setting
of its restricted worker configuration file; it refuses to start without
one), and every valid payload carries that identity:

- **Claims** select only `pending` `command` rows whose `server` exactly equals
  the daemon's identity. Jobs addressed to other servers stay pending,
  untouched, indefinitely.
- **Output publication** stamps each inserted chunk and live-tail update with
  the daemon's server and retains the root with both a row-level lock and an
  exact server match, so a daemon can never mutate another server's row.
- **Cancellation** requires the expected server identity in its CAS predicate:
  a row whose server differs is never touched and the request fails loudly.
- **Finalization, heartbeat, lease recovery, and GC** are likewise guarded by
  the exact server match, so two daemons configured with different servers can
  share one physical queue without ever executing, mutating, or collecting each
  other's jobs.

The match is a strict text equality against a JSON **string**; the SQL
predicates check `jsonb_typeof((payload::jsonb)->'server') = 'string'` before
comparing, so a row with `server: 123` can never alias-match `"123"`. There is
no implicit or default server anywhere in the system.

## Context-safety contract

Every individual job payload and output chunk has a strict maximum size, and
every documented orchestrator polling/read operation has a bounded result
size. Checking one root job by ID is always safe and useful: the root row
always contains current lifecycle state plus a substantial recent rolling
output window, independent of chunk rotation. Literally arbitrary SQL is not
bounded; the guarantee covers Lubko's row representation and documented
workflows.

## Protocol v4 payload kinds

Version 4 defines exactly two kinds:

- `command` — a runnable root job;
- `output_chunk` — an immutable, explicitly owned historical output chunk.

### `command` rows

A `command` payload is a JSON object:

```json
{
  "v": 4,
  "type": "command",
  "server": "alpha-server",
  "request": {
    "cwd": "/workspace/project",
    "process": ["git", "status", "--short"]
  },
  "state": {
    "status": "pending"
  }
}
```

While a job runs (and once it is terminal) the row carries a bounded rolling
live output window:

```json
{
  "v": 4,
  "type": "command",
  "server": "alpha-server",
  "request": { "cwd": "/workspace/project", "process": ["ls", "/etc"] },
  "state": { "status": "running", "...": "..." },
  "output": {
    "stdout": {
      "tail": "<LAST UP TO 4000 CHARACTERS>",
      "start": 16342,
      "end": 20342,
      "previous": "<UUID OF PREVIOUS IMMUTABLE STDOUT CHUNK>"
    },
    "stderr": {
      "tail": "<LAST UP TO 4000 CHARACTERS>",
      "start": 0,
      "end": 1217,
      "previous": null
    }
  }
}
```

The live tail is **never shortened by archival rotation**: once a stream has at
least 4000 raw bytes of output, normal root-row reads continue to expose the
latest 4000-byte window, and archiving old output is observationally
invisible to a normal `SELECT` of the root job. Overlap between the live tail
and the latest immutable chunk is intentional and represented unambiguously by
byte offsets.

#### `server` — routing identity

Required non-empty string on every `command` and `output_chunk` payload. It
names the execution server that owns and may execute the row. There is no
implicit or default server: submitters must name the target server explicitly,
and a daemon claims only rows whose `server` exactly equals its configured
identity (see [Server routing](#server-routing)).

#### `request` — immutable submission

Required object.

| Field     | Type            | Required | Meaning                                |
| --------- | --------------- | -------- | -------------------------------------- |
| `cwd`     | string          | yes      | absolute working directory for the job |
| `process` | non-empty array of non-empty strings | yes | argv executed directly, never through a shell |

`process` is the **only** executable field in protocol v4. The worker executes
its elements verbatim as the child process argv in `cwd`; it never interprets
them with a shell, so shell metacharacters are passed to the program literally.
To obtain shell semantics the submitter must select a shell interpreter
explicitly, for example `"process": ["/bin/sh", "-c", "<snippet>"]`.

The legacy v2 request fields `command` and `args` remain **forbidden**. A v4
payload carrying either key is rejected outright — even when a valid
`request.process` is also present — never silently ignored. Other unknown
additive fields may be retained additively within a version, but `command` and
`args` are explicitly excluded as removed protocol fields.

#### `state` — mutable lifecycle

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
| `worker_incarnation`  | string   | unique per-daemon-lifetime identity, recorded at claim |
| `lease_expires_at`    | timestamp | UTC ISO-8601 lease deadline; refreshed by the owner's heartbeat while running |
| `recovered_at`        | timestamp | UTC ISO-8601, set when a recovery pass marks a stale running job failed |
| `process_pid`         | integer  | exact PID of the spawned process while running       |
| `process_pgid`        | integer  | exact process group of the spawned process           |
| `cancel_requested_at` | timestamp | UTC ISO-8601, set by the orchestrator to cancel      |

A submitted job must carry `state.status = "pending"`; the worker only claims
`pending` `command` rows.

#### `output` — bounded live tails

Optional object present once the worker publishes output. Each stream window
holds the decoded text of the newest at most 4000 bytes (`tail`), the inclusive
`start` byte offset, the exclusive `end` byte offset, and the `previous` UUID
of the newest immutable chunk for that stream (or `null`). Byte offsets make
gaps and intentional overlap mechanically detectable.

#### `result` — terminal data

Optional object, set when the job is terminal. `result.stdout`/`stderr` are
bounded to the final live tail text, decoded from at most 4000 raw bytes
(therefore at most 4000 characters).

| Field                | Type   | Meaning                                     |
| -------------------- | ------ | ------------------------------------------- |
| `stdout`             | string | bounded captured standard output tail       |
| `stderr`             | string | bounded captured standard error tail        |
| `exit_code`          | integer | process exit code, or negative on a signal  |
| `cancellation_note`  | string | human-readable cancellation diagnostic      |
| `recovery_note`      | string | human-readable diagnostic when a recovery pass marked a stale running job failed |

### `output_chunk` rows

Historical stdout/stderr is stored as immutable rows in the same two-column
table:

```json
{
  "v": 4,
  "type": "output_chunk",
  "server": "alpha-server",
  "thread": "<ROOT JOB UUID>",
  "stream": "stdout",
  "sequence": 17,
  "start": 15342,
  "end": 19342,
  "value": "<IMMUTABLE OUTPUT>",
  "previous": "<PREVIOUS CHUNK UUID OR NULL>"
}
```

Properties:

- chunks are immutable once inserted;
- `thread` explicitly identifies the owning root job;
- `stream` distinguishes `stdout` from `stderr`;
- `sequence` gives deterministic ordering/debuggability;
- `start`/`end` are logical byte offsets of the raw captured stream, so chunks
  are contiguous from offset zero and gaps are mechanically detectable;
- `previous` allows direct backwards traversal from the root's current tail;
- chunk values are at most 2000 characters.

Offsets are byte offsets into the raw captured byte stream. `tail`/`value`
text is the UTF-8 decoding of the corresponding byte range with replacement,
so a multi-byte character split across a boundary renders as U+FFFD at that
boundary.

#### Reading more history

Use structured JSON predicates and deterministic ordering, never substring
matching:

```sql
select id, payload
from lubko.jobs
where (payload::jsonb)->>'type' = 'output_chunk'
  and (payload::jsonb)->>'thread' = '<JOB UUID>'
order by ((payload::jsonb)->'sequence')::bigint desc
limit 4;
```

The orchestrator can also follow the `previous` UUID from the root tail for
exact backwards traversal.

#### Cleanup

Cleaning up a root job must clean up every chunk belonging to it, using
explicit ownership rather than recursively trusting the pointer chain. Delete
the root first, then the owned chunks, in one transaction:

```sql
delete from lubko.jobs where id = '<ROOT JOB UUID>';

delete from lubko.jobs
where (payload::jsonb)->>'type' = 'output_chunk'
  and (payload::jsonb)->>'thread' = '<ROOT JOB UUID>';
```

Deleting the root before the chunks serializes cleanup with output
publication, which retains the root `command` row with a row-level lock before
inserting new chunks. A single unordered `DELETE` may scan and delete chunk
rows before acquiring the root lock, so chunks committed by a concurrent
publication after that statement's snapshot could survive the later root
deletion as orphans; the root-first ordering closes that window. The two
statements still run in one transaction, so a crash rolls back the whole
cleanup. This also removes orphaned chunks whose `previous` chain became
incomplete because of a crash or corruption.

#### Automatic transport garbage collection

The worker periodically collects terminal command rows and their owned output
chunks after a configurable safe retention window (`LUBKO_GC_RETENTION_SECONDS`,
default 3600). Abandoned running rows first go through existing lease recovery;
pending and running rows are never collected. Three bounded phases run in
separate transactions each cycle:

**Phase 1 — Mark** (one transaction): A bounded batch of terminal `command`
rows whose `finished_at` is older than the retention window and whose `status`
is one of `succeeded`, `failed`, or `cancelled` is selected with `FOR UPDATE
SKIP LOCKED` and atomically marked with `state.gc = true`. Publication
explicitly refuses GC-marked roots (`WHERE ... IS DISTINCT FROM 'true'`), so
no new `output_chunk` rows can be created for them after the mark commits.
Unknown or future status values are retained, not collected.

**Phase 2 — Chunk drain + root finalization** (one transaction per batch):
For each GC-marked root, a bounded batch of its owned chunks (via `thread`) is
deleted using `FOR UPDATE SKIP LOCKED`, capped at `LUBKO_GC_BATCH_LIMIT` rows.
This keeps rows, transaction size, and lock duration bounded even when a single
root owns millions of chunks. After bounded chunk deletion, if no chunks remain
for a root, the root row itself is deleted. The root only disappears after its
chunks are drained, which preserves the root-first publication-safety invariant:
the `gc` flag prevents new chunks, and the root is removed once all chunks from
the marking snapshot are gone.

**Phase 3 — Orphan cleanup** (one transaction): A bounded anti-join `SELECT`
(with `LIMIT` and `FOR UPDATE ... SKIP LOCKED`) finds `output_chunk` rows
whose owning root `command` row is absent. The comparison is cast-free
(`root.id::text = thread`), so malformed, empty, or non-UUID thread text
never causes a cast error regardless of planner predicate reordering. Matched
rows are deleted in one bounded `DELETE`. This pass is safe without root-first
ordering: the owning root is already gone, so no concurrent publication can
create new chunks for it.

All three phases run every `LUBKO_GC_INTERVAL_SECONDS` (default 60) and log
only aggregate counts of roots marked, chunks deleted, and orphans cleaned,
never job contents or secrets. The pass is idempotent under multiple workers
and restarts.

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

- **Claiming (atomic, multi-worker):** a bounded batch of pending `command`
  rows is selected with `FOR UPDATE SKIP LOCKED` and each is flipped to
  `running` with `worker_id`, `worker_incarnation`, `started_at`,
  `updated_at`, a fresh `lease_expires_at`, and (if absent) `created_at`,
  through one atomic `jsonb_set` chain stored back as `::text`. The batch
  limit is a fairness bound on one supervisor turn, never a concurrency cap.
- **Lease heartbeat:** while jobs run, the owning worker refreshes every owned
  running `command` row's `state.lease_expires_at` in one bulk statement on an
  interval, guarded by `status = 'running'`. A healthy long-running job
  therefore always shows a fresh lease and is never stolen.
- **Recovering stale running jobs (atomic, multi-worker):** a rate-limited
  recovery pass selects `command` rows whose `state.lease_expires_at` is
  present and in the past with `FOR UPDATE SKIP LOCKED`, then atomically marks
  them `failed`, records `state.recovered_at`, `state.finished_at`, and a
  `result` object whose `recovery_note` names the expired lease and the owning
  worker. Recovery never re-executes a job, so two workers can never execute
  the same job concurrently; a running job without a lease field is never
  selected and is left for manual repair. `output_chunk` rows are never
  candidates for claim or lease recovery.
- **Cancelling a pending job:** a CAS update flips `state.status` to
  `cancelled` and writes the `result` object, guarded by
  `(payload::jsonb)->'state'->>'status' = 'pending'`.
- **Cancelling a running job:** a CAS update sets `state.cancel_requested_at`
  guarded by `(payload::jsonb)->'state'->>'status' = 'running'`; the owning
  worker discovers the marker in bounded batches and signals the exact
  recorded `state.process_pgid`.
- **Publishing output:** while a job runs, the worker publishes a fresh live
  tail roughly once per second when output has changed. When enough
  historical output exists, immutable chunks are inserted and the root row's
  `previous` pointer / live-window metadata is updated **in the same
  transaction**, so a crash can never leave the root pointing at nonexistent
  history. The transaction first retains the root `command` row with a
  row-level lock: once a concurrent root deletion has committed, publication
  observes no root and inserts no chunk rows, so publication itself never
  leaves an explicitly owned orphan chunk. Live tails are always recomputed as
  the newest 4000 bytes, so archiving never shortens them.
- **Finalizing:** a CAS update writes the `result` object and the terminal
  status guarded by `(payload::jsonb)->'state'->>'status' = 'running'`.
  Cancellation wins: if `state.cancel_requested_at` is set at finalization the
  status is forced to `cancelled`.

## Lease, heartbeat, recovery, and the supervisor

The daemon is one nonblocking supervisor holding a single PostgreSQL connection
and an unbounded in-memory registry of active jobs. There is **no
application-level concurrency limit** and no thread or connection per job; each
job process runs as its own OS process/session/process group, executed directly
from its `request.process` argv (never through a shell), and the daemon
observes it with `Popen.poll()`-style checks. This is a deliberate,
durable decision (see `docs/adr/0001-no-application-concurrency-cap.md`): each
job's live/output window is bounded and the machine-readable health snapshot is a
fixed-size aggregate with no per-job entries, but aggregate output, spool, and
active-row database work still scale with the number of concurrently active jobs.
The application therefore keeps per-job and per-turn costs bounded (one shared
connection, set-based operations, explicit batch/scan/deadline caps) and leaves
the hard ceiling on *execution* concurrency to the surrounding container/host/OS
rather than acting as a scheduler. The supervisor loop services
running jobs (observe exits, escalate cancellations, publish output, finalize),
refreshes leases, runs recovery, and claims a bounded batch of new pending
jobs each turn, so an endless pending queue can never starve heartbeats,
output publication, cancellation, or finalization of already-running jobs.

Every claimed job carries a lease deadline in `state.lease_expires_at`, and the
owning worker refreshes that deadline by heartbeat while the job runs. If a
worker crashes or is restarted, its jobs stop being heartbeated; once their
lease truly expires, any worker's recovery pass atomically marks them `failed`
with a clear `result.recovery_note` instead of re-executing them. Re-executing
an abandoned job is deliberately avoided: a job may have already performed
side effects (git pushes, agent launches, deployments) that must not run twice.

During a database outage the supervisor stops claiming new jobs, keeps the
in-memory active registry, keeps reaping/observing child processes locally, and
retries the connection. It tracks local lease timing and terminates any owned
process group **before** its lease can expire, so there is never a live command
process that Lubko has knowingly allowed to become unowned according to the
database lease protocol. After reconnection it finalizes affected jobs with a
clear diagnostic when possible; otherwise stale-job recovery marks them failed.

Graceful daemon shutdown stops claiming, terminates and reaps every tracked
active process group (escalating to `SIGKILL` after the bounded grace period),
finalizes the affected jobs when PostgreSQL is available, and removes temporary
capture files.

The worker behavior is configurable through environment variables, plus the
execution-server identity which is never environmental: it is the required
non-empty `server` setting of the restricted worker configuration file
(`$XDG_CONFIG_HOME/lubko/worker.conf`, fallback `~/.config/lubko/worker.conf`,
path selectable via `LUBKO_WORKER_CONFIG`, mode `0600`); a missing, malformed,
empty, or group/world-accessible file
makes the daemon (and every deploy-side readiness probe) fail closed.

| Variable                                | Default | Meaning                                          |
| --------------------------------------- | ------- | ------------------------------------------------ |
| `LUBKO_LEASE_DURATION_SECONDS`          | `30`    | how far in the future a claim or heartbeat pushes the lease deadline |
| `LUBKO_LEASE_REFRESH_INTERVAL_SECONDS`  | `5`     | how often the worker heartbeats its running jobs  |
| `LUBKO_LEASE_RECOVERY_INTERVAL_SECONDS` | `10`    | how often a worker runs the stale-job recovery pass |
| `LUBKO_OUTPUT_PUBLICATION_INTERVAL_SECONDS` | `1` | how often changed output tails/chunks are published |
| `LUBKO_CLAIM_BATCH_LIMIT`               | `8`     | maximum claiming work in one supervisor turn (a fairness bound, never a concurrency cap) |
| `LUBKO_LEASE_SAFETY_MARGIN_SECONDS`     | `5`     | how long before lease expiry the daemon terminates an owned group during an outage |
| `LUBKO_DB_OPERATION_TIMEOUT_SECONDS`    | `15`    | statement/connect timeout bounding database operations |
| `LUBKO_GC_RETENTION_SECONDS`            | `3600`  | how long to keep terminal command rows and owned chunks before GC |
| `LUBKO_GC_INTERVAL_SECONDS`             | `60`    | how often the transport garbage collection pass runs  |
| `LUBKO_GC_BATCH_LIMIT`                  | `100`   | maximum terminal roots collected in one GC pass       |

The refresh interval must be smaller than the lease duration so a healthy
worker's lease never expires between heartbeats; the worker refuses to start
with an invalid combination. Recovery never signals process groups it does not
own: a surviving orphan process runs to completion inside the container and its
output is discarded, which keeps the pass safe from recycled process groups and
never lets it steal a live job.

## Fresh-install schema

A fresh (purged) database applies the single canonical baseline
`migrations/0001_two_column_protocol.sql`, which creates the type-aware
two-column `lubko.jobs` table, its indexes, the worker role grants, and the
invariant comment. The baseline is idempotent and safe to apply more than
once. There is no older schema, no staging table, and no rollback path: the
two-column table is the only supported binding, and the worker refuses to
start against any other shape. After the table exists, submit jobs in protocol
v4 JSON form, addressed to a specific server (there is no default):

```sql
insert into lubko.jobs (payload)
values ('{"v":4,"type":"command","server":"alpha-server","request":{"cwd":"...","process":["git","status"]},"state":{"status":"pending"}}');
```

The job runs only on the daemon whose configured server identity (the
non-empty `server` setting of its restricted worker configuration file) is
`alpha-server`; jobs addressed to other servers stay pending untouched.

Upgrading an existing transport from v3 is a destructive cutover that replaces
the payload-shape constraint via
`migrations/0003_protocol_v4_server_routing.sql`: v4 rejects every v3 payload,
because v3 rows carry no required top-level `server` routing identity. Run it
against the live queue: quiesce new submissions, let any in-flight v3 work
become durably terminal, `truncate lubko.jobs` while quiescent (discarding
every old root `command` row and `output_chunk` history — truncating before
applying is **required**; applying against nonconforming rows fails fast with
an explicit diagnostic), apply the migration, start each daemon with its
configured server identity, and prove a fresh v4 round trip.
Old v3 contents are discarded; there is no protocol-data drain/migration path,
and no v3 row is transformed or preserved. Only the payload protocol version
and the shape constraint change; the physical schema stays the canonical
two-column table.
