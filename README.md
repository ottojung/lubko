# Lubko

Lubko is a small worker that claims shell jobs from PostgreSQL and executes them
directly inside the Lubko container, honoring each job's requested working
directory.

## Development

```sh
uv sync
uv run ruff format --check .
uv run ruff check .
uv run mypy .
uv run pytest
```

## Runtime

The worker reads its PostgreSQL connection settings from a single
permission-restricted file rather than from environment variables, so no
database host, port, database name, user, or password ever appears in the
worker process environment.

The configuration file path is `$LUBKO_DATABASE_CONFIG` when set, otherwise
`$XDG_CONFIG_HOME/lubko/database.conf`, defaulting to
`~/.config/lubko/database.conf`.

The file uses a simple `key=value` format, one setting per line, with `#`
comments and blank lines ignored:

```text
# Lubko PostgreSQL connection settings.
host=db.example.com
port=5432
dbname=postgres
user=lubko_worker
password=...
```

The required settings are `host`, `port`, `dbname`, `user`, and `password`.
The file must be readable and writable only by the owning user (mode `0600`);
the worker and `lubko-deploy` refuse to use a file that is accessible by the
group or by other users.

Optional runtime settings:

- `LUBKO_WORKER_ID` — worker identifier, default is the host name.
- `LUBKO_POLL_INTERVAL_SECONDS` — idle polling interval, default `1`.
- `LUBKO_PROCESS_POLL_INTERVAL_SECONDS` — interval for polling a running job's
  process state and its cancellation marker, default `0.1`.
- `LUBKO_CANCEL_GRACE_SECONDS` — grace period after `SIGTERM` before a running
  job's process group is force-killed, default `5`.
- `LUBKO_MAX_OUTPUT_BYTES` — maximum bytes retained from each output stream,
  default `262144`.
- `LUBKO_LEASE_DURATION_SECONDS` — how far in the future a claim or heartbeat
  pushes a running job's lease deadline, default `30`.
- `LUBKO_LEASE_REFRESH_INTERVAL_SECONDS` — how often the worker heartbeats
  (refreshes) the lease of its running job, default `5`.
- `LUBKO_LEASE_RECOVERY_INTERVAL_SECONDS` — how often the worker scans for
  running jobs whose lease has expired and recovers them, default `10`.

The lease refresh interval must be smaller than the lease duration; the worker
refuses to start with an invalid combination.

`lubko-deploy` strips libpq `PG*` variables, `DATABASE_URL`, and other
credential-bearing variables from the environment it hands to a deployed
worker, so credentials are never carried in the worker process environment.

Jobs run through `bash -lc` directly in the container, in the directory
requested by each job. Each job is started as its own session and process
group leader.

### Container init (reaping PID 1)

The Lubko container must run under a real reaping PID 1 that adopts and
reaps orphaned children — for example Docker `--init` / Compose `init: true`
(tini), or `dumb-init` — so that abandoned job/agent descendants never
accumulate as zombies. Lubko intentionally ships **no process reaper of its
own**: reaping adopted children is the container runtime's responsibility.
Cancellation, stop, and kill operations in Lubko always signal only exact,
recorded process groups (never `pkill`/`killall`/name matching).

## Two-column transport invariant

The transport table `lubko.jobs` has **exactly two columns forever**:

```sql
id      uuid primary key default gen_random_uuid()
payload text not null
```

`payload` is one string containing a JSON object; all evolving job/request/
result/state/cancellation/process-identity data lives inside it. **Never add a
third column.** See `docs/protocol.md` for the versioned binding: the payload
carries a protocol version `v` (currently `1`, kind `command`) with `request`,
`state`, and `result` sections. SQL casts `payload::jsonb` only transiently for
predicates and atomic updates and stores `::text` back. The worker refuses to
start against a table that violates this invariant.

Submit a job in protocol v1 form:

```sql
insert into lubko.jobs (payload)
values ('{"v":1,"type":"command","request":{"cwd":"/workspace/project","command":"git status --short"},"state":{"status":"pending"}}')
returning id;
```

Poll it with:

```sql
select id, (payload::jsonb)->'state'->>'status' as status
from lubko.jobs
where id = '<job-id>';
```

## Cancellation

The orchestrator cancels a job by writing to its JSON payload; no extra column
is involved. A `pending` job is cancelled immediately, without ever being
claimed or executed:

```sql
update lubko.jobs
set payload = (
    jsonb_set(jsonb_set(jsonb_set(jsonb_set(jsonb_set(
        payload::jsonb,
        '{state,status}', '"cancelled"'),
        '{state,cancel_requested_at}', to_jsonb(now())),
        '{state,finished_at}', to_jsonb(now())),
        '{state,updated_at}', to_jsonb(now())),
        '{result}', '{"stdout":"","stderr":"","exit_code":null,"cancellation_note":"cancelled before the worker claimed the job"}'::jsonb)
)::text
where id = '<job-id>' and (payload::jsonb)->'state'->>'status' = 'pending';
```

A `running` job is cancelled by setting its cancellation marker:

```sql
update lubko.jobs
set payload = jsonb_set(
    jsonb_set(payload::jsonb,
        '{state,cancel_requested_at}', to_jsonb(now())),
        '{state,updated_at}', to_jsonb(now())
)::text
where id = '<job-id>' and (payload::jsonb)->'state'->>'status' = 'running';
```

Cancellation requests are only accepted while a job is `pending` or `running`.
Already terminal jobs are left unchanged. If a request is accepted before the
worker finalizes the job, cancellation wins and the final status is
`cancelled`.

While a job runs, the worker records its exact process identity in
`state.process_pid` and `state.process_pgid` inside the payload. On
cancellation it sends `SIGTERM` to the recorded process group, waits
`LUBKO_CANCEL_GRACE_SECONDS`, then sends `SIGKILL` to the group while any
member remains. It never uses `pkill`, `killall`, or process-name matching,
and it never signals a group after the tracked process is known to be fully
gone. The final `cancelled` result keeps the output accumulated so far and
records a diagnostic in `result.cancellation_note`.

## Lease, heartbeat, and recovery

A worker can claim a job and then crash or be restarted before writing the
final result, leaving the row stuck in `running`. Lubko recovers such stale
jobs automatically with a lease/heartbeat model kept entirely inside the JSON
payload:

- **Claim** — when a worker claims a pending job it writes
  `state.worker_incarnation` (a unique per-worker-process identity) and sets
  `state.lease_expires_at` to `now + LUBKO_LEASE_DURATION_SECONDS`.
- **Heartbeat** — while a job runs, the owning worker refreshes
  `state.lease_expires_at` every `LUBKO_LEASE_REFRESH_INTERVAL_SECONDS`, so a
  genuinely live long-running job always shows a fresh lease and is never
  stolen.
- **Recovery** — every `LUBKO_LEASE_RECOVERY_INTERVAL_SECONDS` each worker
  atomically scans for `running` jobs whose lease is present and expired
  (`FOR UPDATE SKIP LOCKED` plus a single JSON compare-and-swap update) and
  marks them `failed` with a clear `result.recovery_note` naming the expired
  lease and the owning worker. Recovery **never re-executes** an abandoned job,
  so two workers can never execute the same job concurrently. A running job
  without a lease field is never selected and is left for manual repair.

The lease refresh interval must be smaller than the lease duration; the worker
refuses to start with an invalid combination. Recovery never signals process
groups it does not own: a surviving orphan process runs to completion inside
the container and its output is discarded.

## Database schema and migrations

A fresh installation applies the single baseline migration
`migrations/0001_two_column_protocol.sql`, which creates the canonical
two-column `lubko.jobs` table, its checks, the queue index, the invariant
comment, and the worker role grant. Schema changes live in `migrations/` as
idempotent SQL files applied in filename order, for example with `psql`:

```sh
psql "$DATABASE_URL" -f migrations/0001_two_column_protocol.sql
```

Each migration is safe to apply more than once. There is no legacy schema or
rollback path: the two-column table is the only supported binding, and the
worker refuses to start against any other shape. See `docs/protocol.md` for
the authoritative binding.

Run with:

```sh
uv run lubko-worker
```

## Self-deployment and worker lifecycle

Upgrades are deployed through `lubko-deploy`, which validates a checkout,
replaces the previously maintained worker, and records enough exact process
identity to stop or replace that worker later — never via broad `pkill`,
`killall`, or process-name matching.

Per-user lifecycle state follows XDG conventions under
`$XDG_STATE_HOME/lubko` (default `~/.local/state/lubko`):

- `worker/meta.json` — lifecycle metadata, written atomically;
- `worker/worker.log` — appended stdout/stderr of the maintained worker;
- `worker/deploy.log` — deployment event log;
- `worker/.deploy.lock` — flock-protected serialization of deployments;
- `toolchain.json` — versioned record of the maintained `uv` executable.

### Commands

```sh
lubko-deploy status
lubko-deploy deploy [--bootstrap] [--repo DIR] [--uv PATH] [--grace-seconds N]
lubko-deploy stop [--grace-seconds N]
lubko-deploy log [--lines N]
```

`lubko-deploy status` reports the current worker state, its PID/process-group/
session identity, the deployed git commit, and the log path.

`lubko-deploy deploy` resolves the `uv` executable with this strict
precedence:

1. an explicit `--uv` argument, validated and never silently replaced;
2. `uv` found on the current PATH;
3. the `uv` executable recorded in `toolchain.json` (validated to still exist
   and be executable).

If none is usable, deployment fails with a clear, actionable error. The
resolved executable is used to run validation (`uv sync`, `ruff`, `mypy`,
`pytest`) and to start the replacement worker.

`lubko-deploy deploy`:

1. acquires an exclusive deployment lock so two deploys cannot race;
2. validates the checkout by running `uv sync` followed by
   `ruff format --check`, `ruff check`, `mypy`, and `pytest`. If any command
   fails, deployment is refused and the current worker is left untouched;
3. reads the git commit of the checkout — it never pulls, resets, stashes, or
   otherwise mutates git state;
4. starts the replacement worker detached from the invoking shell as its own
   session and process-group leader, appending its output to `worker.log`;
5. verifies the replacement is alive with an exact identity match and can reach
   PostgreSQL with a bounded timeout, rather than merely spawning it. On
   verification failure the replacement is stopped and the previous worker is
   left untouched;
6. stops the previous maintained worker using its recorded PID/process-group/
   session identity: `SIGTERM`, then a bounded `SIGKILL` while members remain;
7. atomically records the new worker's identity and the deployed commit, and
   reports the deployed git commit.

`lubko-deploy stop` terminates the maintained worker with the same precise
identity validation; it never signals a process that no longer matches the
recorded identity.

### Identity and PID reuse

A worker's metadata records its PID, process-group ID, session ID, start time
in clock ticks, and a per-deployment lifecycle token placed in the process
environment. Identity checks require every recorded field to match, so a
recycled PID can never be mistaken for the maintained worker, and stopping
only ever signals the exact recorded process group.

### Bootstrap from an unmanaged legacy worker

Before the first managed deployment, the worker is an unmanaged legacy daemon
with no lifecycle metadata. `lubko-deploy status` reports this, and both
`deploy` and `stop` refuse to claim they can stop it by identity.

The one-time migration is a single manual stop of the legacy worker, after
which the first managed deployment is started with:

```sh
lubko-deploy deploy --bootstrap
```

Subsequent upgrades replace maintained workers with no manual PID discovery.

## Agent management CLI

`lubko-agent` is the maintained interface for managed AI agent sessions inside
the Lubko container. It provides stable Lubko agent IDs, explicit working
directories, durable logs, continuation, waiting, stopping, killing, deletion,
and cleanup:

```text
lubko-agent new [--cwd DIR] --prompt TEXT [--title TEXT] [--json]
lubko-agent list [--running|--finished|--succeeded|--failed|--stopped|--killed] [--limit N] [--json]
lubko-agent status <id> [--json]
lubko-agent prompt <id> --prompt TEXT [--steer] [--json]
lubko-agent log <id> [--lines N] [--follow]
lubko-agent result <id> [--json]
lubko-agent wait <id> --timeout SEC
lubko-agent stop <id>
lubko-agent kill <id>
lubko-agent delete <id> [--force]
lubko-agent clean [--days N] [--dry-run]
lubko-agent last
```

The orchestrator deals only with Lubko agent IDs; the underlying agent
implementation, its session IDs, its process tree, and its storage are
implementation details. `my-lubko-agent` is kept as a transition alias for the
same interface.

Per-agent state lives under `$XDG_STATE_HOME/lubko/agents/<id>/` (default
`~/.local/state/lubko/agents/<id>/`) with `meta.json`, `output.log`, and a
`.lock` file serializing metadata updates. Agent IDs are stable and never
reused.

## Installing the maintained commands

The maintained entry points (`lubko-agent`, `lubko-worker`, `lubko-deploy`)
are versioned in `pyproject.toml`. In a checkout they are available through the
project virtualenv (`uv sync`); to make them available on PATH in every login
and interactive shell without a hand-maintained copy, install them into the
user bin directory with:

```sh
uv tool install --force --from /path/to/lubko lubko
```

This is wrapped by the maintained `lubko-install` command, which also verifies
that every command resolves on PATH:

```sh
lubko-install --repo /path/to/lubko
lubko-install --repo /path/to/lubko --dry-run
```

`lubko-install` targets `$XDG_BIN_HOME` or `~/.local/bin`, which is already
prepended to PATH for login and interactive shells. Rebuilding or reinstalling
the Lubko checkout recreates the commands reproducibly; no shell aliases or
`~/.local/bin` copies need to be maintained by hand.

The exact `uv` executable a successful install used is recorded, with a schema
version, in `$XDG_STATE_HOME/lubko/toolchain.json` (default
`~/.local/state/lubko/toolchain.json`). `lubko-deploy deploy` then falls back
to that recorded executable when `uv` is not on PATH, so deployments keep
working even after `uv` itself is removed from PATH. Reinstall with an explicit
path when `uv` is unavailable on PATH:

```sh
lubko-install --repo /path/to/lubko --uv /absolute/path/to/uv
```
