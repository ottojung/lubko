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

The worker uses libpq environment variables (`PGHOST`, `PGPORT`, `PGDATABASE`,
`PGUSER`, `PGPASSWORD`) for PostgreSQL.

Optional settings:

- `LUBKO_WORKER_ID` — worker identifier, default is the host name.
- `LUBKO_POLL_INTERVAL_SECONDS` — idle polling interval, default `1`.
- `LUBKO_PROCESS_POLL_INTERVAL_SECONDS` — interval for polling a running job's
  process state and its cancellation marker, default `0.1`.
- `LUBKO_CANCEL_GRACE_SECONDS` — grace period after `SIGTERM` before a running
  job's process group is force-killed, default `5`.
- `LUBKO_MAX_OUTPUT_BYTES` — maximum bytes retained from each output stream,
  default `262144`.

Jobs run through `bash -lc` directly in the container, in the directory
requested by each job. Each job is started as its own session and process
group leader.

## Cancellation

The orchestrator can cancel a job by setting its cancellation marker:

```sql
update lubko.jobs
set cancel_requested_at = now()
where id = '<job-id>' and status in ('pending', 'running');
```

A job that is still `pending` may instead be marked `cancelled` immediately,
without ever being claimed or executed:

```sql
update lubko.jobs
set status = 'cancelled',
    cancel_requested_at = now(),
    cancellation_note = 'cancelled before the worker claimed the job',
    finished_at = now(),
    updated_at = now()
where id = '<job-id>' and status = 'pending';
```

Cancellation requests are only accepted while a job is `pending` or `running`.
Already terminal jobs are left unchanged. If a request is accepted before the
worker finalizes the job, cancellation wins and the final status is
`cancelled`.

While a job runs, the worker records its exact process identity in
`process_pid` and `process_pgid`. On cancellation it sends `SIGTERM` to the
recorded process group, waits `LUBKO_CANCEL_GRACE_SECONDS`, then sends
`SIGKILL` to the group while any member remains. It never uses `pkill`,
`killall`, or process-name matching, and it never signals a group after the
tracked process is known to be fully gone. The final `cancelled` result keeps
the output accumulated so far and records a diagnostic in `cancellation_note`.

## Database schema and migrations

Schema changes live in `migrations/` as idempotent SQL files applied in
filename order, for example with `psql`:

```sh
psql "$DATABASE_URL" -f migrations/0001_job_cancellation.sql
```

Each migration is safe to apply more than once.

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
`$XDG_STATE_HOME/lubko/worker` (default `~/.local/state/lubko/worker`):

- `meta.json` — lifecycle metadata, written atomically;
- `worker.log` — appended stdout/stderr of the maintained worker;
- `deploy.log` — deployment event log;
- `.deploy.lock` — flock-protected serialization of deployments.

### Commands

```sh
lubko-deploy status
lubko-deploy deploy [--bootstrap] [--repo DIR] [--uv PATH] [--grace-seconds N]
lubko-deploy stop [--grace-seconds N]
lubko-deploy log [--lines N]
```

`lubko-deploy status` reports the current worker state, its PID/process-group/
session identity, the deployed git commit, and the log path.

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
