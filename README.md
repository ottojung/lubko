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
