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
- `LUBKO_LEASE_DURATION_SECONDS` — how far in the future a claim or heartbeat
  pushes a running job's lease deadline, default `30`.
- `LUBKO_LEASE_REFRESH_INTERVAL_SECONDS` — how often the worker heartbeats
  (refreshes) the leases of its running jobs, default `5`.
- `LUBKO_LEASE_RECOVERY_INTERVAL_SECONDS` — how often the worker scans for
  running jobs whose lease has expired and recovers them, default `10`.
- `LUBKO_OUTPUT_PUBLICATION_INTERVAL_SECONDS` — how often changed output
  tails/chunks are published, default `1`.
- `LUBKO_CLAIM_BATCH_LIMIT` — maximum claiming work in one supervisor turn, a
  fairness bound (never a concurrency cap), default `8`.
- `LUBKO_LEASE_SAFETY_MARGIN_SECONDS` — how long before lease expiry the
  daemon terminates an owned process group during a database outage, default `5`.
- `LUBKO_DB_OPERATION_TIMEOUT_SECONDS` — statement/connect timeout bounding
  database operations, default `15`.

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
result/state/cancellation/process-identity/output data lives inside it.
**Never add a third column.** See `docs/protocol.md` for the versioned binding:
the payload carries a protocol version `v` (currently `2`) with `command` rows
(`request`, `state`, optional terminal `result`, and bounded live `output`
tails) and immutable `output_chunk` rows. SQL casts `payload::jsonb` only
transiently for predicates and atomic updates and stores `::text` back. The
worker refuses to start against a table that violates this invariant.

Submit a job in protocol v2 form:

```sql
insert into lubko.jobs (payload)
values ('{"v":2,"type":"command","request":{"cwd":"/workspace/project","command":"git status --short"},"state":{"status":"pending"}}')
returning id;
```

Poll it with:

```sql
select id, (payload::jsonb)->'state'->>'status' as status
from lubko.jobs
where id = '<job-id>';
```

While a job runs, the root row carries a bounded rolling live output window in
`payload.output` — the newest up to 4000 raw bytes of stdout/stderr per
stream, plus byte offsets and a `previous` pointer to the newest immutable
chunk. Historical output is archived into immutable `output_chunk` rows in the
same two-column table, keyed by explicit `thread` ownership. Publication
retains the root `command` row with a row-level lock in the same transaction,
so a root deleted concurrently leaves no new chunk rows. Archiving never
shortens the live tail, and every payload Lubko writes is strictly bounded.
See `docs/protocol.md` for reading history and cleaning up chunks.

## Concurrent jobs

The worker is a single nonblocking supervisor. It holds one PostgreSQL
connection and an in-memory registry of active jobs; each shell command runs as
its own OS process/session/process group, and the daemon never allocates a
thread or a connection per job and never synchronously waits for any one child.
There is **no application-level concurrency limit**: 2, 20, or 200 independent
pending jobs can all run at the same time if the host can start them. If the
OS refuses to start a particular process, that job fails clearly and
independently while the daemon stays alive to supervise the jobs that did
start. A database outage stops new claims but keeps local supervision and
terminates any owned process group before its lease can expire, so a live
command process is never knowingly allowed to become unowned.

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

## Database schema

Lubko uses a single canonical baseline migration for a fresh (purged)
database:

```sh
psql "$DATABASE_URL" -f migrations/0001_two_column_protocol.sql
```

The baseline is idempotent and safe to apply more than once. It creates the
canonical two-column `lubko.jobs` table with its type-aware checks, the
command queue index, the output-chunk ownership/ordering indexes, the
invariant comment, and the worker role grant. There is no older schema and no
rollback path: the two-column table is the only supported binding, and the
worker verifies the protocol v2 output-chunk shape at startup, refusing to
run against any other table. See `docs/protocol.md` for the authoritative
binding.

The baseline grants the `lubko_worker` role everything protocol v2 needs:
`USAGE` on the `lubko` schema and `SELECT`, `INSERT`, `UPDATE` on
`lubko.jobs` (INSERT is required to publish immutable `output_chunk` rows).
The grant is guarded by `to_regrole`, so applying the baseline before the role
is provisioned does not fail.

Run the worker with:

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
2. requires a clean checkout (`git status --porcelain` empty). The worker runs
   from the checkout and the maintained CLIs are built from the committed
   HEAD, so a dirty tree would make the worker execute working-tree code while
   the CLIs come from HEAD; deployment is refused until the tree is clean;
3. validates the checkout by running `uv sync` followed by
   `ruff format --check`, `ruff check`, `mypy`, and `pytest`. If any command
   fails, deployment is refused and the current worker is left untouched;
4. reads the git commit of the checkout — it never pulls, resets, stashes, or
   otherwise mutates git state;
5. prepares the maintained CLI environment for that exact commit, refusing the
   deployment if it cannot be built so the global CLIs never go stale;
6. starts the replacement worker detached from the invoking shell as its own
   session and process-group leader, appending its output to `worker.log`;
7. verifies the replacement is alive with an exact identity match and can reach
   PostgreSQL with a bounded timeout, rather than merely spawning it. On
   verification failure the replacement is stopped and the previous worker is
   left untouched;
8. stops the previous maintained worker using its recorded PID/process-group/
   session identity: `SIGTERM`, then a bounded `SIGKILL` while members remain;
9. atomically records the new worker's identity and the deployed commit, then
   activates the maintained CLI environment for that commit, and reports the
   deployed git commit.

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
the Lubko container. It provides stable, caller-chosen Lubko agent IDs, explicit
working directories, durable logs, continuation, waiting, stopping, killing,
deletion, and cleanup:

```text
lubko-agent new --id <ID> [--cwd DIR] [--title TEXT] [--json]
lubko-agent list [--running|--finished|--succeeded|--failed|--stopped|--killed] [--limit N] [--json]
lubko-agent status <id> / status --id <ID> [--json]
lubko-agent prompt --id <ID> [--steer] [--detach] PROMPT
lubko-agent log <id> [--lines N] [--follow]
lubko-agent wait <id> --timeout SEC
lubko-agent stop <id>
lubko-agent kill <id>
lubko-agent delete <id> [--force]
lubko-agent clean [--days N] [--dry-run]
```

The orchestrator generates each agent ID (a fresh base-16 string) before
submitting any transport job, so the ID is known up front and never has to be
scraped from output. `new --id <ID>` only creates an idle managed session
record — it launches no AI work and accepts no prompt. The first
`prompt --id <ID> PROMPT` creates and starts the underlying native agent
session and follows/streams it by default, returning only when that invocation
finishes; `--detach` is the explicit fire-and-forget mode. `--steer` only
changes behavior while the agent is currently running; on an idle, finished,
or never-started agent it is exactly equivalent to an ordinary prompt.

The orchestrator deals only with Lubko agent IDs; the underlying agent
implementation, its session IDs, its process tree, and its storage are
implementation details. `my-lubko-agent` is kept as a transition alias for the
same interface.

`log` normalizes what it presents: ANSI CSI/SGR color and control escape
sequences (including OSC sequences such as hyperlinks) are stripped before
display, in both the non-follow tail and the `--follow` stream, so output
consumed non-interactively through Lubko/Supabase is clean and parseable. The
durable `output.log` is never rewritten and always retains the raw underlying
output for debugging; the normalized view and the raw log are therefore
clearly separated.

Per-agent state lives under `$XDG_STATE_HOME/lubko/agents/<id>/` (default
`~/.local/state/lubko/agents/<id>/`) with `meta.json`, `output.log`, and a
`.lock` file serializing metadata updates. Agent IDs are stable and never
reused.

## Installing the maintained commands

The maintained entry points (`lubko-agent`, `lubko-worker`, `lubko-deploy`,
`lubko-deploy-ctl`, `lubko-install`, `my-lubko-agent`) are versioned in
`pyproject.toml`. In a checkout they are available through the project
virtualenv (`uv sync`); to make them available on PATH in every login and
interactive shell without a hand-maintained copy, install them into the user
bin directory with the maintained installer:

```sh
lubko-install --repo /path/to/lubko
lubko-install --repo /path/to/lubko --dry-run
```

`lubko-install` targets `$XDG_BIN_HOME` or `~/.local/bin`, which is already
prepended to PATH for login and interactive shells. Every maintained entry
point becomes a small **stable launcher** script that resolves one `current`
symlink under `$XDG_STATE_HOME/lubko/cli/` and executes the matching entry
point from the immutable per-commit CLI environment (a `git archive` extraction
of one exact commit plus its `uv sync` virtualenv). Deployments never rewrite
the launchers; they only switch the `current` symlink, so the global CLIs stay
coherent with the confirmed maintained worker commit.

`lubko-deploy deploy` (including `--bootstrap`) builds the CLI environment and
activates it as part of the deployment, and the supervised
`lubko-deploy-ctl` protocol does the same for the confirmed candidate: a
provisional candidate never moves the CLIs, and a rollback restores the prior
confirmed CLI version by construction. See
`docs/issue21-deploy-protocol.md`.

The exact `uv` executable a successful install used is recorded, with a schema
version, in `$XDG_STATE_HOME/lubko/toolchain.json` (default
`~/.local/state/lubko/toolchain.json`). `lubko-deploy deploy` then falls back
to that recorded executable when `uv` is not on PATH, so deployments keep
working even after `uv` itself is removed from PATH. Reinstall with an explicit
path when `uv` is unavailable on PATH:

```sh
lubko-install --repo /path/to/lubko --uv /absolute/path/to/uv
```

Until a maintained CLI environment exists (a fresh system before the first
`lubko-install` or deployment), invoke the commands through a checkout's own
virtualenv, for example `uv run --project /path/to/lubko lubko-deploy
deploy --bootstrap`.
