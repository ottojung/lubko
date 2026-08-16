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

### Testing safety invariant

The full validation suite must be safe to run from the same Unix user and
container as the live Lubko worker, without changing any live Lubko
lifecycle/CLI/deploy state and without signalling any live worker process.

This is enforced by default, not by opt-in:

- every test runs with all XDG-backed Lubko state roots (`XDG_STATE_HOME`,
  `XDG_DATA_HOME`, `XDG_CONFIG_HOME`, `XDG_CACHE_HOME`, `XDG_BIN_HOME`)
  redirected to a pytest-owned temporary directory before any lifecycle path
  can resolve it, and the redirect is inherited by every subprocess the tests
  spawn;
- destructive lifecycle helpers (for example the deployment E2E cleanup that
  stops recorded worker identities) fail closed unless the resolved state root
  is under the current test's pytest-owned temporary directory, so ambient
  per-user metadata can never be read or signalled by a test;
- teardown only signals processes owned by the current test execution through
  the explicit process-guard registry; lifecycle-state cleanup supplements it
  only inside the verified test root;
- an ambient "production-like" sentinel state tree and live worker process are
  created for the session, and the suite fails if the tree is ever mutated or
  the sentinel is ever signalled.

Do not relax the isolation in a test; never point `XDG_STATE_HOME` (or another
XDG home variable) at a real user path from a test.

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
lubko-deploy restart
lubko-deploy migrate --commit <sha> [--repo DIR] [--uv PATH]
lubko-deploy recover [--repo DIR] [--uv PATH] [--probe-timeout N]
lubko-deploy repair --repo DIR --recovery-worker-pid PID [--uv PATH] [--probe-timeout N]
lubko-deploy log [--lines N]
```

`lubko-deploy status` reports the current worker state, its PID/process-group/
session identity, the deployed git commit, and the log path.

Lubko is an **always-on supervised service**: while its virtual/container
environment is running, Lubko is always intended to be running. There is no
supported in-environment stopped state and no `lubko-deploy stop`. The only
supported way to fully stop Lubko is to stop the virtual/container environment
that runs it. The supervisor owns the maintained worker and restores it after
any unexpected exit, planned restart, or container restart.

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

`lubko-deploy restart` replaces the current worker process with a fresh process
of the **same exact confirmed deployed commit**, through the external
supervisor, and waits until the replacement is proven queue-ready. A restart
never uses Git, the network, or the mutable source checkout: the exact commit
is read from the supervisor's durable state and the fresh worker runs from that
commit's already-sealed runtime. A restart is a new desired intent at a strictly
newer generation, so any older deployment/mission record automatically becomes
stale history that can never override it.

`lubko-deploy migrate` is the explicit, supported bootstrap/repair path for
production state that predates the external supervisor (stale or legacy
`supervisor/desired.json`, `supervisor/state.json`, or `worker/rollback.json`).
It runs only when no supervisor is live: it verifies an exact commit's sealed
runtime and writes a strictly newer desired intent for it, replacing corrupt or
legacy mission state, so the next supervisor start reconstructs that verified
commit deterministically instead of failing closed. Normal startup remains
fail-closed; never hand-edit these JSON files.

### Repairing corrupted lifecycle state

If lifecycle state was ever corrupted (for example by a pre-isolation test run
that wrote `test-worker` metadata and a synthetic commit into the live state
tree), do not repair it by editing `meta.json` and do not run
`lubko-deploy deploy` as the first action, because its validation executes the
full test suite.

`lubko-deploy repair` is the supported recovery path. It never trusts the
stale metadata. Start a recovery worker with the supported helper first, which
deliberately starts a **detached** worker as its own session and
process-group leader so its exact PID is a stable dedicated identity that
`repair` can safely adopt later:

```sh
# Start a detached recovery worker from the intended maintained checkout:
lubko-deploy recover --repo /workspace/.lubko-deployment
# ... then adopt its exact, independently reported PID:
lubko-deploy repair --repo /workspace/.lubko-deployment --recovery-worker-pid <PID>
```

Do **not** start the recovery worker in the foreground (for example
`cd /workspace/.lubko-deployment && uv run lubko-worker` in a terminal): a
foreground worker inherits the terminal's session and foreground process group,
so it is not a session/group leader and `repair` correctly refuses to adopt it,
because lifecycle stop/replace must never signal an ambient shell group.

`lubko-deploy recover`:

1. requires that no maintained worker is already live, that the checkout is
   clean, and that PostgreSQL is reachable;
2. refuses to start a second consumer when another worker is already consuming
   the queue (adopt that existing worker instead);
3. starts the recovery worker detached as its own session/process-group leader
   (the same mechanism a deployment replacement uses) and reports its exact
   PID, worker id, and commit, without writing any lifecycle metadata.

`lubko-deploy repair`:

1. requires a clean checkout and reads its exact git commit;
2. verifies the supplied PID is a live, session/process-group-leading process
   whose command line is genuinely a Lubko worker;
3. verifies PostgreSQL is reachable and that no other live maintained worker
   or live supervised candidate/previous identity is recorded (a live pending
   supervised mission blocks the repair);
4. proves one-consumer queue semantics with a real roundtrip bound to the
   exact supplied PID: a probe job must be claimed by the supplied worker (the
   persisted `process_pid` of the probe command must be a descendant of the
   supplied PID in `/proc`, with `worker_id` as an additional check), after
   which the probe is cancelled, awaited terminal, and removed;
5. only then rewrites the maintained metadata with the adopted exact identity
   (recording the worker's real lifecycle token when present), reconciles the
   maintained CLI `current` pointer to the checkout commit, removes stale
   readiness markers and CLI roots whose ownership is proven stale, and
   rewrites an unusable `toolchain.json`.

After a successful repair, verify with `lubko-deploy status` that the reported
worker identity, worker id, git commit, and maintained CLI pointer are coherent
again, confirm a fresh queue roundtrip succeeds, and stop any leftover
recovery/foreground/bridge worker whose identity was not adopted.

### Identity and PID reuse

A worker's metadata records its PID, process-group ID, session ID, start time
in clock ticks, and a per-deployment lifecycle token placed in the process
environment. Identity checks require every recorded field to match, so a
recycled PID can never be mistaken for the maintained worker, and stopping
only ever signals the exact recorded process group.

### Bootstrap from an unmanaged legacy worker

Before the first managed deployment, the worker is an unmanaged legacy daemon
with no lifecycle metadata. `lubko-deploy status` reports this, and `deploy`
refuses to claim it can stop it by identity.

The one-time migration is a single manual stop of the legacy worker, after
which the first managed deployment is started with:

```sh
lubko-deploy deploy --bootstrap
```

Subsequent upgrades replace maintained workers with no manual PID discovery.
Pre-supervisor production state that is stale or corrupt (including legacy
`supervisor/desired.json`, `supervisor/state.json`, or `worker/rollback.json`)
is migrated explicitly with `lubko-deploy migrate --commit <sha>` as described
above; normal startup stays fail-closed rather than trusting hand-edited JSON.

## External worker supervision

`lubko-supervisor` is the small, stable control component outside the worker
process whose job is to ensure that exactly one intended maintained Lubko
worker is running and to restore a verified last-known-good worker after an
unexpected worker exit. It is deliberately independent of the worker and of
the Lubko job queue: it never needs a queue roundtrip to notice or repair
worker death.

### Container startup contract

The supervisor is designed to be the container's **main long-lived process**.
The production container runs `tini-static -- sleep infinity` as PID 1 today;
the supported startup contract replaces the `sleep infinity` child of Tini with
the supervisor:

```sh
tini-static -- lubko-supervisor
```

This is the **external startup boundary**: there is no Docker/containerd/Podman
socket or CLI, no systemd, no cron, and no server-management integration
available inside the container, so a profile/autostart hack cannot be part of
the supported recovery path. The repository-side pieces required by the
contract are fully implemented here (`lubko-supervisor`, its durable protocol,
sealed per-commit runtimes, and deterministic startup reconstruction), and the
repository-owned `lubko-install` machinery installs the stable launcher for
`lubko-supervisor` into `$XDG_BIN_HOME`/`~/.local/bin`. The image-level CMD
change is the single external step needed for full environment-restart
recovery: on every container start Tini launches the supervisor, which
reconstructs the intended maintained worker deterministically from durable
state and restores service without any human terminal. Until that one image
command is switched, the supervisor must be started by other means and
container-restart recovery is not yet automatic; that is a genuine external
boundary, not a repository gap.

For the launcher to be found at container start, use a **stable absolute
maintained launcher path** (for example
`$XDG_BIN_HOME/lubko-supervisor`, resolved to an absolute path at image build
time) rather than relying on a login-shell PATH that may not be set for PID 1.

A fresh system bootstraps the maintained worker once with
`lubko-deploy deploy --bootstrap` (which builds the sealed per-commit runtime
and activates the maintained commands), then the container command above is
installed. From then on the supervisor owns and restarts the worker across
every crash and every container restart.

### Ownership model

The supervisor is the stable authority that actually starts, stops, and
restarts worker processes:

- it spawns the worker as its **direct child** from the **sealed** per-commit
  runtime (`$XDG_STATE_HOME/lubko/cli/<full-commit-sha>/`), never from a
  mutable working tree, so a crash never launches arbitrary checkout contents
  and ordinary restarts never run repository validation, use Git, or touch the
  source checkout at all;
- it never uses `pkill`/`killall`/argv matching/process-name discovery: every
  stop and liveness check uses the exact recorded process identity (PID,
  process group, session, start time, lifecycle token, and direct-parent
  check), so a recycled PID can never be signalled;
- `lubko-deploy deploy`, `lubko-deploy restart`, and the supervised
  `lubko-deploy-ctl` protocol communicate only through durable state: a
  monotonic generation space shared by the desired intent, the daemon's
  applied state, and the deployment mission. The supervisor is the **single
  process-lifecycle authority** for maintained workers, including deployment
  candidates: deployctl may own confirmation/rollback decisions and durable
  mission metadata, but it never directly spawns, stops, or replaces a worker
  while the supervisor is active. A normal maintained install **refuses** to
  fall back to direct spawning when the supervisor is absent; only the
  one-time `--bootstrap` path and the explicit emergency `recover`/`repair`
  commands start workers without it;
- a pending supervised deployment mission at a newer generation is itself an
  active intent: the supervisor retires the previous worker and starts the
  candidate from its sealed runtime as its own direct child, then proves queue
  readiness. Confirmation settles the desired intent onto the candidate
  commit, and rollback settles it onto the previous commit, each at a strictly
  newer generation, so terminal `confirmed`/`rolled_back` records are history
  that can never override a newer restart/deploy;
- there is no durable stopped mode and no `lubko-deploy stop`. A healthy
  supervisor with a confirmed deployment always wants its worker running; the
  only supported way to fully stop Lubko is to stop the environment that runs
  the supervisor.

### Sealed per-commit runtimes

Every deployed commit runs from a separate runtime materialized under
`$XDG_STATE_HOME/lubko/cli/<full-commit-sha>/` (a `git archive` extraction of
the exact full commit plus its `uv sync` virtualenv). After successful
preparation the runtime is **sealed read-only** and bound to the exact commit
by a small manifest; normal worker execution sets `PYTHONDONTWRITEBYTECODE=1`
and never writes into the runtime (logs and state stay under XDG worker
paths). Ordinary crash/restart/probe use only this sealed runtime and remain
functional even if the source checkout is modified, moved, or deleted.
Verification rejects an unsealed, incomplete, wrong-commit, or corrupt runtime
(fail closed) rather than falling back to the mutable checkout or Git.
Explicit unseal/removal exists only for GC/rebuild (`gc_cli_roots`,
`remove_cli_root`, and rebuild paths); never unseal during normal operation.

### Generation precedence

The desired intent, the supervisor applied state, and the deployment mission
share **one monotonic generation space**. Only the newest generation may
influence the desired worker:

- a mission older than the desired intent is stale history and is ignored
  whatever its status (`pending`, `confirmed`, or `rolled_back`);
- a newer pending mission is the active candidate intent and is run;
- a pending mission at the desired generation only runs when it selects the
  same commit, otherwise the contradiction holds;
- a terminal mission at the desired generation or newer is an unsettled,
  incomplete settlement and holds: terminal status alone never chooses a
  commit, which eliminates the PR #66 stale-terminal-override bug;
- corrupt/contradictory state fails closed into a hold with a diagnostic;
- a supervisor restart at any point reconstructs exactly one worker from
  desired/mission generations.

### Durable protocol state

Supervisor state lives under `$XDG_STATE_HOME/lubko/supervisor/`:

- `desired.json` — the explicit run intent written by `lubko-deploy deploy`
  and `lubko-deploy restart`; a monotonic `generation` makes concurrent writers
  last-writer-wins and lets the daemon recognise every new intent exactly once,
  and a newer same-commit intent is a process replacement, never a no-op;
- `state.json` — the daemon's durable record of the generation it applied, the
  worker child it owns, and its crash-loop backoff, so a supervisor restart
  reconstructs deterministically without duplicating the worker;
- `status.json` — a machine-readable observation surface (`lubko-deploy
  status` reports it; `lubko-supervisor --status` prints it) exposing the
  supervisor PID, applied generation, desired commit, live worker exact
  identity, queue readiness, last worker exit, restart count, next retry time,
  and the supervised-deployment mission state;
- `supervisor.pid` — the daemon's own exact identity, used only to detect that
  a daemon is running and to refuse a second daemon.

### Crash, backoff, and readiness

An unexpected worker exit is recorded (return code and time), restarted with
bounded exponential backoff, and never run from a mutable checkout. Readiness
is a real queue roundtrip bound to the exact supervisor child — PID liveness
and database connectivity are not enough — so a replacement is only reported
ready after it actually consumes `lubko.jobs`. A worker that stays alive during
a transient PostgreSQL/DNS outage is never duplicated, and a restart during an
outage backs off until readiness is possible again.

Corrupt or contradictory supervised-deployment metadata fails closed: the
supervisor holds without a worker rather than launching an arbitrary process
during an unknown handoff. Missing, corrupt, inconsistent, or unsealed exact
runtimes also fail closed with a diagnostic; the supervisor never falls back
to the mutable checkout or to Git during restart or crash recovery.

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

The maintained entry points (`lubko-agent`, `lubko-worker`, `lubko-supervisor`,
`lubko-deploy`, `lubko-deploy-ctl`, `lubko-install`, `my-lubko-agent`) are
versioned in `pyproject.toml`. In a checkout they are available through the
project virtualenv (`uv sync`); to make them available on PATH in every login
and interactive shell without a hand-maintained copy, install them into the
user bin directory with the maintained installer:

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
