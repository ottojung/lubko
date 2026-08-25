# Lubko

Lubko is a small job runner for running shell-style tasks inside a dedicated
container. You describe a command and where it should run; Lubko picks it up,
executes it in the requested working directory, captures its output and exit
status, and makes the results available while the job runs and after it
finishes.

## Why Lubko

- **Delegate work safely.** Run arbitrary commands — builds, scripts, agent
  sessions — inside a container instead of on your workstation or orchestration
  host.
- **See progress live.** Watch a job's output as it runs, not only after it
  finishes.
- **Stay in control.** Cancel running work cleanly and get a clear final status
  (`succeeded`, `failed`, `cancelled`) either way.
- **Survive crashes.** If a worker dies mid-job, the stuck job is detected and
  marked failed automatically — it is never silently lost or run twice.

## When would you use it

Use Lubko whenever something outside the container needs to execute commands
*in* the container: remote task execution, background automation, or managing
long-lived AI coding-agent sessions from an orchestrator that can only reach
the container's queue.

## What you get

- **`lubko-worker`** — the always-on service that claims and runs jobs.
- **`lubko-supervisor`** — keeps exactly one worker running across crashes and
  environment restarts.
- **`lubko-deploy`** — installs, upgrades, restarts, and repairs the worker
  with built-in verification.
- **`lubko-install`** — puts the maintained commands on your PATH.
- **`lubko-agent`** — manage durable AI agent sessions: start them, prompt
  them, steer or detach, read their logs, wait for completion, stop or clean
  them up.

Jobs are submitted through PostgreSQL (see `docs/protocol.md`), and each job's
output is retained as a durable log.

## Development

```sh
uv sync
uv run ruff format --check .
uv run ruff check .
uv run mypy .
uv run pytest
```
