# Lubko

Lubko lets you run commands and manage AI agent sessions inside a dedicated
container, from outside it. You describe what should run; Lubko executes it in
the working directory you choose, shows its output while it runs, records the
result, and lets you cancel or clean up at any time.

## Why Lubko

- **Delegate work safely.** Run builds, scripts, and other tasks inside a
  container instead of on your workstation or orchestration host.
- **See progress live.** Watch a task's output as it happens, not only after
  it finishes.
- **Stay in control.** Cancel running work cleanly; every task ends with a
  clear status (`succeeded`, `failed`, `cancelled`).
- **Nothing gets lost.** Tasks that were interrupted are detected and marked,
  never silently lost or run twice.

## When would you use it

Use Lubko whenever something outside a container needs to execute work *in*
that container: remote task execution, background automation, or managing
long-lived AI coding-agent sessions from an orchestrator.

## Getting started

Install the maintained commands with `lubko-install`, start Lubko in your
container, and submit tasks as described in `docs/`. The `lubko-agent`
command manages AI agent sessions end to end: start them, prompt them, read
their logs, wait for completion, stop or clean them up.

## Development

```sh
uv sync
uv run ruff format --check .
uv run ruff check .
uv run mypy .
uv run pytest
```
