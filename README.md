# Lubko

Lubko lets you delegate commands and AI agent sessions and manage them
end to end. You describe what should run; Lubko runs it in the working
directory you choose, shows its output while it runs, records the result,
and lets you cancel or clean up at any time.

## Why Lubko

- **Delegate work safely.** Run builds, scripts, and other tasks without
  babysitting them on your own machine.
- **See progress live.** Watch a task's output as it happens, not only after
  it finishes.
- **Stay in control.** Cancel running work cleanly; every task ends with a
  clear status (`succeeded`, `failed`, `cancelled`).
- **Nothing gets lost.** Tasks that were interrupted are detected and marked,
  never silently lost or run twice.

## When would you use it

Use Lubko whenever you need to hand off work and stay informed: remote task
execution, background automation, or managing long-lived AI coding-agent
sessions from an orchestrator or your own tooling.

## Getting started

Install the maintained commands with `lubko-install`, start Lubko, and submit
tasks as described in `docs/`. The `lubko-agent` command manages AI agent
sessions end to end: start them, prompt them, read their logs, wait for
completion, stop or clean them up.

## Development

Lubko targets exactly CPython 3.12 and pins a single `uv` version; see
[`docs/TOOLCHAIN.md`](docs/TOOLCHAIN.md) for the supported toolchain and the
explicit upgrade procedure.

```sh
uv sync
uv run ruff format --check .
uv run ruff check .
uv run mypy .
uv run pytest
```

## License

Lubko is licensed under the GNU Affero General Public License version 3 only
(`AGPL-3.0-only`). See [LICENSE](LICENSE) for the full, canonical license text.
