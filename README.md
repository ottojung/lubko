# Lubko

Lubko is the agent-agnostic connector and execution transport for remote development commands. ChatGPT submits commands through the `lubko.jobs` queue, a Lubko worker executes them in the requested working directory, and the worker publishes bounded output and a terminal result to the same job row.

## Getting started

Install the maintained transport commands with `lubko-install`, start Lubko, and submit commands as described in [`docs/SKILL.md`](docs/SKILL.md). The transport preserves pending, running, succeeded, failed, and cancelled job states, bounded output, cancellation, worker recovery, and deployment behavior.

`lubko-board` is the thin client for the shared Borys issue/message board. Reads are public; mutating commands use the capability from `LUBKO_BOARD_CAPABILITY`.

## Development

Lubko requires CPython 3.12 or later. See [`docs/TOOLCHAIN.md`](docs/TOOLCHAIN.md) and [`docs/GOVERNANCE.md`](docs/GOVERNANCE.md).

```sh
uv sync --frozen --extra dev
uv run ruff format --check .
uv run ruff check .
uv run mypy .
uv run pytest
```

## License

Lubko is licensed under the GNU Affero General Public License version 3 only (`AGPL-3.0-only`). See [LICENSE](LICENSE).
