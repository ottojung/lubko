# Lubko

Lubko is the connector and execution transport for remote development commands. ChatGPT submits commands through the `lubko.jobs` queue, a Lubko worker executes them in the requested working directory, and the worker publishes bounded output and a terminal result to the same job row.

## Getting started

Install the maintained transport commands with `lubko-install`, start Lubko, and submit commands as described in [`docs/SKILL.md`](docs/SKILL.md). The transport preserves pending, running, succeeded, failed, and cancelled job states, bounded output, cancellation, worker recovery, and deployment behavior.

`lubko-board` is the thin client for the shared Borys issue board. Reads are public; mutating commands use the capability from `LUBKO_BOARD_CAPABILITY`. The board uses schema version 2 and remains stored at `borys/board-v1`; deployed schema version 1 boards are normalized to v2 in memory before the next write.

```sh
lubko-board create "Title" --body "Issue body"
lubko-board edit 12 --body "Replacement body"
lubko-board resource add 12 lubko://server /workspace/project
lubko-board resource list --host lubko://server --issue 12
lubko-board resource remove 12 lubko://server /workspace/project
```

Hosts use `lubko://<server>` and paths use canonical absolute POSIX syntax. A resource is protected while any dependent issue is open and collectible once all dependents are closed. `--json` is accepted globally and on subcommands for deterministic machine-readable output.

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
