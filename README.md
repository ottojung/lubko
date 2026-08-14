# Lubko

Lubko is a small worker that claims shell jobs from PostgreSQL and executes them
inside a Docker development container.

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

- `LUBKO_CONTAINER` — Docker container name, default `phoebe-dev`.
- `LUBKO_WORKER_ID` — worker identifier, default is the host name.
- `LUBKO_POLL_INTERVAL_SECONDS` — idle polling interval, default `1`.
- `LUBKO_MAX_OUTPUT_BYTES` — maximum bytes retained from each output stream,
  default `262144`.

Run with:

```sh
uv run lubko-worker
```
