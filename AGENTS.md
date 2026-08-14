# Development rules

- Manage Python and dependencies with `uv`.
- Python 3.12+ only.
- Keep Ruff configured with `select = ["ALL"]` and preview lint rules enabled.
- Keep mypy in strict mode.
- Do not add lint or type-check ignores unless a concrete library/interface limitation requires one.
- Before committing, run:
  - `uv run ruff format --check .`
  - `uv run ruff check .`
  - `uv run mypy .`
  - `uv run pytest`
