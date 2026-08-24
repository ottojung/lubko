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

## Git is good

If you have access to `git`, then:
- commit frequently,
- commit small, conceptual changes,
- and write helpful multiline commit messages.

It is always safe to commit, do it even if you weren't explicitly told to.
Never squash conceptually unrelated changes, even if the result is still small.
