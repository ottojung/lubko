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

## Testing requirements

These are hard requirements, not goals or preferences.

- **The complete test suite must finish in under ten seconds.** `uv run pytest` must run the entire repository test suite and complete in less than 10.0 seconds of wall-clock time once the development environment is installed. There is no slow-test allowance. Delete or rewrite tests that require real sleeps, long polling, repeated nested test-suite runs, heavyweight service/process startup, stress loops, or other intrinsically slow behavior. Prefer deterministic fakes, injected clocks/state, and direct invariant checks.
- **There are no optional tests.** Every test that exists must run on every normal CI run, and the canonical CI test command must be the same complete `uv run pytest` command developers run locally. Do not create slow/extended/integration/manual test tiers, opt-in markers, environment-gated tests, CI-only/local-only tests, or normal-environment `skip`/`skipif` exclusions. If a test cannot always run, it must be rewritten or deleted.
- **There are no situational tests.** Tests must assert stable, general product invariants rather than memorialize one particular issue, PR, production incident, timing accident, process layout, migration, or one-time fix. Do not organize tests around issue/PR numbers. When a regression reveals a real invariant, keep the invariant and rewrite the test to express it simply and generally; delete the historical scaffolding and redundant incident-specific regressions.
- Test count and preservation of existing test structure are not goals. Aggressively delete obsolete, redundant, overly specific, or disproportionately expensive tests. A smaller deterministic suite that directly covers stable invariants is preferred over accumulated regression history.
- Never weaken these requirements by moving tests out of the canonical suite. If a test is valuable, it must be fast, unconditional, and general enough to run every time.

## Git is good

If you have access to `git`, then:
- commit frequently,
- commit small, conceptual changes,
- and write helpful multiline commit messages.

It is always safe to commit, do it even if you weren't explicitly told to.
Never squash conceptually unrelated changes, even if the result is still small.
