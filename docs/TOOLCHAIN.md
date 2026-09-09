# Toolchain support policy

## Reproducible validation

Canonical CI validates CPython 3.12 and one explicit `uv` version. `uv.lock` is committed, and validation/install synchronization uses `uv sync --frozen --extra dev` so dependency resolution never changes silently. Plain `uv sync --frozen` installs only runtime dependencies and is used by the installed CLI environment.

The supported runtime and package policy is CPython >=3.12. Termux acceptance validates the current Termux Python (currently 3.14).

### psycopg platform split

On non-Android platforms, Lubko depends on `psycopg[binary]` which ships its own bundled libpq. On Android/Termux, `psycopg[binary]` wheels are unavailable, so the dependency resolves to plain `psycopg` and native `libpq` is an explicit Termux package prerequisite.

The exact CI `uv` pin is a build-validation choice, not a production runtime protocol.

## Runtime `uv`

Lubko runtime/deployment resolves `uv` in this order:

1. explicit `--uv`;
2. `uv` on `PATH`;
3. the last executable path recorded in Lubko state.

A candidate must exist and be executable. Lubko does **not** reject it merely because `uv --version` differs from CI's exact patch version, and it does not persist an expected patch version in runtime authority state.

Actual commands still consume the committed lockfile frozen. Incompatible future `uv` behavior should be handled when encountered by normal command failure and by updating the documented/CI toolchain deliberately, rather than by treating every patch release as part of Lubko's runtime protocol.
