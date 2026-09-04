# Toolchain support policy

## Reproducible validation

CI uses one reviewed CPython 3.12 series and one explicit `uv` version. `uv.lock` is committed, and validation/install synchronization uses `uv sync --frozen` so dependency resolution never changes silently.

The exact CI `uv` pin is a build-validation choice, not a production runtime protocol.

## Runtime `uv`

Lubko runtime/deployment resolves `uv` in this order:

1. explicit `--uv`;
2. `uv` on `PATH`;
3. the last executable path recorded in Lubko state.

A candidate must exist and be executable. Lubko does **not** reject it merely because `uv --version` differs from CI's exact patch version, and it does not persist an expected patch version in runtime authority state.

Actual commands still consume the committed lockfile frozen. Incompatible future `uv` behavior should be handled when encountered by normal command failure and by updating the documented/CI toolchain deliberately, rather than by treating every patch release as part of Lubko's runtime protocol.
