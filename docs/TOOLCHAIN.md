# Toolchain support policy

Lubko validates exactly one toolchain so identical commits are checked with a
predictable Python and `uv` environment. This document is the contract; every
other place that names a Python or `uv` version must stay consistent with it.

## Supported Python

- Exactly the CPython 3.12 series, declared as `requires-python = "==3.12.*"`
  in `pyproject.toml`.
- CI exercises only CPython 3.12 (`.github/workflows/ci.yml`,
  `python-version: "3.12"`).
- `.python-version` pins `3.12`.
- No other Python version is supported or continuously tested, so no other
  version is claimed.

## Supported `uv`

`uv` is pinned to one explicit version everywhere it is installed. The current
pinned version is `0.10.12`.

- CI installs it explicitly:
  `python -m pip install "uv==0.10.12"`.
- Maintained runtimes must install the same pinned version before the first
  managed deployment, for example:
  `python -m pip install "uv==0.10.12"`.
- `lubko-install` records the exact `uv` executable it used in
  `~/.local/state/lubko/toolchain.json`; `lubko-deploy` falls back to that
  recorded executable when `uv` is not on `PATH` (see `src/lubko/toolchain.py`).
  The pin above is the version that must be recorded.

Every resolved `uv` candidate — explicit `--uv`, `uv` on `PATH`, and the
recorded fallback — is verified at resolution time by running `uv --version`
and comparing the reported version against the runtime pin
(`SUPPORTED_UV_VERSION` in `src/lubko/toolchain.py`). Resolution fails closed
on a missing/unreadable executable, a non-zero `uv --version`, malformed
output, a version mismatch, or a timeout. The recorded candidate is
re-validated at use time, so a binary swapped in place at the recorded path
cannot bypass the pin.

Because `uv` is installed from an explicit `==` pin and every candidate is
re-checked against that pin, an upstream `uv` release cannot change validation
behavior for an unchanged Lubko commit.

## Dependency lock

`uv.lock` is committed and consumed frozen. CI and installs run
`uv sync --frozen`, which refuses to re-resolve or modify dependencies. Changes
to dependencies or the Python/`uv` contract are rolled forward deliberately as
their own reviewed change, never silently.

## Upgrading Python or `uv`

Changing the supported Python series or the `uv` pin is a toolchain change and
must be a single, reviewed commit (do not fold it into unrelated work):

1. Update `requires-python` in `pyproject.toml` (and `.python-version` when the
   interpreter series changes).
2. Regenerate the lock so it stays consistent with the new contract:
   `uv lock`. Commit the regenerated `uv.lock`.
3. Update the `uv` pin in `.github/workflows/ci.yml` to the new single version,
   and update the version stated in this document to the same value.
4. Keep `uv sync --frozen` validation green and run the full suite:
   `uv run ruff format --check . && uv run ruff check . && uv run mypy . &&
   uv run pytest`.
5. Open the change as its own PR for review before merging.
