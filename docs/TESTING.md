# Testing Safety

The full validation suite (`uv run pytest`) is safe to run beside a live
maintained worker. Three independent layers guarantee isolation:

1. **XDG root isolation** — every test runs with all XDG-backed Lubko state
   roots redirected to a pytest-owned temp directory before any lifecycle path
   can resolve it (conftest `_isolated_lubko_state`).

2. **Fail-closed ownership guard** — destructive lifecycle helpers call
   `assert_test_owned_state_root()` before reading metadata or signalling a
   recorded identity; the guard refuses unless the resolved root is under the
   current test's tmp dir (`tests/_isolation.py`).

3. **Process guard teardown** — every spawned process is tracked by exact
   identity; after each test the guard stops leaked processes by SIGTERM/SIGKILL
   on the exact group/PID and fails the test on leak (`tests/_process_guard.py`).

An ambient production-like sentinel (state tree + live worker process) is
created once per session and asserted unchanged at session end, proving the
suite never mutates or signals ambient state.

## Running

```bash
uv run pytest                    # full suite
uv run pytest tests/test_lifecycle.py  # lifecycle-only
```
