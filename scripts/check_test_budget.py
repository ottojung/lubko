"""Enforce the repository 10-second wall-clock pytest budget.

Runs ``uv run pytest`` as a subprocess, measures wall-clock duration with
``time.monotonic``, and exits non-zero when tests fail or the budget is exceeded.
"""

from __future__ import annotations

import subprocess
import sys
import time

BUDGET_SECONDS: float = 10.0
PYTEST_CMD: list[str] = ["uv", "run", "pytest"]


def _write_stderr(message: str) -> None:
    """Write *message* to stderr with a trailing newline."""
    sys.stderr.write(message + "\n")


def _write_stdout(message: str) -> None:
    """Write *message* to stdout with a trailing newline."""
    sys.stdout.write(message + "\n")


def main() -> int:
    """Execute pytest and verify wall-clock budget.

    Returns:
        Exit code: 0 for success, 1 for test failure or budget exceeded.
    """
    wall_start = time.monotonic()
    result = subprocess.run(PYTEST_CMD, check=False)
    wall_elapsed = time.monotonic() - wall_start

    summary = f"test-budget: {wall_elapsed:.2f}s elapsed (limit {BUDGET_SECONDS:.1f}s)"
    _write_stdout(f"\n--- {summary} ---")

    if result.returncode != 0:
        _write_stderr(f"FAIL: pytest exited with code {result.returncode}")
        return 1

    if wall_elapsed >= BUDGET_SECONDS:
        _write_stderr(
            f"FAIL: test suite took {wall_elapsed:.2f}s which exceeds"
            f" the {BUDGET_SECONDS:.1f}s budget."
        )
        return 1

    _write_stdout("OK: within budget")
    return 0


if __name__ == "__main__":
    sys.exit(main())
