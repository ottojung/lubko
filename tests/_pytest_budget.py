"""In-process pytest session budget enforcement.

Records ``time.perf_counter`` at ``pytest_sessionstart`` and, when the session
finishes without prior test failures, checks elapsed time against
``BUDGET_SECONDS``.  When the budget is exceeded the session exit status is
set to ``TESTS_FAILED`` so that CI catches the regression immediately.

The module is importable and testable in isolation: callers can supply a
custom clock for deterministic unit tests.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import Callable

BUDGET_SECONDS: float = 10.0
"""Hard wall-clock ceiling for the full ``pytest`` session (seconds)."""


class _BudgetGate:
    """Tracks session wall-clock time and enforces the budget at finish."""

    def __init__(
        self,
        clock: Callable[[], float] | None = None,
        budget: float = BUDGET_SECONDS,
    ) -> None:
        self._clock = clock or time.perf_counter
        self._budget = budget
        self._start: float = 0.0
        self.budget_exceeded: bool = False

    def sessionstart(self, session: pytest.Session) -> None:  # ruff: ignore[unused-method-argument]
        """Record the session start timestamp."""
        self._start = self._clock()

    def sessionfinish(
        self,
        session: pytest.Session,
        exitstatus: pytest.ExitCode,
    ) -> None:
        """Fail the session when the budget is exceeded *and* no tests failed."""
        elapsed = self._clock() - self._start
        line = f"test-budget: {elapsed:.2f}s elapsed (limit {self._budget:.1f}s)"

        if exitstatus != pytest.ExitCode.OK:
            return

        if elapsed >= self._budget:
            self.budget_exceeded = True
            session.exitstatus = pytest.ExitCode.TESTS_FAILED
            reporter = session.config.pluginmanager.get_plugin("terminalreporter")
            if reporter is not None:
                reporter.write_line(
                    f"FAIL: {line} — session exceeded the {self._budget:.1f}s budget",
                )


_gate = _BudgetGate()


def pytest_sessionstart(session: pytest.Session) -> None:
    """Delegate to the budget gate."""
    _gate.sessionstart(session)


def pytest_sessionfinish(
    session: pytest.Session,
    exitstatus: pytest.ExitCode,
) -> None:
    """Delegate to the budget gate."""
    _gate.sessionfinish(session, exitstatus)
