"""In-process pytest execution budget enforcement.

AGENTS.md budgets the *execution* phase of a ``pytest`` run, not collection and
not process startup: "The ten-second budget measures pytest execution only.
Installation, environment provisioning, dependency installation, image
construction, and other acceptance checks are outside this budget and may take
longer."  This gate therefore starts its clock at the *end of collection*, not at
session start, so that a slow host that spends seconds importing or collecting
is not charged for time the documentation excludes.

The start point is chosen from public, documented pytest hooks rather than from
positions in pytest's own source, so it does not move between pytest versions:

* ``pytest_collection_finish`` is the documented "collection is finished" hook.
  pytest's own ``pytest_collection`` implementation ends in
  ``session.perform_collect()``, whose last act is to call
  ``pytest_collection_finish``; the very next statement in pytest's session
  driver is ``pytest_runtestloop``.  So the clock starts immediately before the
  execution phase.
* ``pytest_runtestloop`` is the documented start of the execution phase, and is
  registered as a fallback.  ``pytest_collection`` is a ``firstresult`` hook, so
  a third-party plugin that returns a truthy value short-circuits
  ``perform_collect`` and ``pytest_collection_finish`` never fires.  The
  runtestloop fallback still starts the clock in that case.  Both hooks are
  idempotent and the first one to fire wins, and ``pytest_collection_finish``
  always precedes ``pytest_runtestloop``, so the pair is correct on every
  ordering pytest can produce.

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
"""Hard wall-clock ceiling for the pytest *execution* phase (seconds)."""


class _BudgetGate:
    """Tracks execution-phase wall-clock time and enforces the budget at finish."""

    def __init__(
        self,
        clock: Callable[[], float] | None = None,
        budget: float = BUDGET_SECONDS,
    ) -> None:
        self._clock = clock or time.perf_counter
        self._budget = budget
        self._start: float | None = None
        self.budget_exceeded: bool = False

    def execution_start(self) -> None:
        """Record the start of the execution phase.

        Idempotent: the first caller wins, so registering both the
        ``pytest_collection_finish`` and ``pytest_runtestloop`` boundaries is
        safe and the earlier of the two is the one that counts.
        """
        if self._start is None:
            self._start = self._clock()

    def elapsed(self) -> float | None:
        """Return execution-phase seconds, or ``None`` if execution never began."""
        if self._start is None:
            return None
        return self._clock() - self._start

    def sessionfinish(
        self,
        session: pytest.Session,
        exitstatus: pytest.ExitCode,
    ) -> None:
        """Fail the session when the execution phase exceeded *and* no tests failed."""
        elapsed = self.elapsed()
        if elapsed is None or exitstatus != pytest.ExitCode.OK:
            return

        if elapsed >= self._budget:
            self.budget_exceeded = True
            session.exitstatus = pytest.ExitCode.TESTS_FAILED
            reporter = session.config.pluginmanager.get_plugin("terminalreporter")
            if reporter is not None:
                reporter.write_line(
                    f"FAIL: test-budget: {elapsed:.2f}s execution (limit"
                    f" {self._budget:.1f}s) — session exceeded the"
                    f" {self._budget:.1f}s budget",
                )


_gate = _BudgetGate()


def pytest_collection_finish(session: pytest.Session) -> None:
    """Start the execution clock: collection is done, tests are about to run."""
    del session
    _gate.execution_start()


def pytest_runtestloop(session: pytest.Session) -> None:
    """Start the execution clock, in case collection was short-circuited."""
    del session
    _gate.execution_start()


def pytest_sessionfinish(
    session: pytest.Session,
    exitstatus: pytest.ExitCode,
) -> None:
    """Delegate to the budget gate."""
    _gate.sessionfinish(session, exitstatus)
