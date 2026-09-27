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
custom clock for deterministic unit tests. ``sessionfinish`` returns the line
it reported, so the wording can be asserted without capturing a terminal.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import Callable

BUDGET_SECONDS: float = 10.0
"""Hard wall-clock ceiling for the pytest *execution* phase (seconds)."""


def _report_line(session: pytest.Session, line: str) -> str:
    """Write *line* to the terminal reporter, and return it for testing.

    Returns:
        *line*, unchanged, so callers and tests see what would have been shown.
    """
    reporter = session.config.pluginmanager.get_plugin("terminalreporter")
    if reporter is not None:
        reporter.write_line(line)
    return line


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
    ) -> str:
        """Report the budget verdict for every run, and enforce it when it can.

        The verdict is always reported. A run that is already red for unrelated
        reasons must not hide the budget figure: silence there is the most
        expensive kind, because the least healthy run is then the one that says
        least about the budget.

        Enforcement stays narrow. The exit status is changed to ``TESTS_FAILED``
        only when the session would otherwise have been green, so a budget
        breach can never overwrite or reinterpret an existing failure, and an
        over-budget red run says so in terms that name it as informational
        rather than as the cause of the red.

        Returns:
            The verdict line that was reported.
        """
        elapsed = self.elapsed()
        if elapsed is None:
            return _report_line(
                session,
                "test-budget: NOT MEASURED — no execution phase ran (collection"
                " never finished), so no budget verdict is available and the"
                " budget was not enforced",
            )

        self.budget_exceeded = elapsed >= self._budget
        figure = f"{elapsed:.2f}s execution of the {self._budget:.1f}s budget"
        scope = "(collection excluded, per AGENTS.md)"

        if not self.budget_exceeded:
            return _report_line(
                session,
                f"test-budget: OK — {figure} {scope}",
            )

        if exitstatus == pytest.ExitCode.OK:
            session.exitstatus = pytest.ExitCode.TESTS_FAILED
            return _report_line(
                session,
                f"test-budget: FAIL — {figure} {scope}. The budget is the reason"
                " this session exits non-zero.",
            )

        return _report_line(
            session,
            f"test-budget: FAIL — {figure} {scope}. Reported for information only:"
            " this session was already non-zero before the budget was checked, so"
            " the budget was not enforced and is not the cause of that status.",
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
