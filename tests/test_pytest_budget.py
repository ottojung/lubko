"""Deterministic tests for the in-process pytest session budget gate."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

import tests._pytest_budget as budget_plugin
from tests._pytest_budget import BUDGET_SECONDS, _BudgetGate


def _fake_session(exitstatus: pytest.ExitCode = pytest.ExitCode.OK) -> MagicMock:
    """Return a minimal mock ``pytest.Session``."""
    session = MagicMock()
    session.exitstatus = exitstatus
    return session


def _gate_at(elapsed: float, budget: float = BUDGET_SECONDS) -> _BudgetGate:
    """Return a gate whose clock yields *start*=0 then *finish*=*elapsed*."""
    times = iter([0.0, elapsed])
    return _BudgetGate(clock=lambda: next(times), budget=budget)


def test_below_budget_passes() -> None:
    """Session completing under budget retains OK status."""
    gate = _gate_at(9.99)
    session = _fake_session()
    gate.execution_start()
    gate.sessionfinish(session, pytest.ExitCode.OK)
    assert session.exitstatus == pytest.ExitCode.OK
    assert not gate.budget_exceeded


def test_exactly_at_budget_fails() -> None:
    """Elapsed time equal to the budget triggers failure."""
    gate = _gate_at(10.0)
    session = _fake_session()
    gate.execution_start()
    gate.sessionfinish(session, pytest.ExitCode.OK)
    assert session.exitstatus == pytest.ExitCode.TESTS_FAILED
    assert gate.budget_exceeded


def test_over_budget_fails() -> None:
    """Elapsed time beyond the budget triggers failure."""
    gate = _gate_at(15.5)
    session = _fake_session()
    gate.execution_start()
    gate.sessionfinish(session, pytest.ExitCode.OK)
    assert session.exitstatus == pytest.ExitCode.TESTS_FAILED
    assert gate.budget_exceeded


def test_existing_failure_not_overridden() -> None:
    """When tests already failed the budget gate must not change exitstatus."""
    gate = _gate_at(999.0)
    session = _fake_session(exitstatus=pytest.ExitCode.TESTS_FAILED)
    gate.execution_start()
    gate.sessionfinish(session, pytest.ExitCode.TESTS_FAILED)
    assert session.exitstatus == pytest.ExitCode.TESTS_FAILED
    assert not gate.budget_exceeded


def test_existing_nonzero_exit_preserved() -> None:
    """Any nonzero exit status is preserved even if budget is exceeded."""
    gate = _gate_at(999.0)
    session = _fake_session(exitstatus=pytest.ExitCode.INTERRUPTED)
    gate.execution_start()
    gate.sessionfinish(session, pytest.ExitCode.INTERRUPTED)
    assert session.exitstatus == pytest.ExitCode.INTERRUPTED
    assert not gate.budget_exceeded


def test_budget_constant_is_ten_seconds() -> None:
    """The configured budget matches the AGENTS.md 10.0s requirement."""
    assert pytest.approx(10.0) == BUDGET_SECONDS


# --- Which interval the gate measures -------------------------------------
#
# AGENTS.md budgets "pytest execution only" and explicitly puts collection
# outside the budget.  These tests pin that the clock starts at the documented
# end-of-collection boundary and not at session start, using only public
# pytest hooks (no dependence on line numbers inside pytest's own source).


def test_execution_clock_starts_at_collection_finish() -> None:
    """The gate's documented execution boundary is a public pytest hook."""
    assert callable(budget_plugin.pytest_collection_finish)
    assert callable(budget_plugin.pytest_runtestloop)


def test_execution_clock_is_not_started_at_session_start() -> None:
    """No ``pytest_sessionstart`` hook: session start precedes collection."""
    assert not hasattr(budget_plugin, "pytest_sessionstart")


def test_time_before_execution_starts_is_not_charged_to_the_budget() -> None:
    """Time burned before the execution phase is excluded from the figure."""
    # The clock reads 900.0 when collection ends (the 900s before it is
    # collection cost, which the documentation excludes) and 902.0 at finish.
    times = iter([900.0, 902.0])
    gate = _BudgetGate(clock=lambda: next(times))
    assert gate.elapsed() is None, "clock must not be running before execution"
    gate.execution_start()
    assert gate.elapsed() == pytest.approx(2.0)


def test_execution_start_is_first_call_wins() -> None:
    """Both boundaries may fire; the earlier one is the one that counts."""
    times = iter([100.0, 103.0])
    gate = _BudgetGate(clock=lambda: next(times))
    gate.execution_start()
    gate.execution_start()
    assert gate.elapsed() == pytest.approx(3.0)


def test_live_gate_clock_is_running_while_tests_execute() -> None:
    """In a real session the clock is already started by the time a test runs.

    This exercises the actually-registered plugin, not a hand-built gate: the
    body of a test runs during the execution phase, so a non-``None`` reading
    proves the clock was started by a collection/runtestloop boundary rather
    than left unstarted.
    """
    assert budget_plugin._gate.elapsed() is not None
