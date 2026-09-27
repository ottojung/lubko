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
    """When tests already failed the budget gate must not change exitstatus.

    The breach is still *reported* as a fact; only the exit status is left
    alone. Before issue 65 this test also asserted ``not gate.budget_exceeded``,
    which pinned the inverted behaviour this front removed.
    """
    gate = _gate_at(999.0)
    session = _fake_session(exitstatus=pytest.ExitCode.TESTS_FAILED)
    gate.execution_start()
    gate.sessionfinish(session, pytest.ExitCode.TESTS_FAILED)
    assert session.exitstatus == pytest.ExitCode.TESTS_FAILED
    assert gate.budget_exceeded


def test_existing_nonzero_exit_preserved() -> None:
    """Any nonzero exit status is preserved even if budget is exceeded."""
    gate = _gate_at(999.0)
    session = _fake_session(exitstatus=pytest.ExitCode.INTERRUPTED)
    gate.execution_start()
    gate.sessionfinish(session, pytest.ExitCode.INTERRUPTED)
    assert session.exitstatus == pytest.ExitCode.INTERRUPTED
    assert gate.budget_exceeded


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


# --- The gate reports its verdict on every run ----------------------------
#
# A gate that is silent on a red run is the worst kind: the least healthy run
# reports least. These tests pin that a verdict is always produced, and that it
# is worded so it cannot be misread as claiming blame for an unrelated failure.


def test_green_run_reports_ok() -> None:
    """An under-budget green run says so instead of printing nothing."""
    gate = _gate_at(3.28)
    session = _fake_session()
    gate.execution_start()
    line = gate.sessionfinish(session, pytest.ExitCode.OK)
    assert "OK" in line
    assert "3.28s execution" in line
    assert not gate.budget_exceeded


def test_red_run_still_reports_its_budget_verdict() -> None:
    """A run that already failed tests still reports the budget figure."""
    gate = _gate_at(1.0)
    session = _fake_session(exitstatus=pytest.ExitCode.TESTS_FAILED)
    gate.execution_start()
    line = gate.sessionfinish(session, pytest.ExitCode.TESTS_FAILED)
    assert "1.00s execution" in line
    assert "OK" in line


def test_over_budget_red_run_reports_fail_without_claiming_blame() -> None:
    """Over budget *and* red: report the breach, disown the exit status."""
    gate = _gate_at(12.5)
    session = _fake_session(exitstatus=pytest.ExitCode.TESTS_FAILED)
    gate.execution_start()
    line = gate.sessionfinish(session, pytest.ExitCode.TESTS_FAILED)
    assert gate.budget_exceeded
    assert "FAIL" in line
    assert "12.50s execution" in line
    assert "Reported for information only" in line
    assert "not the cause of that status" in line


def test_over_budget_green_run_takes_credit_for_the_exit_status() -> None:
    """Over budget with an otherwise green session: the gate is the cause."""
    gate = _gate_at(11.0)
    session = _fake_session()
    gate.execution_start()
    line = gate.sessionfinish(session, pytest.ExitCode.OK)
    assert session.exitstatus == pytest.ExitCode.TESTS_FAILED
    assert "the reason this session exits non-zero" in line


def test_verdict_is_written_to_the_terminal_reporter() -> None:
    """The line reaches the terminal, not just the return value."""
    gate = _gate_at(0.5)
    session = _fake_session()
    gate.execution_start()
    gate.sessionfinish(session, pytest.ExitCode.OK)
    session.config.pluginmanager.get_plugin.assert_called_once_with("terminalreporter")
    session.config.pluginmanager.get_plugin.return_value.write_line.assert_called_once()


def test_reported_figure_is_execution_not_session() -> None:
    """The reported number is the execution interval, not the whole session."""
    gate = _gate_at(3.28, budget=100.0)
    session = _fake_session()
    gate.execution_start()
    line = gate.sessionfinish(session, pytest.ExitCode.OK)
    assert "3.28s execution" in line
    assert "collection excluded" in line


def test_missing_execution_phase_is_reported_not_silently_ignored() -> None:
    """A session that never reached execution says so instead of implying OK."""
    gate = _BudgetGate(clock=lambda: 0.0)
    session = _fake_session(exitstatus=pytest.ExitCode.INTERRUPTED)
    line = gate.sessionfinish(session, pytest.ExitCode.INTERRUPTED)
    assert "NOT MEASURED" in line
    assert not gate.budget_exceeded
    assert session.exitstatus == pytest.ExitCode.INTERRUPTED
