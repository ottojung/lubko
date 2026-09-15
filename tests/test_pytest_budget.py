"""Deterministic tests for the in-process pytest session budget gate."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

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
    gate.sessionstart()
    gate.sessionfinish(session, pytest.ExitCode.OK)
    assert session.exitstatus == pytest.ExitCode.OK
    assert not gate.budget_exceeded


def test_exactly_at_budget_fails() -> None:
    """Elapsed time equal to the budget triggers failure."""
    gate = _gate_at(10.0)
    session = _fake_session()
    gate.sessionstart()
    gate.sessionfinish(session, pytest.ExitCode.OK)
    assert session.exitstatus == pytest.ExitCode.TESTS_FAILED
    assert gate.budget_exceeded


def test_over_budget_fails() -> None:
    """Elapsed time beyond the budget triggers failure."""
    gate = _gate_at(15.5)
    session = _fake_session()
    gate.sessionstart()
    gate.sessionfinish(session, pytest.ExitCode.OK)
    assert session.exitstatus == pytest.ExitCode.TESTS_FAILED
    assert gate.budget_exceeded


def test_existing_failure_not_overridden() -> None:
    """When tests already failed the budget gate must not change exitstatus."""
    gate = _gate_at(999.0)
    session = _fake_session(exitstatus=pytest.ExitCode.TESTS_FAILED)
    gate.sessionstart()
    gate.sessionfinish(session, pytest.ExitCode.TESTS_FAILED)
    assert session.exitstatus == pytest.ExitCode.TESTS_FAILED
    assert not gate.budget_exceeded


def test_existing_nonzero_exit_preserved() -> None:
    """Any nonzero exit status is preserved even if budget is exceeded."""
    gate = _gate_at(999.0)
    session = _fake_session(exitstatus=pytest.ExitCode.INTERRUPTED)
    gate.sessionstart()
    gate.sessionfinish(session, pytest.ExitCode.INTERRUPTED)
    assert session.exitstatus == pytest.ExitCode.INTERRUPTED
    assert not gate.budget_exceeded


def test_budget_constant_is_ten_seconds() -> None:
    """The configured budget matches the AGENTS.md 10.0s requirement."""
    assert pytest.approx(10.0) == BUDGET_SECONDS
