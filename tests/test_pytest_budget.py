"""Deterministic tests for the in-process pytest session budget gate."""

from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pytest

from tests._pytest_budget import BUDGET_SECONDS, _BudgetGate

if TYPE_CHECKING:
    from collections.abc import Callable


def _fake_session(exitstatus: pytest.ExitCode = pytest.ExitCode.OK) -> MagicMock:
    """Return a minimal mock ``pytest.Session``."""
    session = MagicMock()
    session.exitstatus = exitstatus
    return session


def _make_gate(
    clock: Callable[[], float] | None = None,
    budget: float = BUDGET_SECONDS,
) -> _BudgetGate:
    return _BudgetGate(clock=clock, budget=budget)


# -- below budget -----------------------------------------------------------


class TestBelowBudget:
    """Session completes within budget and passes."""

    def test_ok_when_fast(self) -> None:
        times = iter([0.0, 9.99])
        gate = _make_gate(clock=lambda: next(times))
        session = _fake_session()

        gate.sessionstart(session)
        gate.sessionfinish(session, pytest.ExitCode.OK)

        assert session.exitstatus == pytest.ExitCode.OK
        assert gate.budget_exceeded is False

    def test_zero_elapsed(self) -> None:
        gate = _make_gate(clock=lambda: 0.0)
        session = _fake_session()

        gate.sessionstart(session)
        gate.sessionfinish(session, pytest.ExitCode.OK)

        assert session.exitstatus == pytest.ExitCode.OK
        assert gate.budget_exceeded is False


# -- at / over budget -------------------------------------------------------


class TestAtOrOverBudget:
    """Session reaching or exceeding the budget is failed."""

    def test_at_budget_is_failure(self) -> None:
        times = iter([0.0, 10.0])
        gate = _make_gate(clock=lambda: next(times))
        session = _fake_session()

        gate.sessionstart(session)
        gate.sessionfinish(session, pytest.ExitCode.OK)

        assert session.exitstatus == pytest.ExitCode.TESTS_FAILED
        assert gate.budget_exceeded is True

    def test_over_budget_is_failure(self) -> None:
        times = iter([0.0, 15.5])
        gate = _make_gate(clock=lambda: next(times))
        session = _fake_session()

        gate.sessionstart(session)
        gate.sessionfinish(session, pytest.ExitCode.OK)

        assert session.exitstatus == pytest.ExitCode.TESTS_FAILED
        assert gate.budget_exceeded is True

    def test_just_under_budget_passes(self) -> None:
        times = iter([0.0, 9.999])
        gate = _make_gate(clock=lambda: next(times))
        session = _fake_session()

        gate.sessionstart(session)
        gate.sessionfinish(session, pytest.ExitCode.OK)

        assert session.exitstatus == pytest.ExitCode.OK
        assert gate.budget_exceeded is False


# -- existing failures are not masked ---------------------------------------


class TestExistingFailuresPreserved:
    """When tests already failed, the budget gate must not change exitstatus."""

    def test_existing_failure_not_overridden(self) -> None:
        times = iter([0.0, 999.0])
        gate = _make_gate(clock=lambda: next(times))
        session = _fake_session(exitstatus=pytest.ExitCode.TESTS_FAILED)

        gate.sessionstart(session)
        gate.sessionfinish(session, pytest.ExitCode.TESTS_FAILED)

        assert session.exitstatus == pytest.ExitCode.TESTS_FAILED
        assert gate.budget_exceeded is False

    def test_existing_nonzero_not_overridden(self) -> None:
        times = iter([0.0, 999.0])
        gate = _make_gate(clock=lambda: next(times))
        session = _fake_session(exitstatus=pytest.ExitCode.INTERRUPTED)

        gate.sessionstart(session)
        gate.sessionfinish(session, pytest.ExitCode.INTERRUPTED)

        assert session.exitstatus == pytest.ExitCode.INTERRUPTED
        assert gate.budget_exceeded is False


# -- budget constant --------------------------------------------------------


class TestBudgetConstant:
    """Verify the budget constant matches AGENTS.md requirement."""

    def test_budget_is_ten_seconds(self) -> None:
        assert BUDGET_SECONDS == 10.0


# -- custom budget for test isolation ---------------------------------------


class TestCustomBudget:
    """Tests can exercise the gate with a reduced budget for determinism."""

    def test_custom_budget_low(self) -> None:
        times = iter([0.0, 1.0])
        gate = _make_gate(clock=lambda: next(times), budget=1.0)
        session = _fake_session()

        gate.sessionstart(session)
        gate.sessionfinish(session, pytest.ExitCode.OK)

        assert session.exitstatus == pytest.ExitCode.TESTS_FAILED
        assert gate.budget_exceeded is True

    def test_custom_budget_just_under(self) -> None:
        times = iter([0.0, 0.5])
        gate = _make_gate(clock=lambda: next(times), budget=1.0)
        session = _fake_session()

        gate.sessionstart(session)
        gate.sessionfinish(session, pytest.ExitCode.OK)

        assert session.exitstatus == pytest.ExitCode.OK
        assert gate.budget_exceeded is False
