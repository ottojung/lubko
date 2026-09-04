"""Strict persisted supervisor last-exit parsing."""

from typing import Protocol

import pytest

from lubko import supervise


class _HasLastExit(Protocol):
    @property
    def last_exit(self) -> supervise.LastExit | None: ...


def _parse_all(data: dict[str, object]) -> tuple[_HasLastExit, ...]:
    return (
        supervise.SupervisorState.from_dict(data),
        supervise.SupervisorStatus.from_dict(data),
        supervise.SupervisorDiagnostic.from_dict(data),
    )


def test_canonical_last_exit_round_trips() -> None:
    """Canonical writer-shaped last-exit data is preserved."""
    for parsed in _parse_all({"last_exit": {"returncode": 7, "at": 12.5}}):
        assert parsed.last_exit == supervise.LastExit(returncode=7, at=12.5)

    for parsed in _parse_all({"last_exit": {"returncode": None, "at": 0.0}}):
        assert parsed.last_exit == supervise.LastExit(returncode=None, at=0.0)


def test_healthy_status_serialization_is_unchanged() -> None:
    """Strict parsing preserves the canonical status JSON shape."""
    status = supervise.SupervisorStatus.from_dict({"last_exit": {"returncode": 0, "at": 42.25}})
    assert status.to_dict()["last_exit"] == {"returncode": 0, "at": 42.25}


@pytest.mark.parametrize("returncode", ["7", 7.0, True, False, [], {}, "", "bad"])
def test_malformed_returncode_is_rejected(returncode: object) -> None:
    """Malformed return codes never coerce to canonical integers."""
    for parsed in _parse_all({"last_exit": {"returncode": returncode, "at": 12.5}}):
        assert parsed.last_exit is None


@pytest.mark.parametrize(
    "at",
    ["12.5", True, False, [], {}, "", float("nan"), float("inf"), float("-inf"), -1.0],
)
def test_malformed_timestamp_is_rejected(at: object) -> None:
    """Malformed or out-of-domain timestamps never become exit history."""
    for parsed in _parse_all({"last_exit": {"returncode": 1, "at": at}}):
        assert parsed.last_exit is None


def test_falsey_malformed_values_do_not_default() -> None:
    """Falsey corruption cannot collapse to None or zero defaults."""
    for parsed in _parse_all({"last_exit": {"returncode": False, "at": 1.0}}):
        assert parsed.last_exit is None
    for parsed in _parse_all({"last_exit": {"returncode": 1, "at": False}}):
        assert parsed.last_exit is None
