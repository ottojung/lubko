"""Strict persisted supervisor status parsing invariants."""

from __future__ import annotations

import math

import pytest

from lubko import supervise


def _status() -> supervise.SupervisorStatus:
    """Return one canonical status snapshot."""
    return supervise.SupervisorStatus(
        schema_version=supervise.SCHEMA_VERSION,
        supervisor_pid=4242,
        supervisor_start_time_ticks=111,
        started_at=1.25,
        applied_generation=7,
        mode=supervise.MODE_RUN,
        commit="a" * 40,
        child=None,
        intent=supervise.INTENT_RUN,
        restart_count=2,
        next_attempt_at=3.5,
        last_exit=None,
        mission="deploy",
        db_ready=True,
        ready=False,
        message="warming",
        worker_health={"alive": True},
        holding=False,
    )


def test_canonical_supervisor_status_round_trips() -> None:
    """Canonical writer output remains readable without normalization."""
    status = _status()
    assert supervise.SupervisorStatus.from_dict(status.to_dict()) == status


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("schema_version", "1"),
        ("schema_version", True),
        ("supervisor_pid", "4242"),
        ("supervisor_pid", 4242.0),
        ("supervisor_pid", True),
        ("supervisor_start_time_ticks", "111"),
        ("started_at", "1.25"),
        ("started_at", True),
        ("started_at", math.inf),
        ("started_at", math.nan),
        ("applied_generation", "7"),
        ("applied_generation", 7.0),
        ("restart_count", False),
        ("restart_count", -1),
        ("next_attempt_at", "3.5"),
        ("next_attempt_at", math.inf),
        ("holding", "false"),
        ("holding", None),
        ("db_ready", "true"),
        ("ready", 0),
        ("mode", ["run"]),
        ("mode", "unknown"),
        ("intent", {}),
        ("intent", "unknown"),
        ("commit", 123),
        ("mission", []),
        ("message", False),
        ("worker_health", []),
    ],
)
def test_malformed_supervisor_status_scalars_are_rejected(key: str, value: object) -> None:
    """Present malformed values cannot become canonical-looking status."""
    data = _status().to_dict()
    data[key] = value

    with pytest.raises((TypeError, ValueError)):
        supervise.SupervisorStatus.from_dict(data)


def test_malformed_status_child_is_not_erased_as_absence() -> None:
    """A corrupt present child remains an explicit parse failure."""
    data = _status().to_dict()
    data["child"] = {"pid": "4242"}

    with pytest.raises((TypeError, ValueError, KeyError)):
        supervise.SupervisorStatus.from_dict(data)


def test_read_status_treats_malformed_snapshot_as_unusable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The observation reader returns no status when stored scalars are corrupt."""
    data = _status().to_dict()
    data["supervisor_pid"] = "4242"
    monkeypatch.setattr(supervise, "_read_json", lambda _path: data)

    assert supervise.read_status() is None
