"""Stable fairness invariants of the worker database turn."""

from __future__ import annotations

import math
import time
from typing import TYPE_CHECKING, cast
from uuid import uuid4

import pytest

from lubko import worker
from lubko.config import DatabaseConfig
from lubko.worker import (
    DbOperationDeadlineError,
    Settings,
    Supervisor,
)

if TYPE_CHECKING:
    from lubko.worker import JobsConnection


def test_worker_db_phase_has_no_gc_dependency(monkeypatch: pytest.MonkeyPatch) -> None:
    """Normal worker DB progress never invokes transport GC."""
    settings = Settings(
        worker_id="worker",
        server="server",
        poll_interval_seconds=0.1,
        process_poll_interval_seconds=0.1,
        cancel_grace_seconds=1.0,
    )
    supervisor = Supervisor(
        settings,
        DatabaseConfig(host="host", port=5432, dbname="db", user="user", password=str(uuid4())),
    )
    supervisor.conn = cast("JobsConnection", object())
    supervisor._next_recovery_at = 2.0
    supervisor._next_cancel_scan_at = 2.0
    supervisor._next_reaper_at = 2.0
    calls: list[str] = []
    monkeypatch.setattr(supervisor, "_claim_batch", lambda: calls.append("claim"))
    monkeypatch.setattr(supervisor, "_publish_all", lambda _now: calls.append("publish"))
    monkeypatch.setattr(supervisor, "_finalize_completed", lambda: calls.append("finalize"))
    monkeypatch.setattr(supervisor, "_retry_terminalizations", lambda: calls.append("retry"))
    monkeypatch.setattr(
        worker,
        "collect_transport",
        lambda *_a, **_kw: pytest.fail("GC entered the normal worker DB phase"),
    )

    supervisor._db_phase(1.0)

    assert calls == ["claim", "publish", "finalize", "retry"]


def test_claiming_precedes_output_publication_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """A bulky publication failure cannot consume the claim opportunity."""
    settings = Settings(
        worker_id="worker",
        server="server",
        poll_interval_seconds=0.1,
        process_poll_interval_seconds=0.1,
        cancel_grace_seconds=1.0,
    )
    supervisor = Supervisor(
        settings,
        DatabaseConfig(host="host", port=5432, dbname="db", user="user", password=str(uuid4())),
    )
    supervisor.conn = cast("JobsConnection", object())
    supervisor._next_recovery_at = 2.0
    supervisor._next_cancel_scan_at = 2.0
    supervisor._next_reaper_at = 2.0
    calls: list[str] = []
    monkeypatch.setattr(supervisor, "_claim_batch", lambda: calls.append("claim"))

    failure = DbOperationDeadlineError("deadline")

    def fail_publication(_now: float) -> None:
        calls.append("publish")
        raise failure

    monkeypatch.setattr(supervisor, "_publish_all", fail_publication)
    monkeypatch.setattr(supervisor, "_finalize_completed", lambda: calls.append("finalize"))
    monkeypatch.setattr(supervisor, "_retry_terminalizations", lambda: None)

    with pytest.raises(DbOperationDeadlineError):
        supervisor._db_phase(1.0)

    assert calls == ["claim", "publish"]


def test_empty_claim_scan_is_backed_off(monkeypatch: pytest.MonkeyPatch) -> None:
    """An idle queue is not reparsed at process-poll frequency."""
    settings = Settings(
        worker_id="worker",
        server="server",
        poll_interval_seconds=0.1,
        process_poll_interval_seconds=0.1,
        cancel_grace_seconds=1.0,
    )
    supervisor = Supervisor(
        settings,
        DatabaseConfig(host="host", port=5432, dbname="db", user="user", password=str(uuid4())),
    )
    supervisor.conn = cast("JobsConnection", object())
    scans: list[float] = []
    clock = iter((10.0, 10.1, 11.1))
    monkeypatch.setattr(time, "monotonic", lambda: next(clock))

    def empty_claims(*_args: object, **_kwargs: object) -> list[object]:
        scans.append(1.0)
        return []

    monkeypatch.setattr("lubko.worker.claim_jobs", empty_claims)

    supervisor._claim_batch()
    supervisor._claim_batch()
    supervisor._claim_batch()

    assert len(scans) == 2


def test_reaper_respects_and_advances_its_schedule(monkeypatch: pytest.MonkeyPatch) -> None:
    """Retired-version maintenance runs only when due and advances its cadence."""
    settings = Settings(
        worker_id="worker",
        server="server",
        poll_interval_seconds=0.1,
        process_poll_interval_seconds=0.1,
        cancel_grace_seconds=1.0,
        lease_recovery_interval_seconds=10.0,
    )
    supervisor = Supervisor(
        settings,
        DatabaseConfig(host="host", port=5432, dbname="db", user="user", password=str(uuid4())),
    )
    supervisor.conn = cast("JobsConnection", object())
    supervisor._next_recovery_at = 2.0
    supervisor._next_cancel_scan_at = 2.0
    supervisor._next_reaper_at = 2.0
    calls: list[str] = []
    monkeypatch.setattr(supervisor, "_publish_all", lambda _now: None)
    monkeypatch.setattr(supervisor, "_finalize_completed", lambda: None)
    monkeypatch.setattr(supervisor, "_retry_terminalizations", lambda: None)
    monkeypatch.setattr(supervisor, "_claim_batch", lambda: calls.append("claim"))
    monkeypatch.setattr(supervisor, "_run_reaper", lambda: calls.append("reaper"))

    supervisor._db_phase(1.0)
    assert calls == ["claim"]
    assert math.isclose(supervisor._next_reaper_at, 2.0)

    supervisor._next_reaper_at = 0.0
    monkeypatch.setattr(time, "monotonic", lambda: 50.0)
    supervisor._db_phase(1.0)

    assert calls == ["claim", "claim", "reaper"]
    assert math.isclose(supervisor._next_reaper_at, 60.0)
