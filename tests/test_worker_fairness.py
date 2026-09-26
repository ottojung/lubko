"""Stable fairness invariants of the worker database turn."""

from __future__ import annotations

import math
import time
from typing import TYPE_CHECKING, cast
from uuid import uuid4

import pytest

from lubko.config import DatabaseConfig
from lubko.worker import DbOperationDeadlineError, Settings, Supervisor

if TYPE_CHECKING:
    from lubko.worker import JobsConnection


def test_claiming_precedes_optional_garbage_collection(monkeypatch: pytest.MonkeyPatch) -> None:
    """A due maintenance pass cannot consume the pending-job opportunity."""
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
    supervisor._next_reaper_at = 0.0
    supervisor._next_gc_at = 0.0
    calls: list[str] = []
    monkeypatch.setattr(supervisor, "_publish_all", lambda _now: None)
    monkeypatch.setattr(supervisor, "_finalize_completed", lambda: None)
    monkeypatch.setattr(supervisor, "_retry_terminalizations", lambda: None)
    monkeypatch.setattr(supervisor, "_claim_batch", lambda: calls.append("claim"))
    monkeypatch.setattr(supervisor, "_run_reaper", lambda: calls.append("reaper"))
    monkeypatch.setattr(supervisor, "_run_gc", lambda: calls.append("gc"))

    supervisor._db_phase(1.0)

    assert calls == ["claim", "reaper", "gc"]


def test_saturated_gc_retries_on_next_worker_turn(monkeypatch: pytest.MonkeyPatch) -> None:
    """A saturated GC pass yields, then retries on the next worker turn."""
    settings = Settings(
        worker_id="worker",
        server="server",
        poll_interval_seconds=0.1,
        process_poll_interval_seconds=0.1,
        cancel_grace_seconds=1.0,
        gc_interval_seconds=60.0,
    )
    supervisor = Supervisor(
        settings,
        DatabaseConfig(host="host", port=5432, dbname="db", user="user", password=str(uuid4())),
    )
    supervisor.conn = cast("JobsConnection", object())
    supervisor._next_recovery_at = 2.0
    supervisor._next_cancel_scan_at = 2.0
    supervisor._next_reaper_at = 2.0
    supervisor._next_gc_at = 0.0
    monkeypatch.setattr(supervisor, "_publish_all", lambda _now: None)
    monkeypatch.setattr(supervisor, "_finalize_completed", lambda: None)
    monkeypatch.setattr(supervisor, "_retry_terminalizations", lambda: None)
    monkeypatch.setattr(supervisor, "_claim_batch", lambda: None)
    monkeypatch.setattr(supervisor, "_run_gc", lambda: True)
    monkeypatch.setattr(time, "monotonic", lambda: 50.0)

    supervisor._db_phase(1.0)

    assert math.isclose(supervisor._next_gc_at, 50.1)


def test_caught_up_gc_returns_to_idle_cadence(monkeypatch: pytest.MonkeyPatch) -> None:
    """A non-saturated GC pass waits for the configured idle cadence."""
    settings = Settings(
        worker_id="worker",
        server="server",
        poll_interval_seconds=0.1,
        process_poll_interval_seconds=0.1,
        cancel_grace_seconds=1.0,
        gc_interval_seconds=60.0,
    )
    supervisor = Supervisor(
        settings,
        DatabaseConfig(host="host", port=5432, dbname="db", user="user", password=str(uuid4())),
    )
    supervisor.conn = cast("JobsConnection", object())
    supervisor._next_recovery_at = 2.0
    supervisor._next_cancel_scan_at = 2.0
    supervisor._next_reaper_at = 2.0
    supervisor._next_gc_at = 0.0
    monkeypatch.setattr(supervisor, "_publish_all", lambda _now: None)
    monkeypatch.setattr(supervisor, "_finalize_completed", lambda: None)
    monkeypatch.setattr(supervisor, "_retry_terminalizations", lambda: None)
    monkeypatch.setattr(supervisor, "_claim_batch", lambda: None)
    monkeypatch.setattr(supervisor, "_run_gc", lambda: False)
    monkeypatch.setattr(time, "monotonic", lambda: 50.0)

    supervisor._db_phase(1.0)

    assert math.isclose(supervisor._next_gc_at, 110.0)


def test_gc_failure_still_advances_to_idle_schedule(monkeypatch: pytest.MonkeyPatch) -> None:
    """A failed GC pass backs off to the normal idle cadence."""
    settings = Settings(
        worker_id="worker",
        server="server",
        poll_interval_seconds=0.1,
        process_poll_interval_seconds=0.1,
        cancel_grace_seconds=1.0,
        gc_interval_seconds=60.0,
    )
    supervisor = Supervisor(
        settings,
        DatabaseConfig(host="host", port=5432, dbname="db", user="user", password=str(uuid4())),
    )
    supervisor.conn = cast("JobsConnection", object())
    supervisor._next_recovery_at = 2.0
    supervisor._next_cancel_scan_at = 2.0
    supervisor._next_reaper_at = 2.0
    supervisor._next_gc_at = 0.0
    monkeypatch.setattr(supervisor, "_publish_all", lambda _now: None)
    monkeypatch.setattr(supervisor, "_finalize_completed", lambda: None)
    monkeypatch.setattr(supervisor, "_retry_terminalizations", lambda: None)
    monkeypatch.setattr(supervisor, "_claim_batch", lambda: None)
    monkeypatch.setattr(time, "monotonic", lambda: 50.0)

    failure = DbOperationDeadlineError("deadline")

    def fail_gc() -> bool:
        raise failure

    monkeypatch.setattr(supervisor, "_run_gc", fail_gc)

    with pytest.raises(DbOperationDeadlineError):
        supervisor._db_phase(1.0)

    assert math.isclose(supervisor._next_gc_at, 110.0)


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
    supervisor._next_gc_at = 2.0
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
