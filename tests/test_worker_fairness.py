"""Stable fairness invariants of the worker database turn."""

from __future__ import annotations

import math
import time
from typing import TYPE_CHECKING, cast
from uuid import uuid4

from lubko.config import DatabaseConfig
from lubko.worker import Settings, Supervisor

if TYPE_CHECKING:
    import pytest

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
