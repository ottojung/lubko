"""Executable premises for the transport-GC liveness theorem."""

from __future__ import annotations

import threading
import time
from typing import TYPE_CHECKING, cast
from uuid import uuid4

import pytest

from lubko import worker
from lubko.config import DatabaseConfig
from lubko.worker import MAX_GC_BATCH_LIMIT, Settings, Supervisor

if TYPE_CHECKING:
    from lubko.worker import JobsConnection


def _settings(**overrides: object) -> Settings:
    values: dict[str, object] = {
        "worker_id": "worker",
        "server": "server",
        "poll_interval_seconds": 0.1,
        "process_poll_interval_seconds": 0.1,
        "cancel_grace_seconds": 1.0,
    }
    values.update(overrides)
    return Settings(**values)  # type: ignore[arg-type]


def _database() -> DatabaseConfig:
    return DatabaseConfig(
        host="host",
        port=5432,
        dbname="db",
        user="user",
        password=str(uuid4()),
    )


def test_gc_batch_limit_has_a_hard_constant_upper_bound() -> None:
    """Supported configuration cannot invalidate the row-lock bound."""
    _settings(gc_batch_limit=MAX_GC_BATCH_LIMIT)
    with pytest.raises(ValueError, match="constant-bounded"):
        _settings(gc_batch_limit=MAX_GC_BATCH_LIMIT + 1)


def test_gc_stop_never_joins_a_blocked_gc_thread(monkeypatch: pytest.MonkeyPatch) -> None:
    """Shutdown signaling is constant-time even if a GC pass is wedged."""
    runner = worker._GcRunner(_settings(), _database())
    entered = threading.Event()
    release = threading.Event()

    def blocked_pass() -> bool:
        entered.set()
        release.wait(timeout=2.0)
        return False

    monkeypatch.setattr(runner, "_run_once", blocked_pass)
    runner.start()
    assert entered.wait(timeout=1.0)

    start = time.monotonic()
    runner.stop()
    elapsed = time.monotonic() - start
    release.set()

    assert elapsed < 0.05


def test_blocked_gc_cannot_delay_worker_db_phase(monkeypatch: pytest.MonkeyPatch) -> None:
    """The worker DB phase progresses while the daemon GC thread is blocked."""
    supervisor = Supervisor(_settings(), _database())
    supervisor.conn = cast("JobsConnection", object())
    supervisor._next_recovery_at = 2.0
    supervisor._next_cancel_scan_at = 2.0
    supervisor._next_reaper_at = 2.0

    entered = threading.Event()
    release = threading.Event()

    def blocked_pass() -> bool:
        entered.set()
        release.wait(timeout=2.0)
        return False

    monkeypatch.setattr(supervisor._gc_runner, "_run_once", blocked_pass)
    supervisor._gc_runner.start()
    assert entered.wait(timeout=1.0)

    calls: list[str] = []
    monkeypatch.setattr(supervisor, "_claim_batch", lambda: calls.append("claim"))
    monkeypatch.setattr(supervisor, "_publish_all", lambda _now: calls.append("publish"))
    monkeypatch.setattr(supervisor, "_finalize_completed", lambda: calls.append("finalize"))
    monkeypatch.setattr(supervisor, "_retry_terminalizations", lambda: calls.append("retry"))

    start = time.monotonic()
    supervisor._db_phase(1.0)
    elapsed = time.monotonic() - start
    release.set()
    supervisor._gc_runner.stop()

    assert calls == ["claim", "publish", "finalize", "retry"]
    assert elapsed < 0.05


def test_gc_connection_has_finite_lock_and_statement_waits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The GC connection carries hard lock and statement deadlines."""
    captured: dict[str, object] = {}

    class FakeConn:
        operation_deadline = 0.0

    def fake_connect(*args: object, **kwargs: object) -> FakeConn:
        captured.update(kwargs)
        return FakeConn()

    monkeypatch.setattr(worker.DeadlineConnection, "connect", fake_connect)
    runner = worker._GcRunner(_settings(), _database())

    conn = runner._connect()

    options = str(captured["options"])
    assert f"lock_timeout={worker.GC_LOCK_TIMEOUT_MS}" in options
    assert "statement_timeout=" in options
    assert conn.operation_deadline > time.monotonic()
