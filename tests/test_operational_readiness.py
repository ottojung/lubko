"""Operational readiness: queue-operational health separate from process liveness.

Acceptance cases for #764 plus a supervisor-level regression proving
_check_readiness consumes the stronger operational result.
"""

from __future__ import annotations

import os
import time
from typing import TYPE_CHECKING

import pytest

import lubko.health as health_module
from lubko import supervise, supervisor
from lubko.health import (
    WORKER_HEALTH_SCHEMA_VERSION,
    WorkerHealth,
    interpret_operational_readiness,
    interpret_worker_health,
    proc_start_ticks,
    worker_health_payload,
)

if TYPE_CHECKING:
    from pathlib import Path


def _snapshot(**overrides: object) -> WorkerHealth:
    """Build a minimal valid health snapshot with sensible defaults.

    Returns:
        A ``WorkerHealth`` instance with the given overrides applied.
    """
    fields: dict[str, object] = {
        "schema_version": WORKER_HEALTH_SCHEMA_VERSION,
        "worker_id": "w",
        "worker_incarnation": "inc",
        "pid": 1,
        "start_time_ticks": 100,
        "started_at": 1000.0,
        "published_at": 1000.0,
        "alive": True,
        "db_connected": True,
        "db_connected_at": 1000.0,
        "db_error_at": None,
        "active_jobs": 0,
        "stopping_jobs": 0,
        "completed_jobs": 0,
        "oldest_active_job_age_seconds": None,
        "lease_safety_margin_seconds": 5.0,
        "min_lease_safety_remaining_seconds": None,
        "db_operation_deadline_seconds": 3.0,
        "db_last_activity_at": 1000.0,
        "db_deadline_breached_at": None,
        "db_deadline_breach_count": 0,
        "capture_streams_open": 0,
        "spool_held_bytes": 0,
        "scan_batch_limit": 16,
        "last_scan_batch_size": 0,
        "last_cancellation_scan_at": None,
        "last_recovery_at": None,
        "last_gc_at": None,
        "cancellation_scan_overdue": False,
        "recovery_overdue": False,
        "gc_overdue": False,
        "gc_batch_limit": 32,
        "gc_batch_bound_hit": False,
        "cancellation_batch_limit": 100,
        "cancellation_batch_bound_hit": False,
        "recovery_batch_limit": 100,
        "recovery_batch_bound_hit": False,
        "shutting_down": False,
    }
    fields.update(overrides)
    return WorkerHealth(**fields)  # type: ignore[arg-type]


def _live_snapshot(**overrides: object) -> WorkerHealth:
    """Snapshot matching the current process for liveness acceptance.

    Returns:
        A ``WorkerHealth`` whose PID and start-time ticks match the caller.
    """
    pid = os.getpid()
    ticks = proc_start_ticks(pid)
    assert ticks is not None
    return _snapshot(
        pid=pid,
        start_time_ticks=ticks,
        published_at=time.time(),
        **overrides,
    )


def _monkey_liveness(monkeypatch: pytest.MonkeyPatch) -> None:
    """Install pidfd stubs so interpret_worker_health accepts the snapshot as live."""
    monkeypatch.setattr(
        health_module, "_open_pidfd", lambda _pid: os.open("/dev/null", os.O_RDONLY)
    )
    monkeypatch.setattr(health_module, "_process_is_live", lambda _pid: True)
    monkeypatch.setattr(health_module, "_pidfd_send_signal", lambda _fd, _sig: None)


# ------------------------------------------------------------------
# 1. All healthy => live and ready
# ------------------------------------------------------------------


def test_all_healthy_is_live_and_ready(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fresh live process with all operational signals healthy => live and ready."""
    _monkey_liveness(monkeypatch)
    eff = interpret_worker_health(_live_snapshot())
    assert eff.live is True
    assert eff.reason == "ok"
    assert eff.operational.ready is True
    assert eff.operational.reason == "ok"


# ------------------------------------------------------------------
# 2. cancellation_scan_overdue => live but operational not ready
# ------------------------------------------------------------------


def test_cancellation_overdue_makes_live_but_not_ready(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Live process with cancellation_scan_overdue => live but operational not ready."""
    _monkey_liveness(monkeypatch)
    eff = interpret_worker_health(_live_snapshot(cancellation_scan_overdue=True))
    assert eff.live is True
    assert eff.reason == "ok"
    assert eff.operational.ready is False
    assert "cancellation" in eff.operational.reason


# ------------------------------------------------------------------
# 3. negative lease-safety => not ready
# ------------------------------------------------------------------


def test_negative_lease_safety_not_ready() -> None:
    """Negative min_lease_safety_remaining => operational not ready."""
    op = interpret_operational_readiness(_snapshot(min_lease_safety_remaining_seconds=-2.0))
    assert op.ready is False
    assert op.lease_safety_negative is True
    assert "lease safety negative" in op.reason


# ------------------------------------------------------------------
# 4. unrecovered DB deadline breach => not ready
# ------------------------------------------------------------------


def test_unrecovered_db_breach_not_ready() -> None:
    """DB breach with no later successful DB phase => not ready."""
    op = interpret_operational_readiness(
        _snapshot(db_deadline_breached_at=900.0, db_last_activity_at=800.0)
    )
    assert op.ready is False
    assert op.unrecovered_db_deadline_breach is True
    assert "unrecovered DB deadline breach" in op.reason


# ------------------------------------------------------------------
# 5. recovered historical breach count => ready (not permanently degraded)
# ------------------------------------------------------------------


def test_recovered_breach_not_permanently_degraded() -> None:
    """Historical db_deadline_breach_count=387 with later activity => ready."""
    op = interpret_operational_readiness(
        _snapshot(
            db_deadline_breached_at=500.0,
            db_last_activity_at=900.0,
            db_deadline_breach_count=387,
        )
    )
    assert op.ready is True
    assert op.unrecovered_db_deadline_breach is False


def test_breach_recovered_when_activity_after_breach() -> None:
    """db_last_activity_at > db_deadline_breached_at proves recovery."""
    assert (
        health_module._db_deadline_breach_recovered(
            _snapshot(db_deadline_breached_at=900.0, db_last_activity_at=950.0)
        )
        is True
    )


def test_breach_not_recovered_when_activity_before_breach() -> None:
    """Activity timestamp older than breach timestamp is not recovery proof."""
    assert (
        health_module._db_deadline_breach_recovered(
            _snapshot(db_deadline_breached_at=900.0, db_last_activity_at=800.0)
        )
        is False
    )


def test_breach_not_recovered_when_no_activity() -> None:
    """No DB activity after breach means breach is unrecovered."""
    assert (
        health_module._db_deadline_breach_recovered(
            _snapshot(db_deadline_breached_at=900.0, db_last_activity_at=None)
        )
        is False
    )


# ------------------------------------------------------------------
# 5b. DB error recovery
# ------------------------------------------------------------------


def test_db_error_recovered_by_reconnection() -> None:
    """db_connected=True AND db_connected_at > db_error_at => recovered."""
    op = interpret_operational_readiness(
        _snapshot(db_connected=True, db_connected_at=950.0, db_error_at=900.0)
    )
    assert op.ready is True
    assert op.unrecovered_db_error is False


def test_db_error_not_recovered_when_connected_before_error() -> None:
    """Stale db_connected=True with db_connected_at BEFORE error is not recovery."""
    assert (
        health_module._db_error_recovered(
            _snapshot(db_connected=True, db_connected_at=800.0, db_error_at=900.0)
        )
        is False
    )


def test_db_error_not_recovered_when_disconnected() -> None:
    """Disconnected state with an error is unrecovered."""
    assert (
        health_module._db_error_recovered(_snapshot(db_connected=False, db_error_at=900.0)) is False
    )


def test_no_error_is_trivially_recovered() -> None:
    """No error recorded means trivially recovered."""
    assert health_module._db_error_recovered(_snapshot(db_error_at=None)) is True


# ------------------------------------------------------------------
# 6. stale/dead/PID-mismatched retains fail-closed liveness
# ------------------------------------------------------------------


def test_stale_snapshot_not_live() -> None:
    """Stale snapshot => not live, but operational still computed."""
    eff = interpret_worker_health(_snapshot(published_at=0.0), max_staleness_seconds=10.0)
    assert eff.live is False
    assert eff.stale is True


def test_future_published_at_not_live() -> None:
    """Future published_at => not live."""
    eff = interpret_worker_health(_snapshot(published_at=time.time() + 60.0))
    assert eff.live is False
    assert "future" in eff.reason


def test_none_snapshot_not_live() -> None:
    """No snapshot => not live and not operationally ready."""
    eff = interpret_worker_health(None)
    assert eff.live is False
    assert eff.operational.ready is False


def _raise_os_error(_pid: int) -> int:
    """Raise OSError for pidfd monkeypatch.

    Raises:
        OSError: Always.
    """
    msg = "pidfd unavailable"
    raise OSError(msg)


def test_pidfd_pin_failure_not_live(monkeypatch: pytest.MonkeyPatch) -> None:
    """PID that cannot be pinned => not live."""
    pid = os.getpid()
    ticks = proc_start_ticks(pid)
    assert ticks is not None
    monkeypatch.setattr(health_module, "_open_pidfd", _raise_os_error)
    eff = interpret_worker_health(
        _snapshot(pid=pid, start_time_ticks=ticks, published_at=time.time())
    )
    assert eff.live is False
    assert "could not be pinned" in eff.reason


# ------------------------------------------------------------------
# 7. supervisor _check_readiness consumes operational result
# ------------------------------------------------------------------


def test_supervisor_check_readiness_rejects_operational_degradation(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """_check_readiness returns not-ready when liveness passes but operational fails.

    This is the key regression: the supervisor must not collapse health to
    eff.live alone.  We exercise the real _check_readiness code path by
    mocking its external dependencies (queue probe, health file, pidfd)
    and proving it returns False with an operational reason.
    """
    _monkey_liveness(monkeypatch)

    token = "a" * 32
    child = supervise.WorkerChild(
        pid=os.getpid(),
        pgid=os.getpid(),
        sid=os.getpid(),
        start_time_ticks=proc_start_ticks(os.getpid()),  # type: ignore[arg-type]
        token=token,
        worker_id="test-worker",
        spawned_at=time.time(),
    )

    monkeypatch.setattr(
        "lubko.supervisor.lifecycle.verify_worker_consumes_queue",
        lambda _wid, _cwd, _pid, _timeout: True,
    )
    # Health snapshot: live but cancellation_scan_overdue => operational not ready
    snapshot = _snapshot(
        pid=child.pid,
        start_time_ticks=child.start_time_ticks,
        published_at=time.time(),
        worker_incarnation=token,
        cancellation_scan_overdue=True,
    )
    monkeypatch.setattr(
        "lubko.supervisor.read_worker_health_by_incarnation",
        lambda _tok: snapshot,
    )

    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    ready, reason = daemon._check_readiness(child, str(tmp_path))

    assert ready is False
    assert "operational not ready" in reason
    assert "cancellation" in reason
    # Liveness was accepted; the rejection is purely operational
    assert "not live" not in reason


def test_supervisor_check_readiness_accepts_fully_healthy(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """_check_readiness returns ok when both liveness and operational pass."""
    _monkey_liveness(monkeypatch)

    token = "b" * 32
    child = supervise.WorkerChild(
        pid=os.getpid(),
        pgid=os.getpid(),
        sid=os.getpid(),
        start_time_ticks=proc_start_ticks(os.getpid()),  # type: ignore[arg-type]
        token=token,
        worker_id="test-worker",
        spawned_at=time.time(),
    )

    monkeypatch.setattr(
        "lubko.supervisor.lifecycle.verify_worker_consumes_queue",
        lambda _wid, _cwd, _pid, _timeout: True,
    )
    snapshot = _snapshot(
        pid=child.pid,
        start_time_ticks=child.start_time_ticks,
        published_at=time.time(),
        worker_incarnation=token,
    )
    monkeypatch.setattr(
        "lubko.supervisor.read_worker_health_by_incarnation",
        lambda _tok: snapshot,
    )

    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    ready, reason = daemon._check_readiness(child, str(tmp_path))
    assert ready is True
    assert reason == "ok"


# ------------------------------------------------------------------
# Combined signals
# ------------------------------------------------------------------


def test_multiple_overdue_scans_reported() -> None:
    """Multiple overdue scans produce a single actionable reason."""
    op = interpret_operational_readiness(
        _snapshot(cancellation_scan_overdue=True, recovery_overdue=True, gc_overdue=True)
    )
    assert op.ready is False
    assert op.any_scan_overdue is True
    for name in ("cancellation", "recovery", "gc"):
        assert name in op.reason


def test_shutting_down_not_ready() -> None:
    """Shutting down worker is operationally not ready."""
    op = interpret_operational_readiness(_snapshot(shutting_down=True))
    assert op.ready is False
    assert op.shutting_down is True


def test_operational_reason_never_mentions_liveness() -> None:
    """Operational reason names operational conditions, never PID/staleness."""
    eff = interpret_worker_health(_snapshot(cancellation_scan_overdue=True))
    assert "PID" not in eff.operational.reason
    assert "stale" not in eff.operational.reason


def test_liveness_reason_never_mentions_operational() -> None:
    """Liveness reason names liveness conditions, never overdue/lease safety."""
    eff = interpret_worker_health(_snapshot(cancellation_scan_overdue=True))
    assert "overdue" not in eff.reason
    assert "lease safety" not in eff.reason


# ------------------------------------------------------------------
# Serialization
# ------------------------------------------------------------------


def test_payload_includes_liveness_and_overall_reason(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """worker_health_payload has liveness_reason and overall_reason."""
    _monkey_liveness(monkeypatch)
    payload = worker_health_payload(_live_snapshot())
    assert payload is not None
    assert "liveness_reason" in payload
    assert "overall_reason" in payload
    assert "operational" in payload
    assert payload["liveness_reason"] == "ok"
    assert payload["overall_reason"] == "ok"


def test_degraded_payload_exposes_unqualified_reason_ok(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Degraded operational status cannot produce overall_reason='ok'."""
    _monkey_liveness(monkeypatch)
    payload = worker_health_payload(_live_snapshot(cancellation_scan_overdue=True))
    assert payload is not None
    assert payload["overall_reason"] != "ok"
    assert "cancellation" in payload["overall_reason"]
    # liveness_reason is still ok because the process is live
    assert payload["liveness_reason"] == "ok"


def test_dead_snapshot_degraded_payload() -> None:
    """Stale/dead snapshot => overall_reason is the liveness failure, not 'ok'."""
    payload = worker_health_payload(_snapshot(published_at=0.0))
    assert payload is not None
    assert payload["overall_reason"] != "ok"
    assert payload["liveness_reason"] != "ok"


def test_operational_readiness_round_trips() -> None:
    """OperationalReadiness.to_dict() preserves all fields."""
    snapshot = _snapshot(
        cancellation_scan_overdue=True,
        min_lease_safety_remaining_seconds=-3.0,
        db_deadline_breached_at=800.0,
        db_last_activity_at=700.0,
        db_error_at=850.0,
        db_connected=False,
        shutting_down=True,
    )
    d = interpret_operational_readiness(snapshot).to_dict()
    assert d["ready"] is False
    assert d["cancellation_scan_overdue"] is True
    assert d["lease_safety_negative"] is True
    assert d["unrecovered_db_deadline_breach"] is True
    assert d["unrecovered_db_error"] is True
    assert d["shutting_down"] is True
    assert d["min_lease_safety_remaining_seconds"] == pytest.approx(-3.0)
