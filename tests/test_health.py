"""Non-finite worker health timestamps fail closed; finite behavior is kept."""

import json
import math
import os
import time

import pytest

from lubko.health import (
    WORKER_HEALTH_SCHEMA_VERSION,
    WorkerHealth,
    interpret_worker_health,
    proc_start_ticks,
    worker_health_payload,
)


def _snapshot(**overrides: object) -> WorkerHealth:
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
        "oldest_active_job_id": None,
        "oldest_active_job_age_seconds": None,
        "lease_safety_margin_seconds": 5.0,
        "min_lease_remaining_seconds": None,
        "db_operation_deadline_seconds": 3.0,
        "db_last_activity_at": 1000.0,
        "capture_streams_open": 0,
        "spool_held_bytes": 0,
        "scan_batch_limit": 16,
        "last_scan_batch_size": 0,
        "shutting_down": False,
    }
    fields.update(overrides)
    return WorkerHealth(**fields)  # type: ignore[arg-type]


def _from_dict_or_none(data: dict[str, object]) -> WorkerHealth | None:
    try:
        return WorkerHealth.from_dict(data)
    except (TypeError, ValueError, KeyError):
        return None


@pytest.mark.parametrize("bad", [math.nan, math.inf, -math.inf])
@pytest.mark.parametrize(
    "field",
    [
        "published_at",
        "started_at",
        "lease_safety_margin_seconds",
        "db_operation_deadline_seconds",
    ],
)
def test_persisted_non_finite_required_timestamps_fail_closed(bad: float, field: str) -> None:
    """NaN/Infinity required timestamps never parse into durable state."""
    data = json.loads(json.dumps(_snapshot().to_dict()))
    data[field] = bad
    with pytest.raises(ValueError, match=field):
        WorkerHealth.from_dict(data)
    assert _from_dict_or_none(data) is None


@pytest.mark.parametrize(
    ("field", "bad"),
    [
        ("db_connected_at", math.nan),
        ("db_error_at", math.inf),
        ("oldest_active_job_age_seconds", -math.inf),
        ("min_lease_remaining_seconds", math.nan),
        ("db_last_activity_at", math.inf),
    ],
)
def test_persisted_optional_timestamps_reject_non_finite(field: str, bad: float) -> None:
    """Optional timestamp fields reject non-finite values when present."""
    data = json.loads(json.dumps(_snapshot().to_dict()))
    data[field] = bad
    with pytest.raises(ValueError, match="finite"):
        WorkerHealth.from_dict(data)


@pytest.mark.parametrize("bad", [math.nan, math.inf, -math.inf])
def test_directly_constructed_non_finite_published_at_never_live(bad: float) -> None:
    """interpret_worker_health fails closed on non-finite published_at."""
    effective = interpret_worker_health(_snapshot(published_at=bad), max_staleness_seconds=0.0)
    assert effective.live is False
    assert effective.stale is True


def test_zero_staleness_cannot_be_bypassed_by_nan() -> None:
    """A live PID with exact start ticks still cannot appear fresh via NaN."""
    pid = os.getpid()
    ticks = proc_start_ticks(pid)
    assert ticks is not None
    snapshot = _snapshot(pid=pid, start_time_ticks=ticks, published_at=math.nan)
    effective = interpret_worker_health(snapshot, max_staleness_seconds=0.0)
    assert effective.live is False
    assert effective.stale is True


@pytest.mark.parametrize("bad", [math.nan, math.inf, -math.inf])
def test_health_serialization_never_emits_non_finite_values(bad: float) -> None:
    """Serialized health/status payloads stay strict-JSON-safe."""
    payload = worker_health_payload(_snapshot(published_at=bad), max_staleness_seconds=10.0)
    assert payload is not None
    text = json.dumps(payload, allow_nan=False)
    assert "NaN" not in text
    assert "Infinity" not in text
    dumped = json.dumps(_snapshot(published_at=bad).to_dict(), allow_nan=False)
    assert "NaN" not in dumped
    assert "Infinity" not in dumped


def test_ordinary_finite_snapshot_keeps_current_behavior() -> None:
    """Finite timestamps round-trip and interpret exactly as before."""
    snapshot = _snapshot(published_at=time.time())
    assert WorkerHealth.from_dict(snapshot.to_dict()) == snapshot
    fresh = interpret_worker_health(snapshot, max_staleness_seconds=10.0)
    assert fresh.stale is False
    stale = interpret_worker_health(_snapshot(published_at=0.0), max_staleness_seconds=10.0)
    assert stale.live is False
    assert stale.stale is True


def test_concurrency_aware_aggregates_are_bounded() -> None:
    """Health exposes job aggregates, not a single misleading job id."""
    snapshot = _snapshot(
        active_jobs=3,
        stopping_jobs=1,
        completed_jobs=7,
        oldest_active_job_id="abc",
        oldest_active_job_age_seconds=12.5,
        capture_streams_open=2,
        spool_held_bytes=4096,
        scan_batch_limit=16,
        last_scan_batch_size=4,
        min_lease_remaining_seconds=-1.0,
    )
    restored = WorkerHealth.from_dict(snapshot.to_dict())
    assert restored.active_jobs == 3
    assert restored.stopping_jobs == 1
    assert restored.completed_jobs == 7
    assert restored.oldest_active_job_id == "abc"
    assert restored.oldest_active_job_age_seconds == pytest.approx(12.5)
    assert restored.spool_held_bytes == 4096
    assert restored.last_scan_batch_size == 4
    assert restored.min_lease_remaining_seconds == pytest.approx(-1.0)


def test_unsupported_schema_version_fails_closed() -> None:
    """An old singular-schema (v1) snapshot is never treated as current."""
    data = json.loads(json.dumps(_snapshot().to_dict()))
    data["schema_version"] = 1
    with pytest.raises(ValueError, match="schema"):
        WorkerHealth.from_dict(data)
    assert _from_dict_or_none(data) is None
