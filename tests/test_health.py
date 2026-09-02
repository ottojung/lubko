"""Persisted worker health fails closed on malformed scalar authority."""

import json
import math
import os
import time
from pathlib import Path

import pytest

import lubko.health as health_module
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
        ("min_lease_safety_remaining_seconds", math.nan),
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


def test_future_published_at_cannot_bypass_staleness() -> None:
    """A future-dated snapshot never becomes positive liveness authority."""
    pid = os.getpid()
    ticks = proc_start_ticks(pid)
    assert ticks is not None
    snapshot = _snapshot(pid=pid, start_time_ticks=ticks, published_at=time.time() + 60.0)
    effective = interpret_worker_health(snapshot, max_staleness_seconds=0.0)
    assert effective.live is False
    assert effective.stale is True
    assert "future" in effective.reason


@pytest.mark.parametrize(
    "field",
    [
        "started_at",
        "published_at",
        "lease_safety_margin_seconds",
        "db_operation_deadline_seconds",
    ],
)
def test_missing_required_finite_health_fields_fail_closed(field: str) -> None:
    """Required finite health metrics cannot manufacture zero from absence."""
    data = _snapshot().to_dict()
    del data[field]
    with pytest.raises(ValueError, match=field):
        WorkerHealth.from_dict(data)
    assert _from_dict_or_none(data) is None


def test_missing_db_deadline_cannot_bypass_strictly_positive_domain() -> None:
    """Missing and explicit zero database deadlines both fail closed."""
    missing = _snapshot().to_dict()
    del missing["db_operation_deadline_seconds"]
    explicit_zero = _snapshot().to_dict()
    explicit_zero["db_operation_deadline_seconds"] = 0.0
    for data in (missing, explicit_zero):
        with pytest.raises(ValueError, match="db_operation_deadline_seconds"):
            WorkerHealth.from_dict(data)


@pytest.mark.parametrize(
    "field",
    [
        "started_at",
        "published_at",
        "db_connected_at",
        "db_error_at",
        "db_last_activity_at",
        "db_deadline_breached_at",
        "last_cancellation_scan_at",
        "last_recovery_at",
        "last_gc_at",
    ],
)
def test_negative_wall_clock_timestamps_fail_closed(field: str) -> None:
    """Persisted wall-clock timestamps stay inside the writer domain."""
    data = _snapshot().to_dict()
    data[field] = -1.0
    with pytest.raises(ValueError, match=field):
        WorkerHealth.from_dict(data)
    assert _from_dict_or_none(data) is None


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


def test_pidfd_pinned_health_rejects_disappearance_after_identity_proof(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A vanished pinned worker cannot borrow a reused numeric PID's liveness."""
    pid = os.getpid()
    ticks = proc_start_ticks(pid)
    assert ticks is not None
    snapshot = _snapshot(pid=pid, start_time_ticks=ticks, published_at=time.time())
    monkeypatch.setattr(
        health_module, "_open_pidfd", lambda _pid: os.open("/dev/null", os.O_RDONLY)
    )
    monkeypatch.setattr(health_module, "_process_is_live", lambda _pid: True)

    def vanished(_pidfd: int, _sig: int) -> None:
        raise ProcessLookupError

    monkeypatch.setattr(health_module, "_pidfd_send_signal", vanished)
    effective = interpret_worker_health(snapshot)
    assert effective.live is False
    assert "disappeared" in effective.reason


def test_pidfd_pinned_health_accepts_same_live_process(monkeypatch: pytest.MonkeyPatch) -> None:
    """The same pinned process remains valid through final liveness acceptance."""
    pid = os.getpid()
    ticks = proc_start_ticks(pid)
    assert ticks is not None
    snapshot = _snapshot(pid=pid, start_time_ticks=ticks, published_at=time.time())
    monkeypatch.setattr(
        health_module, "_open_pidfd", lambda _pid: os.open("/dev/null", os.O_RDONLY)
    )
    monkeypatch.setattr(health_module, "_process_is_live", lambda _pid: True)
    monkeypatch.setattr(health_module, "_pidfd_send_signal", lambda _fd, _sig: None)
    assert interpret_worker_health(snapshot).live is True


def test_pidfd_capability_failure_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    """Unavailable process pinning is ambiguous authority and therefore not live."""
    pid = os.getpid()
    ticks = proc_start_ticks(pid)
    assert ticks is not None
    snapshot = _snapshot(pid=pid, start_time_ticks=ticks, published_at=time.time())

    def unavailable(_pid: int) -> int:
        raise OSError

    monkeypatch.setattr(health_module, "_open_pidfd", unavailable)
    effective = interpret_worker_health(snapshot)
    assert effective.live is False
    assert "could not be pinned" in effective.reason


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
        oldest_active_job_age_seconds=12.5,
        capture_streams_open=2,
        spool_held_bytes=4096,
        scan_batch_limit=16,
        last_scan_batch_size=4,
        min_lease_safety_remaining_seconds=-1.0,
        db_deadline_breached_at=500.0,
        db_deadline_breach_count=2,
        last_cancellation_scan_at=900.0,
        last_recovery_at=901.0,
        last_gc_at=902.0,
        cancellation_scan_overdue=True,
        recovery_overdue=False,
        gc_overdue=True,
        gc_batch_limit=32,
        gc_batch_bound_hit=True,
        cancellation_batch_limit=100,
        cancellation_batch_bound_hit=True,
        recovery_batch_limit=100,
        recovery_batch_bound_hit=True,
    )
    restored = WorkerHealth.from_dict(snapshot.to_dict())
    assert restored.active_jobs == 3
    assert restored.stopping_jobs == 1
    assert restored.completed_jobs == 7
    assert restored.oldest_active_job_age_seconds == pytest.approx(12.5)
    assert restored.spool_held_bytes == 4096
    assert restored.last_scan_batch_size == 4
    assert restored.min_lease_safety_remaining_seconds == pytest.approx(-1.0)
    assert restored.db_deadline_breached_at == pytest.approx(500.0)
    assert restored.db_deadline_breach_count == 2
    assert restored.cancellation_scan_overdue is True
    assert restored.recovery_overdue is False
    assert restored.gc_overdue is True
    assert restored.gc_batch_limit == 32
    assert restored.gc_batch_bound_hit is True
    assert restored.cancellation_batch_limit == 100
    assert restored.cancellation_batch_bound_hit is True
    assert restored.recovery_batch_limit == 100
    assert restored.recovery_batch_bound_hit is True


def test_no_job_identity_is_published() -> None:
    """The health schema never carries a per-job identifier."""
    payload = _snapshot().to_dict()
    assert "oldest_active_job_id" not in payload
    assert "current_job_id" not in payload
    assert "worker_id" in payload  # bounded process identity, not a job id


@pytest.mark.parametrize(
    ("field", "bad"),
    [
        ("schema_version", "2"),
        ("worker_id", 7),
        ("worker_incarnation", ["inc"]),
        ("pid", "1"),
        ("pid", 1.0),
        ("start_time_ticks", "100"),
        ("started_at", "1000.0"),
        ("published_at", "1000.0"),
        ("db_connected_at", "1000.0"),
        ("active_jobs", "1"),
        ("active_jobs", 1.0),
    ],
)
def test_present_scalar_fields_require_their_json_schema_types(field: str, bad: object) -> None:
    """Present scalar fields are never normalized from malformed JSON types."""
    data = json.loads(json.dumps(_snapshot().to_dict()))
    data[field] = bad
    with pytest.raises((TypeError, ValueError)):
        WorkerHealth.from_dict(data)
    assert _from_dict_or_none(data) is None


def test_malformed_identity_file_is_not_usable_health(tmp_path: Path) -> None:
    """A persisted malformed identity cannot become effective live health."""
    data = json.loads(json.dumps(_snapshot().to_dict()))
    data["pid"] = "1"
    path = tmp_path / "health.json"
    path.write_text(json.dumps(data), encoding="utf-8")
    assert health_module._read_health_file(path) is None


def test_unsupported_schema_version_fails_closed() -> None:
    """An old singular-schema (v1) snapshot is never treated as current."""
    data = json.loads(json.dumps(_snapshot().to_dict()))
    data["schema_version"] = 1
    with pytest.raises(ValueError, match="schema"):
        WorkerHealth.from_dict(data)
    assert _from_dict_or_none(data) is None


@pytest.mark.parametrize(
    ("field", "bad"),
    [
        ("schema_version", "2"),
        ("schema_version", 2.0),
        ("schema_version", True),
        ("pid", "1"),
        ("pid", 1.5),
        ("pid", True),
        ("pid", 0),
        ("start_time_ticks", "100"),
        ("start_time_ticks", 100.0),
        ("start_time_ticks", True),
        ("start_time_ticks", 0),
    ],
)
def test_process_identity_fields_require_exact_json_integers(field: str, bad: object) -> None:
    """Persisted process identity never gains authority through numeric coercion."""
    data = json.loads(json.dumps(_snapshot().to_dict()))
    data[field] = bad
    with pytest.raises((TypeError, ValueError), match=field):
        WorkerHealth.from_dict(data)
    assert _from_dict_or_none(data) is None


@pytest.mark.parametrize("bad_schema", ["bogus", [], {}, 2.0, "2"])
def test_health_file_reader_fails_closed_on_malformed_schema(
    tmp_path: Path, bad_schema: object
) -> None:
    """Malformed schema authority never escapes the fail-closed disk reader."""
    data = _snapshot().to_dict()
    data["schema_version"] = bad_schema
    path = tmp_path / "health.json"
    path.write_text(json.dumps(data), encoding="utf-8")
    assert health_module._read_health_file(path) is None


def test_fractional_pid_cannot_be_truncated_into_live_health() -> None:
    """A malformed fractional PID cannot normalize to this process and become live."""
    pid = os.getpid()
    ticks = proc_start_ticks(pid)
    assert ticks is not None
    data = _snapshot(
        pid=pid,
        start_time_ticks=ticks,
        published_at=time.time(),
    ).to_dict()
    data["pid"] = float(pid) + 0.9
    with pytest.raises(TypeError, match="pid"):
        WorkerHealth.from_dict(data)
    assert _from_dict_or_none(data) is None


@pytest.mark.parametrize(
    "field",
    [
        "active_jobs",
        "stopping_jobs",
        "completed_jobs",
        "db_deadline_breach_count",
        "capture_streams_open",
        "spool_held_bytes",
        "last_scan_batch_size",
    ],
)
def test_persisted_worker_health_counts_reject_negative_values(field: str) -> None:
    """Counts and sizes outside their writer domain fail closed."""
    data = _snapshot().to_dict()
    data[field] = -1
    with pytest.raises(ValueError, match=field):
        WorkerHealth.from_dict(data)
    assert _from_dict_or_none(data) is None


@pytest.mark.parametrize(
    "field",
    [
        "scan_batch_limit",
        "gc_batch_limit",
        "cancellation_batch_limit",
        "recovery_batch_limit",
    ],
)
@pytest.mark.parametrize("bad", [0, -1])
def test_persisted_worker_health_batch_limits_must_be_positive(field: str, bad: int) -> None:
    """Configured batch limits retain the positive runtime contract."""
    data = _snapshot().to_dict()
    data[field] = bad
    with pytest.raises(ValueError, match=field):
        WorkerHealth.from_dict(data)


@pytest.mark.parametrize(
    ("field", "bad"),
    [
        ("oldest_active_job_age_seconds", -0.1),
        ("lease_safety_margin_seconds", -0.1),
        ("db_operation_deadline_seconds", 0.0),
        ("db_operation_deadline_seconds", -0.1),
    ],
)
def test_persisted_worker_health_durations_enforce_runtime_domains(field: str, bad: float) -> None:
    """Elapsed/configured durations reject semantically impossible values."""
    data = _snapshot().to_dict()
    data[field] = bad
    with pytest.raises(ValueError, match=field):
        WorkerHealth.from_dict(data)


def test_negative_lease_safety_remaining_is_intentionally_preserved() -> None:
    """Negative remaining safety budget still means its deadline has passed."""
    data = _snapshot().to_dict()
    data["min_lease_safety_remaining_seconds"] = -1.5
    restored = WorkerHealth.from_dict(data)
    assert restored.min_lease_safety_remaining_seconds == pytest.approx(-1.5)


def test_health_file_reader_rejects_out_of_domain_metric(tmp_path: Path) -> None:
    """The public persisted-health read boundary fails closed on bad domains."""
    data = _snapshot().to_dict()
    data["active_jobs"] = -1
    path = tmp_path / "health.json"
    path.write_text(json.dumps(data), encoding="utf-8")
    assert health_module._read_health_file(path) is None


def test_canonical_worker_health_round_trips() -> None:
    """Canonical writer output preserves every health field."""
    snapshot = _snapshot()
    assert WorkerHealth.from_dict(snapshot.to_dict()) == snapshot


@pytest.mark.parametrize(
    "field",
    [
        "scan_batch_limit",
        "gc_batch_limit",
        "cancellation_batch_limit",
        "recovery_batch_limit",
    ],
)
def test_required_batch_limits_reject_absence(field: str) -> None:
    """Required positive configuration cannot disappear into an invalid zero."""
    data = _snapshot().to_dict()
    del data[field]
    with pytest.raises(TypeError, match=field):
        WorkerHealth.from_dict(data)


@pytest.mark.parametrize(
    "field",
    [
        "active_jobs",
        "stopping_jobs",
        "completed_jobs",
        "db_deadline_breach_count",
        "capture_streams_open",
        "spool_held_bytes",
        "last_scan_batch_size",
    ],
)
def test_legacy_optional_counts_default_absence_to_zero(field: str) -> None:
    """Backward-compatible count fields deliberately retain zero-on-absence semantics."""
    data = _snapshot().to_dict()
    del data[field]
    parsed = WorkerHealth.from_dict(data)
    assert getattr(parsed, field) == 0


@pytest.mark.parametrize(
    "field",
    [
        "scan_batch_limit",
        "gc_batch_limit",
        "cancellation_batch_limit",
        "recovery_batch_limit",
    ],
)
def test_health_file_reader_rejects_missing_required_batch_limit(
    tmp_path: Path, field: str
) -> None:
    """Persisted health files fail closed when required configuration is truncated."""
    data = _snapshot().to_dict()
    del data[field]
    path = tmp_path / "health.json"
    path.write_text(json.dumps(data), encoding="utf-8")
    assert health_module._read_health_file(path) is None


@pytest.mark.parametrize("token", ["", "bad token", "a/b", ".", "..", "bad.token"])
def test_persisted_health_rejects_invalid_incarnation_tokens(token: str) -> None:
    """Incarnation strings outside the artifact-safe domain are unusable health."""
    data = _snapshot().to_dict()
    data["worker_incarnation"] = token
    with pytest.raises(ValueError, match="incarnation token"):
        WorkerHealth.from_dict(data)


def test_health_reader_rejects_invalid_incarnation_before_path_construction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Invalid caller tokens fail closed before selecting a health artifact."""
    monkeypatch.setattr(
        health_module,
        "health_incarnation_path",
        lambda _token: pytest.fail("invalid token reached path construction"),
    )
    assert health_module.read_worker_health_by_incarnation("../other") is None
