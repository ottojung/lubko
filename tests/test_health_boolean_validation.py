"""Strict persisted boolean validation for worker health."""

import pytest

from lubko.health import WORKER_HEALTH_SCHEMA_VERSION, WorkerHealth

BOOLEAN_FIELDS = (
    "alive",
    "db_connected",
    "cancellation_scan_overdue",
    "recovery_overdue",
    "gc_overdue",
    "gc_batch_bound_hit",
    "cancellation_batch_bound_hit",
    "recovery_batch_bound_hit",
    "shutting_down",
)


def _data() -> dict[str, object]:
    return {
        "schema_version": WORKER_HEALTH_SCHEMA_VERSION,
        "worker_id": "worker",
        "worker_incarnation": "incarnation",
        "pid": 1,
        "start_time_ticks": 1,
        "started_at": 1.0,
        "published_at": 1.0,
        "scan_batch_limit": 1,
        "gc_batch_limit": 1,
        "cancellation_batch_limit": 1,
        "recovery_batch_limit": 1,
        "alive": True,
        "db_connected": True,
        "cancellation_scan_overdue": False,
        "recovery_overdue": False,
        "gc_overdue": False,
        "gc_batch_bound_hit": False,
        "cancellation_batch_bound_hit": False,
        "recovery_batch_bound_hit": False,
        "shutting_down": False,
    }


def test_persisted_boolean_fields_require_json_booleans() -> None:
    """Present boolean fields accept only literal JSON booleans."""
    original = _data()
    for field in BOOLEAN_FIELDS:
        for value in (True, False):
            data = dict(original)
            data[field] = value
            assert getattr(WorkerHealth.from_dict(data), field) is value

        malformed = dict(original)
        malformed[field] = "false"
        with pytest.raises(TypeError, match=field):
            WorkerHealth.from_dict(malformed)


def test_persisted_boolean_truthiness_is_rejected() -> None:
    """Representative non-booleans cannot become positive health authority."""
    bad_values: tuple[object, ...] = ("true", 0, 1, None, {}, [])
    for bad in bad_values:
        data = _data()
        data["alive"] = bad
        with pytest.raises(TypeError, match="alive"):
            WorkerHealth.from_dict(data)


def test_absent_persisted_boolean_fields_default_false() -> None:
    """Genuine absence retains the backwards-compatible false default."""
    data = _data()
    for field in BOOLEAN_FIELDS:
        data.pop(field)
    snapshot = WorkerHealth.from_dict(data)
    for field in BOOLEAN_FIELDS:
        assert getattr(snapshot, field) is False
