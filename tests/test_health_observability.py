"""Deterministic checks for bounded deadline and GC saturation signals."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast
from unittest.mock import patch

from lubko import worker
from lubko.health import WORKER_HEALTH_SCHEMA_VERSION, WorkerHealth
from lubko.worker import Supervisor

if TYPE_CHECKING:
    from lubko.worker import JobsConnection


def _bare_supervisor() -> Supervisor:
    """Build a Supervisor without ``__init__`` like the row-loss tests do.

    Returns:
        A minimally wired supervisor suitable for method-level tests.
    """
    sup = object.__new__(Supervisor)
    sup.conn = None
    sup.settings = worker.Settings.from_environment(server="srv")
    sup.active = {}
    sup._start_time_ticks = 0
    sup._started_at = 0.0
    sup._db_connected_at = None
    sup._db_error_at = None
    return sup


def test_gc_phase_bound_hit_true_when_one_phase_saturates() -> None:
    """A single phase reaching the limit of 4 saturates the bound (true hit)."""
    assert worker._gc_phase_bound_hit(marked=4, gc_roots=0, chunk_counts=[], orphans=0, limit=4)
    assert worker._gc_phase_bound_hit(marked=0, gc_roots=4, chunk_counts=[], orphans=0, limit=4)
    assert worker._gc_phase_bound_hit(marked=0, gc_roots=0, chunk_counts=[4], orphans=0, limit=4)
    assert worker._gc_phase_bound_hit(marked=0, gc_roots=0, chunk_counts=[], orphans=4, limit=4)


def test_gc_phase_bound_hit_false_when_sum_exceeds_but_no_phase_saturates() -> None:
    """Summed rows can exceed the limit without a true per-phase saturation.

    This is the false-positive case the summed-count heuristic would misreport:
    two roots each drain one chunk, plus one marked root and one orphan — the
    total is 5, above limit 4, yet no single phase reached its bound.
    """
    assert (
        worker._gc_phase_bound_hit(marked=1, gc_roots=2, chunk_counts=[1, 1], orphans=1, limit=4)
        is False
    )


def test_gc_phase_bound_hit_false_when_under_limit() -> None:
    """Below-limit per-phase counts are not a saturation."""
    assert (
        worker._gc_phase_bound_hit(marked=2, gc_roots=2, chunk_counts=[2, 2], orphans=2, limit=4)
        is False
    )


def test_gc_phase_bound_hit_zero_limit_is_false() -> None:
    """A non-positive limit never saturates."""
    hit = worker._gc_phase_bound_hit(marked=0, gc_roots=0, chunk_counts=[], orphans=0, limit=0)
    assert hit is False


def test_db_deadline_breach_recorded_at_failure_path() -> None:
    """The explicit breach signal records time and count when invoked."""
    sup = _bare_supervisor()
    sup._record_db_deadline_breach()
    sup._record_db_deadline_breach()
    assert sup._db_deadline_breach_count == 2
    assert isinstance(sup._db_deadline_breached_at, float)
    health = sup._build_health()
    assert health.db_deadline_breach_count == 2
    assert health.db_deadline_breached_at == sup._db_deadline_breached_at


def test_gc_bound_hit_propagates_from_collect_transport() -> None:
    """_run_gc wires the saturation flag from collect_transport into health."""
    sup = _bare_supervisor()
    sup.conn = cast("JobsConnection", object())  # non-None so _run_gc proceeds

    with patch.object(worker, "collect_transport", return_value=([], 0, 0, True)):
        sup._run_gc()
    assert sup._gc_batch_bound_hit is True
    assert isinstance(sup._last_gc_at, float)

    with patch.object(worker, "collect_transport", return_value=([], 0, 0, False)):
        sup._run_gc()
    assert sup._gc_batch_bound_hit is False


def test_gc_bound_hit_reflected_in_health() -> None:
    """The saturation flag flows from the worker into the built health snapshot."""
    sup = _bare_supervisor()
    sup._gc_batch_bound_hit = True
    assert sup._build_health().gc_batch_bound_hit is True
    sup._gc_batch_bound_hit = False
    assert sup._build_health().gc_batch_bound_hit is False


def test_scan_recency_fields_wired_from_periodic_passes() -> None:
    """Cancellation/recovery/GC recency timestamps are set by their passes."""
    sup = _bare_supervisor()
    sup.conn = cast("JobsConnection", object())

    with (
        patch.object(worker, "discover_cancellations", return_value=[]),
        patch.object(worker, "recover_stale_jobs", return_value=[]),
        patch.object(worker, "collect_transport", return_value=([], 0, 0, False)),
    ):
        sup._discover_cancellations()
        sup._run_recovery()
        sup._run_gc()
    health = sup._build_health()
    assert isinstance(health.last_cancellation_scan_at, float)
    assert isinstance(health.last_recovery_at, float)
    assert isinstance(health.last_gc_at, float)


def test_schema_has_no_job_identity_and_includes_new_signals() -> None:
    """The v2 schema is bounded and free of per-job identifiers."""
    fields = set(WorkerHealth.__dataclass_fields__)
    assert "oldest_active_job_id" not in fields
    assert "current_job_id" not in fields
    for expected in (
        "db_deadline_breached_at",
        "db_deadline_breach_count",
        "gc_batch_bound_hit",
        "last_cancellation_scan_at",
        "last_recovery_at",
        "last_gc_at",
    ):
        assert expected in fields
    assert WORKER_HEALTH_SCHEMA_VERSION == 2
