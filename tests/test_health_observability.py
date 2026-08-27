"""Deterministic checks for bounded deadline and GC saturation signals."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING, cast
from unittest.mock import patch
from uuid import uuid4

import pytest

from lubko import worker
from lubko.health import WORKER_HEALTH_SCHEMA_VERSION, WorkerHealth
from lubko.protocol import PROTOCOL_VERSION
from lubko.worker import CANCEL_DISCOVERY_LIMIT, LEASE_RECOVERY_LIMIT, Supervisor

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


def _fake_active(last_heartbeat_at: float, *, claimed_at: float = 0.0) -> worker.ActiveJob:
    """Build a minimal ActiveJob double for aggregate computation.

    Returns:
        An ActiveJob whose lease/stream fields are wired for the aggregator.
    """
    job: worker.ActiveJob = object.__new__(worker.ActiveJob)
    job.term_sent = False
    job.kill_sent = False
    job.stop_started = None
    job.claimed_at = claimed_at
    job.last_heartbeat_at = last_heartbeat_at
    job.stdout = worker.OutputStream(path=Path("/dev/null"))
    job.stderr = worker.OutputStream(path=Path("/dev/null"))
    job.version = PROTOCOL_VERSION
    return job


def _set_lease_timing(sup: Supervisor, duration: float, margin: float) -> None:
    """Override the lease timing on a fresh Settings for a deterministic check.

    Args:
        sup: The supervisor whose settings are replaced.
        duration: Lease duration in seconds.
        margin: Lease-safety margin in seconds.
    """
    sup.settings = replace(
        sup.settings,
        lease_duration_seconds=duration,
        lease_safety_margin_seconds=margin,
    )


def test_lease_safety_remaining_subtracts_margin_and_passes_negative() -> None:
    """min_lease_safety_remaining subtracts the margin; negative = passed."""
    sup = _bare_supervisor()
    _set_lease_timing(sup, 60.0, 10.0)
    sup.active = {uuid4(): _fake_active(900.0)}  # 900 + 60 - 10 - 1000 = -50
    agg = sup._collect_health_aggregates(now_mono=1000.0)
    assert agg.min_lease_safety_remaining_seconds == pytest.approx(-50.0)


def test_lease_safety_remaining_positive_when_margin_not_exceeded() -> None:
    """Positive value means the safety deadline (expiry minus margin) is ahead."""
    sup = _bare_supervisor()
    _set_lease_timing(sup, 60.0, 10.0)
    sup.active = {uuid4(): _fake_active(1000.0)}  # 1000 + 60 - 10 - 1000 = 50
    agg = sup._collect_health_aggregates(now_mono=1000.0)
    assert agg.min_lease_safety_remaining_seconds == pytest.approx(50.0)


def test_lease_safety_remaining_zero_at_exact_margin_boundary() -> None:
    """At exactly the safety deadline the remaining budget is zero."""
    sup = _bare_supervisor()
    _set_lease_timing(sup, 60.0, 10.0)
    sup.active = {uuid4(): _fake_active(950.0)}  # 950 + 60 - 10 - 1000 = 0
    agg = sup._collect_health_aggregates(now_mono=1000.0)
    assert agg.min_lease_safety_remaining_seconds == pytest.approx(0.0)


def test_cancellation_batch_bound_hit_below_and_at_limit() -> None:
    """Cancellation saturation is set from the actual returned count."""
    sup = _bare_supervisor()
    sup.conn = cast("JobsConnection", object())
    with patch.object(worker, "discover_cancellations", return_value=list(range(3))):
        sup._discover_cancellations()
    assert sup._cancellation_batch_bound_hit is False

    with patch.object(
        worker,
        "discover_cancellations",
        return_value=list(range(CANCEL_DISCOVERY_LIMIT)),
    ):
        sup._discover_cancellations()
    assert sup._cancellation_batch_bound_hit is True


def test_cancellation_batch_bound_hit_reflected_in_health() -> None:
    """The cancellation saturation flag flows into the built health snapshot."""
    sup = _bare_supervisor()
    sup._cancellation_batch_bound_hit = True
    assert sup._build_health().cancellation_batch_bound_hit is True
    sup._cancellation_batch_bound_hit = False
    assert sup._build_health().cancellation_batch_bound_hit is False
    assert sup._build_health().cancellation_batch_limit == CANCEL_DISCOVERY_LIMIT


def test_recovery_batch_bound_hit_below_and_at_limit() -> None:
    """Recovery saturation is set from the actual returned count."""
    sup = _bare_supervisor()
    sup.conn = cast("JobsConnection", object())
    below = [(uuid4(), "succeeded") for _ in range(3)]
    at_limit = [(uuid4(), "succeeded") for _ in range(LEASE_RECOVERY_LIMIT)]
    with patch.object(worker, "recover_stale_jobs", return_value=below):
        sup._run_recovery()
    assert sup._recovery_batch_bound_hit is False

    with patch.object(worker, "recover_stale_jobs", return_value=at_limit):
        sup._run_recovery()
    assert sup._recovery_batch_bound_hit is True


def test_recovery_batch_bound_hit_reflected_in_health() -> None:
    """The recovery saturation flag flows into the built health snapshot."""
    sup = _bare_supervisor()
    sup._recovery_batch_bound_hit = True
    assert sup._build_health().recovery_batch_bound_hit is True
    sup._recovery_batch_bound_hit = False
    assert sup._build_health().recovery_batch_bound_hit is False
    assert sup._build_health().recovery_batch_limit == LEASE_RECOVERY_LIMIT


def test_schema_has_no_job_identity_and_includes_new_signals() -> None:
    """The v2 schema is bounded and free of per-job identifiers."""
    fields = set(WorkerHealth.__dataclass_fields__)
    assert "oldest_active_job_id" not in fields
    assert "current_job_id" not in fields
    for expected in (
        "db_deadline_breached_at",
        "db_deadline_breach_count",
        "min_lease_safety_remaining_seconds",
        "gc_batch_bound_hit",
        "cancellation_batch_bound_hit",
        "recovery_batch_bound_hit",
        "last_cancellation_scan_at",
        "last_recovery_at",
        "last_gc_at",
    ):
        assert expected in fields
    assert WORKER_HEALTH_SCHEMA_VERSION == 2
