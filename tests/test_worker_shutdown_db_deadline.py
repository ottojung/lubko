"""Deterministic worker-shutdown regressions for remote-DB finalization failure.

Issue #281 is about the queue *worker* shutdown in :mod:`lubko.worker`. The
shutdown installs a hard client DB deadline, drives local process-group
convergence (``_drain_active_groups``), and then attempts remote DB
finalization/publication (``_finalize_all_for_shutdown``). The invariant:

* Local ownership convergence and local cleanup (capture/spool removal) plus
  the final local health snapshot are **unconditional** — a
  :class:`DbOperationDeadlineError` or a connectivity loss during remote
  finalization must never prevent them.
* Remote DB terminalization is best-effort and fail-closed: on a deadline
  breach or connectivity loss the connection is discarded (so the final health
  never falsely reports ``db_connected=True``) and no further remote attempt is
  made on the unusable connection; the affected rows stay safely recoverable.
* The exact drain sentinel is written only after a *clean* local drain, so a
  failed drain never yields a false sentinel.
* Deterministic/schema/programming faults are still propagated — but only after
  the unconditional local cleanup and final health snapshot have run.
"""

from __future__ import annotations

import dataclasses
import uuid
from typing import TYPE_CHECKING, cast

import psycopg
import pytest

from lubko import worker as worker_mod
from lubko.config import DatabaseConfig
from lubko.health import read_worker_health_by_incarnation
from lubko.worker import ActiveJob, DbOperationDeadlineError, OutputStream, Supervisor

if TYPE_CHECKING:
    from pathlib import Path

INCARNATION = "a" * 32


def make_settings(
    *,
    db_operation_timeout_seconds: float = 3.0,
    cancel_grace_seconds: float = 1.0,
) -> worker_mod.Settings:
    """Build validated worker settings with an explicit DB deadline.

    Args:
        db_operation_timeout_seconds: Hard client deadline cap.
        cancel_grace_seconds: Grace before SIGKILL escalation.

    Returns:
        Validated worker settings.
    """
    return worker_mod.Settings(
        worker_id="w-test",
        poll_interval_seconds=0.0,
        process_poll_interval_seconds=0.0,
        cancel_grace_seconds=cancel_grace_seconds,
        server="srv-test",
        lease_duration_seconds=30.0,
        lease_safety_margin_seconds=5.0,
        lease_refresh_interval_seconds=5.0,
        db_operation_timeout_seconds=db_operation_timeout_seconds,
        worker_incarnation=INCARNATION,
    )


class _FakeConn:
    """A deadline-capable connection double with a trivial work seam."""

    operation_deadline: float = 0.0
    broken: bool = False
    closed: bool = False

    def close(self) -> None:
        self.closed = True


def _make_job(tmp_path: Path, job_id: uuid.UUID) -> ActiveJob:
    """Build a structurally complete registry entry without spawning a process.

    Args:
        tmp_path: Test scratch directory for the capture spool files.
        job_id: Unique job identifier.

    Returns:
        An active-job registry entry with no live child process.
    """
    job = object.__new__(ActiveJob)
    for f in dataclasses.fields(ActiveJob):
        if f.default is not dataclasses.MISSING:
            setattr(job, f.name, f.default)
        elif f.default_factory is not dataclasses.MISSING:
            setattr(job, f.name, f.default_factory())
    job.id = job_id
    job.cwd = str(tmp_path)
    job.process = ("true",)
    job.proc = object()  # type: ignore[assignment]
    job.pid = -1
    job.pgid = 9_000_000
    job.started_mono = 0.0
    job.claimed_at = 0.0
    job.version = 1
    job.stdout = OutputStream(tmp_path / f"out-{job_id}")
    job.stderr = OutputStream(tmp_path / f"err-{job_id}")
    job.completed = True
    job.term_sent = True
    return job


def _build_supervisor(_tmp_path: Path) -> Supervisor:
    """Construct a supervisor with a fake connection.

    Returns:
        A supervisor whose connection is a harmless in-memory double.
    """
    supervisor = Supervisor(
        make_settings(),
        DatabaseConfig(host="h", port=1, dbname="d", user="u", password=str(uuid.uuid4())),
    )
    supervisor.conn = cast("worker_mod.JobsConnection", _FakeConn())
    return supervisor


def _seed_jobs(supervisor: Supervisor, tmp_path: Path, *, count: int) -> list[ActiveJob]:
    """Seed ``count`` completed active jobs with on-disk capture spools.

    Args:
        supervisor: The supervisor whose registry is populated.
        tmp_path: Test scratch directory for the spool files.
        count: Number of jobs to seed.

    Returns:
        The seeded jobs (also registered in ``supervisor.active``).
    """
    jobs: list[ActiveJob] = []
    for _ in range(count):
        job = _make_job(tmp_path, uuid.uuid4())
        job.stdout.path.touch()
        job.stderr.path.touch()
        supervisor.active[job.id] = job
        jobs.append(job)
    return jobs


@pytest.fixture(autouse=True)
def _shutdown_seams(monkeypatch: pytest.MonkeyPatch) -> None:
    """Isolate the shutdown from real signals, group liveness, and DB work."""
    monkeypatch.setattr(worker_mod, "request_stop", lambda _job, _reason: None)
    monkeypatch.setattr(worker_mod, "_owned_group_alive", lambda _job: False)


def test_shutdown_finalizes_locally_when_db_deadline_breaches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A DbOperationDeadlineError during finalization must not block shutdown.

    Local convergence, capture cleanup, and the final health snapshot must
    still run; the connection is discarded so health reports ``db_connected``
    honestly; the affected row stays recoverable (the job remains tracked); and
    no second remote attempt is made on the dead connection.
    """
    supervisor = _build_supervisor(tmp_path)
    jobs = _seed_jobs(supervisor, tmp_path, count=2)
    monkeypatch.setattr(supervisor, "_drain_active_groups", lambda: True)
    calls: list[uuid.UUID] = []
    deadline_message = "hung established connection"

    def deadline_finalize(job: ActiveJob) -> None:
        calls.append(job.id)
        raise DbOperationDeadlineError(deadline_message)

    monkeypatch.setattr(supervisor, "finalize_completed_job_bounded", deadline_finalize)

    supervisor._shutdown()

    # Local convergence + cleanup completed despite the remote failure.
    assert not jobs[0].stdout.path.exists(), "local capture spool was not cleaned"
    assert not jobs[1].stdout.path.exists(), "local capture spool was not cleaned"
    # The connection was discarded: no second remote attempt on a dead handle.
    assert calls == [jobs[0].id], "remote finalization was retried on a dead connection"
    assert supervisor.conn is None, "the failed connection was not discarded"
    # The affected row stays recoverable (the job is still tracked).
    assert jobs[0].id in supervisor.active
    # The clean drain proved the sentinel; it must be present (no false absence).
    assert worker_mod.drain_sentinel_path(INCARNATION).exists(), "clean-drain sentinel missing"
    health = read_worker_health_by_incarnation(INCARNATION)
    assert health is not None, "final local health snapshot was not published"
    assert health.db_connected is False, "health falsely reported db_connected=True"


def test_shutdown_finalizes_locally_when_db_connectivity_lost(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A connectivity loss during finalization is best-effort and fail-closed.

    Both owned groups still converge locally, the connection is discarded, the
    final health is honest, and every not-yet-finalized row stays recoverable.
    """
    supervisor = _build_supervisor(tmp_path)
    jobs = _seed_jobs(supervisor, tmp_path, count=2)
    monkeypatch.setattr(supervisor, "_drain_active_groups", lambda: True)
    loss_message = "connection reset"

    def connectivity_finalize(_job: ActiveJob) -> None:
        raise psycopg.OperationalError(loss_message)

    monkeypatch.setattr(supervisor, "finalize_completed_job_bounded", connectivity_finalize)
    monkeypatch.setattr(supervisor, "_is_connectivity_error", lambda _exc: True)

    supervisor._shutdown()

    assert supervisor.conn is None, "the failed connection was not discarded"
    assert jobs[0].id in supervisor.active, "first job row was not retained as recoverable"
    assert jobs[1].id in supervisor.active, "second job row was not retained as recoverable"
    assert not jobs[0].stdout.path.exists(), "local capture spool was not cleaned"
    assert worker_mod.drain_sentinel_path(INCARNATION).exists(), "clean-drain sentinel missing"
    health = read_worker_health_by_incarnation(INCARNATION)
    assert health is not None
    assert health.db_connected is False


def test_shutdown_withholds_false_sentinel_when_drain_unproven(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failed local drain never produces a false drain sentinel.

    When the exact groups cannot be proven gone, the sentinel is withheld and
    the jobs are retained for exact-identity recovery even if remote
    finalization then fails.
    """
    supervisor = _build_supervisor(tmp_path)
    jobs = _seed_jobs(supervisor, tmp_path, count=1)
    monkeypatch.setattr(supervisor, "_drain_active_groups", lambda: False)
    deadline_message = "hung established connection"

    def deadline_finalize(_job: ActiveJob) -> None:
        raise DbOperationDeadlineError(deadline_message)

    monkeypatch.setattr(supervisor, "finalize_completed_job_bounded", deadline_finalize)

    supervisor._shutdown()

    assert not worker_mod.drain_sentinel_path(INCARNATION).exists(), (
        "a false drain sentinel was written for an unproven drain"
    )
    assert jobs[0].id in supervisor.active, "unproven-drain job was not retained for recovery"
    assert supervisor.conn is None
    health = read_worker_health_by_incarnation(INCARNATION)
    assert health is not None
    assert health.db_connected is False


def test_shutdown_succeeds_and_publishes_final_health(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A clean shutdown finalizes remote rows and still publishes local health.

    The clean drain proves the sentinel, every job is finalized and locally
    cleaned, and the final health snapshot is written with an honest
    ``db_connected`` state.
    """
    supervisor = _build_supervisor(tmp_path)
    jobs = _seed_jobs(supervisor, tmp_path, count=1)
    monkeypatch.setattr(supervisor, "_drain_active_groups", lambda: True)

    def succeed(job: ActiveJob) -> None:
        worker_mod.cleanup_job(job)
        job.finalized = True
        supervisor.active.pop(job.id, None)

    monkeypatch.setattr(supervisor, "finalize_completed_job_bounded", succeed)

    supervisor._shutdown()

    assert jobs[0].id not in supervisor.active, "successful job was not finalized"
    assert not jobs[0].stdout.path.exists(), "local capture spool was not cleaned"
    assert worker_mod.drain_sentinel_path(INCARNATION).exists(), "clean-drain sentinel missing"
    assert supervisor.conn is None
    health = read_worker_health_by_incarnation(INCARNATION)
    assert health is not None
    assert health.db_connected is False
