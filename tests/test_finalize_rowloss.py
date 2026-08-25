"""A root row vanishing during finalization is a local row-loss outcome.

Finalization must converge an exact job whose command root row was deleted
concurrently — after output publication committed and before the terminal
update — without raising out of the supervisor turn, while a row that still
exists in another terminal state keeps its normal observation semantics and
sibling jobs stay supervised.
"""

from __future__ import annotations

import dataclasses
import os
from typing import TYPE_CHECKING, Any, Self, cast
from uuid import uuid4

import psycopg
import pytest

from lubko import worker
from lubko.worker import ActiveJob, JobResult, OutputStream, Supervisor, finish_job

if TYPE_CHECKING:
    from pathlib import Path

    from lubko.worker import JobsConnection


class _FakeCursor:
    """Scripted tuple-row cursor returning queued rows per statement."""

    def __init__(self, results: list[tuple[Any, ...] | None]) -> None:
        self._results = results
        self.row_factory = tuple

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, _query: str, _params: object = None) -> None:
        del self

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._results.pop(0)


class _FakeConn:
    """Minimal connection double with scripted per-statement results."""

    def __init__(self, results: list[tuple[Any, ...] | None]) -> None:
        self._results = results
        self.transactions = 0

    def transaction(self) -> _NoopContext:
        # Instance-bound on purpose: the production connection is used as an
        # instance-bound context-manager factory.
        self.transactions += 1
        return _NoopContext()

    def cursor(self, *, row_factory: object = None) -> _FakeCursor:
        del row_factory
        return _FakeCursor(self._results)


class _NoopContext:
    def __enter__(self) -> None:
        return None

    def __exit__(self, *_args: object) -> None:
        return None


def make_active_job(tmp_path: Path, *, completed: bool) -> ActiveJob:
    """Build a structurally complete registry entry with no live child.

    Args:
        tmp_path: Directory for the job's capture spool files.
        completed: Whether the job is already observed as exited.

    Returns:
        An active-job registry entry with drained capture streams.
    """
    job = object.__new__(ActiveJob)
    for f in dataclasses.fields(ActiveJob):
        if f.default is not dataclasses.MISSING:
            setattr(job, f.name, f.default)
        elif f.default_factory is not dataclasses.MISSING:
            setattr(job, f.name, f.default_factory())
    job.id = uuid4()
    job.cwd = str(tmp_path)
    job.process = ("true",)
    job.pid = -1
    job.pgid = os.getpid()
    job.started_mono = 0.0
    job.claimed_at = 0.0
    stdout_path = tmp_path / f"out-{job.id}"
    stderr_path = tmp_path / f"err-{job.id}"
    stdout_path.write_bytes(b"out")
    stderr_path.write_bytes(b"err")
    job.stdout = OutputStream(path=stdout_path)
    job.stderr = OutputStream(path=stderr_path)
    # Capture pipes are already fully drained so the bounded finalization
    # cycle proceeds straight to publication without waiting on EOF.
    for stream in (job.stdout, job.stderr):
        stream.eof = True
    job.completed = completed
    job.returncode = 0 if completed else None
    return job


def make_supervisor(tmp_path: Path, conn: JobsConnection | None) -> Supervisor:
    """Build an unstarted supervisor over the given connection double.

    Args:
        tmp_path: Unused working directory for the settings environment.
        conn: The connection double to install (or ``None``).

    Returns:
        A supervisor with health publication stubbed out.
    """
    supervisor = object.__new__(Supervisor)
    supervisor.conn = conn
    supervisor.settings = worker.Settings.from_environment(server="srv")
    supervisor.active = {}
    supervisor._retry_terminations = {}
    supervisor._stopping = False
    supervisor._last_completed_job_id = None
    supervisor._last_completed_at = 0.0
    supervisor._last_completed_status = None
    monkey_patch_health(supervisor)
    del tmp_path
    return supervisor


def monkey_patch_health(supervisor: Supervisor) -> None:
    """Skip durable health publication side effects."""
    supervisor._publish_health_force = lambda: None  # type: ignore[method-assign]
    supervisor._publish_health = lambda **_kwargs: None  # type: ignore[method-assign]


def test_finish_job_missing_row_is_explicit_none() -> None:
    """A deleted root row yields ``None`` instead of a RuntimeError."""
    conn = cast("JobsConnection", _FakeConn([None, None]))

    status = finish_job(
        conn,
        uuid4(),
        JobResult(status="succeeded", exit_code=0, stdout="", stderr="", cancellation_note=None),
        server="srv",
    )

    assert status is None


def test_finish_job_existing_terminal_row_keeps_observed_status() -> None:
    """A lease-recovered/already-terminal row still reports its own status."""
    conn = cast("JobsConnection", _FakeConn([None, ("failed",)]))

    status = finish_job(
        conn,
        uuid4(),
        JobResult(status="succeeded", exit_code=0, stdout="", stderr="", cancellation_note=None),
        server="srv",
    )

    assert status == "failed"


def test_root_deleted_after_publication_converges_as_local_row_loss(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Deletion between publication and finish never escapes the supervisor turn.

    The vanished job converges through the local row-loss path (untracked,
    spool cleaned), no exception escapes finalization of any completed job in
    the same turn, and a sibling running job remains supervised.
    """
    conn = cast("JobsConnection", object())
    supervisor = make_supervisor(tmp_path, conn)
    lost = make_active_job(tmp_path, completed=True)
    sibling = make_active_job(tmp_path, completed=False)
    supervisor.active[lost.id] = lost
    supervisor.active[sibling.id] = sibling
    monkeypatch.setattr(worker, "publish_output", lambda *_a, **_k: True)
    monkeypatch.setattr(worker, "finish_job", lambda *_a, **_k: None)

    supervisor._finalize_completed()

    assert lost.id not in supervisor.active
    assert lost.row_lost
    assert lost.finalized
    assert not lost.stdout.path.exists()
    assert not lost.stderr.path.exists()
    # The sibling stays tracked and untouched.
    assert supervisor.active[sibling.id] is sibling
    assert not sibling.row_lost
    assert not sibling.finalized


def test_spool_evicted_finalization_shares_missing_row_handling(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The spool-evicted path routes a vanished root through row loss too."""
    supervisor = make_supervisor(tmp_path, cast("JobsConnection", object()))
    job = make_active_job(tmp_path, completed=True)
    job.spool_evicted = True
    supervisor.active[job.id] = job
    monkeypatch.setattr(worker, "finish_job", lambda *_a, **_k: None)

    supervisor.finalize_completed_job_bounded(job)

    assert job.id not in supervisor.active
    assert job.row_lost
    assert job.finalized


def test_immediate_finalization_tolerates_missing_row(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Immediate pre-spawn finalization treats a missing row as benign."""
    supervisor = make_supervisor(tmp_path, cast("JobsConnection", object()))
    job_id = uuid4()
    monkeypatch.setattr(worker, "finish_job", lambda *_a, **_k: None)

    supervisor._finalize_immediate(
        job_id,
        JobResult(status="failed", exit_code=127, stdout="", stderr="x", cancellation_note=None),
    )

    assert not supervisor._retry_terminations
    assert supervisor._stopping is False


def test_existing_failed_row_is_normal_non_exceptional_terminalization(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A lease-recovered failed row finalizes normally, without row loss."""
    supervisor = make_supervisor(tmp_path, cast("JobsConnection", object()))
    job = make_active_job(tmp_path, completed=True)
    supervisor.active[job.id] = job
    monkeypatch.setattr(worker, "publish_output", lambda *_a, **_k: True)
    monkeypatch.setattr(worker, "finish_job", lambda *_a, **_k: "failed")

    supervisor._try_finalize_one_completed(job)

    assert job.id not in supervisor.active
    assert not job.row_lost
    assert supervisor._last_completed_status == "failed"
    assert supervisor._last_completed_job_id == str(job.id)


def test_connectivity_errors_still_propagate_from_finalization(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Only exact-job loss is contained; connectivity still enters outage."""

    def raise_connectivity(*_a: object, **_k: object) -> None:
        message = "connection exception"
        raise psycopg.OperationalError(message)

    supervisor = make_supervisor(tmp_path, cast("JobsConnection", object()))
    job = make_active_job(tmp_path, completed=True)
    supervisor.active[job.id] = job
    monkeypatch.setattr(worker, "publish_output", lambda *_a, **_k: True)
    monkeypatch.setattr(worker, "finish_job", raise_connectivity)
    monkeypatch.setattr(supervisor, "_is_connectivity_error", lambda _exc: True)

    with pytest.raises(psycopg.Error):
        supervisor._try_finalize_one_completed(job)
