"""Observation-only diagnostics stay harmless when persistent storage is exhausted."""

from __future__ import annotations

import errno
import logging
import os
import tempfile
import time
from contextlib import suppress
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

import pytest

from lubko import deployctl as dc
from lubko import health as health_module
from lubko import lifecycle, supervise, supervisor, worker
from lubko.config import DatabaseConfig
from lubko.health import (
    WORKER_HEALTH_SCHEMA_VERSION,
    WorkerHealth,
    configure_worker_logging,
    health_write_drops,
    read_worker_health_by_incarnation,
    worker_log_drops,
    write_worker_health,
)


def _snapshot(**overrides: object) -> WorkerHealth:
    """Build a minimal valid worker health snapshot.

    Args:
        overrides: Field values replacing the defaults.

    Returns:
        A valid snapshot for the ``test-incarnation`` incarnation.
    """
    fields: dict[str, object] = {
        "schema_version": WORKER_HEALTH_SCHEMA_VERSION,
        "worker_id": "w",
        "worker_incarnation": "test-incarnation",
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


def _no_space(*_args: object, **_kwargs: object) -> None:
    """Simulate exhausted persistent storage deterministically.

    Raises:
        OSError: Always, with ``ENOSPC``.
    """
    raise OSError(errno.ENOSPC, "No space left on device")


def test_health_snapshot_capacity_failure_drops_silently(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exhausted capacity drops a health snapshot without raising."""
    before = health_write_drops()
    monkeypatch.setattr(tempfile, "mkstemp", _no_space)
    assert write_worker_health(_snapshot()) is False
    assert health_write_drops() == before + 1


def test_health_snapshot_non_capacity_failure_still_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Only capacity failures are dropped; other errors still propagate."""

    def _denied(*_args: object, **_kwargs: object) -> None:
        raise OSError(errno.EACCES, "Permission denied")

    monkeypatch.setattr(tempfile, "mkstemp", _denied)
    with pytest.raises(OSError, match="Permission denied"):
        write_worker_health(_snapshot())


def test_health_snapshot_recovers_after_capacity_returns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A dropped snapshot does not poison later publication or readers."""
    monkeypatch.setattr(tempfile, "mkstemp", _no_space)
    assert write_worker_health(_snapshot()) is False
    monkeypatch.undo()
    assert write_worker_health(_snapshot(published_at=time.time())) is True
    snapshot = read_worker_health_by_incarnation("test-incarnation")
    assert snapshot is not None
    assert snapshot.worker_incarnation == "test-incarnation"


def test_worker_logging_falls_back_without_failing_startup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Worker logging degrades to a NullHandler when no capacity remains."""
    logger = logging.getLogger("lubko.worker")
    before = list(logger.handlers)
    monkeypatch.setattr(Path, "mkdir", _no_space)
    logger = configure_worker_logging("test-incarnation")
    try:
        assert any(isinstance(handler, logging.NullHandler) for handler in logger.handlers)
    finally:
        for handler in list(logger.handlers):
            if handler not in before:
                logger.removeHandler(handler)


def test_worker_log_emission_drops_capacity_failures_silently(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """One failed log emission is counted once and never raises."""
    logger = logging.getLogger("lubko.worker.capacity-probe")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    handler = health_module._QuietWorkerLogHandler(
        str(tmp_path / "probe.log"), maxBytes=1024, backupCount=1, encoding="utf-8"
    )
    logger.addHandler(handler)
    try:

        class _FullDisk:
            @staticmethod
            def write(_data: object) -> None:
                raise OSError(errno.ENOSPC, "No space left on device")

            @staticmethod
            def flush() -> None:
                raise OSError(errno.ENOSPC, "No space left on device")

            @staticmethod
            def seek(*_args: object) -> None:
                raise OSError(errno.ENOSPC, "No space left on device")

            @staticmethod
            def tell() -> int:
                raise OSError(errno.ENOSPC, "No space left on device")

        monkeypatch.setattr(handler, "stream", _FullDisk())
        before = worker_log_drops()
        logger.info("probe message")
        assert worker_log_drops() == before + 1
    finally:
        logger.removeHandler(handler)
        with suppress(OSError):
            handler.close()


def _live_identity(monkeypatch: pytest.MonkeyPatch, ticks: int) -> None:
    """Fake a live supervisor process identity for status reads.

    Args:
        monkeypatch: The pytest patcher.
        ticks: The fake supervisor start-time ticks.
    """
    monkeypatch.setattr(supervise, "_process_is_zombie", lambda _pid: False)
    monkeypatch.setattr(supervise, "proc_start_ticks", lambda _pid: ticks)
    monkeypatch.setattr(supervise, "_read_cmdline", lambda _pid: "lubko-supervisor --serve")

    def _fake_open_pidfd(_pid: int) -> int:
        fd, _ = os.pipe()
        return fd

    monkeypatch.setattr(supervise, "_open_supervisor_pidfd", _fake_open_pidfd)
    monkeypatch.setattr(supervise, "_pidfd_send_signal", lambda _pidfd, _sig: None)


@pytest.mark.usefixtures("supervisor_token")
def test_expired_status_reads_as_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    """Readers honestly report an expired snapshot as unavailable, not stale data."""
    pid = 4242
    ticks = 111
    _live_identity(monkeypatch, ticks)
    supervise.write_supervisor_pid(pid, ticks)
    fresh = supervise.SupervisorStatus(
        schema_version=supervise.SCHEMA_VERSION,
        supervisor_pid=pid,
        supervisor_start_time_ticks=ticks,
        started_at=1.0,
        applied_generation=7,
        mode=supervise.MODE_RUN,
        commit=None,
        child=None,
        intent=supervise.INTENT_RUN,
        restart_count=0,
        next_attempt_at=None,
        last_exit=None,
        mission=None,
        db_ready=None,
        ready=None,
        message=None,
        worker_health=None,
        published_at=time.time(),
    )
    supervise.write_status(fresh)
    assert supervise.read_status() is not None

    expired = replace(fresh, published_at=time.time() - 3600.0)
    supervise.write_status(expired)
    assert supervise.read_status() is None

    future = replace(fresh, published_at=time.time() + 3600.0)
    supervise.write_status(future)
    assert supervise.read_status() is None

    untimestamped = replace(fresh, published_at=0.0)
    supervise.write_status(untimestamped)
    assert supervise.read_status() is not None


@pytest.mark.usefixtures("supervisor_token")
def test_readiness_survives_stable_surface_outage(monkeypatch: pytest.MonkeyPatch) -> None:
    """A failed symlink publication never blocks proven queue readiness."""
    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    token = uuid4().hex
    child = supervise.WorkerChild(
        pid=4242,
        pgid=4242,
        sid=4242,
        start_time_ticks=111,
        token=token,
        worker_id="w",
        spawned_at=1.0,
    )
    state = supervise.SupervisorState.from_dict({
        "schema_version": supervise.SCHEMA_VERSION,
        "applied_generation": 1,
        "mode": supervise.MODE_RUN,
        "commit": "ab" * 20,
        "intent": supervise.INTENT_RUN,
    })
    state = replace(state, child=child)
    monkeypatch.setattr(supervisor, "read_state", lambda: state)
    monkeypatch.setattr(supervisor.SupervisorDaemon, "_child_alive", lambda _self, _s: True)
    monkeypatch.setattr(
        supervisor.SupervisorDaemon, "_check_readiness", lambda _self, _c, _d: (True, "ok")
    )
    monkeypatch.setattr(supervisor, "publish_current_surfaces", _no_space)
    monkeypatch.setattr(lifecycle, "append_deploy_log", lambda _msg: None)
    monkeypatch.setattr(supervisor, "prune_old_incarnation_artifacts", lambda _token: None)
    published: list[object] = []

    def _record_allowed(next_state: object) -> bool:
        published.append(next_state)
        return True

    monkeypatch.setattr(
        supervisor.SupervisorDaemon,
        "_write_state_authority_safe",
        lambda _self, s: _record_allowed(s),
    )
    daemon._probe_readiness(0.0)
    assert len(published) == 1
    assert daemon._surface_write_drops == 1


@pytest.mark.usefixtures("supervisor_token")
def test_control_status_serves_live_drops_during_file_outage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The control socket exposes live drops even while file writes fail."""
    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    daemon._next_db_check_at = float("inf")
    state = supervise.SupervisorState.from_dict({})
    monkeypatch.setattr(supervisor, "read_state", lambda: state)
    monkeypatch.setattr(supervisor, "read_worker_health", lambda: None)
    monkeypatch.setattr(dc, "read_rollback_state", lambda: None)
    monkeypatch.setattr(supervisor, "write_status", _no_space)
    daemon._write_status()
    assert daemon._status_write_drops == 1
    response = daemon._control_status_response()
    assert response["ok"] is True
    status = response["status"]
    assert isinstance(status, dict)
    assert status["diagnostic_drops"] == 1
    assert status["published_at"] > 0


def test_prune_failures_stay_quiet_and_harmless(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Deferred pruning never raises and never amplifies into error logs."""
    health_dir = health_module._health_dir()
    health_dir.mkdir(parents=True, exist_ok=True)
    stale = health_dir / "health-old.json"
    stale.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(Path, "unlink", _no_space)
    with caplog.at_level(logging.WARNING, logger="lubko.health"):
        health_module.prune_old_incarnation_artifacts("current")
    assert stale.exists()
    assert [record for record in caplog.records if record.levelno >= logging.WARNING] == []


def test_worker_health_publish_stays_throttled_across_drops(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Dropped health writes still advance the throttle: bounded retries only."""
    settings = worker.Settings(
        worker_id="w-test",
        poll_interval_seconds=0.0,
        process_poll_interval_seconds=0.0,
        cancel_grace_seconds=1.0,
        server="srv-test",
    )
    database = DatabaseConfig(host="h", port=1, dbname="d", user="u", password=str(uuid4()))
    daemon = worker.Supervisor(settings, database)
    calls: list[bool] = []

    def _drop_snapshot(_health: object) -> bool:
        calls.append(True)
        return False

    monkeypatch.setattr(worker, "write_worker_health", _drop_snapshot)
    daemon._publish_health(force=True)
    first_deadline = daemon._next_health_publish_at
    assert len(calls) == 1
    daemon._publish_health(force=False)
    assert len(calls) == 1
    assert daemon._next_health_publish_at == first_deadline
    assert not daemon._stopping
