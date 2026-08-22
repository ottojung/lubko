"""Tests for machine-readable worker health snapshots and bounded logging.

Minimal tests exercising the changed public API: per-incarnation storage,
stable symlink publication with all-or-nothing rollback, liveness
interpretation, bounded rotating logs, and incarnation token validation.
"""

from __future__ import annotations

import time
from logging.handlers import RotatingFileHandler
from pathlib import Path

import pytest

from lubko.health import (
    WorkerHealth,
    configure_worker_logging,
    health_current_path,
    health_incarnation_path,
    interpret_worker_health,
    publish_current_surfaces,
    read_worker_health,
    read_worker_health_by_incarnation,
    validate_incarnation_token,
    worker_log_current_path,
    worker_log_incarnation_path,
    write_worker_health,
)

_NOW = time.time()


def _health(
    *,
    incarnation: str = "abc123",
    pid: int = 1000,
    start_time_ticks: int = 42,
    published_at: float | None = None,
) -> WorkerHealth:
    """Build a minimal valid health snapshot.

    Returns:
        A ``WorkerHealth`` snapshot with the given parameters.
    """
    return WorkerHealth(
        schema_version=1,
        worker_id="test-worker",
        worker_incarnation=incarnation,
        pid=pid,
        start_time_ticks=start_time_ticks,
        started_at=_NOW,
        published_at=published_at if published_at is not None else _NOW,
        alive=True,
        db_connected=True,
        db_connected_at=None,
        db_error_at=None,
        current_job_id=None,
        current_job_started_at=None,
        last_completed_job_id=None,
        last_completed_at=None,
        last_completed_status=None,
        shutting_down=False,
    )


# -- Per-incarnation storage ------------------------------------------------


def test_write_and_read_by_incarnation() -> None:
    """Worker writes to its own incarnation file and can read it back."""
    write_worker_health(_health(incarnation="inc-001"))
    loaded = read_worker_health_by_incarnation("inc-001")
    assert loaded is not None
    assert loaded.worker_incarnation == "inc-001"
    assert loaded.pid == 1000


def test_worker_does_not_update_stable_symlink() -> None:
    """write_worker_health must never create the stable health.json symlink."""
    write_worker_health(_health(incarnation="inc-002"))
    assert not health_current_path().exists()


def test_two_incarnations_do_not_interfere() -> None:
    """Two overlapping incarnations write independent files."""
    write_worker_health(_health(incarnation="old", pid=100, start_time_ticks=10))
    write_worker_health(_health(incarnation="new", pid=200, start_time_ticks=20))
    assert read_worker_health_by_incarnation("old").pid == 100  # type: ignore[union-attr]
    assert read_worker_health_by_incarnation("new").pid == 200  # type: ignore[union-attr]


# -- Stable symlink publication ---------------------------------------------


def test_publish_creates_both_symlinks() -> None:
    """publish_current_surfaces creates health and log symlinks."""
    write_worker_health(_health(incarnation="inc-10"))
    publish_current_surfaces("inc-10")
    assert health_current_path().readlink() == Path("health/health-inc-10.json")
    assert worker_log_current_path().readlink() == Path("logs/worker-inc-10.log")


def test_read_via_stable_symlink_after_publish() -> None:
    """read_worker_health resolves the stable symlink."""
    write_worker_health(_health(incarnation="inc-30"))
    publish_current_surfaces("inc-30")
    loaded = read_worker_health()
    assert loaded is not None
    assert loaded.worker_incarnation == "inc-30"


def test_read_without_publish_returns_none() -> None:
    """Without supervisor publish, read_worker_health returns None."""
    write_worker_health(_health(incarnation="inc-40"))
    assert read_worker_health() is None


def test_publish_repoints_symlink() -> None:
    """Supervisor can repoint symlinks to a new incarnation."""
    write_worker_health(_health(incarnation="old"))
    write_worker_health(_health(incarnation="new"))
    publish_current_surfaces("old")
    assert read_worker_health().worker_incarnation == "old"  # type: ignore[union-attr]
    publish_current_surfaces("new")
    assert read_worker_health().worker_incarnation == "new"  # type: ignore[union-attr]


def test_old_incarnation_file_intact_after_repoint() -> None:
    """Old incarnation health file survives repoint."""
    write_worker_health(_health(incarnation="retiring", pid=111))
    publish_current_surfaces("retiring")
    write_worker_health(_health(incarnation="candidate", pid=600))
    publish_current_surfaces("candidate")
    old = read_worker_health_by_incarnation("retiring")
    assert old is not None
    assert old.pid == 111


def test_all_or_nothing_rollback() -> None:
    """If log symlink fails, health symlink is rolled back."""
    write_worker_health(_health(incarnation="inc-rb"))
    publish_current_surfaces("inc-rb")
    old_target = str(health_current_path().readlink())
    log_dir = worker_log_current_path().parent
    log_dir.mkdir(parents=True, exist_ok=True)
    log_dir.chmod(0o555)
    try:
        publish_current_surfaces("inc-other")
    except OSError:
        pass
    finally:
        log_dir.chmod(0o755)
    assert str(health_current_path().readlink()) == old_target


# -- Identity cross-check ---------------------------------------------------


def test_incarnation_mismatch_rejected() -> None:
    """Snapshot worker_incarnation not matching child token is rejected."""
    snap = _health(incarnation="wrong-token", pid=777, start_time_ticks=55)
    assert snap.worker_incarnation != "correct-token"


# -- Liveness interpretation ------------------------------------------------


def test_interpret_rejects_none() -> None:
    """None snapshot is never live."""
    eff = interpret_worker_health(None)
    assert eff.live is False


def test_interpret_rejects_dead_pid() -> None:
    """Snapshot with a dead PID is never live."""
    eff = interpret_worker_health(_health(pid=99_999_999, start_time_ticks=1))
    assert eff.live is False


def test_interpret_rejects_stale() -> None:
    """Snapshot older than max_staleness_seconds is stale."""
    eff = interpret_worker_health(
        _health(published_at=time.time() - 100), max_staleness_seconds=10.0
    )
    assert eff.stale is True


def test_interpret_fresh_despite_old_started_at() -> None:
    """Long-lived worker with recent published_at is not stale."""
    eff = interpret_worker_health(
        _health(published_at=time.time() - 1),
        max_staleness_seconds=10.0,
    )
    assert eff.stale is False


# -- Corruption handling ----------------------------------------------------


def test_corrupt_json_returns_none() -> None:
    """Corrupt JSON returns None."""
    path = health_incarnation_path("bad")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("not json", encoding="utf-8")
    assert read_worker_health_by_incarnation("bad") is None


def test_dangling_symlink_returns_none() -> None:
    """Dangling symlink returns None."""
    symlink = health_current_path()
    symlink.parent.mkdir(parents=True, exist_ok=True)
    symlink.symlink_to("nonexistent.json")
    assert read_worker_health() is None


# -- Bounded logging --------------------------------------------------------


def test_logging_scoped_to_lubko_worker() -> None:
    """Handler is on lubko.worker logger, not root (no secret leakage)."""
    logger = configure_worker_logging("log-inc-1")
    assert logger.name == "lubko.worker"
    handlers = [h for h in logger.handlers if isinstance(h, RotatingFileHandler)]
    assert len(handlers) >= 1
    h = handlers[-1]
    assert h.backupCount >= 1
    assert h.maxBytes == 2 * 1024 * 1024


def test_log_writes_to_per_incarnation_file() -> None:
    """Log messages appear in the per-incarnation file."""
    logger = configure_worker_logging("log-inc-w")
    logger.info("hello from incarnation")
    path = worker_log_incarnation_path("log-inc-w")
    assert "hello from incarnation" in path.read_text(encoding="utf-8")


# -- Token validation -------------------------------------------------------


def test_validate_incarnation_token() -> None:
    """Tokens with unsafe characters are rejected."""
    validate_incarnation_token("abc123def")
    with pytest.raises(ValueError, match="empty"):
        validate_incarnation_token("")
    with pytest.raises(ValueError, match="unsafe"):
        validate_incarnation_token("inc/../../etc")
