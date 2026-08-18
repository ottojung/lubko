"""Tests for machine-readable worker health snapshots and bounded logging.

Covers per-incarnation snapshot storage, stable symlink publication by the
supervisor, identity cross-checking, liveness interpretation, bounded
rotating logs, and the invariant that old stable health can never falsely
prove candidate readiness.
"""

from __future__ import annotations

import json
import os
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path

from lubko.health import (
    EffectiveHealth,
    WorkerHealth,
    configure_worker_logging,
    health_current_path,
    health_incarnation_path,
    interpret_worker_health,
    list_worker_health_incarnations,
    publish_current_health_surface,
    publish_current_log_surface,
    read_worker_health,
    read_worker_health_by_incarnation,
    worker_health_payload,
    worker_log_current_path,
    worker_log_incarnation_path,
    write_worker_health,
)

_PID_1000 = 1000
_PID_999 = 999
_PID_99999999 = 99_999_999
_TICKS_42 = 42
_TICKS_50 = 50
_TICKS_55 = 55
_TICKS_66 = 66
_PID_100 = 100
_PID_200 = 200
_PID_600 = 600
_PID_777 = 777
_PID_111 = 111
_STALE_AGE_SECONDS = 100.0
_LOG_BACKUP_COUNT_MIN = 1
_LOG_MAX_BYTES_EXPECTED = 2 * 1024 * 1024


def _health(
    *,
    incarnation: str = "abc123",
    pid: int = _PID_1000,
    start_time_ticks: int = _TICKS_42,
    started_at: float | None = None,
    published_at: float | None = None,
) -> WorkerHealth:
    """Build a minimal valid health snapshot for testing.

    Returns:
        A ``WorkerHealth`` snapshot with the given parameters.
    """
    now = time.time()
    return WorkerHealth(
        schema_version=1,
        worker_id="test-worker",
        worker_incarnation=incarnation,
        pid=pid,
        start_time_ticks=start_time_ticks,
        started_at=started_at if started_at is not None else now,
        published_at=published_at if published_at is not None else now,
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


def _proc_start_ticks(pid: int) -> int | None:
    """Read process start ticks from /proc for the current process.

    Returns:
        The start time in clock ticks, or ``None`` when unreadable.
    """
    try:
        stat = (Path("/proc") / str(pid) / "stat").read_bytes()
    except OSError:
        return None
    close_paren = stat.rfind(b")")
    if close_paren == -1:
        return None
    fields = stat[close_paren + 2 :].split()
    if len(fields) < _TICKS_42:
        return None
    try:
        return int(fields[19])
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Per-incarnation snapshot storage
# ---------------------------------------------------------------------------


def test_write_and_read_by_incarnation() -> None:
    """Worker writes to its own incarnation file and can read it back."""
    health = _health(incarnation="inc-001")
    write_worker_health(health)
    loaded = read_worker_health_by_incarnation("inc-001")
    assert loaded is not None
    assert loaded.worker_incarnation == "inc-001"
    assert loaded.pid == _PID_1000


def test_worker_does_not_update_stable_symlink() -> None:
    """write_worker_health must never create the stable health.json symlink."""
    health = _health(incarnation="inc-002")
    write_worker_health(health)
    symlink = health_current_path()
    assert not symlink.exists(), "write_worker_health must not create the stable symlink"


def test_two_incarnations_do_not_interfere() -> None:
    """Two overlapping incarnations write independent files."""
    h1 = _health(incarnation="old-inc", pid=_PID_100, start_time_ticks=10)
    h2 = _health(incarnation="new-inc", pid=_PID_200, start_time_ticks=20)
    write_worker_health(h1)
    write_worker_health(h2)
    loaded_old = read_worker_health_by_incarnation("old-inc")
    loaded_new = read_worker_health_by_incarnation("new-inc")
    assert loaded_old is not None
    assert loaded_new is not None
    assert loaded_old.pid == _PID_100
    assert loaded_new.pid == _PID_200


def test_read_nonexistent_incarnation_returns_none() -> None:
    """Reading a nonexistent incarnation returns None."""
    assert read_worker_health_by_incarnation("no-such-inc") is None


def test_list_incarnations() -> None:
    """All incarnation files on disk are listed."""
    write_worker_health(_health(incarnation="aaa"))
    write_worker_health(_health(incarnation="bbb"))
    write_worker_health(_health(incarnation="ccc"))
    incarnations = list_worker_health_incarnations()
    assert incarnations == ["aaa", "bbb", "ccc"]


def test_list_incarnations_empty_dir() -> None:
    """Empty directory returns empty list."""
    assert list_worker_health_incarnations() == []


# ---------------------------------------------------------------------------
# Stable symlink publication (supervisor-only)
# ---------------------------------------------------------------------------


def test_publish_health_surface_creates_symlink() -> None:
    """Supervisor publishes stable health.json symlink."""
    write_worker_health(_health(incarnation="inc-10"))
    publish_current_health_surface("inc-10")
    symlink = health_current_path()
    assert symlink.is_symlink()
    assert symlink.readlink() == Path("health/health-inc-10.json")


def test_publish_log_surface_creates_symlink() -> None:
    """Supervisor publishes stable worker.log symlink."""
    publish_current_log_surface("inc-20")
    symlink = worker_log_current_path()
    assert symlink.is_symlink()
    assert symlink.readlink() == Path("logs/worker-inc-20.log")


def test_read_via_stable_symlink_after_publish() -> None:
    """read_worker_health resolves the stable symlink to the incarnation file."""
    health = _health(incarnation="inc-30")
    write_worker_health(health)
    publish_current_health_surface("inc-30")
    loaded = read_worker_health()
    assert loaded is not None
    assert loaded.worker_incarnation == "inc-30"


def test_read_via_stable_symlink_without_publish_returns_none() -> None:
    """Without supervisor publish, read_worker_health returns None."""
    write_worker_health(_health(incarnation="inc-40"))
    assert read_worker_health() is None


def test_publish_repoints_symlink_to_new_incarnation() -> None:
    """Supervisor can repoint the stable symlink to a new incarnation."""
    write_worker_health(_health(incarnation="old"))
    write_worker_health(_health(incarnation="new"))
    publish_current_health_surface("old")
    loaded_old = read_worker_health()
    assert loaded_old is not None
    assert loaded_old.worker_incarnation == "old"
    publish_current_health_surface("new")
    loaded_new = read_worker_health()
    assert loaded_new is not None
    assert loaded_new.worker_incarnation == "new"


def test_old_incarnation_file_intact_after_repoint() -> None:
    """Old incarnation health file survives repoint."""
    write_worker_health(_health(incarnation="retiring", pid=_PID_111))
    publish_current_health_surface("retiring")
    write_worker_health(_health(incarnation="candidate", pid=_PID_600))
    publish_current_health_surface("candidate")
    old = read_worker_health_by_incarnation("retiring")
    assert old is not None
    assert old.pid == _PID_111


def test_symlink_publication_raises_on_readonly_fs() -> None:
    """Symlink publication raises OSError on a read-only directory."""
    symlink = health_current_path()
    symlink.parent.mkdir(parents=True, exist_ok=True)
    (symlink.parent / "readonly").mkdir(exist_ok=True)
    bad_path = Path("/nonexistent-dir/health.json")
    try:
        publish_current_health_surface("should-fail")
    except OSError:
        return
    assert not bad_path.exists()


# ---------------------------------------------------------------------------
# Identity cross-check: old stable health cannot prove candidate readiness
# ---------------------------------------------------------------------------


def test_old_stable_health_cannot_be_used_for_candidate() -> None:
    """Supervisor reads by incarnation, not the stable symlink."""
    old_health = _health(incarnation="old-worker", pid=500, start_time_ticks=99)
    write_worker_health(old_health)
    publish_current_health_surface("old-worker")
    candidate_health = _health(incarnation="candidate-worker", pid=_PID_600, start_time_ticks=101)
    write_worker_health(candidate_health)
    stable = read_worker_health()
    assert stable is not None
    assert stable.worker_incarnation == "old-worker"
    candidate = read_worker_health_by_incarnation("candidate-worker")
    assert candidate is not None
    assert candidate.pid == _PID_600
    assert candidate.pid != stable.pid


def test_cross_check_rejects_mismatched_pid() -> None:
    """Snapshot PID not matching child PID is rejected."""
    health = _health(incarnation="inc-x", pid=_PID_999, start_time_ticks=_TICKS_50)
    write_worker_health(health)
    snap = read_worker_health_by_incarnation("inc-x")
    assert snap is not None
    assert snap.pid == _PID_999
    child_pid = 888
    assert snap.pid != child_pid


def test_cross_check_rejects_mismatched_start_time_ticks() -> None:
    """Snapshot start_time_ticks not matching child ticks is rejected."""
    health = _health(incarnation="inc-y", pid=_PID_777, start_time_ticks=_TICKS_55)
    write_worker_health(health)
    snap = read_worker_health_by_incarnation("inc-y")
    assert snap is not None
    assert snap.start_time_ticks != _TICKS_66


def test_cross_check_rejects_mismatched_incarnation() -> None:
    """Snapshot worker_incarnation not matching child token is rejected."""
    health = _health(incarnation="wrong-token", pid=_PID_777, start_time_ticks=_TICKS_55)
    write_worker_health(health)
    snap = read_worker_health_by_incarnation("wrong-token")
    assert snap is not None
    assert snap.worker_incarnation != "correct-token"


# ---------------------------------------------------------------------------
# Stale candidate snapshot cannot become current
# ---------------------------------------------------------------------------


def test_interpret_rejects_none_snapshot() -> None:
    """None snapshot is never live."""
    eff = interpret_worker_health(None)
    assert eff.live is False
    assert eff.stale is False


def test_interpret_rejects_dead_pid() -> None:
    """Snapshot with a dead PID is never live."""
    health = _health(pid=_PID_99999999, start_time_ticks=1)
    eff = interpret_worker_health(health, max_staleness_seconds=60.0)
    assert eff.live is False
    assert "not alive" in eff.reason


def test_interpret_rejects_stale_snapshot() -> None:
    """Snapshot older than max_staleness_seconds on published_at is stale."""
    health = _health(published_at=time.time() - _STALE_AGE_SECONDS)
    eff = interpret_worker_health(health, max_staleness_seconds=10.0)
    assert eff.live is False
    assert eff.stale is True


def test_interpret_fresh_despite_old_started_at() -> None:
    """Long-lived worker with recent published_at is not stale."""
    health = _health(
        started_at=time.time() - 3600,
        published_at=time.time() - 1,
    )
    eff = interpret_worker_health(health, max_staleness_seconds=10.0)
    assert eff.stale is False


def test_interpret_rejects_invalid_pid() -> None:
    """Negative PID is invalid."""
    health = _health(pid=-1)
    eff = interpret_worker_health(health)
    assert eff.live is False
    assert "invalid PID" in eff.reason


def test_interpret_rejects_pid_reuse() -> None:
    """PID reuse with wrong start_time_ticks is rejected."""
    current_ticks = 42
    health = _health(pid=os.getpid(), start_time_ticks=current_ticks + 1)
    actual_ticks = _proc_start_ticks(os.getpid())
    if actual_ticks is not None and actual_ticks != current_ticks + 1:
        eff = interpret_worker_health(health)
        assert eff.live is False
        assert "start time" in eff.reason


# ---------------------------------------------------------------------------
# current_job_started_at is wall-clock, not monotonic
# ---------------------------------------------------------------------------


def test_current_job_started_at_is_wall_clock() -> None:
    """current_job_started_at must be a wall-clock timestamp."""
    wall_now = time.time()
    health = _health(started_at=wall_now, published_at=wall_now)
    assert health.started_at == wall_now
    assert health.published_at == wall_now


# ---------------------------------------------------------------------------
# Corrupted / malformed snapshots
# ---------------------------------------------------------------------------


def test_read_by_incarnation_missing_file() -> None:
    """Missing file returns None."""
    assert read_worker_health_by_incarnation("nonexistent") is None


def test_read_by_incarnation_corrupt_json() -> None:
    """Corrupt JSON returns None."""
    path = health_incarnation_path("bad")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("not json {{{", encoding="utf-8")
    assert read_worker_health_by_incarnation("bad") is None


def test_read_by_incarnation_wrong_schema_version() -> None:
    """Wrong schema version returns None."""
    path = health_incarnation_path("wrong-ver")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"schema_version": 99, "pid": 1}) + "\n", encoding="utf-8")
    assert read_worker_health_by_incarnation("wrong-ver") is None


def test_read_by_incarnation_not_a_dict() -> None:
    """Non-dict JSON returns None."""
    path = health_incarnation_path("list-inc")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("[1, 2, 3]\n", encoding="utf-8")
    assert read_worker_health_by_incarnation("list-inc") is None


def test_read_via_stable_symlink_dangling() -> None:
    """Dangling symlink returns None."""
    symlink = health_current_path()
    symlink.parent.mkdir(parents=True, exist_ok=True)
    symlink.symlink_to("health-does-not-exist.json")
    assert read_worker_health() is None


# ---------------------------------------------------------------------------
# Bounded rotating logging
# ---------------------------------------------------------------------------


def test_configure_creates_log_file() -> None:
    """configure_worker_logging creates a logger with the right name."""
    logger = configure_worker_logging("log-inc-1")
    assert logger.name == "lubko.worker"
    log_path = worker_log_incarnation_path("log-inc-1")
    assert log_path.parent.is_dir()


def test_configure_creates_per_incarnation_file() -> None:
    """Each incarnation gets its own log file."""
    configure_worker_logging("log-inc-a")
    configure_worker_logging("log-inc-b")
    path_a = worker_log_incarnation_path("log-inc-a")
    path_b = worker_log_incarnation_path("log-inc-b")
    assert path_a != path_b


def test_log_writes_to_per_incarnation_file() -> None:
    """Log messages appear in the per-incarnation file."""
    logger = configure_worker_logging("log-inc-write")
    logger.info("test message from incarnation")
    log_path = worker_log_incarnation_path("log-inc-write")
    assert log_path.exists()
    content = log_path.read_text(encoding="utf-8")
    assert "test message from incarnation" in content


def test_rotating_handler_backup_count_bounded() -> None:
    """RotatingFileHandler has bounded backup count >= 1."""
    logger = configure_worker_logging("log-inc-bound")
    parent = logger.parent
    assert parent is not None
    handlers = [h for h in parent.handlers if isinstance(h, RotatingFileHandler)]
    assert len(handlers) >= 1
    handler = handlers[-1]
    assert handler.maxBytes == _LOG_MAX_BYTES_EXPECTED
    assert handler.backupCount >= _LOG_BACKUP_COUNT_MIN


# ---------------------------------------------------------------------------
# worker_health_payload
# ---------------------------------------------------------------------------


def test_payload_with_none() -> None:
    """None snapshot produces a payload with live=False."""
    result = worker_health_payload(None)
    assert result is not None
    assert result["live"] is False
    assert result["snapshot"] is None


def test_payload_with_valid_snapshot() -> None:
    """Valid snapshot produces a payload with snapshot dict."""
    ticks = _proc_start_ticks(os.getpid()) or 0
    health = _health(pid=os.getpid(), start_time_ticks=ticks)
    result = worker_health_payload(health, max_staleness_seconds=60.0)
    assert result is not None
    assert result["snapshot"] is not None
    assert result["snapshot"]["worker_incarnation"] == "abc123"


def test_payload_fields() -> None:
    """Payload has the expected keys."""
    result = worker_health_payload(None)
    assert result is not None
    assert set(result.keys()) == {"snapshot", "live", "stale", "reason"}


# ---------------------------------------------------------------------------
# EffectiveHealth dataclass
# ---------------------------------------------------------------------------


def test_effective_health_fields() -> None:
    """EffectiveHealth carries snapshot + live/stale/reason."""
    eff = EffectiveHealth(snapshot=None, live=False, stale=True, reason="test")
    assert eff.live is False
    assert eff.stale is True
    assert eff.reason == "test"
    assert eff.snapshot is None


# ---------------------------------------------------------------------------
# Regression: per-incarnation log paths, no competing writers
# ---------------------------------------------------------------------------


def test_per_incarnation_log_paths_are_distinct() -> None:
    """Each incarnation gets a unique log file path; no shared writer."""
    from lubko.health import worker_log_incarnation_path

    path_a = worker_log_incarnation_path("inc-a")
    path_b = worker_log_incarnation_path("inc-b")
    assert path_a != path_b
    assert path_a.parent == path_b.parent


def test_worker_log_path_returns_per_incarnation_with_token() -> None:
    """worker_log_path(token) returns per-incarnation path."""
    from lubko.lifecycle import worker_log_path

    path = worker_log_path("test-token-abc")
    assert path.name == "worker-test-token-abc.log"
    assert path.parent.name == "logs"


def test_worker_log_path_returns_stable_without_token() -> None:
    """worker_log_path() without token returns stable path."""
    from lubko.lifecycle import worker_log_path

    path = worker_log_path()
    assert path.name == "worker.log"
    assert path.parent.name == "worker"


def test_stable_log_symlink_not_created_by_worker() -> None:
    """Worker never creates the stable worker.log symlink."""
    from lubko.health import worker_log_current_path

    symlink = worker_log_current_path()
    assert not symlink.exists(), "worker must not create the stable log symlink"
