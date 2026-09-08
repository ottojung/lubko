"""Stable invariants for bounded persistent supervisor diagnostics."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from lubko import deployctl, supervise, supervisor

if TYPE_CHECKING:
    from pathlib import Path

    import pytest


def _logger(handler: logging.Handler) -> logging.Logger:
    logger = logging.getLogger("test.supervisor.logging")
    logger.handlers.clear()
    logger.propagate = False
    logger.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter("%(levelname)s %(message)s"))
    logger.addHandler(handler)
    return logger


def _raise_failure(detail: str) -> None:
    raise ValueError(detail)


def _exception(logger: logging.Logger, message: str, detail: str) -> None:
    try:
        _raise_failure(detail)
    except ValueError:
        logger.exception(message)


def test_identical_persistent_failures_are_coalesced_and_recover(tmp_path: Path) -> None:
    """Thousands of identical failures retain one full traceback plus summaries."""
    log = tmp_path / "supervisor.log"
    handler = supervisor._BoundedSupervisorLogHandler(
        log, max_bytes=1_000_000, backup_count=1, repeat_interval=100
    )
    logger = _logger(handler)
    for _ in range(1_000):
        handler.begin_reconciliation_cycle()
        _exception(logger, "corrupt durable state", "same failure")
        handler.end_reconciliation_cycle()
    handler.begin_reconciliation_cycle()
    handler.end_reconciliation_cycle()
    handler.close()
    contents = log.read_text()
    assert contents.count("Traceback (most recent call last)") == 1
    assert "persistent supervisor diagnostic repeated 900 times" in contents
    assert "persistent supervisor diagnostic recovered after 999 suppressed repeats" in contents
    assert len(contents) < 20_000


def test_changed_failure_gets_a_fresh_diagnostic(tmp_path: Path) -> None:
    """A materially changed failure retains a fresh full diagnostic."""
    log = tmp_path / "supervisor.log"
    handler = supervisor._BoundedSupervisorLogHandler(
        log, max_bytes=1_000_000, backup_count=1, repeat_interval=100
    )
    logger = _logger(handler)
    for detail in ("first", "first", "second"):
        handler.begin_reconciliation_cycle()
        _exception(logger, "corrupt durable state", detail)
        handler.end_reconciliation_cycle()
    handler.close()
    contents = log.read_text()
    assert contents.count("Traceback (most recent call last)") == 2
    assert "persistent supervisor diagnostic changed after 1 suppressed repeats" in contents
    assert "ValueError: first" in contents
    assert "ValueError: second" in contents


def test_supervisor_log_retention_is_bounded(tmp_path: Path) -> None:
    """Rotation retains only the configured number of bounded log files."""
    log = tmp_path / "supervisor.log"
    handler = supervisor._BoundedSupervisorLogHandler(
        log, max_bytes=512, backup_count=2, repeat_interval=10
    )
    logger = _logger(handler)
    for index in range(2_000):
        logger.info("ordinary diagnostic %04d %s", index, "x" * 40)
    handler.close()
    logs = sorted(tmp_path.glob("supervisor.log*"))
    assert len(logs) <= 3
    assert sum(item.stat().st_size for item in logs) < 2_000


def test_log_rotation_failure_cannot_escape_into_supervisor_control(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Filesystem errors from rollover are swallowed at the logging boundary."""
    handler = supervisor._BoundedSupervisorLogHandler(
        tmp_path / "supervisor.log", max_bytes=1, backup_count=1, repeat_interval=10
    )
    logger = _logger(handler)

    def fail_rollover() -> None:
        message = "disk unavailable"
        raise OSError(message)

    monkeypatch.setattr(handler, "doRollover", fail_rollover)
    logger.info("this record requires rollover")
    handler.close()


def test_status_preserves_current_corruption_recovery_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Status keeps the current reconciliation result under corrupt mission state."""
    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    daemon._message = (
        "corrupt supervised-deployment state; restoring independently confirmed runtime " + "a" * 40
    )
    daemon._next_db_check_at = float("inf")
    captured: list[supervise.SupervisorStatus] = []
    monkeypatch.setattr(supervisor, "read_state", lambda: supervise.SupervisorState.from_dict({}))

    def corrupt_mission() -> None:
        error = "malformed mission"
        raise deployctl.DeployCtlError(error)

    monkeypatch.setattr(deployctl, "read_rollback_state", corrupt_mission)
    monkeypatch.setattr(supervisor, "read_worker_health", lambda: None)
    monkeypatch.setattr(supervisor, "write_status", captured.append)
    daemon._write_status()
    assert len(captured) == 1
    assert captured[0].message == daemon._message
    assert "restoring independently confirmed runtime" in (captured[0].message or "")
