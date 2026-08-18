"""Machine-readable worker health snapshots and bounded operational logging.

Every worker incarnation writes its health snapshot to an incarnation-specific
file (``health/health-{incarnation}.json``) and its operational log to an
incarnation-specific ``RotatingFileHandler`` (``logs/worker-{incarnation}.log``).
Overlapping old/candidate workers never race on a single file.

The supervisor is the sole lifecycle authority that publishes the stable
read surface: ``health.json`` and ``worker.log`` symlinks pointing to the
confirmed incarnation's files.  The supervisor updates these symlinks only
after matching the exact child PID, start-time ticks, incarnation, and
queue-readiness — so a retiring old worker or a stale candidate can never
make the confirmed worker appear unhealthy, and candidate evidence remains
intact across handoff/rollback.

No job payload contents, secrets, or decrypted command text ever reach the
health snapshot or the operational log.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import tempfile
import time
from contextlib import suppress
from dataclasses import dataclass
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final

if TYPE_CHECKING:
    from types import TracebackType

from lubko.state import worker_state_dir

LOGGER: Final = logging.getLogger(__name__)

HEALTH_DIR: Final = "health"
LOGS_DIR: Final = "logs"
HEALTH_SYMLINK_FILENAME: Final = "health.json"
WORKER_LOG_SYMLINK_FILENAME: Final = "worker.log"
WORKER_LOG_MAX_BYTES: Final = 2 * 1024 * 1024  # 2 MiB per file
WORKER_LOG_BACKUP_COUNT: Final = 3

LIFECYCLE_MARKER_VAR: Final = "LUBKO_LIFECYCLE_TOKEN"


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------


def _health_dir() -> Path:
    """Return the directory holding per-incarnation health files.

    Returns:
        The ``worker/health/`` directory.
    """
    return worker_state_dir() / HEALTH_DIR


def _logs_dir() -> Path:
    """Return the directory holding per-incarnation log files.

    Returns:
        The ``worker/logs/`` directory.
    """
    return worker_state_dir() / LOGS_DIR


def health_incarnation_path(incarnation: str) -> Path:
    """Return the file path for a specific incarnation's health snapshot.

    Args:
        incarnation: Worker incarnation identifier.

    Returns:
        The per-incarnation health JSON file path.
    """
    return _health_dir() / f"health-{incarnation}.json"


def health_current_path() -> Path:
    """Return the stable symlink path that points to the current incarnation.

    The supervisor is the sole writer of this symlink.

    Returns:
        The ``health.json`` symlink path under the worker state directory.
    """
    return worker_state_dir() / HEALTH_SYMLINK_FILENAME


def worker_log_incarnation_path(incarnation: str) -> Path:
    """Return the per-incarnation log file path.

    Each incarnation gets its own ``RotatingFileHandler`` target so two
    overlapping workers never share a log file.

    Args:
        incarnation: Worker incarnation identifier.

    Returns:
        The per-incarnation log file path.
    """
    return _logs_dir() / f"worker-{incarnation}.log"


def worker_log_current_path() -> Path:
    """Return the stable symlink path for the worker log.

    The supervisor is the sole writer of this symlink.

    Returns:
        The ``worker.log`` symlink path under the worker state directory.
    """
    return worker_state_dir() / WORKER_LOG_SYMLINK_FILENAME


# ---------------------------------------------------------------------------
# Health snapshot model
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class WorkerHealth:
    """Machine-readable snapshot of the worker's live state.

    Every field is JSON-serialisable.  ``pid`` and ``start_time_ticks`` anchor
    identity to a specific process incarnation so a stale snapshot from a dead
    or PID-reused worker is never mistaken for current.
    """

    schema_version: int
    worker_id: str
    worker_incarnation: str
    pid: int
    start_time_ticks: int
    started_at: float
    alive: bool
    db_connected: bool
    db_connected_at: float | None
    db_error_at: float | None
    current_job_id: str | None
    current_job_started_at: float | None
    last_completed_job_id: str | None
    last_completed_at: float | None
    last_completed_status: str | None
    shutting_down: bool

    def to_dict(self) -> dict[str, Any]:
        """Serialize the snapshot for atomic storage.

        Returns:
            A JSON-serialisable mapping.
        """
        return {
            "schema_version": self.schema_version,
            "worker_id": self.worker_id,
            "worker_incarnation": self.worker_incarnation,
            "pid": self.pid,
            "start_time_ticks": self.start_time_ticks,
            "started_at": self.started_at,
            "alive": self.alive,
            "db_connected": self.db_connected,
            "db_connected_at": self.db_connected_at,
            "db_error_at": self.db_error_at,
            "current_job_id": self.current_job_id,
            "current_job_started_at": self.current_job_started_at,
            "last_completed_job_id": self.last_completed_job_id,
            "last_completed_at": self.last_completed_at,
            "last_completed_status": self.last_completed_status,
            "shutting_down": self.shutting_down,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> WorkerHealth:
        """Rebuild a snapshot from a stored mapping.

        Args:
            data: Mapping produced by :meth:`to_dict`.

        Returns:
            The reconstructed health snapshot.
        """
        return cls(
            schema_version=int(data.get("schema_version") or 1),
            worker_id=str(data.get("worker_id") or ""),
            worker_incarnation=str(data.get("worker_incarnation") or ""),
            pid=int(data.get("pid") or 0),
            start_time_ticks=int(data.get("start_time_ticks") or 0),
            started_at=float(data.get("started_at") or 0.0),
            alive=bool(data.get("alive")),
            db_connected=bool(data.get("db_connected")),
            db_connected_at=_optional_float(data.get("db_connected_at")),
            db_error_at=_optional_float(data.get("db_error_at")),
            current_job_id=_optional_str(data.get("current_job_id")),
            current_job_started_at=_optional_float(data.get("current_job_started_at")),
            last_completed_job_id=_optional_str(data.get("last_completed_job_id")),
            last_completed_at=_optional_float(data.get("last_completed_at")),
            last_completed_status=_optional_str(data.get("last_completed_status")),
            shutting_down=bool(data.get("shutting_down")),
        )


def _optional_float(value: object) -> float | None:
    """Coerce a JSON value to float or None.

    Args:
        value: Raw JSON value.

    Returns:
        The float, or ``None``.
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def _optional_str(value: object) -> str | None:
    """Coerce a JSON value to str or None.

    Args:
        value: Raw JSON value.

    Returns:
        The string, or ``None``.
    """
    return value if isinstance(value, str) else None


# ---------------------------------------------------------------------------
# Worker-only: atomic per-incarnation snapshot storage
# ---------------------------------------------------------------------------


def write_worker_health(health: WorkerHealth) -> None:
    """Atomically persist a worker health snapshot for its incarnation.

    The worker writes **only** to its own per-incarnation file.  It never
    touches the stable ``health.json`` symlink: the supervisor is the sole
    authority that publishes the stable read surface.

    On failure the temporary file is cleaned up so a partial write never
    poisons the directory.

    Args:
        health: Health snapshot to store.
    """
    inc_path = health_incarnation_path(health.worker_incarnation)
    inc_path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        dir=str(inc_path.parent),
        prefix=f"health-{health.worker_incarnation}",
        suffix=".tmp",
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(health.to_dict(), fh, sort_keys=True)
            fh.write("\n")
        Path(tmp_name).replace(inc_path)
    except BaseException:
        with suppress(OSError):
            Path(tmp_name).unlink()
        raise


# ---------------------------------------------------------------------------
# Supervisor-only: stable read surface publication
# ---------------------------------------------------------------------------


def publish_current_health_surface(incarnation: str) -> None:
    """Update the stable ``health.json`` symlink to point at an incarnation.

    This is the **sole** place the stable symlink is ever written.  The
    supervisor calls this only after matching the exact child PID, start-time
    ticks, incarnation identifier, and queue-readiness — so a retiring old
    worker or a stale candidate can never move the symlink.

    Args:
        incarnation: The confirmed incarnation whose snapshot is current.
    """
    symlink = health_current_path()
    symlink.parent.mkdir(parents=True, exist_ok=True)
    target_name = str(Path(HEALTH_DIR) / f"health-{incarnation}.json")
    _atomic_symlink_update(symlink, target_name)


def publish_current_log_surface(incarnation: str) -> None:
    """Update the stable ``worker.log`` symlink to point at an incarnation.

    This is the **sole** place the stable log symlink is ever written.
    Same contract as :func:`publish_current_health_surface`.

    Args:
        incarnation: The confirmed incarnation whose log is current.
    """
    symlink = worker_log_current_path()
    symlink.parent.mkdir(parents=True, exist_ok=True)
    target_name = str(Path(LOGS_DIR) / f"worker-{incarnation}.log")
    _atomic_symlink_update(symlink, target_name)


def _atomic_symlink_update(symlink: Path, target_name: str) -> None:
    """Atomically replace a symlink to point at a new target name.

    Uses a temporary symlink + ``os.replace()`` so a concurrent reader never
    sees a partial update or a dangling intermediate.

    Args:
        symlink: The stable symlink path.
        target_name: The new target filename (relative to the symlink's parent).
    """
    tmp_symlink = symlink.with_name(f"{symlink.name}.tmp")
    try:
        tmp_symlink.unlink(missing_ok=True)
        tmp_symlink.symlink_to(target_name)
        tmp_symlink.replace(symlink)
    except OSError:
        LOGGER.debug("failed to update stable symlink %s", symlink, exc_info=True)


# ---------------------------------------------------------------------------
# Reader: stable symlink resolution
# ---------------------------------------------------------------------------


def read_worker_health() -> WorkerHealth | None:
    """Load the current incarnation's health snapshot via the stable symlink.

    The symlink is resolved to read the per-incarnation file.  When the
    symlink is missing, dangling, or the target is corrupted, ``None`` is
    returned so stale candidates never masquerade as the confirmed worker.

    Returns:
        The parsed snapshot, or ``None`` when absent, malformed, or
        schema-mismatched.
    """
    symlink = health_current_path()
    try:
        target = symlink.readlink()
    except OSError:
        return None
    inc_path = symlink.parent / target
    return _read_health_file(inc_path)


def read_worker_health_by_incarnation(incarnation: str) -> WorkerHealth | None:
    """Load a specific incarnation's health snapshot.

    Args:
        incarnation: Worker incarnation identifier.

    Returns:
        The parsed snapshot, or ``None`` when absent or malformed.
    """
    return _read_health_file(health_incarnation_path(incarnation))


def list_worker_health_incarnations() -> list[str]:
    """List all incarnation identifiers that have health snapshots on disk.

    Returns:
        Sorted list of incarnation identifiers.
    """
    d = _health_dir()
    if not d.is_dir():
        return []
    result: list[str] = []
    for entry in d.iterdir():
        if entry.name.startswith("health-") and entry.name.endswith(".json"):
            inc = entry.name[len("health-") : -len(".json")]
            if inc:
                result.append(inc)
    return sorted(result)


def _read_health_file(path: Path) -> WorkerHealth | None:
    """Read and parse one health file, failing closed on any problem.

    Args:
        path: Path to a health JSON file.

    Returns:
        The parsed snapshot, or ``None``.
    """
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError:
        return None
    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, ValueError):
        return None
    if not isinstance(data, dict):
        return None
    schema_version = int(data.get("schema_version") or 0)
    if schema_version != 1:
        return None
    try:
        return WorkerHealth.from_dict(data)
    except (TypeError, ValueError, KeyError):
        return None


# ---------------------------------------------------------------------------
# Liveness interpretation
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class EffectiveHealth:
    """Interpreted health combining raw snapshot with liveness verification.

    ``live`` is ``True`` only when the snapshot is fresh, the PID is alive,
    and the start-time ticks match the expected process identity — so a
    SIGKILLed worker's stale ``alive=true`` is never trusted.
    """

    snapshot: WorkerHealth | None
    live: bool
    stale: bool
    reason: str


def interpret_worker_health(
    snapshot: WorkerHealth | None,
    *,
    max_staleness_seconds: float = 10.0,
) -> EffectiveHealth:
    """Cross-check a health snapshot against live process state.

    The interpretation verifies:
    - the snapshot is present and parseable;
    - the recorded PID matches a live, non-zombie process;
    - the start-time ticks match the current process start time;
    - the snapshot is not stale (written within ``max_staleness_seconds``).

    Args:
        snapshot: The raw health snapshot.
        max_staleness_seconds: How old a snapshot may be before it is stale.

    Returns:
        An ``EffectiveHealth`` result.
    """
    if snapshot is None:
        return EffectiveHealth(snapshot=None, live=False, stale=False, reason="no health snapshot")
    now = time.time()
    age = now - snapshot.started_at
    pid = snapshot.pid
    reason = "ok"
    live = snapshot.alive
    stale = False
    if age > max_staleness_seconds:
        reason = f"snapshot age {age:.1f}s exceeds {max_staleness_seconds}s"
        live = False
        stale = True
    elif pid <= 0:
        reason = "invalid PID in snapshot"
        live = False
    else:
        current_ticks = proc_start_ticks(pid)
        if current_ticks is None:
            reason = f"PID {pid} is not alive"
            live = False
        elif current_ticks != snapshot.start_time_ticks:
            reason = f"PID {pid} start time {current_ticks} != expected {snapshot.start_time_ticks}"
            live = False
        elif not _process_is_live(pid):
            reason = f"PID {pid} is a zombie or dead"
            live = False
    return EffectiveHealth(snapshot=snapshot, live=live, stale=stale, reason=reason)


def _process_is_live(pid: int) -> bool:
    """Return whether a PID is a live non-zombie process.

    Args:
        pid: Process ID to inspect.

    Returns:
        ``True`` when the process is alive and not a zombie.
    """
    try:
        stat = (Path("/proc") / str(pid) / "stat").read_bytes()
    except OSError:
        return False
    close_paren = stat.rfind(b")")
    if close_paren == -1:
        return False
    fields = stat[close_paren + 2 :].split()
    if not fields:
        return False
    return fields[0] not in {b"Z", b"X"}


def worker_health_payload(
    snapshot: WorkerHealth | None,
    *,
    max_staleness_seconds: float = 10.0,
) -> dict[str, Any] | None:
    """Combine raw snapshot and effective interpretation for external tools.

    Args:
        snapshot: The raw health snapshot.
        max_staleness_seconds: Staleness threshold for liveness interpretation.

    Returns:
        A JSON-serialisable mapping, or ``None`` when absent.
    """
    effective = interpret_worker_health(snapshot, max_staleness_seconds=max_staleness_seconds)
    return {
        "snapshot": effective.snapshot.to_dict() if effective.snapshot is not None else None,
        "live": effective.live,
        "stale": effective.stale,
        "reason": effective.reason,
    }


# ---------------------------------------------------------------------------
# Process identity helpers
# ---------------------------------------------------------------------------

STAT_MIN_FIELDS: Final = 20
STAT_STARTTIME_FIELD_INDEX: Final = 19


def proc_start_ticks(pid: int) -> int | None:
    """Return a process start time in clock ticks, or ``None`` if unknown.

    The start time is unique per process on a boot and survives PID reuse,
    so it anchors identity checks.

    Args:
        pid: Process ID to inspect.

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
    if len(fields) < STAT_MIN_FIELDS:
        return None
    try:
        return int(fields[STAT_STARTTIME_FIELD_INDEX])
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Bounded operational logging
# ---------------------------------------------------------------------------


def configure_worker_logging(incarnation: str) -> logging.Logger:
    """Configure the root logger with a per-incarnation ``RotatingFileHandler``.

    The handler writes to a per-incarnation log file so two overlapping
    workers never share a log file.  The supervisor later publishes a stable
    ``worker.log`` symlink pointing to the confirmed incarnation's log.

    This is the **only** place a ``RotatingFileHandler`` for any worker log
    is created in the worker process: the parent process (supervisor,
    lifecycle scripts) never opens the worker log, so there is exactly one
    writer/owner per incarnation.

    Args:
        incarnation: Worker incarnation identifier.

    Returns:
        The configured ``lubko.worker`` logger.
    """
    log_path = worker_log_incarnation_path(incarnation)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    handler = RotatingFileHandler(
        str(log_path),
        maxBytes=WORKER_LOG_MAX_BYTES,
        backupCount=WORKER_LOG_BACKUP_COUNT,
        encoding="utf-8",
    )
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.addHandler(handler)
    return logging.getLogger("lubko.worker")


def install_worker_exception_hooks() -> None:
    """Route unhandled exceptions into the bounded operational log.

    Installs a custom ``sys.excepthook`` so unhandled failures are visible
    in the worker log rather than silently lost.
    """
    default_hook = sys.excepthook

    def _log_exception(
        exc_type: type[BaseException],
        exc_value: BaseException,
        exc_traceback: TracebackType | None,
    ) -> None:
        LOGGER.error("Unhandled exception: %s", exc_value)
        default_hook(exc_type, exc_value, exc_traceback)

    sys.excepthook = _log_exception


# ---------------------------------------------------------------------------
# Lifecycle detection
# ---------------------------------------------------------------------------


def worker_under_lifecycle() -> bool:
    """Return whether this worker was launched by a lifecycle-managed deployment.

    The lifecycle marker environment variable is set by ``lubko-deploy`` and
    ``lubko-deploy-ctl`` when they spawn a worker.

    Returns:
        ``True`` when the lifecycle marker is present in the environment.
    """
    return bool(os.environ.get(LIFECYCLE_MARKER_VAR))
