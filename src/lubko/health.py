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
import math
import os
import re
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

from lubko._exact_signal import open_pidfd as _open_pidfd
from lubko._exact_signal import pidfd_send_signal as _pidfd_send_signal
from lubko.state import worker_state_dir

LOGGER: Final = logging.getLogger(__name__)

HEALTH_DIR: Final = "health"
LOGS_DIR: Final = "logs"
HEALTH_SYMLINK_FILENAME: Final = "health.json"
WORKER_LOG_SYMLINK_FILENAME: Final = "worker.log"
WORKER_LOG_MAX_BYTES: Final = 2 * 1024 * 1024  # 2 MiB per file
WORKER_LOG_BACKUP_COUNT: Final = 3

#: Current on-disk schema version for the per-incarnation worker health
#: snapshot.  Version 1 exposed a singular ``current_job_id`` that could only
#: ever describe one of potentially many concurrently active jobs and therefore
#: misrepresented a busy worker as idle-or-single-job.  Version 2 replaces it
#: with concurrency-aware aggregates and bounded operational counters.  Old
#: snapshots are never treated as current (they fail closed in the reader).
WORKER_HEALTH_SCHEMA_VERSION: Final = 2

LIFECYCLE_MARKER_VAR: Final = "LUBKO_LIFECYCLE_TOKEN"

#: Regex matching safe filename components (hex tokens, alphanumerics, hyphens).
_SAFE_FILENAME_RE: Final = re.compile(r"^[a-zA-Z0-9_-]+$")


def validate_incarnation_token(token: str) -> None:
    """Validate that a lifecycle incarnation token is safe for use in filenames.

    The token is embedded in per-incarnation health and log file paths, so it
    must contain only characters that are safe across all platforms.

    Args:
        token: The incarnation token to validate.

    Raises:
        ValueError: If the token contains unsafe characters.
    """
    if not token:
        msg = "incarnation token must not be empty"
        raise ValueError(msg)
    if not _SAFE_FILENAME_RE.match(token):
        msg = f"incarnation token contains unsafe characters: {token!r}"
        raise ValueError(msg)


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
    """Machine-readable snapshot of the worker's live, concurrency-aware state.

    Every field is JSON-serialisable and bounded: no job id list, command
    text, secret, or unbounded payload ever reaches the snapshot.  ``pid`` and
    ``start_time_ticks`` anchor identity to a specific process incarnation so a
    stale snapshot from a dead or PID-reused worker is never mistaken for
    current.

    The singular ``current_job_id`` of earlier schemas was misleading: a worker
    supervises an unbounded number of concurrently active jobs, so a single id
    could never describe its true state.  This schema reports aggregates
    (``active_jobs``/``stopping_jobs``/``completed_jobs``), a bounded oldest
    active age (never a job id), and bounded operational counters/timestamps
    for lease safety, capture/spool pressure, scan batch pressure, periodic
    scan saturation, and database deadline recency.
    """

    schema_version: int
    worker_id: str
    worker_incarnation: str
    pid: int
    start_time_ticks: int
    started_at: float
    published_at: float
    alive: bool
    db_connected: bool
    db_connected_at: float | None
    db_error_at: float | None
    #: Number of jobs this worker incarnation is currently supervising.
    active_jobs: int
    #: Number of currently active jobs in a terminal stop/escalation phase.
    stopping_jobs: int
    #: Total jobs this worker incarnation has finalized (bounded lifetime count).
    completed_jobs: int
    #: Wall-clock-agnostic age (seconds) of the oldest active job, or ``None``.
    #: No job id is ever published: identity is intentionally not exposed.
    oldest_active_job_age_seconds: float | None
    #: Configured lease-safety margin the worker enforces before eviction.
    lease_safety_margin_seconds: float
    #: Remaining lease-safety budget for the most-at-risk active job, or
    #: ``None``.  Computed as ``last_heartbeat + lease_duration -
    #: lease_safety_margin - now``, so it is the safety deadline (lease expiry
    #: minus the configured margin), not the full-lease remaining.  Negative
    #: means the safety deadline has already passed and the job is at risk.
    min_lease_safety_remaining_seconds: float | None
    #: Configured hard client-side database operation deadline.
    db_operation_deadline_seconds: float
    #: Wall-clock time of the last database operation, or ``None`` (recency).
    db_last_activity_at: float | None
    #: Wall-clock time of the most recent hard client deadline breach, or
    #: ``None``.  This is the explicit signal that a database operation actually
    #: exceeded its deadline; ``db_last_activity_at`` alone cannot prove it.
    db_deadline_breached_at: float | None
    #: Count of hard client deadline breaches observed this incarnation.
    db_deadline_breach_count: int
    #: Count of active capture streams still draining (open, non-EOF pipes).
    capture_streams_open: int
    #: Aggregate bytes currently held in active on-disk capture spool files.
    spool_held_bytes: int
    #: Configured fairness cap on one claiming turn's batch size.
    scan_batch_limit: int
    #: Number of jobs actually claimed in the most recent scan batch (pressure).
    last_scan_batch_size: int
    #: Wall-clock time of the most recent cancellation-scan turn, or ``None``.
    last_cancellation_scan_at: float | None
    #: Wall-clock time of the most recent recovery pass, or ``None``.
    last_recovery_at: float | None
    #: Wall-clock time of the most recent GC pass, or ``None``.
    last_gc_at: float | None
    #: Whether the cancellation scan is overdue (next due time already passed).
    cancellation_scan_overdue: bool
    #: Whether the recovery pass is overdue (next due time already passed).
    recovery_overdue: bool
    #: Whether the GC pass is overdue (next due time already passed).
    gc_overdue: bool
    #: Configured per-turn GC batch cap.
    gc_batch_limit: int
    #: Whether the most recent GC pass saturated a per-phase batch bound (a
    #: pressure/saturation signal), derived inside ``collect_transport`` from
    #: the actual capped selections/deletions rather than summed row counts.
    #: Combine with ``gc_overdue`` and ``last_gc_at`` for a "behind" diagnosis.
    gc_batch_bound_hit: bool
    #: Configured per-turn cancellation-discovery batch cap.
    cancellation_batch_limit: int
    #: Whether the most recent cancellation scan saturated its batch bound (a
    #: pressure/saturation signal), from the actual returned cancellation count.
    cancellation_batch_bound_hit: bool
    #: Configured per-turn stale-job recovery batch cap.
    recovery_batch_limit: int
    #: Whether the most recent recovery pass saturated its batch bound (a
    #: pressure/saturation signal), from the actual returned recovered count.
    recovery_batch_bound_hit: bool
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
            "started_at": _finite_or_zero(self.started_at),
            "published_at": _finite_or_zero(self.published_at),
            "alive": self.alive,
            "db_connected": self.db_connected,
            "db_connected_at": _finite_or_none(self.db_connected_at),
            "db_error_at": _finite_or_none(self.db_error_at),
            "active_jobs": self.active_jobs,
            "stopping_jobs": self.stopping_jobs,
            "completed_jobs": self.completed_jobs,
            "oldest_active_job_age_seconds": _finite_or_none(self.oldest_active_job_age_seconds),
            "lease_safety_margin_seconds": _finite_or_zero(self.lease_safety_margin_seconds),
            "min_lease_safety_remaining_seconds": _finite_or_none(
                self.min_lease_safety_remaining_seconds
            ),
            "db_operation_deadline_seconds": _finite_or_zero(self.db_operation_deadline_seconds),
            "db_last_activity_at": _finite_or_none(self.db_last_activity_at),
            "db_deadline_breached_at": _finite_or_none(self.db_deadline_breached_at),
            "db_deadline_breach_count": self.db_deadline_breach_count,
            "capture_streams_open": self.capture_streams_open,
            "spool_held_bytes": self.spool_held_bytes,
            "scan_batch_limit": self.scan_batch_limit,
            "last_scan_batch_size": self.last_scan_batch_size,
            "last_cancellation_scan_at": _finite_or_none(self.last_cancellation_scan_at),
            "last_recovery_at": _finite_or_none(self.last_recovery_at),
            "last_gc_at": _finite_or_none(self.last_gc_at),
            "cancellation_scan_overdue": self.cancellation_scan_overdue,
            "recovery_overdue": self.recovery_overdue,
            "gc_overdue": self.gc_overdue,
            "gc_batch_limit": self.gc_batch_limit,
            "gc_batch_bound_hit": self.gc_batch_bound_hit,
            "cancellation_batch_limit": self.cancellation_batch_limit,
            "cancellation_batch_bound_hit": self.cancellation_batch_bound_hit,
            "recovery_batch_limit": self.recovery_batch_limit,
            "recovery_batch_bound_hit": self.recovery_batch_bound_hit,
            "shutting_down": self.shutting_down,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> WorkerHealth:
        """Rebuild a snapshot from a stored mapping.

        Args:
            data: Mapping produced by :meth:`to_dict`.

        Returns:
            The reconstructed health snapshot.

        Raises:
            ValueError: If the mapping is schema-incompatible or holds a
                non-finite timestamp.
        """
        version = _required_json_int(data.get("schema_version"), "schema_version", minimum=0)
        if version != WORKER_HEALTH_SCHEMA_VERSION:
            msg = f"unsupported worker health schema version: {version}"
            raise ValueError(msg)
        return cls(
            schema_version=version,
            worker_id=str(data.get("worker_id") or ""),
            worker_incarnation=str(data.get("worker_incarnation") or ""),
            pid=_required_json_int(data.get("pid"), "pid", minimum=1),
            start_time_ticks=_required_json_int(
                data.get("start_time_ticks"), "start_time_ticks", minimum=1
            ),
            started_at=_required_finite_float(data.get("started_at"), "started_at"),
            published_at=_required_finite_float(data.get("published_at"), "published_at"),
            alive=bool(data.get("alive")),
            db_connected=bool(data.get("db_connected")),
            db_connected_at=_optional_finite_float(data.get("db_connected_at")),
            db_error_at=_optional_finite_float(data.get("db_error_at")),
            active_jobs=_coerce_int(data.get("active_jobs"), "active_jobs"),
            stopping_jobs=_coerce_int(data.get("stopping_jobs"), "stopping_jobs"),
            completed_jobs=_coerce_int(data.get("completed_jobs"), "completed_jobs"),
            oldest_active_job_age_seconds=_optional_finite_float(
                data.get("oldest_active_job_age_seconds")
            ),
            lease_safety_margin_seconds=_required_finite_float(
                data.get("lease_safety_margin_seconds"), "lease_safety_margin_seconds"
            ),
            min_lease_safety_remaining_seconds=_optional_finite_float(
                data.get("min_lease_safety_remaining_seconds")
            ),
            db_operation_deadline_seconds=_required_finite_float(
                data.get("db_operation_deadline_seconds"), "db_operation_deadline_seconds"
            ),
            db_last_activity_at=_optional_finite_float(data.get("db_last_activity_at")),
            db_deadline_breached_at=_optional_finite_float(data.get("db_deadline_breached_at")),
            db_deadline_breach_count=_coerce_int(
                data.get("db_deadline_breach_count"), "db_deadline_breach_count"
            ),
            capture_streams_open=_coerce_int(
                data.get("capture_streams_open"), "capture_streams_open"
            ),
            spool_held_bytes=_coerce_int(data.get("spool_held_bytes"), "spool_held_bytes"),
            scan_batch_limit=_coerce_int(data.get("scan_batch_limit"), "scan_batch_limit"),
            last_scan_batch_size=_coerce_int(
                data.get("last_scan_batch_size"), "last_scan_batch_size"
            ),
            last_cancellation_scan_at=_optional_finite_float(data.get("last_cancellation_scan_at")),
            last_recovery_at=_optional_finite_float(data.get("last_recovery_at")),
            last_gc_at=_optional_finite_float(data.get("last_gc_at")),
            cancellation_scan_overdue=_coerce_bool(data.get("cancellation_scan_overdue")),
            recovery_overdue=_coerce_bool(data.get("recovery_overdue")),
            gc_overdue=_coerce_bool(data.get("gc_overdue")),
            gc_batch_limit=_coerce_int(data.get("gc_batch_limit"), "gc_batch_limit"),
            gc_batch_bound_hit=_coerce_bool(data.get("gc_batch_bound_hit")),
            cancellation_batch_limit=_coerce_int(
                data.get("cancellation_batch_limit"), "cancellation_batch_limit"
            ),
            cancellation_batch_bound_hit=_coerce_bool(data.get("cancellation_batch_bound_hit")),
            recovery_batch_limit=_coerce_int(
                data.get("recovery_batch_limit"), "recovery_batch_limit"
            ),
            recovery_batch_bound_hit=_coerce_bool(data.get("recovery_batch_bound_hit")),
            shutting_down=bool(data.get("shutting_down")),
        )


def _required_json_int(value: object, field: str, *, minimum: int) -> int:
    """Return a required JSON integer without coercing malformed authority.

    Raises:
        TypeError: If the persisted value is not an actual JSON integer.
        ValueError: If the integer is below the allowed minimum.
    """
    if isinstance(value, bool) or not isinstance(value, int):
        msg = f"{field} must be a JSON integer, got {value!r}"
        raise TypeError(msg)
    if value < minimum:
        msg = f"{field} must be >= {minimum}, got {value!r}"
        raise ValueError(msg)
    return value


def _coerce_float(value: object) -> float | None:
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


def _coerce_int(value: object, field: str) -> int:
    """Coerce a JSON value to a non-negative bounded int.

    Args:
        value: Raw JSON value.
        field: Field name for error reporting.

    Returns:
        The int (``0`` when absent).

    Raises:
        TypeError: If the value is present but of a non-numeric type.
        ValueError: If the value is present but not an integer.
    """
    if value is None or (isinstance(value, str) and not value):
        return 0
    if isinstance(value, bool):
        msg = f"{field} must be an integer, got boolean"
        raise TypeError(msg)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not value.is_integer():
            msg = f"{field} must be an integer, got {value!r}"
            raise ValueError(msg)
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError as exc:
            msg = f"{field} must be an integer, got {value!r}"
            raise ValueError(msg) from exc
    msg = f"{field} must be an integer, got {value!r}"
    raise TypeError(msg)


def _coerce_bool(value: object) -> bool:
    """Coerce a JSON value to a bool, treating absence as ``False``.

    Args:
        value: Raw JSON value.

    Returns:
        The boolean (``False`` when absent or not a JSON boolean).
    """
    return value is True


def _required_finite_float(value: object, field: str) -> float:
    """Coerce a required persisted timestamp, rejecting non-finite values.

    Args:
        value: Raw JSON value.
        field: Field name for error reporting.

    Returns:
        The coerced finite float (``0.0`` when absent).

    Raises:
        ValueError: If the value is present but not a finite number.
    """
    if value is None or (isinstance(value, str) and not value):
        return 0.0
    result = _coerce_float(value)
    if result is None or not math.isfinite(result):
        msg = f"{field} must be a finite timestamp, got {value!r}"
        raise ValueError(msg)
    return result


def _optional_finite_float(value: object) -> float | None:
    """Coerce an optional persisted timestamp to a finite float or None.

    Non-finite values are rejected rather than silently accepted so
    corrupted durable state fails closed instead of leaking into memory.

    Args:
        value: Raw JSON value.

    Returns:
        The finite float, or ``None``.

    Raises:
        ValueError: If the value is non-finite.
    """
    result = _coerce_float(value)
    if result is not None and not math.isfinite(result):
        msg = f"timestamp must be finite, got {value!r}"
        raise ValueError(msg)
    return result


def _finite_or_zero(value: float) -> float:
    """Return the value when finite, else ``0.0`` for strict JSON safety."""
    return value if math.isfinite(value) else 0.0


def _finite_or_none(value: float | None) -> float | None:
    """Return the value when finite, else ``None`` for strict JSON safety."""
    if value is None or math.isfinite(value):
        return value
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
    validate_incarnation_token(health.worker_incarnation)
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


def publish_current_surfaces(incarnation: str) -> None:
    """Update both stable symlinks atomically; roll back on second failure.

    The health and log symlinks are updated in sequence.  If the second
    symlink fails, the first is rolled back to its previous target (or
    removed if it did not exist), so a partial publication never leaves the
    read surface in an inconsistent state.

    Args:
        incarnation: The confirmed incarnation whose files are current.

    Raises:
        OSError: If either symlink update fails (after rollback).
    """
    validate_incarnation_token(incarnation)
    health_symlink = health_current_path()
    health_symlink.parent.mkdir(parents=True, exist_ok=True)
    health_target = str(Path(HEALTH_DIR) / f"health-{incarnation}.json")
    old_health_target = _read_symlink_target(health_symlink)
    _atomic_symlink_update(health_symlink, health_target)
    try:
        log_symlink = worker_log_current_path()
        log_symlink.parent.mkdir(parents=True, exist_ok=True)
        log_target = str(Path(LOGS_DIR) / f"worker-{incarnation}.log")
        _atomic_symlink_update(log_symlink, log_target)
    except OSError:
        _rollback_symlink(health_symlink, old_health_target)
        raise


def _read_symlink_target(symlink: Path) -> str | None:
    """Read the current target of a symlink, or ``None`` if absent.

    Args:
        symlink: The symlink path.

    Returns:
        The symlink target name, or ``None``.
    """
    try:
        return str(symlink.readlink())
    except OSError:
        return None


def _rollback_symlink(symlink: Path, old_target: str | None) -> None:
    """Roll back a symlink to its previous target or remove it.

    Args:
        symlink: The symlink to roll back.
        old_target: The previous target, or ``None`` to remove.
    """
    if old_target is None:
        symlink.unlink(missing_ok=True)
    else:
        _atomic_symlink_update(symlink, old_target)


def _atomic_symlink_update(symlink: Path, target_name: str) -> None:
    """Atomically replace a symlink to point at a new target name.

    Uses a temporary symlink + ``Path.replace()`` so a concurrent reader
    never sees a partial update or a dangling intermediate.

    Args:
        symlink: The stable symlink path.
        target_name: The new target filename (relative to the symlink's parent).
    """
    tmp_symlink = symlink.with_name(f"{symlink.name}.tmp")
    tmp_symlink.unlink(missing_ok=True)
    tmp_symlink.symlink_to(target_name)
    tmp_symlink.replace(symlink)


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
    try:
        return WorkerHealth.from_dict(data)
    except (TypeError, ValueError, KeyError):
        return None


# ---------------------------------------------------------------------------
# Bounded disk: prune old incarnation artifacts
# ---------------------------------------------------------------------------


def prune_old_incarnation_artifacts(current_token: str) -> None:
    """Best-effort remove health/log files from older incarnations.

    Retains the current incarnation's ``health-{token}.json``,
    ``worker-{token}.log``, and its rotation backups (``.1``, ``.2``, ...).
    Everything else matching the known filename patterns in the health and
    logs directories is removed.  Failures are logged as warnings and never
    undo the current stable surfaces.

    Args:
        current_token: The confirmed incarnation token to keep.
    """
    _prune_dir(_health_dir(), f"health-{current_token}.json", "health-*.json")
    _prune_dir(_logs_dir(), f"worker-{current_token}.log", "worker-*.log*")


def _prune_dir(directory: Path, keep_prefix: str, pattern: str) -> None:
    """Remove files matching ``pattern`` that do not start with ``keep_prefix``.

    Args:
        directory: The directory to scan.
        keep_prefix: Filename prefix to retain (e.g. ``health-abc.json``).
        pattern: Glob pattern for candidate files.
    """
    if not directory.is_dir():
        return
    for entry in directory.glob(pattern):
        if not entry.is_file():
            continue
        if entry.name == keep_prefix or entry.name.startswith(keep_prefix + "."):
            continue
        try:
            entry.unlink()
        except OSError:
            LOGGER.warning("could not prune old artifact %s", entry, exc_info=True)


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


def _pinned_snapshot_process_live(snapshot: WorkerHealth) -> tuple[bool, str]:
    """Validate one worker snapshot against one pinned kernel process.

    Returns:
        ``(live, reason)`` for the exact pinned process identity.
    """
    pid = snapshot.pid
    try:
        pidfd = _open_pidfd(pid)
    except OSError:
        return False, f"PID {pid} could not be pinned"
    try:
        current_ticks = proc_start_ticks(pid)
        if current_ticks is None:
            return False, f"PID {pid} is not alive"
        if current_ticks != snapshot.start_time_ticks:
            return False, (
                f"PID {pid} start time {current_ticks} != expected {snapshot.start_time_ticks}"
            )
        if not _process_is_live(pid):
            return False, f"PID {pid} is a zombie or dead"
        try:
            _pidfd_send_signal(pidfd, 0)
        except OSError:
            return False, f"PID {pid} disappeared during liveness validation"
        return snapshot.alive, "ok"
    finally:
        os.close(pidfd)


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
    if not math.isfinite(snapshot.published_at):
        return EffectiveHealth(
            snapshot=snapshot,
            live=False,
            stale=True,
            reason="non-finite published_at in snapshot",
        )
    now = time.time()
    age = now - snapshot.published_at
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
        live, reason = _pinned_snapshot_process_live(snapshot)
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
    """Configure the ``lubko.worker`` logger with a per-incarnation handler.

    The handler writes to a per-incarnation log file so two overlapping
    workers never share a log file.  The handler is attached to the
    ``lubko.worker`` logger (not the root logger), so unrelated library
    loggers cannot leak sentinel secrets or operational noise into the
    worker log.

    This is the **only** place a ``RotatingFileHandler`` for any worker log
    is created in the worker process: the parent process (supervisor,
    lifecycle scripts) never opens the worker log, so there is exactly one
    writer/owner per incarnation.

    Args:
        incarnation: Worker incarnation identifier.

    Returns:
        The configured ``lubko.worker`` logger.
    """
    validate_incarnation_token(incarnation)
    log_path = worker_log_incarnation_path(incarnation)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    handler = RotatingFileHandler(
        str(log_path),
        maxBytes=WORKER_LOG_MAX_BYTES,
        backupCount=WORKER_LOG_BACKUP_COUNT,
        encoding="utf-8",
    )
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    logger = logging.getLogger("lubko.worker")
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    return logger


def install_worker_exception_hooks() -> None:
    """Route unhandled exceptions into the bounded operational log.

    Installs a custom ``sys.excepthook`` so unhandled failures are visible
    in the worker log rather than silently lost.
    """
    default_hook = sys.excepthook
    worker_logger = logging.getLogger("lubko.worker")

    def _log_exception(
        exc_type: type[BaseException],
        exc_value: BaseException,
        exc_traceback: TracebackType | None,
    ) -> None:
        worker_logger.error("Unhandled exception: %s", exc_value)
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
