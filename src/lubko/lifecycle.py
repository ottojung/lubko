"""Deterministic self-deployment and lifecycle management for the Lubko worker.

The ``lubko-deploy`` command validates a repository checkout, replaces the
previously maintained worker with a freshly started one, and records enough
exact process identity to stop or replace that worker later without any broad
``pkill``/``killall``/process-name matching.

Per-user state lives under ``$XDG_STATE_HOME/lubko`` (default
``$HOME/.local/state/lubko``):

- ``worker/meta.json`` — lifecycle metadata written atomically;
- ``worker/worker.log`` — appended stdout/stderr of the maintained worker;
- ``worker/deploy.log`` — deployment event log;
- ``worker/.deploy.lock`` — flock-protected serialization of deployments;
- ``toolchain.json`` — versioned record of the maintained ``uv`` executable.

The ``uv`` executable used to run validation and to spawn the worker is
resolved with a strict precedence: an explicit ``--uv``, then ``uv`` on PATH,
then the executable recorded in ``toolchain.json``; see :mod:`lubko.toolchain`.
"""

from __future__ import annotations

import argparse
import fcntl
import json
import logging
import math
import os
import secrets
import signal
import socket
import stat
import subprocess
import sys
import time
from contextlib import contextmanager, suppress
from dataclasses import dataclass, replace
from pathlib import Path
from typing import TYPE_CHECKING, Final

import psycopg
from psycopg.rows import tuple_row

from lubko import cli, lifecycle_state, protocol, startup_contract, supervise, toolchain
from lubko._exact_signal import open_pidfd as _open_exact_pidfd
from lubko._exact_signal import pidfd_send_signal, process_pgrp
from lubko.config import (
    load_database_config,
    load_worker_protocol_range,
    load_worker_server,
)
from lubko.durable import DurabilityError, remove_durable, write_json_durable
from lubko.protocol_versioning import negotiate_submission_version
from lubko.state import rollback_state_path, state_root
from lubko.toolchain import UvResolutionError, resolve_uv
from lubko.worker import (
    DEFAULT_CANCEL_GRACE_SECONDS,
    JOB_ID_ENV,
    delete_job_and_chunks,
    drain_sentinel_matches,
    request_cancel,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Sequence
    from uuid import UUID

    from lubko.worker import JobsConnection

LOGGER = logging.getLogger(__name__)

EXIT_OK: Final = 0
EXIT_ERROR: Final = 1

SCHEMA_VERSION: Final = 1

STATE_UNMANAGED: Final = "unmanaged"
STATE_RUNNING: Final = "running"
STATE_STOPPED: Final = "stopped"
STATE_PENDING: Final = "pending"

LIFECYCLE_MARKER_VAR: Final = "LUBKO_LIFECYCLE_TOKEN"

DEFAULT_STOP_GRACE_SECONDS: Final = 5.0
#: Bounded finalization overhead the outer stop must grant the worker before it
#: may treat a still-alive worker as wedged and issue an emergency SIGKILL. The
#: outer wait is ``max(grace, cancel_grace + this)`` so the two equal/default
#: timers can never race worker cleanup.
STOP_DRAIN_OVERHEAD_SLACK_SECONDS: Final = 2.0
DEFAULT_POSTGRES_TIMEOUT_SECONDS: Final = 5.0
DEFAULT_LOCK_TIMEOUT_SECONDS: Final = 30.0
DEFAULT_VALIDATION_TIMEOUT_SECONDS: Final = 1200.0
DEFAULT_GIT_TIMEOUT_SECONDS: Final = 10.0
DEFAULT_CLI_TIMEOUT_SECONDS: Final = cli.DEFAULT_BUILD_TIMEOUT_SECONDS
DEFAULT_REPAIR_PROBE_TIMEOUT_SECONDS: Final = 60.0
DEFAULT_RECOVER_PREFLIGHT_SECONDS: Final = 3.0
CLI_ACTIVATION_ATTEMPTS: Final = 5
CLI_ACTIVATION_RETRY_SECONDS: Final = 1.0
LOCK_POLL_INTERVAL_SECONDS: Final = 0.1
SESSION_ESTABLISH_TIMEOUT_SECONDS: Final = 5.0
SESSION_WAIT_INTERVAL_SECONDS: Final = 0.01
UV_HTTP_TIMEOUT: Final = "30"

STAT_MIN_FIELDS: Final = 20
STAT_STARTTIME_FIELD_INDEX: Final = 19
STAT_STATE_FIELD_INDEX: Final = 0
STAT_PPID_FIELD_INDEX: Final = 1
STAT_PPID_MIN_FIELDS: Final = 2

VALIDATION_STEPS: Final = (
    ("sync",),
    ("run", "ruff", "format", "--check", "."),
    ("run", "ruff", "check", "."),
    ("run", "mypy", "."),
    ("run", "pytest"),
)

UNMANAGED_WORKER_MESSAGE: Final = (
    "no maintained worker metadata exists; the currently running worker is an "
    "unmanaged legacy daemon with no recorded identity"
)


class LockTimeoutError(RuntimeError):
    """Raised when the deployment lock cannot be acquired within the timeout."""


class DeployAbortedError(RuntimeError):
    """Raised when a deployment aborts and the current worker stays untouched."""


@dataclass(frozen=True, slots=True)
class ProcessIdentity:
    """Exact identity of a live process, immune to PID reuse."""

    pid: int
    pgid: int
    sid: int
    start_time_ticks: int


@dataclass(frozen=True, slots=True)
class WorkerMeta:
    """Persisted identity and deployment metadata of the maintained worker."""

    schema_version: int
    state: str
    pid: int | None
    pgid: int | None
    sid: int | None
    start_time_ticks: int | None
    token: str | None
    repo: str
    git_commit: str | None
    worker_id: str | None
    log_path: str
    started_at: float | None
    stopped_at: float | None

    def to_dict(self) -> dict[str, object]:
        """Serialize the metadata for storage.

        Returns:
            A JSON-serializable mapping.
        """
        return {
            "schema_version": self.schema_version,
            "state": self.state,
            "pid": self.pid,
            "pgid": self.pgid,
            "sid": self.sid,
            "start_time_ticks": self.start_time_ticks,
            "token": self.token,
            "repo": self.repo,
            "git_commit": self.git_commit,
            "worker_id": self.worker_id,
            "log_path": self.log_path,
            "started_at": self.started_at,
            "stopped_at": self.stopped_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> WorkerMeta:
        """Rebuild metadata from a stored mapping.

        Args:
            data: Mapping produced by :meth:`to_dict`.

        Returns:
            The reconstructed metadata.

        Raises:
            ValueError: If required metadata is missing or outside its valid domain.
        """
        schema_version = _required_meta_int(data, "schema_version", minimum=1)
        if schema_version != SCHEMA_VERSION:
            msg = f"unsupported worker metadata schema version {schema_version}"
            raise ValueError(msg)
        return cls(
            schema_version=schema_version,
            state=_meta_worker_state(data),
            pid=_meta_optional_int(data, "pid", minimum=1),
            pgid=_meta_optional_int(data, "pgid", minimum=1),
            sid=_meta_optional_int(data, "sid", minimum=0),
            start_time_ticks=_meta_optional_int(data, "start_time_ticks", minimum=1),
            token=_meta_optional_string(data, "token"),
            repo=_meta_string(data, "repo", default=""),
            git_commit=_meta_optional_string(data, "git_commit"),
            worker_id=_meta_optional_string(data, "worker_id"),
            log_path=_meta_string(data, "log_path", default=""),
            started_at=_meta_optional_finite_float(data, "started_at"),
            stopped_at=_meta_optional_finite_float(data, "stopped_at"),
        )


@dataclass(frozen=True, slots=True)
class ValidationReport:
    """Outcome of running the repository validation commands."""

    ok: bool
    detail: str


@dataclass(frozen=True, slots=True)
class DeployOptions:
    """Tunable inputs for a single deployment."""

    repo: Path
    uv_path: str
    bootstrap: bool
    stop_grace_seconds: float
    postgres_timeout_seconds: float
    lock_timeout_seconds: float
    validation_timeout_seconds: float
    git_timeout_seconds: float
    cli_timeout_seconds: float
    probe_timeout_seconds: float = DEFAULT_REPAIR_PROBE_TIMEOUT_SECONDS
    direct_spawn: bool = False


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------


def worker_state_dir() -> Path:
    """Return the directory holding worker lifecycle state.

    Returns:
        The per-user worker state directory.
    """
    return state_root() / "worker"


def meta_path() -> Path:
    """Return the path to the worker lifecycle metadata file.

    Returns:
        The metadata path.
    """
    return worker_state_dir() / "meta.json"


def worker_log_path(incarnation: str | None = None) -> Path:
    """Return the log path for a worker incarnation.

    When ``incarnation`` is provided, returns the per-incarnation file path
    so metadata advertises a truthful single-writer log location.  When
    ``incarnation`` is ``None``, returns the stable ``worker.log`` path
    (a supervisor-owned symlink target or a legacy direct-logging path).

    Args:
        incarnation: Worker incarnation identifier, or ``None``.

    Returns:
        The worker log path.
    """
    if incarnation is not None:
        return worker_state_dir() / "logs" / f"worker-{incarnation}.log"
    return worker_state_dir() / "worker.log"


def deploy_log_path() -> Path:
    """Return the path of the deployment event log.

    Returns:
        The deploy log path.
    """
    return worker_state_dir() / "deploy.log"


def lock_path() -> Path:
    """Return the path of the deployment lock file.

    Returns:
        The lock path.
    """
    return worker_state_dir() / ".deploy.lock"


# ---------------------------------------------------------------------------
# Process identity
# ---------------------------------------------------------------------------


def proc_start_ticks(pid: int) -> int | None:
    """Return a process start time in clock ticks, or ``None`` if unknown.

    The start time is unique per process on a boot and survives PID reuse, so
    it anchors identity checks.

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


def process_is_zombie(pid: int) -> bool:
    """Return whether a process is a zombie or dead.

    Args:
        pid: Process ID to inspect.

    Returns:
        ``True`` when the process is zombie, dead, or unreadable.
    """
    try:
        stat = (Path("/proc") / str(pid) / "stat").read_bytes()
    except OSError:
        return True
    close_paren = stat.rfind(b")")
    if close_paren == -1:
        return True
    fields = stat[close_paren + 2 :].split()
    if not fields:
        return True
    return fields[STAT_STATE_FIELD_INDEX] in {b"Z", b"X"}


def process_identity(pid: int) -> ProcessIdentity | None:
    """Return the exact identity of a live process, or ``None``.

    Args:
        pid: Process ID to inspect.

    Returns:
        The identity, or ``None`` if the process is dead, zombie, or unknown.
    """
    if process_is_zombie(pid):
        return None
    start_time_ticks = proc_start_ticks(pid)
    if start_time_ticks is None:
        return None
    try:
        pgid = os.getpgid(pid)
        sid = os.getsid(pid)
    except ProcessLookupError:
        return None
    return ProcessIdentity(pid=pid, pgid=pgid, sid=sid, start_time_ticks=start_time_ticks)


def process_has_token(pid: int, token: str) -> bool:
    """Return whether a process environment carries the lifecycle token.

    ``/proc/<pid>/environ`` is NUL-separated; a naive substring check on the
    raw bytes could falsely accept a *different* environment variable whose
    value or key happens to contain ``LUBKO_LIFECYCLE_TOKEN=<token>`` as a
    prefix or infix (e.g. ``X_LUBKO_LIFECYCLE_TOKEN=<token>``).  Instead we
    split on NUL and require exact ``KEY=VALUE`` equality on one entry.

    Args:
        pid: Process ID to inspect.
        token: Expected lifecycle token.

    Returns:
        ``True`` when the token marker is present in the process environment.
    """
    expected = f"{LIFECYCLE_MARKER_VAR}={token}".encode()
    try:
        environ = (Path("/proc") / str(pid) / "environ").read_bytes()
    except OSError:
        return False
    return any(entry == expected for entry in environ.split(b"\0"))


def identity_matches(meta: WorkerMeta, identity: ProcessIdentity) -> bool:
    """Return whether a live identity corresponds to the recorded metadata.

    Args:
        meta: Recorded worker metadata.
        identity: Observed live process identity.

    Returns:
        ``True`` when the process group, session, and start time all match.
    """
    if meta.pid is None:
        return False
    if meta.start_time_ticks is None or meta.start_time_ticks != identity.start_time_ticks:
        return False
    if meta.pgid is not None and meta.pgid != identity.pgid:
        return False
    if meta.sid is not None and meta.sid != identity.sid:
        return False
    return meta.pid == identity.pid


def worker_alive(meta: WorkerMeta) -> bool:
    """Return whether the recorded worker is really alive and is really ours.

    A PID alone is never trusted: the start time, process group, session, and
    lifecycle token must all match so a recycled PID can never be mistaken for
    the maintained worker.

    Args:
        meta: Recorded worker metadata.

    Returns:
        ``True`` when a live process matches every recorded identity field.
    """
    if meta.pid is None:
        return False
    identity = process_identity(meta.pid)
    if identity is None:
        return False
    if not identity_matches(meta, identity):
        return False
    return meta.token is None or process_has_token(meta.pid, meta.token)


# ---------------------------------------------------------------------------
# Metadata
# ---------------------------------------------------------------------------


def _required_meta_int(
    data: dict[str, object],
    field: str,
    *,
    minimum: int,
) -> int:
    """Return one required exact JSON integer from maintained metadata.

    Raises:
        TypeError: If the field has the wrong JSON type.
        ValueError: If the field is missing or below its valid minimum.
    """
    if field not in data:
        msg = f"worker metadata field {field!r} is missing"
        raise ValueError(msg)
    value = data[field]
    if isinstance(value, bool) or not isinstance(value, int):
        msg = f"worker metadata field {field!r} must be an integer"
        raise TypeError(msg)
    if value < minimum:
        msg = f"worker metadata field {field!r} must be >= {minimum}"
        raise ValueError(msg)
    return value


def _meta_optional_int(
    data: dict[str, object],
    field: str,
    *,
    minimum: int,
) -> int | None:
    """Return an optional exact JSON integer from maintained metadata."""
    if field not in data or data[field] is None:
        return None
    return _required_meta_int(data, field, minimum=minimum)


def _meta_string(data: dict[str, object], field: str, *, default: str) -> str:
    """Return a string field, preserving only genuine-absence compatibility.

    Raises:
        TypeError: If a present field is not a JSON string.
    """
    if field not in data:
        return default
    value = data[field]
    if not isinstance(value, str):
        msg = f"worker metadata field {field!r} must be a string"
        raise TypeError(msg)
    return value


def _meta_worker_state(data: dict[str, object]) -> str:
    """Return a supported persisted maintained-worker lifecycle state.

    Raises:
        ValueError: If the persisted state is outside the supported domain.
    """
    state = _meta_string(data, "state", default=STATE_STOPPED)
    if state not in {STATE_RUNNING, STATE_STOPPED}:
        msg = f"unsupported worker metadata state {state!r}"
        raise ValueError(msg)
    return state


def _meta_optional_string(data: dict[str, object], field: str) -> str | None:
    """Return an optional string, rejecting malformed present values."""
    if field not in data or data[field] is None:
        return None
    return _meta_string(data, field, default="")


def _meta_optional_finite_float(data: dict[str, object], field: str) -> float | None:
    """Return an optional finite JSON number from maintained metadata.

    Raises:
        TypeError: If a present field is not a JSON number.
        ValueError: If a present number is not finite.
    """
    if field not in data or data[field] is None:
        return None
    value = data[field]
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        msg = f"worker metadata field {field!r} must be a finite number"
        raise TypeError(msg)
    try:
        result = float(value)
    except OverflowError as exc:
        msg = f"worker metadata field {field!r} must be a finite number"
        raise ValueError(msg) from exc
    if not math.isfinite(result):
        msg = f"worker metadata field {field!r} must be a finite number"
        raise ValueError(msg)
    if result < 0:
        msg = f"worker metadata field {field!r} must be >= 0"
        raise ValueError(msg)
    return result


def _optional_int(value: object | None) -> int | None:
    """Coerce an optional JSON value to an integer.

    Args:
        value: Value loaded from JSON.

    Returns:
        The integer, or ``None`` when missing or non-integer.
    """
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None


def _optional_float(value: object | None) -> float | None:
    """Coerce an optional JSON value to a float.

    Args:
        value: Value loaded from JSON.

    Returns:
        The float, or ``None`` when missing or non-numeric.
    """
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def _optional_str(value: object | None) -> str | None:
    """Coerce an optional JSON value to a string.

    Args:
        value: Value loaded from JSON.

    Returns:
        The string, or ``None`` when missing or not a string.
    """
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, str):
        return value
    return None


def write_meta(meta: WorkerMeta) -> None:
    """Crash-durably persist worker lifecycle metadata.

    ``meta.json`` is recovery authority for the maintained worker: it records
    the live worker identity the supervisor reconciles against, so the write
    must be confirmed durable before any dependent lifecycle action proceeds.

    Args:
        meta: Worker metadata to persist.

    Note:
        Fails closed: the write raises :class:`DurabilityError` from
        :func:`lubko.durable.write_json_durable` when it cannot be confirmed
        durable, so callers must not advance a dependent action.
    """
    lifecycle_state.failpoint(lifecycle_state.FAILPOINT_METADATA_PUBLICATION)
    write_json_durable(meta_path(), meta.to_dict())


class WorkerMetadataError(RuntimeError):
    """Raised when present maintained-worker metadata cannot be trusted."""


def read_meta_strict() -> WorkerMeta | None:
    """Load maintained-worker metadata, distinguishing absence from corruption.

    Returns:
        Parsed metadata, or ``None`` only when the artifact is genuinely absent.

    Raises:
        WorkerMetadataError: If a present metadata artifact is unreadable or malformed.
    """
    path = meta_path()
    try:
        raw = path.read_text()
    except FileNotFoundError as exc:
        if os.path.lexists(path):
            msg = "maintained-worker metadata is present but unreadable"
            raise WorkerMetadataError(msg) from exc
        return None
    except OSError as exc:
        msg = f"cannot read maintained-worker metadata: {exc}"
        raise WorkerMetadataError(msg) from exc
    try:
        data = json.loads(raw)
    except ValueError as exc:
        msg = "maintained-worker metadata is not valid JSON"
        raise WorkerMetadataError(msg) from exc
    if not isinstance(data, dict):
        msg = "maintained-worker metadata must be a JSON object"
        raise WorkerMetadataError(msg)
    try:
        return WorkerMeta.from_dict(data)
    except (KeyError, TypeError, ValueError) as exc:
        msg = "maintained-worker metadata is malformed"
        raise WorkerMetadataError(msg) from exc


def read_meta() -> WorkerMeta | None:
    """Load worker lifecycle metadata, tolerating absence and corruption.

    Returns:
        The stored metadata, or ``None`` when metadata is absent or untrustworthy.
    """
    try:
        return read_meta_strict()
    except WorkerMetadataError:
        return None


def worker_state(meta: WorkerMeta | None) -> str:
    """Derive the effective worker state from metadata and liveness.

    Args:
        meta: Recorded worker metadata, or ``None``.

    Returns:
        ``unmanaged``, ``running``, or ``stopped``.
    """
    if meta is None:
        return STATE_UNMANAGED
    if worker_alive(meta):
        return STATE_RUNNING
    return STATE_STOPPED


# ---------------------------------------------------------------------------
# Locking
# ---------------------------------------------------------------------------


@contextmanager
def deploy_lock(timeout_seconds: float) -> Iterator[None]:
    """Hold an exclusive deployment lock, bounded by a timeout.

    Two concurrent deployments cannot race: the second waits up to
    ``timeout_seconds`` and then fails cleanly.

    Args:
        timeout_seconds: Maximum seconds to wait for the lock.

    Yields:
        Nothing while the lock is held.

    Raises:
        LockTimeoutError: If the lock cannot be acquired within the timeout.
    """
    path = lock_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+") as handle:
        deadline = time.monotonic() + timeout_seconds
        while True:
            try:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    msg = "timed out waiting for the deployment lock"
                    raise LockTimeoutError(msg) from None
                time.sleep(LOCK_POLL_INTERVAL_SECONDS)
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def append_deploy_log(message: str) -> None:
    """Append a timestamped event to the deployment log.

    Args:
        message: Event description.
    """
    with suppress(OSError):
        path = deploy_log_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(f"{time.strftime('%Y-%m-%dT%H:%M:%S')} {message}\n")


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def _run_capture(
    repo: Path,
    argv: Sequence[str],
    env: dict[str, str],
    timeout_seconds: float,
) -> subprocess.CompletedProcess[str]:
    """Run a command, capturing output, with a bounded timeout.

    Args:
        repo: Working directory for the command.
        argv: Command and arguments.
        env: Environment for the command.
        timeout_seconds: Bounded timeout.

    Returns:
        The completed process result.
    """
    return subprocess.run(
        list(argv),
        cwd=repo,
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
        check=False,
    )


def run_validation(repo: Path, uv_path: str, timeout_seconds: float) -> ValidationReport:
    """Run ``uv sync`` and the repository-required validation commands.

    Runs ``uv sync`` followed by ``ruff format --check``, ``ruff check``,
    ``mypy``, and ``pytest`` exactly as the repository requires, with a bounded
    timeout per command and a bounded network timeout for ``uv sync``.

    Args:
        repo: Repository checkout to validate.
        uv_path: Path to the ``uv`` executable.
        timeout_seconds: Bounded timeout per validation command.

    Returns:
        A report of the first failing command, or a passing report.
    """
    env = dict(os.environ)
    env.setdefault("UV_HTTP_TIMEOUT", UV_HTTP_TIMEOUT)
    for step in VALIDATION_STEPS:
        argv = (uv_path, *step)
        label = " ".join(argv)
        try:
            proc = _run_capture(repo, argv, env, timeout_seconds)
        except subprocess.TimeoutExpired:
            msg = f"{label} timed out after {timeout_seconds}s"
            return ValidationReport(ok=False, detail=msg)
        except OSError as exc:
            return ValidationReport(ok=False, detail=f"{label} could not be run: {exc}")
        if proc.returncode != 0:
            output = proc.stderr or proc.stdout or ""
            lines = [line for line in output.splitlines() if line]
            detail = "\n".join(lines[-10:]) or f"exit code {proc.returncode}"
            return ValidationReport(ok=False, detail=f"{label} failed:\n{detail}")
    return ValidationReport(ok=True, detail="")


def git_commit(repo: Path, timeout_seconds: float) -> str | None:
    """Return the full git commit of the checkout, read-only.

    This only reads git state; it never pulls, resets, or discards local
    changes.

    Args:
        repo: Repository checkout to inspect.
        timeout_seconds: Bounded timeout.

    Returns:
        The full commit hash, or ``None`` when unavailable.
    """
    try:
        proc = _run_capture(repo, ("git", "rev-parse", "HEAD"), dict(os.environ), timeout_seconds)
    except (OSError, subprocess.TimeoutExpired):
        return None
    if proc.returncode != 0:
        return None
    commit = proc.stdout.strip()
    return commit or None


def require_clean_checkout(repo: Path, timeout_seconds: float) -> bool:
    """Return whether a checkout has no uncommitted working-tree changes.

    ``lubko-deploy deploy`` runs the worker from the checkout and builds the
    maintained CLIs from the committed HEAD, so the checkout must be clean for
    the worker and the CLIs to really run the same exact code. Any tracked
    modification, staged change, or untracked file (outside ``.gitignore``)
    counts as dirty.

    Args:
        repo: Repository checkout to inspect.
        timeout_seconds: Bounded timeout.

    Returns:
        ``True`` when the checkout is clean, ``False`` when dirty or unreadable.
    """
    try:
        proc = _run_capture(
            repo, ("git", "status", "--porcelain"), dict(os.environ), timeout_seconds
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    if proc.returncode != 0:
        return False
    return not proc.stdout


# ---------------------------------------------------------------------------
# PostgreSQL readiness
# ---------------------------------------------------------------------------


def check_postgres(timeout_seconds: float) -> bool:
    """Verify PostgreSQL is reachable using the worker's file-based config.

    Connection settings are loaded from the same permission-restricted
    database configuration file the worker reads, with a bounded connect
    timeout. Connection details are never printed.

    Args:
        timeout_seconds: Bounded connect timeout.

    Returns:
        ``True`` when a trivial query succeeds.
    """
    try:
        config = load_database_config()
    except (OSError, ValueError):
        return False
    try:
        with (
            psycopg.connect(
                config.conninfo(),
                connect_timeout=int(timeout_seconds),
                row_factory=tuple_row,
            ) as conn,
            conn.cursor() as cursor,
        ):
            cursor.execute("SELECT 1")
            return cursor.fetchone() is not None
    except (psycopg.Error, OSError):
        return False


# ---------------------------------------------------------------------------
# Worker start and stop
# ---------------------------------------------------------------------------


def _worker_command(uv_path: str) -> list[str]:
    """Return the command used to start the worker.

    Args:
        uv_path: Path to the ``uv`` executable.

    Returns:
        The worker argv.
    """
    return [uv_path, "run", "lubko-worker"]


def spawn_worker(
    repo: Path,
    uv_path: str,
    log_path: Path,
    env: dict[str, str],
) -> subprocess.Popen[bytes]:
    """Start the worker detached from the invoking shell.

    The worker becomes its own session and process group leader, with stdin
    disconnected and both output streams directed to ``/dev/null``.  The
    worker owns its own ``RotatingFileHandler`` for ``worker.log`` so there
    is exactly one writer; the parent never opens the worker log file.

    Args:
        repo: Repository checkout to run the worker from.
        uv_path: Path to the ``uv`` executable.
        log_path: Stable path of the worker log (unused, kept for interface
            compatibility; the worker owns its own log).
        env: Environment for the worker, including the lifecycle token.

    Returns:
        The started worker process.
    """
    del log_path
    lifecycle_state.failpoint("popen")
    return subprocess.Popen(
        _worker_command(uv_path),
        cwd=repo,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        close_fds=True,
        env=env,
    )


def _credential_environment_variable(name: str) -> bool:
    """Return whether an environment variable may carry credentials.

    Args:
        name: Environment variable name.

    Returns:
        ``True`` for libpq ``PG*`` variables, ``DATABASE_URL``, and any
        variable whose name suggests a password, secret, or credential.
    """
    if name.startswith("PG") or name == "DATABASE_URL":
        return True
    lowered = name.lower()
    return any(token in lowered for token in ("password", "secret", "credential"))


def worker_env(token: str) -> dict[str, str]:
    """Build the worker environment with the lifecycle token marker.

    Database credentials are deliberately not inherited: libpq ``PG*``
    variables, connection strings, and other credential-bearing variables are
    stripped so the worker never exposes secrets through its environment. The
    worker instead reads its connection settings from the restricted database
    configuration file.

    Args:
        token: Unique lifecycle token for this deployment.

    Returns:
        The worker environment.
    """
    env = {
        name: value
        for name, value in os.environ.items()
        if not _credential_environment_variable(name)
    }
    env[LIFECYCLE_MARKER_VAR] = token
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    return env


def _wait_for_identity(pid: int) -> ProcessIdentity | None:
    """Wait until a spawned process establishes its session and group.

    On timeout the *last observed* identity is returned even when it does not
    yet satisfy ``pgid == pid == sid``: that observation is the only exact
    pre-transition ownership anchor (PID plus start-time ticks) available for
    converging a child whose PGID/SID transitions after the deadline, and it
    is lost if the timeout collapses into ``None``. ``None`` therefore means
    only that the process was never observable (already dead). Callers must
    explicitly reject a returned identity that is not a private session and
    hand the observed identity to :func:`_converge_unproven_spawn`.

    Args:
        pid: Process ID of the spawned worker.

    Returns:
        The exact private-session identity once observed, otherwise the last
        observed identity at the timeout, or ``None`` if the process died
        before any identity could be read.
    """
    deadline = time.monotonic() + SESSION_ESTABLISH_TIMEOUT_SECONDS
    last_observed: ProcessIdentity | None = None
    while True:
        identity = process_identity(pid)
        if identity is not None and identity.pgid == pid and identity.sid == pid:
            return identity
        if identity is not None:
            # Keep the newest non-private observation: the final poll before
            # the deadline can transiently return None (for example when the
            # /proc entry is momentarily unreadable) without discarding the
            # exact startup anchor.
            last_observed = identity
        if time.monotonic() >= deadline:
            return last_observed
        time.sleep(SESSION_WAIT_INTERVAL_SECONDS)


def _signal_pinned_anchor(pin: int, pid: int, anchor: ProcessIdentity, sig: int) -> None:
    """Deliver ``sig`` to the pinned process only while its anchor still holds.

    The pinned descriptor addresses the exact kernel process instance, so
    delivery itself can never hit a recycled PID. The preceding re-proof is a
    safety gate in the direction of refusing to signal: the occupant observed
    through ``/proc`` must still be the exact anchored instance (same PID and
    same start-time ticks; PGID/SID may legitimately have transitioned), so a
    recycled numeric occupant is never signalled.

    Args:
        pin: pidfd pinning the exact process instance.
        pid: Numeric PID, used only for the ``/proc`` identity re-proof.
        anchor: The exact identity observed for this child earlier.
        sig: Signal to deliver.
    """
    observed = process_identity(pid)
    if observed is None or observed.pid != anchor.pid:
        LOGGER.error("pinned process %d is no longer observable; not signalling", pid)
        return
    if observed.start_time_ticks != anchor.start_time_ticks:
        LOGGER.error(
            "pinned process %d no longer matches its anchored start-time ticks; "
            "it is a different process instance and is never signalled",
            pid,
        )
        return
    with suppress(OSError):
        pidfd_send_signal(pin, sig)


def _converge_unproven_spawn(
    proc: subprocess.Popen[bytes],
    grace_seconds: float,
    anchor: ProcessIdentity | None,
) -> None:
    """Terminate and reap a spawned worker whose identity was never proven.

    ``_wait_for_identity`` returns a non-private identity (or ``None``) for a
    child that is still alive when the identity deadline expires. Such a child
    is never forgotten: while it is still this deployer's direct ``Popen``
    child — which is itself the lifecycle authority over it — it is converged
    exactly. When the caller observed a pre-timeout identity, that observation
    is the ownership anchor: the numeric PID is pinned with a pidfd, the
    occupant is re-proved under the pin against the anchor's PID and
    start-time ticks (PGID/SID transitions are tolerated), and TERM/KILL
    escalation is delivered only through ``pidfd_send_signal`` on that pin, so
    neither a broad group nor a recycled numeric PID can ever absorb a signal
    — including when the original child exits between the anchor observation
    and the pin and its PID is reused. Without an anchor, or when the anchor
    cannot be re-proved under the pin, nothing is signalled at all and this
    function fails closed by positively reaping the original direct child
    before returning, so no unresolved child can coexist with another worker.

    Args:
        proc: The direct ``Popen`` handle of the spawned child.
        grace_seconds: Grace period before the emergency force-kill.
        anchor: The exact identity observed for the child, or ``None``.
    """
    if proc.poll() is not None:
        # Already exited before its identity was established: an ordinary
        # retryable failure with nothing left to converge.
        return
    LOGGER.error(
        "worker pid %d is live without an acceptable identity; converging it",
        proc.pid,
    )
    grace = max(grace_seconds, 0.0)
    if anchor is not None:
        try:
            pin = _open_exact_pidfd(anchor.pid)
        except (OSError, AttributeError):
            LOGGER.exception(
                "worker pid %d could not be pinned; no signal can be authorized", anchor.pid
            )
            pin = None
        if pin is not None:
            try:
                with suppress(OSError):
                    _signal_pinned_anchor(pin, anchor.pid, anchor, signal.SIGTERM)
                try:
                    proc.wait(timeout=grace)
                except subprocess.TimeoutExpired:
                    pass
                else:
                    return
                with suppress(OSError):
                    _signal_pinned_anchor(pin, anchor.pid, anchor, signal.SIGKILL)
            finally:
                with suppress(OSError):
                    os.close(pin)
    try:
        proc.wait(timeout=grace)
    except subprocess.TimeoutExpired:
        LOGGER.exception(
            "worker pid %d cannot be converged by exact identity; "
            "failing closed until it provably exits",
            proc.pid,
        )
        proc.wait()


def _live_group_member_pids(pgid: int) -> list[int]:
    """Return the current numeric PIDs of every live process in ``pgid``.

    The snapshot is a *candidate* list only: membership is re-proven under a
    pidfd pin before any signal is delivered (see :func:`_signal_exact_group`),
    so a numeric PID that was recycled between the snapshot and delivery can
    never absorb a signal.

    Args:
        pgid: Process group whose members to enumerate.

    Returns:
        Candidate member PIDs, in /proc order.
    """
    try:
        entries = list(Path("/proc").iterdir())
    except OSError:
        return []
    return [
        int(entry.name)
        for entry in entries
        if entry.name.isdigit() and process_pgrp(int(entry.name)) == pgid
    ]


def _signal_exact_group(pgid: int, sig: int, token: str | None) -> bool:
    """Deliver ``sig`` to every provably-owned current member of the group.

    A holding pidfd does NOT keep a numeric PID/PGID reserved: the kernel
    frees the numeric ID before the final pinned reference is released, so
    signalling by number after a proof — even an immediately preceding one —
    stays racy. Delivery therefore never goes through a numeric
    ``killpg`` at all. Instead, every candidate member from the live group
    snapshot is individually pinned with a pidfd and re-proven under its own
    pin (still in the recorded process group AND still carrying exactly our
    lifecycle token), and only then is the signal delivered through
    ``pidfd_send_signal``, which addresses the kernel-pinned process itself.

    A recycled numeric PID either fails to pin (already gone), or pins some
    other process which then fails the group/token re-proof under the pin —
    so a replacement occupant can never be signalled. Members that exit
    between snapshot and delivery are simply skipped (benign). When nothing
    at all can be proven — including platforms with no pidfd-send binding,
    or when no token exists to prove ownership — nothing is signalled: fail
    closed.

    Args:
        pgid: Recorded process group of the proven worker instance.
        sig: Signal to deliver.
        token: Exact lifecycle token every signalled member must carry.

    Returns:
        ``True`` when at least one member was proven and signalled.
    """
    if token is None:
        # Without a recorded token no per-member ownership proof exists, so
        # no member may ever be signalled.
        return False
    attempted = False
    for pid in _live_group_member_pids(pgid):
        try:
            pidfd = _open_exact_pidfd(pid)
        except (OSError, AttributeError):
            continue  # unpinnable: gone already, or no pin capability
        try:
            # Re-proof happens strictly AFTER the pin, so the checks below
            # describe the same process the signal will address.
            if process_pgrp(pid) != pgid:
                continue
            if not process_has_token(pid, token):
                continue
            pidfd_send_signal(pidfd, sig)
        except (OSError, AttributeError):
            continue  # exited before delivery, or no send capability
        else:
            attempted = True
        finally:
            with suppress(OSError):
                os.close(pidfd)
    return attempted


def _worker_process_alive(meta: WorkerMeta) -> bool:
    """Return whether the exact recorded worker process instance is still alive.

    Unlike :func:`worker_alive`, this does not require the lifecycle token in
    the process environment. Liveness is decided purely by exact process
    identity (PID, process group, session, and start-time ticks), which is
    sufficient and PID-reuse-safe: a recycled PID would carry different
    start-time ticks and is therefore not treated as alive.

    This is the predicate the retirement state machine must use to decide
    whether a planned stop has actually completed. Requiring the token here
    would let a live worker whose environment lacks the marker be mis-reported
    as already stopped, which would let retirement claim success and hand off
    sole-consumer authority while the worker — and every command process group
    it owns — were still alive.

    Args:
        meta: Recorded worker metadata.

    Returns:
        ``True`` when a live process matches every recorded identity field.
    """
    if meta.pid is None:
        return False
    identity = process_identity(meta.pid)
    if identity is None:
        return False
    return identity_matches(meta, identity)


def stop_worker(
    meta: WorkerMeta,
    grace_seconds: float,
    *,
    cancel_grace_seconds: float = DEFAULT_CANCEL_GRACE_SECONDS,
) -> bool:
    """Terminate the recorded worker without leaking owned command groups.

    The worker deliberately starts every command as its own session/process
    group, so killing the worker process never kills its active command groups.
    A planned transition must therefore let the worker drain its own groups
    first. This function asks the worker to drain (``SIGTERM`` to its exact
    process group) and then observes the worker's explicit safe-to-reap
    boundary — either the worker process exits, or it writes a drain sentinel
    proving every owned group is dead — before considering retirement complete.

    Retirement is reported successful only once the exact worker process
    instance is genuinely gone. The wait floor is ``max(grace_seconds,
    cancel_grace_seconds + STOP_DRAIN_OVERHEAD_SLACK_SECONDS)`` so the
    equal/default outer and inner timers can never race: the worker always gets
    its full cancel grace plus bounded finalization overhead before an emergency
    SIGKILL is even possible.

    Every signal is authorized by exact worker identity *at signal time*.
    Because a holding pidfd does not keep a numeric PID/PGID reserved, the
    recorded PID is first pinned with a pidfd and identity plus lifecycle-
    token proofs are taken under that pin to authorize retirement at all;
    each actual SIGTERM/SIGKILL is then delivered per member through its own
    freshly opened pidfd after re-proving group membership and token
    ownership under that member's pin (never via numeric ``killpg``). If
    stable proof is unavailable — no pin possible on a live process, or a
    missing or mismatched token — nothing is signalled and retirement is not
    claimed (fail closed). A recycled PID/PGID occupant can therefore never
    absorb either signal.

    Only if the worker ignores ``SIGTERM`` (wedged) is an emergency SIGKILL sent
    to the worker group. Owned command groups that survive a wedged worker must
    be recovered by exact process-group identity elsewhere (see
    :func:`recover_owned_job_groups`); this function never broad-kills and never
    reports success while the worker process is still alive.

    Args:
        meta: Recorded worker metadata.
        grace_seconds: Intended grace period before the emergency force-kill.
        cancel_grace_seconds: The worker's own command cancel grace, used to
            bound how long the worker is given to drain before it can be
            considered wedged.

    Returns:
        ``True`` when the exact worker process is no longer alive afterwards
        (or was already gone / reused). ``False`` when the exact process
        instance is alive but cannot be authorized for a signal because it does
        not carry our lifecycle token (including when none was recorded), or
        because stable kernel proof of its identity is unavailable, so
        retirement is not claimed.
    """
    if meta.pid is None:
        return True
    lifecycle_state.failpoint("process_retirement")
    try:
        pin = _open_exact_pidfd(meta.pid)
    except (OSError, AttributeError):
        # The pin failed either because the exact worker already exited (the
        # numeric PID may even have been recycled since) or because the
        # platform cannot pin PIDs at all. Distinguish by re-reading identity:
        # a live occupant we cannot pin must never be signalled — fail closed.
        return process_identity(meta.pid) is None
    try:
        return _stop_pinned(meta, grace_seconds, cancel_grace_seconds)
    finally:
        os.close(pin)


def _stop_pinned(
    meta: WorkerMeta,
    grace_seconds: float,
    cancel_grace_seconds: float,
) -> bool:
    """Run the drain/escalate retirement for the proven worker instance.

    The leader pin held by the caller covers only the authorization proof;
    every actual signal is delivered by :func:`_signal_exact_group` through a
    per-member pidfd at the moment of emission, because a holding pidfd does
    not keep a numeric PID/PGID reserved across the interval.

    Args:
        meta: Recorded worker metadata.
        grace_seconds: Intended grace period before the emergency force-kill.
        cancel_grace_seconds: The worker's own command cancel grace.

    Returns:
        ``True`` when the exact worker process is no longer alive afterwards.
    """
    if meta.pid is None:
        return True
    identity = process_identity(meta.pid)
    if identity is None or not identity_matches(meta, identity):
        # The exact worker process is gone or its identity changed (a recycled
        # PID can never be mis-signalled). Nothing to stop; retirement succeeds.
        return True
    # Signal authorization requires the exact lifecycle token: an unowned live
    # process — a wrong token or none at all — is never signalled and
    # retirement is never claimed, so the caller holds rather than handing off
    # sole-consumer authority. This is distinct from the dead/reused case
    # above, where no live process matches the recorded identity and
    # retirement genuinely succeeds.
    if meta.token is None or not process_has_token(meta.pid, meta.token):
        return False
    start = time.monotonic()
    kill_floor = start + cancel_grace_seconds + STOP_DRAIN_OVERHEAD_SLACK_SECONDS
    wait_deadline = start + max(
        grace_seconds,
        cancel_grace_seconds + STOP_DRAIN_OVERHEAD_SLACK_SECONDS,
    )
    # 1. Ask the worker's provably-owned group members to drain.
    _signal_exact_group(identity.pgid, signal.SIGTERM, meta.token)
    # 2. Wait for the worker's explicit safe-to-reap boundary.
    if _wait_for_drain(meta, wait_deadline):
        return True
    # 3. Emergency: the worker ignored SIGTERM and never drained. Never fire
    #    the SIGKILL before the worker's own cancel grace plus finalization
    #    slack, and never fire it at all unless the exact worker instance is
    #    still alive (it may have exited during the floor wait). Delivery is
    #    per-member pinned, so any replacement occupant of a recycled numeric
    #    group member fails its re-proof rather than absorbing the SIGKILL.
    if time.monotonic() < kill_floor:
        time.sleep(kill_floor - time.monotonic())
    if not _worker_process_alive(meta):
        return True
    _signal_exact_group(identity.pgid, signal.SIGKILL, meta.token)
    deadline = time.monotonic() + grace_seconds
    while time.monotonic() < deadline and _worker_process_alive(meta):
        time.sleep(LOCK_POLL_INTERVAL_SECONDS)
    return not _worker_process_alive(meta)


def _wait_for_drain(meta: WorkerMeta, wait_deadline: float) -> bool:
    """Wait until the worker exits or proves its owned groups are drained.

    Returns ``True`` once the exact worker process instance is gone or its
    drain sentinel is present (and the worker has then also exited). ``False``
    means the worker neither drained nor exited before the deadline (it is
    wedged). Liveness is decided by exact process identity
    (:func:`_worker_process_alive`), never by the token environment, so a live
    worker is never mistakenly reported as already stopped.

    Args:
        meta: Recorded worker metadata.
        wait_deadline: Monotonic deadline for the wait.

    Returns:
        ``True`` when the worker reached a safe-to-reap boundary.
    """
    while time.monotonic() < wait_deadline:
        if not _worker_process_alive(meta):
            return True
        if meta.token is not None and drain_sentinel_matches(meta.token):
            while time.monotonic() < wait_deadline and _worker_process_alive(meta):
                time.sleep(LOCK_POLL_INTERVAL_SECONDS)
            return not _worker_process_alive(meta)
        time.sleep(LOCK_POLL_INTERVAL_SECONDS)
    return False


# ---------------------------------------------------------------------------
# Deployment
# ---------------------------------------------------------------------------


def deploy(options: DeployOptions) -> int:
    """Validate a checkout and replace the running worker.

    A deploy submitted through the Lubko queue itself is routed to a detached
    handoff helper so the initiating root job reaches durable ``succeeded``
    before the external supervisor retires the very worker running it: without
    that handoff, the old worker's shutdown terminates the deploying job's own
    process group and records it ``cancelled`` even though the deployment
    converges (the production split-state regression). The helper performs all
    reversible preparation, reports its outcome so the row is durably terminal,
    waits for that durable success, and only then requests the supervisor
    handoff and reconciles the maintained CLIs. A manual invocation retains the
    synchronous locked safe path.

    Args:
        options: Deployment inputs.

    Returns:
        A process exit code.
    """
    try:
        job_id, cancelled = _current_queue_job()
    except DeployAbortedError as exc:
        _err(str(exc) or "deployment was refused")
        return EXIT_ERROR
    if job_id is None:
        return _deploy_manual(options)
    if cancelled:
        _err("deploy job was cancelled during deployment")
        return EXIT_ERROR
    try:
        return _queue_deploy(options, job_id)
    except DeployAbortedError as exc:
        _err(str(exc) or "deployment was refused")
        return EXIT_ERROR


def _deploy_manual(options: DeployOptions) -> int:
    """Run the synchronous locked deploy path outside a queue job.

    Args:
        options: Deployment inputs.

    Returns:
        A process exit code.
    """
    try:
        with deploy_lock(options.lock_timeout_seconds):
            return _deploy_locked(options)
    except LockTimeoutError:
        _err("another deployment is already running; refusing to race")
        return EXIT_ERROR
    except DeployAbortedError:
        return EXIT_ERROR


def _current_queue_job() -> tuple[object | None, bool]:
    """Identify whether this command runs inside a Lubko queue job.

    The owning worker injects the exact root job UUID into the command
    environment (``LUBKO_JOB_ID``), so queue detection never depends on the
    timing of any later ``process_pgid`` persistence. A validation failure
    fails closed: the deploy/restart never silently falls back to the manual
    synchronous path, because that path retires the very worker executing the
    job and would reproduce the killed-control-job regression.

    Returns:
        ``(job_id, cancelled)`` when queue-invoked, otherwise ``(None, False)``.

    Raises:
        DeployAbortedError: If an injected queue job cannot be validated.
    """
    if os.environ.get(JOB_ID_ENV) is None:
        return None, False
    from lubko import (  # ruff: ignore[import-outside-top-level] - breaks the deployctl<->lifecycle import cycle
        deployctl,
    )

    try:
        return deployctl.current_queue_job_id()
    except deployctl.DeployCtlError as exc:
        _err(str(exc))
        raise DeployAbortedError from None


def detach_standard_streams(keep: set[int]) -> None:
    """Sever inherited stdio of a detached child and close every other stray fd.

    A detached deployment child (queue handoff helper or rollback watchdog)
    outlives the job process that forked it while the owning worker may be
    finalizing that exact job: under worker-owned pipe capture the inherited
    standard streams ARE the capture-pipe write ends, so keeping them open
    pins output the worker no longer owns, and writing after the worker closes
    its read ends kills the helper with ``EPIPE`` before it can settle durable
    state. This redirects fd 0/1/2 to ``/dev/null`` (so the child always has a
    valid, harmless stdin/stdout/stderr), closes every other inherited
    descriptor except the explicitly kept ones (the response writer), and
    leaves the child owning exactly: /dev/null stdio plus ``keep``.

    Args:
        keep: Descriptor numbers that must remain open besides 0/1/2.
    """
    for stream in (sys.stdout, sys.stderr):
        with suppress(OSError, ValueError):
            stream.flush()
    devnull = os.open(os.devnull, os.O_RDWR)
    try:
        for stream_fd in (0, 1, 2):
            os.dup2(devnull, stream_fd)
    finally:
        with suppress(OSError):
            os.close(devnull)
    close_inherited_descriptors({0, 1, 2, *keep})


def close_inherited_descriptors(keep: set[int]) -> None:
    """Close every inherited file descriptor except the explicitly kept ones.

    The detached handoff helpers are forked from the queue job's controller, so
    they inherit the parent's descriptor table. Any descriptor other than the
    stable response pipe and the standard streams is closed so a long-lived
    helper never pins a connection, lock, or capture file it does not own.

    Args:
        keep: Descriptor numbers that must stay open.
    """
    try:
        entries = list(Path("/proc/self/fd").iterdir())
    except OSError:
        return
    for entry in entries:
        if not entry.name.isdigit():
            continue
        fd = int(entry.name)
        if fd in keep:
            continue
        with suppress(OSError):
            os.close(fd)


def _queue_prepared_response(commit: str) -> dict[str, object]:
    """Build the successful helper response delivered before the handoff.

    Args:
        commit: Exact validated candidate commit.

    Returns:
        A JSON response object reporting that validation succeeded.
    """
    return {
        "ok": True,
        "type": "deploy",
        "commit": commit,
        "phase": "requested",
    }


def _queue_deploy(options: DeployOptions, job_id: object) -> int:
    """Handle a queue-invoked deploy through a detached helper process.

    The controller forks a helper into a separate session; the helper performs
    all reversible preparation and reports its outcome, this parent delivers a
    summary and exits zero so the owning worker finalizes the deploy row as
    durably ``succeeded``, and only then does the helper cross the destructive
    handoff and reconcile the CLIs. The parent never waits for the helper to
    finish and never touches the terminal row itself.

    Args:
        options: Deployment inputs.
        job_id: Captured deploy queue row identifier.

    Returns:
        A process exit code.

    Raises:
        DeployAbortedError: If the helper cannot be forked or never reports.
    """
    from lubko import (  # ruff: ignore[import-outside-top-level] - breaks the deployctl<->lifecycle import cycle
        deployctl,
    )

    reader, writer = os.pipe()
    try:
        try:
            pid = os.fork()
        except OSError as exc:
            msg = f"could not fork the deployment handoff helper: {exc}"
            raise DeployAbortedError(msg) from None
        if pid == 0:
            os.close(reader)
            _run_deploy_helper(options, job_id, writer)
        os.close(writer)
        try:
            raw = deployctl.read_pipe_line(reader)
        finally:
            os.close(reader)
    finally:
        with suppress(OSError):
            os.close(reader)
        with suppress(OSError):
            os.close(writer)
    if not raw:
        msg = "deployment handoff helper exited before reporting an outcome"
        raise DeployAbortedError(msg)
    try:
        response = json.loads(raw)
    except ValueError as exc:
        msg = "deployment handoff helper reported an invalid response"
        raise DeployAbortedError(msg) from exc
    if not isinstance(response, dict):
        msg = "deployment handoff helper reported a non-object response"
        raise DeployAbortedError(msg)
    if response.get("ok") is not True:
        detail = response.get("error")
        message = "deployment was refused"
        if isinstance(detail, str):
            message = f"deployment was refused: {detail}"
        raise DeployAbortedError(message)
    commit = response.get("commit")
    if isinstance(commit, str):
        _out(f"validated exact commit {commit}; requesting the supervisor to run it")
    _out(
        "deployment requested; it converges detached from this job through the external "
        "supervisor and the maintained CLIs"
    )
    return EXIT_OK


def _run_deploy_helper(options: DeployOptions, job_id: object, writer: int) -> None:
    """Run the detached queue-deploy handoff helper to completion in the child.

    The child detaches into its own session immediately so the retiring
    worker's group shutdown can never reach it; a failed detach fails closed
    with an error response before any prepared/success outcome, because a
    helper still attached to the job's session would be killed during the
    retirement it is about to trigger. It then closes every inherited
    descriptor except the response pipe and the standard streams, acquires the
    deployment lock itself, performs all reversible preparation, delivers the
    outcome to the parent, waits for the initiating row to be durably
    ``succeeded``, and only then crosses the destructive handoff and reconciles
    the maintained CLIs. This function never returns: it exits the child
    process.

    Args:
        options: Deployment inputs.
        job_id: Captured deploy queue row identifier.
        writer: Write end of the response pipe to the parent.
    """
    from lubko import (  # ruff: ignore[import-outside-top-level] - breaks the deployctl<->lifecycle import cycle
        deployctl,
    )

    try:
        os.setsid()
    except OSError as exc:
        deployctl.send_helper_error(
            writer, f"deployment handoff helper could not detach into its own session: {exc}"
        )
        with suppress(OSError):
            os.close(writer)
        os._exit(0)
    detach_standard_streams(keep={writer})
    try:
        try:
            with deploy_lock(options.lock_timeout_seconds):
                _deploy_helper_locked(options, job_id, writer)
        except LockTimeoutError as exc:
            deployctl.send_helper_error(writer, f"timed out waiting for the deployment lock: {exc}")
        except DeployAbortedError as exc:
            deployctl.send_helper_error(writer, f"deployment was refused: {exc}")
        except OSError as exc:
            deployctl.send_helper_error(writer, f"operating-system error: {exc}")
    finally:
        with suppress(OSError):
            os.close(writer)
    os._exit(0)


def _deploy_helper_locked(options: DeployOptions, job_id: object, writer: int) -> None:
    """Run one lock-held queue deploy mission in the detached helper.

    The response or error is delivered to the parent before any destructive
    step, so the parent exits zero only for a genuine prepared response and the
    owning worker finalizes the deploying row as durably ``succeeded``; a
    helper error or helper death exits non-zero so the row is durably
    ``failed`` and a dead helper can never leave a falsely-successful row. The
    helper then waits for that exact row to be durably ``succeeded`` before
    crossing the handoff, so the control job is never killed by the old
    worker's own shutdown (the production split-state regression). Any failure
    before durable success aborts with nothing destructive done: the previous
    worker is left running, the provisional CLI root is removed, and the
    deployment never silently falls back to the manual synchronous path.

    Args:
        options: Deployment inputs.
        job_id: Captured deploy queue row identifier.
        writer: Write end of the response pipe to the parent.
    """
    from lubko import (  # ruff: ignore[import-outside-top-level] - breaks the deployctl<->lifecycle import cycle
        deployctl,
    )

    previous = read_meta()
    state = worker_state(previous)
    error: str | None
    if options.bootstrap:
        error = (
            "queue-invoked bootstrap is refused: the worker executing this job is a live queue "
            "consumer, and the bootstrap path requires the legacy worker to be stopped manually "
            "first so the replacement never starts alongside a live consumer"
        )
    elif state == STATE_UNMANAGED:
        error = UNMANAGED_WORKER_MESSAGE
    else:
        error = _supervised_mutation_blocker() or (
            None
            if supervise.supervisor_running()
            else (
                "no external supervisor is running; refusing to deploy the maintained worker "
                "without automatic restart protection"
            )
        )
    if error is not None:
        deployctl.send_helper_error(writer, error)
        return
    try:
        commit = _validate_and_prepare(options)
    except DeployAbortedError as exc:
        deployctl.send_helper_error(writer, str(exc) or "deployment validation failed")
        return
    if _refuse_version_changing_deploy(previous, commit) and previous is not None:
        cli.remove_cli_root(commit)
        deployctl.send_helper_error(
            writer,
            f"maintained worker metadata records commit {previous.git_commit}; ordinary deploys "
            "cannot change versions; use 'lubko-deploy-ctl checkout'",
        )
        return
    deployctl.send_helper_response(writer, _queue_prepared_response(commit))
    durable_deadline = time.time() + deployctl.handoff_durable_wait_seconds
    try:
        deployctl.wait_for_durable_success(job_id, durable_deadline)
    except deployctl.DeployCtlError as exc:
        cli.remove_cli_root(commit)
        append_deploy_log(f"queue deploy aborted before the destructive handoff: {exc}")
        return
    _finish_queue_deploy(options, commit, previous, state)


def _finish_queue_deploy(
    options: DeployOptions,
    commit: str,
    previous: WorkerMeta | None,
    state: str,
) -> None:
    """Complete a queue deploy after durable success, converging to coherence.

    The row is already durably ``succeeded``, so a handoff or CLI-activation
    failure can no longer be reflected in it. A zero exit means the candidate
    worker and the maintained CLIs both converged. Any other outcome — a raised
    handoff error or a nonzero result (CLI activation exhausted its bounded
    retries after the candidate went live) — restores the previous confirmed
    commit through :func:`_restore_after_handoff_failure`, so the live worker,
    the supervisor desired/applied state, and ``cli/current`` never diverge.

    Args:
        options: Deployment inputs.
        commit: Exact candidate commit.
        previous: Previously recorded worker metadata, or ``None``.
        state: Effective state of the previous worker.
    """
    try:
        exit_code = _complete_deploy_handoff(options, commit, previous, state)
    except DeployAbortedError as exc:
        append_deploy_log(f"queue deploy handoff failed after durable success: {exc}")
        _restore_after_handoff_failure(options, commit, previous)
        return
    if exit_code == EXIT_OK:
        append_deploy_log("queue deploy converged after durable success")
        return
    append_deploy_log(
        "queue deploy converged after durable success but the maintained CLIs did not; "
        "restoring the previous commit"
    )
    _restore_after_handoff_failure(options, commit, previous)


def _restore_after_handoff_failure(
    options: DeployOptions,
    commit: str,
    previous: WorkerMeta | None,
) -> None:
    """Restore service when a queue deploy fails after durable success.

    The initiating row is already durably ``succeeded`` (the parent exited
    before the handoff), so a later handoff or CLI-activation failure can no
    longer be reflected in the row. Instead the detached helper converges the
    environment to exactly one coherent state. A failure is only accepted as
    harmless when the candidate commit is live, proven, *and* the maintained
    CLIs already select it — that is a genuinely converged deployment (for
    example the failure came from a transient readiness check after activation).
    In every other case — including a live candidate whose CLI activation never
    converged — the supervisor is settled back to the previous confirmed commit
    at a strictly newer generation and the maintained CLI pointer is reconciled
    to it, so the live worker, the supervisor desired/applied state, and
    ``cli/current`` all select the same exact commit with no manual
    reconciliation required.

    Args:
        options: Deployment inputs.
        commit: Exact candidate commit whose deploy failed.
        previous: Previously recorded worker metadata, or ``None``.
    """
    if not supervise.supervisor_running():
        append_deploy_log(
            "queue deploy failed after durable success without a live supervisor; nothing to "
            "restore"
        )
        return
    status = supervise.read_status()
    if (
        status is not None
        and status.commit == commit
        and status.child is not None
        and status.ready
        and cli.current_commit() == commit
    ):
        append_deploy_log(f"queue deploy fully converged on commit {commit}; nothing to restore")
        return
    if previous is None or previous.git_commit is None:
        append_deploy_log("queue deploy failed after durable success with no known previous commit")
        return
    try:
        settle = supervise.request_run(
            previous.git_commit,
            repo=str(options.repo),
            uv_path=options.uv_path,
            worker_id=os.getenv("LUBKO_WORKER_ID") or socket.gethostname(),
        )
    except OSError as exc:
        append_deploy_log(
            f"queue deploy failed after durable success and restoring the previous commit "
            f"errored: {exc}"
        )
        return
    restored = supervise.wait_for_generation(
        settle, supervise.DEFAULT_REQUEST_TIMEOUT_SECONDS
    ) and supervise.wait_until_ready(settle, supervise.DEFAULT_REQUEST_TIMEOUT_SECONDS)
    if restored and cli.reconcile_pointer(previous.git_commit):
        append_deploy_log(
            "queue deploy failed after durable success; supervisor restored previous commit "
            f"{previous.git_commit} and the maintained CLIs"
        )
    else:
        append_deploy_log(
            "queue deploy failed after durable success and the previous commit could not be fully "
            "restored"
        )


def _verify_replacement(new_meta: WorkerMeta, options: DeployOptions) -> bool:
    """Verify the replacement is alive and can reach PostgreSQL.

    Args:
        new_meta: Metadata of the freshly started worker.
        options: Deployment inputs.

    Returns:
        ``True`` when the replacement passes verification.
    """
    if not worker_alive(new_meta):
        _err("replacement worker did not stay alive")
        stop_worker(new_meta, options.stop_grace_seconds)
        return False
    if not check_postgres(options.postgres_timeout_seconds):
        _err("replacement worker cannot reach PostgreSQL; leaving the current worker untouched")
        stop_worker(new_meta, options.stop_grace_seconds)
        return False
    return True


def _validate_and_prepare(options: DeployOptions) -> str:
    """Validate a checkout and prepare its maintained CLI environment.

    Args:
        options: Deployment inputs.

    Returns:
        The exact checkout commit.

    Raises:
        DeployAbortedError: If the checkout is not deployable.
    """
    if not require_clean_checkout(options.repo, options.git_timeout_seconds):
        _err(
            "deployment checkout is dirty; the worker and the maintained CLIs must run the exact "
            "committed code, so commit or discard working-tree changes first"
        )
        raise DeployAbortedError from None
    _out("validating checkout ...")
    report = run_validation(options.repo, options.uv_path, options.validation_timeout_seconds)
    if not report.ok:
        _err("validation failed; the current worker is left untouched")
        _err(report.detail)
        raise DeployAbortedError
    commit = git_commit(options.repo, options.git_timeout_seconds)
    if commit is None:
        _err("could not read the git commit of the deployment checkout")
        raise DeployAbortedError from None
    if not _prepare_maintained_cli(options, commit):
        raise DeployAbortedError from None
    return commit


def _prepare_maintained_cli(options: DeployOptions, commit: str) -> bool:
    """Build the maintained CLI environment for one exact commit.

    A failed CLI build aborts before any worker is replaced, so the current
    worker and the prior confirmed CLIs both stay untouched.

    Args:
        options: Deployment inputs.
        commit: Exact commit to build the CLI environment for.

    Returns:
        ``True`` when the environment is usable, ``False`` otherwise.
    """
    _out("preparing the maintained CLI environment ...")
    try:
        cli.build_cli_root(options.repo, commit, options.uv_path, options.cli_timeout_seconds)
    except cli.CliError as exc:
        _err(
            "could not prepare the maintained CLI environment; the current worker is left untouched"
        )
        _err(str(exc))
        return False
    return True


def _clear_stale_supervisor_override(confirmed_commit: str) -> None:
    """Clear the supervisor-runtime override after a successful CLI activation.

    The override is a temporary bootstrap pin: it directs the stable
    ``lubko-supervisor`` launcher to a specific sealed runtime on the next
    container restart.  Once any CLI activation succeeds, the override has
    served its purpose and must be removed unconditionally — regardless of
    whether the override target matches the newly confirmed commit — so a
    stale override (e.g. staged for B, later activation moves to C) can
    never pin an obsolete supervisor runtime on the next restart.

    Args:
        confirmed_commit: The newly confirmed commit that ``cli/current``
            now selects (used only for the deploy log entry).
    """
    override = supervise.read_supervisor_runtime_override()
    if override is not None:
        supervise.clear_supervisor_runtime_override()
        append_deploy_log(
            f"cleared supervisor-runtime override: commit {confirmed_commit} is now confirmed"
        )


def _activate_maintained_cli(commit: str) -> bool:
    """Activate the confirmed CLI commit, preserving the prior coherent CLI.

    Activation happens only after the new worker metadata is durable. The
    pointer switch is retried a bounded number of times so a transient
    filesystem failure cannot leave a freshly live worker with a stale global
    CLI that requires a manual ``lubko-deploy-ctl status`` reconciliation. On
    final failure the previous CLI commit stays active and its environment is
    never garbage-collected, so the global CLIs remain usable even though they
    are temporarily behind the worker; the next status/checkout still repairs
    the pointer idempotently.

    When a supervisor-runtime override was staged by ``lubko-deploy bootstrap``
    and CLI activation succeeds, the override is unconditionally cleared so
    future upgrades never pin an obsolete supervisor runtime — even when the
    override target differs from the newly confirmed commit (e.g. bootstrap
    staged B, later activation confirmed C).

    Args:
        commit: Exact commit to activate.

    Returns:
        ``True`` when the pointer now selects ``commit``, ``False`` otherwise.
    """
    last_error: cli.CliError | None = None
    for attempt in range(CLI_ACTIVATION_ATTEMPTS):
        try:
            cli.set_current(commit)
        except cli.CliError as exc:
            last_error = exc
            if attempt < CLI_ACTIVATION_ATTEMPTS - 1:
                time.sleep(CLI_ACTIVATION_RETRY_SECONDS)
            continue
        _clear_stale_supervisor_override(commit)
        cli.gc_cli_roots((commit,))
        return True
    _err(f"error: maintained CLI activation failed: {last_error}")
    _err(
        "the previous CLI commit remains active and usable; run 'lubko-deploy-ctl status' "
        "or 'lubko-install' to repair the maintained CLIs"
    )
    return False


def _deploy_through_supervisor(options: DeployOptions, commit: str) -> WorkerMeta:
    """Ask the external supervisor to start the worker for the confirmed commit.

    The supervisor owns the maintained worker process, so a deployment hands
    the exact commit to the daemon through the durable desired-intent protocol
    and waits until the daemon reports a live worker for it.  The supervisor
    already retired any previous worker it owned, so no separate stop is
    needed here.

    Args:
        options: Deployment inputs.
        commit: Exact commit to deploy.

    Returns:
        The maintained metadata the supervisor recorded for the worker.

    Raises:
        DeployAbortedError: If the supervisor did not start a verified worker.
    """
    worker_id = os.getenv("LUBKO_WORKER_ID") or socket.gethostname()
    _out("requesting the external supervisor to start the worker ...")
    generation = supervise.request_run(
        commit,
        repo=str(options.repo),
        uv_path=options.uv_path,
        worker_id=worker_id,
    )
    if not supervise.wait_for_generation(generation, supervise.DEFAULT_REQUEST_TIMEOUT_SECONDS):
        _err("the external supervisor did not apply the requested worker start")
        raise DeployAbortedError
    if not supervise.wait_until_ready(generation, supervise.DEFAULT_REQUEST_TIMEOUT_SECONDS):
        _err(
            "the external supervisor did not prove its worker consumes the queue; "
            "the deployment is not verified"
        )
        raise DeployAbortedError
    meta = read_meta()
    if meta is None or not worker_alive(meta) or meta.git_commit != commit:
        _err(
            "the external supervisor did not report a live maintained worker for the deployed "
            "commit"
        )
        raise DeployAbortedError
    if not check_postgres(options.postgres_timeout_seconds):
        _err("the replacement worker cannot reach PostgreSQL; leaving the current worker untouched")
        raise DeployAbortedError
    append_deploy_log(f"supervisor started worker pid={meta.pid} commit={commit}")
    return meta


def _deploy_direct(
    options: DeployOptions,
    previous: WorkerMeta | None,
    state: str,
    commit: str,
) -> WorkerMeta:
    """Start the replacement worker directly, bypassing the external supervisor.

    This is the narrow legacy path used only by the one-time bootstrap and by
    tests that exercise the direct mechanism explicitly.  A normal maintained
    install never reaches it: :func:`_deploy_locked` refuses without the
    external supervisor.

    Args:
        options: Deployment inputs.
        previous: Previously recorded worker metadata, or ``None``.
        state: Effective state of the previous worker.
        commit: Exact commit to deploy.

    Returns:
        The maintained metadata of the started worker.

    Raises:
        DeployAbortedError: If the direct replacement cannot be verified or the
            previous worker cannot be stopped.
    """
    token = secrets.token_hex(16)
    env = worker_env(token)
    worker_id = env.get("LUBKO_WORKER_ID") or socket.gethostname()

    _out("starting replacement worker ...")
    try:
        proc = spawn_worker(options.repo, options.uv_path, worker_log_path(token), env)
    except OSError as exc:
        _err(f"could not start the replacement worker: {exc}")
        raise DeployAbortedError from None

    identity = _wait_for_identity(proc.pid)
    if identity is None:
        if proc.poll() is None:
            # Never observable yet still live: no exact anchor exists, so
            # convergence may only fail closed and reap the direct child.
            _err("replacement worker stayed live without an observable identity; converging it")
        else:
            _err("replacement worker exited before establishing its identity")
        _converge_unproven_spawn(proc, options.stop_grace_seconds, None)
        raise DeployAbortedError
    if identity.pgid != proc.pid or identity.sid != proc.pid:
        _err(
            "replacement worker timed out before establishing its private session; "
            "converging it before aborting"
        )
        _converge_unproven_spawn(proc, options.stop_grace_seconds, identity)
        raise DeployAbortedError

    new_meta = WorkerMeta(
        schema_version=SCHEMA_VERSION,
        state=STATE_RUNNING,
        pid=identity.pid,
        pgid=identity.pgid,
        sid=identity.sid,
        start_time_ticks=identity.start_time_ticks,
        token=token,
        repo=str(options.repo),
        git_commit=commit,
        worker_id=worker_id,
        log_path=str(worker_log_path(token)),
        started_at=time.time(),
        stopped_at=None,
    )

    if not _verify_replacement(new_meta, options):
        raise DeployAbortedError

    if state == STATE_RUNNING and previous is not None:
        _out(f"stopping previous worker pid {previous.pid} ...")
        if not stop_worker(previous, options.stop_grace_seconds):
            _err("failed to stop the previous worker; rolling back the replacement")
            stop_worker(new_meta, options.stop_grace_seconds)
            raise DeployAbortedError

    write_meta(new_meta)
    return new_meta


def _supervised_mutation_blocker() -> str | None:
    """Return why ordinary lifecycle mutation is currently blocked, if it is.

    ``lubko-deploy-ctl`` is the only authority for version-changing worker
    mutations, and the external supervisor owns the mission state that decides
    whether a handoff is in flight. Ordinary ``lubko-deploy`` mutations must
    therefore fail closed while a supervised deployment is pending or while the
    durable rollback state is unreadable/corrupt: mutating during an unknown
    handoff could disrupt a rollback or resurrect a superseded commit. Callers
    must invoke this under the deployment lock so no guard-to-mutation TOCTOU
    window remains.

    The decision is delegated to :func:`lubko.lifecycle_state.mutation_blocker_reason`,
    the single authority-state model, so the refusal logic cannot diverge from
    the documented invariants.

    Returns:
        ``None`` when mutation may proceed, otherwise a human-readable refusal
        reason.

    """
    return lifecycle_state.mutation_blocker_reason()


def _refuse_version_changing_deploy(previous: WorkerMeta | None, commit: str) -> bool:
    """Decide whether an ordinary deploy would change the recorded version.

    Maintained-worker metadata is lifecycle/deploy authority: once any
    maintained worker is recorded — running, stopped, or otherwise non-live —
    an ordinary ``lubko-deploy deploy`` must never change its commit. Only a
    clean first installation (no metadata) and a same-commit invocation are
    allowed.

    The decision is delegated to
    :func:`lubko.lifecycle_state.refuses_version_change`, the single authority
    model, so the recorded-version rule cannot diverge from the documented
    invariants.

    Args:
        previous: Previously recorded worker metadata, or ``None``.
        commit: Exact validated target commit.

    Returns:
        ``True`` when the deploy must be refused.
    """
    previous_commit = previous.git_commit if previous is not None else None
    return lifecycle_state.refuses_version_change(previous, commit, git_commit=previous_commit)


def _deploy_locked(options: DeployOptions) -> int:
    """Perform a deployment while holding the deployment lock.

    The external supervisor is the single authority that owns the maintained
    worker; a normal deployment hands the exact commit to the daemon and never
    silently falls back to direct spawning when the daemon is absent. Ordinary
    deploys serve only the clean first maintained-worker installation and the
    same-commit restart; version-changing mutations must go through
    ``lubko-deploy-ctl``, and mutation is refused while supervised rollback
    state is pending or corrupt. The guard and the mutation share this held
    deployment lock, so no guard-to-mutation TOCTOU window exists.

    Args:
        options: Deployment inputs.

    Returns:
        A process exit code.

    Raises:
        DeployAbortedError: If the deployment must abort and leave the current
            worker untouched.
    """
    try:
        previous = read_meta_strict()
    except WorkerMetadataError as exc:
        _err(
            "maintained-worker metadata is present but untrustworthy "
            f"({exc}); refusing lifecycle mutation"
        )
        _err("run 'lubko-deploy repair' to recover maintained-worker authority")
        raise DeployAbortedError from exc
    state = worker_state(previous)

    blocker = _supervised_mutation_blocker()
    if blocker is not None:
        _err(blocker)
        raise DeployAbortedError

    if state == STATE_UNMANAGED:
        if not options.bootstrap:
            _err(UNMANAGED_WORKER_MESSAGE)
            _err("stop the legacy worker manually once, then rerun with --bootstrap")
            raise DeployAbortedError
        _out("bootstrap: no maintained worker metadata; assuming the legacy worker was stopped")

    commit = _validate_and_prepare(options)
    if _refuse_version_changing_deploy(previous, commit) and previous is not None:
        _err(
            f"maintained worker metadata records commit {previous.git_commit}; ordinary "
            "'lubko-deploy deploy' cannot change versions"
        )
        _err("use 'lubko-deploy-ctl checkout' for the rollback-safe version change")
        raise DeployAbortedError
    return _complete_deploy_handoff(options, commit, previous, state)


def _complete_deploy_handoff(
    options: DeployOptions,
    commit: str,
    previous: WorkerMeta | None,
    state: str,
) -> int:
    """Cross the destructive handoff for one validated commit.

    The external supervisor is the single authority that owns the maintained
    worker; the worker is handed to the daemon and never directly replaced when
    the daemon is present. The maintained CLI environment is activated only
    after the worker handoff so the global commands stay coherent with the
    confirmed commit, even when the caller is the detached queue handoff helper
    that must converge after its root job is already durably ``succeeded``.

    Args:
        options: Deployment inputs.
        commit: Exact validated commit to deploy.
        previous: Previously recorded worker metadata, or ``None``.
        state: Effective state of the previous worker.

    Returns:
        A process exit code.

    Raises:
        DeployAbortedError: If the worker handoff cannot complete.
    """
    if supervise.supervisor_running():
        new_meta = _deploy_through_supervisor(options, commit)
        log_file = worker_log_path(new_meta.token)
        _out(f"worker running: pid={new_meta.pid} pgid={new_meta.pgid} session={new_meta.sid}")
    elif options.bootstrap or options.direct_spawn:
        new_meta = _deploy_direct(options, previous, state, commit)
        log_file = worker_log_path(new_meta.token)
        supervise.request_run(
            commit,
            repo=str(options.repo),
            uv_path=options.uv_path,
            worker_id=os.getenv("LUBKO_WORKER_ID") or socket.gethostname(),
        )
        _out(f"worker running: pid={new_meta.pid} pgid={new_meta.pgid} session={new_meta.sid}")
    else:
        _err(
            "no external supervisor is running; refusing to deploy the maintained worker without "
            "automatic restart protection"
        )
        _err(
            "start the supervisor as the container's main process (see README 'External worker "
            "supervision'), or use the one-time '--bootstrap' path on a fresh install"
        )
        raise DeployAbortedError

    cli_ok = _activate_maintained_cli(commit)
    append_deploy_log(
        f"deployed commit {commit} pid={new_meta.pid}"
        + ("" if cli_ok else "; maintained CLI activation failed")
    )
    _out(f"deployed git commit {commit}")
    _out(f"log: {log_file}")
    if not cli_ok:
        _err("error: the worker runs the new commit but the maintained CLIs could not be switched")
        _err(
            "the previous CLI commit remains active and usable; run 'lubko-deploy-ctl status' "
            "or 'lubko-install' to repair"
        )
        return EXIT_ERROR
    return EXIT_OK


# ---------------------------------------------------------------------------
# Repair
# ---------------------------------------------------------------------------


def _read_process_env(pid: int) -> dict[str, str]:
    """Read the exact environment of a live process from ``/proc``.

    Args:
        pid: Process whose environment to inspect.

    Returns:
        The process environment as a mapping, or ``{}`` when unreadable.
    """
    try:
        raw = (Path("/proc") / str(pid) / "environ").read_bytes()
    except OSError:
        return {}
    environ: dict[str, str] = {}
    for part in raw.split(b"\0"):
        if b"=" not in part:
            continue
        name, _separator, value = part.partition(b"=")
        environ[name.decode("utf-8", "replace")] = value.decode("utf-8", "replace")
    return environ


def _read_cmdline(pid: int) -> str | None:
    """Read the command line of a live process from ``/proc``.

    Args:
        pid: Process whose command line to inspect.

    Returns:
        The joined command line, or ``None`` when unreadable.
    """
    try:
        raw = (Path("/proc") / str(pid) / "cmdline").read_bytes()
    except OSError:
        return None
    return " ".join(part.decode("utf-8", "replace") for part in raw.split(b"\0") if part)


def _is_lubko_worker_process(pid: int) -> bool:
    """Return whether a process command line names a Lubko worker.

    The exact command evidence is part of adopting an independently provided
    recovery worker PID: the operator names the PID, the exact session/group
    identity is verified, and the command line must actually be a worker
    before its identity is recorded. Nothing here signals by process name; it
    only refuses to adopt a PID that is not a worker.

    Args:
        pid: Process whose command line to inspect.

    Returns:
        ``True`` when the command line carries the worker module or script.
    """
    cmdline = _read_cmdline(pid)
    if cmdline is None:
        return False
    return "lubko-worker" in cmdline or "lubko.worker" in cmdline


class _AdoptionError(RuntimeError):
    """Raised when a recovery worker cannot be safely adopted."""


def _required_rollback_status(data: dict[str, object]) -> str:
    """Return a canonical persisted rollback lifecycle status.

    Raises:
        ValueError: If the status is absent or outside the canonical lifecycle states.
    """
    status = _meta_string(data, "status", default="")
    if status not in {STATE_PENDING, "confirmed", "rolled_back"}:
        msg = f"unsupported rollback status {status!r}"
        raise ValueError(msg)
    return status


def _repair_rollback_state(recovery_worker_pid: int) -> None:
    """Resolve stale rollback state before adopting a recovery worker.

    A terminal or abandoned supervised-deployment record is inert and removed.
    A genuinely live pending mission, or any live recorded identity other than
    the recovery worker, aborts the repair so it can never adopt a second
    consumer or disturb an in-flight deployment.

    Args:
        recovery_worker_pid: Exact PID of the recovery worker being adopted.

    Raises:
        _AdoptionError: If rollback authority is malformed, a live mission is
            active, or a different live identity is recorded.
    """
    path = rollback_state_path()
    if not path.is_file():
        return
    try:
        data = json.loads(path.read_text())
    except (OSError, ValueError) as exc:
        msg = "rollback state is present but malformed; repair refuses to erase authority"
        raise _AdoptionError(msg) from exc
    if not isinstance(data, dict):
        msg = "rollback state is present but malformed; repair refuses to erase authority"
        raise _AdoptionError(msg)
    try:
        new_meta = WorkerMeta.from_dict(data.get("new_meta") or {})
        previous_meta = WorkerMeta.from_dict(data.get("previous_meta") or {})
        deadline = _meta_optional_finite_float(data, "deadline")
        status = _required_rollback_status(data)
    except (KeyError, TypeError, ValueError) as exc:
        msg = "rollback state is present but malformed; repair refuses to erase authority"
        raise _AdoptionError(msg) from exc
    if deadline is None:
        msg = "rollback state is present but malformed; repair refuses to erase authority"
        raise _AdoptionError(msg)
    if status == STATE_PENDING and worker_alive(new_meta) and deadline > time.time():
        msg = "another supervised deployment is still pending confirmation"
        raise _AdoptionError(msg)
    if worker_alive(new_meta) and new_meta.pid != recovery_worker_pid:
        msg = (
            f"a live supervised candidate worker pid {new_meta.pid} exists; "
            "repair refuses to adopt a different process"
        )
        raise _AdoptionError(msg)
    if worker_alive(previous_meta) and previous_meta.pid != recovery_worker_pid:
        msg = (
            f"a live supervised previous worker pid {previous_meta.pid} exists; "
            "repair refuses to adopt a different process"
        )
        raise _AdoptionError(msg)
    remove_durable(path)


def _probe_server() -> str:
    """Return the configured server identity a probe job must be addressed to.

    The probe targets the local daemon, whose single configured server
    identity comes from the restricted worker configuration file; there is
    no implicit or default server.

    Returns:
        The non-empty configured server identity.

    Raises:
        RuntimeError: If the configured server identity cannot be loaded.
    """
    try:
        return load_worker_server()
    except (OSError, ValueError) as exc:
        msg = (
            "the worker configuration file must provide a non-empty 'server' "
            "identity addressed by the probe job; there is no implicit or "
            f"default server ({exc})"
        )
        raise RuntimeError(msg) from exc


def _insert_probe_job(conn: JobsConnection, cwd: str) -> UUID | None:
    """Insert one pending queue probe job.

    The probe is submitted at the highest protocol version this client shares
    with the target server's supported window (see
    :func:`lubko.protocol_versioning.negotiate_submission_version`), so a
    mixed-version fleet converges new work onto the newest supported generation
    while older in-flight jobs keep running on daemons that still advertise the
    older version.

    Args:
        conn: Open PostgreSQL connection.
        cwd: Working directory for the probe process.

    Returns:
        The probe job identifier, or ``None`` if the insert failed.
    """
    server_range = load_worker_protocol_range()
    version = negotiate_submission_version(server_range)
    probe_payload = json.dumps(
        protocol.build_payload(
            server=_probe_server(),
            cwd=cwd,
            process=["/usr/bin/sleep", "60"],
            version=version,
        )
    )
    with conn.cursor() as cursor:
        cursor.execute(
            "INSERT INTO lubko.jobs (payload) VALUES (%s) RETURNING id",
            (probe_payload,),
        )
        row = cursor.fetchone()
    return row[0] if row is not None else None


def _read_ppid(pid: int) -> int | None:
    """Read the exact parent process ID of a live process.

    Args:
        pid: Process whose parent to inspect.

    Returns:
        The parent PID, or ``None`` when the process is gone or unreadable.
    """
    try:
        stat = (Path("/proc") / str(pid) / "stat").read_bytes()
    except OSError:
        return None
    close_paren = stat.rfind(b")")
    if close_paren == -1:
        return None
    fields = stat[close_paren + 2 :].split()
    if len(fields) < STAT_PPID_MIN_FIELDS:
        return None
    try:
        return int(fields[STAT_PPID_FIELD_INDEX])
    except ValueError:
        return None


def _spawned_by_recovery_worker(process_pid: int, recovery_worker_pid: int) -> bool:
    """Return whether a process was spawned by the exact recovery worker.

    The worker daemon directly ``Popen``-spawns every job process, so the
    probe command process is an ancestor-chain descendant of the worker daemon
    that claimed it (through the daemon, and through a ``uv run lubko-worker``
    launcher when the daemon is the launcher's child). Walking the exact parent
    chain binds the claim to the supplied recovery worker PID regardless of
    whether the recorded ``worker_id`` is unique.

    Args:
        process_pid: Persisted process ID of the probe command process.
        recovery_worker_pid: Exact PID of the worker being adopted.

    Returns:
        ``True`` only when the process's ancestor chain contains the exact
        recovery worker PID.
    """
    current = process_pid
    seen: set[int] = set()
    while current not in seen and current > 0:
        seen.add(current)
        if current == recovery_worker_pid:
            return True
        parent = _read_ppid(current)
        if parent is None:
            return False
        current = parent
    return False


def _parse_probe_claim_state(
    state: object,
) -> tuple[str, str | None, int | None] | None:
    """Parse persisted recovery-probe claim state without scalar coercion.

    Returns:
        Canonical claim fields, or ``None`` when persisted authority is malformed.
    """
    if not isinstance(state, dict):
        return None
    status = state.get("status")
    if not isinstance(status, str):
        return None

    owner_raw = state.get("worker_id")
    if "worker_id" in state and not isinstance(owner_raw, str):
        return None
    owner = owner_raw if isinstance(owner_raw, str) else None

    if "process_pid" not in state:
        return status, owner, None
    process_pid = state["process_pid"]
    if isinstance(process_pid, bool) or not isinstance(process_pid, int) or process_pid <= 0:
        return None
    return status, owner, process_pid


def _wait_for_probe_claim(
    conn: JobsConnection,
    probe_id: UUID,
    expected_worker_id: str,
    recovery_worker_pid: int,
    timeout_seconds: float,
) -> bool:
    """Wait until the exact recovery worker claims the probe job.

    ``worker_id`` alone is not proof of identity: it defaults to the host name
    and is shared by every worker on the machine. The claim is therefore bound
    to the exact supplied recovery worker PID by waiting for the persisted
    ``process_pid`` of the probe command and verifying, from ``/proc``, that
    the command process is a descendant of the recovery worker. The worker_id
    match is retained as an additional check.

    Args:
        conn: Open PostgreSQL connection.
        probe_id: Probe job identifier.
        expected_worker_id: Worker identifier the recovery worker records.
        recovery_worker_pid: Exact PID of the worker being adopted.
        timeout_seconds: Maximum seconds to wait.

    Returns:
        ``True`` only when the exact recovery worker claimed and executed the
        probe; ``False`` on timeout, terminal status, a different worker_id,
        or a claim whose process was not spawned by the recovery worker.
    """
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        with conn.cursor(row_factory=tuple_row) as cursor:
            cursor.execute(
                "SELECT (payload::jsonb)->'state' FROM lubko.jobs WHERE id = %s",
                (probe_id,),
            )
            row = cursor.fetchone()
        if row is None:
            return False
        claim = _parse_probe_claim_state(row[0])
        if claim is None:
            return False
        status, owner, process_pid = claim
        if status == STATE_RUNNING:
            if owner != expected_worker_id:
                return False
            if process_pid is None:
                time.sleep(LOCK_POLL_INTERVAL_SECONDS)
                continue
            return _spawned_by_recovery_worker(process_pid, recovery_worker_pid)
        if status in {"succeeded", "failed", "cancelled"}:
            return False
        time.sleep(LOCK_POLL_INTERVAL_SECONDS)
    return False


def _wait_for_probe_terminal(
    conn: JobsConnection,
    probe_id: UUID,
    timeout_seconds: float,
) -> None:
    """Wait until the probe job reaches a terminal state or is deleted.

    The recovery worker cancels and reaps the probe process group; waiting for
    the terminal row before deleting it guarantees the roundtrip leaves no
    probe process behind even when the worker is slow.

    Args:
        conn: Open PostgreSQL connection.
        probe_id: Probe job identifier.
        timeout_seconds: Maximum seconds to wait.
    """
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        with conn.cursor(row_factory=tuple_row) as cursor:
            cursor.execute(
                "SELECT (payload::jsonb)->'state'->>'status' FROM lubko.jobs WHERE id = %s",
                (probe_id,),
            )
            row = cursor.fetchone()
        if row is None:
            return
        if str(row[0]) in {"succeeded", "failed", "cancelled"}:
            return
        time.sleep(LOCK_POLL_INTERVAL_SECONDS)


def verify_worker_consumes_queue(
    worker_id: str,
    cwd: str,
    worker_pid: int,
    timeout_seconds: float,
) -> bool:
    """Prove an exact worker process consumes the queue through a real roundtrip.

    This is the public readiness proof used by the external supervisor: PID
    aliveness and database connectivity do not prove a worker is the queue
    consumer, so a probe job must be claimed and executed by the exact process.
    The probe is cancelled, awaited terminal, and removed in all cases.

    Args:
        worker_id: Worker identifier the worker records on claims.
        cwd: Working directory for the probe job.
        worker_pid: Exact PID of the worker process to prove.
        timeout_seconds: Maximum seconds to wait for the probe to be claimed.

    Returns:
        ``True`` only when the exact worker consumed the probe.
    """
    return _verify_queue_roundtrip(worker_id, cwd, worker_pid, timeout_seconds)


def _verify_queue_roundtrip(
    worker_id: str,
    cwd: str,
    recovery_worker_pid: int,
    timeout_seconds: float,
) -> bool:
    """Verify the exact recovery worker really consumes the queue.

    A probe job is inserted and must be claimed and executed by the exact
    recovery worker: the claim is bound to the supplied PID through the
    persisted ``process_pid`` descendant check, with ``worker_id`` as an
    additional check. If any other worker claims the probe, one-consumer
    semantics are violated and the repair fails. The probe is cancelled,
    awaited terminal, and removed in all cases, so the roundtrip leaves no
    queue row and no process behind.

    Args:
        worker_id: Worker identifier the recovery worker will record on claims.
        cwd: Working directory for the probe job.
        recovery_worker_pid: Exact PID of the worker being adopted.
        timeout_seconds: Maximum seconds to wait for the probe to be claimed.

    Returns:
        ``True`` only when the exact worker consumed the probe.
    """
    try:
        database = load_database_config()
    except (OSError, ValueError):
        return False
    try:
        conn = psycopg.connect(database.conninfo(), row_factory=tuple_row)
    except (psycopg.Error, OSError):
        return False
    conn.autocommit = True
    try:
        probe_id = _insert_probe_job(conn, cwd)
        if probe_id is None:
            return False
        try:
            outcome = _wait_for_probe_claim(
                conn, probe_id, worker_id, recovery_worker_pid, timeout_seconds
            )
        finally:
            with suppress(psycopg.Error):
                request_cancel(conn, probe_id, server=_probe_server())
            _wait_for_probe_terminal(conn, probe_id, timeout_seconds)
            with suppress(psycopg.Error):
                delete_job_and_chunks(conn, probe_id, server=_probe_server())
        return outcome
    finally:
        conn.close()


def _cleanup_ready_markers(recovery_worker_pid: int) -> None:
    """Remove stale readiness markers that do not describe the adopted worker.

    A readiness marker is kept only when it records the exact adopted process
    identity; any other marker (stale token, dead identity, or unparseable
    test residue) is removed.

    Args:
        recovery_worker_pid: Exact PID of the adopted worker.
    """
    directory = worker_state_dir()
    if not directory.is_dir():
        return
    for path in directory.iterdir():
        if not path.name.startswith("ready-"):
            continue
        try:
            data = json.loads(path.read_text())
        except (OSError, ValueError):
            with suppress(OSError):
                path.unlink(missing_ok=True)
            continue
        marker_pid = data.get("pid") if isinstance(data, dict) else None
        valid_marker = (
            isinstance(marker_pid, int)
            and not isinstance(marker_pid, bool)
            and marker_pid == recovery_worker_pid
        )
        if not valid_marker:
            with suppress(OSError):
                path.unlink(missing_ok=True)


def _reconcile_toolchain(uv_path: str) -> None:
    """Rewrite the maintained toolchain record when it is unusable.

    A stale test-produced or otherwise unusable ``toolchain.json`` is replaced
    with the resolved executable actually used for the repair.

    Args:
        uv_path: The resolved ``uv`` executable.
    """
    recorded = toolchain.read_toolchain()
    if recorded is None or not toolchain.is_executable(recorded.uv_path):
        toolchain.write_toolchain(uv_path)


def _adoption_candidate(
    options: DeployOptions,
    recovery_worker_pid: int,
    commit: str,
) -> tuple[WorkerMeta, str]:
    """Verify a recovery worker and build the metadata that adopts it.

    Never trusts stale metadata: the adopted identity comes from the exact PID
    the operator supplies, verified alive, session/process-group leader,
    genuinely a Lubko worker, PostgreSQL-reachable, and proven to consume the
    queue. A stale rollback mission or any other live recorded identity blocks
    the adoption.

    Args:
        options: Deployment inputs.
        recovery_worker_pid: Exact PID of the running recovery worker.
        commit: Exact commit the recovery worker runs.

    Returns:
        ``(new_meta, worker_id)`` for the verified worker.

    Raises:
        _AdoptionError: If any adoption check fails or a live mission or other
            live identity blocks the adoption.
    """
    if not require_clean_checkout(options.repo, options.git_timeout_seconds):
        msg = "repair checkout is dirty; commit or discard working-tree changes first"
        raise _AdoptionError(msg)
    identity = process_identity(recovery_worker_pid)
    if identity is None:
        msg = f"recovery worker pid {recovery_worker_pid} is not a live process"
        raise _AdoptionError(msg)
    if not (identity.pgid == recovery_worker_pid and identity.sid == recovery_worker_pid):
        msg = f"pid {recovery_worker_pid} is not a session/process-group leader; refusing to adopt"
        raise _AdoptionError(msg)
    if not _is_lubko_worker_process(recovery_worker_pid):
        msg = f"pid {recovery_worker_pid} is not a Lubko worker process; refusing to adopt"
        raise _AdoptionError(msg)
    if not check_postgres(options.postgres_timeout_seconds):
        msg = "cannot reach PostgreSQL; refusing to adopt the recovery worker"
        raise _AdoptionError(msg)

    previous = read_meta()
    if previous is not None and worker_alive(previous) and previous.pid != recovery_worker_pid:
        msg = f"a live maintained worker pid {previous.pid} is already recorded; stop it first"
        raise _AdoptionError(msg)

    _repair_rollback_state(recovery_worker_pid)

    process_env = _read_process_env(recovery_worker_pid)
    worker_id = process_env.get("LUBKO_WORKER_ID") or socket.gethostname()
    if not _verify_queue_roundtrip(
        worker_id, str(options.repo), recovery_worker_pid, options.probe_timeout_seconds
    ):
        msg = (
            f"recovery worker pid {recovery_worker_pid} did not consume the queue as "
            f"worker {worker_id!r}; refusing to adopt"
        )
        raise _AdoptionError(msg)
    return WorkerMeta(
        schema_version=SCHEMA_VERSION,
        state=STATE_RUNNING,
        pid=identity.pid,
        pgid=identity.pgid,
        sid=identity.sid,
        start_time_ticks=identity.start_time_ticks,
        token=process_env.get(LIFECYCLE_MARKER_VAR),
        repo=str(options.repo),
        git_commit=commit,
        worker_id=worker_id,
        log_path=str(worker_log_path(process_env.get(LIFECYCLE_MARKER_VAR))),
        started_at=time.time(),
        stopped_at=None,
    ), worker_id


def _adoption_matches_obligation(
    obligation: supervise.SpawningObligation, meta: WorkerMeta
) -> bool:
    """Return whether a durable recovery authority names exactly this worker.

    Only an exact match — same PID, same start ticks, same lifecycle token,
    and the pid-less manual-recovery shape (no kernel parent-death guarantee)
    that a ``recover`` spawn always records — releases the authority.

    Args:
        obligation: The durable pre-spawn recovery obligation.
        meta: The verified metadata of the adopted recovery worker.

    Returns:
        ``True`` when the obligation describes exactly the adopted worker.
    """
    return (
        obligation.pid == meta.pid
        and obligation.start_time_ticks == meta.start_time_ticks
        and obligation.token == meta.token
        and not obligation.parent_death_signal
    )


def _release_adoption_authority(meta: WorkerMeta) -> str | None:
    """Durably release the recovery authority naming exactly this worker.

    The shared supervisor state is re-read immediately before releasing so a
    concurrently replaced or malformed authority is never cleared: an absent
    hold releases nothing, a malformed hold and a mismatched record both fail
    closed. The caller must hold the shared consumer-establishment lock
    across validation and this release so recover or a supervisor
    establishment cannot interleave with either step.

    Args:
        meta: The verified metadata of the adopted recovery worker.

    Returns:
        ``None`` on success (including a genuinely absent hold), otherwise a
        failure message for the operator.
    """
    current = supervise.read_state()
    if current.spawning_hold_malformed:
        return (
            "the recovery worker was adopted, but its durable recovery authority "
            "became malformed; it keeps blocking every consumer until an explicit "
            "operator repair resolves it"
        )
    if current.spawning is not None and not _adoption_matches_obligation(current.spawning, meta):
        return (
            "the durable recovery authority changed during repair; it stays "
            "blocking until it is resolved"
        )
    if current.spawning is not None and not _clear_spawning_obligation():
        return (
            "the recovery worker was adopted, but its durable recovery authority "
            "could not be released; it keeps blocking every consumer until a "
            "later repair releases it"
        )
    return None


def _pre_adoption_authority_error(meta: WorkerMeta) -> str | None:
    """Return why ``meta`` may not be adopted under the durable authority.

    Args:
        meta: The verified metadata of the candidate recovery worker.

    Returns:
        ``None`` when adoption may proceed, otherwise a failure message.
    """
    state = supervise.read_state()
    if state.spawning_hold_malformed:
        return (
            "the durable pre-spawn recovery obligation is malformed; only an explicit "
            "operator repair of the supervisor state may clear it before adoption"
        )
    if state.spawning is not None and not _adoption_matches_obligation(state.spawning, meta):
        return (
            "a durable recovery authority names another worker instance; resolve it "
            "(for example with 'lubko-deploy recover') before adopting a different one"
        )
    return None


def _stale_candidate_error(new_meta: WorkerMeta) -> str | None:
    """Return why a pre-validated adoption candidate is no longer adoptable.

    The candidate was proven outside the shared consumer-establishment lock.
    Before any authoritative write under that lock it must still be the exact
    same live private-session process carrying its lifecycle token, and no
    newer live maintained worker may have been published meanwhile.

    Args:
        new_meta: The previously validated candidate metadata.

    Returns:
        ``None`` when the candidate is still exactly valid, otherwise a
        failure message.
    """
    if new_meta.pid is None:
        return "the adoption candidate has no process identity to re-prove"
    identity = process_identity(new_meta.pid)
    if (
        identity is None
        or identity.pgid != new_meta.pgid
        or identity.sid != new_meta.sid
        or identity.start_time_ticks != new_meta.start_time_ticks
    ):
        return (
            f"recovery worker pid {new_meta.pid} changed or exited before its "
            "adoption could be published; refusing stale metadata"
        )
    if new_meta.token is None or not process_has_token(new_meta.pid, new_meta.token):
        return (
            f"recovery worker pid {new_meta.pid} no longer carries the exact "
            "lifecycle token it was validated with; refusing stale metadata"
        )
    previous = read_meta()
    if previous is not None and worker_alive(previous) and previous.pid != new_meta.pid:
        return (
            f"a newer live maintained worker pid {previous.pid} is already recorded; "
            "refusing to overwrite it with stale recovery-worker metadata"
        )
    return None


def _report_adoption(new_meta: WorkerMeta, worker_id: str, commit: str, *, cli_ok: bool) -> int:
    """Report a completed adoption and honor CLI reconciliation failures.

    Args:
        new_meta: The adopted worker's published metadata.
        worker_id: The adopted worker's id.
        commit: The commit the repair checkout is on.
        cli_ok: Whether the maintained CLIs were reconciled.

    Returns:
        A process exit code.
    """
    append_deploy_log(f"repaired: adopted recovery worker pid={new_meta.pid} commit={commit}")
    _out(f"adopted recovery worker pid={new_meta.pid} pgid={new_meta.pgid} session={new_meta.sid}")
    _out(f"worker id: {worker_id}")
    _out(f"git commit: {commit}")
    _out(f"log: {new_meta.log_path}")
    if not cli_ok:
        _err(
            "error: maintained CLIs could not be reconciled; run 'lubko-deploy-ctl status' "
            "to repair"
        )
        return EXIT_ERROR
    return EXIT_OK


def _repair_locked(options: DeployOptions, recovery_worker_pid: int) -> int:
    """Adopt an independently known recovery worker into coherent metadata.

    This is the deliberate supported recovery path for lifecycle state already
    corrupted (for example by pre-isolation test runs). Adoption is verified
    before any metadata is rewritten; only then is the maintained CLI pointer
    reconciled and stale test-produced state removed. When a durable recovery
    authority exists it must name exactly the adopted worker; it stays
    replacement-blocking until the maintained metadata is published and the
    post-repair queue verification succeeds, then it is released durably. A
    release that cannot be confirmed leaves the hold blocking for a later
    repair instead of reporting success with an unrepresented consumer.

    The authority validation through its release runs under the shared
    consumer-establishment lock (deployment lock first, then this lock), so a
    concurrent recover or supervisor establishment can neither replace the
    validated authority mid-repair nor race the release.

    Args:
        options: Deployment inputs.
        recovery_worker_pid: Exact PID of the running recovery worker.

    Returns:
        A process exit code.
    """
    commit = git_commit(options.repo, options.git_timeout_seconds)
    if commit is None:
        _err("could not read the git commit of the repair checkout")
        return EXIT_ERROR
    try:
        new_meta, worker_id = _adoption_candidate(options, recovery_worker_pid, commit)
    except _AdoptionError as exc:
        _err(str(exc))
        return EXIT_ERROR
    try:
        with supervise.consumer_lock(options.lock_timeout_seconds):
            return _repair_authority_transition_locked(
                options, recovery_worker_pid, commit, new_meta, worker_id
            )
    except supervise.ConsumerLockTimeoutError:
        _err(
            "the supervisor is currently establishing a queue consumer; refusing "
            "to interleave the adoption of the recovery worker"
        )
        return EXIT_ERROR


def _repair_authority_transition_locked(
    options: DeployOptions,
    recovery_worker_pid: int,
    commit: str,
    new_meta: WorkerMeta,
    worker_id: str,
) -> int:
    """Run the authority validation and release under the consumer lock.

    Args:
        options: Deployment inputs.
        recovery_worker_pid: Exact PID of the running recovery worker.
        commit: The commit of the repair checkout.
        new_meta: The verified metadata of the candidate recovery worker.
        worker_id: The verified worker id.

    Returns:
        A process exit code.
    """
    stale_error = _stale_candidate_error(new_meta)
    if stale_error is not None:
        _err(stale_error)
        return EXIT_ERROR
    authority_error = _pre_adoption_authority_error(new_meta)
    if authority_error is not None:
        _err(authority_error)
        return EXIT_ERROR
    write_meta(new_meta)
    cli_ok = cli.reconcile_pointer(commit)
    cli.gc_cli_roots((commit,))
    _cleanup_ready_markers(recovery_worker_pid)
    _reconcile_toolchain(options.uv_path)
    if not _verify_queue_roundtrip(
        worker_id, str(options.repo), recovery_worker_pid, options.probe_timeout_seconds
    ):
        _err(
            "post-repair verification failed: the adopted worker no longer exclusively "
            "consumes the queue; refusing to report success"
        )
        return EXIT_ERROR
    release_error = _release_adoption_authority(new_meta)
    if release_error is not None:
        _err(release_error)
        return EXIT_ERROR
    return _report_adoption(new_meta, worker_id, commit, cli_ok=cli_ok)


def repair(options: DeployOptions, recovery_worker_pid: int) -> int:
    """Adopt an independently known recovery worker under the deploy lock.

    Args:
        options: Deployment inputs.
        recovery_worker_pid: Exact PID of the running recovery worker.

    Returns:
        A process exit code.
    """
    try:
        with deploy_lock(options.lock_timeout_seconds):
            return _repair_locked(options, recovery_worker_pid)
    except LockTimeoutError:
        _err("another deployment is already running; refusing to race")
        return EXIT_ERROR


# ---------------------------------------------------------------------------
# Recover
# ---------------------------------------------------------------------------


def _wait_for_any_claim(
    conn: JobsConnection,
    probe_id: UUID,
    timeout_seconds: float,
) -> bool:
    """Wait until any worker claims the probe job.

    Args:
        conn: Open PostgreSQL connection.
        probe_id: Probe job identifier.
        timeout_seconds: Maximum seconds to wait.

    Returns:
        ``True`` when any worker claims the probe while it runs.
    """
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        with conn.cursor(row_factory=tuple_row) as cursor:
            cursor.execute(
                "SELECT (payload::jsonb)->'state'->>'status' FROM lubko.jobs WHERE id = %s",
                (probe_id,),
            )
            row = cursor.fetchone()
        if row is None:
            return False
        if str(row[0]) == STATE_RUNNING:
            return True
        if str(row[0]) in {"succeeded", "failed", "cancelled"}:
            return False
        time.sleep(LOCK_POLL_INTERVAL_SECONDS)
    return False


def _queue_has_consumer(cwd: str, timeout_seconds: float) -> bool:
    """Return whether any worker is currently consuming the queue.

    A probe job is inserted and must not be claimed by any worker: a claim
    proves a live consumer already exists, which would make starting a second
    recovery worker unsafe. The probe is cancelled, awaited terminal, and
    removed in all cases.

    Args:
        cwd: Working directory for the probe command.
        timeout_seconds: Maximum seconds to wait for a claim.

    Returns:
        ``True`` when some worker claimed the probe.
    """
    try:
        database = load_database_config()
    except (OSError, ValueError):
        return False
    try:
        conn = psycopg.connect(database.conninfo(), row_factory=tuple_row)
    except (psycopg.Error, OSError):
        return False
    conn.autocommit = True
    try:
        probe_id = _insert_probe_job(conn, cwd)
        if probe_id is None:
            return False
        try:
            return _wait_for_any_claim(conn, probe_id, timeout_seconds)
        finally:
            with suppress(psycopg.Error):
                request_cancel(conn, probe_id, server=_probe_server())
            _wait_for_probe_terminal(conn, probe_id, timeout_seconds)
            with suppress(psycopg.Error):
                delete_job_and_chunks(conn, probe_id, server=_probe_server())
    finally:
        conn.close()


def _recover_preflight(options: DeployOptions) -> str:
    """Return the recovery checkout commit after preflight safety checks.

    A recovery worker may only be started when no maintained worker is live,
    the checkout is clean, PostgreSQL is reachable, and no worker is already
    consuming the queue; otherwise a second consumer would be created.

    Args:
        options: Deployment inputs.

    Returns:
        The exact checkout commit the recovery worker will run.

    Raises:
        _AdoptionError: If a recovery worker cannot safely be started.
    """
    previous = read_meta()
    if previous is not None and worker_alive(previous):
        msg = (
            f"a live maintained worker pid {previous.pid} is already recorded; "
            "no recovery worker is needed"
        )
        raise _AdoptionError(msg)
    if not require_clean_checkout(options.repo, options.git_timeout_seconds):
        msg = "recovery checkout is dirty; commit or discard working-tree changes first"
        raise _AdoptionError(msg)
    commit = git_commit(options.repo, options.git_timeout_seconds)
    if commit is None:
        msg = "could not read the git commit of the recovery checkout"
        raise _AdoptionError(msg)
    if not check_postgres(options.postgres_timeout_seconds):
        msg = "cannot reach PostgreSQL; refusing to start a recovery worker"
        raise _AdoptionError(msg)
    if _queue_has_consumer(
        str(options.repo), min(options.probe_timeout_seconds, DEFAULT_RECOVER_PREFLIGHT_SECONDS)
    ):
        msg = (
            "a worker is already consuming the queue; adopt the existing worker with "
            "'lubko-deploy repair --recovery-worker-pid <PID>' instead of starting a "
            "second consumer"
        )
        raise _AdoptionError(msg)
    return commit


def _recover_owned_groups(incarnation: str) -> bool:
    """Recover every command group owned by an exact worker incarnation.

    This is a seam over the maintained supervisor's exact-incarnation
    owned-group recovery: command jobs run in independent sessions/process
    groups, so reaping a worker PID never implies its job groups are gone.
    Only exact persisted process-group identities are signalled, always via
    pidfd/start-time proof — never name matching or broad numeric kills.

    Args:
        incarnation: The worker lifecycle token whose groups must be recovered.

    Returns:
        ``True`` when recovery provably succeeded; ``False`` when it failed
        and the incarnation's execution authority must not be dropped.
    """
    # ruff: ignore[import-outside-top-level] - supervisor imports this module
    from lubko.supervisor import OwnedGroupRecoveryError, recover_owned_groups

    try:
        recover_owned_groups(incarnation)
    except OwnedGroupRecoveryError:
        LOGGER.exception("owned-group recovery failed for incarnation %s", incarnation)
        return False
    return True


def _obligation_instance_gone(pid: int | None, start_time_ticks: int | None) -> bool:
    """Return whether an obligation's exact recorded instance is provably gone.

    Args:
        pid: The recorded child PID, or ``None`` when never published.
        start_time_ticks: The recorded start time ticks, or ``None``.

    Returns:
        ``True`` only when the PID is absent-and-unpublishable or a live
        process with that PID carries different (or unreadable) start ticks.
    """
    if pid is None:
        return False
    observed = proc_start_ticks(pid)
    if start_time_ticks is None:
        return observed is None
    return observed != start_time_ticks


def _clear_spawning_obligation() -> bool:
    """Durably clear the pre-spawn recovery obligation.

    Returns:
        ``True`` when the state write was confirmed durable.
    """
    try:
        supervise.write_state(replace(supervise.read_state(), spawning=None))
    except DurabilityError:
        LOGGER.exception("could not durably clear the pre-spawn recovery obligation")
        return False
    return True


def _write_spawning_obligation(obligation: supervise.SpawningObligation) -> bool:
    """Durably persist ``obligation`` as the replacement-blocking authority.

    Args:
        obligation: The obligation to record.

    Returns:
        ``True`` when the state write was confirmed durable.
    """
    try:
        supervise.write_state(replace(supervise.read_state(), spawning=obligation))
    except DurabilityError:
        LOGGER.exception(
            "could not durably record the recovery obligation for token %s", obligation.token
        )
        return False
    return True


def _recovery_obligation(token: str, commit: str) -> supervise.SpawningObligation:
    """Build a pid-less pre-spawn obligation for a manual recovery worker.

    Args:
        token: The recovery worker's lifecycle token.
        commit: The commit the worker will be started for.

    Returns:
        The durable replacement-blocking obligation.
    """
    return supervise.SpawningObligation(
        token=token,
        commit=commit,
        creator_pid=os.getpid(),
        creator_start_time_ticks=proc_start_ticks(os.getpid()) or 0,
        pid=None,
        start_time_ticks=None,
        created_at=time.time(),
        boot_id=supervise.current_boot_id(),
        # A manually spawned detached worker carries no kernel parent-death
        # guarantee, so no successor may ever resolve this record by
        # assumption; it must be positively resolved or repaired.
        parent_death_signal=False,
    )


def _resolve_stale_recovery_obligation() -> bool:
    """Resolve a leftover pre-spawn obligation before spawning a new consumer.

    A previous failed or interrupted recovery durably recorded that a worker
    token's owned command groups may be unresolved. Before this command may
    start another queue consumer, that exact instance must be provably gone
    *and* its owned groups successfully recovered. Anything else — including a
    pid-less record that cannot be resolved by assumption at all, or a
    malformed authority whose true fate is unreadable — fails
    closed so no replacement consumer races unresolved side-effecting process
    groups.

    Returns:
        ``True`` when no blocking obligation remains.
    """
    state = supervise.read_state()
    if state.spawning_hold_malformed:
        # The pre-spawn authority is present but unreadable: its recorded
        # spawn may still be live and owning groups. This deliberately does
        # not self-heal and is never overwritten; only operator repair clears
        # it, exactly as for the maintained supervisor.
        return False
    obligation = state.spawning
    if obligation is None:
        return True
    if obligation.pid is None:
        return False
    if not _obligation_instance_gone(obligation.pid, obligation.start_time_ticks):
        return False
    if not _recover_owned_groups(obligation.token):
        return False
    return _clear_spawning_obligation()


def _converge_failed_recovery_worker(
    proc: subprocess.Popen[bytes],
    options: DeployOptions,
    token: str,
    anchor: ProcessIdentity | None,
) -> int:
    """Converge an unproven recovery worker without dropping owned groups.

    The direct child is converged first exactly as before (pinned pidfd
    signalling when an anchor exists, positive reap otherwise). Then every
    command group owned by the worker's exact incarnation is recovered, since
    worker death does not imply job-group death. Only after owned-group
    recovery succeeds is the pre-spawn obligation released and an ordinary
    failure returned. When owned-group recovery fails, the already-durable
    shared supervisor-state obligation keeps blocking every later consumer —
    manual recover and maintained supervisor alike — until it resolves.

    Args:
        proc: The direct recovery worker's ``Popen`` handle.
        options: Deployment inputs.
        token: The recovery worker's lifecycle token.
        anchor: The exact identity anchor for pinned convergence, or ``None``.

    Returns:
        A process exit code.
    """
    _converge_unproven_spawn(proc, options.stop_grace_seconds, anchor)
    if not _recover_owned_groups(token):
        _err(
            "owned command groups of the converged recovery worker could not be recovered; "
            f"durable recovery authority for token {token} blocks any replacement consumer "
            "until 'lubko-deploy recover' resolves it"
        )
        return EXIT_ERROR
    if not _clear_spawning_obligation():
        LOGGER.warning("recovery authority for token %s stays blocking until resolved", token)
    append_deploy_log(f"recovered owned groups for converged recovery worker token={token}")
    return EXIT_ERROR


def _settle_spawned_recovery_worker(
    proc: subprocess.Popen[bytes],
    options: DeployOptions,
    token: str,
    commit: str,
    worker_id: str,
) -> int:
    """Classify the spawned recovery worker and settle its fate.

    Args:
        proc: The direct ``Popen`` handle of the spawned worker.
        options: Deployment inputs.
        token: The recovery worker's lifecycle token.
        commit: The commit the worker was started for.
        worker_id: The reported worker id.

    Returns:
        A process exit code.
    """
    identity = _wait_for_identity(proc.pid)
    anchor: ProcessIdentity | None
    if identity is None:
        if proc.poll() is None:
            # Still live but never observable: there is no exact anchor, so
            # convergence may only fail closed and reap the direct child.
            _err("recovery worker stayed live without an observable identity; converging it")
        else:
            _err("recovery worker exited before establishing a dedicated session")
        anchor = None
    elif identity.pgid != proc.pid or identity.sid != proc.pid:
        _err(
            "recovery worker timed out before establishing its private session; "
            "converging it before failing"
        )
        anchor = identity
    elif proc.poll() is not None:
        # The child established its session but already exited: never report
        # an adoptable PID for a dead process. It may have consumed jobs, so
        # its owned command groups must be recovered before the token is
        # forgotten.
        _err("recovery worker exited before it could be adopted")
        anchor = None
    else:
        # The healthy path keeps the already-durable obligation naming this
        # exact live worker: it stays the sole replacement-blocking recovery
        # authority until 'lubko-deploy repair' adopts it (clearing the
        # authority) or a later convergence proves the exact instance gone and
        # recovers its owned groups. Clearing here would leave a live consumer
        # unrepresented, letting a supervisor restart spawn a second one.
        append_deploy_log(f"recovery worker started pid={identity.pid} commit={commit}")
        _out(f"recovery worker pid={identity.pid} pgid={identity.pgid} session={identity.sid}")
        _out(f"worker id: {worker_id}")
        _out(f"git commit: {commit}")
        _out(
            f"adopt it with: lubko-deploy repair --repo {options.repo} "
            f"--recovery-worker-pid {identity.pid}"
        )
        return EXIT_OK
    return _converge_failed_recovery_worker(proc, options, token, anchor)


def _recover_locked(options: DeployOptions) -> int:
    """Start a detached recovery worker and report its adoptable identity.

    The worker is started with the same detached session/process-group-leader
    mechanism a deployment replacement uses, so its exact PID is a stable
    dedicated leader that ``lubko-deploy repair --recovery-worker-pid`` can
    safely adopt later. No worker lifecycle metadata (``meta.json``) is
    written here.

    A shared durable recovery authority is established in the supervisor
    state *before* the spawn, so the spawned token's execution ownership is
    never held without durable protection. Every failure exit after a
    successful spawn first converges the direct child and then recovers the
    exact incarnation's owned command groups; only then may the authority be
    released. If that recovery fails, the obligation keeps blocking every
    later consumer — manual recover and maintained supervisor alike — until a
    subsequent run resolves it. On success the obligation naming the exact
    live recovery worker stays durably in place: it is the sole authority
    until repair adopts the worker or convergence plus owned-group recovery
    proves the instance gone.

    The whole preflight-to-publication critical section runs under the
    shared consumer-establishment lock the maintained supervisor holds
    around its own gate-to-spawn decision, so from one initially
    consumer-free state exactly one of the two paths can authorize a spawn:
    a stale preflight observation can never outlive competing supervisor
    authority, and the supervisor can never overwrite an established manual
    recovery obligation and spawn beside it.

    Args:
        options: Deployment inputs.

    Returns:
        A process exit code.
    """
    try:
        with supervise.consumer_lock(options.lock_timeout_seconds):
            return _recover_consumer_locked(options)
    except supervise.ConsumerLockTimeoutError:
        _err(
            "the supervisor is currently establishing a queue consumer; "
            "refusing to race it into a second consumer"
        )
        return EXIT_ERROR


def _recover_consumer_locked(options: DeployOptions) -> int:
    """Start a detached recovery worker while holding the consumer lock.

    Args:
        options: Deployment inputs.

    Returns:
        A process exit code.
    """
    try:
        commit = _recover_preflight(options)
    except _AdoptionError as exc:
        _err(str(exc))
        return EXIT_ERROR
    if not _resolve_stale_recovery_obligation():
        _err(
            "a previously recorded recovery obligation could not be resolved; refusing "
            "to start another consumer beside possibly-live owned command groups"
        )
        return EXIT_ERROR
    token = secrets.token_hex(16)
    env = worker_env(token)
    worker_id = env.get("LUBKO_WORKER_ID") or socket.gethostname()
    # Durable shared recovery authority, written and fsync-confirmed BEFORE
    # the spawn: from this instant the token's execution ownership is recorded
    # in the supervisor state every consumer-establishing path honors, so no
    # crash or failure can ever relinquish authority without durable
    # protection. The record carries no parent-death guarantee, so no
    # successor may resolve it by assumption.
    obligation = _recovery_obligation(token, commit)
    if not _write_spawning_obligation(obligation):
        _err(
            "could not durably establish the shared recovery authority; "
            "not starting a recovery worker"
        )
        return EXIT_ERROR
    try:
        proc = spawn_worker(options.repo, options.uv_path, worker_log_path(token), env)
    except OSError as exc:
        _err(f"could not start the recovery worker: {exc}")
        if not _clear_spawning_obligation():
            LOGGER.warning("the pid-less authority for token %s stays blocking", token)
        return EXIT_ERROR
    obligation = replace(obligation, pid=proc.pid, start_time_ticks=proc_start_ticks(proc.pid))
    if not _write_spawning_obligation(obligation):
        LOGGER.warning(
            "could not publish the exact identity of recovery worker %s; "
            "the pid-less pre-spawn authority remains blocking",
            token,
        )
    return _settle_spawned_recovery_worker(proc, options, token, commit, worker_id)


def recover(options: DeployOptions) -> int:
    """Start a detached recovery worker under the deploy lock.

    Args:
        options: Deployment inputs.

    Returns:
        A process exit code.
    """
    try:
        with deploy_lock(options.lock_timeout_seconds):
            return _recover_locked(options)
    except LockTimeoutError:
        _err("another deployment is already running; refusing to race")
        return EXIT_ERROR


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _out(message: str) -> None:
    """Write a user-facing line to standard output.

    Args:
        message: Message to write.
    """
    sys.stdout.write(message + "\n")


def _err(message: str) -> None:
    """Write a user-facing line to standard error.

    Args:
        message: Message to write.
    """
    sys.stderr.write(message + "\n")


def _print_supervisor_status() -> None:
    """Report whether an external supervisor guards the maintained worker.

    The supervisor owns the worker process and restores it after an unexpected
    exit; its presence is the difference between a crash that self-heals and a
    crash that requires a human.
    """
    if not supervise.supervisor_running():
        _out("supervisor: not running (no automatic worker restart)")
        return
    status = supervise.read_status()
    if status is None:
        _out("supervisor: running (status not yet published)")
        return
    _out(f"supervisor: running (pid {status.supervisor_pid})")
    _out(f"supervisor generation: {status.applied_generation}")
    _out(f"supervisor mode: {status.mode}")
    if status.commit is not None:
        _out(f"supervisor commit: {status.commit}")
    if status.mission is not None:
        _out(f"supervisor mission: {status.mission}")
    if status.restart_count:
        _out(f"supervisor restarts: {status.restart_count}")
        if status.next_attempt_at is not None:
            retry = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(status.next_attempt_at))
            _out(f"supervisor next retry: {retry}")
    if status.last_exit is not None:
        _out(f"last worker exit code: {status.last_exit.returncode}")
    if status.db_ready is not None:
        _out(f"supervisor database reachable: {status.db_ready}")
    if status.message is not None:
        _out(f"supervisor message: {status.message}")


def status_cmd() -> int:
    """Show the effective worker lifecycle state.

    Returns:
        A process exit code.
    """
    meta = read_meta()
    state = worker_state(meta)
    _out(f"state: {state}")
    if state == STATE_UNMANAGED:
        _out(UNMANAGED_WORKER_MESSAGE)
        _out("after stopping the legacy worker manually once, run: lubko-deploy deploy --bootstrap")
        _print_supervisor_status()
        _print_startup_contract()
        return EXIT_OK
    if meta is None:
        _print_supervisor_status()
        _print_startup_contract()
        return EXIT_OK
    _out(f"pid: {meta.pid}")
    _out(f"pgid: {meta.pgid}")
    _out(f"session: {meta.sid}")
    _out(f"git commit: {meta.git_commit or 'unknown'}")
    _out(f"worker id: {meta.worker_id or 'unknown'}")
    _out(f"log: {meta.log_path}")
    if meta.started_at is not None:
        started = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(meta.started_at))
        _out(f"started at: {started}")
    if meta.stopped_at is not None:
        stopped = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(meta.stopped_at))
        _out(f"stopped at: {stopped}")
    if meta.git_commit is not None:
        active = cli.current_commit()
        if active != meta.git_commit:
            _err(
                f"warning: maintained CLIs resolve to {active or 'nothing'}, not the maintained "
                f"worker commit {meta.git_commit}; run lubko-deploy-ctl status to reconcile"
            )
    _print_supervisor_status()
    _print_startup_contract()
    return EXIT_OK


def _print_startup_contract() -> None:
    """Report the versioned startup contract and its live topology proof.

    The contract is the authoritative, repository-owned definition of how the
    container must start the supervisor; the proof demonstrates the live
    process topology actually matches it, rather than merely inferring worker
    liveness from queue state.
    """
    assessment = startup_contract.assess_recorded_contract()
    if assessment.state == "current" and assessment.contract is not None:
        _out(f"startup contract: current (version {assessment.contract.schema_version})")
    elif assessment.state == "missing":
        _out("startup contract: MISSING (run 'lubko-install' or 'lubko-deploy bootstrap')")
    elif assessment.state == "corrupt":
        _out(f"startup contract: CORRUPT ({assessment.message})")
    else:
        _out(f"startup contract: MISMATCH ({assessment.message})")
    launcher_ok = startup_contract.validate_startup_launcher(_resolve_bin_home())
    launcher_state = "installed" if launcher_ok else "MISSING"
    _out(f"startup launcher ({startup_contract.STARTUP_LAUNCHER_NAME}): {launcher_state}")
    definition = startup_contract.validate_startup_definition()
    _out(f"startup definition: {'OK' if definition.ok else 'FAIL'} ({definition.message})")
    paths = startup_contract.validate_contract_paths()
    _out(f"startup state paths: {'OK' if paths.ok else 'FAIL'} ({paths.message})")
    config_paths = startup_contract.validate_contract_config()
    _out(f"private config paths: {'OK' if config_paths.ok else 'FAIL'} ({config_paths.message})")
    proof = startup_contract.verify_live_topology()
    _out(f"startup topology: {'OK' if proof.ok else 'FAIL'}")
    _out(f"  init (pid {proof.init_pid}): {proof.init_cmdline or 'unknown'}")
    _out(f"  init is supported tini: {proof.init_is_tini}")
    if proof.supervisor_pid:
        _out(f"  supervisor (pid {proof.supervisor_pid}): {proof.supervisor_cmdline or 'unknown'}")
    _out(f"  supervisor under tini: {proof.supervisor_under_init}")
    _out(f"  supervisor is lubko-supervisor: {proof.supervisor_is_contract_binary}")
    _out(f"  supervisor identity matches recorded: {proof.supervisor_identity_matches}")
    _out(f"  uses sleep-infinity placeholder: {proof.uses_sleep_placeholder}")
    if proof.worker_pid is not None:
        _out(
            f"  worker (pid {proof.worker_pid}) direct child of supervisor: "
            f"{proof.worker_is_direct_child}"
        )
        _out(f"  worker identity matches recorded: {proof.worker_identity_matches}")
    _out(f"  proof: {proof.message}")
    rap = startup_contract.prove_restart_authority(startup_contract.CURRENT_CONTRACT)
    _out(f"restart authority: {'OK' if rap.ok else 'FAIL'} ({rap.source}: {rap.message})")


def startup_contract_cmd(args: argparse.Namespace) -> int:
    """Verify, and optionally publish, the live supervisor startup contract.

    The command requires every supported-deployment boundary to hold before it
    reports the startup contract active, and it fails closed on any missing
    piece. Concretely it requires:

    * the recorded contract to exactly equal the code's current contract
      (missing/malformed/unsupported/mismatch all fail closed);
    * the repository-owned startup launcher to be installed and match the versioned
      source;
    * the installed startup definition to match the current contract exactly;
    * the required private state directories to exist with the exact safe mode;
    * the private config files to exist with no group/world access;
    * the live Tini -> supervisor -> worker topology to be proven; and
    * concrete, configured restart-authority evidence from the deployment seam
      (the contract of record alone is not activation proof).

    It exits non-zero unless every check passes — for example when the container
    still uses the ``sleep infinity`` placeholder, the recorded contract has
    silently drifted, the startup definition is missing, or no restart-policy
    evidence is supplied.

    Args:
        args: Parsed command line arguments.

    Returns:
        A process exit code.
    """
    if getattr(args, "write", False):
        startup_contract.write_contract()
        startup_contract.write_startup_launcher(_resolve_bin_home())
        startup_contract.write_startup_definition()
        _out(
            f"startup contract version {startup_contract.CONTRACT_SCHEMA_VERSION}, "
            f"launcher, and startup definition written; the container must run "
            f"'{startup_contract.STARTUP_LAUNCHER_NAME}' and the deployment seam must "
            f"supply {startup_contract.RESTART_POLICY_ENV} for restart authority"
        )
    assessment = startup_contract.assess_recorded_contract()
    contract_ok = assessment.state == "current"
    if not contract_ok:
        _out(f"startup contract: {assessment.state.upper()} ({assessment.message})")
    _print_startup_contract()
    launcher_ok = startup_contract.validate_startup_launcher(_resolve_bin_home())
    definition_ok = startup_contract.validate_startup_definition().ok
    paths_ok = startup_contract.validate_contract_paths().ok
    config_ok = startup_contract.validate_contract_config().ok
    proof = startup_contract.verify_live_topology()
    rap = startup_contract.prove_restart_authority(startup_contract.CURRENT_CONTRACT)
    return (
        EXIT_OK
        if (
            contract_ok
            and launcher_ok
            and definition_ok
            and paths_ok
            and config_ok
            and proof.ok
            and rap.ok
        )
        else EXIT_ERROR
    )


def restart_cmd(_args: argparse.Namespace) -> int:
    """Restart the currently confirmed commit through the external supervisor.

    A restart never uses Git, the network, or the mutable source checkout: the
    exact commit is read from the supervisor's durable state, a fresh worker
    process is requested for that same commit at a newer generation, and the
    command waits until the supervisor proves the replacement consumes the
    queue. The deployed version never changes.

    A restart submitted through the Lubko queue itself is routed to a detached
    handoff helper, exactly like a queue-invoked deploy: the supervisor retires
    the very worker executing the restart command during the handoff, so without
    the helper the initiating root row would be cancelled by its own old worker.
    The helper validates the restart, reports the outcome so the row is durably
    terminal, waits for that durable success, and only then requests the
    supervisor process replacement.

    Args:
        _args: Parsed command line arguments (unused).

    Returns:
        A process exit code.
    """
    try:
        job_id, cancelled = _current_queue_job()
    except DeployAbortedError as exc:
        _err(str(exc) or "restart was refused")
        return EXIT_ERROR
    if job_id is not None:
        if cancelled:
            _err("restart job was cancelled during deployment")
            return EXIT_ERROR
        try:
            return _queue_restart(job_id)
        except DeployAbortedError as exc:
            _err(str(exc) or "restart was refused")
            return EXIT_ERROR
    return _restart_manual()


def _restart_intent_locked() -> tuple[int | None, int | None, str | None]:
    """Run the supervised-state guard and request a restart under the locks.

    Must be called while the deployment lock is held so the pending/corrupt
    rollback-state guard and the restart intent write are serialized against
    concurrent lifecycle mutation. Nesting ``supervise.request_restart``'s
    generation lock is safe: lock ordering is deployment-lock before
    generation-lock everywhere.

    Returns:
        ``(generation, previous_pid, error)``. On success ``generation`` is the
        written intent generation and ``error`` is ``None``; otherwise
        ``generation`` is ``None`` and ``error`` describes the refusal.
    """
    blocker = _supervised_mutation_blocker()
    if blocker is not None:
        return None, None, blocker
    if not supervise.supervisor_running():
        msg = (
            "no external supervisor is running; a supervised restart is not possible "
            "(the only supported way to stop Lubko is to stop its environment)"
        )
        return None, None, msg
    state = supervise.read_state()
    commit = state.commit
    if commit is None:
        return None, None, "no usable sealed runtime to restart"
    if not cli.runtime_is_usable(commit):
        msg = (
            f"the exact sealed runtime for commit {commit} is missing, corrupt, "
            "incomplete, or not sealed; refusing to restart"
        )
        return None, None, msg
    previous = supervise.read_status()
    previous_pid = (
        previous.child.pid if previous is not None and previous.child is not None else None
    )
    _out(f"requesting a supervised restart of confirmed commit {commit} ...")
    desired = supervise.read_desired()
    generation = supervise.request_restart(
        commit,
        repo=desired.repo if desired is not None else "",
        uv_path=desired.uv_path if desired is not None else "",
        worker_id=(
            desired.worker_id
            if desired is not None
            else os.getenv("LUBKO_WORKER_ID") or socket.gethostname()
        ),
    )
    return generation, previous_pid, None


def _restart_manual() -> int:
    """Restart the confirmed commit synchronously outside a queue job.

    The pending/corrupt supervised-state guard and the restart intent request
    are serialized under the same deployment lock; readiness waits run after
    the lock is released.

    Args:
        None.

    Returns:
        A process exit code.
    """
    try:
        with deploy_lock(DEFAULT_LOCK_TIMEOUT_SECONDS):
            generation, previous_pid, error = _restart_intent_locked()
    except LockTimeoutError as exc:
        _err(f"timed out waiting for the deployment lock: {exc}")
        return EXIT_ERROR
    if error is not None or generation is None:
        _err(error or "restart could not be requested")
        return EXIT_ERROR
    if not supervise.wait_for_generation(generation, supervise.DEFAULT_REQUEST_TIMEOUT_SECONDS):
        _err("the external supervisor did not apply the restart")
        return EXIT_ERROR
    if not supervise.wait_until_ready(generation, supervise.DEFAULT_REQUEST_TIMEOUT_SECONDS):
        _err("the external supervisor did not prove the fresh worker consumes the queue")
        return EXIT_ERROR
    current = supervise.read_status()
    current_pid = current.child.pid if current is not None and current.child is not None else None
    if previous_pid is not None and current_pid == previous_pid:
        _err("the worker process was not replaced by a fresh process")
        return EXIT_ERROR
    _out(f"restart complete: fresh worker pid={current_pid}")
    return EXIT_OK


def _restart_prepared_response(commit: str) -> dict[str, object]:
    """Build the successful restart helper response delivered before the handoff.

    Args:
        commit: Exact confirmed commit being restarted.

    Returns:
        A JSON response object reporting that the restart validated.
    """
    return {
        "ok": True,
        "type": "restart",
        "commit": commit,
        "phase": "requested",
    }


def _queue_restart(job_id: object) -> int:
    """Handle a queue-invoked restart through a detached helper process.

    The controller forks a helper into a separate session; the helper validates
    the restart and reports its outcome, this parent delivers a summary and
    exits zero so the owning worker finalizes the restart row as durably
    ``succeeded``, and only then does the helper request the supervisor process
    replacement. The parent never waits for the helper to finish and never
    touches the terminal row itself.

    Args:
        job_id: Captured restart queue row identifier.

    Returns:
        A process exit code.

    Raises:
        DeployAbortedError: If the helper cannot be forked or never reports.
    """
    from lubko import (  # ruff: ignore[import-outside-top-level] - breaks the deployctl<->lifecycle import cycle
        deployctl,
    )

    reader, writer = os.pipe()
    try:
        try:
            pid = os.fork()
        except OSError as exc:
            msg = f"could not fork the restart handoff helper: {exc}"
            raise DeployAbortedError(msg) from None
        if pid == 0:
            os.close(reader)
            _run_restart_helper(job_id, writer)
        os.close(writer)
        try:
            raw = deployctl.read_pipe_line(reader)
        finally:
            os.close(reader)
    finally:
        with suppress(OSError):
            os.close(reader)
        with suppress(OSError):
            os.close(writer)
    if not raw:
        msg = "restart handoff helper exited before reporting an outcome"
        raise DeployAbortedError(msg)
    try:
        response = json.loads(raw)
    except ValueError as exc:
        msg = "restart handoff helper reported an invalid response"
        raise DeployAbortedError(msg) from exc
    if not isinstance(response, dict):
        msg = "restart handoff helper reported a non-object response"
        raise DeployAbortedError(msg)
    if response.get("ok") is not True:
        detail = response.get("error")
        message = "restart was refused"
        if isinstance(detail, str):
            message = f"restart was refused: {detail}"
        raise DeployAbortedError(message)
    commit = response.get("commit")
    if isinstance(commit, str):
        _out(f"validated exact commit {commit}; requesting a supervised restart")
    _out("restart requested; it completes detached from this job through the external supervisor")
    return EXIT_OK


def _run_restart_helper(job_id: object, writer: int) -> None:
    """Run the detached queue-restart handoff helper to completion in the child.

    The child detaches into its own session immediately so the retiring
    worker's group shutdown can never reach it; a failed detach fails closed
    with an error response before any prepared/success outcome. It then closes
    every inherited descriptor except the response pipe and the standard
    streams, validates the restart, delivers the outcome to the parent, waits
    for the initiating row to be durably ``succeeded``, and only then requests
    the supervisor process replacement. This function never returns: it exits
    the child process.

    Args:
        job_id: Captured restart queue row identifier.
        writer: Write end of the response pipe to the parent.
    """
    from lubko import (  # ruff: ignore[import-outside-top-level] - breaks the deployctl<->lifecycle import cycle
        deployctl,
    )

    try:
        os.setsid()
    except OSError as exc:
        deployctl.send_helper_error(
            writer, f"restart handoff helper could not detach into its own session: {exc}"
        )
        with suppress(OSError):
            os.close(writer)
        os._exit(0)
    detach_standard_streams(keep={writer})
    try:
        try:
            _restart_helper_locked(job_id, writer)
        except DeployAbortedError as exc:
            deployctl.send_helper_error(writer, f"restart was refused: {exc}")
        except OSError as exc:
            deployctl.send_helper_error(writer, f"operating-system error: {exc}")
    finally:
        with suppress(OSError):
            os.close(writer)
    os._exit(0)


def _prepare_restart_locked(writer: int) -> bool:
    """Validate a queue restart and report the prepared response under the lock.

    Must be called while the deployment lock is held so the pending/corrupt
    supervised-state guard is serialized with the restart handoff decision.
    On refusal a helper error is delivered and ``False`` is returned; on
    success the prepared response is delivered and ``True`` is returned.

    Args:
        writer: Write end of the response pipe to the parent.

    Returns:
        ``True`` when the restart validated and may proceed after the lock.
    """
    from lubko import (  # ruff: ignore[import-outside-top-level] - breaks the deployctl<->lifecycle import cycle
        deployctl,
    )

    blocker = _supervised_mutation_blocker()
    if blocker is not None:
        deployctl.send_helper_error(writer, blocker)
        return False
    if not supervise.supervisor_running():
        deployctl.send_helper_error(
            writer,
            "no external supervisor is running; a supervised restart is not possible",
        )
        return False
    state = supervise.read_state()
    commit = state.commit
    if commit is None or not cli.runtime_is_usable(commit):
        deployctl.send_helper_error(
            writer,
            "no usable sealed runtime to restart"
            if commit is None
            else (
                f"the exact sealed runtime for commit {commit} is missing, corrupt, "
                "incomplete, or not sealed; refusing to restart"
            ),
        )
        return False
    deployctl.send_helper_response(writer, _restart_prepared_response(commit))
    return True


def _restart_helper_locked(job_id: object, writer: int) -> None:
    """Run one queue restart mission in the detached helper.

    The response or error is delivered to the parent before any destructive
    step, so the parent exits zero only for a genuine prepared response and the
    owning worker finalizes the restarting row as durably ``succeeded``; a
    helper error or helper death exits non-zero so the row is durably
    ``failed``. The helper then waits for that exact row to be durably
    ``succeeded`` before requesting the supervisor process replacement, so the
    control job is never killed by the old worker's own shutdown.

    Args:
        job_id: Captured restart queue row identifier.
        writer: Write end of the response pipe to the parent.
    """
    from lubko import (  # ruff: ignore[import-outside-top-level] - breaks the deployctl<->lifecycle import cycle
        deployctl,
    )

    try:
        with deploy_lock(DEFAULT_LOCK_TIMEOUT_SECONDS):
            if not _prepare_restart_locked(writer):
                return
    except LockTimeoutError as exc:
        deployctl.send_helper_error(writer, f"timed out waiting for the deployment lock: {exc}")
        return
    durable_deadline = time.time() + deployctl.handoff_durable_wait_seconds
    try:
        deployctl.wait_for_durable_success(job_id, durable_deadline)
    except deployctl.DeployCtlError as exc:
        append_deploy_log(f"queue restart aborted before the destructive handoff: {exc}")
        return
    try:
        _complete_restart_handoff()
    except DeployAbortedError as exc:
        append_deploy_log(f"queue restart handoff failed after durable success: {exc}")
        return
    append_deploy_log("queue restart converged after durable success")


def _request_restart_intent_locked() -> tuple[int, int | None]:
    """Recheck the supervised guard and write the restart intent under the lock.

    Must be called while the deployment lock is held so a mission that appears
    after the prepared/durable-success boundary cannot be outranked or
    disrupted by this handoff.

    Returns:
        ``(generation, previous_pid)`` for the written restart intent.

    Raises:
        DeployAbortedError: If the supervised-state guard refuses, no confirmed
            commit exists, or its sealed runtime is unusable.
    """
    blocker = _supervised_mutation_blocker()
    if blocker is not None:
        raise DeployAbortedError(blocker)
    state = supervise.read_state()
    commit = state.commit
    if commit is None:
        msg = "no confirmed commit to restart"
        raise DeployAbortedError(msg)
    if not cli.runtime_is_usable(commit):
        msg = (
            f"the exact sealed runtime for commit {commit} is missing, corrupt, "
            "incomplete, or not sealed; refusing to restart"
        )
        raise DeployAbortedError(msg)
    previous = supervise.read_status()
    previous_pid = (
        previous.child.pid if previous is not None and previous.child is not None else None
    )
    desired = supervise.read_desired()
    generation = supervise.request_restart(
        commit,
        repo=desired.repo if desired is not None else "",
        uv_path=desired.uv_path if desired is not None else "",
        worker_id=(
            desired.worker_id
            if desired is not None
            else os.getenv("LUBKO_WORKER_ID") or socket.gethostname()
        ),
    )
    return generation, previous_pid


def _complete_restart_handoff() -> None:
    """Request and await the supervised same-commit process replacement.

    The supervisor is the single process-lifecycle authority: deployctl/lifecycle
    only record the restart intent at a strictly newer generation and wait until
    the daemon proves a fresh worker consumes the queue. A worker process that
    was not actually replaced is reported as a failure.

    The pending/corrupt supervised-state guard, the runtime recheck, and the
    restart intent request are serialized under a freshly acquired deployment
    lock: a supervised checkout mission that appears after the prepared/
    durable-success boundary must abort the handoff instead of being outranked
    or disrupted. Lock ordering stays deployment-lock before generation-lock,
    so nesting with ``supervise.request_restart``'s generation lock cannot
    deadlock; generation/readiness waits run after the lock is released.

    Raises:
        DeployAbortedError: If the guarded intent request fails (including a
            supervised state that turned pending/corrupt after durable
            success), if the supervisor did not apply or prove the replacement,
            or the worker process was not replaced.
    """
    try:
        with deploy_lock(DEFAULT_LOCK_TIMEOUT_SECONDS):
            generation, previous_pid = _request_restart_intent_locked()
    except LockTimeoutError as exc:
        msg = f"timed out waiting for the deployment lock: {exc}"
        raise DeployAbortedError(msg) from None
    if not supervise.wait_for_generation(generation, supervise.DEFAULT_REQUEST_TIMEOUT_SECONDS):
        msg = "the external supervisor did not apply the restart"
        raise DeployAbortedError(msg)
    if not supervise.wait_until_ready(generation, supervise.DEFAULT_REQUEST_TIMEOUT_SECONDS):
        msg = "the external supervisor did not prove the fresh worker consumes the queue"
        raise DeployAbortedError(msg)
    current = supervise.read_status()
    current_pid = current.child.pid if current is not None and current.child is not None else None
    if previous_pid is not None and current_pid == previous_pid:
        msg = "the worker process was not replaced by a fresh process"
        raise DeployAbortedError(msg)
    _out(f"restart complete: fresh worker pid={current_pid}")


def migrate_cmd(args: argparse.Namespace) -> int:
    """Replace stale/corrupt pre-supervisor state with a verified exact commit.

    With no live supervisor this is the explicit, supported migration for
    production state that predates the external supervisor (stale or legacy
    ``supervisor/desired.json``, ``supervisor/state.json``, or
    ``worker/rollback.json``). The operator names an exact commit whose sealed
    runtime is verified; a strictly newer desired intent is written for it, and
    corrupt/legacy or stale-pending mission state is replaced (removed or
    archived terminal), so the next supervisor start reconstructs that exact
    commit deterministically instead of failing closed. Normal startup without
    this command stays fail-closed.

    Args:
        args: Parsed command line arguments.

    Returns:
        A process exit code.
    """
    if supervise.supervisor_running():
        _err("the external supervisor is running; no state migration is needed")
        return EXIT_ERROR
    commit = args.commit
    if not cli.runtime_is_usable(commit):
        _err(
            f"no verified sealed runtime for exact commit {commit}; build or deploy it first "
            "(refusing to trust mutable state)"
        )
        return EXIT_ERROR
    try:
        uv_path = resolve_uv(args.uv)
    except UvResolutionError as exc:
        _err(str(exc))
        return EXIT_ERROR
    try:
        with deploy_lock(args.lock_timeout):
            return _migrate_locked(commit, args.repo, uv_path)
    except LockTimeoutError:
        _err("another deployment is running; refusing to race")
        return EXIT_ERROR


def _migrate_locked(commit: str, repo: Path, uv_path: str) -> int:
    """Write the verified desired intent and replace stale mission state.

    Args:
        commit: Exact verified commit to run.
        repo: Maintained checkout the commit belongs to.
        uv_path: Resolved ``uv`` executable.

    Returns:
        A process exit code.
    """
    from lubko import (  # ruff: ignore[import-outside-top-level] - breaks the deployctl<->lifecycle import cycle
        deployctl,
    )

    try:
        mission = deployctl.read_rollback_state()
    except deployctl.DeployCtlError:
        mission = None
    if mission is None:
        # The supervised-deployment authority is absent or already corrupt: this
        # migration intentionally supersedes it, so remove any present file
        # before allocating a generation. Allocation then observes genuine
        # absence rather than failing closed on authority we are about to
        # replace, and no malformed authority is silently deleted outside an
        # explicit recovery path.
        remove_durable(rollback_state_path())
        append_deploy_log("migration replaced corrupt/legacy supervised-deployment state")
    with supervise.generation_lock():
        generation = supervise.next_generation()
        # The migration flag travels inside this one atomically written
        # desired intent: publishing the migrated target commit and recording
        # the convergence obligation is a single durable transition, so no
        # crash can leave the supervisor running the migrated commit without
        # its completion obligation (nor an orphaned migration intent without
        # a published commit).
        supervise.write_desired(
            supervise.SupervisorDesired(
                schema_version=supervise.SCHEMA_VERSION,
                generation=generation,
                commit=commit,
                repo=str(repo),
                uv_path=uv_path,
                worker_id=os.getenv("LUBKO_WORKER_ID") or socket.gethostname(),
                restart=False,
                requested_at=time.time(),
                migration=True,
            )
        )
    if (
        mission is not None
        and mission.status == deployctl.STATUS_PENDING
        and mission.generation < generation
    ):
        deployctl.archive_mission(mission, deployctl.STATUS_ROLLED_BACK)
        append_deploy_log(
            f"migration archived stale pending mission generation {mission.generation}"
        )
    elif (
        mission is not None
        and mission.status in {deployctl.STATUS_CONFIRMED, deployctl.STATUS_ROLLED_BACK}
        and mission.generation < generation
    ):
        # A strictly newer cold-migration intent supersedes older terminal
        # mission authority: leaving a terminal ``confirmed`` record for an
        # older commit intact would keep deployctl (and through its
        # reconciliation the maintained CLI pointer) permanently pinned to the
        # obsolete commit even after the migrated target is proven ready.
        deployctl.archive_mission(mission, deployctl.STATUS_ROLLED_BACK)
        append_deploy_log(
            f"migration superseded terminal {mission.status} mission generation "
            f"{mission.generation} (commit {mission.commit})"
        )
    append_deploy_log(f"migrated lifecycle state to verified exact commit {commit}")
    _out(f"lifecycle state migrated to verified exact commit {commit}")
    _out("start the external supervisor (lubko-supervisor) to reconstruct the worker")
    return EXIT_OK


# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------


def bootstrap_cmd(args: argparse.Namespace) -> int:
    """Materialize a target runtime and stage a supervisor-runtime override.

    This is the narrow bootstrap path for the pre-fix live state where an
    old supervisor (running buggy probe code) cannot confirm readiness for
    a new commit.  The command:

    1. builds and seals the target commit's immutable CLI runtime;
    2. publishes a plain-text supervisor-runtime override so the *stable*
       ``lubko-supervisor`` launcher will execute the new code on the next
       container/environment restart;
    3. updates the ``lubko-supervisor`` launcher script to carry the
       override-checking logic;
    4. preserves the currently confirmed worker runtime (``cli/current``)
       and all other runtimes for rollback.

    The command does **not** modify ``cli/current``, ``desired.json``,
    ``worker/meta.json``, or any other confirmed-worker state.  It does
    **not** kill the running supervisor.  After a successful run the
    operator restarts the container/environment; the launcher starts the
    new supervisor code which reads the existing confirmed desired intent
    and restores exactly one worker for the confirmed commit.  A normal
    ``lubko-deploy deploy <target>`` can then confirm the target and
    advance ``cli/current``.

    Safe if interrupted: every step is idempotent.  If interrupted before
    the override is published the old state is unchanged; if interrupted
    after the override is published the next restart uses the new
    supervisor code while the confirmed worker commit is untouched.

    Args:
        args: Parsed command line arguments.

    Returns:
        A process exit code.
    """
    commit = args.commit
    if not cli.is_valid_commit_name(commit):
        _err(f"commit must be exactly 40 hexadecimal characters, got: {commit!r}")
        return EXIT_ERROR
    if not supervise.supervisor_running():
        _err(
            "the external supervisor is not running; "
            "use 'lubko-deploy migrate' for cold pre-supervisor state"
        )
        return EXIT_ERROR
    try:
        uv_path = resolve_uv(args.uv)
    except UvResolutionError as exc:
        _err(str(exc))
        return EXIT_ERROR
    try:
        with deploy_lock(args.lock_timeout):
            return _bootstrap_locked(commit, args.repo, uv_path, args.cli_timeout)
    except LockTimeoutError:
        _err("another deployment is already running; refusing to race")
        return EXIT_ERROR


def _bootstrap_locked(
    commit: str,
    repo: Path,
    uv_path: str,
    cli_timeout: float,
) -> int:
    """Run the bootstrap preparation under the deployment lock.

    Every step is idempotent: re-running after a partial failure or
    interruption completes the remaining steps without disturbing the
    confirmed worker commit, desired intent, or ``cli/current``.

    Args:
        commit: Exact 40-hex target commit.
        repo: Repository checkout containing the commit.
        uv_path: Resolved ``uv`` executable.
        cli_timeout: Timeout for building the CLI environment.

    Returns:
        A process exit code.
    """
    confirmed = cli.current_commit()
    if confirmed == commit:
        _out(f"commit {commit} is already the confirmed CLI commit; nothing to bootstrap")
        return EXIT_OK

    _out(f"bootstrap: materializing sealed runtime for {commit} ...")
    try:
        cli.build_cli_root(repo, commit, uv_path, cli_timeout)
    except cli.CliError as exc:
        _err(f"could not build the CLI environment for {commit}: {exc}")
        return EXIT_ERROR

    if not cli.runtime_is_usable(commit):
        _err(f"runtime for {commit} is not usable after build; refusing to stage override")
        return EXIT_ERROR

    _out("bootstrap: installing lubko-supervisor launcher ...")
    bin_home = _resolve_bin_home()
    try:
        install_supervisor_launcher(bin_home)
    except OSError as exc:
        _err(f"could not install the lubko-supervisor launcher: {exc}")
        _err("refusing to continue without a verified launcher")
        return EXIT_ERROR

    _out("bootstrap: installing versioned startup launcher ...")
    if (err := startup_contract.install_and_validate_startup_definition(bin_home)) is not None:
        _err(err)
        _err("refusing to continue without a verified startup definition")
        return EXIT_ERROR

    _out(f"bootstrap: publishing supervisor-runtime override for {commit} ...")
    supervise.write_supervisor_runtime_override(commit)

    append_deploy_log(
        f"bootstrap: staged supervisor-runtime override for {commit}"
        + (f" (confirmed commit remains {confirmed})" if confirmed else "")
    )
    _out(f"bootstrap complete: supervisor-runtime override staged for {commit}")
    if confirmed is not None:
        _out(f"confirmed commit remains {confirmed}")
    _out("")
    _out("next steps:")
    _out("  1. restart the container/environment to load the new supervisor code")
    _out("  2. the new supervisor will restore the confirmed worker from desired state")
    _out("  3. run 'lubko-deploy deploy <target>' to confirm the target and advance cli/current")
    startup_contract.write_contract()
    _out(f"startup contract version {startup_contract.CONTRACT_SCHEMA_VERSION} recorded")
    _out(
        f"startup definition installed; the container must run "
        f"'{startup_contract.STARTUP_LAUNCHER_NAME}' and the deployment seam must supply "
        f"{startup_contract.RESTART_POLICY_ENV} for restart authority"
    )
    return EXIT_OK


def _resolve_bin_home() -> Path:
    """Return the user bin directory."""
    explicit = os.environ.get("XDG_BIN_HOME")
    if explicit:
        return Path(explicit)
    return Path.home() / ".local" / "bin"


def install_supervisor_launcher(bin_home: Path) -> None:
    """Install the lubko-supervisor launcher with override-checking logic.

    Only the supervisor launcher is written; all other launchers resolve
    through ``cli/current`` as before.  The installation is verified after
    writing: exact byte content must match the expected source and the
    executable mode bit must be set, so a partial write, truncated file,
    or permission error is detected before the override pointer is
    published.

    Args:
        bin_home: Directory containing the launcher scripts.

    Raises:
        OSError: If the directory is missing, the write fails, or
            verification fails.
    """
    if not bin_home.is_dir():
        msg = f"bin directory {bin_home} does not exist"
        raise OSError(msg)
    target = bin_home / "lubko-supervisor"
    temporary = bin_home / "lubko-supervisor.tmp"
    expected_bytes = cli.launcher_source("lubko-supervisor").encode("utf-8")
    temporary.write_bytes(expected_bytes)
    temporary.chmod(0o755)
    temporary.replace(target)
    actual_bytes = target.read_bytes()
    if actual_bytes != expected_bytes:
        msg = f"launcher content mismatch after installation: {target}"
        raise OSError(msg)
    try:
        mode = target.stat().st_mode
    except OSError as exc:
        msg = f"launcher {target} is not accessible after installation: {exc}"
        raise OSError(msg) from exc
    if not stat.S_ISREG(mode):
        msg = f"launcher {target} is not a regular file after installation"
        raise OSError(msg)
    if not (mode & stat.S_IXUSR):
        msg = f"launcher {target} is not executable after installation"
        raise OSError(msg)


def log_cmd(lines: int) -> int:
    """Show the tail of the maintained worker log.

    Resolves through the stable ``worker.log`` symlink (supervisor path) or
    reads the per-incarnation file from metadata (legacy path).

    Args:
        lines: Number of trailing lines to show.

    Returns:
        A process exit code.
    """
    stable = worker_log_path()
    if stable.is_symlink() or stable.is_file():
        path = stable
    else:
        meta = read_meta()
        if meta is not None and meta.log_path:
            path = Path(meta.log_path)
        else:
            _out("no worker log yet")
            return EXIT_OK
    try:
        text = path.read_text()
    except OSError as exc:
        _err(f"could not read the worker log: {exc}")
        return EXIT_ERROR
    tail = text.splitlines()[-lines:]
    if tail:
        _out("\n".join(tail))
    return EXIT_OK


def deploy_cmd(args: argparse.Namespace) -> int:
    """Run the deploy subcommand.

    Args:
        args: Parsed command line arguments.

    Returns:
        A process exit code.
    """
    try:
        uv_path = resolve_uv(args.uv)
    except UvResolutionError as exc:
        _err(str(exc))
        return EXIT_ERROR
    options = DeployOptions(
        repo=args.repo,
        uv_path=uv_path,
        bootstrap=args.bootstrap,
        direct_spawn=args.bootstrap,
        stop_grace_seconds=args.grace_seconds,
        postgres_timeout_seconds=args.db_timeout,
        lock_timeout_seconds=args.lock_timeout,
        validation_timeout_seconds=args.validation_timeout,
        git_timeout_seconds=args.git_timeout,
        cli_timeout_seconds=args.cli_timeout,
    )
    return deploy(options)


def repair_cmd(args: argparse.Namespace) -> int:
    """Run the repair subcommand.

    Args:
        args: Parsed command line arguments.

    Returns:
        A process exit code.
    """
    try:
        uv_path = resolve_uv(args.uv)
    except UvResolutionError as exc:
        _err(str(exc))
        return EXIT_ERROR
    options = DeployOptions(
        repo=args.repo,
        uv_path=uv_path,
        bootstrap=False,
        stop_grace_seconds=args.grace_seconds,
        postgres_timeout_seconds=args.db_timeout,
        lock_timeout_seconds=args.lock_timeout,
        validation_timeout_seconds=DEFAULT_VALIDATION_TIMEOUT_SECONDS,
        git_timeout_seconds=args.git_timeout,
        cli_timeout_seconds=args.cli_timeout,
        probe_timeout_seconds=args.probe_timeout,
    )
    return repair(options, args.recovery_worker_pid)


def recover_cmd(args: argparse.Namespace) -> int:
    """Run the recover subcommand.

    Args:
        args: Parsed command line arguments.

    Returns:
        A process exit code.
    """
    try:
        uv_path = resolve_uv(args.uv)
    except UvResolutionError as exc:
        _err(str(exc))
        return EXIT_ERROR
    options = DeployOptions(
        repo=args.repo,
        uv_path=uv_path,
        bootstrap=False,
        stop_grace_seconds=DEFAULT_STOP_GRACE_SECONDS,
        postgres_timeout_seconds=args.db_timeout,
        lock_timeout_seconds=args.lock_timeout,
        validation_timeout_seconds=DEFAULT_VALIDATION_TIMEOUT_SECONDS,
        git_timeout_seconds=args.git_timeout,
        cli_timeout_seconds=DEFAULT_CLI_TIMEOUT_SECONDS,
        probe_timeout_seconds=args.probe_timeout,
    )
    return recover(options)


def build_parser() -> argparse.ArgumentParser:
    """Build the ``lubko-deploy`` command line parser.

    Returns:
        The configured parser.
    """
    parser = argparse.ArgumentParser(
        prog="lubko-deploy",
        description="Deploy and manage the Lubko worker deterministically.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("status", help="show worker lifecycle state")

    contract_parser = subparsers.add_parser(
        "startup-contract",
        help="prove the live supervisor startup topology (tini -> supervisor -> worker)",
    )
    contract_parser.add_argument(
        "--write",
        action="store_true",
        help="publish the current versioned startup contract artifact before proving the topology",
    )

    deploy_parser = subparsers.add_parser(
        "deploy",
        help="validate the checkout and replace the running worker",
    )
    deploy_parser.add_argument(
        "--repo",
        type=Path,
        default=Path.cwd(),
        help="repository checkout to deploy",
    )
    deploy_parser.add_argument(
        "--uv",
        default=None,
        help="uv executable (default: uv on PATH, then the recorded Lubko toolchain path)",
    )
    deploy_parser.add_argument(
        "--bootstrap",
        action="store_true",
        help="bootstrap from an unmanaged legacy worker (must be stopped manually first)",
    )
    deploy_parser.add_argument(
        "--grace-seconds",
        type=float,
        default=DEFAULT_STOP_GRACE_SECONDS,
        help="grace period before SIGKILL (default: 5)",
    )
    deploy_parser.add_argument(
        "--db-timeout",
        type=float,
        default=DEFAULT_POSTGRES_TIMEOUT_SECONDS,
        help="PostgreSQL verification timeout in seconds (default: 5)",
    )
    deploy_parser.add_argument(
        "--lock-timeout",
        type=float,
        default=DEFAULT_LOCK_TIMEOUT_SECONDS,
        help="deploy lock wait timeout in seconds (default: 30)",
    )
    deploy_parser.add_argument(
        "--validation-timeout",
        type=float,
        default=DEFAULT_VALIDATION_TIMEOUT_SECONDS,
        help="timeout per validation command in seconds (default: 1200)",
    )
    deploy_parser.add_argument(
        "--git-timeout",
        type=float,
        default=DEFAULT_GIT_TIMEOUT_SECONDS,
        help="git commit lookup timeout in seconds (default: 10)",
    )
    deploy_parser.add_argument(
        "--cli-timeout",
        type=float,
        default=DEFAULT_CLI_TIMEOUT_SECONDS,
        help="maintained CLI environment build timeout in seconds (default: 600)",
    )

    subparsers.add_parser(
        "restart",
        help="restart the confirmed exact commit through the external supervisor",
    )

    migrate_parser = subparsers.add_parser(
        "migrate",
        help="replace stale/corrupt pre-supervisor state with a verified exact commit",
    )
    migrate_parser.add_argument(
        "--commit",
        required=True,
        help="exact 40-hex commit whose sealed runtime is verified",
    )
    migrate_parser.add_argument(
        "--repo",
        type=Path,
        default=Path.cwd(),
        help="repository checkout the commit belongs to (default: current directory)",
    )
    migrate_parser.add_argument(
        "--uv",
        default=None,
        help="uv executable (default: uv on PATH, then the recorded Lubko toolchain path)",
    )
    migrate_parser.add_argument(
        "--lock-timeout",
        type=float,
        default=DEFAULT_LOCK_TIMEOUT_SECONDS,
        help="deploy lock wait timeout in seconds (default: 30)",
    )

    bootstrap_parser = subparsers.add_parser(
        "bootstrap",
        help=(
            "materialize a target runtime and stage a supervisor-runtime override "
            "for the next container restart"
        ),
    )
    bootstrap_parser.add_argument(
        "--commit",
        required=True,
        help="exact 40-hex commit to bootstrap the supervisor onto",
    )
    bootstrap_parser.add_argument(
        "--repo",
        type=Path,
        default=Path.cwd(),
        help="repository checkout the commit belongs to (default: current directory)",
    )
    bootstrap_parser.add_argument(
        "--uv",
        default=None,
        help="uv executable (default: uv on PATH, then the recorded Lubko toolchain path)",
    )
    bootstrap_parser.add_argument(
        "--lock-timeout",
        type=float,
        default=DEFAULT_LOCK_TIMEOUT_SECONDS,
        help="deploy lock wait timeout in seconds (default: 30)",
    )
    bootstrap_parser.add_argument(
        "--cli-timeout",
        type=float,
        default=DEFAULT_CLI_TIMEOUT_SECONDS,
        help="maintained CLI environment build timeout in seconds (default: 600)",
    )

    repair_parser = subparsers.add_parser(
        "repair",
        help="adopt an independently known recovery worker and reconcile coherent lifecycle state",
    )
    repair_parser.add_argument(
        "--repo",
        type=Path,
        default=Path.cwd(),
        help="repository checkout that the recovery worker runs (default: current directory)",
    )
    repair_parser.add_argument(
        "--uv",
        default=None,
        help="uv executable (default: uv on PATH, then the recorded Lubko toolchain path)",
    )
    repair_parser.add_argument(
        "--recovery-worker-pid",
        type=int,
        required=True,
        help="exact PID of the running recovery worker to adopt (required)",
    )
    repair_parser.add_argument(
        "--grace-seconds",
        type=float,
        default=DEFAULT_STOP_GRACE_SECONDS,
        help="grace period before SIGKILL (default: 5)",
    )
    repair_parser.add_argument(
        "--db-timeout",
        type=float,
        default=DEFAULT_POSTGRES_TIMEOUT_SECONDS,
        help="PostgreSQL verification timeout in seconds (default: 5)",
    )
    repair_parser.add_argument(
        "--lock-timeout",
        type=float,
        default=DEFAULT_LOCK_TIMEOUT_SECONDS,
        help="deploy lock wait timeout in seconds (default: 30)",
    )
    repair_parser.add_argument(
        "--git-timeout",
        type=float,
        default=DEFAULT_GIT_TIMEOUT_SECONDS,
        help="git commit lookup timeout in seconds (default: 10)",
    )
    repair_parser.add_argument(
        "--cli-timeout",
        type=float,
        default=DEFAULT_CLI_TIMEOUT_SECONDS,
        help="maintained CLI environment build timeout in seconds (default: 600)",
    )
    repair_parser.add_argument(
        "--probe-timeout",
        type=float,
        default=DEFAULT_REPAIR_PROBE_TIMEOUT_SECONDS,
        help=(
            "maximum seconds to wait for the recovery worker to consume a queue probe (default: 60)"
        ),
    )

    recover_parser = subparsers.add_parser(
        "recover",
        help="start a detached recovery worker and report its adoptable identity",
    )
    recover_parser.add_argument(
        "--repo",
        type=Path,
        default=Path.cwd(),
        help="repository checkout the recovery worker runs (default: current directory)",
    )
    recover_parser.add_argument(
        "--uv",
        default=None,
        help="uv executable (default: uv on PATH, then the recorded Lubko toolchain path)",
    )
    recover_parser.add_argument(
        "--db-timeout",
        type=float,
        default=DEFAULT_POSTGRES_TIMEOUT_SECONDS,
        help="PostgreSQL verification timeout in seconds (default: 5)",
    )
    recover_parser.add_argument(
        "--lock-timeout",
        type=float,
        default=DEFAULT_LOCK_TIMEOUT_SECONDS,
        help="deploy lock wait timeout in seconds (default: 30)",
    )
    recover_parser.add_argument(
        "--git-timeout",
        type=float,
        default=DEFAULT_GIT_TIMEOUT_SECONDS,
        help="git commit lookup timeout in seconds (default: 10)",
    )
    recover_parser.add_argument(
        "--probe-timeout",
        type=float,
        default=DEFAULT_REPAIR_PROBE_TIMEOUT_SECONDS,
        help=(
            "maximum seconds to wait when detecting an existing queue consumer (default: 60, "
            "capped at 3)"
        ),
    )

    log_parser = subparsers.add_parser("log", help="show the tail of the worker log")
    log_parser.add_argument(
        "--lines",
        type=int,
        default=100,
        help="number of trailing lines (default: 100)",
    )

    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the ``lubko-deploy`` command line interface.

    Args:
        argv: Command line arguments, or ``None`` to use ``sys.argv``.

    Returns:
        A process exit code.
    """
    parser = build_parser()
    args = parser.parse_args(argv)
    handlers: dict[str, Callable[[argparse.Namespace], int]] = {
        "status": lambda _namespace: status_cmd(),
        "deploy": deploy_cmd,
        "restart": restart_cmd,
        "migrate": migrate_cmd,
        "bootstrap": bootstrap_cmd,
        "startup-contract": startup_contract_cmd,
        "repair": repair_cmd,
        "recover": recover_cmd,
        "log": lambda namespace: log_cmd(namespace.lines),
    }
    handler = handlers.get(args.command)
    if handler is None:
        parser.error(f"unknown command: {args.command}")
    return handler(args)


if __name__ == "__main__":
    raise SystemExit(main())
