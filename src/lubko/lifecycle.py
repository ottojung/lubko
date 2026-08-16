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
import os
import secrets
import signal
import socket
import subprocess
import sys
import time
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Final

import psycopg
from psycopg.rows import tuple_row

from lubko import cli, supervise, toolchain
from lubko.config import load_database_config
from lubko.state import rollback_state_path, state_root
from lubko.toolchain import UvResolutionError, resolve_uv
from lubko.worker import delete_job_and_chunks, group_has_members, request_cancel

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Sequence
    from uuid import UUID

    from lubko.worker import JobsConnection

EXIT_OK: Final = 0
EXIT_ERROR: Final = 1

SCHEMA_VERSION: Final = 1

STATE_UNMANAGED: Final = "unmanaged"
STATE_RUNNING: Final = "running"
STATE_STOPPED: Final = "stopped"
STATE_PENDING: Final = "pending"

LIFECYCLE_MARKER_VAR: Final = "LUBKO_LIFECYCLE_TOKEN"

DEFAULT_STOP_GRACE_SECONDS: Final = 5.0
DEFAULT_POSTGRES_TIMEOUT_SECONDS: Final = 5.0
DEFAULT_LOCK_TIMEOUT_SECONDS: Final = 30.0
DEFAULT_VALIDATION_TIMEOUT_SECONDS: Final = 1200.0
DEFAULT_GIT_TIMEOUT_SECONDS: Final = 10.0
DEFAULT_CLI_TIMEOUT_SECONDS: Final = cli.DEFAULT_BUILD_TIMEOUT_SECONDS
DEFAULT_REPAIR_PROBE_TIMEOUT_SECONDS: Final = 60.0
DEFAULT_RECOVER_PREFLIGHT_SECONDS: Final = 3.0
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
        """
        return cls(
            schema_version=_optional_int(data.get("schema_version")) or SCHEMA_VERSION,
            state=_optional_str(data.get("state")) or STATE_STOPPED,
            pid=_optional_int(data.get("pid")),
            pgid=_optional_int(data.get("pgid")),
            sid=_optional_int(data.get("sid")),
            start_time_ticks=_optional_int(data.get("start_time_ticks")),
            token=_optional_str(data.get("token")),
            repo=_optional_str(data.get("repo")) or "",
            git_commit=_optional_str(data.get("git_commit")),
            worker_id=_optional_str(data.get("worker_id")),
            log_path=_optional_str(data.get("log_path")) or "",
            started_at=_optional_float(data.get("started_at")),
            stopped_at=_optional_float(data.get("stopped_at")),
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


def worker_log_path() -> Path:
    """Return the stable path of the maintained worker's log.

    Returns:
        The worker log path.
    """
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

    Args:
        pid: Process ID to inspect.
        token: Expected lifecycle token.

    Returns:
        ``True`` when the token marker is present in the process environment.
    """
    marker = f"{LIFECYCLE_MARKER_VAR}={token}".encode()
    try:
        environ = (Path("/proc") / str(pid) / "environ").read_bytes()
    except OSError:
        return False
    return marker in environ


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
    """Atomically persist worker lifecycle metadata.

    Args:
        meta: Worker metadata to persist.
    """
    directory = worker_state_dir()
    directory.mkdir(parents=True, exist_ok=True)
    tmp_path = directory / "meta.json.tmp"
    tmp_path.write_text(json.dumps(meta.to_dict(), indent=2, sort_keys=True) + "\n")
    tmp_path.replace(meta_path())


def read_meta() -> WorkerMeta | None:
    """Load worker lifecycle metadata, tolerating absence and corruption.

    Returns:
        The stored metadata, or ``None`` when no metadata exists.
    """
    try:
        data = json.loads(meta_path().read_text())
    except (OSError, ValueError):
        return None
    if not isinstance(data, dict):
        return None
    try:
        return WorkerMeta.from_dict(data)
    except (KeyError, TypeError, ValueError):
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
    disconnected and both output streams appended to the stable worker log.

    Args:
        repo: Repository checkout to run the worker from.
        uv_path: Path to the ``uv`` executable.
        log_path: Stable path of the worker log.
        env: Environment for the worker, including the lifecycle token.

    Returns:
        The started worker process.
    """
    with log_path.open("ab") as log:
        return subprocess.Popen(
            _worker_command(uv_path),
            cwd=repo,
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=subprocess.STDOUT,
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

    Args:
        pid: Process ID of the spawned worker.

    Returns:
        The exact identity, or ``None`` if the process died first.
    """
    deadline = time.monotonic() + SESSION_ESTABLISH_TIMEOUT_SECONDS
    while True:
        identity = process_identity(pid)
        if identity is not None and identity.pgid == pid and identity.sid == pid:
            return identity
        if time.monotonic() >= deadline:
            return identity
        time.sleep(SESSION_WAIT_INTERVAL_SECONDS)


def _signal_group(pgid: int, sig: int) -> None:
    """Send a signal to an exact process group, ignoring an already-gone group.

    Args:
        pgid: Process group to signal.
        sig: Signal to send.
    """
    with suppress(ProcessLookupError):
        os.killpg(pgid, sig)


def stop_worker(meta: WorkerMeta, grace_seconds: float) -> bool:
    """Terminate the recorded worker using its exact process group identity.

    Sends ``SIGTERM`` to the exact recorded process group, waits up to
    ``grace_seconds``, then sends ``SIGKILL`` while members remain. Identity is
    revalidated at every step so a recycled process can never be signalled.

    Args:
        meta: Recorded worker metadata.
        grace_seconds: Grace period before force-killing.

    Returns:
        ``True`` when the worker is no longer alive afterwards.
    """
    if not worker_alive(meta):
        return True
    if meta.pid is None:
        return True
    identity = process_identity(meta.pid)
    if identity is None:
        return True
    _signal_group(identity.pgid, signal.SIGTERM)
    deadline = time.monotonic() + grace_seconds
    while time.monotonic() < deadline:
        if not worker_alive(meta):
            return True
        time.sleep(LOCK_POLL_INTERVAL_SECONDS)
    _signal_group(identity.pgid, signal.SIGKILL)
    deadline = time.monotonic() + grace_seconds
    while time.monotonic() < deadline:
        if not group_has_members(identity.pgid):
            return True
        time.sleep(LOCK_POLL_INTERVAL_SECONDS)
    return not group_has_members(identity.pgid)


# ---------------------------------------------------------------------------
# Deployment
# ---------------------------------------------------------------------------


def deploy(options: DeployOptions) -> int:
    """Validate a checkout and replace the running worker.

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


def _activate_maintained_cli(commit: str) -> bool:
    """Activate the confirmed CLI commit, preserving the prior coherent CLI.

    Activation happens only after the new worker metadata is durable. On
    failure the previous CLI commit stays active and its environment is never
    garbage-collected, so the global CLIs remain usable even though they are
    temporarily behind the worker.

    Args:
        commit: Exact commit to activate.

    Returns:
        ``True`` when the pointer now selects ``commit``, ``False`` otherwise.
    """
    try:
        cli.set_current(commit)
    except cli.CliError as exc:
        _err(f"error: maintained CLI activation failed: {exc}")
        _err(
            "the previous CLI commit remains active and usable; run 'lubko-deploy-ctl status' "
            "or 'lubko-install' to repair the maintained CLIs"
        )
        return False
    cli.gc_cli_roots((commit,))
    return True


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
    log_file: Path,
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
        log_file: Stable worker log path.

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
        proc = spawn_worker(options.repo, options.uv_path, log_file, env)
    except OSError as exc:
        _err(f"could not start the replacement worker: {exc}")
        raise DeployAbortedError from None

    identity = _wait_for_identity(proc.pid)
    if identity is None:
        _err("replacement worker exited before establishing its identity")
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
        log_path=str(log_file),
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


def _deploy_locked(options: DeployOptions) -> int:
    """Perform a deployment while holding the deployment lock.

    The external supervisor is the single authority that owns the maintained
    worker; a normal deployment hands the exact commit to the daemon and never
    silently falls back to direct spawning when the daemon is absent.

    Args:
        options: Deployment inputs.

    Returns:
        A process exit code.

    Raises:
        DeployAbortedError: If the deployment must abort and leave the current
            worker untouched.
    """
    previous = read_meta()
    state = worker_state(previous)

    if state == STATE_UNMANAGED:
        if not options.bootstrap:
            _err(UNMANAGED_WORKER_MESSAGE)
            _err("stop the legacy worker manually once, then rerun with --bootstrap")
            raise DeployAbortedError
        _out("bootstrap: no maintained worker metadata; assuming the legacy worker was stopped")

    commit = _validate_and_prepare(options)

    log_file = worker_log_path()
    if supervise.supervisor_running():
        new_meta = _deploy_through_supervisor(options, commit)
        _out(f"worker running: pid={new_meta.pid} pgid={new_meta.pgid} session={new_meta.sid}")
    elif options.bootstrap or options.direct_spawn:
        new_meta = _deploy_direct(options, previous, state, commit, log_file)
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


def _repair_rollback_state(recovery_worker_pid: int) -> None:
    """Resolve stale rollback state before adopting a recovery worker.

    A terminal or abandoned supervised-deployment record is inert and removed.
    A genuinely live pending mission, or any live recorded identity other than
    the recovery worker, aborts the repair so it can never adopt a second
    consumer or disturb an in-flight deployment.

    Args:
        recovery_worker_pid: Exact PID of the recovery worker being adopted.

    Raises:
        _AdoptionError: If a live mission or a different live identity is
            recorded.
    """
    path = rollback_state_path()
    if not path.is_file():
        return
    try:
        data = json.loads(path.read_text())
    except (OSError, ValueError):
        path.unlink(missing_ok=True)
        return
    if not isinstance(data, dict):
        path.unlink(missing_ok=True)
        return
    try:
        new_meta = WorkerMeta.from_dict(data.get("new_meta") or {})
        previous_meta = WorkerMeta.from_dict(data.get("previous_meta") or {})
    except (KeyError, TypeError, ValueError):
        path.unlink(missing_ok=True)
        return
    if (
        data.get("status") == STATE_PENDING
        and worker_alive(new_meta)
        and (data.get("deadline", 0.0) > time.time())
    ):
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
    path.unlink(missing_ok=True)


def _insert_probe_job(conn: JobsConnection, cwd: str) -> UUID | None:
    """Insert one pending queue probe job.

    Args:
        conn: Open PostgreSQL connection.
        cwd: Working directory for the probe command.

    Returns:
        The probe job identifier, or ``None`` if the insert failed.
    """
    probe_payload = json.dumps({
        "v": 3,
        "type": "command",
        "request": {"cwd": cwd, "process": ["sleep", "60"]},
        "state": {"status": "pending"},
    })
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
                "SELECT (payload::jsonb)->'state'->>'status', "
                "(payload::jsonb)->'state'->>'worker_id', "
                "(payload::jsonb)->'state'->>'process_pid' "
                "FROM lubko.jobs WHERE id = %s",
                (probe_id,),
            )
            row = cursor.fetchone()
        if row is None:
            return False
        status = str(row[0])
        owner = str(row[1]) if row[1] is not None else None
        if status == STATE_RUNNING:
            if owner != expected_worker_id:
                return False
            if row[2] is None:
                time.sleep(LOCK_POLL_INTERVAL_SECONDS)
                continue
            try:
                process_pid = int(str(row[2]))
            except ValueError:
                return False
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
                request_cancel(conn, probe_id)
            _wait_for_probe_terminal(conn, probe_id, timeout_seconds)
            with suppress(psycopg.Error):
                delete_job_and_chunks(conn, probe_id)
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
            path.unlink(missing_ok=True)
            continue
        if not isinstance(data, dict) or data.get("pid") != recovery_worker_pid:
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
        log_path=str(worker_log_path()),
        started_at=time.time(),
        stopped_at=None,
    ), worker_id


def _repair_locked(options: DeployOptions, recovery_worker_pid: int) -> int:
    """Adopt an independently known recovery worker into coherent metadata.

    This is the deliberate supported recovery path for lifecycle state already
    corrupted (for example by pre-isolation test runs). Adoption is verified
    before any metadata is rewritten; only then is the maintained CLI pointer
    reconciled and stale test-produced state removed.

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
    write_meta(new_meta)
    cli_ok = cli.reconcile_pointer(commit)
    cli.gc_cli_roots((commit,))
    _cleanup_ready_markers(recovery_worker_pid)
    _reconcile_toolchain(options.uv_path)
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
                request_cancel(conn, probe_id)
            _wait_for_probe_terminal(conn, probe_id, timeout_seconds)
            with suppress(psycopg.Error):
                delete_job_and_chunks(conn, probe_id)
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


def _recover_locked(options: DeployOptions) -> int:
    """Start a detached recovery worker and report its adoptable identity.

    The worker is started with the same detached session/process-group-leader
    mechanism a deployment replacement uses, so its exact PID is a stable
    dedicated leader that ``lubko-deploy repair --recovery-worker-pid`` can
    safely adopt later. No lifecycle metadata is written here.

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
    token = secrets.token_hex(16)
    env = worker_env(token)
    worker_id = env.get("LUBKO_WORKER_ID") or socket.gethostname()
    try:
        proc = spawn_worker(options.repo, options.uv_path, worker_log_path(), env)
    except OSError as exc:
        _err(f"could not start the recovery worker: {exc}")
        return EXIT_ERROR
    identity = _wait_for_identity(proc.pid)
    if identity is None or not (identity.pgid == proc.pid and identity.sid == proc.pid):
        _err("recovery worker exited before establishing a dedicated session")
        return EXIT_ERROR
    append_deploy_log(f"recovery worker started pid={identity.pid} commit={commit}")
    _out(f"recovery worker pid={identity.pid} pgid={identity.pgid} session={identity.sid}")
    _out(f"worker id: {worker_id}")
    _out(f"git commit: {commit}")
    _out(
        f"adopt it with: lubko-deploy repair --repo {options.repo} "
        f"--recovery-worker-pid {identity.pid}"
    )
    return EXIT_OK


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
        return EXIT_OK
    if meta is None:
        _print_supervisor_status()
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
    return EXIT_OK


def restart_cmd(_args: argparse.Namespace) -> int:
    """Restart the currently confirmed commit through the external supervisor.

    A restart never uses Git, the network, or the mutable source checkout: the
    exact commit is read from the supervisor's durable state, a fresh worker
    process is requested for that same commit at a newer generation, and the
    command waits until the supervisor proves the replacement consumes the
    queue. The deployed version never changes.

    Args:
        _args: Parsed command line arguments (unused).

    Returns:
        A process exit code.
    """
    if not supervise.supervisor_running():
        _err(
            "no external supervisor is running; a supervised restart is not possible "
            "(the only supported way to stop Lubko is to stop its environment)"
        )
        return EXIT_ERROR
    state = supervise.read_state()
    commit = state.commit
    if commit is None or not cli.runtime_is_usable(commit):
        _err(
            "no usable sealed runtime to restart"
            if commit is None
            else (
                f"the exact sealed runtime for commit {commit} is missing, corrupt, "
                "incomplete, or not sealed; refusing to restart"
            )
        )
        return EXIT_ERROR
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
    with supervise.generation_lock():
        generation = supervise.next_generation()
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
            )
        )
    if mission is None:
        rollback_state_path().unlink(missing_ok=True)
        append_deploy_log("migration replaced corrupt/legacy supervised-deployment state")
    elif mission.status == deployctl.STATUS_PENDING and mission.generation < generation:
        deployctl.archive_mission(mission, deployctl.STATUS_ROLLED_BACK)
        append_deploy_log(
            f"migration archived stale pending mission generation {mission.generation}"
        )
    append_deploy_log(f"migrated lifecycle state to verified exact commit {commit}")
    _out(f"lifecycle state migrated to verified exact commit {commit}")
    _out("start the external supervisor (lubko-supervisor) to reconstruct the worker")
    return EXIT_OK


def log_cmd(lines: int) -> int:
    """Show the tail of the maintained worker log.

    Args:
        lines: Number of trailing lines to show.

    Returns:
        A process exit code.
    """
    path = worker_log_path()
    if not path.is_file():
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
