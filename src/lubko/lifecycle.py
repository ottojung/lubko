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
from dataclasses import dataclass, replace
from pathlib import Path
from typing import TYPE_CHECKING, Final

import psycopg
from psycopg.rows import tuple_row

from lubko import cli
from lubko.config import load_database_config
from lubko.state import state_root
from lubko.toolchain import UvResolutionError, resolve_uv
from lubko.worker import group_has_members

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Sequence

EXIT_OK: Final = 0
EXIT_ERROR: Final = 1

SCHEMA_VERSION: Final = 1

STATE_UNMANAGED: Final = "unmanaged"
STATE_RUNNING: Final = "running"
STATE_STOPPED: Final = "stopped"

LIFECYCLE_MARKER_VAR: Final = "LUBKO_LIFECYCLE_TOKEN"

DEFAULT_STOP_GRACE_SECONDS: Final = 5.0
DEFAULT_POSTGRES_TIMEOUT_SECONDS: Final = 5.0
DEFAULT_LOCK_TIMEOUT_SECONDS: Final = 30.0
DEFAULT_VALIDATION_TIMEOUT_SECONDS: Final = 1200.0
DEFAULT_GIT_TIMEOUT_SECONDS: Final = 10.0
DEFAULT_CLI_TIMEOUT_SECONDS: Final = cli.DEFAULT_BUILD_TIMEOUT_SECONDS
LOCK_POLL_INTERVAL_SECONDS: Final = 0.1
SESSION_ESTABLISH_TIMEOUT_SECONDS: Final = 5.0
SESSION_WAIT_INTERVAL_SECONDS: Final = 0.01
UV_HTTP_TIMEOUT: Final = "30"

STAT_MIN_FIELDS: Final = 20
STAT_STARTTIME_FIELD_INDEX: Final = 19
STAT_STATE_FIELD_INDEX: Final = 0

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


def _activate_maintained_cli(commit: str) -> None:
    """Activate the confirmed CLI commit and garbage-collect older roots.

    Activation happens only after the new worker metadata is durable, so a
    failure here leaves the CLIs stale (previous confirmed version), never
    stranded on unconfirmed candidate code.

    Args:
        commit: Exact commit to activate.
    """
    try:
        cli.set_current(commit)
    except cli.CliError as exc:
        _err(f"warning: maintained CLI activation failed: {exc}")
    cli.gc_cli_roots((commit,))


def _deploy_locked(options: DeployOptions) -> int:
    """Perform a deployment while holding the deployment lock.

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
    _activate_maintained_cli(commit)
    append_deploy_log(f"deployed commit {commit} pid={new_meta.pid}")
    _out(f"deployed git commit {commit}")
    _out(f"worker running: pid={new_meta.pid} pgid={new_meta.pgid} session={new_meta.sid}")
    _out(f"log: {log_file}")
    return EXIT_OK


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
        return EXIT_OK
    if meta is None:
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
    return EXIT_OK


def stop_cmd(grace_seconds: float) -> int:
    """Stop the maintained worker by its exact recorded identity.

    Args:
        grace_seconds: Grace period before force-killing.

    Returns:
        A process exit code.
    """
    meta = read_meta()
    if meta is None:
        _err(UNMANAGED_WORKER_MESSAGE)
        _err("stop the legacy worker manually once; see README 'Bootstrap'")
        return EXIT_ERROR
    if not worker_alive(meta):
        _out("no maintained worker is running")
        return EXIT_OK
    _out(f"stopping maintained worker pid {meta.pid} (pgid {meta.pgid}) ...")
    if not stop_worker(meta, grace_seconds):
        _err(f"could not stop worker pid {meta.pid}")
        return EXIT_ERROR
    write_meta(replace(meta, state=STATE_STOPPED, stopped_at=time.time()))
    append_deploy_log(f"stopped worker pid={meta.pid}")
    _out("stopped")
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
        stop_grace_seconds=args.grace_seconds,
        postgres_timeout_seconds=args.db_timeout,
        lock_timeout_seconds=args.lock_timeout,
        validation_timeout_seconds=args.validation_timeout,
        git_timeout_seconds=args.git_timeout,
        cli_timeout_seconds=args.cli_timeout,
    )
    return deploy(options)


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

    stop_parser = subparsers.add_parser(
        "stop",
        help="stop the maintained worker by exact identity",
    )
    stop_parser.add_argument(
        "--grace-seconds",
        type=float,
        default=DEFAULT_STOP_GRACE_SECONDS,
        help="grace period before SIGKILL (default: 5)",
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
        "stop": lambda namespace: stop_cmd(namespace.grace_seconds),
        "log": lambda namespace: log_cmd(namespace.lines),
    }
    handler = handlers.get(args.command)
    if handler is None:
        parser.error(f"unknown command: {args.command}")
    return handler(args)


if __name__ == "__main__":
    raise SystemExit(main())
