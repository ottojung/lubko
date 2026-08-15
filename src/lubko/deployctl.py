"""Crash-safe supervised self-deployment for Lubko.

``lubko-deploy-ctl`` is the stable control plane used for version-changing
self-deployments. A checkout is provisional until two requests traverse the
replacement worker: first an exact-commit confirmation that returns a random
challenge, then a second exact-commit confirmation containing that challenge
reversed. Until both complete, a forked watchdog retains the known-good process
image and restores the previous commit automatically on timeout or candidate
failure.

The global command line tools are kept coherent with the confirmed commit:
the candidate CLI environment is built during the provisional phase, and the
``current`` pointer is switched only after durable ``confirmed`` state exists,
so rollback can never strand the global CLIs on candidate code. See
:mod:`lubko.cli`.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import secrets
import socket
import subprocess
import sys
import time
from contextlib import suppress
from dataclasses import dataclass, replace
from pathlib import Path
from typing import TYPE_CHECKING, Final

import psycopg
from psycopg.rows import tuple_row

from lubko import cli
from lubko.config import load_database_config
from lubko.lifecycle import (
    SCHEMA_VERSION,
    STATE_RUNNING,
    LockTimeoutError,
    ProcessIdentity,
    WorkerMeta,
    append_deploy_log,
    check_postgres,
    deploy_lock,
    process_identity,
    read_meta,
    run_validation,
    spawn_worker,
    stop_worker,
    worker_alive,
    worker_env,
    worker_log_path,
    write_meta,
)
from lubko.state import rollback_state_path
from lubko.toolchain import UvResolutionError, resolve_uv

if TYPE_CHECKING:
    from collections.abc import Sequence

EXIT_OK: Final = 0
EXIT_ERROR: Final = 1
ROLLBACK_SCHEMA_VERSION: Final = 1
STATUS_PENDING: Final = "pending"
STATUS_CONFIRMED: Final = "confirmed"
STATUS_ROLLED_BACK: Final = "rolled_back"
DEFAULT_CONFIRM_WINDOW_SECONDS: Final = 120.0
DEFAULT_STOP_GRACE_SECONDS: Final = 5.0
DEFAULT_POSTGRES_TIMEOUT_SECONDS: Final = 5.0
DEFAULT_LOCK_TIMEOUT_SECONDS: Final = 30.0
DEFAULT_VALIDATION_TIMEOUT_SECONDS: Final = 1200.0
DEFAULT_GIT_TIMEOUT_SECONDS: Final = 10.0
DEFAULT_CLI_TIMEOUT_SECONDS: Final = cli.DEFAULT_BUILD_TIMEOUT_SECONDS
IDENTITY_TIMEOUT_SECONDS: Final = 5.0
IDENTITY_POLL_SECONDS: Final = 0.02
POST_RELEASE_STABILITY_SECONDS: Final = 0.25
WATCHDOG_POLL_SECONDS: Final = 0.5
COMMIT_RE: Final = re.compile(r"[0-9a-f]{40}")
UTC_ISO_TEXT_SQL: Final = "to_char(now() at time zone 'utc', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"')"

GATED_SHIM_SOURCE: Final = """
import os
import sys
fd = int(sys.argv[1])
uv = sys.argv[2]
try:
    command = os.read(fd, 1)
finally:
    os.close(fd)
if command != b"G":
    raise SystemExit(2)
os.execvpe(uv, [uv, "run", "lubko-worker"], os.environ)
""".strip()


class DeployCtlError(RuntimeError):
    """Raised when a supervised deployment cannot proceed safely."""


@dataclass(frozen=True, slots=True)
class Options:
    """Runtime inputs shared by supervised-deployment operations."""

    repo: Path
    uv_path: str
    confirm_window_seconds: float
    stop_grace_seconds: float
    postgres_timeout_seconds: float
    lock_timeout_seconds: float
    validation_timeout_seconds: float
    git_timeout_seconds: float
    cli_timeout_seconds: float


@dataclass(frozen=True, slots=True)
class RollbackState:
    """Durable mission retained until a candidate is confirmed or restored."""

    schema_version: int
    status: str
    commit: str
    previous_commit: str
    challenge_hash: str | None
    deadline: float
    repo: str
    uv_path: str
    stop_grace_seconds: float
    git_timeout_seconds: float
    previous_retiring: bool
    previous_meta: WorkerMeta
    new_meta: WorkerMeta

    def to_dict(self) -> dict[str, object]:
        """Serialize durable rollback state.

        Returns:
            A JSON-compatible mapping.
        """
        return {
            "schema_version": self.schema_version,
            "status": self.status,
            "commit": self.commit,
            "previous_commit": self.previous_commit,
            "challenge_hash": self.challenge_hash,
            "deadline": self.deadline,
            "repo": self.repo,
            "uv_path": self.uv_path,
            "stop_grace_seconds": self.stop_grace_seconds,
            "git_timeout_seconds": self.git_timeout_seconds,
            "previous_retiring": self.previous_retiring,
            "previous_meta": self.previous_meta.to_dict(),
            "new_meta": self.new_meta.to_dict(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> RollbackState:
        """Parse durable rollback state strictly.

        Args:
            data: Decoded JSON mapping.

        Returns:
            Parsed rollback state.

        Raises:
            DeployCtlError: If the state is malformed.
        """
        try:
            previous = data["previous_meta"]
            replacement = data["new_meta"]
            if not isinstance(previous, dict) or not isinstance(replacement, dict):
                raise TypeError
            return cls(
                schema_version=int(data["schema_version"]),
                status=str(data["status"]),
                commit=str(data["commit"]),
                previous_commit=str(data["previous_commit"]),
                challenge_hash=_optional_string(data.get("challenge_hash")),
                deadline=float(data["deadline"]),
                repo=str(data["repo"]),
                uv_path=str(data["uv_path"]),
                stop_grace_seconds=float(data["stop_grace_seconds"]),
                git_timeout_seconds=float(data["git_timeout_seconds"]),
                previous_retiring=data.get("previous_retiring", False) is True,
                previous_meta=WorkerMeta.from_dict(previous),
                new_meta=WorkerMeta.from_dict(replacement),
            )
        except (KeyError, TypeError, ValueError) as exc:
            msg = "supervised deployment state is malformed"
            raise DeployCtlError(msg) from exc


@dataclass(frozen=True, slots=True)
class GatedWorker:
    """Candidate process blocked from consuming the queue until released."""

    proc: subprocess.Popen[bytes]
    gate_writer: int
    meta: WorkerMeta


def _optional_string(value: object | None) -> str | None:
    """Return a string value or ``None``.

    Args:
        value: JSON value to inspect.

    Returns:
        The string, or ``None``.
    """
    return value if isinstance(value, str) else None


def _write_state(state: RollbackState) -> None:
    """Atomically persist rollback authority state.

    Args:
        state: State to store.
    """
    path = rollback_state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(state.to_dict(), sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def _read_state() -> RollbackState | None:
    """Read rollback state, failing closed on corruption.

    Returns:
        Parsed state, or ``None`` when no state file exists.

    Raises:
        DeployCtlError: If an existing state file is invalid.
    """
    path = rollback_state_path()
    try:
        raw = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return None
    except OSError as exc:
        msg = f"cannot read supervised deployment state: {exc}"
        raise DeployCtlError(msg) from exc
    try:
        decoded = json.loads(raw)
    except ValueError as exc:
        msg = "supervised deployment state is not valid JSON"
        raise DeployCtlError(msg) from exc
    if not isinstance(decoded, dict):
        msg = "supervised deployment state must be an object"
        raise DeployCtlError(msg)
    state = RollbackState.from_dict(decoded)
    if state.schema_version != ROLLBACK_SCHEMA_VERSION:
        msg = f"unsupported supervised deployment state version {state.schema_version}"
        raise DeployCtlError(msg)
    return state


def _run_git(repo: Path, args: Sequence[str], timeout: float) -> subprocess.CompletedProcess[str]:
    """Run one bounded Git operation.

    Args:
        repo: Repository checkout.
        args: Git arguments.
        timeout: Timeout in seconds.

    Returns:
        Completed Git process.
    """
    return subprocess.run(
        ["git", *args],
        cwd=repo,
        capture_output=True,
        text=True,
        check=False,
        timeout=timeout,
    )


def _require_exact_commit(repo: Path, commit: str, timeout: float) -> None:
    """Require an exact full commit hash present in the repository.

    Args:
        repo: Repository checkout.
        commit: Requested commit.
        timeout: Git timeout.

    Raises:
        DeployCtlError: If the commit is not an exact present commit.
    """
    if COMMIT_RE.fullmatch(commit) is None:
        msg = "checkout requires an exact 40-character lowercase commit hash"
        raise DeployCtlError(msg)
    try:
        proc = _run_git(repo, ("rev-parse", "--verify", f"{commit}^{{commit}}"), timeout)
    except (OSError, subprocess.TimeoutExpired) as exc:
        msg = f"could not verify commit {commit}: {exc}"
        raise DeployCtlError(msg) from exc
    if proc.returncode != 0 or proc.stdout.strip() != commit:
        msg = f"commit {commit} is not present in the repository"
        raise DeployCtlError(msg)


def _require_clean_checkout(repo: Path, timeout: float) -> None:
    """Refuse to overwrite uncommitted deployment-checkout changes.

    Args:
        repo: Repository checkout.
        timeout: Git timeout.

    Raises:
        DeployCtlError: If the worktree is dirty or unreadable.
    """
    try:
        proc = _run_git(repo, ("status", "--porcelain"), timeout)
    except (OSError, subprocess.TimeoutExpired) as exc:
        msg = f"could not inspect deployment checkout: {exc}"
        raise DeployCtlError(msg) from exc
    if proc.returncode != 0:
        raise DeployCtlError("could not inspect deployment checkout")
    if proc.stdout:
        raise DeployCtlError("deployment checkout is dirty; commit or discard changes first")


def _checkout(repo: Path, commit: str, timeout: float, *, force: bool) -> bool:
    """Check out an exact detached commit.

    Args:
        repo: Repository checkout.
        commit: Exact commit hash.
        timeout: Git timeout.
        force: Whether rollback may discard candidate worktree changes.

    Returns:
        ``True`` on success.
    """
    args = ["checkout", "--detach"]
    if force:
        args.append("--force")
    args.append(commit)
    try:
        proc = _run_git(repo, args, timeout)
    except (OSError, subprocess.TimeoutExpired):
        return False
    return proc.returncode == 0


def _wait_for_identity(proc: subprocess.Popen[bytes]) -> ProcessIdentity | None:
    """Wait for a candidate to establish an independent session.

    Args:
        proc: Candidate process.

    Returns:
        Exact identity, or ``None`` if it dies or never establishes one.
    """
    deadline = time.monotonic() + IDENTITY_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            return None
        identity = process_identity(proc.pid)
        if identity is not None and identity.pgid == proc.pid and identity.sid == proc.pid:
            return identity
        time.sleep(IDENTITY_POLL_SECONDS)
    return None


def _spawn_gated_candidate(options: Options, commit: str) -> GatedWorker:
    """Spawn a non-consuming candidate behind a stable pipe gate.

    Args:
        options: Deployment options.
        commit: Candidate commit.

    Returns:
        Gated process and exact metadata.

    Raises:
        DeployCtlError: If the candidate cannot establish its identity.
    """
    token = secrets.token_hex(16)
    env = worker_env(token)
    worker_id = env.get("LUBKO_WORKER_ID") or socket.gethostname()
    reader, writer = os.pipe()
    try:
        proc = subprocess.Popen(
            [sys.executable, "-c", GATED_SHIM_SOURCE, str(reader), options.uv_path],
            cwd=options.repo,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
            close_fds=True,
            pass_fds=(reader,),
            env=env,
        )
    finally:
        os.close(reader)
    identity = _wait_for_identity(proc)
    if identity is None:
        os.close(writer)
        raise DeployCtlError("candidate exited before the rollback mission could be armed")
    meta = WorkerMeta(
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
        log_path=str(worker_log_path()),
        started_at=time.time(),
        stopped_at=None,
    )
    return GatedWorker(proc=proc, gate_writer=writer, meta=meta)


def _close_gate(gate_writer: int) -> None:
    """Close a candidate gate without releasing it.

    Args:
        gate_writer: Write end of the stable startup gate.
    """
    with suppress(OSError):
        os.close(gate_writer)


def _release_gate(gate_writer: int) -> None:
    """Release a candidate to exec the worker.

    Args:
        gate_writer: Write end of the stable startup gate.

    Raises:
        DeployCtlError: If the release cannot be delivered.
    """
    try:
        os.write(gate_writer, b"G")
    except OSError as exc:
        msg = f"could not release candidate worker: {exc}"
        raise DeployCtlError(msg) from exc
    finally:
        _close_gate(gate_writer)


def _wait_for_released_worker(meta: WorkerMeta) -> bool:
    """Require the released candidate to survive a short stabilization period.

    The actual queue-health proof is the two confirmation requests themselves:
    only the replacement worker is left consuming the queue, so inability to
    consume prevents confirmation and the watchdog rolls back.

    Args:
        meta: Candidate metadata.

    Returns:
        ``True`` when the candidate remains alive.
    """
    deadline = time.monotonic() + POST_RELEASE_STABILITY_SECONDS
    while time.monotonic() < deadline:
        if not worker_alive(meta):
            return False
        time.sleep(IDENTITY_POLL_SECONDS)
    return worker_alive(meta)


def _candidate_response(state: RollbackState) -> dict[str, object]:
    """Build the successful provisional checkout response.

    Args:
        state: Pending deployment state.

    Returns:
        JSON response object.
    """
    return {
        "type": "checkout",
        "ok": True,
        "phase": "pending",
        "commit": state.commit,
        "worker_pid": state.new_meta.pid,
        "deadline": state.deadline,
    }


def _current_queue_job_id() -> tuple[object | None, bool]:
    """Find the currently running queue job by its live process group.

    Historical PGID reuse cannot collide because only ``running`` rows are
    considered, and a live process group can belong to at most one Lubko job.

    Returns:
        ``(job_id, cancelled)`` when invoked from a queue job, otherwise
        ``(None, False)``.

    Raises:
        DeployCtlError: If more than one running row claims the current group.
    """
    try:
        database = load_database_config()
    except (OSError, ValueError):
        return None, False
    pgid = os.getpgrp()
    try:
        with psycopg.connect(database.conninfo(), row_factory=tuple_row) as conn:
            with conn.transaction(), conn.cursor() as cursor:
                cursor.execute(
                    "SELECT id, (payload::jsonb)->'state'->>'cancel_requested_at' "
                    "FROM lubko.jobs "
                    "WHERE (payload::jsonb)->'state'->>'status' = 'running' "
                    "AND ((payload::jsonb)->'state'->>'process_pgid')::bigint = %s "
                    "FOR UPDATE",
                    (pgid,),
                )
                rows = cursor.fetchall()
    except psycopg.Error as exc:
        msg = f"could not identify the current queue job: {exc.__class__.__name__}"
        raise DeployCtlError(msg) from exc
    if len(rows) > 1:
        raise DeployCtlError("multiple running jobs claim the current process group")
    if not rows:
        return None, False
    return rows[0][0], rows[0][1] is not None


def _self_finalize_checkout(response: dict[str, object]) -> None:
    """Persist checkout success before the owning old worker is gone.

    The checkout command itself is a queue job owned by the worker being
    replaced. After the handoff that worker can no longer finalize the row, so
    the stable wrapper atomically finishes its own currently-running row. Manual
    invocations have no matching running process group and simply return.

    Args:
        response: JSON response also written to stdout.

    Raises:
        DeployCtlError: If a queue invocation cannot be finalized safely.
    """
    job_id, cancelled = _current_queue_job_id()
    if job_id is None:
        return
    if cancelled:
        raise DeployCtlError("checkout job was cancelled during deployment")
    try:
        database = load_database_config()
        stdout = json.dumps(response, sort_keys=True) + "\n"
        with psycopg.connect(database.conninfo(), row_factory=tuple_row) as conn:
            with conn.transaction(), conn.cursor() as cursor:
                cursor.execute(
                    "UPDATE lubko.jobs SET payload = "
                    "jsonb_set("
                    "jsonb_set("
                    "jsonb_set(payload::jsonb, '{state,status}', to_jsonb('succeeded'::text)), "
                    "'{state,finished_at}', to_jsonb(" + UTC_ISO_TEXT_SQL + ")), "
                    "'{result}', jsonb_build_object("
                    "'stdout', to_jsonb(%s::text), 'stderr', to_jsonb(''::text), "
                    "'exit_code', to_jsonb(0::int), 'cancellation_note', to_jsonb(NULL::text)))::text "
                    "WHERE id = %s AND (payload::jsonb)->'state'->>'status' = 'running' "
                    "RETURNING id",
                    (stdout, job_id),
                )
                row = cursor.fetchone()
    except (OSError, ValueError, psycopg.Error) as exc:
        msg = f"could not finalize checkout queue job: {exc.__class__.__name__}"
        raise DeployCtlError(msg) from exc
    if row is None:
        raise DeployCtlError("checkout queue job stopped being running before finalization")


def _restart_previous(state: RollbackState) -> WorkerMeta | None:
    """Restore the previous known-good worker process.

    A previous worker that was never told to retire and is still alive is
    reused under its exact recorded identity (old watchdog behavior). Once the
    controller has durably marked ``previous_retiring`` before stopping that
    worker, a momentarily alive process is never trusted: retirement may have
    begun, so the exact old identity is deterministically stopped and awaited
    dead before a fresh previous-commit worker is spawned and verified. That
    guarantees terminal ``rolled_back`` never means a worker that is about to
    exit, so zero queue consumers cannot be the outcome of a completed
    rollback.

    Args:
        state: Rollback mission.

    Returns:
        Restored worker metadata, or ``None`` on failure.
    """
    previous = state.previous_meta
    if not state.previous_retiring and worker_alive(previous):
        return previous
    if worker_alive(previous) and not stop_worker(previous, state.stop_grace_seconds):
        return None
    token = secrets.token_hex(16)
    env = worker_env(token)
    worker_id = env.get("LUBKO_WORKER_ID") or previous.worker_id or socket.gethostname()
    try:
        proc = spawn_worker(
            Path(state.repo),
            state.uv_path,
            worker_log_path(),
            env,
        )
    except OSError:
        return None
    identity = _wait_for_identity(proc)
    if identity is None:
        return None
    meta = WorkerMeta(
        schema_version=SCHEMA_VERSION,
        state=STATE_RUNNING,
        pid=identity.pid,
        pgid=identity.pgid,
        sid=identity.sid,
        start_time_ticks=identity.start_time_ticks,
        token=token,
        repo=state.repo,
        git_commit=state.previous_commit,
        worker_id=worker_id,
        log_path=str(worker_log_path()),
        started_at=time.time(),
        stopped_at=None,
    )
    if not worker_alive(meta) or not check_postgres(DEFAULT_POSTGRES_TIMEOUT_SECONDS):
        stop_worker(meta, state.stop_grace_seconds)
        return None
    return meta


def _rollback_locked(state: RollbackState) -> bool:
    """Restore the exact previous known-good commit and worker.

    This path deliberately does not require the previous worker to implement
    any new candidate-side readiness protocol. Rollback is controlled entirely
    by the already-loaded stable wrapper process image.

    Args:
        state: Pending rollback mission.

    Returns:
        ``True`` only when checkout, worker, metadata, and state are restored.
    """
    if state.status != STATUS_PENDING:
        return True
    stop_worker(state.new_meta, state.stop_grace_seconds)
    repo = Path(state.repo)
    if not _checkout(repo, state.previous_commit, state.git_timeout_seconds, force=True):
        append_deploy_log("supervised rollback could not restore previous checkout")
        return False
    restored = _restart_previous(state)
    if restored is None:
        append_deploy_log("supervised rollback could not restart previous worker")
        return False
    write_meta(restored)
    _write_state(replace(state, status=STATUS_ROLLED_BACK, challenge_hash=None))
    cli.remove_cli_root(state.commit)
    if cli.reconcile_pointer(state.previous_commit):
        append_deploy_log(f"supervised rollback restored commit {state.previous_commit}")
    else:
        append_deploy_log(
            f"supervised rollback restored commit {state.previous_commit} "
            "but could not restore the maintained CLI pointer"
        )
    return True


def _watchdog_main(lock_timeout_seconds: float) -> None:
    """Retain rollback authority until the mission reaches a terminal state.

    Args:
        lock_timeout_seconds: Deployment-lock timeout for rollback attempts.
    """
    while True:
        try:
            state = _read_state()
        except DeployCtlError:
            time.sleep(WATCHDOG_POLL_SECONDS)
            continue
        if state is None or state.status in {STATUS_CONFIRMED, STATUS_ROLLED_BACK}:
            return
        should_rollback = time.time() >= state.deadline or not worker_alive(state.new_meta)
        if should_rollback:
            try:
                with deploy_lock(lock_timeout_seconds):
                    current = _read_state()
                    if current is not None and current.status == STATUS_PENDING:
                        if _rollback_locked(current):
                            return
            except (DeployCtlError, LockTimeoutError):
                pass
        time.sleep(WATCHDOG_POLL_SECONDS)


def _fork_watchdog(lock_timeout_seconds: float) -> None:
    """Fork the already-loaded stable wrapper before destructive handoff.

    The child closes its copy of the gate writer immediately. Therefore, if the
    parent dies before release, EOF reaches the gated candidate and the
    candidate exits rather than becoming an orphan queue consumer.

    Args:
        lock_timeout_seconds: Lock timeout used by the watchdog.

    Raises:
        DeployCtlError: If the stable rollback authority cannot be forked.
    """
    try:
        pid = os.fork()
    except OSError as exc:
        msg = f"could not arm rollback watchdog: {exc}"
        raise DeployCtlError(msg) from exc
    if pid != 0:
        return
    # The fork happens while the parent holds deploy_lock(). Close every
    # inherited non-stdio descriptor before entering the watchdog loop so a
    # parent crash cannot leave the child owning the inherited flock or gate.
    os.closerange(3, int(os.sysconf("SC_OPEN_MAX")))
    with suppress(OSError):
        os.setsid()
    try:
        _watchdog_main(lock_timeout_seconds)
    finally:
        os._exit(0)


def _cleanup_pending_locked() -> None:
    """Resolve an abandoned pending mission before accepting another checkout.

    Raises:
        DeployCtlError: If a live mission is still pending or rollback cannot
            complete.
    """
    state = _read_state()
    if state is None or state.status in {STATUS_CONFIRMED, STATUS_ROLLED_BACK}:
        return
    if state.status != STATUS_PENDING:
        raise DeployCtlError(f"unknown supervised deployment status {state.status!r}")
    if worker_alive(state.new_meta) and time.time() < state.deadline:
        raise DeployCtlError("another supervised checkout is still pending confirmation")
    if not _rollback_locked(state):
        raise DeployCtlError("an unresolved rollback is still pending")


def _deploy_locked(options: Options, commit: str) -> RollbackState:
    """Prepare and hand off one exact candidate while holding the deploy lock.

    Args:
        options: Deployment options.
        commit: Exact candidate commit.

    Returns:
        Pending rollback state.

    Raises:
        DeployCtlError: On any unsafe or incomplete handoff.
    """
    _cleanup_pending_locked()
    previous = read_meta()
    if previous is None or not worker_alive(previous) or previous.git_commit is None:
        raise DeployCtlError("a live maintained known-good worker is required for safe checkout")
    previous_commit = previous.git_commit
    _require_exact_commit(options.repo, previous_commit, options.git_timeout_seconds)
    _require_exact_commit(options.repo, commit, options.git_timeout_seconds)
    if commit == previous_commit:
        raise DeployCtlError("candidate commit is already the maintained worker commit")
    _require_clean_checkout(options.repo, options.git_timeout_seconds)
    if not _checkout(options.repo, commit, options.git_timeout_seconds, force=False):
        raise DeployCtlError(f"could not check out candidate commit {commit}")
    report = run_validation(options.repo, options.uv_path, options.validation_timeout_seconds)
    if not report.ok:
        _checkout(options.repo, previous_commit, options.git_timeout_seconds, force=True)
        raise DeployCtlError(f"candidate validation failed: {report.detail}")
    try:
        cli.build_cli_root(options.repo, commit, options.uv_path, options.cli_timeout_seconds)
    except cli.CliError as exc:
        _checkout(options.repo, previous_commit, options.git_timeout_seconds, force=True)
        msg = f"candidate CLI environment could not be built: {exc}"
        raise DeployCtlError(msg) from exc
    gated = _spawn_gated_candidate(options, commit)
    if not check_postgres(options.postgres_timeout_seconds):
        _close_gate(gated.gate_writer)
        _checkout(options.repo, previous_commit, options.git_timeout_seconds, force=True)
        raise DeployCtlError("stable wrapper cannot reach PostgreSQL before handoff")
    state = RollbackState(
        schema_version=ROLLBACK_SCHEMA_VERSION,
        status=STATUS_PENDING,
        commit=commit,
        previous_commit=previous_commit,
        challenge_hash=None,
        deadline=time.time() + options.confirm_window_seconds,
        repo=str(options.repo),
        uv_path=options.uv_path,
        stop_grace_seconds=options.stop_grace_seconds,
        git_timeout_seconds=options.git_timeout_seconds,
        previous_retiring=False,
        previous_meta=previous,
        new_meta=gated.meta,
    )
    _write_state(state)
    retiring = state
    try:
        _fork_watchdog(options.lock_timeout_seconds)
        retiring = replace(state, previous_retiring=True)
        _write_state(retiring)
        if not stop_worker(previous, options.stop_grace_seconds):
            raise DeployCtlError("could not stop the known-good worker")
        _release_gate(gated.gate_writer)
        if not _wait_for_released_worker(gated.meta):
            raise DeployCtlError("candidate worker exited immediately after release")
        live = replace(retiring, deadline=time.time() + options.confirm_window_seconds)
        _write_state(live)
        return live
    except DeployCtlError:
        _close_gate(gated.gate_writer)
        _rollback_locked(retiring)
        raise


def _handle_checkout(options: Options, request: dict[str, object]) -> dict[str, object]:
    """Handle an exact-commit checkout request.

    Args:
        options: Deployment options.
        request: Decoded request object.

    Returns:
        Protocol response.
    """
    commit = request.get("commit")
    if not isinstance(commit, str):
        raise DeployCtlError("checkout request requires string field 'commit'")
    try:
        with deploy_lock(options.lock_timeout_seconds):
            _reconcile_cli(_read_state())
            state = _deploy_locked(options, commit)
            response = _candidate_response(state)
            try:
                _self_finalize_checkout(response)
            except DeployCtlError:
                if not _rollback_locked(state):
                    raise DeployCtlError(
                        "checkout succeeded but its queue result could not be finalized; rollback is incomplete"
                    ) from None
                raise DeployCtlError(
                    "checkout queue result could not be finalized; deployment was rolled back"
                ) from None
            return response
    except LockTimeoutError as exc:
        raise DeployCtlError("timed out waiting for the deployment lock") from exc


def _challenge_digest(challenge: str) -> str:
    """Return the durable digest of one confirmation challenge.

    Args:
        challenge: Challenge string.

    Returns:
        Hex SHA-256 digest.
    """
    return hashlib.sha256(challenge.encode()).hexdigest()


def _cli_target_commit(state: RollbackState | None) -> str | None:
    """Return the commit the global CLIs must resolve to right now.

    A pending mission is deliberately ignored: while a candidate is
    provisional the pointer must stay on the previous confirmed commit, so a
    repair never activates candidate code before confirmation.

    Args:
        state: Current supervised-deployment state, or ``None``.

    Returns:
        The exact commit the CLI pointer should select, or ``None``.
    """
    if state is None:
        meta = read_meta()
        return None if meta is None else meta.git_commit
    if state.status == STATUS_PENDING:
        return state.previous_commit
    if state.status == STATUS_CONFIRMED:
        return state.commit
    return state.previous_commit


def _reconcile_cli(state: RollbackState | None) -> None:
    """Idempotently repair a stale maintained CLI pointer.

    A crash after durable ``confirmed`` state is written but before the CLI
    pointer is switched leaves a permanently confirmed worker with stale CLIs.
    Every controller invocation that observes deployment state runs this
    repair: the pointer is moved to the confirmed commit only when that
    commit's environment is already usable, and never towards a provisional
    candidate.

    Args:
        state: Current supervised-deployment state, or ``None``.
    """
    target = _cli_target_commit(state)
    if target is None:
        return
    if cli.reconcile_pointer(target):
        append_deploy_log(f"reconciled maintained CLI pointer to commit {target}")


def _confirm_locked(request: dict[str, object], options: Options) -> dict[str, object]:
    """Advance one pending deployment through the two-phase handshake.

    Args:
        request: Decoded confirm request.
        options: Runtime options.

    Returns:
        Protocol response.

    Raises:
        DeployCtlError: If confirmation is invalid or no mission is pending.
    """
    state = _read_state()
    if state is None or state.status != STATUS_PENDING:
        raise DeployCtlError("no checkout is pending confirmation")
    if time.time() >= state.deadline or not worker_alive(state.new_meta):
        _rollback_locked(state)
        raise DeployCtlError("confirmation window lapsed; deployment was rolled back")
    commit = request.get("commit")
    if commit != state.commit:
        _rollback_locked(state)
        raise DeployCtlError("confirmation commit does not match the proposed commit; rolled back")
    answer = request.get("challenge")
    if answer is None:
        challenge = secrets.token_urlsafe(24)
        _write_state(replace(state, challenge_hash=_challenge_digest(challenge)))
        return {
            "type": "confirm",
            "ok": True,
            "commit": state.commit,
            "challenge": challenge,
        }
    if not isinstance(answer, str) or state.challenge_hash is None:
        _rollback_locked(state)
        raise DeployCtlError("unexpected challenge response; deployment was rolled back")
    if not secrets.compare_digest(_challenge_digest(answer[::-1]), state.challenge_hash):
        _rollback_locked(state)
        raise DeployCtlError("challenge response is incorrect; deployment was rolled back")
    if time.time() >= state.deadline or not worker_alive(state.new_meta):
        _rollback_locked(state)
        raise DeployCtlError("candidate failed before confirmation; deployment was rolled back")
    try:
        cli.build_cli_root(
            Path(state.repo), state.commit, state.uv_path, options.cli_timeout_seconds
        )
    except cli.CliError as exc:
        _rollback_locked(state)
        msg = f"confirmed CLI environment could not be prepared; deployment was rolled back: {exc}"
        raise DeployCtlError(msg) from exc
    write_meta(state.new_meta)
    _write_state(replace(state, status=STATUS_CONFIRMED))
    try:
        cli.set_current(state.commit)
    except cli.CliError as exc:
        append_deploy_log(f"supervised deployment confirmed but CLI activation failed: {exc}")
    cli.gc_cli_roots((state.commit, state.previous_commit))
    append_deploy_log(f"supervised deployment confirmed commit {state.commit}")
    return {"type": "confirm", "ok": True, "commit": state.commit, "confirmed": True}


def _handle_confirm(options: Options, request: dict[str, object]) -> dict[str, object]:
    """Serialize confirmation with rollback/deadline enforcement.

    Args:
        options: Deployment options.
        request: Decoded request object.

    Returns:
        Protocol response.
    """
    try:
        with deploy_lock(options.lock_timeout_seconds):
            return _confirm_locked(request, options)
    except LockTimeoutError as exc:
        raise DeployCtlError("timed out waiting for the deployment lock") from exc


def _handle_status(options: Options) -> dict[str, object]:
    """Report and lazily enforce supervised-deployment state.

    Args:
        options: Deployment options.

    Returns:
        Protocol status response.
    """
    try:
        with deploy_lock(options.lock_timeout_seconds):
            state = _read_state()
            if state is not None and state.status == STATUS_PENDING:
                if time.time() >= state.deadline or not worker_alive(state.new_meta):
                    _rollback_locked(state)
                    state = _read_state()
            meta = read_meta()
            _reconcile_cli(state)
    except LockTimeoutError as exc:
        raise DeployCtlError("timed out waiting for the deployment lock") from exc
    if state is None:
        return {
            "type": "status",
            "ok": True,
            "phase": "idle",
            "known_commit": None if meta is None else meta.git_commit,
        }
    if state.status == STATUS_PENDING:
        return {
            "type": "status",
            "ok": True,
            "phase": "await-reversal" if state.challenge_hash is not None else "await-confirmation",
            "proposed_commit": state.commit,
            "previous_commit": state.previous_commit,
            "deadline": state.deadline,
        }
    known = state.commit if state.status == STATUS_CONFIRMED else state.previous_commit
    return {
        "type": "status",
        "ok": True,
        "phase": "idle",
        "last_outcome": state.status,
        "known_commit": known,
    }


def _dispatch(options: Options, request: dict[str, object]) -> dict[str, object]:
    """Dispatch one protocol request.

    Args:
        options: Runtime options.
        request: Decoded request.

    Returns:
        Protocol response.

    Raises:
        DeployCtlError: For unknown request types.
    """
    request_type = request.get("type")
    if request_type == "checkout":
        return _handle_checkout(options, request)
    if request_type == "confirm":
        return _handle_confirm(options, request)
    if request_type == "status":
        return _handle_status(options)
    raise DeployCtlError("request type must be checkout, confirm, or status")


def _parse_request(text: str) -> dict[str, object]:
    """Decode one JSON protocol request.

    Args:
        text: JSON object text.

    Returns:
        Decoded object.

    Raises:
        DeployCtlError: If the request is not a JSON object.
    """
    try:
        request = json.loads(text)
    except ValueError as exc:
        raise DeployCtlError("request is not valid JSON") from exc
    if not isinstance(request, dict):
        raise DeployCtlError("request must be a JSON object")
    return request


def _emit(response: dict[str, object]) -> None:
    """Write exactly one JSON response line.

    Args:
        response: Response object.
    """
    sys.stdout.write(json.dumps(response, sort_keys=True) + "\n")
    sys.stdout.flush()


def build_parser() -> argparse.ArgumentParser:
    """Build the stable wrapper CLI parser.

    Returns:
        Configured parser.
    """
    parser = argparse.ArgumentParser(
        prog="lubko-deploy-ctl",
        description="Supervise exact-commit Lubko self-deployments with automatic rollback.",
    )
    parser.add_argument("request", help="one JSON request object")
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument("--uv", default=None)
    parser.add_argument(
        "--confirm-window-seconds",
        type=float,
        default=float(
            os.getenv("LUBKO_ROLLBACK_WINDOW_SECONDS", str(DEFAULT_CONFIRM_WINDOW_SECONDS))
        ),
    )
    parser.add_argument("--grace-seconds", type=float, default=DEFAULT_STOP_GRACE_SECONDS)
    parser.add_argument("--db-timeout", type=float, default=DEFAULT_POSTGRES_TIMEOUT_SECONDS)
    parser.add_argument("--lock-timeout", type=float, default=DEFAULT_LOCK_TIMEOUT_SECONDS)
    parser.add_argument(
        "--validation-timeout", type=float, default=DEFAULT_VALIDATION_TIMEOUT_SECONDS
    )
    parser.add_argument("--git-timeout", type=float, default=DEFAULT_GIT_TIMEOUT_SECONDS)
    parser.add_argument("--cli-timeout", type=float, default=DEFAULT_CLI_TIMEOUT_SECONDS)
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run one stable-wrapper protocol request.

    Args:
        argv: CLI arguments, or ``None`` for ``sys.argv``.

    Returns:
        Process exit code. Protocol-level rejections still return zero so queue
        jobs can deliver their structured JSON error to the orchestrator.
    """
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        uv_path = resolve_uv(args.uv)
        options = Options(
            repo=args.repo.resolve(),
            uv_path=uv_path,
            confirm_window_seconds=args.confirm_window_seconds,
            stop_grace_seconds=args.grace_seconds,
            postgres_timeout_seconds=args.db_timeout,
            lock_timeout_seconds=args.lock_timeout,
            validation_timeout_seconds=args.validation_timeout,
            git_timeout_seconds=args.git_timeout,
            cli_timeout_seconds=args.cli_timeout,
        )
        if options.confirm_window_seconds <= 0:
            raise DeployCtlError("confirmation window must be positive")
        response = _dispatch(options, _parse_request(args.request))
    except (DeployCtlError, UvResolutionError) as exc:
        _emit({"ok": False, "error": str(exc)})
        return EXIT_OK
    except OSError as exc:
        _emit({"ok": False, "error": f"operating-system error: {exc}"})
        return EXIT_ERROR
    _emit(response)
    return EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main())
