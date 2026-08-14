"""Poll PostgreSQL for jobs, execute them directly, and support cancellation and recovery.

The transport table ``lubko.jobs`` keeps exactly two columns forever: ``id``
(unique random) and ``payload`` (one string containing a JSON object). All
job/request/result/state/cancellation/process-identity/lease data lives inside
``payload`` using the versioned binding in :mod:`lubko.protocol` (see
``docs/protocol.md``). The worker refuses to start against a table that
violates the two-column invariant.

Running jobs carry a lease: ``state.lease_expires_at`` is set at claim time and
refreshed by a heartbeat while the job runs. When a lease truly expires the
worker that owns the job is presumed dead or unreachable, and any worker
running a recovery pass atomically marks the abandoned job ``failed`` with a
clear diagnostic rather than re-executing it. Recovery is atomic across many
workers and never steals a genuinely live job, whose lease is continuously
refreshed.
"""

from __future__ import annotations

import logging
import os
import shutil
import signal
import socket
import subprocess
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final
from uuid import uuid4

import psycopg
from psycopg.rows import tuple_row

from lubko.config import load_database_config
from lubko.protocol import TWO_COLUMN_INVARIANT, ProtocolError, parse_payload

if TYPE_CHECKING:
    from uuid import UUID

    from lubko.config import DatabaseConfig

JobsConnection = psycopg.Connection[tuple[Any, ...]]

LOGGER: Final = logging.getLogger(__name__)
DEFAULT_POLL_INTERVAL_SECONDS: Final = 1.0
DEFAULT_PROCESS_POLL_INTERVAL_SECONDS: Final = 0.1
DEFAULT_CANCEL_GRACE_SECONDS: Final = 5.0
DEFAULT_MAX_OUTPUT_BYTES: Final = 256 * 1024
DEFAULT_LEASE_DURATION_SECONDS: Final = 30.0
DEFAULT_LEASE_REFRESH_INTERVAL_SECONDS: Final = 5.0
DEFAULT_LEASE_RECOVERY_INTERVAL_SECONDS: Final = 10.0
LEASE_RECOVERY_LIMIT: Final = 100
SESSION_ESTABLISH_TIMEOUT_SECONDS: Final = 1.0
STAT_MIN_FIELDS: Final = 3
STAT_PGRP_FIELD_INDEX: Final = 2
TRUNCATION_MARKER: Final = b"\n... [output truncated] ...\n"
PROTOCOL_ERROR_EXIT_CODE: Final = 2
JOBS_SCHEMA: Final = "lubko"
JOBS_TABLE: Final = "jobs"
JOBS_COLUMN_TYPES: Final = (("id", "uuid"), ("payload", "text"))
UTC_ISO_TEXT_SQL: Final = "to_char(now() at time zone 'utc', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"')"
UTC_ISO_SQL: Final = f"to_jsonb({UTC_ISO_TEXT_SQL})"
LEASE_EXPIRES_AT_SQL: Final = (
    "to_jsonb(to_char("
    "now() at time zone 'utc' + make_interval(secs => %s), "
    '\'YYYY-MM-DD"T"HH24:MI:SS.US"Z"\'))'
)


def _jsonb_set_chain(base: str, updates: list[tuple[str, str]]) -> str:
    """Compose nested ``jsonb_set`` calls updating JSON sub-paths.

    The outer value of every chain is cast back to ``::text`` by the caller so
    the ``payload`` column always stores opaque JSON text.

    Args:
        base: SQL expression producing the ``jsonb`` to start from, normally
            ``payload::jsonb``.
        updates: ``(path, value)`` pairs, outermost last, where ``path`` is a
            comma-separated JSON path and ``value`` is a ``jsonb`` expression.

    Returns:
        A nested ``jsonb_set`` SQL expression.
    """
    expr = base
    for path, value in updates:
        expr = f"jsonb_set({expr}, '{{{path}}}', {value})"
    return expr


@dataclass(frozen=True, slots=True)
class Job:
    """A claimed shell job.

    A job carries either a shell ``command`` (run through ``bash -lc``) or an
    argv-style ``args`` list (executed directly), never both.
    """

    id: UUID
    cwd: str
    command: str | None
    args: tuple[str, ...] | None


@dataclass(frozen=True, slots=True)
class ClaimedJob:
    """A claimed job together with its raw JSON payload text."""

    id: UUID
    payload: str


class SchemaInvariantError(RuntimeError):
    """Raised when ``lubko.jobs`` violates the two-column transport invariant."""


@dataclass(frozen=True, slots=True)
class JobResult:
    """Outcome of executing one job."""

    status: str
    exit_code: int
    stdout: str
    stderr: str
    cancellation_note: str | None


@dataclass(frozen=True, slots=True)
class ProcessRun:
    """A running shell process and its captured output files."""

    proc: subprocess.Popen[bytes]
    stdout_path: Path
    stderr_path: Path
    pid: int
    pgid: int


@dataclass(frozen=True, slots=True)
class Settings:
    """Runtime settings loaded from environment variables.

    Every running job carries a lease (``state.lease_expires_at``) that the
    owning worker refreshes by heartbeat; when the lease expires, a recovery
    pass marks the abandoned job failed. The lease duration must comfortably
    exceed the refresh interval so a healthy long-running job is never stolen.
    """

    worker_id: str
    poll_interval_seconds: float
    process_poll_interval_seconds: float
    cancel_grace_seconds: float
    max_output_bytes: int
    worker_incarnation: str = field(default_factory=lambda: uuid4().hex)
    lease_duration_seconds: float = DEFAULT_LEASE_DURATION_SECONDS
    lease_refresh_interval_seconds: float = DEFAULT_LEASE_REFRESH_INTERVAL_SECONDS
    lease_recovery_interval_seconds: float = DEFAULT_LEASE_RECOVERY_INTERVAL_SECONDS

    def __post_init__(self) -> None:
        """Validate lease timing so a live worker's lease never expires idle.

        Raises:
            ValueError: If any lease timing value is unusable.
        """
        if self.lease_duration_seconds <= 0:
            msg = "LUBKO_LEASE_DURATION_SECONDS must be positive"
            raise ValueError(msg)
        if self.lease_refresh_interval_seconds <= 0:
            msg = "LUBKO_LEASE_REFRESH_INTERVAL_SECONDS must be positive"
            raise ValueError(msg)
        if self.lease_recovery_interval_seconds <= 0:
            msg = "LUBKO_LEASE_RECOVERY_INTERVAL_SECONDS must be positive"
            raise ValueError(msg)
        if self.lease_refresh_interval_seconds >= self.lease_duration_seconds:
            msg = (
                "LUBKO_LEASE_REFRESH_INTERVAL_SECONDS must be smaller than "
                "LUBKO_LEASE_DURATION_SECONDS so a healthy worker's lease never "
                "expires between heartbeats"
            )
            raise ValueError(msg)

    @classmethod
    def from_environment(cls) -> Settings:
        """Load worker settings from environment variables.

        Returns:
            Settings derived from the process environment.
        """
        return cls(
            worker_id=os.getenv("LUBKO_WORKER_ID", socket.gethostname()),
            poll_interval_seconds=float(
                os.getenv(
                    "LUBKO_POLL_INTERVAL_SECONDS",
                    str(DEFAULT_POLL_INTERVAL_SECONDS),
                )
            ),
            process_poll_interval_seconds=float(
                os.getenv(
                    "LUBKO_PROCESS_POLL_INTERVAL_SECONDS",
                    str(DEFAULT_PROCESS_POLL_INTERVAL_SECONDS),
                )
            ),
            cancel_grace_seconds=float(
                os.getenv(
                    "LUBKO_CANCEL_GRACE_SECONDS",
                    str(DEFAULT_CANCEL_GRACE_SECONDS),
                )
            ),
            max_output_bytes=int(
                os.getenv("LUBKO_MAX_OUTPUT_BYTES", str(DEFAULT_MAX_OUTPUT_BYTES))
            ),
            lease_duration_seconds=float(
                os.getenv(
                    "LUBKO_LEASE_DURATION_SECONDS",
                    str(DEFAULT_LEASE_DURATION_SECONDS),
                )
            ),
            lease_refresh_interval_seconds=float(
                os.getenv(
                    "LUBKO_LEASE_REFRESH_INTERVAL_SECONDS",
                    str(DEFAULT_LEASE_REFRESH_INTERVAL_SECONDS),
                )
            ),
            lease_recovery_interval_seconds=float(
                os.getenv(
                    "LUBKO_LEASE_RECOVERY_INTERVAL_SECONDS",
                    str(DEFAULT_LEASE_RECOVERY_INTERVAL_SECONDS),
                )
            ),
        )


def truncate_output(data: bytes, limit: int) -> str:
    """Decode output while retaining at most the newest ``limit`` bytes.

    Args:
        data: Raw process output.
        limit: Maximum number of bytes to retain.

    Returns:
        UTF-8 text, replacing invalid byte sequences.

    Raises:
        ValueError: If ``limit`` is too small for the truncation marker.
    """
    if limit < len(TRUNCATION_MARKER):
        msg = "output limit must be at least as large as the truncation marker"
        raise ValueError(msg)

    if len(data) > limit:
        payload = TRUNCATION_MARKER + data[-(limit - len(TRUNCATION_MARKER)) :]
    else:
        payload = data
    return payload.decode("utf-8", errors="replace")


def claim_job(conn: JobsConnection, settings: Settings) -> ClaimedJob | None:
    """Atomically claim the oldest pending job.

    Claiming uses the same two-phase approach as before: the pending selection
    is locked with ``FOR UPDATE SKIP LOCKED`` and the mutable claim state is
    written with a compare-and-swap update of the JSON payload, so several
    workers can safely compete for the same queue. No table column is added.

    The claim records the worker's incarnation and grants the job a lease by
    writing ``state.lease_expires_at``; the owning worker refreshes that lease
    by heartbeat while the job runs. Recovery acts only on leases that have
    truly expired.

    Args:
        conn: Open PostgreSQL connection.
        settings: Worker runtime settings.

    Returns:
        The claimed job and its payload text, or ``None`` if the queue is empty.
    """
    set_chain = _jsonb_set_chain(
        "job.payload::jsonb",
        [
            ("state,status", "to_jsonb('running'::text)"),
            (
                "state,created_at",
                (
                    "COALESCE("
                    f"to_jsonb((job.payload::jsonb)->'state'->>'created_at'), "
                    f"{UTC_ISO_SQL})"
                ),
            ),
            ("state,started_at", UTC_ISO_SQL),
            ("state,worker_id", "to_jsonb(%s::text)"),
            ("state,worker_incarnation", "to_jsonb(%s::text)"),
            ("state,lease_expires_at", LEASE_EXPIRES_AT_SQL),
            ("state,updated_at", UTC_ISO_SQL),
        ],
    )
    with conn.transaction(), conn.cursor(row_factory=tuple_row) as cursor:
        # The SQL text is assembled from internal constants only; every value
        # from the outside is bound as a %s parameter. No user input ever
        # reaches the SQL string itself.
        cursor.execute(
            "WITH next AS (\n"  # ruff: ignore[hardcoded-sql-expression]
            "    SELECT id\n"
            "    FROM lubko.jobs\n"
            "    WHERE (payload::jsonb)->'state'->>'status' = 'pending'\n"
            "    ORDER BY (payload::jsonb)->'state'->>'created_at', id\n"
            "    FOR UPDATE SKIP LOCKED\n"
            "    LIMIT 1\n"
            ")\n"
            "UPDATE lubko.jobs AS job\n"
            "SET payload = " + set_chain + "::text\n"
            "FROM next\n"
            "WHERE job.id = next.id\n"
            "RETURNING job.id, job.payload\n",
            (
                settings.worker_id,
                settings.worker_incarnation,
                settings.lease_duration_seconds,
            ),
        )
        row = cursor.fetchone()
    if row is None:
        return None
    job_id, payload = row
    return ClaimedJob(id=job_id, payload=payload)


def request_cancel(conn: JobsConnection, job_id: UUID) -> str:
    """Request cancellation of a job using the documented SQL contract.

    A pending job is cancelled immediately without ever being spawned. A
    running job has its ``state.cancel_requested_at`` marker set and is
    terminated by the worker. An already terminal job is left unchanged. All
    cancellation state lives inside the JSON ``payload``; no table column is
    added.

    Args:
        conn: Open PostgreSQL connection.
        job_id: Identifier of the job to cancel.

    Returns:
        The resulting status: ``cancelled``, ``running``, or the unchanged
        terminal status of an already completed job.

    Raises:
        ValueError: If the job does not exist.
    """
    pending_chain = _jsonb_set_chain(
        "payload::jsonb",
        [
            ("state,status", "to_jsonb('cancelled'::text)"),
            ("state,cancel_requested_at", UTC_ISO_SQL),
            ("state,finished_at", UTC_ISO_SQL),
            ("state,updated_at", UTC_ISO_SQL),
            (
                "result",
                (
                    "jsonb_build_object("
                    "'stdout', '', "
                    "'stderr', '', "
                    "'exit_code', null, "
                    "'cancellation_note', 'cancelled before the worker claimed the job')"
                ),
            ),
        ],
    )
    with conn.transaction(), conn.cursor(row_factory=tuple_row) as cursor:
        cursor.execute(
            "UPDATE lubko.jobs\n"
            "SET payload = " + pending_chain + "::text\n"
            "WHERE id = %s AND (payload::jsonb)->'state'->>'status' = 'pending'\n"
            "RETURNING (payload::jsonb)->'state'->>'status'\n",
            (job_id,),
        )
        row = cursor.fetchone()
        if row is not None:
            return str(row[0])

    running_chain = _jsonb_set_chain(
        "payload::jsonb",
        [
            ("state,cancel_requested_at", UTC_ISO_SQL),
            ("state,updated_at", UTC_ISO_SQL),
        ],
    )
    with conn.transaction(), conn.cursor(row_factory=tuple_row) as cursor:
        cursor.execute(
            "UPDATE lubko.jobs\n"
            "SET payload = " + running_chain + "::text\n"
            "WHERE id = %s AND (payload::jsonb)->'state'->>'status' = 'running'\n"
            "RETURNING (payload::jsonb)->'state'->>'status'\n",
            (job_id,),
        )
        row = cursor.fetchone()
        if row is not None:
            return str(row[0])

    with conn.transaction(), conn.cursor(row_factory=tuple_row) as cursor:
        cursor.execute(
            "SELECT (payload::jsonb)->'state'->>'status' FROM lubko.jobs WHERE id = %s",
            (job_id,),
        )
        row = cursor.fetchone()
    if row is None:
        msg = f"job {job_id} not found"
        raise ValueError(msg)
    return str(row[0])


def resolve_shell() -> str | None:
    """Locate the shell executable used to run jobs.

    Returns:
        Absolute path to the shell, or ``None`` if it is not installed.
    """
    return shutil.which("bash")


def _persist_process(conn: JobsConnection, job_id: UUID, pid: int, pgid: int) -> None:
    """Persist the exact process identity of a running job.

    The identity is written into ``payload.state.process_pid`` and
    ``payload.state.process_pgid``, keeping the two-column table invariant.

    Args:
        conn: Open PostgreSQL connection.
        job_id: Identifier of the running job.
        pid: Exact process ID of the shell process.
        pgid: Exact process group ID of the shell process.
    """
    set_chain = _jsonb_set_chain(
        "payload::jsonb",
        [
            ("state,process_pid", "to_jsonb(%s::int)"),
            ("state,process_pgid", "to_jsonb(%s::int)"),
            ("state,updated_at", UTC_ISO_SQL),
        ],
    )
    with conn.transaction(), conn.cursor() as cursor:
        cursor.execute(
            "UPDATE lubko.jobs\nSET payload = " + set_chain + "::text\nWHERE id = %s\n",
            (pid, pgid, job_id),
        )


def _refresh_lease(conn: JobsConnection, job_id: UUID, lease_duration_seconds: float) -> None:
    """Refresh the running job's lease by heartbeat.

    The heartbeat rewrites ``state.lease_expires_at`` (and ``state.updated_at``)
    inside the JSON payload, keeping the two-column table invariant. A healthy
    long-running job therefore never appears stale to a recovery pass.

    Args:
        conn: Open PostgreSQL connection.
        job_id: Identifier of the running job.
        lease_duration_seconds: How far into the future the refreshed lease runs.
    """
    set_chain = _jsonb_set_chain(
        "payload::jsonb",
        [
            ("state,lease_expires_at", LEASE_EXPIRES_AT_SQL),
            ("state,updated_at", UTC_ISO_SQL),
        ],
    )
    with conn.transaction(), conn.cursor() as cursor:
        cursor.execute(
            "UPDATE lubko.jobs\n"
            "SET payload = " + set_chain + "::text\n"
            "WHERE id = %s AND (payload::jsonb)->'state'->>'status' = 'running'\n",
            (lease_duration_seconds, job_id),
        )


def _is_cancel_requested(conn: JobsConnection, job_id: UUID) -> bool:
    """Return whether the job has a cancellation request pending.

    Args:
        conn: Open PostgreSQL connection.
        job_id: Identifier of the running job.

    Returns:
        ``True`` when ``state.cancel_requested_at`` is set.
    """
    with conn.transaction(), conn.cursor(row_factory=tuple_row) as cursor:
        cursor.execute(
            "SELECT (payload::jsonb)->'state'->>'cancel_requested_at'\n"
            "FROM lubko.jobs WHERE id = %s",
            (job_id,),
        )
        row = cursor.fetchone()
    return row is not None and row[0] is not None


def _read_job_status(conn: JobsConnection, job_id: UUID) -> str:
    """Read the current status of a job.

    Args:
        conn: Open PostgreSQL connection.
        job_id: Identifier of the job.

    Returns:
        The current job status.

    Raises:
        RuntimeError: If the job no longer exists.
    """
    with conn.transaction(), conn.cursor(row_factory=tuple_row) as cursor:
        cursor.execute(
            "SELECT (payload::jsonb)->'state'->>'status' FROM lubko.jobs WHERE id = %s",
            (job_id,),
        )
        row = cursor.fetchone()
    if row is None:
        msg = f"job {job_id} disappeared while finalizing"
        raise RuntimeError(msg)
    return str(row[0])


def _recovery_result_expression() -> str:
    """Build the SQL result object recorded on a recovered stale job.

    The diagnostic names the expired lease and the owning worker incarnation so
    the orchestrator can see exactly why the job failed. Only internal constants
    and the row's own JSON are referenced; no external input reaches the SQL.

    Returns:
        A ``jsonb_build_object`` expression suitable for ``jsonb_set``.
    """
    return (
        "jsonb_build_object("
        "'stdout', to_jsonb(''::text), "
        "'stderr', to_jsonb(''::text), "
        "'exit_code', to_jsonb(NULL::int), "
        "'recovery_note', to_jsonb("
        "'lease expired at ' || COALESCE("
        "(job.payload::jsonb)->'state'->>'lease_expires_at', '<none>') || "
        "'; owning worker ' || COALESCE("
        "(job.payload::jsonb)->'state'->>'worker_id', '<unknown>') || "
        "' (incarnation ' || COALESCE("
        "(job.payload::jsonb)->'state'->>'worker_incarnation', '<unknown>') || "
        "') stopped heartbeating; job marked failed rather than re-executed'"
        ")"
        ")"
    )


def recover_stale_jobs(conn: JobsConnection) -> list[tuple[UUID, str]]:
    """Atomically mark jobs whose lease has truly expired as failed.

    A job whose ``state.lease_expires_at`` is in the past is presumed abandoned
    by a crashed or unreachable worker. Recovery marks it ``failed`` with a
    clear diagnostic and never re-executes it, so two workers can never execute
    the same job. Rows are locked with ``FOR UPDATE SKIP LOCKED`` and the status
    transition is a single atomic update, making the pass safe under many
    concurrent workers.

    A running job without a lease field is never selected: recovery acts only on
    leases that are present and expired, so pre-lease payloads are left for
    manual repair.

    Args:
        conn: Open PostgreSQL connection.

    Returns:
        The ``(id, payload)`` pairs of the recovered jobs.
    """
    set_chain = _jsonb_set_chain(
        "job.payload::jsonb",
        [
            ("state,status", "to_jsonb('failed'::text)"),
            ("state,finished_at", UTC_ISO_SQL),
            ("state,recovered_at", UTC_ISO_SQL),
            ("state,updated_at", UTC_ISO_SQL),
            ("result", _recovery_result_expression()),
        ],
    )
    with conn.transaction(), conn.cursor(row_factory=tuple_row) as cursor:
        cursor.execute(
            "WITH stale AS (\n"  # ruff: ignore[hardcoded-sql-expression]
            "    SELECT id\n"
            "    FROM lubko.jobs\n"
            "    WHERE (payload::jsonb)->'state'->>'status' = 'running'\n"
            "        AND (payload::jsonb)->'state'->>'lease_expires_at' IS NOT NULL\n"
            "        AND ((payload::jsonb)->'state'->>'lease_expires_at') < "
            + UTC_ISO_TEXT_SQL
            + "\n"
            "    ORDER BY id\n"
            "    FOR UPDATE SKIP LOCKED\n"
            "    LIMIT %(limit)s\n"
            ")\n"
            "UPDATE lubko.jobs AS job\n"
            "SET payload = " + set_chain + "::text\n"
            "FROM stale\n"
            "WHERE job.id = stale.id\n"
            "RETURNING job.id, job.payload\n",
            {"limit": LEASE_RECOVERY_LIMIT},
        )
        rows = cursor.fetchall()
    return [(row[0], str(row[1])) for row in rows]


def _process_pgrp(pid: int) -> int | None:
    """Return the exact process group of a running process.

    Zombie and dead processes report no group. Unreadable or unparseable
    process table entries are ignored.

    Args:
        pid: Process ID to inspect.

    Returns:
        The process group ID, or ``None`` if the process is dead or unknown.
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
    if fields[0] in {b"Z", b"X"}:
        return None
    try:
        return int(fields[STAT_PGRP_FIELD_INDEX])
    except ValueError:
        return None


def group_has_members(pgid: int) -> bool:
    """Return whether any live process still belongs to the exact process group.

    Uses the process table under ``/proc`` when available so membership is
    matched by exact process group, never by process name. Falls back to
    querying the kernel directly otherwise.

    Args:
        pgid: Process group identifier to inspect.

    Returns:
        ``True`` when at least one running process still belongs to the group.
    """
    proc_dir = Path("/proc")
    if proc_dir.is_dir():
        for entry in proc_dir.iterdir():
            if not entry.name.isdigit():
                continue
            if _process_pgrp(int(entry.name)) == pgid:
                return True
        return False
    try:
        os.getpgid(pgid)
    except ProcessLookupError:
        return False
    return True


def _wait_for_session(pid: int) -> int:
    """Wait until a spawned process establishes its own session and group.

    The child calls ``setsid`` between ``fork`` and ``exec``, after which its
    process group ID equals its process ID. If the child already exited before
    it was observed, its exact group identity would have been its own PID.

    Args:
        pid: Process ID of the spawned child.

    Returns:
        The exact process group ID of the child.
    """
    deadline = time.monotonic() + SESSION_ESTABLISH_TIMEOUT_SECONDS
    while True:
        try:
            pgid = os.getpgid(pid)
        except ProcessLookupError:
            return pid
        if pgid == pid:
            return pgid
        if time.monotonic() >= deadline:
            return pid
        time.sleep(0.01)


def _cleanup_output_files(stdout_path: Path, stderr_path: Path) -> None:
    """Remove captured output files, ignoring any that are already gone.

    Args:
        stdout_path: Capture file for standard output.
        stderr_path: Capture file for standard error.
    """
    stdout_path.unlink(missing_ok=True)
    stderr_path.unlink(missing_ok=True)


def spawn_job(job: Job, shell: str) -> ProcessRun:
    """Start a job as a new session and process group leader.

    A shell ``command`` job runs through ``bash -lc``; an ``args`` job is
    executed directly. Both are started as a new session so cancellation can
    signal the exact process group.

    Args:
        job: Claimed job to execute.
        shell: Absolute path to the shell executable, used for ``command``
            jobs and ignored for ``args`` jobs.

    Returns:
        Handle to the running process with its captured output files.

    Raises:
        OSError: If the command cannot be started.
        ValueError: If the job request has neither ``command`` nor ``args``.
    """
    if job.command is not None:
        argv = [shell, "-lc", job.command]
    elif job.args:
        argv = list(job.args)
    else:
        msg = "job request must provide command or args"
        raise ValueError(msg)
    stdout_fd, stdout_name = tempfile.mkstemp()
    stderr_fd, stderr_name = tempfile.mkstemp()
    stdout_path = Path(stdout_name)
    stderr_path = Path(stderr_name)
    try:
        proc = subprocess.Popen(
            argv,
            cwd=job.cwd,
            stdin=subprocess.DEVNULL,
            stdout=stdout_fd,
            stderr=stderr_fd,
            start_new_session=True,
        )
    except OSError:
        os.close(stdout_fd)
        os.close(stderr_fd)
        _cleanup_output_files(stdout_path, stderr_path)
        raise
    os.close(stdout_fd)
    os.close(stderr_fd)
    pgid = _wait_for_session(proc.pid)
    return ProcessRun(
        proc=proc,
        stdout_path=stdout_path,
        stderr_path=stderr_path,
        pid=proc.pid,
        pgid=pgid,
    )


def read_output(path: Path) -> bytes:
    """Read all bytes captured so far into an output file.

    Args:
        path: Capture file for the stream.

    Returns:
        The captured bytes.
    """
    return path.read_bytes()


def _signal_group(pgid: int, sig: int) -> None:
    """Send a signal to an exact process group, ignoring an already-gone group.

    Args:
        pgid: Process group identifier to signal.
        sig: Signal to send.
    """
    try:
        os.killpg(pgid, sig)
    except ProcessLookupError:
        LOGGER.debug("process group %d already gone", pgid)


def cancel_process_group(run: ProcessRun, settings: Settings) -> tuple[int, str]:
    """Terminate a running job by signalling its exact process group.

    Sends ``SIGTERM`` to the recorded process group, allows a bounded grace
    period for the group to exit, then sends ``SIGKILL`` while members remain.
    Signals are never sent once the tracked process is known to be fully gone,
    so a recycled process group cannot be signalled.

    Args:
        run: Running job process.
        settings: Worker runtime settings.

    Returns:
        The process exit code and a diagnostic note.
    """
    note = "cancelled: process group already gone"
    if group_has_members(run.pgid):
        _signal_group(run.pgid, signal.SIGTERM)
        note = "cancelled: sent SIGTERM to process group"

    deadline = time.monotonic() + settings.cancel_grace_seconds
    while time.monotonic() < deadline:
        if run.proc.poll() is not None and not group_has_members(run.pgid):
            return run.proc.returncode or 0, note
        time.sleep(settings.process_poll_interval_seconds)

    if group_has_members(run.pgid):
        _signal_group(run.pgid, signal.SIGKILL)
        note = f"{note}; grace period expired, sent SIGKILL to process group"
    try:
        return run.proc.wait(timeout=settings.cancel_grace_seconds), note
    except subprocess.TimeoutExpired:
        return run.proc.poll() or 0, note


def _monitor_poll_loop(
    conn: JobsConnection,
    job: Job,
    run: ProcessRun,
    settings: Settings,
) -> str | None:
    """Poll a running job until it exits or a cancellation request arrives.

    On every loop iteration the job's cancellation marker is checked; the lease
    is refreshed by heartbeat at most once per ``lease_refresh_interval_seconds``
    so a healthy long-running job never looks stale to a recovery pass. Any
    database error propagates to :func:`monitor_job`, which terminates the exact
    process group before reconnecting.

    Args:
        conn: Open PostgreSQL connection.
        job: Running job.
        run: Running job process.
        settings: Worker runtime settings.

    Returns:
        The cancellation diagnostic, or ``None`` if the job ran to completion.
    """
    cancellation_note: str | None = None
    next_heartbeat_at = 0.0
    while run.proc.poll() is None:
        if _is_cancel_requested(conn, job.id):
            _, cancellation_note = cancel_process_group(run, settings)
            break
        now = time.monotonic()
        if now >= next_heartbeat_at:
            _refresh_lease(conn, job.id, settings.lease_duration_seconds)
            next_heartbeat_at = now + settings.lease_refresh_interval_seconds
        time.sleep(settings.process_poll_interval_seconds)
    return cancellation_note


def monitor_job(conn: JobsConnection, job: Job, run: ProcessRun, settings: Settings) -> JobResult:
    """Poll a running job for completion or a cancellation request.

    Args:
        conn: Open PostgreSQL connection.
        job: Running job.
        run: Running job process.
        settings: Worker runtime settings.

    Returns:
        The final job result.

    Raises:
        psycopg.Error: If polling the database fails; the process group is
            terminated first.
    """
    try:
        cancellation_note = _monitor_poll_loop(conn, job, run, settings)
    except psycopg.Error:
        LOGGER.exception("database error while polling job %s", job.id)
        try:
            cancel_process_group(run, settings)
        finally:
            _cleanup_output_files(run.stdout_path, run.stderr_path)
        raise

    exit_code = run.proc.poll()
    if exit_code is None:
        exit_code = 0
    stdout = truncate_output(read_output(run.stdout_path), settings.max_output_bytes)
    stderr = truncate_output(read_output(run.stderr_path), settings.max_output_bytes)
    _cleanup_output_files(run.stdout_path, run.stderr_path)
    if cancellation_note is not None:
        status = "cancelled"
    else:
        status = "succeeded" if exit_code == 0 else "failed"
    return JobResult(
        status=status,
        exit_code=exit_code,
        stdout=stdout,
        stderr=stderr,
        cancellation_note=cancellation_note,
    )


def run_job(conn: JobsConnection, job: Job, settings: Settings) -> JobResult:
    """Execute one job directly in its requested working directory.

    The job is started as a new session and process group leader and may be
    cancelled by the orchestrator while it runs.

    Args:
        conn: Open PostgreSQL connection.
        job: Claimed job to execute.
        settings: Worker runtime settings.

    Returns:
        The final job result.
    """
    if job.command is not None:
        shell = resolve_shell()
        if shell is None:
            return JobResult(
                status="failed",
                exit_code=127,
                stdout="",
                stderr="unable to execute job: shell executable not found",
                cancellation_note=None,
            )
    else:
        if not job.args:
            return JobResult(
                status="failed",
                exit_code=127,
                stdout="",
                stderr="unable to execute job: request has neither command nor args",
                cancellation_note=None,
            )
        shell = ""

    if not Path(job.cwd).is_dir():
        return JobResult(
            status="failed",
            exit_code=127,
            stdout="",
            stderr=f"unable to enter working directory {job.cwd!r}: directory does not exist",
            cancellation_note=None,
        )

    try:
        run = spawn_job(job, shell)
    except PermissionError as exc:
        return JobResult(
            status="failed",
            exit_code=127,
            stdout="",
            stderr=f"unable to enter working directory {job.cwd!r}: {exc}",
            cancellation_note=None,
        )
    except OSError as exc:
        return JobResult(
            status="failed",
            exit_code=127,
            stdout="",
            stderr=f"unable to execute job: {exc}",
            cancellation_note=None,
        )

    try:
        _persist_process(conn, job.id, run.pid, run.pgid)
    except psycopg.Error:
        LOGGER.exception("unable to persist process identity for job %s", job.id)
        _signal_group(run.pgid, signal.SIGKILL)
        try:
            run.proc.wait(timeout=settings.cancel_grace_seconds)
        except subprocess.TimeoutExpired:
            LOGGER.warning("process %d did not exit after SIGKILL", run.pid)
        _cleanup_output_files(run.stdout_path, run.stderr_path)
        raise

    return monitor_job(conn, job, run, settings)


def finish_job(conn: JobsConnection, job_id: UUID, result: JobResult) -> str:
    """Persist the final result of a job into its JSON payload.

    A cancellation request accepted before finalization wins over a natural
    completion. Already terminal jobs are never rewritten. Only ``id`` and
    ``payload`` are touched, preserving the two-column table invariant.

    Args:
        conn: Open PostgreSQL connection.
        job_id: Identifier of the job being completed.
        result: Final job result.

    Returns:
        The persisted final status.
    """
    # The whole result object is assembled atomically with jsonb_build_object.
    # A bare to_jsonb(NULL) inside jsonb_set would make the whole update SQL
    # NULL, violating payload NOT NULL; jsonb_build_object turns SQL null into
    # JSON null and replaces/creates the result parent in one jsonb_set call.
    set_chain = _jsonb_set_chain(
        "payload::jsonb",
        [
            (
                "state,status",
                (
                    "CASE\n"
                    "    WHEN (payload::jsonb)->'state'->>'cancel_requested_at' IS NOT NULL\n"
                    "        THEN to_jsonb('cancelled'::text)\n"
                    "    ELSE to_jsonb(%(status)s::text)\n"
                    "END"
                ),
            ),
            ("state,finished_at", UTC_ISO_SQL),
            ("state,updated_at", UTC_ISO_SQL),
            (
                "result",
                (
                    "jsonb_build_object("
                    "'stdout', to_jsonb(%(stdout)s::text), "
                    "'stderr', to_jsonb(%(stderr)s::text), "
                    "'exit_code', to_jsonb(%(exit_code)s::int), "
                    "'cancellation_note', "
                    "CASE\n"
                    "    WHEN (payload::jsonb)->'state'->>'cancel_requested_at' IS NOT NULL\n"
                    "        THEN COALESCE(\n"
                    "            to_jsonb(%(cancellation_note)s::text),\n"
                    "            to_jsonb('cancelled by request'::text))\n"
                    "    ELSE to_jsonb(%(cancellation_note)s::text)\n"
                    "END"
                    ")"
                ),
            ),
        ],
    )
    with conn.transaction(), conn.cursor(row_factory=tuple_row) as cursor:
        cursor.execute(
            "UPDATE lubko.jobs\n"
            "SET payload = " + set_chain + "::text\n"
            "WHERE id = %(job_id)s AND (payload::jsonb)->'state'->>'status' = 'running'\n"
            "RETURNING (payload::jsonb)->'state'->>'status'\n",
            {
                "status": result.status,
                "stdout": result.stdout,
                "stderr": result.stderr,
                "exit_code": result.exit_code,
                "cancellation_note": result.cancellation_note,
                "job_id": job_id,
            },
        )
        row = cursor.fetchone()
    if row is not None:
        return str(row[0])
    return _read_job_status(conn, job_id)


def claim_and_process_one(conn: JobsConnection, settings: Settings) -> bool:
    """Claim and process a single pending job.

    Args:
        conn: Open PostgreSQL connection.
        settings: Worker runtime settings.

    Returns:
        ``True`` if a job was processed, ``False`` if the queue was empty.
    """
    claimed = claim_job(conn, settings)
    if claimed is None:
        return False

    try:
        payload = parse_payload(claimed.payload)
    except ProtocolError as exc:
        LOGGER.warning("rejecting unparseable job %s: %s", claimed.id, exc)
        result = JobResult(
            status="failed",
            exit_code=PROTOCOL_ERROR_EXIT_CODE,
            stdout="",
            stderr=f"invalid job payload: {exc}",
            cancellation_note=None,
        )
        finish_job(conn, claimed.id, result)
        return True

    job = Job(
        id=claimed.id,
        cwd=payload.request.cwd,
        command=payload.request.command,
        args=payload.request.args,
    )
    LOGGER.info("claimed job %s", job.id)
    result = run_job(conn, job, settings)
    final_status = finish_job(conn, job.id, result)
    LOGGER.info(
        "finished job %s with status %s and exit code %d",
        job.id,
        final_status,
        result.exit_code,
    )
    return True


def process_jobs(conn: JobsConnection, settings: Settings) -> None:
    """Process jobs until the database connection fails or the process exits.

    Before every claim, a rate-limited recovery pass marks any job whose lease
    has truly expired as failed, so jobs stranded by a crashed worker are
    recovered automatically even when a freshly started worker is the only one
    running.

    Args:
        conn: Open PostgreSQL connection.
        settings: Worker runtime settings.
    """
    next_recovery_at = 0.0
    while True:
        now = time.monotonic()
        if now >= next_recovery_at:
            for job_id, _payload in recover_stale_jobs(conn):
                LOGGER.warning(
                    "recovered stale job %s: lease expired; marked failed rather than re-executed",
                    job_id,
                )
            next_recovery_at = time.monotonic() + settings.lease_recovery_interval_seconds
        if not claim_and_process_one(conn, settings):
            time.sleep(settings.poll_interval_seconds)


def verify_jobs_table_invariant(conn: JobsConnection) -> None:
    """Assert that ``lubko.jobs`` keeps exactly the two protocol columns.

    The transport table must have exactly two columns forever: ``id`` (unique
    random ``uuid``) and ``payload`` (one string containing a JSON object,
    stored as ``text``). The worker refuses to run against a table that
    drifted, enforcing the invariant documented in ``docs/protocol.md``.

    Args:
        conn: Open PostgreSQL connection.

    Raises:
        SchemaInvariantError: If the table does not have exactly ``id`` and
            ``payload`` as its only columns, with ``payload`` of type ``text``.
    """
    # The read runs inside its own top-level transaction so it commits cleanly
    # before the processing loop. Without it the default implicit transaction
    # stays open and every later conn.transaction() block becomes a savepoint,
    # so claimed job updates would never commit.
    with conn.transaction(), conn.cursor(row_factory=tuple_row) as cursor:
        cursor.execute(
            "SELECT column_name, data_type\n"
            "FROM information_schema.columns\n"
            "WHERE table_schema = %s AND table_name = %s\n"
            "ORDER BY column_name\n",
            (JOBS_SCHEMA, JOBS_TABLE),
        )
        columns = [(str(row[0]), str(row[1])) for row in cursor.fetchall()]
    if columns != list(JOBS_COLUMN_TYPES):
        found = ", ".join(f"{name} {kind}" for name, kind in columns) if columns else "none"
        expected = ", ".join(f"{name} {kind}" for name, kind in JOBS_COLUMN_TYPES)
        msg = (
            f"lubko.jobs violates the two-column transport invariant: "
            f"expected columns {expected} but found {found}. "
            f"{TWO_COLUMN_INVARIANT}"
        )
        raise SchemaInvariantError(msg)


def run(settings: Settings, database: DatabaseConfig) -> None:
    """Reconnect to PostgreSQL as needed and process jobs forever.

    Every connection is first checked against the two-column transport
    invariant; the worker exits loudly if the schema drifted.

    Args:
        settings: Worker runtime settings.
        database: Database connection settings loaded from the restricted file.
    """
    while True:
        try:
            with psycopg.connect(database.conninfo(), row_factory=tuple_row) as conn:
                verify_jobs_table_invariant(conn)
                process_jobs(conn, settings)
        except psycopg.Error:
            LOGGER.exception("database connection failed; retrying")
            time.sleep(settings.poll_interval_seconds)


def main() -> None:
    """Run the Lubko worker.

    Raises:
        SystemExit: If the database configuration file cannot be loaded or the
            runtime settings are invalid.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        database = load_database_config()
    except (OSError, ValueError):
        LOGGER.exception("unable to load database configuration")
        raise SystemExit(1) from None
    try:
        settings = Settings.from_environment()
    except ValueError:
        LOGGER.exception("invalid worker runtime settings")
        raise SystemExit(1) from None
    run(settings, database)


if __name__ == "__main__":
    main()
