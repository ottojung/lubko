"""Poll PostgreSQL for jobs, execute them directly, and support cancellation."""

from __future__ import annotations

import logging
import os
import shutil
import signal
import socket
import subprocess
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Final

import psycopg
from psycopg.rows import class_row, tuple_row

from lubko.config import load_database_config

if TYPE_CHECKING:
    from uuid import UUID

    from lubko.config import DatabaseConfig

LOGGER: Final = logging.getLogger(__name__)
DEFAULT_POLL_INTERVAL_SECONDS: Final = 1.0
DEFAULT_PROCESS_POLL_INTERVAL_SECONDS: Final = 0.1
DEFAULT_CANCEL_GRACE_SECONDS: Final = 5.0
DEFAULT_MAX_OUTPUT_BYTES: Final = 256 * 1024
SESSION_ESTABLISH_TIMEOUT_SECONDS: Final = 1.0
STAT_MIN_FIELDS: Final = 3
STAT_PGRP_FIELD_INDEX: Final = 2
TRUNCATION_MARKER: Final = b"\n... [output truncated] ...\n"


@dataclass(frozen=True, slots=True)
class Job:
    """A claimed shell job."""

    id: UUID
    cwd: str
    command: str


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
    """Runtime settings loaded from environment variables."""

    worker_id: str
    poll_interval_seconds: float
    process_poll_interval_seconds: float
    cancel_grace_seconds: float
    max_output_bytes: int

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


def claim_job(conn: psycopg.Connection[Job], worker_id: str) -> Job | None:
    """Atomically claim the oldest pending job.

    Args:
        conn: Open PostgreSQL connection.
        worker_id: Identifier to record on the claimed job.

    Returns:
        The claimed job, or ``None`` if the queue is empty.
    """
    with conn.transaction(), conn.cursor() as cursor:
        cursor.execute(
            """
            WITH next AS (
                SELECT id
                FROM lubko.jobs
                WHERE status = 'pending'
                ORDER BY created_at
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            UPDATE lubko.jobs AS job
            SET status = 'running',
                worker_id = %s,
                started_at = now(),
                updated_at = now()
            FROM next
            WHERE job.id = next.id
            RETURNING job.id, job.cwd, job.command
            """,
            (worker_id,),
        )
        return cursor.fetchone()


def request_cancel(conn: psycopg.Connection[Job], job_id: UUID) -> str:
    """Request cancellation of a job using the documented SQL contract.

    A pending job is cancelled immediately without ever being spawned. A
    running job has its ``cancel_requested_at`` marker set and is terminated
    by the worker. An already terminal job is left unchanged.

    Args:
        conn: Open PostgreSQL connection.
        job_id: Identifier of the job to cancel.

    Returns:
        The resulting status: ``cancelled``, ``running``, or the unchanged
        terminal status of an already completed job.

    Raises:
        ValueError: If the job does not exist.
    """
    with conn.transaction(), conn.cursor(row_factory=tuple_row) as cursor:
        cursor.execute(
            """
            UPDATE lubko.jobs
            SET status = 'cancelled',
                cancel_requested_at = now(),
                cancellation_note = 'cancelled before the worker claimed the job',
                finished_at = now(),
                updated_at = now()
            WHERE id = %s AND status = 'pending'
            RETURNING status
            """,
            (job_id,),
        )
        row = cursor.fetchone()
        if row is not None:
            return str(row[0])

    with conn.transaction(), conn.cursor(row_factory=tuple_row) as cursor:
        cursor.execute(
            """
            UPDATE lubko.jobs
            SET cancel_requested_at = now(),
                updated_at = now()
            WHERE id = %s AND status = 'running'
            RETURNING status
            """,
            (job_id,),
        )
        row = cursor.fetchone()
        if row is not None:
            return str(row[0])

    with conn.transaction(), conn.cursor(row_factory=tuple_row) as cursor:
        cursor.execute("SELECT status FROM lubko.jobs WHERE id = %s", (job_id,))
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


def _persist_process(conn: psycopg.Connection[Job], job_id: UUID, pid: int, pgid: int) -> None:
    """Persist the exact process identity of a running job.

    Args:
        conn: Open PostgreSQL connection.
        job_id: Identifier of the running job.
        pid: Exact process ID of the shell process.
        pgid: Exact process group ID of the shell process.
    """
    with conn.transaction(), conn.cursor() as cursor:
        cursor.execute(
            """
            UPDATE lubko.jobs
            SET process_pid = %s,
                process_pgid = %s,
                updated_at = now()
            WHERE id = %s
            """,
            (pid, pgid, job_id),
        )


def _is_cancel_requested(conn: psycopg.Connection[Job], job_id: UUID) -> bool:
    """Return whether the job has a cancellation request pending.

    Args:
        conn: Open PostgreSQL connection.
        job_id: Identifier of the running job.

    Returns:
        ``True`` when ``cancel_requested_at`` is set.
    """
    with conn.transaction(), conn.cursor(row_factory=tuple_row) as cursor:
        cursor.execute(
            "SELECT cancel_requested_at FROM lubko.jobs WHERE id = %s",
            (job_id,),
        )
        row = cursor.fetchone()
    return row is not None and row[0] is not None


def _read_job_status(conn: psycopg.Connection[Job], job_id: UUID) -> str:
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
        cursor.execute("SELECT status FROM lubko.jobs WHERE id = %s", (job_id,))
        row = cursor.fetchone()
    if row is None:
        msg = f"job {job_id} disappeared while finalizing"
        raise RuntimeError(msg)
    return str(row[0])


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
    """Start a job shell as a new session and process group leader.

    Args:
        job: Claimed job to execute.
        shell: Absolute path to the shell executable.

    Returns:
        Handle to the running process with its captured output files.

    Raises:
        OSError: If the shell cannot be started.
    """
    stdout_fd, stdout_name = tempfile.mkstemp()
    stderr_fd, stderr_name = tempfile.mkstemp()
    stdout_path = Path(stdout_name)
    stderr_path = Path(stderr_name)
    try:
        proc = subprocess.Popen(
            [shell, "-lc", job.command],
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


def monitor_job(
    conn: psycopg.Connection[Job], job: Job, run: ProcessRun, settings: Settings
) -> JobResult:
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
    cancellation_note: str | None = None
    try:
        while run.proc.poll() is None:
            if _is_cancel_requested(conn, job.id):
                _, cancellation_note = cancel_process_group(run, settings)
                break
            time.sleep(settings.process_poll_interval_seconds)
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


def run_job(conn: psycopg.Connection[Job], job: Job, settings: Settings) -> JobResult:
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
    shell = resolve_shell()
    if shell is None:
        return JobResult(
            status="failed",
            exit_code=127,
            stdout="",
            stderr="unable to execute job: shell executable not found",
            cancellation_note=None,
        )

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


def finish_job(conn: psycopg.Connection[Job], job: Job, result: JobResult) -> str:
    """Persist the final result of a job.

    A cancellation request accepted before finalization wins over a natural
    completion. Already terminal jobs are never rewritten.

    Args:
        conn: Open PostgreSQL connection.
        job: Job being completed.
        result: Final job result.

    Returns:
        The persisted final status.
    """
    with conn.transaction(), conn.cursor(row_factory=tuple_row) as cursor:
        cursor.execute(
            """
            UPDATE lubko.jobs
            SET status = CASE
                    WHEN cancel_requested_at IS NOT NULL THEN 'cancelled'
                    ELSE %(status)s
                END,
                stdout = %(stdout)s,
                stderr = %(stderr)s,
                exit_code = %(exit_code)s,
                cancellation_note = CASE
                    WHEN cancel_requested_at IS NOT NULL
                        THEN COALESCE(%(cancellation_note)s, 'cancelled by request')
                    ELSE cancellation_note
                END,
                finished_at = now(),
                updated_at = now()
            WHERE id = %(job_id)s AND status = 'running'
            RETURNING status
            """,
            {
                "status": result.status,
                "stdout": result.stdout,
                "stderr": result.stderr,
                "exit_code": result.exit_code,
                "cancellation_note": result.cancellation_note,
                "job_id": job.id,
            },
        )
        row = cursor.fetchone()
    if row is not None:
        return str(row[0])
    return _read_job_status(conn, job.id)


def claim_and_process_one(conn: psycopg.Connection[Job], settings: Settings) -> bool:
    """Claim and process a single pending job.

    Args:
        conn: Open PostgreSQL connection.
        settings: Worker runtime settings.

    Returns:
        ``True`` if a job was processed, ``False`` if the queue was empty.
    """
    job = claim_job(conn, settings.worker_id)
    if job is None:
        return False

    LOGGER.info("claimed job %s", job.id)
    result = run_job(conn, job, settings)
    final_status = finish_job(conn, job, result)
    LOGGER.info(
        "finished job %s with status %s and exit code %d",
        job.id,
        final_status,
        result.exit_code,
    )
    return True


def process_jobs(conn: psycopg.Connection[Job], settings: Settings) -> None:
    """Process jobs until the database connection fails or the process exits.

    Args:
        conn: Open PostgreSQL connection.
        settings: Worker runtime settings.
    """
    while True:
        if not claim_and_process_one(conn, settings):
            time.sleep(settings.poll_interval_seconds)


def run(settings: Settings, database: DatabaseConfig) -> None:
    """Reconnect to PostgreSQL as needed and process jobs forever.

    Args:
        settings: Worker runtime settings.
        database: Database connection settings loaded from the restricted file.
    """
    while True:
        try:
            with psycopg.connect(database.conninfo(), row_factory=class_row(Job)) as conn:
                process_jobs(conn, settings)
        except psycopg.Error:
            LOGGER.exception("database connection failed; retrying")
            time.sleep(settings.poll_interval_seconds)


def main() -> None:
    """Run the Lubko worker.

    Raises:
        SystemExit: If the database configuration file cannot be loaded.
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
    run(Settings.from_environment(), database)


if __name__ == "__main__":
    main()
