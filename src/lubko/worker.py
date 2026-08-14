"""Poll PostgreSQL for jobs and execute them directly in the Lubko container."""

from __future__ import annotations

import logging
import os
import shutil
import socket
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Final

import psycopg
from psycopg.rows import class_row

if TYPE_CHECKING:
    from uuid import UUID

LOGGER: Final = logging.getLogger(__name__)
DEFAULT_POLL_INTERVAL_SECONDS: Final = 1.0
DEFAULT_MAX_OUTPUT_BYTES: Final = 256 * 1024
TRUNCATION_MARKER: Final = b"\n... [output truncated] ...\n"


@dataclass(frozen=True, slots=True)
class Job:
    """A claimed shell job."""

    id: UUID
    cwd: str
    command: str


@dataclass(frozen=True, slots=True)
class Settings:
    """Runtime settings loaded from environment variables."""

    worker_id: str
    poll_interval_seconds: float
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


def resolve_shell() -> str | None:
    """Locate the shell executable used to run jobs.

    Returns:
        Absolute path to the shell, or ``None`` if it is not installed.
    """
    return shutil.which("bash")


def execute_job(job: Job, settings: Settings) -> tuple[int, str, str]:
    """Execute one job directly in its requested working directory.

    Args:
        job: Claimed job to execute.
        settings: Worker runtime settings.

    Returns:
        Exit code, standard output, and standard error.
    """
    shell = resolve_shell()
    if shell is None:
        return 127, "", "unable to execute job: shell executable not found"

    if not Path(job.cwd).is_dir():
        return (
            127,
            "",
            f"unable to enter working directory {job.cwd!r}: directory does not exist",
        )

    try:
        completed = subprocess.run(
            [shell, "-lc", job.command],
            cwd=job.cwd,
            check=False,
            capture_output=True,
        )
    except PermissionError as exc:
        return 127, "", f"unable to enter working directory {job.cwd!r}: {exc}"
    except OSError as exc:
        return 127, "", f"unable to execute job: {exc}"

    return (
        completed.returncode,
        truncate_output(completed.stdout, settings.max_output_bytes),
        truncate_output(completed.stderr, settings.max_output_bytes),
    )


def finish_job(
    conn: psycopg.Connection[Job],
    job: Job,
    exit_code: int,
    stdout: str,
    stderr: str,
) -> None:
    """Persist a completed job result.

    Args:
        conn: Open PostgreSQL connection.
        job: Job being completed.
        exit_code: Process exit code.
        stdout: Captured standard output.
        stderr: Captured standard error.
    """
    status = "succeeded" if exit_code == 0 else "failed"
    with conn.transaction(), conn.cursor() as cursor:
        cursor.execute(
            """
            UPDATE lubko.jobs
            SET status = %s,
                stdout = %s,
                stderr = %s,
                exit_code = %s,
                finished_at = now(),
                updated_at = now()
            WHERE id = %s
            """,
            (status, stdout, stderr, exit_code, job.id),
        )


def process_jobs(conn: psycopg.Connection[Job], settings: Settings) -> None:
    """Process jobs until the database connection fails or the process exits.

    Args:
        conn: Open PostgreSQL connection.
        settings: Worker runtime settings.
    """
    while True:
        job = claim_job(conn, settings.worker_id)
        if job is None:
            time.sleep(settings.poll_interval_seconds)
            continue

        LOGGER.info("claimed job %s", job.id)
        exit_code, stdout, stderr = execute_job(job, settings)
        finish_job(conn, job, exit_code, stdout, stderr)
        LOGGER.info("finished job %s with exit code %d", job.id, exit_code)


def run(settings: Settings) -> None:
    """Reconnect to PostgreSQL as needed and process jobs forever.

    Args:
        settings: Worker runtime settings.
    """
    while True:
        try:
            with psycopg.connect("", row_factory=class_row(Job)) as conn:
                process_jobs(conn, settings)
        except psycopg.Error:
            LOGGER.exception("database connection failed; retrying")
            time.sleep(settings.poll_interval_seconds)


def main() -> None:
    """Run the Lubko worker."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    run(Settings.from_environment())


if __name__ == "__main__":
    main()
