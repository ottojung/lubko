"""Poll PostgreSQL for jobs, execute them concurrently, and supervise them safely.

The transport table ``lubko.jobs`` keeps exactly two columns forever: ``id``
(unique random) and ``payload`` (one string containing a JSON object). All
job/request/result/state/cancellation/process-identity/lease/output data lives
inside ``payload`` using the versioned binding in :mod:`lubko.protocol` (see
``docs/protocol.md``). The worker refuses to start against a table that
violates the two-column invariant.

The daemon is a single nonblocking supervisor. It holds one PostgreSQL
connection and an in-memory registry of active jobs; each job runs as its own
OS process/session/process group, executed directly as the submitted argv
(never through a shell), and the Python daemon never synchronously waits for
any one child and never allocates a thread or a connection per job. The
supervisor loop repeatedly does small non-blocking pieces of work: service
running jobs (observe exits, escalate cancellations, publish changed output
tails/chunks, finalize completed jobs), refresh leases and run recovery
housekeeping, collect transport garbage, then claim and start more pending
jobs. There is no application-level concurrency limit; the ``active`` registry
is unbounded and only the number of claims performed in a single supervisor
turn is bounded for fairness.

Running jobs carry a lease: ``state.lease_expires_at`` is set at claim time and
refreshed by a bulk heartbeat while the jobs run. When a lease truly expires
any worker running a recovery pass atomically marks the abandoned job
``failed`` with a clear diagnostic rather than re-executing it. Recovery is
atomic across many workers and never steals a genuinely live job, whose lease
is continuously refreshed.

Transport garbage collection removes terminal command rows and their owned
output chunks after a configurable safe retention window.  Abandoned running
rows first go through lease recovery; pending and running rows are never
collected.  Root-first deletion serializes with concurrent output publication,
and bounded batches ensure the pass never monopolizes the connection.

During a database outage the supervisor stops claiming new jobs but keeps the
in-memory registry, keeps reaping/observing child processes locally, retries
the connection, and terminates any owned process group before its lease can
become unsafe. There is never a live job process that Lubko has knowingly
allowed to become unowned according to the database lease protocol.

Output capture is bounded and versioned. While a job runs, the root row's
``output`` section carries a rolling live tail of at most
``OUTPUT_TAIL_MAX_BYTES`` raw bytes per stream; older output is archived into
immutable ``output_chunk`` rows whose insertion and the root ``previous``
pointer update happen in one transaction. Publication first retains the root
``command`` row with a row-level lock, so a root deleted concurrently leaves
no new chunk rows. Archiving never shortens the live tail. See
:mod:`lubko.protocol` and ``docs/protocol.md``.

The local on-disk stdout/stderr capture files are themselves bounded
independently of how much output the child produces. Each job's standard
output and standard error are captured through dedicated nonblocking pipes that
the supervisor drains in its single nonblocking loop into a bounded on-disk
spool file. The worker owns the flow-control boundary *before* any disk
allocation: a producer faster than the drainer/trimmer fills the kernel pipe
buffer and then blocks on ``write()``, so it is backpressured and the physical
spool can never grow past ``LUBKO_OUTPUT_SPOOL_MAX_BYTES`` (plus the pipe
buffer) regardless of how much the child wants to write. The worker never uses
``RLIMIT_FSIZE``, so a job may still write arbitrarily large files of its own.
Logical byte offsets, immutable chunks and the rolling tail are preserved:
after every successful publication the durably archived prefix (everything
before the live tail window) is discarded from the head of each capture file
while logical offsets and the tail stay the same, so a job's steady-state
spool stays near ``OUTPUT_TAIL_MAX_BYTES`` regardless of total output volume.
A spool stat/read failure fails only the offending job (never the whole
worker) rather than letting one job poison supervision.
"""

from __future__ import annotations

import argparse
import fcntl
import json
import logging
import os
import select
import signal
import socket
import subprocess
import sys
import tempfile
import time
from contextlib import suppress
from dataclasses import dataclass, field
from enum import Enum, auto
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final, override
from uuid import uuid4

import psycopg
from psycopg.rows import tuple_row

from lubko._start_gate import GATE_RELEASE_BYTE
from lubko.config import load_database_config
from lubko.health import (
    WorkerHealth,
    configure_worker_logging,
    install_worker_exception_hooks,
    interpret_worker_health,
    proc_start_ticks,
    read_worker_health,
    worker_under_lifecycle,
    write_worker_health,
)
from lubko.protocol import (
    OUTPUT_CHUNK_MAX_BYTES,
    OUTPUT_TAIL_MAX_BYTES,
    TWO_COLUMN_INVARIANT,
    ProtocolError,
    build_output_chunk_payload,
    build_output_window_payload,
    parse_payload,
)
from lubko.state import state_root

if TYPE_CHECKING:
    from collections.abc import Collection
    from uuid import UUID

    from lubko.config import DatabaseConfig

JobsConnection = psycopg.Connection[tuple[Any, ...]]


def _is_connectivity_error_check(exc: psycopg.Error, conn: JobsConnection | None) -> bool:
    """Classify a database error as a connectivity issue vs. a per-job deterministic failure.

    Connectivity errors require entering outage handling and reconnecting.
    Per-job deterministic errors (for example data representation failures)
    are logged and the offending job is quarantined, but the connection
    remains usable and other jobs are unaffected.

    Classification rules (applied in order):

    * SQLSTATE class ``08`` (connection exception) is always connectivity.
    * An ``OperationalError`` on a broken or closed connection is always
      connectivity, regardless of whether a SQLSTATE is populated (real
      server shutdowns/failovers can surface as OperationalError with a
      non-08 SQLSTATE while the connection is already unusable).
    * Everything else (including server-side ``DataError``,
      ``ProgrammingError``, ``IntegrityError``) is a per-job deterministic
      failure.

    Args:
        exc: The caught psycopg exception.
        conn: The current database connection, or ``None``.

    Returns:
        ``True`` when the error indicates a lost/unusable connection.
    """
    sqlstate = exc.sqlstate
    if sqlstate is not None and sqlstate.startswith("08"):
        return True
    return (
        isinstance(exc, psycopg.OperationalError)
        and conn is not None
        and (conn.broken or conn.closed)
    )


LOGGER: Final = logging.getLogger(__name__)
DEFAULT_POLL_INTERVAL_SECONDS: Final = 1.0
DEFAULT_PROCESS_POLL_INTERVAL_SECONDS: Final = 0.1
DEFAULT_CANCEL_GRACE_SECONDS: Final = 5.0
#: Bounded finalization overhead the outer lifecycle authority must let the
#: worker keep before it may treat a still-alive worker as wedged and issue an
#: emergency SIGKILL. The outer wait deadline is ``max(stop_grace,
#: cancel_grace + this)`` so the two timers can never race.
DRAIN_OVERHEAD_SLACK_SECONDS: Final = 2.0
DEFAULT_LEASE_DURATION_SECONDS: Final = 30.0
DEFAULT_LEASE_REFRESH_INTERVAL_SECONDS: Final = 5.0
DEFAULT_LEASE_RECOVERY_INTERVAL_SECONDS: Final = 10.0
DEFAULT_OUTPUT_PUBLICATION_INTERVAL_SECONDS: Final = 1.0
DEFAULT_HEALTH_PUBLISH_INTERVAL_SECONDS: Final = 1.0
DEFAULT_CLAIM_BATCH_LIMIT: Final = 8
DEFAULT_LEASE_SAFETY_MARGIN_SECONDS: Final = 5.0
DEFAULT_DB_OPERATION_TIMEOUT_SECONDS: Final = 15.0
DEFAULT_GC_RETENTION_SECONDS: Final = 3600.0
DEFAULT_GC_INTERVAL_SECONDS: Final = 60.0
DEFAULT_GC_BATCH_LIMIT: Final = 100
# Configurable safe bound on the per-job local stdout/stderr disk spool.  The
# on-disk capture files are trimmed to the rolling live tail after every
# publication, so a job's steady-state spool stays near OUTPUT_TAIL_MAX_BYTES
# regardless of total child output volume.  When a job's spool nevertheless
# exceeds this bound (a database outage that prevents trimming, or a producer
# faster than the bound) only that job is deterministically failed rather than
# letting the spool grow without limit.
DEFAULT_OUTPUT_SPOOL_MAX_BYTES: Final = 4 * 1024 * 1024
LEASE_RECOVERY_LIMIT: Final = 100
CANCEL_DISCOVERY_LIMIT: Final = 100
SESSION_ESTABLISH_TIMEOUT_SECONDS: Final = 1.0
STAT_MIN_FIELDS: Final = 3
STAT_PGRP_FIELD_INDEX: Final = 2
#: Maximum bytes drained from a capture pipe into a spool file per syscall.  The
#: physical spool is bounded by ``output_spool_max_bytes``; this chunk is only
#: the unit of a single read, never the bound.
DRAIN_CHUNK: Final = 65536
TRUNCATION_MARKER: Final = b"\n... [output truncated] ...\n"
PROTOCOL_ERROR_EXIT_CODE: Final = 2
EXECUTION_ERROR_EXIT_CODE: Final = 127
JOBS_SCHEMA: Final = "lubko"
JOBS_TABLE: Final = "jobs"
JOBS_COLUMN_TYPES: Final = (("id", "uuid"), ("payload", "text"))
TYPE_AWARE_CONSTRAINT_NAME: Final = "jobs_payload_type_shape"
CHUNK_OWNER_INDEX_NAME: Final = "jobs_chunk_owner_idx"
CHUNK_ORDER_INDEX_NAME: Final = "jobs_chunk_order_idx"
UTC_ISO_TEXT_SQL: Final = "to_char(now() at time zone 'utc', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"')"
UTC_ISO_SQL: Final = f"to_jsonb({UTC_ISO_TEXT_SQL})"
LEASE_EXPIRES_AT_SQL: Final = (
    "to_jsonb(to_char("
    "now() at time zone 'utc' + make_interval(secs => %(lease_duration_seconds)s), "
    '\'YYYY-MM-DD"T"HH24:MI:SS.US"Z"\'))'
)

OUTPUT_STREAM_STDOUT: Final = "stdout"
OUTPUT_STREAM_STDERR: Final = "stderr"
OUTPUT_STREAMS: Final = (OUTPUT_STREAM_STDOUT, OUTPUT_STREAM_STDERR)
ARCHIVE_MARGIN_CHARS: Final = 2000

STOP_REASON_CANCEL: Final = "cancel"
STOP_REASON_SHUTDOWN: Final = "shutdown"
STOP_REASON_LEASE: Final = "lease"
STOP_REASON_ROW_LOST: Final = "row_lost"
STOP_REASON_PERSIST: Final = "persist"
STOP_REASON_QUARANTINE: Final = "quarantine"
STOP_REASON_SPOOL: Final = "spool"
QUARANTINE_MAX_RETRIES: Final = 5
QUARANTINE_RETRY_BASE_SECONDS: Final = 0.5
JOB_ID_ENV: Final = "LUBKO_JOB_ID"


class SpoolCaptureError(Exception):
    """A capture spool file could not be stat/read, so the job must fail closed.

    Raised when an active job's on-disk capture spool is unavailable (stat
    failure, read failure, or disappearance) during draining, planning, or final
    publication. The calling supervisor must fail the exact job closed as a
    capture failure rather than assuming a zero-length stream or silently
    omitting the affected stream from publication.

    Args:
        job_id: Identifier of the job whose spool is unavailable.
        stream: Name of the affected capture stream.
        path: Path of the unavailable capture spool file.
    """

    def __init__(self, job_id: object, stream: str, path: object) -> None:
        """Store the offending job, stream, and spool path.

        Args:
            job_id: Identifier of the job whose spool is unavailable.
            stream: Name of the affected capture stream.
            path: Path of the unavailable capture spool file.
        """
        super().__init__(job_id, stream, path)
        self.job_id = job_id
        self.stream = stream
        self.path = path

    @override
    def __str__(self) -> str:
        """Return a human-readable description of the unavailable spool."""
        return (
            f"capture spool for job {self.job_id} stream {self.stream} ({self.path}) is unreadable"
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
    """A claimed job.

    A job carries a required non-empty argv ``process`` tuple that the worker
    executes directly, never through a shell.
    """

    id: UUID
    cwd: str
    process: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ClaimedJob:
    """A claimed job together with its raw JSON payload text."""

    id: UUID
    payload: str


@dataclass(frozen=True, slots=True)
class JobResult:
    """Outcome of executing one job."""

    status: str
    exit_code: int
    stdout: str
    stderr: str
    cancellation_note: str | None


@dataclass(slots=True)
class OutputStream:
    """Per-stream capture and publication state for one active job.

    ``spool_start`` is the logical byte offset of the first byte currently held
    in the on-disk capture file.  The worker drains the job's capture pipe into
    the file and trims durably published prefixes from the head after every
    publication, advancing ``spool_start`` so logical offsets
    (``tail_start``/``tail_end``/chunk ``start``/``end``) stay stable even
    though the physical file only ever retains the rolling live tail plus the
    not-yet-archived gap.

    ``fd`` is the read end of the capture pipe (``None`` once it has reached
    end-of-file or been closed).  ``eof`` is set when the child's write end has
    closed and all buffered bytes have been drained, after which no more output
    can arrive for this stream.
    """

    path: Path
    fd: int | None = None
    eof: bool = False
    pending: bytearray = field(default_factory=bytearray)
    spool_start: int = 0
    published_size: int = 0
    published_at: float = 0.0
    archived_upto: int = 0
    last_chunk: UUID | None = None
    sequence: int = 0
    tail_text: str = ""
    tail_start: int = 0
    tail_end: int = 0


@dataclass(slots=True)
class ActiveJob:
    """One running job tracked by the supervisor registry.

    The registry is deliberately unbounded: there is no application-level
    maximum number of concurrently active jobs.
    """

    id: UUID
    cwd: str
    process: tuple[str, ...]
    proc: subprocess.Popen[bytes]
    pid: int
    pgid: int
    started_mono: float
    claimed_at: float
    stdout: OutputStream = field(init=False)
    stderr: OutputStream = field(init=False)
    completed: bool = False
    returncode: int | None = None
    cancel_requested: bool = False
    term_sent: bool = False
    kill_sent: bool = False
    stop_started: float | None = None
    stop_reason: str | None = None
    cancellation_note: str | None = None
    lease_evicted: bool = False
    row_lost: bool = False
    spool_evicted: bool = False
    finalized: bool = False
    quarantined: bool = False
    quarantine_pending: bool = False
    quarantine_retries: int = 0
    quarantine_next_retry_at: float = 0.0
    # Conservative monotonic origin of the last committed lease event (the claim
    # grant or a bulk refresh), captured before that database operation, never
    # at commit time and never at spawn. Drives lease-safety eviction so a
    # process can never outlive its database lease.
    last_heartbeat_at: float = 0.0


@dataclass(slots=True)
class _RetryTerminalization:
    """Backoff bookkeeping for a job awaiting terminalization retry.

    A claimed job whose immediate finalization write failed is kept locally
    owned for retry so it stays represented.  Its lease is intentionally NOT
    refreshed (it is not heartbeated merely because an unrelated job happens to
    be active); it is left free to expire and be safely recovered as failed
    rather than being silently dropped as an orphan.
    """

    retries: int = 0
    next_retry_at: float = 0.0


class SchemaInvariantError(RuntimeError):
    """Raised when ``lubko.jobs`` violates the two-column transport invariant."""


@dataclass(frozen=True, slots=True)
class Settings:
    """Runtime settings loaded from environment variables.

    Every running job carries a lease (``state.lease_expires_at``) that the
    owning worker refreshes by heartbeat; when the lease expires, a recovery
    pass marks the abandoned job failed. The lease duration must comfortably
    exceed the refresh interval so a healthy long-running job is never stolen.
    ``claim_batch_limit`` is a fairness bound on how much claiming work one
    supervisor turn performs, never a cap on concurrent jobs.  Transport
    garbage collection removes terminal rows and owned chunks after
    ``gc_retention_seconds``, running in bounded batches every
    ``gc_interval_seconds``.
    """

    worker_id: str
    poll_interval_seconds: float
    process_poll_interval_seconds: float
    cancel_grace_seconds: float
    worker_incarnation: str = field(
        default_factory=lambda: os.environ.get("LUBKO_LIFECYCLE_TOKEN", uuid4().hex)
    )
    lease_duration_seconds: float = DEFAULT_LEASE_DURATION_SECONDS
    lease_refresh_interval_seconds: float = DEFAULT_LEASE_REFRESH_INTERVAL_SECONDS
    lease_recovery_interval_seconds: float = DEFAULT_LEASE_RECOVERY_INTERVAL_SECONDS
    output_publication_interval_seconds: float = DEFAULT_OUTPUT_PUBLICATION_INTERVAL_SECONDS
    health_publish_interval_seconds: float = DEFAULT_HEALTH_PUBLISH_INTERVAL_SECONDS
    claim_batch_limit: int = DEFAULT_CLAIM_BATCH_LIMIT
    lease_safety_margin_seconds: float = DEFAULT_LEASE_SAFETY_MARGIN_SECONDS
    db_operation_timeout_seconds: float = DEFAULT_DB_OPERATION_TIMEOUT_SECONDS
    gc_retention_seconds: float = DEFAULT_GC_RETENTION_SECONDS
    gc_interval_seconds: float = DEFAULT_GC_INTERVAL_SECONDS
    gc_batch_limit: int = DEFAULT_GC_BATCH_LIMIT
    output_spool_max_bytes: int = DEFAULT_OUTPUT_SPOOL_MAX_BYTES

    def __post_init__(self) -> None:
        """Validate lease timing so a live worker's lease never expires idle."""
        self._validate_lease_timing()
        self._validate_output_and_gc()
        self._validate_spool()

    def _validate_lease_timing(self) -> None:
        """Validate lease-related settings are consistent.

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
        if self.lease_safety_margin_seconds < 0 or (
            self.lease_safety_margin_seconds >= self.lease_duration_seconds
        ):
            msg = (
                "LUBKO_LEASE_SAFETY_MARGIN_SECONDS must be non-negative and smaller "
                "than LUBKO_LEASE_DURATION_SECONDS"
            )
            raise ValueError(msg)

    def _validate_output_and_gc(self) -> None:
        """Validate output publication and GC settings.

        Raises:
            ValueError: If any output or GC value is unusable.
        """
        if self.output_publication_interval_seconds <= 0:
            msg = "LUBKO_OUTPUT_PUBLICATION_INTERVAL_SECONDS must be positive"
            raise ValueError(msg)
        if self.health_publish_interval_seconds <= 0:
            msg = "LUBKO_HEALTH_PUBLISH_INTERVAL_SECONDS must be positive"
            raise ValueError(msg)
        if self.claim_batch_limit <= 0:
            msg = "LUBKO_CLAIM_BATCH_LIMIT must be positive"
            raise ValueError(msg)
        if self.db_operation_timeout_seconds <= 0:
            msg = "LUBKO_DB_OPERATION_TIMEOUT_SECONDS must be positive"
            raise ValueError(msg)
        if self.gc_retention_seconds < 0:
            msg = "LUBKO_GC_RETENTION_SECONDS must be non-negative"
            raise ValueError(msg)
        if self.gc_interval_seconds <= 0:
            msg = "LUBKO_GC_INTERVAL_SECONDS must be positive"
            raise ValueError(msg)
        if self.gc_batch_limit <= 0:
            msg = "LUBKO_GC_BATCH_LIMIT must be positive"
            raise ValueError(msg)

    def _validate_spool(self) -> None:
        """Validate the per-job local spool bound.

        Raises:
            ValueError: If the bound is unusable.
        """
        if self.output_spool_max_bytes <= 0:
            msg = "LUBKO_OUTPUT_SPOOL_MAX_BYTES must be positive"
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
            output_publication_interval_seconds=float(
                os.getenv(
                    "LUBKO_OUTPUT_PUBLICATION_INTERVAL_SECONDS",
                    str(DEFAULT_OUTPUT_PUBLICATION_INTERVAL_SECONDS),
                )
            ),
            health_publish_interval_seconds=float(
                os.getenv(
                    "LUBKO_HEALTH_PUBLISH_INTERVAL_SECONDS",
                    str(DEFAULT_HEALTH_PUBLISH_INTERVAL_SECONDS),
                )
            ),
            claim_batch_limit=int(
                os.getenv("LUBKO_CLAIM_BATCH_LIMIT", str(DEFAULT_CLAIM_BATCH_LIMIT))
            ),
            lease_safety_margin_seconds=float(
                os.getenv(
                    "LUBKO_LEASE_SAFETY_MARGIN_SECONDS",
                    str(DEFAULT_LEASE_SAFETY_MARGIN_SECONDS),
                )
            ),
            db_operation_timeout_seconds=float(
                os.getenv(
                    "LUBKO_DB_OPERATION_TIMEOUT_SECONDS",
                    str(DEFAULT_DB_OPERATION_TIMEOUT_SECONDS),
                )
            ),
            gc_retention_seconds=float(
                os.getenv(
                    "LUBKO_GC_RETENTION_SECONDS",
                    str(DEFAULT_GC_RETENTION_SECONDS),
                )
            ),
            gc_interval_seconds=float(
                os.getenv(
                    "LUBKO_GC_INTERVAL_SECONDS",
                    str(DEFAULT_GC_INTERVAL_SECONDS),
                )
            ),
            gc_batch_limit=int(os.getenv("LUBKO_GC_BATCH_LIMIT", str(DEFAULT_GC_BATCH_LIMIT))),
            output_spool_max_bytes=int(
                os.getenv(
                    "LUBKO_OUTPUT_SPOOL_MAX_BYTES",
                    str(DEFAULT_OUTPUT_SPOOL_MAX_BYTES),
                )
            ),
        )


# ---------------------------------------------------------------------------
# Output capture helpers
# ---------------------------------------------------------------------------


def truncate_output(data: bytes, limit: int) -> str:
    """Decode output while retaining at most the newest ``limit`` bytes.

    The canonical :func:`pg_safe_decode` conversion is used so the result is
    always safe for PostgreSQL ``text`` / ``jsonb``.

    Args:
        data: Raw process output.
        limit: Maximum number of bytes to retain.

    Returns:
        UTF-8 text, replacing invalid byte sequences and NUL.

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
    return pg_safe_decode(payload)


def read_output(path: Path) -> bytes:
    """Read all bytes captured so far into an output file.

    Args:
        path: Capture file for the stream.

    Returns:
        The captured bytes.
    """
    return path.read_bytes()


def stream_size(path: Path) -> int:
    """Return the current byte size of a capture file.

    Args:
        path: Capture file for the stream.

    Returns:
        The file size in bytes.
    """
    return path.stat().st_size


def read_range(path: Path, start: int, end: int) -> bytes:
    """Read the bytes in ``[start, end)`` from a capture file.

    Args:
        path: Capture file for the stream.
        start: Inclusive byte offset.
        end: Exclusive byte offset.

    Returns:
        The captured bytes in the window.
    """
    with path.open("rb") as fh:
        fh.seek(start)
        return fh.read(end - start)


def drain_capture_stream(
    stream: OutputStream, bound: int, aggregate_used: int | None = None
) -> str:
    """Drain a job's capture pipe into its bounded spool file.

    The supervisor's single nonblocking loop calls this for each stream that
    still has a live read end. Output is appended to the on-disk spool file
    only while the per-job bound has room: when the spool already reaches the
    bound the pipe is deliberately *not* read, so the producer blocks on
    ``write()`` and is backpressured before any further disk allocation. This
    is the flow-control boundary that makes the physical spool provably bounded,
    and the bound is never disabled — not after the producer exits and not
    during shutdown.

    Bytes already read from the pipe are never discarded. They are held in the
    stream's bounded in-memory ``pending`` buffer and retried on the next drain;
    the buffer is itself bounded by the spool bound, so backpressure never lets
    it grow without limit and the worker never loses output it has already
    taken ownership of. When a spool write fails the read bytes stay in
    ``pending`` and ``"error"`` is returned so the owning job can be failed
    closed with every already-read byte still represented, never silently
    dropped. A spool stat failure is reported as ``"error"`` so the owning job
    fails closed rather than assuming a zero-length stream.

    When the spool is full and a stream is still non-end-of-file (an exited
    producer that wrote more than the bound, or a terminating producer still
    flushing), the caller must durably publish and trim the captured bytes to
    free bounded room, then drain again — never read past the bound.

    Args:
        stream: The stream whose pipe to drain.
        bound: Maximum physical spool size in bytes for this job (aggregate
            across both streams). Always enforced.
        aggregate_used: Bytes already on disk for this job's both streams; when
            ``None`` the stream's own on-disk size is used (single-stream drains).

    Returns:
        ``"ok"`` when bytes were drained, ``"full"`` when the spool is at its
        bound and reading was withheld (the producer is now backpressured),
        ``"eof"`` when no more output can arrive, or ``"error"`` when an OS
        error was encountered and the owning job must be failed closed.
    """
    if stream.eof:
        return "eof"
    # Flush any bytes already read but not yet written before pulling more, so
    # a transient write failure never discards output we already own.
    if stream.pending and not _flush_pending(stream):
        return "error"
    if stream.fd is None:
        stream.eof = True
        return "eof"
    try:
        size = stream.path.stat().st_size
    except OSError:
        return "error"
    used = aggregate_used if aggregate_used is not None else size
    room = bound - used - len(stream.pending)
    if room <= 0:
        return "full"
    return _read_capture_chunk(stream, room)


def _read_capture_chunk(stream: OutputStream, room: int) -> str:
    """Read one bounded chunk from the pipe and flush it to the spool.

    Args:
        stream: The stream whose pipe to read.
        room: Bytes still available under the per-job bound.

    Returns:
        ``"ok"`` when bytes were drained, ``"eof"`` when the write end closed,
        or ``"error"`` when the read or spool write failed.
    """
    fd = stream.fd
    if fd is None:
        return "eof"
    try:
        data = os.read(fd, min(room, DRAIN_CHUNK))
    except BlockingIOError:
        return "ok"
    except OSError:
        return "error"
    if not data:
        return _finish_capture_stream(stream)
    stream.pending += data
    return "ok" if _flush_pending(stream) else "error"


def _finish_capture_stream(stream: OutputStream) -> str:
    """Close a stream's read end and mark EOF once pending is flushed.

    Args:
        stream: The stream whose pipe reached end-of-file.

    Returns:
        ``"eof"`` when the stream is fully drained, or ``"error"`` when a
        residual pending-buffer write failed.
    """
    fd = stream.fd
    if fd is not None:
        with suppress(OSError):
            os.close(fd)
    stream.fd = None
    if stream.pending and not _flush_pending(stream):
        return "error"
    stream.eof = True
    return "eof"


def _flush_pending(stream: OutputStream) -> bool:
    """Append the stream's in-memory pending bytes to its on-disk spool file.

    The buffered bytes are written through the exact byte-counted append seam
    :func:`_spill_append`, which opens the spool with ``O_APPEND`` and consumes
    only the bytes it successfully wrote: any prefix it landed is removed from
    the in-memory buffer while the unwritten suffix is retained, so a partial
    write or a mid-write ``OSError`` never loses output already taken from the
    pipe and never duplicates the bytes that did land. A genuine write failure
    (the seam wrote nothing and left the buffer intact) returns ``False`` so the
    caller can retry or fail the job closed without ever discarding output
    already read from the pipe.

    Args:
        stream: The stream whose pending buffer to flush.

    Returns:
        ``True`` when the flush made progress (the buffer is now empty or holds
        only a benignly retained suffix to retry on the next drain),
        ``False`` when the spool write genuinely failed and the buffer must be
        retained intact for a closed failure.
    """
    if not stream.pending:
        return True
    before = len(stream.pending)
    written = _spill_append(stream.path, stream.pending)
    # A zero-byte, unchanged buffer means the seam hit a hard write error
    # (for example the spool could not be opened); nothing was consumed, so the
    # caller must treat this as a failure rather than retry an empty buffer.
    return not (written == 0 and len(stream.pending) == before)


def _spill_append(path: Path, data: bytearray) -> int:
    """Append as much of ``data`` to ``path`` as the OS accepts.

    The spool is opened with ``O_APPEND`` (and deliberately *without*
    ``O_CREAT``: the spool is pre-created at spawn time, so an open failure
    here means the active spool disappeared and the caller must fail the job
    closed rather than silently recreating a fresh empty one) so the worker,
    the file's sole writer,
    can never interleave or lose bytes, and the write advances only as far as
    the kernel accepts. The successfully written prefix is removed from ``data``
    in place; the unwritten suffix is left in ``data`` for the caller to retry,
    so a short write or an ``OSError`` mid-flush loses and duplicates nothing: a
    benign partial write retains exactly the not-yet-written suffix, and a hard
    error leaves ``data`` holding only the bytes written before the failure (or
    none, on an open failure) so the caller can detect the partial / failed
    flush and retain precisely the bytes it still owes the spool.

    Args:
        path: The spool file to append to.
        data: In-memory bytes to append; the written prefix is consumed in place.

    Returns:
        The number of bytes consumed from ``data``.
    """
    try:
        fd = os.open(path, os.O_WRONLY | os.O_APPEND, 0o600)
    except OSError:
        return 0
    try:
        total = 0
        while total < len(data):
            try:
                written = os.write(fd, bytes(data[total:]))
            except OSError:
                break
            if written <= 0:
                break
            total += written
    finally:
        with suppress(OSError):
            os.close(fd)
    del data[:total]
    return total


def _poll_readable(fds: list[int]) -> set[int]:
    """Return the subset of ``fds`` that are currently readable.

    Uses ``select.poll``, which supports arbitrary file-descriptor numbers
    (including valid descriptors well beyond the legacy ``select``
    ``FD_SETSIZE`` limit) so a high-numbered capture fd is never misclassified
    as bad. A closed or otherwise invalid descriptor surfaces as ``POLLNVAL``
    rather than raising, which the caller treats as a bad-fd probe; a descriptor
    that ``poll`` cannot even register still raises and is handled the same way.

    Args:
        fds: Candidate read descriptors.

    Returns:
        The set of descriptors reporting readable readiness.

    Raises:
        OSError: If ``poll`` cannot register or poll one of the descriptors.
    """
    readable: set[int] = set()
    if not fds:
        return readable
    poller = select.poll()
    for fd in fds:
        poller.register(fd, select.POLLIN | select.POLLPRI)
    for fd, flags in poller.poll(0):
        if flags & select.POLLNVAL:
            # A bad fd among many must not poison the whole worker: surface it
            # so the caller probes each candidate individually and isolates the
            # exact offending job.
            msg = f"bad capture fd {fd} reported by poll"
            raise OSError(msg)
        readable.add(fd)
    return readable


def _is_bad_fd(fd: int) -> bool:
    """Return whether ``fd`` is not a pollable capture descriptor.

    ``select.poll`` reports an invalid descriptor as ``POLLNVAL`` rather than
    raising, so a valid high-numbered fd (which ``select.select`` would reject
    with ``ValueError``) is correctly recognised as healthy, while a closed or
    reused descriptor is reported as bad.

    Args:
        fd: The descriptor to probe.

    Returns:
        ``True`` when the descriptor is bad and must be failed closed.
    """
    poller = select.poll()
    poller.register(fd, select.POLLIN)
    return any(flags & select.POLLNVAL for _fd, flags in poller.poll(0))


def pg_safe_decode(data: bytes) -> str:
    r"""Decode arbitrary bytes into a string that is safe for PostgreSQL text/jsonb.

    Invalid UTF-8 byte sequences are replaced with U+FFFD (the standard
    replacement character), and U+0000 (NUL) is replaced with U+FFFD.
    PostgreSQL's ``text`` / ``jsonb`` representation rejects the JSON escape
    sequence for U+0000 (``\\u0000``), so NUL must never survive into protocol
    text.

    The replacement is purely textual; raw byte offsets used for chunking and
    live-tail windowing are computed before decoding and are never affected.

    Args:
        data: Arbitrary captured output bytes.

    Returns:
        UTF-8 text safe for PostgreSQL ``text`` / ``jsonb``.
    """
    return data.decode("utf-8", errors="replace").replace("\x00", "\ufffd")


def decode_range(path: Path, start: int, end: int) -> str:
    """Decode the bytes in ``[start, end)`` as PostgreSQL-safe UTF-8 text.

    The canonical conversion uses :func:`pg_safe_decode`: invalid UTF-8 byte
    sequences become U+FFFD and NUL (U+0000) is replaced with U+FFFD so the
    result is always safe for PostgreSQL ``text`` / ``jsonb``.

    Args:
        path: Capture file for the stream.
        start: Inclusive byte offset.
        end: Exclusive byte offset.

    Returns:
        The decoded text.
    """
    return pg_safe_decode(read_range(path, start, end))


def output_window_text(path: Path, max_chars: int, *, base: int = 0) -> tuple[str, int, int]:
    """Return the newest at most ``max_chars`` bytes as decoded text.

    Byte offsets are used for the window bounds and decoding is UTF-8 with
    replacement, so offsets are deterministic even when a window starts inside
    a multi-byte sequence. ``base`` is the logical offset of the first physical
    byte in the file (``OutputStream.spool_start``); the returned ``start`` and
    ``end`` are logical offsets and the physical read is translated by ``base``.

    Args:
        path: Capture file for the stream.
        max_chars: Maximum number of bytes to retain in the window.
        base: Logical offset of the file's first physical byte.

    Returns:
        A ``(text, start, end)`` tuple where ``end`` is the logical file size.
    """
    size = stream_size(path) + base
    window = min(size, max_chars)
    start = size - window
    return decode_range(path, start - base, size - base), start, size


def archive_target(size: int) -> int:
    """Return the byte offset up to which output may be archived.

    Output strictly before the live tail is historical; archiving may extend
    an ``ARCHIVE_MARGIN_CHARS`` overlap into the live tail, which is
    intentional and represented unambiguously by byte offsets. Archiving never
    shortens the live tail, which is always recomputed from the capture file.

    Args:
        size: Current byte size of the captured stream.

    Returns:
        The archive target offset.
    """
    if size <= OUTPUT_TAIL_MAX_BYTES:
        return 0
    return size - OUTPUT_TAIL_MAX_BYTES + ARCHIVE_MARGIN_CHARS


def publish_output(
    conn: JobsConnection,
    job: ActiveJob,
    stream_names: list[str],
    now: float,
    *,
    force: bool = False,
) -> bool:
    """Publish changed live tails and archive historical output for one job.

    Immutable ``output_chunk`` rows are inserted and the root row's live-window
    metadata (including the ``previous`` pointer) is updated in one PostgreSQL
    transaction, so a crash can never leave the root pointing at nonexistent
    history. The transaction first retains the root ``command`` row with a
    row-level lock: when a concurrent root deletion has already committed, no
    chunk row is inserted at all and publication returns ``False``, so
    publication itself never leaves an explicitly owned orphan chunk. In-memory
    publication state is only advanced after the transaction commits, so a
    failed transaction never leaves the registry pointing at chunks that were
    not inserted. The live tail itself is always recomputed as the newest
    ``OUTPUT_TAIL_MAX_BYTES`` bytes of the capture file, so archiving is
    observationally invisible to a normal root-row ``SELECT``.

    Args:
        conn: Open PostgreSQL connection.
        job: The active job whose output to publish.
        stream_names: Which streams to publish.
        now: Monotonic time of this publication pass.
        force: Whether to publish regardless of the throttle interval.

    Returns:
        ``True`` when the root ``command`` row was retained (or nothing needed
        publishing), ``False`` when the root row no longer exists and the
        planned publication was skipped.
    """
    plans = _plan_streams(job, stream_names, force=force)
    if not plans:
        return True
    windows = _full_windows(job, plans)
    output = {"stdout": windows["stdout"], "stderr": windows["stderr"]}
    with conn.transaction(), conn.cursor() as cursor:
        cursor.execute(
            "SELECT id\n"
            "FROM lubko.jobs\n"
            "WHERE id = %(job_id)s\n"
            "    AND (payload::jsonb)->>'type' = 'command'\n"
            "    AND ((payload::jsonb)->'state'->>'gc') IS DISTINCT FROM 'true'\n"
            "FOR UPDATE\n",
            {"job_id": job.id},
        )
        if cursor.fetchone() is None:
            return False
        for plan in plans.values():
            for chunk_id, chunk_payload in plan.chunks:
                cursor.execute(
                    "INSERT INTO lubko.jobs (id, payload) VALUES (%s, %s)",
                    (chunk_id, chunk_payload),
                )
        cursor.execute(
            _output_update_sql(),
            _output_update_params(job.id, output),
        )
    for name, plan in plans.items():
        _apply_plan(getattr(job, name), plan, now)
    _trim_published(job, plans)
    return True


def _rewrite_head(path: Path, drop: int) -> None:
    """Drop the first ``drop`` bytes from a capture file in place.

    The remainder is staged into a same-directory temporary file so a write
    failure (or any error mid-stage) leaves the original spool byte-for-byte
    intact, and the staged content is swapped onto the live path with an atomic
    ``os.replace``. Readers (including concurrent publications) therefore never
    observe a partially rewritten head, and a failed replacement leaves both the
    bytes and the caller's logical offset unchanged so the next publication
    recomputes the same drop and retries exactly that stream. The file only ever
    holds a bounded suffix of the stream once the prefix has been archived into
    immutable chunks, so rewriting the small remainder is cheap and the new
    content is identical to the bytes that were not dropped.

    Args:
        path: Capture file for the stream.
        drop: Number of leading bytes to remove (must be non-negative).
    """
    if drop <= 0:
        return
    with path.open("rb") as fh:
        fh.seek(drop)
        remainder = fh.read()
    # Stage in the same directory so the final swap is a rename on one
    # filesystem (atomic and never observable half-done). A failure before the
    # replace leaves the original fully intact and the temp file is cleaned up.
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".rewrite.tmp")
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(remainder)
            fh.flush()
            os.fsync(fh.fileno())
        Path(tmp).replace(path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise


def _trim_published(job: ActiveJob, plans: dict[str, _StreamPlan]) -> None:
    """Discard durably published prefixes from a job's on-disk spool files.

    Each published stream's live tail window starts at ``plan.tail_start``; every
    byte strictly before that offset has been archived into immutable
    ``output_chunk`` rows (and the live tail itself is recomputed from the file
    on the next publication), so it is safe to remove from the local spool.
    ``spool_start`` advances by exactly the number of bytes removed so logical
    offsets stay consistent.

    The head discard is always safe without touching the job's process group:
    the child writes to a capture *pipe*, not to the spool file, so the worker
    is the file's sole writer.  There is no concurrent ``O_APPEND`` producer to
    race the rewrite, and the supervisor never issues ``SIGSTOP``/``SIGCONT``
    for capture compaction, avoiding unsafe mixed-group stop/resume semantics.

    Args:
        job: The active job whose published streams to trim.
        plans: The committed publication plans keyed by stream name.
    """
    drops = {name: plan.tail_start - getattr(job, name).spool_start for name, plan in plans.items()}
    if all(drop <= 0 for drop in drops.values()):
        return
    for name, drop in drops.items():
        if drop <= 0:
            continue
        stream = getattr(job, name)
        # Rewrite the head first; only advance the in-memory logical offset
        # after the on-disk rewrite succeeds, so the stream's (file, offset)
        # pair stays coherent even when one stream's rewrite raises: the next
        # publication recomputes the drop from the unchanged offset and retries
        # exactly that stream, leaving the offending job isolated and its state
        # consistent rather than half-trimmed.
        _rewrite_head(stream.path, drop)
        stream.spool_start = plans[name].tail_start


def _full_windows(job: ActiveJob, plans: dict[str, _StreamPlan]) -> dict[str, dict[str, Any]]:
    """Combine planned and last-published windows for both streams.

    PostgreSQL's ``jsonb_set`` does not create intermediate objects for a
    multi-level path, so the whole ``output`` object is written at once and both
    stream windows are always present.

    Args:
        job: The active job whose output to publish.
        plans: The planned stream publications.

    Returns:
        The ``{stdout, stderr}`` window mappings.
    """
    windows: dict[str, dict[str, Any]] = {}
    for name in OUTPUT_STREAMS:
        if name in plans:
            windows[name] = plans[name].window
        else:
            stream = getattr(job, name)
            windows[name] = build_output_window_payload(
                tail=stream.tail_text,
                start=stream.tail_start,
                end=stream.tail_end,
                previous=stream.last_chunk,
            )
    return windows


@dataclass(frozen=True, slots=True)
class _StreamPlan:
    """Planned publication of one stream, applied only after commit."""

    size: int
    tail_text: str
    tail_start: int
    tail_end: int
    archived_upto: int
    last_chunk: UUID | None
    sequence: int
    chunks: tuple[tuple[UUID, str], ...]
    window: dict[str, Any]


def _plan_streams(
    job: ActiveJob, stream_names: list[str], *, force: bool
) -> dict[str, _StreamPlan]:
    """Compute publication plans for changed streams without mutating state.

    Args:
        job: The active job whose output to publish.
        stream_names: Which streams to publish.
        force: Whether to publish regardless of the throttle interval.

    Returns:
        The plans keyed by stream name.

    Raises:
        SpoolCaptureError: When an active stream's capture spool cannot be
            stat/read, so the exact job can be failed closed rather than
            publishing an omitted stream.
    """
    plans: dict[str, _StreamPlan] = {}
    for name in stream_names:
        stream = getattr(job, name)
        try:
            size = stream.spool_start + stream.path.stat().st_size
            tail_text, tail_start, tail_end = output_window_text(
                stream.path, OUTPUT_TAIL_MAX_BYTES, base=stream.spool_start
            )
            chunks, archived_upto, last_chunk, sequence = _plan_chunks(
                job.id, name, stream, tail_end
            )
        except OSError as exc:
            # Never silently omit a stream: a stat/read/disappearance failure on
            # an active job's spool must surface as a capture failure that fails
            # the exact job closed, not as a partial publication of only the
            # other stream.
            raise SpoolCaptureError(job.id, name, stream.path) from exc
        if not force and size == stream.published_size:
            continue
        plans[name] = _StreamPlan(
            size=size,
            tail_text=tail_text,
            tail_start=tail_start,
            tail_end=tail_end,
            archived_upto=archived_upto,
            last_chunk=last_chunk,
            sequence=sequence,
            chunks=chunks,
            window=build_output_window_payload(
                tail=tail_text,
                start=tail_start,
                end=tail_end,
                previous=last_chunk,
            ),
        )
    return plans


def _plan_chunks(
    job_id: UUID,
    name: str,
    stream: OutputStream,
    tail_end: int,
) -> tuple[tuple[tuple[UUID, str], ...], int, UUID | None, int]:
    """Compute the immutable chunks to archive for one stream.

    Args:
        job_id: Owning root job identifier.
        name: Stream name.
        stream: The stream's current publication state.
        tail_end: Current byte size of the stream (end of the live tail).

    Returns:
        The planned ``(chunks, archived_upto, last_chunk, sequence)`` tuple.
    """
    chunks: list[tuple[UUID, str]] = []
    archived_upto = stream.archived_upto
    last_chunk = stream.last_chunk
    sequence = stream.sequence
    target = archive_target(tail_end)
    while target - archived_upto >= OUTPUT_CHUNK_MAX_BYTES:
        chunk_start = archived_upto
        chunk_end = chunk_start + OUTPUT_CHUNK_MAX_BYTES
        value = decode_range(
            stream.path,
            chunk_start - stream.spool_start,
            chunk_end - stream.spool_start,
        )
        chunk_id = uuid4()
        chunk_payload = json.dumps(
            build_output_chunk_payload(
                thread=job_id,
                stream=name,
                sequence=sequence,
                start=chunk_start,
                end=chunk_end,
                value=value,
                previous=last_chunk,
            )
        )
        chunks.append((chunk_id, chunk_payload))
        last_chunk = chunk_id
        sequence += 1
        archived_upto = chunk_end
    return tuple(chunks), archived_upto, last_chunk, sequence


def _apply_plan(stream: OutputStream, plan: _StreamPlan, now: float) -> None:
    """Advance an output stream's in-memory state after a committed publication.

    Args:
        stream: The stream to update.
        plan: The committed publication plan.
        now: Monotonic time of the publication pass.
    """
    stream.published_size = plan.size
    stream.published_at = now
    stream.tail_text = plan.tail_text
    stream.tail_start = plan.tail_start
    stream.tail_end = plan.tail_end
    stream.archived_upto = plan.archived_upto
    stream.last_chunk = plan.last_chunk
    stream.sequence = plan.sequence


def _output_update_sql() -> str:
    """Build the SQL updating a root job's whole bounded ``output`` section.

    The complete ``output`` object (both stream windows) is written with one
    single-level ``jsonb_set`` because PostgreSQL does not create intermediate
    objects for a multi-level path.

    Returns:
        An ``UPDATE`` statement bound only with parameterized values.
    """
    return (
        "UPDATE lubko.jobs\n"
        "SET payload = jsonb_set("
        "jsonb_set(payload::jsonb, '{output}', %(output)s::jsonb), "
        "'{state,updated_at}', " + UTC_ISO_SQL + ")::text\n"
        "WHERE id = %(job_id)s AND (payload::jsonb)->>'type' = 'command'\n"
    )


def _output_update_params(job_id: UUID, output: dict[str, dict[str, Any]]) -> dict[str, object]:
    """Build the parameters for :func:`_output_update_sql`.

    Args:
        job_id: The root job identifier.
        output: The full ``{stdout, stderr}`` window mapping.

    Returns:
        The bound parameters.
    """
    return {"job_id": job_id, "output": json.dumps(output)}


# ---------------------------------------------------------------------------
# Process helpers
# ---------------------------------------------------------------------------


_CLAIM_JOBS_TEMPLATE: Final = """\
WITH next AS (
    SELECT id
    FROM lubko.jobs
    WHERE (payload::jsonb)->>'type' = 'command'
        AND (payload::jsonb)->'state'->>'status' = 'pending'
    ORDER BY (payload::jsonb)->'state'->>'created_at', id
    FOR UPDATE SKIP LOCKED
    LIMIT %(limit)s
)
UPDATE lubko.jobs AS job
SET payload = __SET_CHAIN__::text
FROM next
WHERE job.id = next.id
RETURNING job.id, job.payload
"""

_RECOVER_STALE_JOBS_TEMPLATE: Final = """\
WITH stale AS (
    SELECT id
    FROM lubko.jobs
    WHERE (payload::jsonb)->>'type' = 'command'
        AND (payload::jsonb)->'state'->>'status' = 'running'
        AND (payload::jsonb)->'state'->>'lease_expires_at' IS NOT NULL
        AND ((payload::jsonb)->'state'->>'lease_expires_at') < __ISO_NOW__
    ORDER BY id
    FOR UPDATE SKIP LOCKED
    LIMIT %(limit)s
)
UPDATE lubko.jobs AS job
SET payload = __SET_CHAIN__::text
FROM stale
WHERE job.id = stale.id
RETURNING job.id, job.payload
"""


def _claim_jobs_sql(set_chain: str) -> str:
    """Compose the bounded claiming ``UPDATE`` statement.

    The SQL is assembled from internal constant literals and the prebuilt
    ``jsonb_set`` chain; the chain is slotted into the template with a string
    replacement (never by concatenating a query fragment into a ``SELECT``-bearing
    string) so no external input reaches the query and the template stays a safe
    constant. The only runtime inputs are bound parameters.

    Args:
        set_chain: The prebuilt ``jsonb_set`` chain for the claim state.

    Returns:
        The full claiming statement text.
    """
    return _CLAIM_JOBS_TEMPLATE.replace("__SET_CHAIN__", set_chain)


def _recover_stale_jobs_sql(set_chain: str) -> str:
    """Compose the bounded lease-recovery ``UPDATE`` statement.

    As with :func:`_claim_jobs_sql`, the statement is built only from the
    internal template and the prebuilt chain; the only runtime inputs are bound
    parameters.

    Args:
        set_chain: The prebuilt ``jsonb_set`` chain for the failed state.

    Returns:
        The full recovery statement text.
    """
    return _RECOVER_STALE_JOBS_TEMPLATE.replace("__SET_CHAIN__", set_chain).replace(
        "__ISO_NOW__", UTC_ISO_TEXT_SQL
    )


def _streams_at_eof(job: ActiveJob) -> bool:
    """Return whether both of a job's capture streams have reached EOF.

    Args:
        job: The active job to inspect.

    Returns:
        ``True`` when stdout and stderr are both at end-of-file.
    """
    return job.stdout.eof and job.stderr.eof


def _spool_used_bytes(job: ActiveJob) -> int | None:
    """Return a job's combined on-disk spool usage across both streams.

    Args:
        job: The active job to inspect.

    Returns:
        The summed byte sizes of both spool files, or ``None`` when either
        stat fails (the caller then cannot prove room was freed).
    """
    used = 0
    for name in OUTPUT_STREAMS:
        stream = getattr(job, name)
        try:
            used += stream.path.stat().st_size
        except OSError:
            return None
    return used


def _spool_shrank(job: ActiveJob, used_before: int | None) -> bool:
    """Return whether a durable publish+trim reduced the on-disk spool usage.

    Args:
        job: The active job whose spool usage is compared.
        used_before: Combined spool size observed before publication, or
            ``None`` when it could not be measured.

    Returns:
        ``True`` when the combined spool usage measurably decreased.
    """
    if used_before is None:
        return False
    used_after = _spool_used_bytes(job)
    return used_after is not None and used_after < used_before


def _persist_process(
    conn: JobsConnection,
    job_id: UUID,
    pid: int,
    pgid: int,
    start_ticks: int,
) -> None:
    """Persist the exact process identity of a running job.

    The identity is written into ``payload.state.process_pid``,
    ``payload.state.process_pgid`` and ``payload.state.process_start_time_ticks``,
    keeping the two-column table invariant. The start-time ticks make later
    emergency recovery PID-reuse-safe: a persisted group id that has been
    recycled by an unrelated process no longer matches the recorded command and
    is never signalled.

    Args:
        conn: Open PostgreSQL connection.
        job_id: Identifier of the running job.
        pid: Exact process ID of the spawned process.
        pgid: Exact process group ID of the spawned process.
        start_ticks: Valid positive start-time ticks of the exact process,
            obtained before the start gate was released.
    """
    set_chain = _jsonb_set_chain(
        "payload::jsonb",
        [
            ("state,process_pid", "to_jsonb(%s::int)"),
            ("state,process_pgid", "to_jsonb(%s::int)"),
            (
                "state,process_start_time_ticks",
                "to_jsonb(%s::bigint)",
            ),
            ("state,updated_at", UTC_ISO_SQL),
        ],
    )
    with conn.transaction(), conn.cursor() as cursor:
        cursor.execute(
            "UPDATE lubko.jobs\nSET payload = " + set_chain + "::text\nWHERE id = %s\n",
            (pid, pgid, start_ticks, job_id),
        )


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


def drain_sentinel_dir() -> Path:
    """Return the directory holding per-incarnation drain acknowledgement files.

    Returns:
        The drain sentinel directory under the Lubko worker state root.
    """
    return state_root() / "worker" / "drain"


def drain_sentinel_path(incarnation: str) -> Path:
    """Return the exact drain-sentinel path for an incarnation.

    Args:
        incarnation: The worker incarnation (lifecycle token).

    Returns:
        The sentinel file path.
    """
    return drain_sentinel_dir() / f"{incarnation}.drained"


def write_drain_sentinel(incarnation: str) -> None:
    """Atomically record that this worker has drained every owned group.

    The sentinel is the explicit acknowledgement the outer lifecycle authority
    observes: once present (and matching the incarnation), no command process
    group owned by this worker can still be alive, so the worker may be reaped
    or forgotten without leaking a side-effecting process.

    Args:
        incarnation: The worker incarnation (lifecycle token).
    """
    path = drain_sentinel_path(incarnation)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(f"{incarnation}\n", encoding="utf-8")
    tmp.replace(path)


def drain_sentinel_matches(incarnation: str) -> bool:
    """Return whether a present drain sentinel exactly matches the incarnation.

    Args:
        incarnation: The worker incarnation (lifecycle token) to verify.

    Returns:
        ``True`` only when the sentinel exists and carries the exact token.
    """
    try:
        return drain_sentinel_path(incarnation).read_text(encoding="utf-8").strip() == incarnation
    except OSError:
        return False


def _owned_running_groups(conn: JobsConnection, incarnation: str) -> list[tuple[int, int | None]]:
    """Return the exact (process group id, start-time ticks) of owned commands.

    Args:
        conn: Open PostgreSQL connection.
        incarnation: The worker incarnation (lifecycle token) to match.

    Returns:
        Pairs of the exact process group id and the persisted command start-time
        ticks (``None`` when the legacy row never recorded ticks) for every
        running command owned by the incarnation.
    """
    groups: list[tuple[int, int | None]] = []
    with conn.transaction(), conn.cursor(row_factory=tuple_row) as cursor:
        cursor.execute(
            "SELECT id, (payload::jsonb)->'state'->>'process_pgid',\n"
            "       (payload::jsonb)->'state'->>'process_start_time_ticks'\n"
            "FROM lubko.jobs\n"
            "WHERE (payload::jsonb)->>'type' = 'command'\n"
            "    AND (payload::jsonb)->'state'->>'status' = 'running'\n"
            "    AND (payload::jsonb)->'state'->>'worker_incarnation' = %(inc)s\n"
            "    AND (payload::jsonb)->'state'->>'process_pgid' IS NOT NULL\n",
            {"inc": incarnation},
        )
        for row in cursor.fetchall():
            pgid = row[1]
            start_ticks = row[2]
            if pgid is None:
                continue
            try:
                pgid_i = int(str(pgid))
            except ValueError:
                continue
            start_i: int | None = None
            if start_ticks is not None:
                try:
                    start_i = int(str(start_ticks))
                except ValueError:
                    start_i = None
            groups.append((pgid_i, start_i))
    return groups


class GroupReclaimDecision(Enum):
    """Disposition of one persisted owned command group during recovery.

    Attributes:
        GONE: The group has no live members, so it is already safely reaped and
            recovery has converged for it (no signal, no obligation).
        RECLAIM: The group has live members and its persisted start-time ticks
            are valid/positive and exactly match the live leader's ticks, so it
            is provably our command and safe to signal.
        UNRESOLVED: The group has live members but its exact identity cannot be
            proven — missing, malformed, unreadable, or mismatched persisted
            start-time ticks. It must NOT be signalled, and it remains a durable
            blocking obligation until it is proven dead/recovered by some other
            means (e.g. the unrelated process exits) so the orchestrator holds
            rather than handing off sole-consumer authority alongside a
            potentially stranger-owned group.
    """

    GONE = auto()
    RECLAIM = auto()
    UNRESOLVED = auto()


def _group_reclaim_decision(pgid: int, start_ticks: int | None) -> GroupReclaimDecision:
    """Decide how to treat one persisted owned command group during recovery.

    A live group may be signalled only when it has members and its persisted
    ``process_start_time_ticks`` is valid/positive and exactly matches the live
    group leader's start-time ticks. Anything the persisted identity cannot
    prove is treated as unresolved (never signalled) so a recycled or stranger
    group is never killed, but it still blocks retirement until it converges.

    Args:
        pgid: The persisted process group id.
        start_ticks: The persisted command start-time ticks, ``None`` for a
            legacy row that never recorded them.

    Returns:
        The :class:`GroupReclaimDecision` for this group.
    """
    if not group_has_members(pgid):
        return GroupReclaimDecision.GONE
    if not isinstance(start_ticks, int) or start_ticks <= 0:
        # Missing (legacy/None) or malformed/non-positive ticks: we cannot prove
        # the live group is our command, so never signal it — but it is still
        # alive, so it remains a durable blocking obligation.
        return GroupReclaimDecision.UNRESOLVED
    live_ticks = proc_start_ticks(pgid)
    if live_ticks is None:
        # The leader's identity is unreadable; fail closed rather than risk a
        # mis-signal, but keep the group as a blocking obligation.
        return GroupReclaimDecision.UNRESOLVED
    if live_ticks != start_ticks:
        # The persisted group id has been recycled by an unrelated process (or
        # the wrong command): it is not ours, so never signal it. It stays a
        # blocking obligation until the stranger's group exits and converges.
        return GroupReclaimDecision.UNRESOLVED
    return GroupReclaimDecision.RECLAIM


@dataclass(frozen=True)
class ReclaimedGroups:
    """Exact-owned command groups after an emergency recovery pass.

    Attributes:
        reaped: Exact process-group ids that were signalled and proven dead.
        surviving: Exact process-group ids that were proven ours, signalled,
            but could not be proven dead within the cancel grace. These are a
            durable blocking obligation: the orchestrator must not clear the
            retired worker's sole-consumer authority or spawn a replacement
            until they are proven dead/recovered.
        unresolved: Exact process-group ids that have live members but whose
            exact identity could not be proven (missing/malformed/unreadable/
            mismatched persisted start-time ticks). They are never signalled,
            but remain a durable blocking obligation exactly like ``surviving``:
            the orchestrator must hold and retry rather than clear authority or
            start a replacement.
    """

    reaped: list[int]
    surviving: list[int]
    unresolved: list[int]


def _terminate_one_group(pgid: int, cancel_grace_seconds: float) -> bool:
    """Ask one exact process group to terminate, then SIGKILL and reap it.

    Args:
        pgid: Exact process group id to terminate.
        cancel_grace_seconds: Grace before escalating to SIGKILL.

    Returns:
        ``True`` only when the group is proven to have no surviving members
        after the SIGKILL + reap wait, so the caller knows it is safe to treat
        the recovery as complete.
    """
    _signal_group(pgid, signal.SIGTERM)
    term_deadline = time.monotonic() + cancel_grace_seconds
    while time.monotonic() < term_deadline and group_has_members(pgid):
        time.sleep(0.05)
    if group_has_members(pgid):
        _signal_group(pgid, signal.SIGKILL)
    reap_deadline = time.monotonic() + cancel_grace_seconds
    while time.monotonic() < reap_deadline and group_has_members(pgid):
        time.sleep(0.05)
    return not group_has_members(pgid)


def recover_owned_job_groups(
    conn: JobsConnection,
    incarnation: str,
    cancel_grace_seconds: float = DEFAULT_CANCEL_GRACE_SECONDS,
) -> ReclaimedGroups:
    """Terminate any live command process group still owned by an incarnation.

    This is the exact-ownership emergency recovery used after a maintained
    worker was force-killed while it still owned command process groups (a
    wedged worker whose own graceful drain never ran). Every acting target is an
    exact process group id persisted in the job row (``state.process_pgid``) for
    a ``running`` command whose ``worker_incarnation`` matches, so no
    process-name match or broad ``pkill``/``killall`` is ever used.

    A live group is signalled only when its persisted ``process_start_time_ticks``
    is valid/positive and exactly matches the live leader's start-time ticks
    (see :func:`_group_reclaim_decision`): this makes recovery PID-reuse-safe
    and fail-closed. A group whose exact identity cannot be proven — missing,
    malformed, unreadable, or mismatched ticks while it still has members — is
    never signalled and is reported as ``unresolved`` so the orchestrator holds
    rather than handing off sole-consumer authority alongside a possibly
    stranger-owned group. A group with no live members has already converged.

    Args:
        conn: Open PostgreSQL connection.
        incarnation: The worker incarnation (lifecycle token) whose groups to
            recover.
        cancel_grace_seconds: Grace before escalating to SIGKILL.

    Returns:
        A :class:`ReclaimedGroups` describing which exact groups were reaped,
        which verified-ours groups survived the cancel grace, and which live
        groups could not be identity-verified (unresolved).
    """
    groups = _owned_running_groups(conn, incarnation)
    if not groups:
        return ReclaimedGroups(reaped=[], surviving=[], unresolved=[])
    reaped: list[int] = []
    surviving: list[int] = []
    unresolved: list[int] = []
    for pgid, start_ticks in groups:
        decision = _group_reclaim_decision(pgid, start_ticks)
        if decision is GroupReclaimDecision.GONE:
            continue
        if decision is GroupReclaimDecision.RECLAIM:
            if _terminate_one_group(pgid, cancel_grace_seconds):
                reaped.append(pgid)
            else:
                surviving.append(pgid)
        else:
            unresolved.append(pgid)
    return ReclaimedGroups(reaped=reaped, surviving=surviving, unresolved=unresolved)


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


#: Path of the tiny dedicated start-gate wrapper exec'd by :func:`spawn_job`.
_START_GATE_WRAPPER: Final = Path(__file__).with_name("_start_gate.py")


def spawn_job(
    job: Job,
) -> tuple[subprocess.Popen[bytes], Path, Path, int, int, int, int]:
    """Start a job behind a persist-before-exec START GATE with pipe capture.

    The job's required ``process`` argv is executed directly as the new
    process; the worker never invokes a shell, so argv elements are passed to
    the executable literally. The job is started as a new session so
    cancellation can signal the exact process group.

    Standard output and standard error are captured through dedicated pipes.
    The child writes to the pipe write ends; the supervisor drains the read
    ends into bounded on-disk spool files (see :func:`drain_capture_stream`).
    Because a pipe backpressures its writer, a producer faster than the
    drainer/trimmer is throttled before it can allocate unbounded disk; the
    worker never relies on ``RLIMIT_FSIZE`` and never stops an unrelated job's
    file writes.

    The exact root job UUID is injected into the child environment as
    ``LUBKO_JOB_ID`` before the child execs, so every process of the job can
    identify its owning queue row deterministically without depending on the
    timing of any later database write.

    The wrapper (not the user program) is the dedicated session/process-group
    leader. It inherits a gate file descriptor and blocks on it before exec'ing
    the user argv, so ``Popen`` returns while the wrapper is still gated. The
    caller must durably persist the exact process identity (PID/PGID/start-time
    ticks) and then call :func:`release_gate` on the returned gate write end.
    Only then does the wrapper exec the user code on the exact same PID. If the
    worker dies before releasing, the kernel closes the gate write end, the
    wrapper reads EOF, and it exits WITHOUT executing any user code — so a
    forced SIGKILL in the spawn->persist window can never leave a user side
    effect running with no durable identity to recover it. The gate is not
    implemented with ``preexec_fn``: that would let ``Popen`` return only after
    the child had already run, defeating the gate.

    Every operating-system resource allocated before the child starts is closed
    on any failure: a broken ``os.pipe`` closes the partial pipe ends and spool
    files, and a failure during the post-``Popen`` setup (closing the write
    ends, marking the read ends nonblocking, waiting for the session) kills and
    reaps the exact spawned child, closes its capture pipe read ends, and
    removes its spool files so no resource leaks into the supervisor.

    Args:
        job: Claimed job to execute.

    Returns:
        The running gated process, its spool file paths, its process group ID,
        the worker-side write end of the start gate, and the read ends of its
        stdout/stderr capture pipes (in that order).

    Raises:
        OSError: If the process or its capture pipes cannot be started.
    """
    env = dict(os.environ)
    env[JOB_ID_ENV] = str(job.id)
    stdout_path = _make_spool_file()
    try:
        stderr_path = _make_spool_file()
    except OSError:
        stdout_path.unlink(missing_ok=True)
        raise
    # Sentinels so the partial-cleanup loops below are always defined; any fd
    # not yet created keeps the invalid value and is skipped by os.close.
    stdout_r = stdout_w = stderr_r = stderr_w = -1
    gate_read_fd = gate_write_fd = -1
    try:
        gate_read_fd, gate_write_fd = os.pipe()
        stdout_r, stdout_w = os.pipe()
        stderr_r, stderr_w = os.pipe()
    except OSError:
        for capture_fd in (
            gate_read_fd,
            gate_write_fd,
            stdout_r,
            stdout_w,
            stderr_r,
            stderr_w,
        ):
            with suppress(OSError):
                os.close(capture_fd)
        _cleanup_output_files(stdout_path, stderr_path)
        raise
    # The gate read end must survive the wrapper's exec, so it must not be
    # close-on-exec. Declaring it in ``pass_fds`` makes ``subprocess`` clear the
    # close-on-exec flag on the child side of the fork, so the wrapper blocks on
    # it across the exec boundary. The descriptor number is passed to the
    # wrapper through the environment (not ``argv``) because a Python launcher
    # re-exec can shift ``argv`` and would otherwise misalign the argument.
    env["LUBKO_START_GATE_FD"] = str(gate_read_fd)
    try:
        proc = subprocess.Popen(
            [sys.executable, os.fspath(_START_GATE_WRAPPER), *job.process],
            cwd=job.cwd,
            stdin=subprocess.DEVNULL,
            stdout=stdout_w,
            stderr=stderr_w,
            start_new_session=True,
            env=env,
            pass_fds=(gate_read_fd,),
        )
    except OSError:
        for capture_fd in (
            gate_read_fd,
            gate_write_fd,
            stdout_r,
            stdout_w,
            stderr_r,
            stderr_w,
        ):
            with suppress(OSError):
                os.close(capture_fd)
        _cleanup_output_files(stdout_path, stderr_path)
        raise
    # The wrapper holds the gate read end; the worker must not keep a copy.
    with suppress(OSError):
        os.close(gate_read_fd)
    # Post-Popen setup: any failure here must kill the exact child we already
    # spawned and close every resource, otherwise the supervisor leaks a live
    # process and open file descriptors.
    try:
        pgid = _prepare_capture_fds(proc, stdout_w, stderr_w, stdout_r, stderr_r)
    except BaseException:
        # Kill the whole process GROUP (the session leader's pgid equals its
        # pid), not just the leader: a child that forked descendants which share
        # the group must have every member terminated, otherwise a grandchild
        # would survive orphaned. Killing the unreleased gated wrapper makes it
        # exit without executing user code; the kernel reaps the leader via
        # proc.wait; the group's other members are reaped by PID 1 (tini).
        with suppress(OSError):
            os.killpg(proc.pid, signal.SIGKILL)
        try:
            proc.wait(timeout=DEFAULT_CANCEL_GRACE_SECONDS)
        except subprocess.TimeoutExpired:
            # The reap timing out must never skip the local cleanup below; the
            # inability to prove the group dead is surfaced instead.
            LOGGER.exception(
                "could not prove process group %d dead after post-spawn failure",
                proc.pid,
            )
        for capture_fd in (
            gate_write_fd,
            stdout_r,
            stdout_w,
            stderr_r,
            stderr_w,
        ):
            with suppress(OSError):
                os.close(capture_fd)
        _cleanup_output_files(stdout_path, stderr_path)
        raise
    return proc, stdout_path, stderr_path, pgid, gate_write_fd, stdout_r, stderr_r


def release_gate(gate_fd: int) -> bool:
    """Release a gated start so the wrapper execs the user argv.

    Writes the single release control byte and closes the worker's gate write
    end. The wrapper reads the byte and execs the user program on the exact
    same PID whose identity the caller has already durably persisted. If this
    process dies before closing, the kernel closes the write end and the
    wrapper reads EOF and exits without executing any user code.

    A failure to write the release byte is reported, never silently swallowed:
    the caller must treat it as a start failure and abort the gated start so
    the job does not later look like a normally completed user command.

    Args:
        gate_fd: The worker-side write end of the start gate pipe.

    Returns:
        ``True`` when the release byte was written (the wrapper will exec the
        user argv); ``False`` when the write failed, in which case the wrapper
        can never be released by this handle.
    """
    try:
        os.write(gate_fd, GATE_RELEASE_BYTE)
    except OSError:
        with suppress(OSError):
            os.close(gate_fd)
        return False
    with suppress(OSError):
        os.close(gate_fd)
    return True


@dataclass(frozen=True)
class GatedSpawn:
    """Handles and exact identity of one gated start produced by ``spawn_job``.

    Attributes:
        proc: The gated wrapper process.
        pgid: Exact process group id of the gated wrapper.
        stdout_path: Capture file for standard output.
        stderr_path: Capture file for standard error.
        gate_fd: The worker-side write end of the start gate pipe.
        stdout_read_fd: Read end of the stdout capture pipe, or ``None`` once
            closed.
        stderr_read_fd: Read end of the stderr capture pipe, or ``None`` once
            closed.
    """

    proc: subprocess.Popen[bytes]
    pgid: int
    stdout_path: Path
    stderr_path: Path
    gate_fd: int
    stdout_read_fd: int | None = None
    stderr_read_fd: int | None = None


def abort_gated_start(
    proc: subprocess.Popen[bytes],
    pgid: int,
    stdout_path: Path,
    stderr_path: Path,
    gate_fd: int,
) -> bool:
    """Abort a gated start using ONLY its exact direct-child identity.

    Used when the exact process identity could not be obtained or durably
    persisted, or when the gate release itself failed: the gate is closed
    WITHOUT the release byte so the wrapper exits on EOF. The dedicated group
    is SIGKILLed only WHILE the exact ``Popen`` remains live — before a
    successful release it is this process's own childless session leader, so
    the group is provably ours. Once the direct child becomes terminal and is
    reaped, the original unreleased childless group is gone by construction
    and the numeric PGID is deliberately NEVER used again as proof or target,
    because it could already have been reused by an unrelated process.

    Args:
        proc: The gated wrapper process.
        pgid: Exact process group id of the wrapper (used only while the
            wrapper itself is still live).
        stdout_path: Capture file for standard output.
        stderr_path: Capture file for standard error.
        gate_fd: The worker-side write end of the start gate pipe.

    Returns:
        ``True`` when the direct child is terminal, reaped, and the capture
        files were removed (the unreleased group is gone by construction);
        ``False`` when the child remained live after the bounded first attempt,
        in which case :func:`await_gated_group_gone` owns it until reap.
    """
    # Close the gate WITHOUT releasing: the wrapper reads EOF and exits.
    with suppress(OSError):
        os.close(gate_fd)
    deadline = time.monotonic() + 5.0
    while proc.poll() is None and time.monotonic() < deadline:
        _signal_group(pgid, signal.SIGKILL)
        time.sleep(0.02)
    if proc.poll() is not None:
        with suppress(subprocess.TimeoutExpired):
            proc.wait(timeout=5)
        stdout_path.unlink(missing_ok=True)
        stderr_path.unlink(missing_ok=True)
        return True
    LOGGER.error(
        "gated wrapper %d (exact group %d) stayed live through the bounded "
        "abort attempt; handing to the blocking direct-child convergence loop",
        proc.pid,
        pgid,
    )
    return False


def await_gated_group_gone(gated: GatedSpawn) -> None:
    """Block until the unreleased gated wrapper is terminal and reaped.

    Stronger gated-wrapper invariant: before a successful release,
    ``_start_gate.py`` is this process's DIRECT child, started with
    ``start_new_session=True``, and is childless. While
    ``gated.proc.poll()`` is ``None`` the PID cannot be reused and its
    dedicated process group is provably ours, so repeatedly SIGKILLing that
    exact group is authorized. This synchronously blocks — there is no
    timeout return while the pre-release child remains live. Once the direct
    child is terminal and reaped, the original unreleased childless group is
    gone by construction; the numeric PGID is deliberately never polled or
    signalled afterwards, because it could later be reused by a stranger.

    Args:
        gated: Handles and exact identity of the gated start to converge.
    """
    while gated.proc.poll() is None:
        _signal_group(gated.pgid, signal.SIGKILL)
        time.sleep(0.05)
    with suppress(subprocess.TimeoutExpired):
        gated.proc.wait(timeout=5)
    # The direct child is reaped: the original unreleased childless group is
    # gone by construction. Clean up captures now; never touch the numeric
    # PGID again (it may already be reused by an unrelated process).
    gated.stdout_path.unlink(missing_ok=True)
    gated.stderr_path.unlink(missing_ok=True)
    for capture_fd_attr in ("stdout_read_fd", "stderr_read_fd"):
        capture_fd = getattr(gated, capture_fd_attr)
        if capture_fd is not None:
            with suppress(OSError):
                os.close(capture_fd)


def _prepare_capture_fds(
    proc: subprocess.Popen[bytes],
    stdout_w: int,
    stderr_w: int,
    stdout_r: int,
    stderr_r: int,
) -> int:
    """Close the parent's write ends and mark the read ends nonblocking.

    Args:
        proc: The spawned process whose session to await.
        stdout_w: Parent write end for stdout.
        stderr_w: Parent write end for stderr.
        stdout_r: Parent read end for stdout.
        stderr_r: Parent read end for stderr.

    Returns:
        The exact process group ID of the spawned child.
    """
    # The supervisor keeps only the read ends.  It must close the write ends in
    # the parent: otherwise the pipe would never report end-of-file once the
    # child exited (the supervisor would still hold a write end), and draining
    # would hang.  The child closes them itself on exec via close-on-exec.
    for write_fd in (stdout_w, stderr_w):
        with suppress(OSError):
            os.close(write_fd)
    # Nonblocking read ends so the single supervisor loop can drain many jobs
    # without ever blocking on one slow producer.
    for read_fd in (stdout_r, stderr_r):
        flags = fcntl.fcntl(read_fd, fcntl.F_GETFL)
        fcntl.fcntl(read_fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)
    return _wait_for_session(proc.pid)


def _make_spool_file() -> Path:
    """Create an empty on-disk capture spool file for one stream.

    ``tempfile.mkstemp`` returns an open file descriptor; the spool is written
    later through its own ``O_APPEND`` handle, so the descriptor it hands back
    is closed immediately to avoid leaking it. A creation failure propagates as
    ``OSError`` with no partial file left behind.

    Returns:
        The path of the created, empty spool file.
    """
    fd, name = tempfile.mkstemp()
    with suppress(OSError):
        os.close(fd)
    return Path(name)


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


# ---------------------------------------------------------------------------
# Database operations
# ---------------------------------------------------------------------------


def claim_jobs(conn: JobsConnection, settings: Settings, limit: int) -> list[ClaimedJob]:
    """Atomically claim up to ``limit`` pending command jobs.

    Claiming locks pending rows with ``FOR UPDATE SKIP LOCKED`` and writes the
    mutable claim state with a compare-and-swap update of the JSON payload, so
    several workers can safely compete for the same queue and two daemons never
    execute the same root job. Only ``command`` rows are claimed; immutable
    ``output_chunk`` rows are never claim candidates.

    The claim records the worker's incarnation and grants each job a lease by
    writing ``state.lease_expires_at``; the owning worker refreshes those
    leases by heartbeat while the jobs run.

    Args:
        conn: Open PostgreSQL connection.
        settings: Worker runtime settings.
        limit: Maximum number of jobs to claim in this turn (a fairness bound,
            not a concurrency cap).

    Returns:
        The claimed jobs and their payload text.
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
            ("state,worker_id", "to_jsonb(%(worker_id)s::text)"),
            ("state,worker_incarnation", "to_jsonb(%(worker_incarnation)s::text)"),
            (
                "state,lease_expires_at",
                LEASE_EXPIRES_AT_SQL,
            ),
            ("state,updated_at", UTC_ISO_SQL),
        ],
    )
    with conn.transaction(), conn.cursor(row_factory=tuple_row) as cursor:
        cursor.execute(
            _claim_jobs_sql(set_chain),
            {
                "worker_id": settings.worker_id,
                "worker_incarnation": settings.worker_incarnation,
                "lease_duration_seconds": settings.lease_duration_seconds,
                "limit": limit,
            },
        )
        rows = cursor.fetchall()
    return [ClaimedJob(id=row[0], payload=str(row[1])) for row in rows]


def claim_job(conn: JobsConnection, settings: Settings) -> ClaimedJob | None:
    """Atomically claim the oldest pending command job, if any.

    Args:
        conn: Open PostgreSQL connection.
        settings: Worker runtime settings.

    Returns:
        The claimed job and its payload text, or ``None`` if the queue is empty.
    """
    claimed = claim_jobs(conn, settings, 1)
    return claimed[0] if claimed else None


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


def bulk_refresh_leases(
    conn: JobsConnection, settings: Settings, root_ids: Collection[UUID]
) -> list[UUID]:
    """Refresh the lease of the given owned running command rows.

    One statement updates only the explicitly listed root IDs in a single
    atomic JSON compare-and-swap, keeping heartbeats efficient under many
    concurrent jobs while never touching a row the worker does not locally own.
    The caller is responsible for passing exactly the locally-owned active or
    retry-owned root IDs; this scoping is what guarantees an orphaned owned row
    (for example a claimed job whose immediate finalization write failed) is
    never heartbeated merely because another job is active.

    Args:
        conn: Open PostgreSQL connection.
        settings: Worker runtime settings.
        root_ids: The exact root IDs whose lease should be refreshed. Rows whose
            ID is not in this collection are left untouched even if they are
            owned by this worker.

    Returns:
        The IDs of the rows whose lease was refreshed.
    """
    set_chain = _jsonb_set_chain(
        "payload::jsonb",
        [
            ("state,lease_expires_at", LEASE_EXPIRES_AT_SQL),
            ("state,updated_at", UTC_ISO_SQL),
        ],
    )
    with conn.transaction(), conn.cursor(row_factory=tuple_row) as cursor:
        cursor.execute(
            "UPDATE lubko.jobs\n"
            "SET payload = " + set_chain + "::text\n"
            "WHERE (payload::jsonb)->>'type' = 'command'\n"
            "    AND (payload::jsonb)->'state'->>'status' = 'running'\n"
            "    AND (payload::jsonb)->'state'->>'worker_id' = %(worker_id)s\n"
            "    AND (payload::jsonb)->'state'->>'worker_incarnation' = %(worker_incarnation)s\n"
            "    AND id = ANY(%(root_ids)s)\n"
            "RETURNING id\n",
            {
                "lease_duration_seconds": settings.lease_duration_seconds,
                "worker_id": settings.worker_id,
                "worker_incarnation": settings.worker_incarnation,
                "root_ids": list(root_ids),
            },
        )
        rows = cursor.fetchall()
    return [row[0] for row in rows]


def apply_lease_refresh(
    active: dict[UUID, ActiveJob],
    refreshed: set[UUID],
    refresh_mono: float,
) -> None:
    """Advance the local lease deadline only for rows whose refresh committed.

    The local lease-safety deadline is advanced exclusively for the job
    identifiers returned by a *committed* bulk refresh, and it is anchored to
    ``refresh_mono`` (the monotonic time captured before the refresh database
    operation, never at commit time) so the deadline never exceeds the database
    lease.  A refresh that fails to commit never yields a refreshed set, so
    this function is never invoked with one and a missed heartbeat can never
    silently extend a job's safe lifetime.

    Only locally-owned active jobs that were eligible for a heartbeat in the
    first place are considered; quarantined, row-lost, lease-evicted, and
    finalized jobs are intentionally skipped even when not in ``refreshed``,
    because they were never part of the refresh and must not be stopped as if a
    stale recovery had taken their row.

    Args:
        active: The supervisor's active job registry.
        refreshed: Identifiers whose lease was refreshed and committed.
        refresh_mono: Monotonic time captured before the refresh committed.
    """
    for job_id, job in list(active.items()):
        if job.finalized or job.quarantined or job.row_lost or job.lease_evicted:
            continue
        if job_id in refreshed:
            job.last_heartbeat_at = refresh_mono
        else:
            # The row is no longer running (for example it was recovered by
            # another worker): never let the live process continue.
            LOGGER.warning("job %s is no longer running in the database; stopping it", job_id)
            job.row_lost = True
            request_stop(job, STOP_REASON_ROW_LOST)


def discover_cancellations(conn: JobsConnection, settings: Settings) -> list[UUID]:
    """Find owned running command jobs that have a cancellation marker.

    Reads in a bounded batch so an endless stream of cancellations cannot starve
    other supervisor work.

    Args:
        conn: Open PostgreSQL connection.
        settings: Worker runtime settings.

    Returns:
        The IDs of owned running jobs with ``state.cancel_requested_at`` set.
    """
    with conn.transaction(), conn.cursor(row_factory=tuple_row) as cursor:
        cursor.execute(
            "SELECT id\n"
            "FROM lubko.jobs\n"
            "WHERE (payload::jsonb)->>'type' = 'command'\n"
            "    AND (payload::jsonb)->'state'->>'status' = 'running'\n"
            "    AND (payload::jsonb)->'state'->>'cancel_requested_at' IS NOT NULL\n"
            "    AND (payload::jsonb)->'state'->>'worker_id' = %(worker_id)s\n"
            "    AND (payload::jsonb)->'state'->>'worker_incarnation' = %(worker_incarnation)s\n"
            "LIMIT %(limit)s\n",
            {
                "worker_id": settings.worker_id,
                "worker_incarnation": settings.worker_incarnation,
                "limit": CANCEL_DISCOVERY_LIMIT,
            },
        )
        rows = cursor.fetchall()
    return [row[0] for row in rows]


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
    the same job. Only ``command`` rows are considered; ``output_chunk`` rows
    are never candidates for claim or lease recovery. Rows are locked with
    ``FOR UPDATE SKIP LOCKED`` and the status transition is a single atomic
    update, making the pass safe under many concurrent workers.

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
        cursor.execute(_recover_stale_jobs_sql(set_chain), {"limit": LEASE_RECOVERY_LIMIT})
        rows = cursor.fetchall()
    return [(row[0], str(row[1])) for row in rows]


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


def finish_job(conn: JobsConnection, job_id: UUID, result: JobResult) -> str:
    """Persist the final result of a job into its JSON payload.

    A cancellation request accepted before finalization wins over a natural
    completion. Already terminal jobs are never rewritten. The rolling
    ``output`` live tails written by publication remain in place. Only ``id``
    and ``payload`` are touched, preserving the two-column table invariant.

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


def _quarantine_job(conn: JobsConnection, job_id: UUID, reason: str) -> bool:
    """Terminalize a job that hit a deterministic database error.

    Writes a ``failed`` terminal status directly, bypassing the normal
    finalization path so publication/finalization data errors cannot block
    terminalization. Only non-terminal rows are updated: already-terminal
    rows are left untouched.

    A connectivity error during the terminalization attempt is re-raised
    so the caller can enter outage handling; a different deterministic
    error is logged and returns ``False`` so the caller can retry later.

    Args:
        conn: Open PostgreSQL connection.
        job_id: Identifier of the job to quarantine.
        reason: PostgreSQL-safe diagnostic text (no NUL bytes).

    Returns:
        ``True`` when the row was successfully terminalized or was already
        terminal (durable).  ``False`` when the terminalization write
        failed and must be retried.

    Raises:
        psycopg.Error: When the error is a connectivity issue.
    """
    safe_reason = reason.replace("\x00", "\ufffd")
    try:
        with conn.transaction(), conn.cursor(row_factory=tuple_row) as cursor:
            cursor.execute(
                "UPDATE lubko.jobs\n"
                "SET payload = (\n"
                "  jsonb_set(\n"
                "    jsonb_set(\n"
                "      jsonb_set(\n"
                "        jsonb_set(payload::jsonb, '{state,status}', to_jsonb('failed'::text)),\n"
                "        '{state,finished_at}', " + UTC_ISO_SQL + "\n"
                "      ),\n"
                "      '{state,updated_at}', " + UTC_ISO_SQL + "\n"
                "    ),\n"
                "    '{state,quarantine_reason}', to_jsonb(%(reason)s::text)\n"
                "  )\n"
                ")::text\n"
                "WHERE id = %(job_id)s\n"
                "  AND (payload::jsonb)->>'type' = 'command'\n"
                "  AND (payload::jsonb)->'state'->>'status'\n"
                "      NOT IN ('succeeded','failed','cancelled')\n"
                "RETURNING id\n",
                {"job_id": job_id, "reason": safe_reason},
            )
            cursor.fetchone()
    except psycopg.Error as exc:
        if _is_connectivity_error_check(exc, conn):
            raise
        LOGGER.exception(
            "quarantine terminalization for job %s failed (SQLSTATE %s)",
            job_id,
            exc.sqlstate or "N/A",
        )
        return False
    # RETURNING found no row: either already terminal or type mismatch.
    # Either way the row is safe — nothing more to write.
    return True


def delete_job_and_chunks(conn: JobsConnection, job_id: UUID) -> None:
    """Delete a root job and every output chunk explicitly owned by it.

    Cleanup uses explicit ``thread`` ownership rather than trusting the
    ``previous`` pointer chain, so orphaned chunks whose chain became
    incomplete because of a crash or corruption are also removed. The root row
    is deleted first and the owned chunks in a separate statement, all within
    one transaction.

    Deleting the root before the chunks serializes cleanup with concurrent
    output publication, which retains the root ``command`` row with a row-level
    lock before inserting new ``output_chunk`` rows. A single unordered
    ``DELETE`` may scan and delete chunk rows before acquiring the root row
    lock, so chunks committed by a concurrent publication after that statement
    snapshot can survive the later root deletion as orphans. Deleting the root
    first makes any concurrent publication either block on the root lock until
    cleanup commits and then find no root (inserting nothing), or commit its
    chunks before the chunk-cleanup statement starts, so that statement's fresh
    snapshot removes them.

    Args:
        conn: Open PostgreSQL connection.
        job_id: Identifier of the root job to delete.
    """
    with conn.transaction(), conn.cursor() as cursor:
        cursor.execute(
            "DELETE FROM lubko.jobs\nWHERE id = %(job_id)s\n",
            {"job_id": job_id},
        )
        cursor.execute(
            "DELETE FROM lubko.jobs\n"
            "WHERE (payload::jsonb)->>'type' = 'output_chunk'\n"
            "    AND (payload::jsonb)->>'thread' = %(thread)s\n",
            {"thread": str(job_id)},
        )


def collect_transport(conn: JobsConnection, settings: Settings) -> tuple[list[UUID], int, int]:
    """Collect terminal command rows, their owned chunks, and orphan chunks.

    Three bounded phases run in separate transactions, each with ``FOR UPDATE
    SKIP LOCKED`` so multiple workers/restarts converge safely:

    **Phase 1 — Mark** (one transaction): A bounded batch of terminal
    ``command`` rows whose ``finished_at`` is older than the retention window
    is selected and atomically marked with ``state.gc = true``.  Publication
    explicitly refuses GC-marked roots, so no new ``output_chunk`` rows can be
    created for them after the mark commits.  Only rows with
    ``status IN ('succeeded', 'failed', 'cancelled')`` are eligible; unknown
    or future statuses are retained.  Abandoned ``running`` rows are handled
    by :func:`recover_stale_jobs`.

    **Phase 2 — Chunk drain + root finalization** (one transaction per batch):
    For each GC-marked root, a bounded batch of its owned chunks (via
    ``thread``) is deleted using ``FOR UPDATE SKIP LOCKED``, capped at
    ``gc_batch_limit`` rows.  This keeps rows/transaction/lock duration
    bounded even when a single root owns millions of chunks.  After bounded
    chunk deletion, if no chunks remain for a root, the root row itself is
    deleted.  The root only disappears after its chunks are drained, which
    preserves the documented root-first/publication-safety invariant: the
    ``gc`` flag prevents new chunks, and the root is removed once all chunks
    from the marking snapshot are gone.

    **Phase 3 — Orphan cleanup** (one transaction): A bounded anti-join
    ``SELECT`` (with ``LIMIT`` and ``FOR UPDATE ... SKIP LOCKED``) finds
    ``output_chunk`` rows whose owning root ``command`` row is absent.  The
    comparison is cast-free and case-normalized (``lower(root.id::text) =
    lower(thread)``), so malformed, empty, or non-UUID thread text never
    causes a cast error regardless of planner predicate reordering, and
    uppercase canonical UUIDs match correctly.  Matched rows are deleted in
    one bounded ``DELETE``.  This pass is safe without root-first ordering:
    the owning root is already gone, so no concurrent publication can create
    new chunks for it.

    Args:
        conn: Open PostgreSQL connection.
        settings: Worker runtime settings.

    Returns:
        A ``(roots_marked, chunks_deleted, orphans_deleted)`` triple.
    """
    roots_marked: list[UUID] = []
    roots_deleted = 0
    total_chunks = 0
    total_orphans = 0

    # --- Phase 1: mark terminal roots as GC ---
    # The retention cutoff is pre-computed in a CTE so the main query string
    # contains no concatenation and passes hardcoded-sql-expression.
    with conn.transaction(), conn.cursor(row_factory=tuple_row) as cursor:
        cursor.execute(
            "WITH gc_params AS (\n"
            "    SELECT to_char(\n"
            "        now() at time zone 'utc'\n"
            "        - make_interval(secs => %(gc_retention_seconds)s),\n"
            '        \'YYYY-MM-DD"T"HH24:MI:SS.US"Z"\'\n'
            "    ) AS cutoff\n"
            ")\n"
            "UPDATE lubko.jobs\n"
            "SET payload = jsonb_set(payload::jsonb, '{state,gc}', to_jsonb(true))::text\n"
            "WHERE id IN (\n"
            "    SELECT id\n"
            "    FROM lubko.jobs, gc_params\n"
            "    WHERE (payload::jsonb)->>'type' = 'command'\n"
            "        AND (payload::jsonb)->'state'->>'status'\n"
            "            IN ('succeeded', 'failed', 'cancelled')\n"
            "        AND (payload::jsonb)->'state'->>'finished_at' IS NOT NULL\n"
            "        AND ((payload::jsonb)->'state'->>'finished_at') < gc_params.cutoff\n"
            "        AND ((payload::jsonb)->'state'->>'gc') IS DISTINCT FROM 'true'\n"
            "    ORDER BY ((payload::jsonb)->'state'->>'finished_at'), id\n"
            "    FOR UPDATE SKIP LOCKED\n"
            "    LIMIT %(limit)s\n"
            ")\n"
            "RETURNING id\n",
            {
                "gc_retention_seconds": settings.gc_retention_seconds,
                "limit": settings.gc_batch_limit,
            },
        )
        roots_marked = [row[0] for row in cursor.fetchall()]

    # --- Phase 2: bounded chunk drain + root finalization ---
    with conn.transaction(), conn.cursor(row_factory=tuple_row) as cursor:
        cursor.execute(
            "SELECT id\n"
            "FROM lubko.jobs\n"
            "WHERE (payload::jsonb)->>'type' = 'command'\n"
            "    AND ((payload::jsonb)->'state'->>'gc') = 'true'\n"
            "ORDER BY id\n"
            "FOR UPDATE SKIP LOCKED\n"
            "LIMIT %(limit)s\n",
            {"limit": settings.gc_batch_limit},
        )
        gc_roots = [row[0] for row in cursor.fetchall()]
        for root_id in gc_roots:
            # Bounded chunk deletion for this root.  lower() normalises
            # case so uppercase-UUID thread chunks are matched correctly.
            cursor.execute(
                "DELETE FROM lubko.jobs\n"
                "WHERE id IN (\n"
                "    SELECT id\n"
                "    FROM lubko.jobs\n"
                "    WHERE (payload::jsonb)->>'type' = 'output_chunk'\n"
                "        AND lower((payload::jsonb)->>'thread') = lower(%(thread)s)\n"
                "    FOR UPDATE SKIP LOCKED\n"
                "    LIMIT %(limit)s\n"
                ")\n",
                {"thread": str(root_id), "limit": settings.gc_batch_limit},
            )
            total_chunks += cursor.rowcount
            # Delete root only if no chunks remain.
            cursor.execute(
                "SELECT NOT EXISTS (\n"
                "    SELECT 1\n"
                "    FROM lubko.jobs\n"
                "    WHERE (payload::jsonb)->>'type' = 'output_chunk'\n"
                "        AND lower((payload::jsonb)->>'thread') = lower(%(thread)s)\n"
                ")\n",
                {"thread": str(root_id)},
            )
            no_chunks = cursor.fetchone()
            if no_chunks is not None and no_chunks[0]:
                cursor.execute(
                    "DELETE FROM lubko.jobs\nWHERE id = %(job_id)s\n",
                    {"job_id": root_id},
                )
                roots_deleted += cursor.rowcount

    # --- Phase 3: bounded orphan cleanup ---
    # Cast-free, case-normalized comparison: lower(root.id::text) = lower(thread).
    # No ::uuid cast is attempted on the thread value, and lower() normalises
    # case so uppercase canonical UUIDs match.  Malformed, empty, or non-UUID
    # text simply never matches any root.id text.  This is intrinsically safe
    # regardless of planner predicate reordering.
    with conn.transaction(), conn.cursor(row_factory=tuple_row) as cursor:
        cursor.execute(
            "SELECT chunk.id\n"
            "FROM lubko.jobs AS chunk\n"
            "WHERE chunk.payload::jsonb->>'type' = 'output_chunk'\n"
            "    AND NOT EXISTS (\n"
            "        SELECT 1\n"
            "        FROM lubko.jobs AS root\n"
            "        WHERE lower(root.id::text) =\n"
            "            lower(chunk.payload::jsonb->>'thread')\n"
            "            AND root.payload::jsonb->>'type' = 'command'\n"
            "    )\n"
            "LIMIT %(limit)s\n"
            "FOR UPDATE OF chunk SKIP LOCKED\n",
            {"limit": settings.gc_batch_limit},
        )
        orphan_ids = [row[0] for row in cursor.fetchall()]
        if orphan_ids:
            cursor.execute(
                "DELETE FROM lubko.jobs\nWHERE id = ANY(%(ids)s)\n",
                {"ids": orphan_ids},
            )
            total_orphans += len(orphan_ids)

    if roots_deleted:
        LOGGER.info("gc deleted %d root(s)", roots_deleted)
    return roots_marked, total_chunks, total_orphans


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


def verify_protocol_schema(conn: JobsConnection) -> None:
    """Assert that ``lubko.jobs`` carries the canonical protocol v3 shape.

    The two-column invariant alone does not make a table usable by a v3
    worker: immutable ``output_chunk`` publication requires the type-aware
    ``jobs_payload_type_shape`` check constraint and the chunk
    ownership/ordering indexes, which the single canonical baseline
    ``migrations/0001_two_column_protocol.sql`` declares. The worker refuses to
    start against any table lacking this shape so output publication can never
    fail at runtime on a table that cannot represent immutable chunks.

    Args:
        conn: Open PostgreSQL connection.

    Raises:
        SchemaInvariantError: If the type-aware constraint or any required
            output-chunk index is missing.
    """
    with conn.transaction(), conn.cursor(row_factory=tuple_row) as cursor:
        cursor.execute(
            "SELECT conname\n"
            "FROM pg_constraint\n"
            "WHERE conrelid = to_regclass(%s) AND contype = 'c'\n",
            (f"{JOBS_SCHEMA}.{JOBS_TABLE}",),
        )
        constraints = {str(row[0]) for row in cursor.fetchall()}
        cursor.execute(
            "SELECT indexname\nFROM pg_indexes\nWHERE schemaname = %s AND tablename = %s\n",
            (JOBS_SCHEMA, JOBS_TABLE),
        )
        indexes = {str(row[0]) for row in cursor.fetchall()}
    missing: list[str] = []
    if TYPE_AWARE_CONSTRAINT_NAME not in constraints:
        missing.append(f"check constraint {TYPE_AWARE_CONSTRAINT_NAME}")
    missing.extend(
        f"index {name}"
        for name in (CHUNK_OWNER_INDEX_NAME, CHUNK_ORDER_INDEX_NAME)
        if name not in indexes
    )
    if missing:
        detail = ", ".join(missing)
        msg = (
            f"lubko.jobs lacks the canonical output-chunk schema shape required "
            f"for immutable output publication: missing {detail}. Apply the "
            f"canonical, idempotent baseline migration "
            f"migrations/0001_two_column_protocol.sql (any v2 -> v3 cutover needs "
            f"no DDL: the two-column table is identical for both versions). "
            f"{TWO_COLUMN_INVARIANT}"
        )
        raise SchemaInvariantError(msg)


# ---------------------------------------------------------------------------
# Supervisor
# ---------------------------------------------------------------------------


class Supervisor:
    """One nonblocking daemon supervising arbitrarily many concurrent jobs.

    The supervisor owns a single PostgreSQL connection and an unbounded
    in-memory registry of active jobs. Every tick it services running jobs
    (observes exits, escalates cancellations), refreshes leases, discovers
    cancellation markers, publishes changed output tails/chunks, finalizes
    completed jobs, and claims a bounded batch of new pending jobs. It never
    allocates a thread or a connection per job and never synchronously waits
    for any one child.
    """

    def __init__(self, settings: Settings, database: DatabaseConfig) -> None:
        """Build a supervisor with an empty active registry.

        Args:
            settings: Worker runtime settings.
            database: Database connection settings loaded from the restricted
                file.
        """
        self.settings = settings
        self.database = database
        self.active: dict[UUID, ActiveJob] = {}
        self._retry_terminations: dict[UUID, _RetryTerminalization] = {}
        self.conn: JobsConnection | None = None
        self._stopping = False
        self._next_recovery_at = 0.0
        self._next_lease_refresh_at = 0.0
        self._next_cancel_scan_at = 0.0
        self._next_reconnect_at = 0.0
        self._next_gc_at = 0.0
        self._started_at = time.time()
        self._start_time_ticks = proc_start_ticks(os.getpid()) or 0
        self._db_connected_at: float | None = None
        self._db_error_at: float | None = None
        self._last_completed_job_id: str | None = None
        self._last_completed_at: float | None = None
        self._last_completed_status: str | None = None
        self._next_health_publish_at = 0.0
        self._health_force = True

    def request_shutdown(self) -> None:
        """Request a graceful shutdown from another thread or a signal handler.

        The supervisor stops claiming jobs and then terminates, reaps, and
        finalizes every tracked active process group.
        """
        self._stopping = True

    def run(self) -> None:
        """Run the supervisor loop until a graceful shutdown is requested.

        The first connection is verified against the two-column transport
        invariant; a violated invariant is fatal.

        Connection-level failures (lost/unusable connection) enter outage
        handling and trigger reconnection.  Per-job deterministic data/SQL
        errors are caught locally within the db-phase methods so one bad
        job never poisons the entire worker or triggers a reconnect loop.

        Any unexpected ``psycopg.Error`` that escapes the per-job boundaries
        and reaches this outer catch is classified:

        * Connectivity errors (class 08 or broken/closed connection) enter
          outage handling and reconnection.
        * Non-connectivity errors are unexpected at this level and indicate
          a global programming or schema fault.  The supervisor logs the
          real exception/SQLSTATE and stops, deferring recovery to the
          external process supervisor's crash-loop backoff.
        """
        self._connect()
        self._publish_health(force=True)
        while not self._stopping:
            try:
                self._tick(time.monotonic())
            except psycopg.Error as exc:
                if self._is_connectivity_error(exc):
                    self._enter_outage()
                else:
                    LOGGER.critical(
                        "unexpected non-connectivity database error "
                        "(SQLSTATE %s); stopping supervisor: %s",
                        exc.sqlstate or "N/A",
                        exc,
                    )
                    self._stopping = True
            self._publish_health()
            time.sleep(self.settings.process_poll_interval_seconds)
        self._shutdown()

    def _tick(self, now: float) -> None:
        """Run one supervisor turn: service processes, then database work.

        Args:
            now: Monotonic time at the start of the turn.
        """
        self._service_processes()
        self._drain_captures()
        self._enforce_spool_bounds()
        if self._stopping:
            return
        if self.conn is None:
            self._outage_phase()
            return
        self._db_phase(now)

    def _service_processes(self) -> None:
        """Observe child exits and escalate in-flight cancellations.

        Every child is polled (which reaps it) so concurrent execution never
        leaves zombies. A ``SIGTERM`` that already went out escalates to
        ``SIGKILL`` for the exact process group after the bounded grace period
        while any member remains.
        """
        now = time.monotonic()
        for job in list(self.active.values()):
            self._observe_and_escalate(job, now)

    def _db_phase(self, now: float) -> None:
        """Run the database-backed supervision work of one turn.

        Args:
            now: Monotonic time at the start of the turn.
        """
        if now >= self._next_recovery_at:
            self._run_recovery()
            self._next_recovery_at = (
                time.monotonic() + self.settings.lease_recovery_interval_seconds
            )
        if self.active and now >= self._next_lease_refresh_at:
            self._refresh_leases()
            self._next_lease_refresh_at = (
                time.monotonic() + self.settings.lease_refresh_interval_seconds
            )
        if now >= self._next_cancel_scan_at:
            self._discover_cancellations()
            self._next_cancel_scan_at = time.monotonic() + max(
                self.settings.poll_interval_seconds, 0.5
            )
        self._publish_all(now)
        self._finalize_completed()
        self._retry_terminalizations()
        if now >= self._next_gc_at:
            self._run_gc()
            self._next_gc_at = time.monotonic() + self.settings.gc_interval_seconds
        if not self._stopping:
            self._claim_batch()

    def _drain_captures(self, bound: int | None = None) -> None:
        """Drain every active job's capture pipes into their bounded spools.

        The single nonblocking supervisor loop services all jobs through
        ``poll``; a producer faster than the drainer/trimmer fills the kernel
        pipe buffer and then blocks on ``write()``, so it is backpressured
        before any unbounded disk allocation occurs. A spool stat/read/select
        error fails only the offending job (never the whole worker), and a bad
        capture fd among many jobs is isolated to exactly that job so its
        siblings keep draining.

        The bound is an aggregate per-job limit across both streams, so each
        stream's room is computed against the job's combined on-disk usage. The
        physical spool never exceeds this bound: when it is full the pipe is
        deliberately not read and the producer is backpressured. A completed or
        terminating job whose spool is full is not drained past the bound; instead
        the supervisor publishes and trims the captured bytes to free bounded
        room and drains more (see :meth:`_finalize_completed_job_bounded`), so a
        job's final output is captured completely without ever exceeding the
        bound.

        Args:
            bound: Per-job aggregate spool bound in bytes, or ``None`` for the
                configured default. Always enforced.
        """
        if bound is None:
            bound = self.settings.output_spool_max_bytes
        candidates = self._drain_candidates()
        if not candidates:
            return
        rlist = [stream.fd for _, _name, stream in candidates if stream.fd is not None]
        try:
            readable = _poll_readable(rlist)
        except (OSError, ValueError):
            # A bad fd among many jobs must not poison the whole worker: probe
            # each fd individually and fail closed only the offending job,
            # closing exactly that fd.  ``poll`` reports a closed/changed fd as
            # ``POLLNVAL`` (and only raises for a descriptor it cannot even
            # register), so both are treated as a bad-fd probe.
            self.isolate_bad_fds(candidates)
            return
        if not readable:
            return
        for job, name, stream in candidates:
            if stream.fd not in readable:
                continue
            self._drain_active_stream(job, name, stream, bound)

    def _drain_candidates(self) -> list[tuple[ActiveJob, str, OutputStream]]:
        """Collect the (job, stream name, stream) triples with a live capture fd.

        A spool-evicted (fail-closed) job is never a candidate: its bad spool
        is not re-read while local stop escalation owns its group.

        Returns:
            The drainable candidates.
        """
        candidates: list[tuple[ActiveJob, str, OutputStream]] = []
        for job in list(self.active.values()):
            if job.spool_evicted:
                continue
            for name in OUTPUT_STREAMS:
                stream = getattr(job, name)
                if stream.fd is None or stream.eof:
                    continue
                candidates.append((job, name, stream))
        return candidates

    @staticmethod
    def isolate_bad_fds(candidates: list[tuple[ActiveJob, str, OutputStream]]) -> None:
        """Find and fail closed exactly the jobs whose capture fd is bad.

        Args:
            candidates: The candidate ``(job, name, stream)`` triples that were
                about to be polled.
        """
        for job, name, stream in candidates:
            fd = stream.fd
            if fd is None:
                continue
            if _is_bad_fd(fd):
                LOGGER.error(
                    "bad capture fd %d for job %s stream %s; failing closed",
                    fd,
                    job.id,
                    name,
                )
                job.spool_evicted = True
                with suppress(OSError):
                    os.close(fd)
                stream.fd = None
                request_stop(job, STOP_REASON_SPOOL)

    @staticmethod
    def _drain_active_stream(job: ActiveJob, name: str, stream: OutputStream, bound: int) -> None:
        """Drain one ready capture stream, failing the job closed on error.

        The bound is an aggregate per-job limit, so this stream's room is
        computed against the job's combined on-disk spool usage across both
        streams.

        Args:
            job: The owning active job.
            name: Stream name (for diagnostics).
            stream: The stream whose pipe is ready to read.
            bound: Maximum physical spool size in bytes for the whole job.
        """
        total = 0
        for stream_name in OUTPUT_STREAMS:
            other = getattr(job, stream_name)
            try:
                total += other.path.stat().st_size
            except OSError:
                # A spool stat failure must fail the exact job closed, never be
                # assumed zero: assuming zero would under-count usage and could
                # let a half-captured job finalize with missing output.
                LOGGER.exception(
                    "capture spool stat failed for job %s stream %s; failing closed",
                    job.id,
                    stream_name,
                )
                job.spool_evicted = True
                request_stop(job, STOP_REASON_SPOOL)
                return
        status = drain_capture_stream(stream, bound, aggregate_used=total)
        if status == "error":
            LOGGER.error(
                "spool failure for job %s stream %s; failing job closed",
                job.id,
                name,
            )
            job.spool_evicted = True
            request_stop(job, STOP_REASON_SPOOL)

    def _enforce_spool_bounds(self) -> None:
        """Fail closed only jobs whose aggregate spool overshoots or stat fails.

        The worker owns the flow-control boundary: capture pipes are drained
        into the spool file only while the per-job aggregate has room, so a
        producer is backpressured before the combined stdout+stderr spool can
        exceed ``settings.output_spool_max_bytes``. This pass is a last-resort
        backstop. A spool stat failure, or an actual aggregate overshoot that
        slipped past backpressure, fails only the offending job (never the
        whole worker) so one badly-behaving job cannot starve or crash the
        others.
        """
        bound = self.settings.output_spool_max_bytes
        for job in list(self.active.values()):
            if job.completed:
                continue
            if self.spool_overflow(job, bound):
                LOGGER.error(
                    "terminating job %s: aggregate local stdout/stderr disk spool "
                    "exceeds configured safe bound of %d bytes",
                    job.id,
                    bound,
                )
                job.spool_evicted = True
                request_stop(job, STOP_REASON_SPOOL)

    @staticmethod
    def spool_overflow(job: ActiveJob, bound: int) -> bool:
        """Return whether a job's combined stdout+stderr spool exceeds the bound.

        The bound is an aggregate per-job limit, so both streams' on-disk
        capture files are summed. A stat failure on either stream fails closed.

        Args:
            job: The active job to inspect.
            bound: Maximum allowed aggregate physical spool size in bytes.

        Returns:
            ``True`` when the combined capture files exceed ``bound`` or a
            spool stat fails (fail closed).
        """
        total = 0
        for name in OUTPUT_STREAMS:
            stream = getattr(job, name)
            try:
                total += stream.path.stat().st_size
            except OSError:
                LOGGER.exception(
                    "spool stat failed for job %s stream %s; failing closed",
                    job.id,
                    name,
                )
                return True
        return total > bound

    def _outage_phase(self) -> None:
        """Handle a database outage without losing ownership of active groups.

        New claims are stopped. Local process observation and reaping continue,
        and any owned process group whose lease could expire before a heartbeat
        can be restored is terminated so no live process ever outlives a safe
        lease. The connection is retried on an interval.
        """
        self._enforce_lease_safety()
        if time.monotonic() >= self._next_reconnect_at:
            self._connect()
            if self.conn is not None:
                LOGGER.info("database connection restored")
                self._next_recovery_at = 0.0
                self._next_lease_refresh_at = 0.0
                self._next_gc_at = 0.0
            else:
                self._next_reconnect_at = time.monotonic() + max(
                    self.settings.poll_interval_seconds, 0.5
                )

    def _run_recovery(self) -> None:
        """Run the stale-job recovery pass and stop any recovered own jobs."""
        conn = self.conn
        if conn is None:
            return
        recovered = recover_stale_jobs(conn)
        for job_id, _payload in recovered:
            LOGGER.warning(
                "recovered stale job %s: lease expired; marked failed rather than re-executed",
                job_id,
            )
            job = self.active.get(job_id)
            if job is not None:
                job.row_lost = True
                request_stop(job, STOP_REASON_ROW_LOST)

    def _run_gc(self) -> None:
        """Run the transport garbage collection pass.

        Three-phase staged GC: mark terminal roots, drain their chunks in
        bounded batches, finalize root deletion, then clean orphan chunks.
        Abandoned ``running`` rows go through lease recovery first.
        ``pending`` and ``running`` rows are never collected.
        """
        conn = self.conn
        if conn is None:
            return
        roots, chunks, orphans = collect_transport(conn, self.settings)
        if roots or chunks or orphans:
            LOGGER.info(
                "gc marked %d root(s), deleted %d chunk(s), cleaned %d orphan(s)",
                len(roots),
                chunks,
                orphans,
            )

    def _heartbeat_root_ids(self) -> set[UUID]:
        """Return the root IDs whose lease is refreshed this turn.

        Only explicitly locally-owned running or completed-but-not-finalized
        jobs tracked in the active registry are eligible.  This is what prevents
        an orphaned owned row (for example a claimed job whose immediate
        finalization write failed and which is not locally tracked) from being
        heartbeated merely because another job is active.  Retry-owned jobs are
        handled by a separate terminalization-retry mechanism and are
        intentionally NOT heartbeated: their lease is left free to expire and be
        safely recovered as failed, with no uncertain re-execution.

        Returns:
            The set of root IDs to refresh.
        """
        return {
            job.id
            for job in self.active.values()
            if not job.finalized
            and not job.quarantined
            and not job.row_lost
            and not job.lease_evicted
        }

    def _refresh_leases(self) -> None:
        """Refresh only the locally-owned active leases in one bulk statement.

        The refresh origin is captured immediately before the refresh database
        operation, never at commit time, so the local lease-safety deadline is
        anchored to a conservative monotonic instant that cannot exceed the
        database lease. Only the explicitly eligible locally-owned root IDs are
        refreshed (see :meth:`_heartbeat_root_ids`), and the local deadline is
        advanced exclusively for the identifiers actually returned by a
        *committed* refresh. A refresh that fails to commit (for example a
        connectivity error that escapes to outage handling) leaves
        ``last_heartbeat_at`` untouched, so a missed heartbeat can never
        silently extend a job's safe lifetime.
        """
        conn = self.conn
        if conn is None:
            return
        refresh_mono = time.monotonic()
        eligible = self._heartbeat_root_ids()
        refreshed = set(bulk_refresh_leases(conn, self.settings, eligible))
        apply_lease_refresh(self.active, refreshed, refresh_mono)

    def _retry_terminalizations(self) -> None:
        """Retry the terminalization writes for jobs whose immediate finalization failed.

        A claimed job whose immediate finalization DB write failed is kept
        locally owned here so it remains represented for retry.  Its lease is
        intentionally NOT refreshed: the row is left free to expire and be
        safely recovered as failed (no uncertain re-execution) while this
        mechanism races to terminalize it directly.  The terminalization is
        retried
        with exponential backoff; if every attempt fails the supervisor stops
        for external recovery rather than retrying forever at process-poll rate.
        """
        conn = self.conn
        if conn is None or not self._retry_terminations:
            return
        now = time.monotonic()
        for job_id in list(self._retry_terminations):
            state = self._retry_terminations[job_id]
            if now < state.next_retry_at:
                continue
            if state.retries >= QUARANTINE_MAX_RETRIES:
                LOGGER.critical(
                    "terminalization retry for job %s failed after %d retries; "
                    "stopping supervisor for external recovery",
                    job_id,
                    state.retries,
                )
                self._stopping = True
                return
            if _quarantine_job(conn, job_id, f"retry terminalization for {job_id}"):
                self._retry_terminations.pop(job_id, None)
            else:
                state.retries += 1
                state.next_retry_at = now + QUARANTINE_RETRY_BASE_SECONDS * (2**state.retries)

    def _discover_cancellations(self) -> None:
        """Terminate any owned running job whose cancellation marker was set."""
        conn = self.conn
        if conn is None:
            return
        for job_id in discover_cancellations(conn, self.settings):
            job = self.active.get(job_id)
            if job is not None and not job.cancel_requested:
                LOGGER.info("cancelling job %s by request", job_id)
                job.cancel_requested = True
                request_stop(job, STOP_REASON_CANCEL)

    def _changed_streams(self, job: ActiveJob, now: float) -> list[str]:
        """Return stream names with changed output since last publication.

        Raises:
            SpoolCaptureError: When a stream's spool cannot be stat-ed, so the
                exact job can be failed closed instead of silently skipped.
        """
        interval = self.settings.output_publication_interval_seconds
        changed: list[str] = []
        for name in OUTPUT_STREAMS:
            stream = getattr(job, name)
            if now - stream.published_at < interval:
                continue
            try:
                size = stream.spool_start + stream.path.stat().st_size
            except OSError as exc:
                # A spool stat failure must enter the same exact-job fail-closed
                # path as any other unreadable spool, never be treated as
                # "unchanged" (which would silently skip publication of a half-
                # captured stream).
                raise SpoolCaptureError(job.id, name, stream.path) from exc
            if size != stream.published_size:
                changed.append(name)
        return changed

    def publish_job_output(self, job: ActiveJob, now: float) -> None:
        """Publish changed output for one job, quarantining deterministic errors.

        Args:
            job: The active job to publish.
            now: Monotonic time.

        Raises:
            psycopg.Error: When the error is a connectivity issue.
            OSError: When a spool trim/rewrite failure must quarantine the job.
        """
        conn = self.conn
        if conn is None:
            return
        try:
            changed = self._changed_streams(job, now)
        except SpoolCaptureError:
            # The spool is unreadable: fail the exact job closed rather than
            # publishing a partial/omitted result for only the healthy stream.
            self._fail_capture_closed(job)
            return
        if not changed:
            return
        try:
            published = publish_output(conn, job, changed, now)
        except SpoolCaptureError:
            # The spool is unreadable: fail the exact job closed rather than
            # publishing a partial/omitted result for only the healthy stream.
            self._fail_capture_closed(job)
            return
        except (psycopg.Error, OSError) as exc:
            if isinstance(exc, psycopg.Error) and self._is_connectivity_error(exc):
                raise
            sqlstate = exc.sqlstate if isinstance(exc, psycopg.Error) else "N/A"
            LOGGER.exception(
                "publishing output for job %s failed (SQLSTATE %s)",
                job.id,
                sqlstate,
            )
            if _quarantine_job(conn, job.id, f"publication error: {exc}"):
                job.quarantined = True
            else:
                job.quarantine_pending = True
            request_stop(job, STOP_REASON_QUARANTINE)
            return
        if not published:
            job.row_lost = True
            request_stop(job, STOP_REASON_ROW_LOST)

    def _publish_all(self, now: float) -> None:
        """Publish changed output tails/chunks of running jobs, throttled.

        Connectivity errors are re-raised so the main loop enters outage
        handling.  Deterministic per-job data/SQL errors are quarantined so
        the offending job does not poison publication of unrelated jobs.
        Quarantined, quarantine-pending, and spool-evicted (fail-closed) jobs
        are skipped — only the bounded quarantine retry owner may touch DB
        state until convergence, and a spool-evicted job's bad spool is never
        re-read.
        """
        if self.conn is None:
            return
        for job in list(self.active.values()):
            if (
                not job.completed
                and not job.finalized
                and not job.quarantined
                and not job.quarantine_pending
                and not job.spool_evicted
            ):
                self.publish_job_output(job, now)

    def _cleanup_quarantined_jobs(self) -> None:
        """Untrack quarantined jobs and retry quarantine-pending jobs with backoff.

        After durable quarantine the row is already terminal; once the owned
        process group is dead we clean capture files and remove the job from
        the active registry without re-entering publication/finalization.

        For quarantine-pending jobs (terminalization write previously failed)
        whose process group has exited, we retry the safe quarantine
        terminalization with exponential backoff.  After
        ``QUARANTINE_MAX_RETRIES`` exhausted attempts we log CRITICAL and
        stop the supervisor so the external crash-loop backoff owns further
        recovery — we never retry at process-poll rate forever.
        """
        conn = self.conn
        now = time.monotonic()
        for job in list(self.active.values()):
            if job.quarantined and job.completed and not group_has_members(job.pgid):
                cleanup_job(job)
                job.finalized = True
                self.active.pop(job.id, None)
                continue
            if (
                job.quarantine_pending
                and job.completed
                and not group_has_members(job.pgid)
                and conn is not None
            ):
                if now < job.quarantine_next_retry_at:
                    continue
                if job.quarantine_retries >= QUARANTINE_MAX_RETRIES:
                    LOGGER.critical(
                        "quarantine terminalization for job %s failed after %d "
                        "retries; stopping supervisor for external recovery",
                        job.id,
                        job.quarantine_retries,
                    )
                    self._stopping = True
                    return
                if _quarantine_job(conn, job.id, f"quarantine retry for {job.id}"):
                    job.quarantined = True
                    job.quarantine_pending = False
                    cleanup_job(job)
                    job.finalized = True
                    self.active.pop(job.id, None)
                else:
                    job.quarantine_retries += 1
                    delay = QUARANTINE_RETRY_BASE_SECONDS * (2**job.quarantine_retries)
                    job.quarantine_next_retry_at = now + delay

    def _try_finalize_one_completed(self, job: ActiveJob) -> None:
        """Finalize a completed job, never exceeding the spool bound.

        Drains only up to the aggregate per-job disk bound; when a stream is
        still non-end-of-file only because the spool is full, durably publishes
        the captured bytes and trims the head (after the DB commit) to free
        bounded room, then drains more. Repeats until both streams reach EOF,
        then performs the final publication and finalizes. Capture FDs are never
        closed before EOF and the physical spool never exceeds the configured
        bound.

        Args:
            job: The completed active job.

        Raises:
            psycopg.Error: When a database error is a connectivity issue.
        """
        try:
            self._finalize_completed_job_bounded(job)
        except psycopg.Error as exc:
            if self._is_connectivity_error(exc):
                raise
            # A deterministic per-job error was already quarantined inside the
            # bounded finalizer; nothing further to do here.
            LOGGER.exception("deterministic failure finalizing job %s", job.id)

    def _fail_capture_closed(self, job: ActiveJob) -> None:
        """Fail an exact job closed as a capture failure, without re-reading it.

        Used when an active job's spool is unreadable (stat/read/disappearance
        failure) during draining, planning, or final publication. The spool is
        not re-read; the job is marked failed using the in-memory tail state
        already published, its local resources are released, and it is removed
        from the active registry so finalization never loops on the bad spool.

        A job whose process group has not yet been proven fully terminated is
        never terminalized or untracked here: exact local ownership is retained
        (SIGTERM, then SIGKILL after the grace period) until observation proves
        every group member is gone; only then does the bounded finalizer write
        the fail-closed terminal row without re-reading the bad spool.

        Args:
            job: The active job whose capture spool is unavailable.

        Raises:
            psycopg.Error: When the error is a connectivity issue during the
                terminal finalization write.
        """
        job.spool_evicted = True
        request_stop(job, STOP_REASON_SPOOL)
        if not (job.completed and not group_has_members(job.pgid)):
            # Still running (or leftover group members alive): keep ownership
            # until the stop escalation proves the exact group is gone.
            return
        conn = self.conn
        if conn is None:
            cleanup_job(job)
            job.finalized = True
            self.active.pop(job.id, None)
            return
        try:
            finalized = self._finalize_one(job)
        except psycopg.Error as exc:
            if self._is_connectivity_error(exc):
                raise
            if _quarantine_job(conn, job.id, f"capture failure finalize error: {exc}"):
                job.quarantined = True
            else:
                job.quarantine_pending = True
            request_stop(job, STOP_REASON_QUARANTINE)
            return
        if finalized:
            # _finalize_one already cleaned up the job; record the flag and
            # drop it from the registry so nothing re-enters finalization.
            job.finalized = True
            self.active.pop(job.id, None)
            return
        cleanup_job(job)
        job.finalized = True
        self.active.pop(job.id, None)

    @staticmethod
    def _drain_completed_streams(job: ActiveJob, bound: int) -> bool:
        """Drain each non-EOF stream up to the bound; report whether any progressed.

        Drains only while the aggregate per-job disk bound has room, so the
        physical spool is never exceeded. A spool stat failure fails the exact
        job closed rather than assuming zero.

        Args:
            job: The completed active job whose streams to drain.
            bound: Maximum physical spool size in bytes (always enforced).

        Returns:
            ``True`` when at least one stream was drained or reached EOF this
            turn, ``False`` when no stream made progress (the spool is full and
            every non-EOF stream is stalled, so the caller should publish/trim
            to free bounded room).

        Raises:
            SpoolCaptureError: When an active stream's capture spool is
                unreadable, so the exact job can be failed closed.
        """
        made_progress = False
        for name in OUTPUT_STREAMS:
            stream = getattr(job, name)
            if stream.fd is None or stream.eof:
                continue
            total = 0
            for stream_name in OUTPUT_STREAMS:
                other = getattr(job, stream_name)
                try:
                    total += other.path.stat().st_size
                except OSError as exc:
                    # A spool stat failure must fail the exact job closed, never
                    # be assumed zero (which would risk finalizing a half-
                    # captured job).
                    raise SpoolCaptureError(job.id, stream_name, other.path) from exc
            status = drain_capture_stream(stream, bound, aggregate_used=total)
            if status == "error":
                job.spool_evicted = True
                request_stop(job, STOP_REASON_SPOOL)
                return False
            if status != "full":
                made_progress = True
        return made_progress

    def _publish_bounded(self, conn: JobsConnection, job: ActiveJob, now: float) -> str:
        """Publish a completed job's bounded output and classify the outcome.

        The publication is forced (ignoring the throttle interval) so the bounded
        finalization cycle can free spool room whenever a stream is still
        non-end-of-file only because the spool is full. A capture spool failure
        is reported as ``"capture"`` so the caller fails the exact job closed; a
        deterministic per-job publication error is quarantined and reported as
        ``"quarantine"``; a lost root row is reported as ``"lost"``.

        Args:
            conn: Open PostgreSQL connection.
            job: The completed active job whose output to publish.
            now: Monotonic time of this publication pass.

        Returns:
            One of ``"ok"``, ``"lost"``, ``"capture"``, or ``"quarantine"``.

        Raises:
            psycopg.Error: When a database error is a connectivity issue.
            OSError: When a local capture/file error escapes publication.
        """
        try:
            published = publish_output(conn, job, list(OUTPUT_STREAMS), now, force=True)
        except SpoolCaptureError:
            return "capture"
        except (psycopg.Error, OSError) as exc:
            if isinstance(exc, psycopg.Error) and self._is_connectivity_error(exc):
                raise
            if _quarantine_job(conn, job.id, f"bounded publication error: {exc}"):
                job.quarantined = True
            else:
                job.quarantine_pending = True
            request_stop(job, STOP_REASON_QUARANTINE)
            return "quarantine"
        if not published:
            self._untrack_lost_job(job)
            return "lost"
        return "ok"

    def _publish_or_fail(self, conn: JobsConnection, job: ActiveJob, now: float) -> bool:
        """Publish bounded output and fail the job closed on an unrecoverable outcome.

        A ``"capture"`` outcome (an unreadable spool) fails the exact job closed;
        a ``"quarantine"`` or ``"lost"`` outcome (a deterministic per-job error or
        a vanished root row) stops finalization so the job is handled by its
        quarantine/loss path. Only ``"ok"`` lets the caller continue draining or
        finalize the job.

        Args:
            conn: Open PostgreSQL connection.
            job: The completed active job whose output to publish.
            now: Monotonic time of this publication pass.

        Returns:
            ``True`` when publication succeeded and finalization may continue,
            ``False`` when the job was failed closed, quarantined, or lost.
        """
        outcome = self._publish_bounded(conn, job, now)
        if outcome == "capture":
            self._fail_capture_closed(job)
            return False
        return outcome == "ok"

    def _finalize_completed_job_bounded(self, job: ActiveJob) -> None:
        """Bounded publish/trim/drain finalization for one completed job.

        The physical spool bound is always enforced. The cycle drains each
        non-EOF stream only up to the aggregate per-job disk bound; when a
        stream is still non-EOF solely because the spool is full, it durably
        publishes the captured bytes and trims the head (after the DB commit)
        to free bounded room, then drains more. This repeats until both streams
        reach end-of-file, after which the final publication finalizes the job.

        Capture FDs are never closed before EOF, so every pipe-buffered byte is
        represented. If the bounded cycle cannot reach EOF (for example the
        database cannot make room), the exact job is failed closed rather than
        growing the disk or silently discarding output.

        Args:
            job: The completed active job.
        """
        conn = self.conn
        if conn is None:
            return
        if job.spool_evicted:
            # The spool was already proven unreadable/over-bound while the job
            # ran; fail closed from the in-memory tail state without ever
            # re-reading the bad spool.
            if self._finalize_one(job):
                job.finalized = True
                self.active.pop(job.id, None)
            return
        bound = self.settings.output_spool_max_bytes
        if not self._bounded_finalization_cycle(job, conn, bound):
            return
        if not self._publish_or_fail(conn, job, time.monotonic()):
            return
        if self._finalize_one(job):
            job.finalized = True
            self.active.pop(job.id, None)

    def _bounded_finalization_cycle(self, job: ActiveJob, conn: JobsConnection, bound: int) -> bool:
        """Alternate bounded draining and durable publish+trim until both streams hit EOF.

        A full/no-progress drain never short-circuits publication: whenever a
        stream remains non-EOF only because the spool is at its bound, this
        cycle forces a durable publish and trims the archived head to free
        room, then continues draining to end-of-file. Only a genuine capture,
        quarantine, loss, or provable no-room outcome stops the cycle.

        Args:
            job: The completed active job whose output is being finalized.
            conn: Open PostgreSQL connection.
            bound: Maximum physical spool size in bytes (always enforced).

        Returns:
            ``True`` when both streams reached EOF and finalization may proceed;
            ``False`` when the exact job was failed closed or handed to its
            quarantine/loss path and the caller must stop.
        """
        now = time.monotonic()
        for _ in range(100_000):
            if _streams_at_eof(job):
                return True
            progressed = self._drain_completed_streams(job, bound)
            used_before = _spool_used_bytes(job)
            outcome = self._publish_bounded(conn, job, now)
            if outcome == "capture":
                self._fail_capture_closed(job)
                return False
            if outcome != "ok":
                # Quarantined or lost: the job is owned by its quarantine/loss
                # path, so finalization must not continue here.
                return False
            made_room = progressed or _spool_shrank(job, used_before)
            if not _streams_at_eof(job) and not made_room:
                # A stream is still non-EOF, the spool is at its bound, and a
                # durable publish+trim freed no room: the bounded cycle cannot
                # make progress. Fail the exact job closed instead of spinning
                # or growing the disk.
                self._fail_capture_closed(job)
                return False
        # Could not reach EOF within the bounded cycle; fail the exact job
        # closed instead of growing the disk or discarding output.
        self._fail_capture_closed(job)
        return False

    def _finalize_completed(self) -> None:
        """Publish final output and finalize every job whose process is fully gone.

        Connectivity errors are re-raised so the main loop enters outage
        handling.  Deterministic per-job data/SQL errors are quarantined so
        the offending job does not poison finalization of unrelated jobs.
        Quarantined and quarantine-pending jobs are excluded from normal
        publication/finalization — only the bounded quarantine retry owner
        may touch DB state until convergence.
        """
        conn = self.conn
        if conn is None:
            return
        self._cleanup_quarantined_jobs()
        for job in list(self.active.values()):
            if not (job.completed and not job.finalized):
                continue
            if job.quarantined or job.quarantine_pending:
                continue
            if group_has_members(job.pgid):
                continue
            self._try_finalize_one_completed(job)

    def _untrack_lost_job(self, job: ActiveJob) -> None:
        """Untrack a completed job whose root row was deleted concurrently.

        The concurrent deletion already removed the root and every owned chunk
        in one transaction, so there is nothing left to persist; only the local
        capture files remain to clean up.

        Args:
            job: The completed active job to untrack.
        """
        cleanup_job(job)
        job.finalized = True
        self.active.pop(job.id, None)

    def _finalize_one(self, job: ActiveJob) -> bool:
        """Persist the terminal state of one completed job.

        Connectivity errors are re-raised so the main loop enters outage
        handling.  Deterministic per-job data/SQL errors are quarantined so
        the offending job does not block finalization of unrelated jobs.
        ``last_completed_*`` health fields are only updated after the
        terminal DB finalization succeeds, so a failed persist never poisons
        the health snapshot.

        Args:
            job: The completed active job.

        Returns:
            ``True`` when the job was finalized and its capture files removed.

        Raises:
            psycopg.Error: When the error is a connectivity issue.
        """
        conn = self.conn
        if conn is None:
            return False
        result = JobResult(
            status=_finalize_status(job),
            exit_code=job.returncode if job.returncode is not None else 0,
            stdout=job.stdout.tail_text,
            stderr=job.stderr.tail_text,
            cancellation_note=job.cancellation_note,
        )
        try:
            final_status = finish_job(conn, job.id, result)
        except psycopg.Error as exc:
            if self._is_connectivity_error(exc):
                raise
            LOGGER.exception(
                "finalizing job %s failed (SQLSTATE %s)",
                job.id,
                exc.sqlstate or "N/A",
            )
            if _quarantine_job(conn, job.id, f"finalization error: {exc}"):
                job.quarantined = True
            else:
                job.quarantine_pending = True
            request_stop(job, STOP_REASON_QUARANTINE)
            return False
        duration_seconds = time.monotonic() - job.started_mono
        LOGGER.info(
            "finished job %s with status %s exit_code=%d duration=%.3fs",
            job.id,
            final_status,
            result.exit_code,
            duration_seconds,
        )
        self._last_completed_job_id = str(job.id)
        self._last_completed_at = time.time()
        self._last_completed_status = final_status
        cleanup_job(job)
        self._publish_health_force()
        return True

    def _claim_batch(self) -> None:
        """Claim a bounded batch of pending jobs and start their processes.

        The batch size is a fairness bound on the amount of claiming work done
        in one turn; it is never a cap on the number of simultaneously active
        jobs.

        The claim timestamp is captured immediately before the claim database
        operation commits and threaded into every started job so the local
        lease-safety deadline is bound to the committed lease grant, never to
        the (possibly delayed) spawn. The claim-to-spawn delay therefore
        consumes lease budget instead of extending it.
        """
        conn = self.conn
        if conn is None or self._stopping:
            return
        claim_mono = time.monotonic()
        claimed = claim_jobs(conn, self.settings, self.settings.claim_batch_limit)
        for claimed_job in claimed:
            self._start_job(claimed_job, claim_mono)

    def _start_job(self, claimed: ClaimedJob, claim_mono: float) -> None:
        """Start one claimed job as a process group and register it.

        If the process cannot be started (for example because the operating
        system refused to spawn it), the job fails clearly and independently
        while the daemon stays alive to supervise the jobs that did start.

        The local lease-safety deadline is anchored to ``claim_mono``: the
        monotonic time captured immediately before the claim database operation
        commits (never at commit time). Anchoring to the claim (rather than to
        the spawn that follows it) means any delay between claiming and spawning
        consumes lease budget and never creates extra lease time a recovery
        worker could legitimately treat as live.

        Args:
            claimed: The claimed job.
            claim_mono: Monotonic time captured immediately before the claim
                database operation commits (never at commit time).
        """
        conn = self.conn
        if conn is None:
            return
        try:
            payload = parse_payload(claimed.payload)
        except ProtocolError as exc:
            LOGGER.warning("rejecting unparseable job %s: %s", claimed.id, exc)
            self._finalize_immediate(
                claimed.id,
                JobResult(
                    status="failed",
                    exit_code=PROTOCOL_ERROR_EXIT_CODE,
                    stdout="",
                    stderr=f"invalid job payload: {exc}",
                    cancellation_note=None,
                ),
            )
            return

        job_spec = Job(
            id=claimed.id,
            cwd=payload.request.cwd,
            process=payload.request.process,
        )
        failure = _preflight_failure(job_spec)
        if failure is not None:
            self._finalize_immediate(claimed.id, failure)
            return
        try:
            proc, stdout_path, stderr_path, pgid, gate_fd, stdout_r, stderr_r = spawn_job(job_spec)
        except OSError as exc:
            LOGGER.warning("unable to start job %s: %s", claimed.id, exc)
            self._finalize_immediate(
                claimed.id,
                JobResult(
                    status="failed",
                    exit_code=EXECUTION_ERROR_EXIT_CODE,
                    stdout="",
                    stderr=f"unable to execute job: {exc}",
                    cancellation_note=None,
                ),
            )
            return
        gated = GatedSpawn(
            proc=proc,
            pgid=pgid,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
            gate_fd=gate_fd,
            stdout_read_fd=stdout_r,
            stderr_read_fd=stderr_r,
        )

        # Fail closed BEFORE any release and activate only on success.
        job = self._activate_gated_job(
            conn,
            claimed.id,
            job_spec,
            gated,
            claim_mono=claim_mono,
        )
        if job is None:
            # The gated start was aborted and finalized; close the capture fds
            # and drop the spool files so nothing leaks.
            for capture_fd in (stdout_r, stderr_r):
                with suppress(OSError):
                    os.close(capture_fd)
            _cleanup_output_files(stdout_path, stderr_path)
            return
        job.stdout = OutputStream(path=stdout_path, fd=stdout_r)
        job.stderr = OutputStream(path=stderr_path, fd=stderr_r)
        job.last_heartbeat_at = claim_mono
        self.active[claimed.id] = job
        LOGGER.info("claimed job %s (pid %d)", job.id, job.pid)
        self._publish_health_force()

    @staticmethod
    def _abort_and_converge(gated: GatedSpawn, job_id: UUID) -> None:
        """Abort the gated start and block until its wrapper is reaped.

        A nonconverging ``abort_gated_start`` must NEVER fall back to normal
        flow with a live untracked gated group: this worker locally owns the
        still-gated childless direct child and synchronously blocks — with no
        timeout return — until it is terminal and reaped, at which point the
        original unreleased group is gone by construction.

        Args:
            gated: Handles and exact identity of the gated start.
            job_id: Identifier of the affected job (for diagnostics).
        """
        if abort_gated_start(
            gated.proc, gated.pgid, gated.stdout_path, gated.stderr_path, gated.gate_fd
        ):
            # Converged: release the capture pipe read ends so no descriptor
            # leaks from the aborted start.
            for capture_fd in (gated.stdout_read_fd, gated.stderr_read_fd):
                if capture_fd is not None:
                    with suppress(OSError):
                        os.close(capture_fd)
            return
        LOGGER.error(
            "gated start abort did not converge for job %s (exact group %d); "
            "locally owning the live child until it is reaped",
            job_id,
            gated.pgid,
        )
        await_gated_group_gone(gated)

    def _pre_release_failure(
        self,
        conn: JobsConnection,
        job_id: UUID,
        gated: GatedSpawn,
    ) -> str | None:
        """Obtain and durably persist the exact identity before any release.

        Fail-closed ordering: valid positive start-time ticks must be obtained
        and durably persisted BEFORE any release. On any failure the exact
        gated wrapper is converged (terminal and reaped) first — including on
        the connectivity re-raise path — so no error path can leave a live
        untracked gated group behind and no failed persistence can ever reach
        :func:`release_gate`.

        Args:
            conn: Open PostgreSQL connection.
            job_id: Identifier of the claimed job.
            gated: Handles and exact identity of the gated start.

        Returns:
            The final stderr message when the start failed after convergence
            (the caller must finalize failed), or ``None`` when persistence
            succeeded.

        Raises:
            psycopg.Error: When persisting the identity fails with a
                connectivity error (raised only after exact-group convergence).
        """
        start_ticks = proc_start_ticks(gated.proc.pid)
        if start_ticks is not None and start_ticks > 0:
            try:
                _persist_process(conn, job_id, gated.proc.pid, gated.pgid, start_ticks)
            except psycopg.Error as exc:
                connectivity = self._is_connectivity_error(exc)
                self._abort_and_converge(gated, job_id)
                if connectivity:
                    raise
                LOGGER.exception(
                    "unable to persist process identity for job %s (SQLSTATE %s)",
                    job_id,
                    exc.sqlstate or "N/A",
                )
                return "unable to record process identity; job not started"
            return None
        # No durable exact identity exists, so this worker itself must own the
        # childless gated child to convergence before anything else.
        self._abort_and_converge(gated, job_id)
        LOGGER.error(
            "unable to obtain exact start-time ticks for job %s (pid %d); "
            "gated start aborted without executing user code",
            job_id,
            gated.proc.pid,
        )
        return "unable to record exact process identity; job not started"

    def _activate_gated_job(
        self,
        conn: JobsConnection,
        job_id: UUID,
        job_spec: Job,
        gated: GatedSpawn,
        *,
        claim_mono: float,
    ) -> ActiveJob | None:
        """Persist the exact identity, release the gate, and build the active job.

        Fail-closed ordering: valid positive start-time ticks must be obtained
        and durably persisted BEFORE any release; a persist or release failure
        converges the exact gated group and finalizes the job failed so no user
        side effect survives and a failed start can never later look like a
        normally completed user command.

        Args:
            conn: Open PostgreSQL connection.
            job_id: Identifier of the claimed job.
            job_spec: The claimed job specification.
            gated: Handles and exact identity of the gated start to activate.
            claim_mono: Monotonic claim instant carried onto the active job.

        Returns:
            The :class:`ActiveJob` for normal supervision, or ``None`` when the
            start failed and was finalized as such. Every failure path first
            converges the exact gated wrapper (terminal and reaped), so no
            untracked live group ever outlives this call.
        """
        failure = self._pre_release_failure(conn, job_id, gated)
        if failure is None:
            # The exact identity is durably recorded. Release the gate so the
            # wrapper execs the user argv on the exact same PID, after which
            # normal supervision applies. A failed release is a start failure:
            # converge the live child first, then finalize failed.
            if release_gate(gated.gate_fd):
                return ActiveJob(
                    id=job_id,
                    cwd=job_spec.cwd,
                    process=job_spec.process,
                    proc=gated.proc,
                    pid=gated.proc.pid,
                    pgid=gated.pgid,
                    started_mono=time.monotonic(),
                    claimed_at=claim_mono,
                )
            self._abort_and_converge(gated, job_id)
            LOGGER.error(
                "failed to release the start gate for job %s (pid %d); "
                "gated start aborted without executing user code",
                job_id,
                gated.proc.pid,
            )
            failure = "unable to release gated start; job not started"
        self._finalize_immediate(
            job_id,
            JobResult(
                status="failed",
                exit_code=EXECUTION_ERROR_EXIT_CODE,
                stdout="",
                stderr=failure,
                cancellation_note=None,
            ),
        )
        return None

    def _finalize_immediate(self, job_id: UUID, result: JobResult) -> None:
        """Finalize a job that failed before or during spawning.

        Connectivity errors are re-raised so the main loop enters outage
        handling.  Deterministic per-job errors attempt quarantine
        terminalization directly so the row does not remain non-terminal
        indefinitely.

        Args:
            job_id: The job identifier.
            result: The failure result.

        Raises:
            psycopg.Error: When the error is a connectivity issue.
        """
        conn = self.conn
        if conn is None:
            return
        try:
            finish_job(conn, job_id, result)
        except psycopg.Error as exc:
            if self._is_connectivity_error(exc):
                raise
            LOGGER.exception(
                "unable to finalize job %s (SQLSTATE %s)",
                job_id,
                exc.sqlstate or "N/A",
            )
            if not _quarantine_job(conn, job_id, f"immediate finalization error: {exc}"):
                # Both the finalization and the quarantine terminalization writes
                # failed: keep the claimed job locally owned for retry so it stays
                # represented.  Its lease is intentionally NOT refreshed, so it is
                # never heartbeated merely because another job is active, and it
                # is free to expire and be safely recovered as failed rather than
                # being silently orphaned.
                self._retry_terminations.setdefault(job_id, _RetryTerminalization())

    def _enforce_lease_safety(self) -> None:
        """Terminate owned groups whose lease can no longer be refreshed in time.

        The local lease deadline is derived from the last successful committed
        heartbeat (anchored to the conservative monotonic origin captured
        before that heartbeat's database operation) so the process group is
        terminated before its database lease can expire and another worker could
        legitimately treat the job as abandoned.
        """
        settings = self.settings
        now = time.monotonic()
        for job in list(self.active.values()):
            if job.completed or job.term_sent:
                continue
            unsafe_at = (
                job.last_heartbeat_at
                + settings.lease_duration_seconds
                - settings.lease_safety_margin_seconds
            )
            if now >= unsafe_at:
                LOGGER.error(
                    "terminating job %s: database unreachable and its lease cannot "
                    "be refreshed safely",
                    job.id,
                )
                job.lease_evicted = True
                request_stop(job, STOP_REASON_LEASE)

    # ------------------------------------------------------------------
    # Health publishing
    # ------------------------------------------------------------------

    def _build_health(self, *, alive: bool = True, shutting_down: bool = False) -> WorkerHealth:
        """Build a health snapshot from the current supervisor state.

        Args:
            alive: Whether the worker is alive.
            shutting_down: Whether the worker is shutting down.

        Returns:
            A fresh health snapshot.
        """
        current_job_id: str | None = None
        current_job_started_at: float | None = None
        if self.active:
            first_job = next(iter(self.active.values()))
            current_job_id = str(first_job.id)
            current_job_started_at = first_job.claimed_at
        return WorkerHealth(
            schema_version=1,
            worker_id=self.settings.worker_id,
            worker_incarnation=self.settings.worker_incarnation,
            pid=os.getpid(),
            start_time_ticks=self._start_time_ticks,
            started_at=self._started_at,
            published_at=time.time(),
            alive=alive,
            db_connected=self.conn is not None,
            db_connected_at=self._db_connected_at,
            db_error_at=self._db_error_at,
            current_job_id=current_job_id,
            current_job_started_at=current_job_started_at,
            last_completed_job_id=self._last_completed_job_id,
            last_completed_at=self._last_completed_at,
            last_completed_status=self._last_completed_status,
            shutting_down=shutting_down,
        )

    def _publish_health(self, *, force: bool = False) -> None:
        """Write an atomic health snapshot, throttled unless forced.

        Args:
            force: Publish regardless of the throttle interval.
        """
        now = time.time()
        if not force and now < self._next_health_publish_at:
            return
        health = self._build_health()
        try:
            write_worker_health(health)
        except OSError:
            LOGGER.debug("failed to write health snapshot", exc_info=True)
        interval = self.settings.health_publish_interval_seconds
        self._next_health_publish_at = now + interval
        self._health_force = False

    def _publish_health_force(self) -> None:
        """Publish a health snapshot immediately."""
        self._health_force = True
        self._publish_health(force=True)

    def _connect(self) -> None:
        """Open and verify the supervisor's single database connection.

        Raises:
            SchemaInvariantError: If the transport table violates the two-column
                invariant.
        """
        try:
            conn = psycopg.connect(
                self.database.conninfo(),
                connect_timeout=max(1, min(5, int(self.settings.db_operation_timeout_seconds))),
                row_factory=tuple_row,
                options=(
                    f"-c statement_timeout={int(self.settings.db_operation_timeout_seconds * 1000)}"
                ),
            )
        except psycopg.Error:
            LOGGER.exception("database connection failed")
            self.conn = None
            self._db_error_at = time.time()
            self._publish_health_force()
            return
        try:
            verify_jobs_table_invariant(conn)
            verify_protocol_schema(conn)
        except SchemaInvariantError:
            LOGGER.exception(
                "refusing to run against a table that is not a migrated protocol v3 schema"
            )
            with suppress(Exception):
                conn.close()
            raise
        self.conn = conn
        self._db_connected_at = time.time()
        LOGGER.info("database connection established")
        self._publish_health_force()

    def _is_connectivity_error(self, exc: psycopg.Error) -> bool:
        """Classify a database error as a connectivity issue.

        Delegates to :func:`_is_connectivity_error_check` with the current
        connection.  See that function for the full classification rules.

        Returns:
            ``True`` when the error indicates a lost/unusable connection.
        """
        return _is_connectivity_error_check(exc, self.conn)

    def _enter_outage(self) -> None:
        """Transition into database outage handling, discarding the connection.

        The in-memory active registry is kept so local process ownership is
        never lost.
        """
        LOGGER.error("database operation failed; entering outage handling")
        if self.conn is not None:
            with suppress(Exception):
                self.conn.close()
        self.conn = None
        self._db_error_at = time.time()
        self._next_reconnect_at = 0.0
        self._publish_health_force()

    def _shutdown(self) -> None:
        """Gracefully terminate, reap, and finalize every tracked process group.

        Stops claiming, requests termination of every active group, reaps the
        children, escalates to ``SIGKILL`` after the bounded grace period where
        necessary, finalizes the affected jobs when PostgreSQL is available, and
        removes the temporary capture files.
        """
        LOGGER.info("shutting down: terminating %d active job(s)", len(self.active))
        for job in list(self.active.values()):
            if not job.completed and not job.term_sent:
                request_stop(job, STOP_REASON_SHUTDOWN)
        if not self._drain_active_groups():
            # Positive post-SIGKILL proof failed for at least one exact active
            # group. This is NOT a clean drain: never emit the sentinel, never
            # terminalize/untrack those jobs (their running rows keep the exact
            # persisted identity recoverable by emergency recovery), and fail
            # loudly in the log so the outer authority holds instead of reaping.
            surviving = [job.pgid for job in self.active.values() if group_has_members(job.pgid)]
            LOGGER.error(
                "shutdown cannot prove groups %s member-free; withholding the "
                "drain sentinel and retaining their jobs for exact-identity "
                "recovery",
                surviving,
            )
            self._finalize_all_for_shutdown(retain_groups=surviving)
        else:
            try:
                write_drain_sentinel(self.settings.worker_incarnation)
            except OSError:
                LOGGER.debug("could not write drain sentinel", exc_info=True)
            self._finalize_all_for_shutdown()
        self._cleanup_all_files()
        if self.conn is not None:
            with suppress(Exception):
                self.conn.close()
            self.conn = None
        health = self._build_health(alive=False, shutting_down=True)
        try:
            write_worker_health(health)
        except OSError:
            LOGGER.debug("failed to write shutdown health snapshot", exc_info=True)

    def _drain_active_groups(self) -> bool:
        """Wait for every active process group to exit, escalating to SIGKILL.

        Capture pipes are drained each iteration so every job's final output is
        captured into its spool file before finalization publishes it.

        Returns:
            ``True`` only when every active job's exact group is positively
            proven member-free after the escalation; ``False`` when any exact
            group still has members after the final SIGKILL pass, so no clean
            drain may be claimed.
        """
        deadline = time.monotonic() + self.settings.cancel_grace_seconds
        while time.monotonic() < deadline:
            self._drain_captures()
            all_gone = all(
                self._observe_and_escalate(job, time.monotonic()) for job in self.active.values()
            )
            if all_gone:
                break
            time.sleep(0.05)
        self._drain_captures()
        for job in self.active.values():
            if not self._observe_and_escalate(job, time.monotonic()):
                signal_kill(job)
        for job in self.active.values():
            with suppress(Exception):
                job.proc.wait(timeout=self.settings.cancel_grace_seconds)
        self._drain_captures()
        return all(not group_has_members(job.pgid) for job in self.active.values())

    def _observe_and_escalate(self, job: ActiveJob, now: float) -> bool:
        """Poll one child, escalate its in-flight stop, and report whether it is gone.

        Polling reaps the child so concurrent execution never leaves zombies. A
        ``SIGTERM`` that already went out escalates to ``SIGKILL`` for the exact
        process group after the bounded grace period while any member remains.
        When the root process exits while background members of its exact
        process group are still alive, a group reap is started so the job is
        not finalized (and untracked) until the whole exact group is gone.

        Args:
            job: The active job to observe.
            now: Monotonic time.

        Returns:
            ``True`` when the leader has exited and no group member remains.
        """
        if not job.completed:
            returncode = job.proc.poll()
            if returncode is not None:
                job.completed = True
                job.returncode = returncode
        if job.completed and not job.term_sent and group_has_members(job.pgid):
            LOGGER.info(
                "reaping leftover process group %d of completed job %s",
                job.pgid,
                job.id,
            )
            request_group_reap(job)
        if job.term_sent and not job.kill_sent and job.stop_started is not None:
            grace_elapsed = now - job.stop_started >= self.settings.cancel_grace_seconds
            leader_alive = job.proc.poll() is None
            if grace_elapsed and (leader_alive or group_has_members(job.pgid)):
                signal_kill(job)
        return job.completed and not group_has_members(job.pgid)

    def _finalize_all_for_shutdown(self, *, retain_groups: list[int] | None = None) -> None:
        """Finalize every tracked job when PostgreSQL is available.

        Connectivity errors are re-raised so the caller can handle outage
        before continuing shutdown.  Deterministic per-job errors are logged
        and the job is quarantined, preserving lease/row safety. Jobs whose
        exact group could not be proven member-free (``retain_groups``) are
        retained in the active set and their rows stay recoverable: they are
        never terminalized or untracked here.

        Args:
            retain_groups: Exact group ids that failed post-SIGKILL proof;
                their jobs are retained instead of finalized.

        Raises:
            psycopg.Error: When the error is a connectivity issue.
        """
        if self.conn is None:
            return
        retained_ids = set(retain_groups or [])
        for job in list(self.active.values()):
            if job.finalized or job.pgid in retained_ids:
                continue
            if not job.completed and job.stop_reason is None:
                job.stop_reason = STOP_REASON_SHUTDOWN
                job.cancellation_note = _stop_note(STOP_REASON_SHUTDOWN)
            # Bounded publish/trim/drain cycle: capture every pipe byte into the
            # spool (never past the configured bound, never closing capture FDs
            # before EOF) and then finalize. This guarantees a controlled shutdown
            # represents the full output of a terminating job without growing the
            # disk or silently discarding.
            try:
                self._finalize_completed_job_bounded(job)
            except psycopg.Error as exc:
                if self._is_connectivity_error(exc):
                    raise
                # Deterministic per-job error already quarantined inside the
                # bounded finalizer.
                LOGGER.exception("deterministic failure finalizing job %s", job.id)

    def _cleanup_all_files(self) -> None:
        """Remove every remaining temporary capture file."""
        for job in self.active.values():
            cleanup_job(job)


def _preflight_failure(job: Job) -> JobResult | None:
    """Return the immediate failure result for a job that cannot be spawned.

    Args:
        job: The claimed job.

    Returns:
        A failure result, or ``None`` when the job may be spawned.
    """
    if not Path(job.cwd).is_dir():
        return JobResult(
            status="failed",
            exit_code=EXECUTION_ERROR_EXIT_CODE,
            stdout="",
            stderr=f"unable to enter working directory {job.cwd!r}: directory does not exist",
            cancellation_note=None,
        )
    return None


def _finalize_status(job: ActiveJob) -> str:
    """Derive the terminal status of a completed job.

    Args:
        job: The completed active job.

    Returns:
        The terminal status.
    """
    if job.stop_reason in {STOP_REASON_CANCEL, STOP_REASON_SHUTDOWN}:
        return "cancelled"
    if job.stop_reason in {
        STOP_REASON_LEASE,
        STOP_REASON_ROW_LOST,
        STOP_REASON_PERSIST,
        STOP_REASON_QUARANTINE,
        STOP_REASON_SPOOL,
    }:
        return "failed"
    if job.cancel_requested:
        return "cancelled"
    return "succeeded" if job.returncode == 0 else "failed"


def request_stop(job: ActiveJob, reason: str) -> None:
    """Send ``SIGTERM`` to an exact process group once, recording the reason.

    Args:
        job: The active job to stop.
        reason: Why the job is being stopped.
    """
    if job.term_sent:
        return
    job.term_sent = True
    job.stop_started = time.monotonic()
    job.stop_reason = reason
    job.cancellation_note = _stop_note(reason)
    _signal_group(job.pgid, signal.SIGTERM)


def request_group_reap(job: ActiveJob) -> None:
    """Terminate leftover members of the exact group of a naturally completed job.

    The root process has already exited and produced its own exit status, but
    background members of the same exact process group are still alive (for
    example ``sleep 4 & echo done``). The job is not finalized until the group
    is fully gone, and its natural exit status is preserved: no cancellation
    reason is recorded.

    Args:
        job: The active job whose leftover group members must be terminated.
    """
    if job.term_sent:
        return
    job.term_sent = True
    job.stop_started = time.monotonic()
    _signal_group(job.pgid, signal.SIGTERM)


def signal_kill(job: ActiveJob) -> None:
    """Send ``SIGKILL`` to an exact process group after the grace period.

    Args:
        job: The active job whose group still has members.
    """
    job.kill_sent = True
    _signal_group(job.pgid, signal.SIGKILL)
    if job.cancellation_note is not None:
        job.cancellation_note = f"{job.cancellation_note}; grace period expired, sent SIGKILL"


def cleanup_job(job: ActiveJob) -> None:
    """Remove the temporary capture files and close pipe ends of a finalized job.

    Args:
        job: The finalized active job.
    """
    for stream in (job.stdout, job.stderr):
        if stream.fd is not None:
            with suppress(OSError):
                os.close(stream.fd)
            stream.fd = None
    job.stdout.path.unlink(missing_ok=True)
    job.stderr.path.unlink(missing_ok=True)


def _stop_note(reason: str) -> str:
    """Build the base cancellation diagnostic for a stop reason.

    Args:
        reason: Why the job is being stopped.

    Returns:
        The human-readable note.
    """
    notes = {
        STOP_REASON_CANCEL: "cancelled by request: sent SIGTERM to process group",
        STOP_REASON_SHUTDOWN: "cancelled: worker shutting down; sent SIGTERM to process group",
        STOP_REASON_LEASE: (
            "terminated because its lease could not be refreshed during a database outage"
        ),
        STOP_REASON_ROW_LOST: (
            "terminated because its job row was recovered while the daemon was "
            "unable to reach the database"
        ),
        STOP_REASON_PERSIST: "terminated because its process identity could not be recorded",
        STOP_REASON_SPOOL: (
            "terminated because its local stdout/stderr disk spool was "
            "unavailable or exceeded the configured safe bound"
        ),
    }
    return notes.get(reason, "stopped")


def main(argv: list[str] | None = None) -> int:
    """Run the Lubko worker supervisor.

    Args:
        argv: Command line arguments, or ``None`` to use ``sys.argv``.

    Returns:
        A process exit code.

    Raises:
        SystemExit: If the database configuration file cannot be loaded or the
            runtime settings are invalid.
    """
    parser = argparse.ArgumentParser(
        prog="lubko-worker",
        description=(
            "Poll PostgreSQL for jobs, execute them concurrently, and supervise them safely."
        ),
    )
    parser.add_argument(
        "--health",
        action="store_true",
        help="print the current worker health snapshot and exit",
    )
    args = parser.parse_args(argv)

    if args.health:
        snapshot = read_worker_health()
        if snapshot is None:
            sys.stdout.write("worker: no health snapshot\n")
            return 1
        effective = interpret_worker_health(snapshot)
        output = snapshot.to_dict()
        output["_effective_live"] = effective.live
        output["_effective_stale"] = effective.stale
        output["_effective_reason"] = effective.reason
        sys.stdout.write(json.dumps(output, sort_keys=True, indent=2) + "\n")
        return 0

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

    if worker_under_lifecycle():
        configure_worker_logging(settings.worker_incarnation)
        install_worker_exception_hooks()
    else:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        )
    LOGGER.info(
        "worker starting: worker_id=%s incarnation=%s pid=%d "
        "poll=%.1fs process_poll=%.1fs "
        "lease=%.1fs lease_refresh=%.1fs lease_recovery=%.1fs "
        "output_pub=%.1fs claim_batch=%d health_pub=%.1fs",
        settings.worker_id,
        settings.worker_incarnation,
        os.getpid(),
        settings.poll_interval_seconds,
        settings.process_poll_interval_seconds,
        settings.lease_duration_seconds,
        settings.lease_refresh_interval_seconds,
        settings.lease_recovery_interval_seconds,
        settings.output_publication_interval_seconds,
        settings.claim_batch_limit,
        settings.health_publish_interval_seconds,
    )
    supervisor = Supervisor(settings, database)

    def _handle_shutdown(signum: int, _frame: object) -> None:
        del signum
        supervisor.request_shutdown()

    signal.signal(signal.SIGTERM, _handle_shutdown)
    signal.signal(signal.SIGINT, _handle_shutdown)
    supervisor.run()
    return 0


if __name__ == "__main__":
    main()
