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
housekeeping, then claim and start more pending jobs. There is no
application-level concurrency limit; the ``active`` registry is unbounded and
only the number of claims performed in a single supervisor turn is bounded for
fairness.

Running jobs carry a lease: ``state.lease_expires_at`` is set at claim time and
refreshed by a bulk heartbeat while the jobs run. When a lease truly expires
any worker running a recovery pass atomically marks the abandoned job
``failed`` with a clear diagnostic rather than re-executing it. Recovery is
atomic across many workers and never steals a genuinely live job, whose lease
is continuously refreshed.

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
"""

from __future__ import annotations

import json
import logging
import os
import signal
import socket
import subprocess
import tempfile
import time
from contextlib import suppress
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final
from uuid import uuid4

import psycopg
from psycopg.rows import tuple_row

from lubko.config import load_database_config
from lubko.protocol import (
    OUTPUT_CHUNK_MAX_BYTES,
    OUTPUT_TAIL_MAX_BYTES,
    TWO_COLUMN_INVARIANT,
    ProtocolError,
    build_output_chunk_payload,
    build_output_window_payload,
    parse_payload,
)

if TYPE_CHECKING:
    from uuid import UUID

    from lubko.config import DatabaseConfig

JobsConnection = psycopg.Connection[tuple[Any, ...]]

LOGGER: Final = logging.getLogger(__name__)
DEFAULT_POLL_INTERVAL_SECONDS: Final = 1.0
DEFAULT_PROCESS_POLL_INTERVAL_SECONDS: Final = 0.1
DEFAULT_CANCEL_GRACE_SECONDS: Final = 5.0
DEFAULT_LEASE_DURATION_SECONDS: Final = 30.0
DEFAULT_LEASE_REFRESH_INTERVAL_SECONDS: Final = 5.0
DEFAULT_LEASE_RECOVERY_INTERVAL_SECONDS: Final = 10.0
DEFAULT_OUTPUT_PUBLICATION_INTERVAL_SECONDS: Final = 1.0
DEFAULT_CLAIM_BATCH_LIMIT: Final = 8
DEFAULT_LEASE_SAFETY_MARGIN_SECONDS: Final = 5.0
DEFAULT_DB_OPERATION_TIMEOUT_SECONDS: Final = 15.0
LEASE_RECOVERY_LIMIT: Final = 100
CANCEL_DISCOVERY_LIMIT: Final = 100
SESSION_ESTABLISH_TIMEOUT_SECONDS: Final = 1.0
STAT_MIN_FIELDS: Final = 3
STAT_PGRP_FIELD_INDEX: Final = 2
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

#: NUL (U+0000) is valid UTF-8 but PostgreSQL rejects it in text/jsonb with
#: ``unsupported Unicode escape sequence`` (SQLSTATE 22P05).  Every raw-byte
#: output string must pass through :func:`sanitize_for_postgres` before it can
#: reach a ``text`` / ``jsonb`` column.
PG_NUL_REPLACEMENT: Final = "\ufffd"

STOP_REASON_CANCEL: Final = "cancel"
STOP_REASON_SHUTDOWN: Final = "shutdown"
STOP_REASON_LEASE: Final = "lease"
STOP_REASON_ROW_LOST: Final = "row_lost"
STOP_REASON_PERSIST: Final = "persist"
JOB_ID_ENV: Final = "LUBKO_JOB_ID"

#: PostgreSQL SQLSTATE class ``08`` — connection exceptions.  Errors in this
#: class indicate the connection is broken or unusable and must trigger the
#: existing outage/lease-safety path.  Deterministic data/representation
#: errors (e.g. 22P05 for NUL in text) live outside this class and must not
#: poison the worker-wide DB phase.
PGSQLSTATE_CONNECTIVITY_PREFIX: Final = "08"


def _is_connectivity_error(exc: psycopg.Error, conn: JobsConnection) -> bool:
    """Return ``True`` when *exc* indicates a broken or unusable connection.

    Classification rules (Psycopg 3):

    - ``conn.broken`` or ``conn.closed`` means the connection is unusable
      regardless of the exception's SQLSTATE.
    - SQLSTATE class ``08`` (connection exceptions) is always connectivity.
    - Client-side ``OperationalError`` without a SQLSTATE (e.g. timeout,
      network reset) arising from an established DB operation is treated as
      a client connection failure and triggers outage, not per-job quarantine.
    - Deterministic server/data errors (e.g. 22P05 for NUL in text) on a
      still-usable connection are per-job, not connectivity.

    Args:
        exc: The caught psycopg exception.
        conn: The PostgreSQL connection on which the error occurred.

    Returns:
        ``True`` when the error is a connectivity-level failure.
    """
    if conn.broken or conn.closed:
        return True
    sqlstate: str | None = getattr(exc, "sqlstate", None)
    if sqlstate and sqlstate.startswith(PGSQLSTATE_CONNECTIVITY_PREFIX):
        return True
    # Client-side OperationalError without sqlstate from an established
    # connection (timeout, reset, etc.) is a connectivity failure.
    return sqlstate is None and isinstance(exc, psycopg.OperationalError)


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
    """Per-stream capture and publication state for one active job."""

    path: Path
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
    finalized: bool = False
    last_heartbeat_at: float = 0.0


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
    supervisor turn performs, never a cap on concurrent jobs.
    """

    worker_id: str
    poll_interval_seconds: float
    process_poll_interval_seconds: float
    cancel_grace_seconds: float
    worker_incarnation: str = field(default_factory=lambda: uuid4().hex)
    lease_duration_seconds: float = DEFAULT_LEASE_DURATION_SECONDS
    lease_refresh_interval_seconds: float = DEFAULT_LEASE_REFRESH_INTERVAL_SECONDS
    lease_recovery_interval_seconds: float = DEFAULT_LEASE_RECOVERY_INTERVAL_SECONDS
    output_publication_interval_seconds: float = DEFAULT_OUTPUT_PUBLICATION_INTERVAL_SECONDS
    claim_batch_limit: int = DEFAULT_CLAIM_BATCH_LIMIT
    lease_safety_margin_seconds: float = DEFAULT_LEASE_SAFETY_MARGIN_SECONDS
    db_operation_timeout_seconds: float = DEFAULT_DB_OPERATION_TIMEOUT_SECONDS

    def __post_init__(self) -> None:
        """Validate lease timing so a live worker's lease never expires idle.

        Raises:
            ValueError: If any lease timing or fairness value is unusable.
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
        if self.output_publication_interval_seconds <= 0:
            msg = "LUBKO_OUTPUT_PUBLICATION_INTERVAL_SECONDS must be positive"
            raise ValueError(msg)
        if self.claim_batch_limit <= 0:
            msg = "LUBKO_CLAIM_BATCH_LIMIT must be positive"
            raise ValueError(msg)
        if self.db_operation_timeout_seconds <= 0:
            msg = "LUBKO_DB_OPERATION_TIMEOUT_SECONDS must be positive"
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
        )


# ---------------------------------------------------------------------------
# Output capture helpers
# ---------------------------------------------------------------------------


def sanitize_for_postgres(text: str) -> str:
    """Replace U+0000 (NUL) with U+FFFD so the result is safe for PostgreSQL ``text`` and ``jsonb``.

    PostgreSQL rejects NUL in text/jsonb values with SQLSTATE 22P05
    (``unsupported Unicode escape sequence``).  This canonical conversion
    preserves logical byte offsets because both NUL and U+FFFD are single-byte
    in their respective UTF-8 representations when the input was decoded from
    the raw capture file.  Invalid UTF-8 is already replaced with U+FFFD by the
    ``errors='replace'`` decode strategy used upstream.

    Args:
        text: Decoded UTF-8 text that may contain NUL characters.

    Returns:
        The same text with NUL replaced by the Unicode replacement character.
    """
    return text.replace("\x00", PG_NUL_REPLACEMENT)


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
    return sanitize_for_postgres(payload.decode("utf-8", errors="replace"))


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


def decode_range(path: Path, start: int, end: int) -> str:
    """Decode the bytes in ``[start, end)`` as UTF-8 text with replacement and NUL sanitization.

    The result is safe for PostgreSQL ``text`` / ``jsonb`` columns: invalid
    UTF-8 is replaced with U+FFFD by the decode strategy and NUL (U+0000) is
    replaced with U+FFFD by :func:`sanitize_for_postgres`.

    Args:
        path: Capture file for the stream.
        start: Inclusive byte offset.
        end: Exclusive byte offset.

    Returns:
        The decoded and sanitized text.
    """
    return sanitize_for_postgres(read_range(path, start, end).decode("utf-8", errors="replace"))


def output_window_text(path: Path, max_chars: int) -> tuple[str, int, int]:
    """Return the newest at most ``max_chars`` bytes as decoded text.

    Byte offsets are used for the window bounds and decoding is UTF-8 with
    replacement, so offsets are deterministic even when a window starts inside
    a multi-byte sequence.

    Args:
        path: Capture file for the stream.
        max_chars: Maximum number of bytes to retain in the window.

    Returns:
        A ``(text, start, end)`` tuple where ``end`` is the current file size.
    """
    size = stream_size(path)
    window = min(size, max_chars)
    start = size - window
    return decode_range(path, start, size), start, size


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
            "WHERE id = %(job_id)s AND (payload::jsonb)->>'type' = 'command'\n"
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
    return True


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
    """
    plans: dict[str, _StreamPlan] = {}
    for name in stream_names:
        stream = getattr(job, name)
        try:
            size = stream_size(stream.path)
        except OSError:
            continue
        if not force and size == stream.published_size:
            continue
        tail_text, tail_start, tail_end = output_window_text(stream.path, OUTPUT_TAIL_MAX_BYTES)
        chunks, archived_upto, last_chunk, sequence = _plan_chunks(job.id, name, stream, tail_end)
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
        value = decode_range(stream.path, chunk_start, chunk_end)
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


def _persist_process(conn: JobsConnection, job_id: UUID, pid: int, pgid: int) -> None:
    """Persist the exact process identity of a running job.

    The identity is written into ``payload.state.process_pid`` and
    ``payload.state.process_pgid``, keeping the two-column table invariant.

    Args:
        conn: Open PostgreSQL connection.
        job_id: Identifier of the running job.
        pid: Exact process ID of the spawned process.
        pgid: Exact process group ID of the spawned process.
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


def spawn_job(job: Job) -> tuple[subprocess.Popen[bytes], Path, Path, int]:
    """Start a job as a new session and process group leader.

    The job's required ``process`` argv is executed directly as the new
    process; the worker never invokes a shell, so argv elements are passed to
    the executable literally. The job is started as a new session so
    cancellation can signal the exact process group.

    The exact root job UUID is injected into the child environment as
    ``LUBKO_JOB_ID`` before the child execs, so every process of the job can
    identify its owning queue row deterministically without depending on the
    timing of any later database write.

    Args:
        job: Claimed job to execute.

    Returns:
        The running process, its capture file paths, and its process group ID.

    Raises:
        OSError: If the process cannot be started.
    """
    argv = list(job.process)
    env = dict(os.environ)
    env[JOB_ID_ENV] = str(job.id)
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
            env=env,
        )
    except OSError:
        os.close(stdout_fd)
        os.close(stderr_fd)
        _cleanup_output_files(stdout_path, stderr_path)
        raise
    os.close(stdout_fd)
    os.close(stderr_fd)
    pgid = _wait_for_session(proc.pid)
    return proc, stdout_path, stderr_path, pgid


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
            "WITH next AS (\n"  # ruff: ignore[hardcoded-sql-expression]
            "    SELECT id\n"
            "    FROM lubko.jobs\n"
            "    WHERE (payload::jsonb)->>'type' = 'command'\n"
            "        AND (payload::jsonb)->'state'->>'status' = 'pending'\n"
            "    ORDER BY (payload::jsonb)->'state'->>'created_at', id\n"
            "    FOR UPDATE SKIP LOCKED\n"
            "    LIMIT %(limit)s\n"
            ")\n"
            "UPDATE lubko.jobs AS job\n"
            "SET payload = " + set_chain + "::text\n"
            "FROM next\n"
            "WHERE job.id = next.id\n"
            "RETURNING job.id, job.payload\n",
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


def bulk_refresh_leases(conn: JobsConnection, settings: Settings) -> list[UUID]:
    """Refresh the lease of every running command row owned by this worker.

    One statement updates all owned running command rows in a single atomic
    JSON compare-and-swap, keeping heartbeats efficient under many concurrent
    jobs.

    Args:
        conn: Open PostgreSQL connection.
        settings: Worker runtime settings.

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
            "RETURNING id\n",
            {
                "lease_duration_seconds": settings.lease_duration_seconds,
                "worker_id": settings.worker_id,
                "worker_incarnation": settings.worker_incarnation,
            },
        )
        rows = cursor.fetchall()
    return [row[0] for row in rows]


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
        cursor.execute(
            "WITH stale AS (\n"  # ruff: ignore[hardcoded-sql-expression]
            "    SELECT id\n"
            "    FROM lubko.jobs\n"
            "    WHERE (payload::jsonb)->>'type' = 'command'\n"
            "        AND (payload::jsonb)->'state'->>'status' = 'running'\n"
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
        self.conn: JobsConnection | None = None
        self._stopping = False
        self._next_recovery_at = 0.0
        self._next_lease_refresh_at = 0.0
        self._next_cancel_scan_at = 0.0
        self._next_reconnect_at = 0.0

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
        """
        self._connect()
        while not self._stopping:
            try:
                self._tick(time.monotonic())
            except psycopg.Error as exc:
                self._enter_outage(exc)
            time.sleep(self.settings.process_poll_interval_seconds)
        self._shutdown()

    def _tick(self, now: float) -> None:
        """Run one supervisor turn: service processes, then database work.

        Args:
            now: Monotonic time at the start of the turn.
        """
        self._service_processes()
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
        if not self._stopping:
            self._claim_batch()

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

    def _refresh_leases(self) -> None:
        """Refresh every owned running lease in one bulk statement."""
        conn = self.conn
        if conn is None:
            return
        refreshed = set(bulk_refresh_leases(conn, self.settings))
        now = time.monotonic()
        for job_id, job in list(self.active.items()):
            if job.finalized:
                continue
            if job_id in refreshed:
                job.last_heartbeat_at = now
            elif not job.completed:
                # The row is no longer running (for example it was recovered by
                # another worker): never let the live process continue.
                LOGGER.warning("job %s is no longer running in the database; stopping it", job_id)
                job.row_lost = True
                request_stop(job, STOP_REASON_ROW_LOST)

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

    def _publish_all(self, now: float) -> None:
        """Publish changed output tails/chunks of running jobs, throttled.

        Connectivity errors (SQLSTATE class 08) are re-raised so the caller
        enters the outage/lease-safety path.  Deterministic data errors are
        logged with the real exception and the offending job is quarantined
        without affecting unrelated jobs.
        """
        conn = self.conn
        if conn is None:
            return
        interval = self.settings.output_publication_interval_seconds
        for job in list(self.active.values()):
            if job.completed or job.finalized:
                continue
            changed = self._collect_changed_streams(job, now, interval)
            if changed:
                self._publish_one_job(conn, job, changed, now)

    @staticmethod
    def _collect_changed_streams(job: ActiveJob, now: float, interval: float) -> list[str]:
        """Return stream names whose capture files have grown past the throttle.

        Args:
            job: The active job to inspect.
            now: Monotonic time of this publication pass.
            interval: Minimum seconds between publications per stream.

        Returns:
            The list of changed stream names.
        """
        changed: list[str] = []
        for name in OUTPUT_STREAMS:
            stream = getattr(job, name)
            if now - stream.published_at < interval:
                continue
            try:
                size = stream_size(stream.path)
            except OSError:
                continue
            if size == stream.published_size:
                continue
            changed.append(name)
        return changed

    @staticmethod
    def _publish_one_job(
        conn: JobsConnection, job: ActiveJob, changed: list[str], now: float
    ) -> None:
        """Publish output for one job, classifying errors appropriately.

        Connectivity errors (SQLSTATE class 08) are re-raised so the caller
        enters the outage/lease-safety path.  Deterministic data errors are
        logged with the real exception and the offending job is quarantined
        without affecting unrelated jobs.

        Args:
            conn: Open PostgreSQL connection.
            job: The active job whose output to publish.
            changed: Which streams to publish.
            now: Monotonic time of this publication pass.

        Raises:
            psycopg.Error: When a connectivity-level database failure occurs.
        """
        try:
            retained = publish_output(conn, job, changed, now)
        except psycopg.Error as exc:
            if _is_connectivity_error(exc, conn):
                raise
            LOGGER.exception(
                "publishing output for job %s quarantined; unrelated jobs remain serviceable",
                job.id,
            )
            job.row_lost = True
            request_stop(job, STOP_REASON_ROW_LOST)
            return
        if not retained:
            job.row_lost = True
            request_stop(job, STOP_REASON_ROW_LOST)

    def _finalize_completed(self) -> None:
        """Publish final output and finalize every job whose process is fully gone.

        Connectivity errors (SQLSTATE class 08) are re-raised so the caller
        enters the outage/lease-safety path.  Deterministic data errors are
        logged with the real exception and the offending job is quarantined
        without affecting unrelated jobs.

        Raises:
            psycopg.Error: When a connectivity-level database failure occurs.
        """
        conn = self.conn
        if conn is None:
            return
        for job in list(self.active.values()):
            if not (job.completed and not job.finalized):
                continue
            if group_has_members(job.pgid):
                # Background members of the exact process group are still being
                # reaped; finalizing (and untracking) now would leak them.
                continue
            try:
                retained = publish_output(
                    conn, job, list(OUTPUT_STREAMS), time.monotonic(), force=True
                )
            except psycopg.Error as exc:
                if _is_connectivity_error(exc, conn):
                    raise
                LOGGER.exception(
                    "publishing final output for job %s quarantined; "
                    "unrelated jobs remain serviceable",
                    job.id,
                )
                self._untrack_lost_job(job)
                continue
            if not retained:
                self._untrack_lost_job(job)
                continue
            if self._finalize_one(job):
                job.finalized = True
                self.active.pop(job.id, None)

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

        Args:
            job: The completed active job.

        Returns:
            ``True`` when the job was finalized and its capture files removed.

        Raises:
            psycopg.Error: When a connectivity-level database failure occurs.
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
            if _is_connectivity_error(exc, conn):
                raise
            LOGGER.exception("finalizing job %s quarantined", job.id)
            return False
        LOGGER.info(
            "finished job %s with status %s and exit code %d",
            job.id,
            final_status,
            result.exit_code,
        )
        cleanup_job(job)
        return True

    def _claim_batch(self) -> None:
        """Claim a bounded batch of pending jobs and start their processes.

        The batch size is a fairness bound on the amount of claiming work done
        in one turn; it is never a cap on the number of simultaneously active
        jobs.
        """
        conn = self.conn
        if conn is None or self._stopping:
            return
        claimed = claim_jobs(conn, self.settings, self.settings.claim_batch_limit)
        for claimed_job in claimed:
            self._start_job(claimed_job)

    def _start_job(self, claimed: ClaimedJob) -> None:
        """Start one claimed job as a process group and register it.

        If the process cannot be started (for example because the operating
        system refused to spawn it), the job fails clearly and independently
        while the daemon stays alive to supervise the jobs that did start.

        Args:
            claimed: The claimed job.
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
            proc, stdout_path, stderr_path, pgid = spawn_job(job_spec)
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

        job = ActiveJob(
            id=claimed.id,
            cwd=job_spec.cwd,
            process=job_spec.process,
            proc=proc,
            pid=proc.pid,
            pgid=pgid,
            started_mono=time.monotonic(),
        )
        job.stdout = OutputStream(path=stdout_path)
        job.stderr = OutputStream(path=stderr_path)
        job.last_heartbeat_at = time.monotonic()
        self.active[claimed.id] = job
        LOGGER.info("claimed job %s (pid %d)", job.id, job.pid)
        try:
            _persist_process(conn, job.id, job.pid, job.pgid)
        except psycopg.Error:
            LOGGER.exception(
                "unable to persist process identity for job %s",
                job.id,
            )
            request_stop(job, STOP_REASON_PERSIST)

    def _finalize_immediate(self, job_id: UUID, result: JobResult) -> None:
        """Finalize a job that failed before or during spawning.

        Args:
            job_id: The job identifier.
            result: The failure result.
        """
        conn = self.conn
        if conn is None:
            return
        try:
            finish_job(conn, job_id, result)
        except psycopg.Error:
            LOGGER.exception("unable to finalize job %s", job_id)

    def _enforce_lease_safety(self) -> None:
        """Terminate owned groups whose lease can no longer be refreshed in time.

        The local lease deadline is derived from the last successful heartbeat
        so the process group is terminated before its database lease can expire
        and another worker could legitimately treat the job as abandoned.
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

    def _enter_outage(self, _exc: psycopg.Error) -> None:
        """Transition into database outage handling, discarding the connection.

        The in-memory active registry is kept so local process ownership is
        never lost.  *_exc* is the exception that triggered the outage; the
        caller's ``except`` block provides traceback diagnostics.

        Args:
            _exc: The exception that triggered the outage.
        """
        LOGGER.error("database operation failed; entering outage handling")
        if self.conn is not None:
            with suppress(Exception):
                self.conn.close()
        self.conn = None
        self._next_reconnect_at = 0.0

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
        self._drain_active_groups()
        self._finalize_all_for_shutdown()
        self._cleanup_all_files()
        if self.conn is not None:
            with suppress(Exception):
                self.conn.close()
            self.conn = None

    def _drain_active_groups(self) -> None:
        """Wait for every active process group to exit, escalating to SIGKILL."""
        deadline = time.monotonic() + self.settings.cancel_grace_seconds
        while time.monotonic() < deadline:
            all_gone = all(
                self._observe_and_escalate(job, time.monotonic()) for job in self.active.values()
            )
            if all_gone:
                break
            time.sleep(0.05)
        for job in self.active.values():
            if not self._observe_and_escalate(job, time.monotonic()):
                signal_kill(job)
        for job in self.active.values():
            with suppress(Exception):
                job.proc.wait(timeout=self.settings.cancel_grace_seconds)

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

    def _finalize_all_for_shutdown(self) -> None:
        """Finalize every tracked job when PostgreSQL is available."""
        if self.conn is None:
            return
        for job in list(self.active.values()):
            if job.finalized:
                continue
            if not job.completed and job.stop_reason is None:
                job.stop_reason = STOP_REASON_SHUTDOWN
                job.cancellation_note = _stop_note(STOP_REASON_SHUTDOWN)
            try:
                retained = publish_output(
                    self.conn,
                    job,
                    list(OUTPUT_STREAMS),
                    time.monotonic(),
                    force=True,
                )
            except psycopg.Error:
                LOGGER.exception(
                    "publishing shutdown output for job %s failed",
                    job.id,
                )
                continue
            if not retained:
                self._untrack_lost_job(job)
                continue
            if self._finalize_one(job):
                job.finalized = True
                self.active.pop(job.id, None)

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
    if job.stop_reason in {STOP_REASON_LEASE, STOP_REASON_ROW_LOST, STOP_REASON_PERSIST}:
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
    """Remove the temporary capture files of a finalized job.

    Args:
        job: The finalized active job.
    """
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
    }
    return notes.get(reason, "stopped")


def main() -> None:
    """Run the Lubko worker supervisor.

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
    supervisor = Supervisor(settings, database)

    def _handle_shutdown(signum: int, _frame: object) -> None:
        del signum
        supervisor.request_shutdown()

    signal.signal(signal.SIGTERM, _handle_shutdown)
    signal.signal(signal.SIGINT, _handle_shutdown)
    supervisor.run()


if __name__ == "__main__":
    main()
