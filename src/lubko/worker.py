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
import math
import os
import select
import selectors
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
from typing import TYPE_CHECKING, Any, Final, cast, override
from uuid import uuid4

import psycopg
from psycopg.rows import tuple_row

from lubko._exact_signal import open_pidfd as _shared_open_pidfd
from lubko._exact_signal import pidfd_send_signal as _shared_pidfd_send_signal
from lubko._exact_signal import process_pgrp as _shared_process_pgrp
from lubko._start_gate import GATE_RELEASE_BYTE
from lubko.config import (
    load_database_config,
    load_worker_protocol_range,
    load_worker_server,
)
from lubko.health import (
    WORKER_HEALTH_SCHEMA_VERSION,
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
    _utf8_byte_length,
    build_output_chunk_payload,
    build_output_window_payload,
    parse_payload,
)
from lubko.protocol_versioning import (
    DEFAULT_VERSION_RANGE,
    SUPPORTED_PROTOCOL_VERSIONS,
    JobVersionDisposition,
    claim_version_predicate,
    reaper_disposition,
)
from lubko.state import state_root

if TYPE_CHECKING:
    from collections.abc import Collection, Iterable
    from uuid import UUID

    from psycopg.abc import RV, PQGen

    from lubko.config import DatabaseConfig
    from lubko.protocol_versioning import ProtocolVersionRange

    JobsConnection = psycopg.Connection[tuple[Any, ...]]
else:
    JobsConnection = psycopg.Connection[tuple[Any, ...]]


class DbOperationDeadlineError(TimeoutError):
    """An established database operation exceeded its hard client-side deadline.

    Raised by :func:`wait_with_deadline` when a libpq operation on an
    already-established connection did not complete by the operation's
    absolute monotonic deadline (for example because the socket silently
    black-holes packets). The connection is failed closed before the error is
    raised, so the supervisor classifies it as a connectivity failure and
    converges through the outage-safety path. It deliberately needs no
    psycopg base class: the supervisor catches it directly and treats it as a
    connectivity-classified outage trigger.
    """


def wait_with_deadline(gen: PQGen[RV], fileno: int, deadline: float) -> RV:
    """Drive a nonblocking libpq generator under an absolute monotonic deadline.

    This is the application-owned hard client bound on established database
    operations: unlike ``connect_timeout`` (which bounds only connection
    establishment) and server-side ``statement_timeout`` (which cannot
    guarantee the client ever notices a network black hole), this seam bounds
    the client's own waiting on every readiness cycle of the operation. When
    the deadline passes, the generator is abandoned and
    :class:`DbOperationDeadlineError` is raised; the caller must fail the
    connection closed and enter outage handling.

    Args:
        gen: A psycopg nonblocking generator performing a database operation.
        fileno: The established connection socket descriptor to wait on.
        deadline: Absolute monotonic time by which the operation must finish.

    Returns:
        Whatever ``gen`` returns on completion.
    """
    try:
        state = next(gen)
        with selectors.DefaultSelector() as sel:
            sel.register(fileno, state)
            return _drive_under_deadline(gen, sel, fileno, deadline)
    except StopIteration as ex:
        result: RV = ex.value
        return result


def _drive_under_deadline(
    gen: PQGen[RV], sel: selectors.BaseSelector, fileno: int, deadline: float
) -> RV:
    """Run the readiness loop of :func:`wait_with_deadline` under ``deadline``.

    Args:
        gen: The nonblocking libpq generator being driven.
        sel: Selector with ``fileno`` already registered.
        fileno: The established connection socket descriptor.
        deadline: Absolute monotonic time by which the operation must finish.

    Raises:
        DbOperationDeadlineError: If the deadline passed before completion.
    """
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0.0:
            msg = (
                "database operation exceeded its hard client deadline on an established connection"
            )
            raise DbOperationDeadlineError(msg)
        events = sel.select(timeout=remaining)
        if not events:
            # Mirror psycopg's own selector loop: a timeout with no readiness
            # still probes whether the socket disappeared, then lets the
            # generator decide how to proceed. The event mask ``0`` matches
            # what psycopg itself sends for this probe (selectors deliver
            # plain integer masks).
            os.fstat(fileno)
            gen.send(0)
            continue
        state = gen.send(events[0][1])
        sel.modify(fileno, state)


class DeadlineConnection(psycopg.Connection[tuple[Any, ...]]):
    """A supervisor connection whose operations obey a hard client deadline.

    Every cursor execution drives its libpq generator through :meth:`wait`,
    which enforces the absolute monotonic ``operation_deadline`` currently
    installed by the supervisor for this turn. On breach the underlying libpq
    connection is finished (marked closed/broken) before the deadline error
    propagates, so no later operation can reuse a hung socket and connectivity
    classification always succeeds.

    The deadline is deliberately *not* a fixed per-operation timeout: the
    supervisor derives it from the earliest active job's lease-safety instant
    (capped by the configured ``db_operation_timeout_seconds``), so even an
    operation that starts late in a lease cycle cannot outlive the margin.
    """

    #: Absolute monotonic deadline for the current operation, installed by the
    #: supervisor before each database turn. The presence of this class
    #: attribute is the deadline capability marker checked by
    #: :func:`install_operation_deadline`.
    operation_deadline: float = 0.0

    @override
    def wait(self, gen: PQGen[RV], interval: float = 0.1) -> RV:
        """Drive ``gen`` under the currently installed operation deadline.

        Args:
            gen: The nonblocking libpq generator to drive.
            interval: Unused compatibility parameter from psycopg's interface;
                waiting is bounded solely by the absolute deadline.

        Returns:
            Whatever the generator returns on completion.

        Raises:
            TimeoutError: The hard client deadline passed; the libpq
                connection is failed closed first.
        """
        try:
            return wait_with_deadline(gen, self.pgconn.socket, self.operation_deadline)
        except TimeoutError:
            # The only timeout source inside ``wait_with_deadline`` is the
            # application-owned hard client deadline; failing the connection
            # closed on it is fail-safe by construction.
            LOGGER.exception("database operation breached its client deadline")
            with suppress(Exception):
                self.pgconn.finish()
            raise


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


def operation_deadline_at(
    now_mono: float,
    heartbeat_origins: Collection[float],
    settings: Settings,
) -> float:
    """Return the absolute monotonic deadline for this turn's database work.

    The deadline is capped by the configured ``db_operation_timeout_seconds``
    and, when any live owned group is heartbeated, additionally bounded by the
    earliest job's local lease-safety instant
    ``last_heartbeat_at + lease_duration - margin``. Because settings
    validation guarantees ``db_operation_timeout_seconds < lease_duration -
    margin - refresh_interval``, a hung established operation is always
    detected strictly before any owned process's lease can become unsafe, no
    matter how late in a lease cycle the operation starts.

    Args:
        now_mono: Current monotonic time.
        heartbeat_origins: Conservative monotonic origins of the last committed
            heartbeats of locally-owned jobs with live process groups.
        settings: Worker runtime settings.

    Returns:
        The absolute monotonic operation deadline for this turn.
    """
    limit = now_mono + settings.db_operation_timeout_seconds
    safety_instants = (
        origin + settings.lease_duration_seconds - settings.lease_safety_margin_seconds
        for origin in heartbeat_origins
    )
    earliest = min(safety_instants, default=None)
    if earliest is not None and earliest < limit:
        return earliest
    return limit


def install_operation_deadline(conn: JobsConnection | None, deadline: float) -> None:
    """Install the hard client deadline on the supervisor's live connection.

    The deadline capability is expressed as a ``operation_deadline`` class
    attribute on the connection type: production connections are established
    as :class:`DeadlineConnection`, which defines it, while a plain
    production-established connection does not and therefore fails closed —
    silently operating without the established-operation bound is never
    acceptable. Test doubles may opt in by declaring the same class
    attribute.

    Args:
        conn: The supervisor's current database connection (or ``None``).
        deadline: The absolute monotonic operation deadline to install.

    Raises:
        TypeError: If the connection type lacks the deadline capability.
    """
    if conn is None:
        return
    if getattr(type(conn), "operation_deadline", None) is None:
        msg = (
            "database connection lacks the hard client operation-deadline "
            "capability; refusing to operate without the lease-safety bound"
        )
        raise TypeError(msg)
    cast("Any", conn).operation_deadline = deadline


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
#: Maximum bytes drained from a capture pipe into a spool file per syscall.  The
#: physical spool is bounded by ``output_spool_max_bytes``; this chunk is only
#: the unit of a single read, never the bound.
DRAIN_CHUNK: Final = 65536
#: How long the bounded finalization cycle keeps waiting for end-of-file on a
#: completed job's capture pipes before abandoning them. A job that forked a
#: detached grandchild (a deployment helper) leaves that writer holding the
#: pipe write end forever, so EOF never arrives on its own; after this grace
#: everything already captured is published and the job is finalized. Only
#: relevant once the job process itself is terminal: live producers are
#: cancelled through their own process group, never through this path.
DETACHED_CAPTURE_EOF_GRACE_SECONDS: Final = 5.0
TRUNCATION_MARKER: Final = b"\n... [output truncated] ...\n"
PROTOCOL_ERROR_EXIT_CODE: Final = 2
EXECUTION_ERROR_EXIT_CODE: Final = 127
JOBS_SCHEMA: Final = "lubko"
JOBS_TABLE: Final = "jobs"
JOBS_COLUMN_TYPES: Final = (("id", "uuid"), ("payload", "text"))
TYPE_AWARE_CONSTRAINT_NAME: Final = "jobs_payload_type_shape"
#: Whitespace-stripped fragments that only the protocol v4 (routing-aware)
#: definition of ``jobs_payload_type_shape`` contains; a pre-cutover v3
#: constraint of the same name lacks them and is refused at startup. Compared
#: against ``pg_get_constraintdef`` output with all whitespace removed, so
#: PostgreSQL normalization (extra parentheses and ``::text`` casts included)
#: never defeats detection. The ``='4'::jsonb`` marker proves the constraint
#: enforces the protocol version itself, not merely server presence.
SERVER_ROUTING_CONSTRAINT_MARKERS: Final = (
    "jsonb_typeof",
    "->'server'",
    "='string'",
    "='4'::jsonb",
)
#: SQL predicate selecting rows whose top-level ``server`` is exactly the
#: daemon's configured identity as a JSON *string*. The ``jsonb_typeof`` guard
#: makes text-coercion aliases impossible: a row with ``server: 123`` (a JSON
#: number) can never match a daemon configured with the string ``"123"``,
#: mirroring the parser's strict string typing. In row-selection predicates a
#: missing key yields NULL and therefore never matches.
SERVER_MATCH_SQL: Final = (
    "jsonb_typeof((payload::jsonb)->'server') = 'string'\n    AND (payload::jsonb)->>'server' = "
)
CHUNK_OWNER_INDEX_NAME: Final = "jobs_chunk_owner_idx"
CHUNK_ORDER_INDEX_NAME: Final = "jobs_chunk_order_idx"
UTC_ISO_TEXT_SQL: Final = "to_char(now() at time zone 'utc', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"')"
UTC_ISO_SQL: Final = f"to_jsonb({UTC_ISO_TEXT_SQL})"
GC_FINISHED_AT_PATTERN: Final = (
    r"^[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])"
    r"T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]\.[0-9]{6}Z$"
)
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
    #: Protocol version of the root command payload that owns this job. Every
    #: immutable output_chunk this worker publishes for the job is stamped with
    #: this same version, so chunk history can never drift to a different
    #: protocol generation than its root.
    version: int
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
    # Exact start-time clock ticks of the command process (== group leader),
    # durably persisted before gate release and carried in memory unchanged.
    # This is the non-reusable identity proof that keeps every live
    # supervision/drain/shutdown group decision from ever mistaking a
    # numerically recycled PGID for this job's group.
    start_ticks: int = 0
    # Per-invocation member ledger (pid -> start-time ticks) recorded under
    # positive ownership proof: seeded with the exact persisted leader identity
    # at activation and extended with every member observed while the leader
    # slot provably holds the recorded command. A member counts as ours later
    # (leader exited, PGID possibly recycled) ONLY by exact (pid, ticks) match
    # against this ledger; anything else fails closed and is never signalled.
    owned_members: dict[int, int] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class _HealthAggregates:
    """Bounded per-job aggregates for one health snapshot."""

    active_jobs: int
    stopping_jobs: int
    oldest_active_job_age_seconds: float | None
    min_lease_safety_remaining_seconds: float | None
    capture_streams_open: int
    spool_held_bytes: int
    cancellation_scan_overdue: bool
    recovery_overdue: bool
    gc_overdue: bool


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
    #: Configured execution-server identity of this daemon. Every claim,
    #: mutation, publication, recovery, and GC pass is scoped to exactly this
    #: server; a daemon refuses to start without a valid non-empty identity.
    #: The identity is never environmental: it is read from the restricted
    #: worker configuration file (see :func:`lubko.config.load_worker_server`).
    server: str
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
    #: Supported protocol version window for this daemon. A daemon claims and
    #: executes only jobs whose ``v`` lies inside this window, so a fleet can
    #: run a bounded mixed-version set during a staggered, non-destructive
    #: upgrade while older in-flight jobs keep running on daemons that still
    #: advertise the older version. The window may never include a version this
    #: build cannot parse.
    supported_protocol_range: ProtocolVersionRange = DEFAULT_VERSION_RANGE

    def __post_init__(self) -> None:
        """Validate lease timing so a live worker's lease never expires idle.

        Raises:
            ValueError: If the server identity is empty, any timing value is
                unusable, or the supported protocol window is invalid.
        """
        self._validate_finite_timing()
        if not self.server:
            msg = (
                "a non-empty 'server' setting in the worker configuration file is "
                "required; every daemon owns exactly one configured server and "
                "refuses to start without it"
            )
            raise ValueError(msg)
        self._validate_lease_timing()
        self._validate_output_and_gc()
        self._validate_spool()
        self._validate_protocol_range()

    def _validate_finite_timing(self) -> None:
        """Reject non-finite timing values before domain/order comparisons.

        Raises:
            ValueError: If any timing setting is NaN or infinite.
        """
        fields = (
            "poll_interval_seconds",
            "process_poll_interval_seconds",
            "cancel_grace_seconds",
            "lease_duration_seconds",
            "lease_refresh_interval_seconds",
            "lease_recovery_interval_seconds",
            "output_publication_interval_seconds",
            "health_publish_interval_seconds",
            "lease_safety_margin_seconds",
            "db_operation_timeout_seconds",
            "gc_retention_seconds",
            "gc_interval_seconds",
        )
        for field_name in fields:
            value = getattr(self, field_name)
            if not math.isfinite(value):
                msg = f"{field_name} must be finite"
                raise ValueError(msg)

    def _validate_protocol_range(self) -> None:
        """Fail closed if the window includes an unparseable version.

        Raises:
            ValueError: If any version in the window exceeds what this build can
                parse and execute.
        """
        window = self.supported_protocol_range
        for version in range(window.min, window.max + 1):
            if version not in SUPPORTED_PROTOCOL_VERSIONS:
                msg = (
                    f"supported protocol range [{window.min}, {window.max}] "
                    f"includes version {version} which this build cannot parse; "
                    "the daemon refuses to start rather than claim jobs it "
                    "cannot safely execute"
                )
                raise ValueError(msg)

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
        # Fail-closed ordering invariant: the configured hard client deadline
        # for an established database operation must fit inside the lease-safety
        # budget that remains at the latest possible heartbeat attempt. A
        # refresh starts no later than ``last_heartbeat_at + refresh_interval``
        # and its group must be terminable before ``last_heartbeat_at +
        # lease_duration - margin``, so the operation deadline must be strictly
        # smaller than ``duration - margin - refresh_interval``. Otherwise a
        # single hung database operation could outlive the safety margin and a
        # worker would refuse to start rather than run unsafely.
        if self.db_operation_timeout_seconds >= (
            self.lease_duration_seconds
            - self.lease_safety_margin_seconds
            - self.lease_refresh_interval_seconds
        ):
            msg = (
                "LUBKO_DB_OPERATION_TIMEOUT_SECONDS must be smaller than "
                "LUBKO_LEASE_DURATION_SECONDS - LUBKO_LEASE_SAFETY_MARGIN_SECONDS "
                "- LUBKO_LEASE_REFRESH_INTERVAL_SECONDS so a hung established "
                "database operation can never block past the local lease-safety "
                "deadline"
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
    def from_environment(
        cls, *, server: str, supported_protocol_range: ProtocolVersionRange | None = None
    ) -> Settings:
        """Load worker settings from environment variables.

        The execution-server identity and the supported protocol version window
        are never environmental: they must be supplied explicitly, loaded from the
        restricted worker configuration file by the entry point.

        Args:
            server: Non-empty execution-server identity from the config file.
            supported_protocol_range: The daemon's supported protocol window from
                the config file, or ``None`` to use the default current-version
                window.

        Returns:
            Settings derived from the process environment plus the configured
            server identity and protocol window.
        """
        return cls(
            server=server,
            supported_protocol_range=supported_protocol_range
            if supported_protocol_range is not None
            else DEFAULT_VERSION_RANGE,
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

    truncated = len(data) > limit
    if truncated:
        budget = limit - len(TRUNCATION_MARKER)
        tail = data[-budget:] if budget > 0 else b""
        payload = TRUNCATION_MARKER + tail
    else:
        payload = data
    result = pg_safe_decode(payload)

    # Decoding can expand the byte length (NUL and invalid sequences become
    # the 3-byte U+FFFD), so drop the oldest decoded payload characters,
    # keeping any truncation marker, until the configured limit holds as a
    # hard bound on the encoded output.
    body_offset = len(TRUNCATION_MARKER.decode()) if truncated else 0
    encoded_len = len(result.encode())
    if encoded_len > limit:
        index = body_offset
        while encoded_len > limit:
            encoded_len -= len(result[index].encode())
            index += 1
        result = result[:body_offset] + result[index:]
    return result


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
    taken ownership of. The retained bytes obey exactly the same current
    aggregate on-disk room accounting as fresh pipe reads: a partial write that
    landed a prefix before failing can leave a suffix in ``pending`` while the
    prefix has filled the spool to the bound, so the retry flush of ``pending``
    is limited to the currently available aggregate room and never appends
    beyond the bound — when no room exists the bounded/full condition is
    reported so the caller can publish+trim and retry. When a spool write fails
    the read bytes stay in
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
    try:
        size = stream.path.stat().st_size
    except OSError:
        return "error"
    used = aggregate_used if aggregate_used is not None else size
    # Pending bytes already taken from the pipe obey exactly the same current
    # aggregate on-disk room accounting as fresh pipe reads: a partial-write-
    # then-error can leave a retained suffix in ``pending`` after the successful
    # prefix filled the spool, and unconditionally flushing that suffix here
    # would append past the bound. The pending flush may consume at most the
    # currently available aggregate disk room and never appends beyond the
    # bound — when no room exists the bounded/full condition is reported so the
    # caller can publish+trim and retry.
    disk_room = max(0, bound - used)
    if stream.pending:
        status, landed = _retry_pending_flush(stream, disk_room)
        if status != "ok":
            return status
        # Account for exactly the bytes that landed on disk: with a
        # caller-supplied aggregate, re-statting only this stream's own file
        # would drop the sibling stream's contribution from the total.
        used += landed
        disk_room = max(0, bound - used)
    if stream.fd is None:
        stream.eof = True
        return "eof"
    return "full" if disk_room <= 0 else _read_capture_chunk(stream, disk_room)


def _retry_pending_flush(stream: OutputStream, disk_room: int) -> tuple[str, int]:
    """Retry retained pending bytes against the currently available disk room.

    A partial-write-then-error can leave a suffix in ``pending`` after its
    successful prefix filled the spool; this flush appends at most the current
    aggregate disk room so retained bytes never land beyond the bound.

    Args:
        stream: The stream whose pending buffer to flush.
        disk_room: Currently available aggregate on-disk room under the bound.

    Returns:
        A ``(status, landed)`` pair: ``status`` is ``"ok"`` when every retained
        byte landed within ``disk_room`` (``landed`` counts them), ``"full"``
        when disk room was exhausted while a suffix is still retained (the
        caller must publish+trim before retrying), or ``"error"`` when the
        spool write genuinely failed; ``landed`` is nonzero only for ``"ok"``.
    """
    if disk_room <= 0:
        return ("full", 0)
    pending_before = len(stream.pending)
    if not _flush_pending(stream, disk_room):
        return ("error", 0)
    if stream.pending:
        # The flush consumed all available disk room; the benignly retained
        # suffix must wait until publication/trim frees bounded room.
        return ("full", 0)
    return ("ok", pending_before)


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
        residual pending-buffer write failed or a positive partial write
        landed only a prefix of the final pending bytes.
    """
    fd = stream.fd
    if fd is not None:
        with suppress(OSError):
            os.close(fd)
    stream.fd = None
    # A final flush must land every retained byte: a hard failure retains
    # the buffer intact, and a positive partial write consumes only its
    # landed prefix. Either way a residual suffix means the already-read
    # bytes cannot all be represented on disk, so the stream is failed
    # closed instead of silently marking EOF over unlanded output.
    if stream.pending and (not _flush_pending(stream) or stream.pending):
        return "error"
    stream.eof = True
    return "eof"


def _flush_pending(stream: OutputStream, limit: int | None = None) -> bool:
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
        limit: Maximum number of pending bytes to append in this flush, so a
            retained suffix can never be written past the currently available
            aggregate spool room; ``None`` appends the whole buffer.

    Returns:
        ``True`` when the flush made progress (the buffer is now empty or holds
        only a benignly retained suffix to retry on the next drain),
        ``False`` when the spool write genuinely failed and the buffer must be
        retained intact for a closed failure.
    """
    if not stream.pending:
        return True
    # Always hand the seam a separate copy so its in-place consumption can
    # never double-consume from the real buffer; exactly ``written`` bytes are
    # then removed from ``pending`` once, below.
    data = bytearray(stream.pending if limit is None else stream.pending[:limit])
    before = len(stream.pending)
    written = _spill_append(stream.path, data)
    del stream.pending[:written]
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


_UTF8_CONTINUATION_MIN: Final = 0x80
_UTF8_CONTINUATION_MAX: Final = 0xBF
_UTF8_TWO_BYTE_MIN: Final = 0xC2
_UTF8_TWO_BYTE_MAX: Final = 0xDF
_UTF8_THREE_BYTE_MIN: Final = 0xE0
_UTF8_THREE_BYTE_MAX: Final = 0xEF
_UTF8_FOUR_BYTE_MIN: Final = 0xF0
_UTF8_FOUR_BYTE_MAX: Final = 0xF4
_UTF8_MIN_CODE_POINT_LEN: Final = 2
_UTF8_MAX_CODE_POINT_LEN: Final = 4
_UNICODE_MAX_CODE_POINT: Final = 0x10FFFF
_UTF16_SURROGATE_MIN: Final = 0xD800
_UTF16_SURROGATE_MAX: Final = 0xDFFF
_CODE_POINT_MIN_VALUE: Final[dict[int, int]] = {2: 0x80, 3: 0x800, 4: 0x10000}


def _is_utf8_continuation_byte(byte: int) -> bool:
    """Return ``True`` when ``byte`` continues a multi-byte UTF-8 sequence."""
    return _UTF8_CONTINUATION_MIN <= byte <= _UTF8_CONTINUATION_MAX


def _code_point_length(lead: int) -> int:
    """Return the byte length (2/3/4) of a structurally valid multi-byte lead.

    ASCII (``< 0x80``) and invalid lead bytes (including the overlong ``0xC0``/
    ``0xC1`` heads and the ``0xF5..0xFF`` range) return ``0``; the caller then
    leaves the raw boundary unchanged so ``pg_safe_decode`` defines the behavior.
    """
    if _UTF8_TWO_BYTE_MIN <= lead <= _UTF8_TWO_BYTE_MAX:
        return 2
    if _UTF8_THREE_BYTE_MIN <= lead <= _UTF8_THREE_BYTE_MAX:
        return 3
    if _UTF8_FOUR_BYTE_MIN <= lead <= _UTF8_FOUR_BYTE_MAX:
        return 4
    return 0


def _code_point_scalar(lead: int, cont: bytes, length: int) -> int | None:
    """Decode the Unicode scalar of a structurally-shaped code point, or ``None``.

    The lead/continuation byte shapes are assumed already checked by the caller;
    this only rejects *overlong* encodings (whose scalar could be written in fewer
    bytes) by returning ``None`` when the decoded value is below the minimum for
    ``length``. Callers additionally reject surrogates and values above U+10FFFF so
    that semantically invalid sequences are not treated as valid code points for
    boundary movement.

    Args:
        lead: The lead byte.
        cont: The ``length - 1`` continuation bytes.
        length: The code-point byte length (2, 3 or 4).

    Returns:
        The decoded scalar, or ``None`` when the encoding is overlong.
    """
    lead_mask = (1 << (7 - length)) - 1
    cp = lead & lead_mask
    for byte in cont:
        cp = (cp << 6) | (byte & 0x3F)
    if cp < _CODE_POINT_MIN_VALUE[length]:
        return None
    return cp


def _valid_code_point_at(data: bytes, start: int, length: int) -> bool:
    """Return ``True`` if ``data[start:start + length]`` is a valid code point.

    A valid code point has a structurally correct lead byte whose declared length
    matches ``length``, whose continuation bytes are all continuation bytes, and
    whose decoded scalar is a legal Unicode value (not overlong, not a UTF-16
    surrogate, not above U+10FFFF). Semantically invalid sequences such as
    ``E0 80 80`` (overlong), ``ED A0 80`` (surrogate), ``F0 80 80 80`` (overlong)
    and ``F4 90 80 80`` (> U+10FFFF) are rejected so they stay on the raw boundary
    and ``pg_safe_decode`` defines their deterministic replacement. The check
    inspects only ``length`` bytes, never scanning further.
    """
    if length < _UTF8_MIN_CODE_POINT_LEN or length > _UTF8_MAX_CODE_POINT_LEN:
        return False
    if start < 0 or start + length > len(data):
        return False
    if _code_point_length(data[start]) != length:
        return False
    cont = data[start + 1 : start + length]
    if not all(_is_utf8_continuation_byte(byte) for byte in cont):
        return False
    cp = _code_point_scalar(data[start], cont, length)
    if cp is None or cp > _UNICODE_MAX_CODE_POINT:
        return False
    return not (_UTF16_SURROGATE_MIN <= cp <= _UTF16_SURROGATE_MAX)


def align_code_point_start(data: bytes, candidate: int) -> int:
    """Return the smallest code-point boundary at or after ``candidate``.

    The live-tail head only moves when ``candidate`` is strictly inside a
    structurally valid 2/3/4-byte code point: the head snaps forward to that
    code point's end (the next boundary) so the newest tail never begins
    mid-rune. Because a code point is at most four bytes, the lead lies within
    three bytes before ``candidate``; only those few bytes are inspected and a
    continuation run with no valid lead nearby is left untouched. The head moves
    at most three bytes forward, so the window stays within its byte bound, and
    genuinely invalid bytes are left to ``pg_safe_decode``.

    Args:
        data: Raw bytes of the capture file.
        candidate: Requested head offset, clamped into ``[0, len(data)]``.

    Returns:
        The aligned head offset (a code-point boundary or the bounds of ``data``).
    """
    n = len(data)
    if candidate <= 0:
        return 0
    if candidate >= n:
        return n
    for lead_back in (1, 2, 3):
        lead = candidate - lead_back
        if lead < 0:
            break
        length = _code_point_length(data[lead])
        if length == 0 or lead_back >= length:
            continue
        if _valid_code_point_at(data, lead, length):
            return lead + length
    return candidate


def align_code_point_end(data: bytes, candidate: int) -> int:
    """Return the largest code-point boundary at or before ``candidate``.

    An archive chunk end only moves when ``candidate`` is strictly inside a
    structurally valid 2/3/4-byte code point: the end snaps back to that code
    point's start so the immutable chunk never splits a rune and only ever holds
    complete code points. Only the few bytes around ``candidate`` are inspected;
    a continuation run with no valid lead within three bytes is left untouched and
    decoded deterministically by ``pg_safe_decode``. The chunk stays within its
    byte bound because the end only moves backward (toward older bytes).

    Args:
        data: Raw bytes of the capture file.
        candidate: Requested end offset, clamped into ``[0, len(data)]``.

    Returns:
        The aligned end offset (a code-point boundary or the bounds of ``data``).
    """
    n = len(data)
    if candidate <= 0:
        return 0
    if candidate >= n:
        return n
    for lead_back in (1, 2, 3):
        lead = candidate - lead_back
        if lead < 0:
            break
        length = _code_point_length(data[lead])
        if length == 0 or lead_back >= length:
            continue
        if _valid_code_point_at(data, lead, length):
            return lead
    return candidate


def _bounded_suffix(raw: bytes, limit: int) -> tuple[int, str]:
    """Keep the newest ``raw`` bytes whose decoded text fits a UTF-8 byte limit.

    The newest tail of ``raw`` is retained: the returned ``keep`` offset is the
    smallest code-point boundary at which ``pg_safe_decode(raw[keep:])`` encodes
    to at most ``limit`` UTF-8 bytes. Offsets stay on raw byte boundaries, so the
    returned text is exactly the decode of its ``[keep, len(raw))`` range and the
    canonical invalid-byte replacement policy is unchanged; only the represented
    byte interval is shortened (from the oldest end) when sanitizing invalid
    bytes would otherwise expand the encoded payload past the protocol's ceiling.

    Args:
        raw: The candidate raw byte window (already code-point aligned at the start).
        limit: Maximum UTF-8 byte length of the returned decoded text.

    Returns:
        A ``(keep, text)`` pair where ``text`` is the bounded decoded suffix.
    """
    full = pg_safe_decode(raw)
    if _utf8_byte_length(full) <= limit:
        return 0, full
    boundaries = [k for k in range(len(raw) + 1) if align_code_point_end(raw, k) == k]
    lo, hi = 0, len(boundaries) - 1
    keep = len(raw)
    while lo <= hi:
        mid = (lo + hi) // 2
        k = boundaries[mid]
        if _utf8_byte_length(pg_safe_decode(raw[k:])) <= limit:
            keep = k
            hi = mid - 1
        else:
            lo = mid + 1
    return keep, pg_safe_decode(raw[keep:])


def _bounded_prefix(raw: bytes, limit: int) -> tuple[int, str]:
    """Keep the oldest ``raw`` bytes whose decoded text fits a UTF-8 byte limit.

    The oldest prefix of ``raw`` is retained: the returned ``end`` offset is the
    largest code-point boundary at which ``pg_safe_decode(raw[:end])`` encodes to
    at most ``limit`` UTF-8 bytes, and ``end`` is always at least one byte so the
    represented range makes forward progress. Offsets stay on raw byte boundaries,
    so the returned text is exactly the decode of its ``[0, end)`` range and the
    canonical invalid-byte replacement policy is unchanged; only the represented
    byte interval is shortened (from the newest end) when sanitizing invalid
    bytes would otherwise expand the encoded payload past the protocol's ceiling.

    Args:
        raw: The candidate raw byte window (already code-point aligned at both ends).
        limit: Maximum UTF-8 byte length of the returned decoded text.

    Returns:
        An ``(end, text)`` pair where ``text`` is the bounded decoded prefix.
    """
    if not raw:
        return 0, ""
    boundaries = [k for k in range(1, len(raw) + 1) if align_code_point_end(raw, k) == k]
    lo, hi = 0, len(boundaries) - 1
    end = 1
    while lo <= hi:
        mid = (lo + hi) // 2
        k = boundaries[mid]
        if _utf8_byte_length(pg_safe_decode(raw[:k])) <= limit:
            end = k
            lo = mid + 1
        else:
            hi = mid - 1
    return end, pg_safe_decode(raw[:end])


def output_window_text(path: Path, max_chars: int, *, base: int = 0) -> tuple[str, int, int]:
    """Return the newest at most ``max_chars`` bytes as decoded text.

    Byte offsets are used for the window bounds and decoding is UTF-8 with
    replacement, so offsets are deterministic even when a window starts inside
    a multi-byte sequence. The window head is aligned forward to the next
    code-point boundary so a multi-byte rune is never split across the live-tail
    boundary: the returned text is exactly the decode of its ``[start, end)``
    byte range, with no partial rune replaced by U+FFFD, while the window stays
    within ``max_chars``. ``base`` is the logical offset of the first physical
    byte in the file (``OutputStream.spool_start``); the returned ``start`` and
    ``end`` are logical offsets.

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
    physical_head = start - base
    # Read only the bounded tail plus the at most three prefix bytes needed to
    # classify a code-point crossing; the whole spool is never materialized.
    read_from = max(0, physical_head - 3)
    tail = read_range(path, read_from, size - base)
    local_candidate = physical_head - read_from
    aligned_local = align_code_point_start(tail, local_candidate)
    aligned_physical = read_from + aligned_local
    raw_window = tail[aligned_local:]
    keep, text = _bounded_suffix(raw_window, max_chars)
    return text, aligned_physical + keep + base, size


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


def publish_output(  # ruff: ignore[too-many-arguments] -- server and force complete the publication contract; splitting them hides intent
    conn: JobsConnection,
    job: ActiveJob,
    stream_names: list[str],
    now: float,
    *,
    server: str,
    force: bool = False,
) -> bool:
    """Publish changed live tails and archive historical output for one job.

    Immutable ``output_chunk`` rows are inserted and the root row's live-window
    metadata (including the ``previous`` pointer) is updated in one PostgreSQL
    transaction, so a crash can never leave the root pointing at nonexistent
    history. Every inserted chunk carries the daemon's configured ``server``
    identity. The transaction first retains the root ``command`` row with a
    row-level lock (and only when its server matches): when a concurrent root
    deletion has already committed, no chunk row is inserted at all and
    publication returns ``False``, so publication itself never leaves an
    explicitly owned orphan chunk. In-memory publication state is only advanced
    after the transaction commits, so a failed transaction never leaves the
    registry pointing at chunks that were not inserted. The live tail itself is
    always recomputed as the newest ``OUTPUT_TAIL_MAX_BYTES`` bytes of the
    capture file, so archiving is observationally invisible to a normal
    root-row ``SELECT``.

    Args:
        conn: Open PostgreSQL connection.
        job: The active job whose output to publish.
        stream_names: Which streams to publish.
        now: Monotonic time of this publication pass.
        server: The daemon's configured server identity stamped onto every
            inserted chunk and required on the retained root row.
        force: Whether to publish regardless of the throttle interval.

    Returns:
        ``True`` when the root ``command`` row was retained (or nothing needed
        publishing), ``False`` when the root row no longer exists and the
        planned publication was skipped.
    """
    plans = _plan_streams(job, stream_names, server=server, force=force)
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
            "    AND " + SERVER_MATCH_SQL + "%(server)s\n"
            "    AND ((payload::jsonb)->'state'->>'gc') IS DISTINCT FROM 'true'\n"
            "FOR UPDATE\n",
            {"job_id": job.id, "server": server},
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
            _output_update_params(job.id, output, server),
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
    job: ActiveJob, stream_names: list[str], *, server: str, force: bool
) -> dict[str, _StreamPlan]:
    """Compute publication plans for changed streams without mutating state.

    Args:
        job: The active job whose output to publish.
        stream_names: Which streams to publish.
        server: The daemon's configured server identity stamped onto chunks.
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
                job.id,
                name,
                stream,
                tail_end,
                server,
                version=job.version,
                tail_start=tail_start,
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


def _plan_chunks(  # ruff: ignore[too-many-arguments] -- the chunk identity/offset/version fields are each required; bundling them hides intent
    job_id: UUID,
    name: str,
    stream: OutputStream,
    tail_end: int,
    server: str,
    *,
    version: int,
    tail_start: int = 0,
) -> tuple[tuple[tuple[UUID, str], ...], int, UUID | None, int]:
    """Compute the immutable chunks to archive for one stream.

    Args:
        job_id: Owning root job identifier.
        name: Stream name.
        stream: The stream's current publication state.
        tail_end: Current byte size of the stream (end of the live tail).
        server: The daemon's configured server identity stamped onto chunks.
        version: Protocol version of the owning root job; every emitted chunk is
            stamped with this same version so chunk history cannot drift to a
            different protocol generation.
        tail_start: Logical offset where the live tail begins. Archiving must
            reach at least this offset so the immutable chunks and the live tail
            cover every raw byte of the stream with no gap: byte offsets strictly
            before ``tail_start`` are historical and belong in chunks, while the
            tail owns ``[tail_start, tail_end)``. ``archive_target`` overlaps the
            tail by ``ARCHIVE_MARGIN_CHARS`` (intentional, offset-disambiguated),
            but invalid UTF-8 expands to U+FFFD on decode and can push the tail
            head forward past that margin; when it does, archiving must extend to
            ``tail_start`` itself rather than stopping short and leaving an
            uncovered, and ultimately trimmed-away, gap.

    Returns:
        The planned ``(chunks, archived_upto, last_chunk, sequence)`` tuple.
    """
    chunks: list[tuple[UUID, str]] = []
    archived_upto = stream.archived_upto
    last_chunk = stream.last_chunk
    sequence = stream.sequence
    target = archive_target(tail_end)
    # Emit full-size chunks toward the margin target. Each aligned end moves
    # backward by at most three bytes, so every full chunk makes guaranteed
    # forward progress and the loop terminates; the leftover partial window is
    # intentionally left in the live-tail overlap (it is archived by a later
    # publication once it forms a full chunk).
    while target - archived_upto >= OUTPUT_CHUNK_MAX_BYTES:
        chunk_start = archived_upto
        candidate = chunk_start + OUTPUT_CHUNK_MAX_BYTES - stream.spool_start
        # Classify the boundary with a bounded neighborhood read: a few bytes before
        # the candidate (for the lead) and up to three after it (the longest a
        # 4-byte rune can extend), so the whole spool is never materialized.
        n_from = max(0, candidate - 3)
        neighborhood = read_range(stream.path, n_from, candidate + 4)
        # End the chunk on a complete code point so its decoded value is exactly
        # the decode of its byte range and never collides mid-rune with the
        # adjacent chunk or the live tail. The alignment moves the end backward by
        # at most three bytes, so the chunk stays within OUTPUT_CHUNK_MAX_BYTES.
        chunk_end = (
            n_from + align_code_point_end(neighborhood, candidate - n_from) + stream.spool_start
        )
        # Decode only the bounded chunk bytes for the immutable value.
        chunk_bytes = read_range(
            stream.path, chunk_start - stream.spool_start, chunk_end - stream.spool_start
        )
        value = pg_safe_decode(chunk_bytes)
        # Invalid bytes sanitize to the three-byte U+FFFD, so the decoded value
        # can encode to more UTF-8 bytes than the raw range holds. Shrink the
        # represented interval from the newest end to the oldest boundary that
        # fits the protocol byte ceiling, keeping offsets and replacement
        # semantics intact and preserving forward progress.
        if _utf8_byte_length(value) > OUTPUT_CHUNK_MAX_BYTES:
            end, value = _bounded_prefix(chunk_bytes, OUTPUT_CHUNK_MAX_BYTES)
            chunk_end = chunk_start + end
        chunk_id = uuid4()
        chunk_payload = json.dumps(
            build_output_chunk_payload(
                server=server,
                thread=job_id,
                stream=name,
                sequence=sequence,
                start=chunk_start,
                end=chunk_end,
                value=value,
                previous=last_chunk,
                version=version,
            )
        )
        chunks.append((chunk_id, chunk_payload))
        last_chunk = chunk_id
        sequence += 1
        archived_upto = chunk_end
    # Invalid UTF-8 expands to U+FFFD on decode and can push the live-tail head
    # (``tail_start``) forward past the margin ``target``. ``tail_start`` is
    # recomputed as a code-point boundary by ``output_window_text``, so when it
    # exceeds ``target`` the historical prefix up to ``tail_start`` must still be
    # archived; otherwise those raw bytes are never covered and a later trim
    # drops them as an uncovered gap between the chunks and the live tail.
    if tail_start > target:
        while archived_upto < tail_start:
            chunk_start = archived_upto
            candidate = tail_start - stream.spool_start
            # ``tail_start`` is itself a code-point boundary, so the aligned end
            # never snaps backward past the chunk start; this loop therefore
            # always makes forward progress and terminates.
            n_from = max(0, candidate - 3)
            neighborhood = read_range(stream.path, n_from, candidate + 4)
            chunk_end = (
                n_from + align_code_point_end(neighborhood, candidate - n_from) + stream.spool_start
            )
            chunk_bytes = read_range(
                stream.path, chunk_start - stream.spool_start, chunk_end - stream.spool_start
            )
            value = pg_safe_decode(chunk_bytes)
            if _utf8_byte_length(value) > OUTPUT_CHUNK_MAX_BYTES:
                end, value = _bounded_prefix(chunk_bytes, OUTPUT_CHUNK_MAX_BYTES)
                chunk_end = chunk_start + end
            chunk_id = uuid4()
            chunk_payload = json.dumps(
                build_output_chunk_payload(
                    server=server,
                    thread=job_id,
                    stream=name,
                    sequence=sequence,
                    start=chunk_start,
                    end=chunk_end,
                    value=value,
                    previous=last_chunk,
                    version=version,
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
        "    AND " + SERVER_MATCH_SQL + "%(server)s\n"
    )


def _output_update_params(
    job_id: UUID, output: dict[str, dict[str, Any]], server: str
) -> dict[str, object]:
    """Build the parameters for :func:`_output_update_sql`.

    Args:
        job_id: The root job identifier.
        output: The full ``{stdout, stderr}`` window mapping.
        server: The daemon's configured server identity guarding the update.

    Returns:
        The bound parameters.
    """
    return {"job_id": job_id, "output": json.dumps(output), "server": server}


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
    gated: GatedSpawn,
    start_ticks: int,
    settings: Settings,
) -> bool:
    """Persist the exact process identity behind a compare-and-swap guard.

    The identity is written into ``payload.state.process_pid``,
    ``payload.state.process_pgid`` and ``payload.state.process_start_time_ticks``,
    keeping the two-column table invariant. The start-time ticks make later
    emergency recovery PID-reuse-safe: a persisted group id that has been
    recycled by an unrelated process no longer matches the recorded command and
    is never signalled.

    The write is guarded: it only applies to exactly the row this worker still
    owns — same configured server, matching ``state.worker_id`` and
    ``state.worker_incarnation`` (consistent with lease refresh and
    finalization guards), and status ``running`` — so a row that vanished, was
    re-owned by another worker or server, or already left the running state is
    never mutated. The gate may be released only when the update positively
    reports exactly one affected row.

    Args:
        conn: Open PostgreSQL connection.
        job_id: Identifier of the running job.
        gated: Handles and exact identity of the gated start.
        start_ticks: Valid positive start-time ticks of the exact process,
            obtained before the start gate was released.
        settings: Worker runtime settings whose ``server``, ``worker_id`` and
            ``worker_incarnation`` must all match the row for the update to
            apply.

    Returns:
        ``True`` when exactly one owned running row was updated; ``False``
        when zero rows matched (the caller must fail closed).
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
            "UPDATE lubko.jobs\n"
            "SET payload = " + set_chain + "::text\n"
            "WHERE id = %s\n"
            "    AND " + SERVER_MATCH_SQL + "%s\n"
            "    AND (payload::jsonb)->'state'->>'worker_id' = %s\n"
            "    AND (payload::jsonb)->'state'->>'worker_incarnation' = %s\n"
            "    AND (payload::jsonb)->'state'->>'status' = 'running'\n",
            (
                gated.proc.pid,
                gated.pgid,
                start_ticks,
                job_id,
                settings.server,
                settings.worker_id,
                settings.worker_incarnation,
            ),
        )
        return cursor.rowcount == 1


def _process_pgrp(pid: int) -> int | None:
    """Return the exact process group of a running process.

    Args:
        pid: Process ID to inspect.

    Returns:
        The process group ID, or ``None`` if the process is dead or unknown.
    """
    return _shared_process_pgrp(pid)


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


def _parse_owned_running_group_row(
    row_id: object,
    pgid: object,
    start_ticks: object | None,
) -> tuple[int | None, int | None, str]:
    """Parse one selected owned-command identity without dropping corruption.

    A malformed or non-positive *present* PGID becomes ``None`` so the caller
    can retain the job as an explicit blocking obligation instead of silently
    treating it as absent. Start ticks preserve the existing fail-closed rule:
    malformed values become ``None`` and therefore can never authorize a signal.

    Returns:
        Parsed PGID (or ``None`` for malformed/non-positive authority), parsed
        start ticks (or ``None`` when unprovable), and the job id as text.
    """
    try:
        parsed_pgid = int(str(pgid))
    except ValueError:
        pgid_i: int | None = None
    else:
        pgid_i = parsed_pgid if parsed_pgid > 0 else None

    start_i: int | None = None
    if start_ticks is not None:
        try:
            start_i = int(str(start_ticks))
        except ValueError:
            start_i = None
    return pgid_i, start_i, str(row_id)


def _owned_running_groups(
    conn: JobsConnection,
    incarnation: str,
) -> list[tuple[int | None, int | None, str]]:
    """Return persisted owned-command identities for one incarnation.

    Args:
        conn: Open PostgreSQL connection.
        incarnation: The worker incarnation (lifecycle token) to match.

    Returns:
        Triples of the process group id, persisted command start-time ticks,
        and job row id for every selected owned running command. A ``None``
        group id means the durable ``process_pgid`` was present (the query
        excludes genuine absence) but malformed or non-positive, and therefore
        remains an explicit blocking recovery obligation. ``None`` start ticks
        likewise mean missing or malformed exact-start identity.
    """
    groups: list[tuple[int | None, int | None, str]] = []
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
            row_id = row[0]
            pgid = row[1]
            start_ticks = row[2]
            if pgid is None:
                continue
            groups.append(_parse_owned_running_group_row(row_id, pgid, start_ticks))
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
class _OwnedGroupView:
    """Exact-identity view of one recorded command group at observation time.

    Attributes:
        leader_ours: ``True`` when the process currently occupying the leader
            slot (pid == pgid) provably is the recorded command (its live
            start-time ticks match the persisted identity). The whole numeric
            group is then ours by construction and may be signalled as such.
        pids: Live member pids that are provably original members of the
            recorded group even though the leader slot does not prove it (the
            leader exited, or the numeric PGID was recycled by an unrelated
            later leader). Only these pids — never the whole numeric group —
            may be signalled individually.
    """

    leader_ours: bool
    pids: tuple[int, ...]


def _group_member_pids(pgid: int) -> list[int]:
    """Return live pids whose exact process group equals ``pgid``.

    Args:
        pgid: Process group identifier to inspect.

    Returns:
        Every live pid in the exact group (empty when none).
    """
    proc_dir = Path("/proc")
    if proc_dir.is_dir():
        return [
            int(entry.name)
            for entry in proc_dir.iterdir()
            if entry.name.isdigit() and _process_pgrp(int(entry.name)) == pgid
        ]
    try:
        os.getpgid(pgid)
    except ProcessLookupError:
        return []
    return [pgid]


# Exact-signalling primitives are shared with other lifecycle paths; see
# :mod:`lubko._exact_signal`. They remain reachable as module globals so
# tests can substitute them per call site.
_pidfd_open = _shared_open_pidfd
_pidfd_send_signal = _shared_pidfd_send_signal


def _pin_and_signal(pid: int, sig: int, expected_ticks: int) -> bool:
    """Deliver ``sig`` to exactly the process proven as ``pid`` — or nothing.

    The pidfd is opened FIRST, which kernel-pins the numeric pid: the kernel
    cannot recycle that pid for any other process while this descriptor exists,
    even if the process exits. Only after pinning is the identity re-checked
    against ``expected_ticks``; if it matches, the signal goes through
    ``pidfd_send_signal`` and can therefore hit ONLY the pinned, verified
    process. A reuse that happens at any point — before or after proof —
    either fails the ticks re-check or fails the pin itself, so a recycled
    numeric identity is never signalled.

    Args:
        pid: Proven member pid to signal.
        sig: Signal number to deliver.
        expected_ticks: Start-time ticks the pinned process must still show.

    Returns:
        ``True`` only when the signal was delivered to the verified process.
    """
    try:
        pidfd = _pidfd_open(pid)
    except (OSError, AttributeError):
        # AttributeError: the platform resolves no pidfd binding at all —
        # fail closed with a controlled refusal, never a crash.
        LOGGER.debug("process %d could not be pinned", pid)
        return False
    try:
        if proc_start_ticks(pid) != expected_ticks:
            LOGGER.debug("process %d no longer matches its proven identity", pid)
            return False
        _pidfd_send_signal(pidfd, sig)
    except (OSError, AttributeError):
        LOGGER.debug("process %d already gone", pid)
        return False
    else:
        return True
    finally:
        with suppress(OSError):
            os.close(pidfd)


def _signal_owned_pids(
    pids: Iterable[int],
    sig: int,
    ledger: dict[int, int],
    marker: str | None = None,
) -> bool:
    """Signal provably-owned members individually by pinned exact identity.

    Every individual emission is re-validated just-in-time (live start-time
    ticks plus exact ledger identity or exact job marker) and then delivered
    through a pidfd pinned to that exact identity, so PID or PGID recycling
    between classification, re-proof, and the signal syscall itself can never
    redirect the signal to an unrelated process.

    Args:
        pids: Candidate member pids to signal.
        sig: Signal number to deliver.
        ledger: Recorded per-member identities for JIT re-proof.
        marker: Invocation ``LUBKO_JOB_ID`` value for JIT re-proof.

    Returns:
        ``True`` when at least one member was proven (delivery attempted),
        ``False`` when nothing was proven.
    """
    attempted = False
    for pid in pids:
        ticks = _proven_member_ticks(pid, ledger, marker)
        if ticks is None:
            continue
        _pin_and_signal(pid, sig, ticks)
        attempted = True
    return attempted


def _process_job_id(pid: int) -> str | None:
    """Return the invocation-specific job marker of a live process.

    Args:
        pid: Process id whose environment to inspect.

    Returns:
        The exact ``LUBKO_JOB_ID`` value carried by the process, or ``None``
        when its environment is unreadable, empty, or lacks the marker.
    """
    try:
        data = (Path("/proc") / str(pid) / "environ").read_bytes()
    except OSError:
        return None
    prefix = f"{JOB_ID_ENV}=".encode()
    for entry in data.split(b"\0"):
        if entry.startswith(prefix):
            return entry[len(prefix) :].decode("utf-8", errors="replace")
    return None


def _proven_member_ticks(
    pid: int,
    ledger: dict[int, int],
    marker: str | None,
) -> int | None:
    """Return the proven start-time ticks of a live group member, if any.

    Ledger proof is strongest: the member's live start-time ticks exactly equal
    the ticks recorded for that pid under a prior positive ownership proof.
    Failing that, an unproven member is accepted ONLY when its own environment
    carries exactly ``LUBKO_JOB_ID=<this invocation's id>``: the marker is
    injected per spawn, so a newer same-worker job (a different UUID) can never
    satisfy an older invocation's marker. Missing/unreadable/different markers
    fail closed.

    Args:
        pid: Candidate member pid.
        ledger: Recorded per-member identities from positive-proof passes.
        marker: This invocation's exact ``LUBKO_JOB_ID`` value, or ``None``
            when no marker proof is available (ledger-only mode).

    Returns:
        The proven start-time ticks of the member, or ``None`` when it cannot
        be proven ours right now.
    """
    member_ticks = proc_start_ticks(pid)
    if member_ticks is None:
        return None
    if ledger.get(pid) == member_ticks:
        return member_ticks
    if marker is not None and _process_job_id(pid) == marker:
        return member_ticks
    return None


def _ledger_owned_members(
    pgid: int,
    ledger: dict[int, int],
    marker: str | None = None,
) -> tuple[int, ...]:
    """Return group members proven ours by exact identity or exact job marker.

    A member counts as ours when either non-reusable proof holds right now:
    its live start-time ticks exactly equal the ticks recorded for that pid
    under a prior positive ownership proof (the leader slot provably held the
    recorded command), or its environment carries exactly this invocation's
    ``LUBKO_JOB_ID`` marker (see :func:`_proven_member_ticks`). Per-worker
    ancestry is deliberately NOT used, because it is not invocation-specific —
    a newer job of the same worker can recycle the numeric PGID and its
    reparented descendants would then be indistinguishable from the old
    invocation's. Anything never recorded and carrying no exact marker fails
    closed: it is never signalled.

    Args:
        pgid: The recorded process group id.
        ledger: Recorded per-member identities from positive-proof passes.
        marker: This invocation's exact ``LUBKO_JOB_ID`` value, or ``None``.

    Returns:
        The provably-owned member pids (the leader slot itself is excluded).
    """
    return tuple(
        pid
        for pid in _group_member_pids(pgid)
        if pid != pgid and _proven_member_ticks(pid, ledger, marker) is not None
    )


def _classify_group(
    pgid: int,
    start_ticks: int | None,
    ledger: dict[int, int],
    marker: str | None = None,
) -> _OwnedGroupView:
    """Classify a numeric group against a persisted exact identity and ledger.

    Membership is *ours* only under non-reusable proof:

    * the live process at the leader slot (pid == pgid) has start-time ticks
      exactly equal to the persisted identity — the whole numeric group is
      then ours by construction, and every member's exact identity is recorded
      into the ledger for later convergence; or
    * the member matches the positive-proof ledger exactly (pid AND live
      start-time ticks); or
    * the member's own environment carries exactly this invocation's
      ``LUBKO_JOB_ID`` marker (a per-spawn UUID, never satisfiable by a newer
      same-worker job).

    Anything else — a recycled occupant of the leader slot, its descendants,
    or members whose provenance cannot be established — fails closed and is
    never signalled.

    Args:
        pgid: The recorded process group id.
        start_ticks: The persisted command start-time ticks (invalid/zero when
            none was ever obtained).
        ledger: Per-member identities recorded under positive proof.
        marker: This invocation's exact ``LUBKO_JOB_ID`` value, or ``None``.

    Returns:
        The :class:`_OwnedGroupView` proving which parts of the numeric group,
        if any, still belong to the recorded job.
    """
    if not group_has_members(pgid):
        return _OwnedGroupView(leader_ours=False, pids=())
    leader_ticks = proc_start_ticks(pgid)
    if isinstance(start_ticks, int) and start_ticks > 0 and leader_ticks == start_ticks:
        # The recorded command itself provably occupies the leader slot, so
        # every current member is its descendant. Snapshot their identities
        # while this positive proof holds.
        for pid in _group_member_pids(pgid):
            member_ticks = proc_start_ticks(pid)
            if member_ticks is not None:
                ledger[pid] = member_ticks
        return _OwnedGroupView(leader_ours=True, pids=())
    return _OwnedGroupView(
        leader_ours=False,
        pids=_ledger_owned_members(pgid, ledger, marker),
    )


def _owned_group_view(job: ActiveJob) -> _OwnedGroupView:
    """Classify one active job's numeric group against its carried identity.

    Args:
        job: The active job whose recorded group to inspect.

    Returns:
        The :class:`_OwnedGroupView` for the job's exact group.
    """
    return _classify_group(job.pgid, job.start_ticks, job.owned_members, str(job.id))


def _signal_owned_group(job: ActiveJob, sig: int) -> None:
    """Signal only the provably-owned members of a job's recorded group.

    Bare group signalling is deliberately avoided: a ``killpg`` between proof
    and syscall could hit a recycled numeric PGID. Instead every live member —
    including the leader slot when it provably holds the recorded command —
    is signalled individually. Identity is re-proven immediately before every
    emission and delivered through a pidfd pinned to that exact identity (see
    :func:`_pin_and_signal`), so PID or PGID reuse at any point between
    classification, re-proof, and the kernel call can never redirect the
    signal to an unrelated process.

    Args:
        job: The active job whose exact group should receive ``sig``.
        sig: Signal number to deliver.
    """
    view = _owned_group_view(job)
    marker = str(job.id)
    candidates = _group_member_pids(job.pgid) if view.leader_ours else list(view.pids)
    for pid in candidates:
        # Just-in-time per-pid guard: re-read the identity AND the marker at
        # emission time; delivery is then bound to that proven identity.
        ticks = _proven_member_ticks(pid, job.owned_members, marker)
        if ticks is not None:
            _pin_and_signal(pid, sig, ticks)


def _owned_group_alive(job: ActiveJob) -> bool:
    """Return whether any member provably belonging to the job's group lives.

    A numerically recycled PGID occupied by an unrelated process does NOT keep
    the old owned group alive: on identity mismatch the old group is treated
    as gone so terminalization, drain, and shutdown can never be blocked (nor
    authorized to signal) by a stranger.

    Args:
        job: The active job whose recorded group to inspect.

    Returns:
        ``True`` only when at least one provably-owned member is still alive.
    """
    view = _owned_group_view(job)
    return view.leader_ours or bool(view.pids)


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
            but remain a durable blocking obligation exactly like ``surviving``.
        malformed: Job ids whose durable ``process_pgid`` was present but could
            not be parsed as a positive process-group id. They cannot be safely
            inspected or signalled and therefore remain explicit blocking
            recovery obligations until durable authority is repaired.
    """

    reaped: list[int]
    surviving: list[int]
    unresolved: list[int]
    malformed: list[str]


def _terminate_one_group(
    pgid: int,
    start_ticks: int,
    cancel_grace_seconds: float,
    marker: str | None = None,
) -> bool:
    """Ask one exact owned process group to terminate, then SIGKILL and reap it.

    The exact identity (persisted start-time ticks) is re-verified at every
    stage — including immediately before the SIGKILL escalation — so a proof
    that was valid at SIGTERM time cannot go stale. Bare group signalling is
    avoided: member identities are recorded into a local ledger while the
    leader slot provably holds the recorded command, then every live member is
    signalled individually, re-proven at emission time (exact ledger identity
    or exact invocation job marker) and delivered through a pidfd pinned to
    that proven identity, so PID or PGID reuse between classification,
    re-proof, and the kernel call can never redirect a signal. Convergence is
    reported only when the leader is no longer provably ours AND every
    provably-owned member is gone; an unproven live occupant never counts as
    convergence of the old owned group.

    Args:
        pgid: Exact process group id to terminate.
        start_ticks: Persisted start-time ticks proving ownership of pgid.
        cancel_grace_seconds: Grace before escalating to SIGKILL.
        marker: Invocation ``LUBKO_JOB_ID`` value enabling marker-based
            member proof, or ``None`` for ledger-only classification.

    Returns:
        ``True`` only when the old owned group is proven gone — no surviving
        provably-owned member, and any current numeric occupant is a recycled
        stranger that must not be touched.
    """
    ledger: dict[int, int] = {}

    def view() -> _OwnedGroupView:
        return _classify_group(pgid, start_ticks, ledger, marker)

    def emit(view_now: _OwnedGroupView, sig: int) -> bool:
        # Every emission goes member-by-member through pinned identity; the
        # numeric group is never signalled wholesale.
        candidates = _group_member_pids(pgid) if view_now.leader_ours else list(view_now.pids)
        return _signal_owned_pids(candidates, sig, ledger, marker)

    first = view()
    # Identity lost between decision and signal: only members re-proven at
    # emission time may be signalled, never the numeric group.
    if not emit(first, signal.SIGTERM):
        return True
    term_deadline = time.monotonic() + cancel_grace_seconds
    while time.monotonic() < term_deadline and group_has_members(pgid):
        time.sleep(0.05)
    emit(view(), signal.SIGKILL)
    reap_deadline = time.monotonic() + cancel_grace_seconds
    while time.monotonic() < reap_deadline:
        current = view()
        if not (current.leader_ours or current.pids):
            return True
        time.sleep(0.05)
    final = view()
    # Terminal proof must cover the leader AND every provably-owned member.
    return not (final.leader_ours or final.pids)


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
        return ReclaimedGroups(reaped=[], surviving=[], unresolved=[], malformed=[])
    reaped: list[int] = []
    surviving: list[int] = []
    unresolved: list[int] = []
    malformed: list[str] = []
    for pgid, start_ticks, marker in groups:
        if pgid is None:
            malformed.append(marker)
            continue
        decision = _group_reclaim_decision(pgid, start_ticks)
        if decision is GroupReclaimDecision.GONE:
            continue
        if decision is GroupReclaimDecision.RECLAIM:
            if _terminate_one_group(pgid, start_ticks or 0, cancel_grace_seconds, marker):
                reaped.append(pgid)
            else:
                surviving.append(pgid)
        else:
            unresolved.append(pgid)
    return ReclaimedGroups(
        reaped=reaped,
        surviving=surviving,
        unresolved=unresolved,
        malformed=malformed,
    )


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
        # The unreleased gated wrapper is this process's exact DIRECT child in
        # its own childless dedicated group. Keep synchronous local ownership:
        # SIGKILL the exact group ONLY while the direct child is still live,
        # and block — never returning or re-raising on a reap timeout while it
        # remains live — until the child is actually reaped. Once reaped, the
        # original unreleased group is gone by construction and its numeric
        # PGID is deliberately never probed or signalled again, because it
        # could already have been reused by an unrelated process.
        while proc.poll() is None:
            with suppress(OSError):
                os.killpg(proc.pid, signal.SIGKILL)
            time.sleep(0.02)
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


def _unlink_gated_spool_best_effort(path: Path) -> None:
    """Best-effort remove one temporary gated capture spool file.

    Called only after the unreleased gated direct child has been positively
    converged and reaped, so a filesystem fault here can no longer affect any
    process: it must never propagate into the higher-level start-failure or
    connectivity outcome.

    Args:
        path: Capture spool file to remove.
    """
    try:
        path.unlink(missing_ok=True)
    except OSError as exc:
        LOGGER.warning("failed to remove gated spool %s: %s", path, exc)


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
        files were best-effort removed (the unreleased group is gone by
        construction; spool cleanup failure cannot propagate); ``False`` when
        the child remained live after the bounded first attempt,
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
        _unlink_gated_spool_best_effort(stdout_path)
        _unlink_gated_spool_best_effort(stderr_path)
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
    # PGID again (it may already be reused by an unrelated process). Cleanup
    # is bounded best-effort and independent per stream.
    _unlink_gated_spool_best_effort(gated.stdout_path)
    _unlink_gated_spool_best_effort(gated.stderr_path)
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
    execute the same root job. Only ``command`` rows whose top-level
    ``server`` exactly equals the daemon's configured server identity are
    claimed; jobs addressed to other servers stay pending, and immutable
    ``output_chunk`` rows are never claim candidates. The claim is further gated
    to rows whose protocol ``v`` lies inside the daemon's supported version
    window (see :mod:`lubko.protocol_versioning`), so a daemon never locks a job
    it cannot parse or execute and a mixed-version fleet can run a
    non-destructive, staggered upgrade while older in-flight jobs keep running
    on daemons that still advertise the older version.

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
    version_fragment, version_params = claim_version_predicate(settings.supported_protocol_range)
    with conn.transaction(), conn.cursor(row_factory=tuple_row) as cursor:
        cursor.execute(
            "WITH next AS (\n"
            "    SELECT id\n"
            "    FROM lubko.jobs\n"
            "    WHERE (payload::jsonb)->>'type' = 'command'\n"
            + version_fragment
            + "        AND "
            + SERVER_MATCH_SQL
            + "%(server)s\n"
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
                "server": settings.server,
                "worker_id": settings.worker_id,
                "worker_incarnation": settings.worker_incarnation,
                "lease_duration_seconds": settings.lease_duration_seconds,
                "limit": limit,
                **version_params,
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


def request_cancel(conn: JobsConnection, job_id: UUID, *, server: str) -> str:
    """Request cancellation of a job using the documented SQL contract.

    A pending job is cancelled immediately without ever being spawned. A
    running job has its ``state.cancel_requested_at`` marker set and is
    terminated by the worker. An already terminal job is left unchanged. The
    mutation requires the expected top-level ``server`` identity: a row whose
    server differs is never touched and reports its unchanged terminal status.
    All cancellation state lives inside the JSON ``payload``; no table column
    is added.

    Args:
        conn: Open PostgreSQL connection.
        job_id: Identifier of the job to cancel.
        server: Expected server identity of the job.

    Returns:
        The resulting status: ``cancelled``, ``running``, or the unchanged
        terminal status of an already completed job.

    Raises:
        ValueError: If the job does not exist or belongs to another server.
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
            "WHERE id = %(job_id)s AND " + SERVER_MATCH_SQL + "%(server)s\n"
            "    AND (payload::jsonb)->'state'->>'status' = 'pending'\n"
            "RETURNING (payload::jsonb)->'state'->>'status'\n",
            {"job_id": job_id, "server": server},
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
            "WHERE id = %(job_id)s AND " + SERVER_MATCH_SQL + "%(server)s\n"
            "    AND (payload::jsonb)->'state'->>'status' = 'running'\n"
            "RETURNING (payload::jsonb)->'state'->>'status'\n",
            {"job_id": job_id, "server": server},
        )
        row = cursor.fetchone()
        if row is not None:
            return str(row[0])

    with conn.transaction(), conn.cursor(row_factory=tuple_row) as cursor:
        cursor.execute(
            "SELECT (payload::jsonb)->>'server', (payload::jsonb)->'state'->>'status'\n"
            "FROM lubko.jobs WHERE id = %s",
            (job_id,),
        )
        row = cursor.fetchone()
    if row is None:
        msg = f"job {job_id} not found"
        raise ValueError(msg)
    if str(row[0]) != server:
        msg = f"job {job_id} belongs to server {row[0]!r}, not {server!r}"
        raise ValueError(msg)
    return str(row[1])


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
            "    AND " + SERVER_MATCH_SQL + "%(server)s\n"
            "    AND (payload::jsonb)->'state'->>'status' = 'running'\n"
            "    AND (payload::jsonb)->'state'->>'worker_id' = %(worker_id)s\n"
            "    AND (payload::jsonb)->'state'->>'worker_incarnation' = %(worker_incarnation)s\n"
            "    AND id = ANY(%(root_ids)s)\n"
            "RETURNING id\n",
            {
                "server": settings.server,
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
            "    AND " + SERVER_MATCH_SQL + "%(server)s\n"
            "    AND (payload::jsonb)->'state'->>'status' = 'running'\n"
            "    AND (payload::jsonb)->'state'->>'cancel_requested_at' IS NOT NULL\n"
            "    AND (payload::jsonb)->'state'->>'worker_id' = %(worker_id)s\n"
            "    AND (payload::jsonb)->'state'->>'worker_incarnation' = %(worker_incarnation)s\n"
            "LIMIT %(limit)s\n",
            {
                "server": settings.server,
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
        "'; owning server ' || COALESCE("
        "(job.payload::jsonb)->>'server', '<unknown>') || "
        "'; owning worker ' || COALESCE("
        "(job.payload::jsonb)->'state'->>'worker_id', '<unknown>') || "
        "' (incarnation ' || COALESCE("
        "(job.payload::jsonb)->'state'->>'worker_incarnation', '<unknown>') || "
        "') stopped heartbeating; job marked failed rather than re-executed'"
        ")"
        ")"
    )


def recover_stale_jobs(conn: JobsConnection, server: str) -> list[tuple[UUID, str]]:
    """Atomically mark jobs whose lease has truly expired as failed.

    A job whose ``state.lease_expires_at`` is in the past is presumed abandoned
    by a crashed or unreachable worker. Recovery marks it ``failed`` with a
    clear diagnostic and never re-executes it, so two workers can never execute
    the same job. Only ``command`` rows belonging to the daemon's own
    configured ``server`` are considered; rows of other servers are left to
    their owning daemons, and ``output_chunk`` rows are never candidates for
    claim or lease recovery. Rows are locked with ``FOR UPDATE SKIP LOCKED``
    and the status transition is a single atomic update, making the pass safe
    under many concurrent workers.

    A running job without a lease field is never selected: recovery acts only on
    leases that are present and expired, so pre-lease payloads are left for
    manual repair.

    Args:
        conn: Open PostgreSQL connection.
        server: The daemon's configured server identity scoping recovery.

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
            "WITH stale AS (\n"
            "    SELECT id\n"
            "    FROM lubko.jobs\n"
            "    WHERE (payload::jsonb)->>'type' = 'command'\n"
            "        AND " + SERVER_MATCH_SQL + "%(server)s\n"
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
            {"server": server, "limit": LEASE_RECOVERY_LIMIT},
        )
        rows = cursor.fetchall()
    return [(row[0], str(row[1])) for row in rows]


def _read_job_status(conn: JobsConnection, job_id: UUID) -> str | None:
    """Read the current status of a job.

    Args:
        conn: Open PostgreSQL connection.
        job_id: Identifier of the job.

    Returns:
        The current job status, or ``None`` when the root row no longer
        exists (it was deleted concurrently during finalization).
    """
    with conn.transaction(), conn.cursor(row_factory=tuple_row) as cursor:
        cursor.execute(
            "SELECT (payload::jsonb)->'state'->>'status' FROM lubko.jobs WHERE id = %s",
            (job_id,),
        )
        row = cursor.fetchone()
    if row is None:
        LOGGER.warning("root row for job %s disappeared while finalizing", job_id)
        return None
    return str(row[0])


def finish_job(conn: JobsConnection, job_id: UUID, result: JobResult, *, server: str) -> str | None:
    """Persist the final result of a job into its JSON payload.

    A cancellation request accepted before finalization wins over a natural
    completion. Already terminal jobs are never rewritten. The update is
    scoped to the daemon's configured ``server`` so one server can never
    finalize another server's in-flight row. The rolling ``output`` live tails
    written by publication remain in place. Only ``id`` and ``payload`` are
    touched, preserving the two-column table invariant.

    Args:
        conn: Open PostgreSQL connection.
        job_id: Identifier of the job being completed.
        result: Final job result.
        server: The daemon's configured server identity guarding the update.

    Returns:
        The persisted final status, or ``None`` when the root row no longer
        exists because it was deleted concurrently during finalization. A row
        that still exists in another (for example lease-recovered terminal)
        state yields that observed status instead.
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
            "WHERE id = %(job_id)s AND (payload::jsonb)->>'type' = 'command'\n"
            "    AND " + SERVER_MATCH_SQL + "%(server)s\n"
            "    AND (payload::jsonb)->'state'->>'status' = 'running'\n"
            "RETURNING (payload::jsonb)->'state'->>'status'\n",
            {
                "server": server,
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


def _quarantine_job(conn: JobsConnection, job_id: UUID, reason: str, *, server: str) -> bool:
    """Terminalize a job that hit a deterministic database error.

    Writes a ``failed`` terminal status directly, bypassing the normal
    finalization path so publication/finalization data errors cannot block
    terminalization. Only non-terminal rows of the daemon's own configured
    ``server`` are updated: already-terminal rows and rows belonging to other
    servers are left untouched.

    A connectivity error during the terminalization attempt is re-raised
    so the caller can enter outage handling; a different deterministic
    error is logged and returns ``False`` so the caller can retry later.

    Args:
        conn: Open PostgreSQL connection.
        job_id: Identifier of the job to quarantine.
        reason: PostgreSQL-safe diagnostic text (no NUL bytes).
        server: The daemon's configured server identity guarding the update.

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
                "  AND " + SERVER_MATCH_SQL + "%(server)s\n"
                "  AND (payload::jsonb)->'state'->>'status'\n"
                "      NOT IN ('succeeded','failed','cancelled')\n"
                "RETURNING id\n",
                {"job_id": job_id, "reason": safe_reason, "server": server},
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


_REAP_UNSUPPORTED_TEMPLATE: Final = """\
SELECT id, ((payload::jsonb)->'v')::int AS version
FROM lubko.jobs
WHERE (payload::jsonb)->>'type' = 'command'
    AND jsonb_typeof((payload::jsonb)->'server') = 'string'
    AND (payload::jsonb)->>'server' = %(server)s
    AND (payload::jsonb)->'state'->>'status' = 'pending'
ORDER BY (payload::jsonb)->'state'->>'created_at', id
LIMIT %(limit)s
FOR UPDATE SKIP LOCKED
"""


def fail_unsupported_job(
    conn: JobsConnection, job_id: UUID, diagnostic: str, *, server: str
) -> bool:
    """Fail a pending job closed because its protocol version is unservable.

    Mirrors :func:`_quarantine_job` in bypassing the normal finalization CAS
    (which only matches ``running`` rows) so a never-claimed pending job can be
    durably terminalized. The write is scoped to the daemon's server and to
    non-terminal rows, so it can never finalize another server's row or a row
    already terminal.

    Args:
        conn: Open PostgreSQL connection.
        job_id: Identifier of the job to fail closed.
        diagnostic: Human-readable reason (no NUL bytes).
        server: The daemon's configured server identity guarding the update.

    Returns:
        ``True`` when the row was terminalized or was already safe; ``False`` when
        the write failed and must be retried.

    Raises:
        psycopg.Error: When the error is a connectivity issue.
    """
    safe_diagnostic = diagnostic.replace("\x00", "\ufffd")
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
                "    '{state,unsupported_protocol_version_reason}', to_jsonb(%(reason)s::text)\n"
                "  )\n"
                ")::text\n"
                "WHERE id = %(job_id)s\n"
                "  AND (payload::jsonb)->>'type' = 'command'\n"
                "  AND " + SERVER_MATCH_SQL + "%(server)s\n"
                "  AND (payload::jsonb)->'state'->>'status'\n"
                "      NOT IN ('succeeded','failed','cancelled')\n"
                "RETURNING id\n",
                {"job_id": job_id, "reason": safe_diagnostic, "server": server},
            )
            cursor.fetchone()
    except psycopg.Error as exc:
        if _is_connectivity_error_check(exc, conn):
            raise
        LOGGER.exception(
            "unsupported-version terminalization for job %s failed (SQLSTATE %s)",
            job_id,
            exc.sqlstate or "N/A",
        )
        return False
    return True


def reap_unsupported_jobs(conn: JobsConnection, settings: Settings, limit: int) -> list[UUID]:
    """Fail closed pending jobs whose protocol version no daemon can serve.

    A fleet-wide safety net so a pending ``command`` job submitted at a version no
    running daemon understands can never sit stranded forever. The reaper only
    touches jobs whose version is unservable by the *entire* fleet, decided by
    :func:`lubko.protocol_versioning.reaper_disposition`, so it never destroys
    work a different daemon could still execute (for example a ``v5`` job during a
    ``[4,5]`` staggered upgrade while older ``[4,4]`` daemons still run). The pass
    is bounded to ``limit`` rows per turn.

    Args:
        conn: Open PostgreSQL connection.
        settings: Worker runtime settings (server identity and supported window).
        limit: Maximum number of candidate rows to scan and potentially reap.

    Returns:
        The identifiers of the jobs failed closed this pass.
    """
    reaped: list[UUID] = []
    with conn.transaction(), conn.cursor(row_factory=tuple_row) as cursor:
        cursor.execute(_REAP_UNSUPPORTED_TEMPLATE, {"server": settings.server, "limit": limit})
        for job_id, version in cursor.fetchall():
            if version is None:
                continue
            if (
                reaper_disposition(int(version), settings.supported_protocol_range)
                is JobVersionDisposition.FAIL_CLOSED
            ):
                diagnostic = (
                    f"protocol version {version} is unsupported by every running "
                    f"daemon (supported window [{settings.supported_protocol_range.min}, "
                    f"{settings.supported_protocol_range.max}]); the job is failed closed"
                )
                if fail_unsupported_job(conn, job_id, diagnostic, server=settings.server):
                    reaped.append(job_id)
    return reaped


def delete_job_and_chunks(conn: JobsConnection, job_id: UUID, *, server: str) -> None:
    """Delete a root job and every output chunk explicitly owned by it.

    Cleanup uses explicit ``thread`` ownership rather than trusting the
    ``previous`` pointer chain, so orphaned chunks whose chain became
    incomplete because of a crash or corruption are also removed. The root row
    is deleted first and the owned chunks in a separate statement, all within
    one transaction.

    Every mutation is scoped to the expected top-level ``server`` identity:
    the root deletion requires an exact JSON string server match, and the
    owned-chunk deletion only removes chunks stamped with the same server. A
    root belonging to another server is never touched and fails closed with
    :class:`ValueError`; when the root is already missing, same-server orphan
    chunks are still cleaned up.

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
        server: Expected server identity of the root job and its chunks.

    Raises:
        ValueError: If the root row exists but belongs to another server;
            nothing is deleted in that case.
    """
    with conn.transaction(), conn.cursor() as cursor:
        cursor.execute(
            "SELECT (payload::jsonb)->>'server'\nFROM lubko.jobs\nWHERE id = %(job_id)s\n",
            {"job_id": job_id},
        )
        row = cursor.fetchone()
        if row is not None and str(row[0]) != server:
            msg = f"job {job_id} belongs to server {row[0]!r}, not {server!r}"
            raise ValueError(msg)
        cursor.execute(
            "DELETE FROM lubko.jobs\n"
            "WHERE id = %(job_id)s\n"
            "    AND (payload::jsonb)->>'type' = 'command'\n"
            "    AND " + SERVER_MATCH_SQL + "%(server)s\n",
            {"job_id": job_id, "server": server},
        )
        cursor.execute(
            "DELETE FROM lubko.jobs\n"
            "WHERE (payload::jsonb)->>'type' = 'output_chunk'\n"
            "    AND lower((payload::jsonb)->>'thread') = lower(%(thread)s)\n"
            "    AND " + SERVER_MATCH_SQL + "%(server)s\n",
            {"thread": str(job_id), "server": server},
        )


def _gc_phase_bound_hit(
    marked: int, gc_roots: int, chunk_counts: list[int], orphans: int, limit: int
) -> bool:
    """Return whether any GC phase saturated its per-phase batch bound.

    Each GC phase selects/deletes through an independent ``LIMIT``.  A bound is
    hit only when one phase's own capped selection reached ``limit`` (a
    saturation-pressure signal).  This is deliberately *not* the summed row
    count: the phases are independently capped, so their total can equal or
    exceed ``limit`` even when no single phase was saturated, which would be a
    false-positive saturation signal.

    Args:
        marked: Phase-1 marked-root count.
        gc_roots: Phase-2 selected GC-root count.
        chunk_counts: Per-root phase-2 chunk-deletion counts.
        orphans: Phase-3 orphan-deletion count.
        limit: The configured ``gc_batch_limit``.

    Returns:
        ``True`` when at least one phase reached its bound (saturated).
    """
    if limit <= 0:
        return False
    if marked >= limit or gc_roots >= limit or orphans >= limit:
        return True
    return any(count >= limit for count in chunk_counts)


def collect_transport(
    conn: JobsConnection, settings: Settings
) -> tuple[list[UUID], int, int, bool]:
    """Collect terminal command rows, their owned chunks, and orphan chunks.

    Three bounded phases run in separate transactions, each with ``FOR UPDATE
    SKIP LOCKED`` so multiple workers/restarts converge safely. Every phase is
    scoped to the daemon's configured ``server`` identity so one server's
    daemon never collects another server's transport rows.

    **Phase 1 — Mark** (one transaction): A bounded batch of terminal
    ``command`` rows whose ``finished_at`` is older than the retention window
    is selected and atomically marked with ``state.gc = true``.  Publication
    explicitly refuses GC-marked roots, so no new ``output_chunk`` rows can be
    created for them after the mark commits.  Only rows with
    ``status IN ('succeeded', 'failed', 'cancelled')`` are eligible; unknown
    or future statuses are retained.  Abandoned ``running`` rows are handled
    by :func:`recover_stale_jobs`.

    **Phase 2 — Chunk drain + root finalization** (one transaction): Exactly
    one GC-marked root whose terminal status and canonical ``finished_at`` still
    prove retention eligibility is selected, and at most ``gc_batch_limit`` of
    its owned
    chunks (via ``thread``) are deleted using ``FOR UPDATE SKIP LOCKED``.
    Processing one root gives the pass a constant SQL-round-trip bound; the
    batch limit cannot multiply into work for many roots. After bounded chunk
    deletion, the root is deleted if no chunks remain. The root only
    disappears after its chunks are drained, which
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

    The returned ``gc_batch_bound_hit`` flag is the accurate saturation
    signal: it is ``True`` only when an actual bounded selection/deletion
    reached its per-phase ``LIMIT`` (a pressure/saturation condition).  It is
    intentionally *not* derived from the aggregate row counts, because the
    phases are independently capped and their sum can reach or exceed the
    limit even when no single phase was saturated.

    Args:
        conn: Open PostgreSQL connection.
        settings: Worker runtime settings.

    Returns:
        A ``(roots_marked, chunks_deleted, orphans_deleted, gc_batch_bound_hit)``
        tuple.
    """
    limit = settings.gc_batch_limit
    roots_marked: list[UUID] = []
    roots_deleted = 0
    total_chunks = 0
    total_orphans = 0
    per_root_chunk_counts: list[int] = []

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
            "        AND " + SERVER_MATCH_SQL + "%(server)s\n"
            "        AND (payload::jsonb)->'state'->>'status'\n"
            "            IN ('succeeded', 'failed', 'cancelled')\n"
            "        AND jsonb_typeof((payload::jsonb)->'state'->'finished_at') = 'string'\n"
            "        AND ((payload::jsonb)->'state'->>'finished_at')\n"
            "            ~ %(gc_finished_at_pattern)s\n"
            "        AND left((payload::jsonb)->'state'->>'finished_at', 4) <> '0000'\n"
            "        AND ((payload::jsonb)->'state'->>'finished_at') < gc_params.cutoff\n"
            "        AND ((payload::jsonb)->'state'->>'gc') IS DISTINCT FROM 'true'\n"
            "    ORDER BY ((payload::jsonb)->'state'->>'finished_at'), id\n"
            "    FOR UPDATE SKIP LOCKED\n"
            "    LIMIT %(limit)s\n"
            ")\n"
            "RETURNING id\n",
            {
                "server": settings.server,
                "gc_retention_seconds": settings.gc_retention_seconds,
                "gc_finished_at_pattern": GC_FINISHED_AT_PATTERN,
                "limit": limit,
            },
        )
        roots_marked = [row[0] for row in cursor.fetchall()]

    # --- Phase 2: bounded chunk drain + root finalization ---
    with conn.transaction(), conn.cursor(row_factory=tuple_row) as cursor:
        cursor.execute(
            "WITH gc_params AS (\n"
            "    SELECT to_char(\n"
            "        now() at time zone 'utc'\n"
            "        - make_interval(secs => %(gc_retention_seconds)s),\n"
            '        \'YYYY-MM-DD"T"HH24:MI:SS.US"Z"\'\n'
            "    ) AS cutoff\n"
            ")\n"
            "SELECT id\n"
            "FROM lubko.jobs, gc_params\n"
            "WHERE (payload::jsonb)->>'type' = 'command'\n"
            "    AND " + SERVER_MATCH_SQL + "%(server)s\n"
            "    AND (payload::jsonb)->'state'->>'status'\n"
            "        IN ('succeeded', 'failed', 'cancelled')\n"
            "    AND jsonb_typeof((payload::jsonb)->'state'->'finished_at') = 'string'\n"
            "    AND ((payload::jsonb)->'state'->>'finished_at')\n"
            "        ~ %(gc_finished_at_pattern)s\n"
            "    AND left((payload::jsonb)->'state'->>'finished_at', 4) <> '0000'\n"
            "    AND ((payload::jsonb)->'state'->>'finished_at') < gc_params.cutoff\n"
            "    AND ((payload::jsonb)->'state'->>'gc') = 'true'\n"
            "ORDER BY id\n"
            "FOR UPDATE SKIP LOCKED\n"
            "LIMIT %(limit)s\n",
            {
                "server": settings.server,
                "gc_retention_seconds": settings.gc_retention_seconds,
                "gc_finished_at_pattern": GC_FINISHED_AT_PATTERN,
                "limit": limit,
            },
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
                "        AND " + SERVER_MATCH_SQL + "%(server)s\n"
                "        AND lower((payload::jsonb)->>'thread') = lower(%(thread)s)\n"
                "    FOR UPDATE SKIP LOCKED\n"
                "    LIMIT %(limit)s\n"
                ")\n",
                {
                    "server": settings.server,
                    "thread": str(root_id),
                    "limit": limit,
                },
            )
            total_chunks += cursor.rowcount
            per_root_chunk_counts.append(cursor.rowcount)
            # Delete root only if no chunks remain.
            cursor.execute(
                "DELETE FROM lubko.jobs AS root\n"
                "WHERE root.id = %(job_id)s\n"
                "    AND NOT EXISTS (\n"
                "        SELECT 1\n"
                "        FROM lubko.jobs AS chunk\n"
                "        WHERE chunk.payload::jsonb->>'type' = 'output_chunk'\n"
                "            AND "
                + SERVER_MATCH_SQL.replace("payload", "chunk.payload")
                + "%(server)s\n"
                "            AND lower(chunk.payload::jsonb->>'thread') = lower(%(thread)s)\n"
                "    )\n",
                {"job_id": root_id, "server": settings.server, "thread": str(root_id)},
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
            "    AND jsonb_typeof(chunk.payload::jsonb->'server') = 'string'\n"
            "    AND chunk.payload::jsonb->>'server' = %(server)s\n"
            "    AND NOT EXISTS (\n"
            "        SELECT 1\n"
            "        FROM lubko.jobs AS root\n"
            "        WHERE lower(root.id::text) =\n"
            "            lower(chunk.payload::jsonb->>'thread')\n"
            "            AND root.payload::jsonb->>'type' = 'command'\n"
            "    )\n"
            "LIMIT %(limit)s\n"
            "FOR UPDATE OF chunk SKIP LOCKED\n",
            {"server": settings.server, "limit": limit},
        )
        orphan_ids = [row[0] for row in cursor.fetchall()]
        if orphan_ids:
            cursor.execute(
                "DELETE FROM lubko.jobs\nWHERE id = ANY(%(ids)s)\n",
                {"ids": orphan_ids},
            )
            total_orphans += len(orphan_ids)

    batch_bound_hit = _gc_phase_bound_hit(
        len(roots_marked), len(gc_roots), per_root_chunk_counts, len(orphan_ids), limit
    )
    if roots_deleted:
        LOGGER.info("gc deleted %d root(s)", roots_deleted)
    return roots_marked, total_chunks, total_orphans, batch_bound_hit


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
    """Assert that ``lubko.jobs`` carries the canonical protocol v4 shape.

    The two-column invariant alone does not make a table usable by a v4
    worker: immutable ``output_chunk`` publication and explicit multi-server
    routing require the type-aware ``jobs_payload_type_shape`` check
    constraint **in its v4 form** — enforcing the required non-empty top-level
    ``server`` field — plus the chunk ownership/ordering indexes, which
    ``migrations/0001_two_column_protocol.sql`` declares. An existing table
    still carrying the pre-cutover v3 constraint (same name, no ``server``
    enforcement) is refused exactly like a missing one. The worker refuses to
    start against any table lacking this shape so output publication can never
    fail at runtime on a table that cannot represent immutable chunks, and no
    daemon ever runs against a transport that does not DB-enforce server
    routing.

    Args:
        conn: Open PostgreSQL connection.

    Raises:
        SchemaInvariantError: If the type-aware constraint (in its v4,
            routing-aware form) or any required output-chunk index is missing.
    """
    with conn.transaction(), conn.cursor(row_factory=tuple_row) as cursor:
        cursor.execute(
            "SELECT conname, pg_get_constraintdef(oid)\n"
            "FROM pg_constraint\n"
            "WHERE conrelid = to_regclass(%s) AND contype = 'c'\n",
            (f"{JOBS_SCHEMA}.{JOBS_TABLE}",),
        )
        constraints = {str(row[0]): str(row[1]) for row in cursor.fetchall()}
        cursor.execute(
            "SELECT indexname\nFROM pg_indexes\nWHERE schemaname = %s AND tablename = %s\n",
            (JOBS_SCHEMA, JOBS_TABLE),
        )
        indexes = {str(row[0]) for row in cursor.fetchall()}
    missing: list[str] = []
    shape_def = constraints.get(TYPE_AWARE_CONSTRAINT_NAME)
    if shape_def is None:
        missing.append(f"check constraint {TYPE_AWARE_CONSTRAINT_NAME}")
    elif not all(m in "".join(shape_def.split()) for m in SERVER_ROUTING_CONSTRAINT_MARKERS):
        msg = (
            f"lubko.jobs carries a pre-v4 {TYPE_AWARE_CONSTRAINT_NAME} check "
            f"constraint without server-routing enforcement. Apply the "
            f"protocol v4 cutover migration "
            f"migrations/0003_protocol_v4_server_routing.sql while quiescent, "
            f"then truncate lubko.jobs (the v3 -> v4 row cutover is "
            f"destructive; no legacy row is converted). {TWO_COLUMN_INVARIANT}"
        )
        raise SchemaInvariantError(msg)
    missing.extend(
        f"index {name}"
        for name in (CHUNK_OWNER_INDEX_NAME, CHUNK_ORDER_INDEX_NAME)
        if name not in indexes
    )
    if missing:
        detail = ", ".join(missing)
        msg = (
            f"lubko.jobs lacks the canonical output-chunk schema shape "
            f"required for immutable output publication and server routing: "
            f"missing {detail}. Apply the canonical, idempotent baseline "
            f"migration migrations/0001_two_column_protocol.sql (fresh "
            f"installs) or the protocol v4 cutover migration "
            f"migrations/0003_protocol_v4_server_routing.sql (existing v3 "
            f"tables; then truncate lubko.jobs while quiescent — the row "
            f"cutover is destructive). {TWO_COLUMN_INVARIANT}"
        )
        raise SchemaInvariantError(msg)


SERVER_ISOLATION_FUNCTION: Final = "lubko.session_server"
JOBS_RLS_POLICY_PREFIX: Final = "jobs_isolation"


def verify_server_isolation(conn: JobsConnection) -> None:
    """Assert the PostgreSQL per-server authorization boundary is in place.

    The worker still scopes every query by its configured server identity
    (defense in depth), but cross-server isolation must also be enforced at the
    database authorization boundary so a compromised or misconfigured worker
    credential cannot read, mutate, or spoof another execution server's rows, and
    so an output chunk can only reference a command root of its own server. The
    boundary is row-level security on ``lubko.jobs``, the trusted
    ``lubko.session_server()`` identity function, the same-server chunk
    enforcement (the ``enforce_chunk_root_server()`` trigger function, which
    inlines the root lookup), and the per-server isolation policies. The worker
    refuses to run without it, failing closed.

    Args:
        conn: Open PostgreSQL connection.

    Raises:
        SchemaInvariantError: If row-level security is not enabled, the
            session-server identity function is missing, the same-server chunk
            enforcement is missing, or no isolation policy is present.
    """
    with conn.transaction(), conn.cursor(row_factory=tuple_row) as cursor:
        cursor.execute(
            "SELECT relrowsecurity\nFROM pg_class\nWHERE oid = to_regclass(%s)\n",
            (f"{JOBS_SCHEMA}.{JOBS_TABLE}",),
        )
        rls_row = cursor.fetchone()
        rls_enabled = bool(rls_row and rls_row[0])
        cursor.execute(
            "SELECT 1\n"
            "FROM pg_proc p\n"
            "JOIN pg_namespace n ON n.oid = p.pronamespace\n"
            "WHERE n.nspname = %s AND p.proname = 'session_server'\n",
            (JOBS_SCHEMA,),
        )
        session_function_exists = cursor.fetchone() is not None
        cursor.execute(
            "SELECT 1\n"
            "FROM pg_proc p\n"
            "JOIN pg_namespace n ON n.oid = p.pronamespace\n"
            "WHERE n.nspname = %s AND p.proname = 'enforce_chunk_root_server'\n",
            (JOBS_SCHEMA,),
        )
        chunk_function_exists = cursor.fetchone() is not None
        cursor.execute(
            "SELECT 1\n"
            "FROM pg_trigger t\n"
            "JOIN pg_class c ON c.oid = t.tgrelid\n"
            "WHERE c.relnamespace = to_regnamespace(%s) AND c.relname = %s\n"
            "    AND t.tgname = 'jobs_chunk_root_server'\n",
            (JOBS_SCHEMA, JOBS_TABLE),
        )
        chunk_trigger_exists = cursor.fetchone() is not None
        cursor.execute(
            "SELECT polname\nFROM pg_policies\nWHERE schemaname = %s AND tablename = %s\n",
            (JOBS_SCHEMA, JOBS_TABLE),
        )
        policies = [str(row[0]) for row in cursor.fetchall()]
    missing: list[str] = []
    if not rls_enabled:
        missing.append("row-level security on lubko.jobs")
    if not session_function_exists:
        missing.append(f"{SERVER_ISOLATION_FUNCTION}() identity function")
    if not chunk_function_exists:
        missing.append("lubko.enforce_chunk_root_server() same-server ownership function")
    if not chunk_trigger_exists:
        missing.append("jobs_chunk_root_server same-server chunk trigger")
    if not any(p.startswith(JOBS_RLS_POLICY_PREFIX) for p in policies):
        missing.append("per-server isolation policies on lubko.jobs")
    if missing:
        detail = ", ".join(missing)
        msg = (
            "lubko.jobs is not protected by the per-server PostgreSQL "
            f"authorization boundary: missing {detail}. Apply the server "
            "isolation migration migrations/0004_server_isolation_boundary.sql "
            "and provision per-server worker roles. "
            f"{TWO_COLUMN_INVARIANT}"
        )
        raise SchemaInvariantError(msg)


def verify_server_identity(conn: JobsConnection, server: str) -> None:
    """Bind the live session to the configured execution-server identity.

    Resolves the server identity the database has assigned to the connected
    login principal through ``lubko.session_server()`` and refuses to run unless
    it exactly matches the daemon's configured server. This makes the PostgreSQL
    session provably bound to exactly one execution-server identity: a worker
    cannot run under a principal mapped to a different server, and a principal
    with no mapping (or the wrong one) is rejected fail-closed.

    Args:
        conn: Open PostgreSQL connection.
        server: The daemon's configured, non-empty server identity.

    Raises:
        SchemaInvariantError: If the database-assigned server is ``None`` or
            differs from the configured ``server``.
    """
    with conn.transaction(), conn.cursor(row_factory=tuple_row) as cursor:
        cursor.execute("SELECT lubko.session_server()")
        identity_row = cursor.fetchone()
    bound = str(identity_row[0]) if identity_row and identity_row[0] is not None else None
    if bound != server:
        msg = (
            f"database session is not bound to server {server!r}: the connected "
            f"principal resolves to {bound!r} via {SERVER_ISOLATION_FUNCTION}(). "
            "Check the lubko.server_principals mapping and the per-server worker "
            "role in database.conf (user=). The worker refuses to run against a "
            "session that is not bound to exactly one execution-server identity."
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
        self._next_reaper_at = 0.0
        self._started_at = time.time()
        self._start_time_ticks = proc_start_ticks(os.getpid()) or 0
        self._db_connected_at: float | None = None
        self._db_error_at: float | None = None
        self._last_completed_job_id: str | None = None
        self._last_completed_at: float | None = None
        self._last_completed_status: str | None = None
        self._completed_count = 0
        self._last_claim_batch = 0
        self._last_db_activity_at: float | None = None
        self._db_deadline_breached_at: float | None = None
        self._db_deadline_breach_count = 0
        self._last_cancellation_scan_at: float | None = None
        self._last_recovery_at: float | None = None
        self._last_gc_at: float | None = None
        self._gc_batch_bound_hit = False
        self._cancellation_batch_bound_hit = False
        self._recovery_batch_bound_hit = False
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
            except DbOperationDeadlineError:
                # A hung established operation breached its hard client
                # deadline mid-turn. Record the breach explicitly (distinct
                # from mere DB activity recency), then enter outage handling
                # and enforce lease safety immediately rather than sleeping a
                # turn so an owned group can never outlive its database lease.
                self._record_db_deadline_breach()
                self._enter_outage()
                self._enforce_lease_safety()
            except psycopg.Error as exc:
                if self._is_connectivity_error(exc):
                    self._enter_outage()
                    self._enforce_lease_safety()
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
        install_operation_deadline(self.conn, self._operation_deadline(now))
        self._db_phase(now)

    def _operation_deadline(self, now_mono: float) -> float:
        """Return the hard client deadline for this turn's database operations.

        Derived from the earliest live owned job's lease-safety instant (see
        :func:`operation_deadline_at`), so no single hung database operation
        can ever block the supervisor past a local lease-safety deadline.

        Args:
            now_mono: Current monotonic time.

        Returns:
            The absolute monotonic operation deadline.
        """
        origins = [
            job.last_heartbeat_at
            for job in self.active.values()
            if not job.completed and not job.term_sent and job.last_heartbeat_at > 0.0
        ]
        return operation_deadline_at(now_mono, origins, self.settings)

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
        self._last_db_activity_at = time.time()
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
        if not self._stopping:
            self._claim_batch()
        # Optional maintenance follows claiming. Even if the connection fails
        # or its lease-safe deadline is reached during GC, pending queue work
        # has already received its bounded opportunity in this turn.
        if now >= self._next_gc_at:
            self._run_gc()
            self._next_gc_at = time.monotonic() + self.settings.gc_interval_seconds

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
        room and drains more (see :meth:`finalize_completed_job_bounded`), so a
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
                self._next_reaper_at = 0.0
            else:
                self._next_reconnect_at = time.monotonic() + max(
                    self.settings.poll_interval_seconds, 0.5
                )

    def _run_recovery(self) -> None:
        """Run the stale-job recovery pass and stop any recovered own jobs."""
        conn = self.conn
        if conn is None:
            return
        self._last_recovery_at = time.time()
        recovered = recover_stale_jobs(conn, self.settings.server)
        self._recovery_batch_bound_hit = len(recovered) >= LEASE_RECOVERY_LIMIT
        for job_id, _payload in recovered:
            LOGGER.warning(
                "recovered stale job %s: lease expired; marked failed rather than re-executed",
                job_id,
            )
            job = self.active.get(job_id)
            if job is not None:
                job.row_lost = True
                request_stop(job, STOP_REASON_ROW_LOST)

    def _run_reaper(self) -> None:
        """Fail closed pending jobs at a protocol version no daemon can serve."""
        conn = self.conn
        if conn is None:
            return
        reaped = reap_unsupported_jobs(conn, self.settings, LEASE_RECOVERY_LIMIT)
        if reaped:
            LOGGER.warning(
                "reaped %d pending job(s) at an unsupported protocol version", len(reaped)
            )

    def _run_gc(self) -> None:
        """Run the transport garbage collection pass.

        Three-phase staged GC: mark terminal roots, drain one root's chunks in
        a bounded batch, finalize that root when empty, then clean orphan chunks.
        Abandoned ``running`` rows go through lease recovery first.
        ``pending`` and ``running`` rows are never collected.
        """
        conn = self.conn
        if conn is None:
            return
        self._last_gc_at = time.time()
        roots, chunks, orphans, bound_hit = collect_transport(conn, self.settings)
        self._gc_batch_bound_hit = bound_hit
        if roots or chunks or orphans:
            LOGGER.info(
                "gc marked %d root(s), deleted %d chunk(s), cleaned %d orphan(s)%s",
                len(roots),
                chunks,
                orphans,
                "; batch bound hit (saturated)" if bound_hit else "",
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
            if _quarantine_job(
                conn, job_id, f"retry terminalization for {job_id}", server=self.settings.server
            ):
                self._retry_terminations.pop(job_id, None)
            else:
                state.retries += 1
                state.next_retry_at = now + QUARANTINE_RETRY_BASE_SECONDS * (2**state.retries)

    def _discover_cancellations(self) -> None:
        """Terminate any owned running job whose cancellation marker was set."""
        conn = self.conn
        if conn is None:
            return
        self._last_cancellation_scan_at = time.time()
        found = discover_cancellations(conn, self.settings)
        self._cancellation_batch_bound_hit = len(found) >= CANCEL_DISCOVERY_LIMIT
        for job_id in found:
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
            published = publish_output(conn, job, changed, now, server=self.settings.server)
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
            if _quarantine_job(
                conn, job.id, f"publication error: {exc}", server=self.settings.server
            ):
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
            if job.quarantined and job.completed and not _owned_group_alive(job):
                cleanup_job(job)
                job.finalized = True
                self.active.pop(job.id, None)
                continue
            if (
                job.quarantine_pending
                and job.completed
                and not _owned_group_alive(job)
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
                if _quarantine_job(
                    conn, job.id, f"quarantine retry for {job.id}", server=self.settings.server
                ):
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
            self.finalize_completed_job_bounded(job)
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
        if not (job.completed and not _owned_group_alive(job)):
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
            if _quarantine_job(
                conn,
                job.id,
                f"capture failure finalize error: {exc}",
                server=self.settings.server,
            ):
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
        physical spool is never exceeded. A spool stat failure raises
        :class:`SpoolCaptureError` so the exact job is failed closed rather
        than assuming zero.

        Args:
            job: The completed active job whose streams to drain.
            bound: Maximum physical spool size in bytes (always enforced).

        Returns:
            ``True`` when at least one stream delivered bytes or reached EOF
            this turn, ``False`` when no stream made real progress: a pipe
            that is merely still open with no data available right now is not
            progress, so the caller can apply its publish/trim and end-of-file
            grace logic instead of spinning.

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
                    raise SpoolCaptureError(job.id, name, other.path) from exc
            try:
                size_before = stream.path.stat().st_size
            except OSError as exc:
                # A spool stat failure must fail the exact job closed, never
                # be assumed zero (which would risk finalizing a half-
                # captured job).
                raise SpoolCaptureError(job.id, name, stream.path) from exc
            was_eof = stream.eof
            status = drain_capture_stream(stream, bound, aggregate_used=total)
            if status == "error":
                job.spool_evicted = True
                request_stop(job, STOP_REASON_SPOOL)
                return False
            # ``"ok"`` covers both real drains and a pipe that is merely still
            # open with no data available right now. Only landed bytes (spool
            # growth) or an end-of-file transition are genuine progress:
            # counting an empty non-blocking read as progress would let the
            # bounded cycle spin forever on a completed job whose pipe write
            # end is held open by a detached grandchild.
            try:
                size_after = stream.path.stat().st_size
            except OSError as exc:
                raise SpoolCaptureError(job.id, name, stream.path) from exc
            if status == "eof" or (stream.eof and not was_eof) or size_after > size_before:
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
            published = publish_output(
                conn, job, list(OUTPUT_STREAMS), now, server=self.settings.server, force=True
            )
        except SpoolCaptureError:
            return "capture"
        except (psycopg.Error, OSError) as exc:
            if isinstance(exc, psycopg.Error) and self._is_connectivity_error(exc):
                raise
            if _quarantine_job(
                conn, job.id, f"bounded publication error: {exc}", server=self.settings.server
            ):
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

    def finalize_completed_job_bounded(self, job: ActiveJob) -> None:
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

        A completed job's capture-pipe write end can be held open indefinitely
        by a detached grandchild the job forked before exiting (deployment
        helpers do this by design). Such a writer keeps the pipe non-EOF — and
        possibly trickling bytes — forever, so after a bounded grace period
        with no end-of-file the cycle drains one last time, durably publishes
        everything already captured, and finalizes with that content instead
        of spinning: every byte read from the pipe is still represented, the
        physical spool stays within its bound, and the worker can never be
        wedged past a terminal job by an unkillable foreign writer.

        Args:
            job: The completed active job whose output is being finalized.
            conn: Open PostgreSQL connection.
            bound: Maximum physical spool size in bytes (always enforced).

        Returns:
            ``True`` when both streams reached EOF — or the detached-writer
            grace expired with everything captured durably published — so
            finalization may proceed; ``False`` when the exact job was failed
            closed or handed to its quarantine/loss path and the caller must
            stop.
        """
        now = time.monotonic()
        eof_deadline = now + DETACHED_CAPTURE_EOF_GRACE_SECONDS
        for _ in range(100_000):
            if _streams_at_eof(job):
                return True
            verdict = self._bounded_cycle_turn(job, conn, bound, eof_deadline, now)
            if verdict == "eof":
                return True
            if verdict == "stop":
                return False
        # Could not reach EOF within the bounded cycle; fail the exact job
        # closed instead of growing the disk or discarding output.
        self._fail_capture_closed(job)
        return False

    def _drain_or_fail_closed(self, job: ActiveJob, bound: int) -> tuple[bool, bool]:
        """Drain a completed job's streams, converting spool stat failures.

        Args:
            job: The completed active job whose streams to drain.
            bound: Maximum physical spool size in bytes (always enforced).

        Returns:
            A ``(progressed, failed_closed)`` pair: whether any stream made
            real progress, and whether a spool stat failure was converted into
            a capture failure for this exact job (the caller must stop
            finalizing it via its ``"capture"`` outcome path).
        """
        try:
            return self._drain_completed_streams(job, bound), False
        except SpoolCaptureError:
            # A spool stat failure during completed-job drain must enter the
            # same exact-job fail-closed path as any other unreadable spool,
            # never escape bounded finalization as a raw filesystem error.
            return False, True

    def _bounded_cycle_turn(
        self,
        job: ActiveJob,
        conn: JobsConnection,
        bound: int,
        eof_deadline: float,
        now: float,
    ) -> str:
        """Run one bounded-finalization turn and classify what the caller must do.

        Args:
            job: The completed active job being finalized.
            conn: Open PostgreSQL connection.
            bound: Maximum physical spool size in bytes (always enforced).
            eof_deadline: Monotonic time after which a still-open capture pipe
                is treated as held by a detached writer and abandoned.
            now: Cycle-start monotonic time used as the publication timestamp.

        Returns:
            ``"eof"`` when both streams reached EOF (or were abandoned within
            contract) and finalization may proceed; ``"stop"`` when the job
            was failed closed or handed to its quarantine/loss path; ``"wait"``
            when the cycle should keep draining.
        """
        progressed, failed_closed = self._drain_or_fail_closed(job, bound)
        # A drain-time spool stat failure already failed the exact job closed;
        # classify that as a ``"capture"`` outcome so this turn stops without
        # publishing or re-reading the bad spool.
        used_before = None if failed_closed else _spool_used_bytes(job)
        outcome = "capture" if failed_closed else self._publish_bounded(conn, job, now)
        if outcome == "capture":
            self._fail_capture_closed(job)
            return "stop"
        if outcome != "ok":
            # Quarantined or lost: the job is owned by its quarantine/loss
            # path, so finalization must not continue here.
            return "stop"
        made_room = progressed or _spool_shrank(job, used_before)
        if _streams_at_eof(job):
            return "eof"
        if time.monotonic() >= eof_deadline:
            # A detached grandchild holds the pipe write end open: EOF will
            # never arrive on its own. Everything read so far is durably
            # published (the last pass was "ok"); abandon the pipe and
            # finalize rather than wedge the worker forever.
            return "eof" if self._abandon_open_capture_streams(job, bound) else "stop"
        if not made_room and used_before is not None and used_before >= bound:
            # A stream is still non-EOF, the spool sits at its bound, and a
            # durable publish+trim freed no room: the bounded cycle cannot
            # make progress. Fail the exact job closed instead of spinning or
            # growing the disk.
            self._fail_capture_closed(job)
            return "stop"
        if not made_room:
            # Quiet open pipe with spool room: wait instead of hammering the
            # database while the grace window runs out. A deliberate bounded
            # drain must never outlive an active sibling job's lease, so keep
            # the bulk heartbeat running across the wait.
            self._refresh_leases_quietly()
            time.sleep(0.05)
        return "wait"

    def _refresh_leases_quietly(self) -> None:
        """Best-effort bulk lease refresh for long deliberate local drains.

        Shutdown and bounded finalization can legitimately spend longer than a
        lease interval draining one job's capture pipes while another active
        job's row must stay lease-owned (otherwise stale recovery would
        reclassify that still-cancelled job as failed). Connectivity failures
        are swallowed here: the regular tick/outage path owns reconnects, and a
        missed refresh never falsely extends a lease.
        """
        with suppress(psycopg.Error):
            self._refresh_leases()

    def _abandon_open_capture_streams(self, job: ActiveJob, bound: int) -> bool:
        """Finalize the capture state of streams whose writers outlived the job.

        Every remaining pipe byte is drained one last time into the bounded
        spool; then any retained in-memory suffix is flushed only within the
        proven current aggregate disk room — never past the configured bound.
        A stream whose retained bytes cannot be durably represented within the
        bound (no room and no trim possible at grace expiry) fails the exact
        job closed instead of overshooting the bound or silently dropping the
        bytes. When every open stream is drained within contract, the read end
        is closed and the stream marked at end-of-file so finalization can
        proceed.

        Args:
            job: The completed active job whose open capture streams are
                abandoned.
            bound: Maximum physical aggregate spool size in bytes (always
                enforced).

        Returns:
            ``True`` when every open stream was drained, flushed within the
            bound, and closed; ``False`` when retained bytes could not be
            represented within the bound (the exact job was failed closed) or
            a genuine spool write failure occurred.
        """
        with suppress(SpoolCaptureError):
            self._drain_completed_streams(job, bound)

        def _aggregate_used() -> int | None:
            try:
                return job.stdout.path.stat().st_size + job.stderr.path.stat().st_size
            except OSError:
                return None

        used = _aggregate_used()
        if used is None:
            # The spool disappeared during abandonment: fail closed rather
            # than assume zero and risk writing past an unknown physical size.
            self._fail_capture_closed(job)
            return False
        for name in OUTPUT_STREAMS:
            stream = getattr(job, name)
            if stream.fd is None:
                continue
            if stream.pending:
                current = _aggregate_used()
                if current is None:
                    self._fail_capture_closed(job)
                    return False
                room = max(0, bound - current)
                if room == 0 or not _flush_pending(stream, min(room, len(stream.pending))):
                    self._fail_capture_closed(job)
                    return False
                if stream.pending:
                    # Even a bounded flush retained a suffix: those bytes
                    # cannot be represented within the bound, so closing the
                    # descriptor here would silently drop them.
                    self._fail_capture_closed(job)
                    return False
            with suppress(OSError):
                os.close(stream.fd)
            stream.fd = None
            stream.eof = True
        return True

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
            if _owned_group_alive(job):
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
            final_status = finish_job(conn, job.id, result, server=self.settings.server)
        except psycopg.Error as exc:
            if self._is_connectivity_error(exc):
                raise
            LOGGER.exception(
                "finalizing job %s failed (SQLSTATE %s)",
                job.id,
                exc.sqlstate or "N/A",
            )
            if _quarantine_job(
                conn, job.id, f"finalization error: {exc}", server=self.settings.server
            ):
                job.quarantined = True
            else:
                job.quarantine_pending = True
            request_stop(job, STOP_REASON_QUARANTINE)
            return False
        if final_status is None:
            # The root row was deleted concurrently after output publication
            # committed and before the terminal update. This is exact-job row
            # loss, never a process-wide error: converge through the same
            # local row-loss path as a publication-time disappearance.
            LOGGER.warning(
                "root row for job %s vanished during finalization; untracking as row loss",
                job.id,
            )
            job.row_lost = True
            cleanup_job(job)
            return True
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
        self._completed_count = getattr(self, "_completed_count", 0) + 1
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
        self._last_claim_batch = len(claimed)
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
            payload = parse_payload(
                claimed.payload, supported=self.settings.supported_protocol_range
            )
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

        if payload.server != self.settings.server:
            LOGGER.warning(
                "rejecting job %s addressed to server %r (this daemon is %r)",
                claimed.id,
                payload.server,
                self.settings.server,
            )
            self._finalize_immediate(
                claimed.id,
                JobResult(
                    status="failed",
                    exit_code=PROTOCOL_ERROR_EXIT_CODE,
                    stdout="",
                    stderr=(
                        f"job server {payload.server!r} does not match this daemon's "
                        f"server {self.settings.server!r}"
                    ),
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
            version=payload.version,
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
    ) -> tuple[str | None, int]:
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
            A pair of the final stderr message when the start failed after
            convergence (the caller must finalize failed) — or ``None`` when
            persistence positively proved the guarded update of exactly one
            owned running row — together with the exact start-time ticks that
            were obtained and persisted (``0`` on any failure). The returned
            ticks are the authoritative in-memory identity of the command and
            must be carried onto the active job unchanged, never re-read from
            ``/proc`` after gate release.

        Raises:
            psycopg.Error: When persisting the identity fails with a
                connectivity error (raised only after exact-group convergence).
        """
        start_ticks = proc_start_ticks(gated.proc.pid)
        if start_ticks is not None and start_ticks > 0:
            try:
                persisted = _persist_process(conn, job_id, gated, start_ticks, self.settings)
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
                return "unable to record process identity; job not started", 0
            if not persisted:
                # Fail closed: zero rows matched, so this worker can no longer
                # positively prove it still owns a same-server running row for
                # this incarnation. The gate must never be released.
                self._abort_and_converge(gated, job_id)
                LOGGER.error(
                    "process-identity persistence matched no owned running row "
                    "for job %s; gated start aborted without executing user code",
                    job_id,
                )
                return "unable to record process identity; job not started", 0
            return None, start_ticks
        # No durable exact identity exists, so this worker itself must own the
        # childless gated child to convergence before anything else.
        self._abort_and_converge(gated, job_id)
        LOGGER.error(
            "unable to obtain exact start-time ticks for job %s (pid %d); "
            "gated start aborted without executing user code",
            job_id,
            gated.proc.pid,
        )
        return "unable to record exact process identity; job not started", 0

    def _activate_gated_job(  # ruff: ignore[too-many-arguments] -- each field is required by the activation contract
        self,
        conn: JobsConnection,
        job_id: UUID,
        job_spec: Job,
        gated: GatedSpawn,
        *,
        claim_mono: float,
        version: int,
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
            version: Protocol version of the root command payload; stored on the
                active job so every emitted output chunk is stamped with it.

        Returns:
            The :class:`ActiveJob` for normal supervision, or ``None`` when the
            start failed and was finalized as such. Every failure path first
            converges the exact gated wrapper (terminal and reaped), so no
            untracked live group ever outlives this call.
        """
        failure, start_ticks = self._pre_release_failure(conn, job_id, gated)
        if failure is None:
            # The exact identity is durably recorded. Release the gate so the
            # wrapper execs the user argv on the exact same PID, after which
            # normal supervision applies. A failed release is a start failure:
            # converge the live child first, then finalize failed.
            if release_gate(gated.gate_fd):
                # Carry the exact ticks that were obtained and persisted before
                # the gate release: exec keeps the identity, and /proc is never
                # re-read after release. The member ledger is seeded with the
                # persisted leader identity itself.
                return ActiveJob(
                    id=job_id,
                    cwd=job_spec.cwd,
                    process=job_spec.process,
                    proc=gated.proc,
                    pid=gated.proc.pid,
                    pgid=gated.pgid,
                    started_mono=time.monotonic(),
                    claimed_at=claim_mono,
                    version=version,
                    start_ticks=start_ticks,
                    owned_members={gated.proc.pid: start_ticks},
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
            finish_job(conn, job_id, result, server=self.settings.server)
        except psycopg.Error as exc:
            if self._is_connectivity_error(exc):
                raise
            LOGGER.exception(
                "unable to finalize job %s (SQLSTATE %s)",
                job_id,
                exc.sqlstate or "N/A",
            )
            if not _quarantine_job(
                conn, job_id, f"immediate finalization error: {exc}", server=self.settings.server
            ):
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

        The snapshot is concurrency-aware: it reports job counts and bounded
        per-job aggregates rather than a single misleading ``current_job_id``.

        Args:
            alive: Whether the worker is alive.
            shutting_down: Whether the worker is shutting down.

        Returns:
            A fresh health snapshot.
        """
        now_mono = time.monotonic()
        now_wall = time.time()
        agg = self._collect_health_aggregates(now_mono)
        return WorkerHealth(
            schema_version=WORKER_HEALTH_SCHEMA_VERSION,
            worker_id=self.settings.worker_id,
            worker_incarnation=self.settings.worker_incarnation,
            pid=os.getpid(),
            start_time_ticks=self._start_time_ticks,
            started_at=self._started_at,
            published_at=now_wall,
            alive=alive,
            db_connected=self.conn is not None,
            db_connected_at=self._db_connected_at,
            db_error_at=self._db_error_at,
            active_jobs=agg.active_jobs,
            stopping_jobs=agg.stopping_jobs,
            completed_jobs=getattr(self, "_completed_count", 0),
            oldest_active_job_age_seconds=agg.oldest_active_job_age_seconds,
            lease_safety_margin_seconds=self.settings.lease_safety_margin_seconds,
            min_lease_safety_remaining_seconds=agg.min_lease_safety_remaining_seconds,
            db_operation_deadline_seconds=self.settings.db_operation_timeout_seconds,
            db_last_activity_at=getattr(self, "_last_db_activity_at", None),
            db_deadline_breached_at=getattr(self, "_db_deadline_breached_at", None),
            db_deadline_breach_count=getattr(self, "_db_deadline_breach_count", 0),
            capture_streams_open=agg.capture_streams_open,
            spool_held_bytes=agg.spool_held_bytes,
            scan_batch_limit=self.settings.claim_batch_limit,
            last_scan_batch_size=getattr(self, "_last_claim_batch", 0),
            last_cancellation_scan_at=getattr(self, "_last_cancellation_scan_at", None),
            last_recovery_at=getattr(self, "_last_recovery_at", None),
            last_gc_at=getattr(self, "_last_gc_at", None),
            cancellation_scan_overdue=agg.cancellation_scan_overdue,
            recovery_overdue=agg.recovery_overdue,
            gc_overdue=agg.gc_overdue,
            gc_batch_limit=self.settings.gc_batch_limit,
            gc_batch_bound_hit=getattr(self, "_gc_batch_bound_hit", False),
            cancellation_batch_limit=CANCEL_DISCOVERY_LIMIT,
            cancellation_batch_bound_hit=getattr(self, "_cancellation_batch_bound_hit", False),
            recovery_batch_limit=LEASE_RECOVERY_LIMIT,
            recovery_batch_bound_hit=getattr(self, "_recovery_batch_bound_hit", False),
            shutting_down=shutting_down,
        )

    def _collect_health_aggregates(self, now_mono: float) -> _HealthAggregates:
        """Aggregate bounded concurrency/capture metrics from active jobs.

        Args:
            now_mono: Current monotonic time for age/lease computation.

        Returns:
            The bounded per-job aggregates for the health snapshot.
        """
        active_jobs = self.active
        stopping = 0
        oldest_age: float | None = None
        min_lease_safety_remaining: float | None = None
        capture_open = 0
        spool_held = 0
        for job in active_jobs.values():
            if job.term_sent or job.kill_sent or job.stop_started is not None:
                stopping += 1
            if job.claimed_at > 0.0:
                age = now_mono - job.claimed_at
                if oldest_age is None or age > oldest_age:
                    oldest_age = age
            if job.last_heartbeat_at > 0.0:
                # Safety remaining, not full-lease remaining: subtract the
                # configured safety margin so a negative value means the
                # lease-safety deadline (expiry minus margin) has passed.
                remaining = (
                    job.last_heartbeat_at
                    + self.settings.lease_duration_seconds
                    - self.settings.lease_safety_margin_seconds
                    - now_mono
                )
                if min_lease_safety_remaining is None or remaining < min_lease_safety_remaining:
                    min_lease_safety_remaining = remaining
            for name in OUTPUT_STREAMS:
                stream = getattr(job, name)
                if stream.fd is not None and not stream.eof:
                    capture_open += 1
                with suppress(OSError):
                    size = stream.path.stat().st_size
                    if size > 0:
                        spool_held += size
        return _HealthAggregates(
            active_jobs=len(active_jobs),
            stopping_jobs=stopping,
            oldest_active_job_age_seconds=oldest_age,
            min_lease_safety_remaining_seconds=min_lease_safety_remaining,
            capture_streams_open=capture_open,
            spool_held_bytes=spool_held,
            cancellation_scan_overdue=now_mono > getattr(self, "_next_cancel_scan_at", 0.0),
            recovery_overdue=now_mono > getattr(self, "_next_recovery_at", 0.0),
            gc_overdue=now_mono > getattr(self, "_next_gc_at", 0.0),
        )

    def _record_db_deadline_breach(self) -> None:
        """Record that a hard client database deadline was breached.

        Called from the actual deadline-failure path so the bounded health
        signal ``db_deadline_breached_at``/``db_deadline_breach_count`` answers
        whether a database operation recently exceeded its deadline, which
        ``db_last_activity_at`` alone cannot prove.
        """
        self._db_deadline_breached_at = time.time()
        self._db_deadline_breach_count = getattr(self, "_db_deadline_breach_count", 0) + 1

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
            conn = DeadlineConnection.connect(
                self.database.conninfo(),
                connect_timeout=max(1, min(5, int(self.settings.db_operation_timeout_seconds))),
                row_factory=tuple_row,
                options=(
                    f"-c statement_timeout={int(self.settings.db_operation_timeout_seconds * 1000)}"
                ),
            )
            # The invariant-verification queries below are established
            # operations too: bound them by the same hard client deadline.
            conn.operation_deadline = time.monotonic() + self.settings.db_operation_timeout_seconds
        except psycopg.Error:
            LOGGER.exception("database connection failed")
            self.conn = None
            self._db_error_at = time.time()
            self._publish_health_force()
            return
        try:
            verify_jobs_table_invariant(conn)
            verify_protocol_schema(conn)
            verify_server_isolation(conn)
            verify_server_identity(conn, self.settings.server)
        except SchemaInvariantError:
            LOGGER.exception(
                "refusing to run against a table that is not a migrated protocol v4 schema"
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

    def _discard_db_connection(self) -> None:
        """Discard the database connection so it is never reused or misreported.

        Closes the connection (ignoring any error) and clears the handle. After
        this, the final health snapshot reports ``db_connected=False`` and no
        later remote finalization is attempted on a known-unusable connection.
        """
        if self.conn is not None:
            with suppress(Exception):
                self.conn.close()
        self.conn = None

    def _shutdown(self) -> None:
        """Gracefully terminate, reap, and finalize every tracked process group.

        Local ownership convergence (reaping and, where provable, killing every
        exact process group) and local cleanup (removing every capture file)
        plus the final local health snapshot are **unconditional**: a remote
        database deadline breach (:class:`DbOperationDeadlineError`) or a
        connectivity loss during finalization must never prevent them. Remote DB
        terminalization is best-effort and fail-closed — its deadline/connectivity
        failures are caught at their narrow boundary, discard the connection, and
        stop further remote attempts, while the affected rows stay safely
        recoverable. The exact drain sentinel is written only after a *clean*
        local drain, so a failed drain never produces a false sentinel. Any other
        (deterministic/schema/programming) fault propagates naturally: Python runs
        the ``finally`` cleanup and final health publication first, then re-raises.
        """
        try:
            self._shutdown_finalize()
        finally:
            # Unconditional local convergence cleanup: remove every capture file
            # regardless of any remote terminalization outcome.
            self._cleanup_all_files()
            # Discard the connection so the final health snapshot never falsely
            # reports db_connected=True after a deadline breach or connectivity
            # loss, and no further remote step can use a known-unusable handle.
            self._discard_db_connection()
            health = self._build_health(alive=False, shutting_down=True)
            try:
                write_worker_health(health)
            except OSError:
                LOGGER.debug("failed to write shutdown health snapshot", exc_info=True)

    def _shutdown_finalize(self) -> None:
        """Stop, drain, and remote-finalize every tracked group.

        See :meth:`_shutdown` for the invariant: this helper performs the
        remote-touching work, and any non-best-effort exception it lets escape
        is handled by the caller's ``finally`` boundary.
        """
        LOGGER.info("shutting down: terminating %d active job(s)", len(self.active))
        install_operation_deadline(
            self.conn, time.monotonic() + self.settings.db_operation_timeout_seconds
        )
        for job in list(self.active.values()):
            if not job.completed and not job.term_sent:
                request_stop(job, STOP_REASON_SHUTDOWN)
        if not self._drain_active_groups():
            # Positive post-SIGKILL proof failed for at least one exact active
            # group. This is NOT a clean drain: never emit the sentinel, and
            # retain those jobs (their running rows keep the exact persisted
            # identity recoverable by emergency recovery).
            surviving = [job.pgid for job in self.active.values() if _owned_group_alive(job)]
            LOGGER.error(
                "shutdown cannot prove groups %s member-free; withholding the "
                "drain sentinel and retaining their jobs for exact-identity "
                "recovery",
                surviving,
            )
            self._finalize_all_for_shutdown(retain_groups=surviving)
        else:
            # Clean local drain: the worker proved every owned group is gone, so
            # the sentinel may be written exactly once. Remote finalization below
            # is best-effort.
            try:
                write_drain_sentinel(self.settings.worker_incarnation)
            except OSError:
                LOGGER.debug("could not write drain sentinel", exc_info=True)
            self._finalize_all_for_shutdown()

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
            self._refresh_leases_quietly()
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
        return all(not _owned_group_alive(job) for job in self.active.values())

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
        if job.completed and not job.term_sent and _owned_group_alive(job):
            LOGGER.info(
                "reaping leftover process group %d of completed job %s",
                job.pgid,
                job.id,
            )
            request_group_reap(job)
        if job.term_sent and not job.kill_sent and job.stop_started is not None:
            grace_elapsed = now - job.stop_started >= self.settings.cancel_grace_seconds
            leader_alive = job.proc.poll() is None
            if grace_elapsed and (leader_alive or _owned_group_alive(job)):
                signal_kill(job)
        return job.completed and not _owned_group_alive(job)

    def _finalize_all_for_shutdown(self, *, retain_groups: list[int] | None = None) -> None:
        """Finalize every tracked job when PostgreSQL is available.

        This is the only step that touches the remote database during shutdown,
        so it is the only step that can fail with a database operation deadline
        breach (:class:`DbOperationDeadlineError`) or a connectivity loss. Both
        are best-effort and fail-closed here: the connection is discarded (so no
        later job attempts a remote finalization on a known-unusable handle) and
        the loop stops, leaving every not-yet-finalized job's row safely
        recoverable. Deterministic per-job errors are logged and the job is
        quarantined, preserving lease/row safety. Jobs whose exact group could
        not be proven member-free (``retain_groups``) are retained in the active
        set and their rows stay recoverable: they are never terminalized or
        untracked here.

        Args:
            retain_groups: Exact group ids that failed post-SIGKILL proof;
                their jobs are retained instead of finalized.
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
                self.finalize_completed_job_bounded(job)
            except DbOperationDeadlineError:
                # The hard client deadline breached while terminalizing this
                # job's remote rows. This is a best-effort remote step: the row
                # stays recoverable and the connection is now unusable, so stop
                # attempting further remote finalizations and let shutdown
                # proceed with its unconditional local cleanup.
                LOGGER.exception(
                    "shutdown finalization hit the database operation deadline for job %s",
                    job.id,
                )
                self._discard_db_connection()
                return
            except psycopg.Error as exc:
                if self._is_connectivity_error(exc):
                    # Connectivity loss during shutdown finalization is
                    # best-effort: the row stays recoverable and the connection
                    # is unusable, so stop further remote attempts and let
                    # shutdown finish locally.
                    LOGGER.exception(
                        "shutdown finalization lost database connectivity for job %s",
                        job.id,
                    )
                    self._discard_db_connection()
                    return
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
    _signal_owned_group(job, signal.SIGTERM)


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
    _signal_owned_group(job, signal.SIGTERM)


def signal_kill(job: ActiveJob) -> None:
    """Send ``SIGKILL`` to an exact process group after the grace period.

    Args:
        job: The active job whose group still has members.
    """
    job.kill_sent = True
    _signal_owned_group(job, signal.SIGKILL)
    if job.cancellation_note is not None:
        job.cancellation_note = f"{job.cancellation_note}; grace period expired, sent SIGKILL"


def cleanup_job(job: ActiveJob) -> None:
    """Best-effort remove capture files and close pipe ends of a finalized job.

    Args:
        job: The finalized active job.
    """
    for stream in (job.stdout, job.stderr):
        if stream.fd is not None:
            with suppress(OSError):
                os.close(stream.fd)
            stream.fd = None
    for stream in (job.stdout, job.stderr):
        try:
            stream.path.unlink(missing_ok=True)
        except OSError as exc:
            LOGGER.warning("failed to remove capture spool %s: %s", stream.path, exc)


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
        server = load_worker_server()
    except (OSError, ValueError):
        LOGGER.exception("unable to load the worker server configuration")
        raise SystemExit(1) from None
    try:
        protocol_range = load_worker_protocol_range()
    except (OSError, ValueError):
        LOGGER.exception("unable to load the worker protocol window configuration")
        raise SystemExit(1) from None
    try:
        database = load_database_config()
    except (OSError, ValueError):
        LOGGER.exception("unable to load database configuration")
        raise SystemExit(1) from None
    try:
        settings = Settings.from_environment(server=server, supported_protocol_range=protocol_range)
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
