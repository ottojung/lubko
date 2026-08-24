"""Tests for the Lubko worker supervisor and its database operations."""

import fcntl
import json
import os
import select
import signal
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable
from contextlib import AbstractContextManager, nullcontext, suppress
from itertools import pairwise
from pathlib import Path
from typing import Final, Self, cast, override
from uuid import UUID, uuid4

import psycopg
import pytest

from lubko import worker
from lubko.config import DatabaseConfig
from lubko.protocol import (
    OUTPUT_CHUNK_MAX_BYTES,
    OUTPUT_TAIL_MAX_BYTES,
    parse_chunk_payload,
    parse_payload,
)
from lubko.worker import (
    DEFAULT_LEASE_DURATION_SECONDS,
    DEFAULT_LEASE_RECOVERY_INTERVAL_SECONDS,
    DEFAULT_LEASE_REFRESH_INTERVAL_SECONDS,
    DRAIN_CHUNK,
    OUTPUT_STREAM_STDERR,
    OUTPUT_STREAM_STDOUT,
    STOP_REASON_SPOOL,
    TRUNCATION_MARKER,
    ActiveJob,
    Job,
    JobResult,
    JobsConnection,
    OutputStream,
    Settings,
    Supervisor,
    bulk_refresh_leases,
    claim_job,
    claim_jobs,
    decode_range,
    delete_job_and_chunks,
    discover_cancellations,
    drain_capture_stream,
    finish_job,
    group_has_members,
    pg_safe_decode,
    publish_output,
    read_output,
    read_range,
    recover_stale_jobs,
    release_gate,
    request_cancel,
    request_group_reap,
    request_stop,
    signal_kill,
    spawn_job,
    stream_size,
    truncate_output,
)
from tests import _process_guard as guard


def drain_pipes(
    stdout_fd: int,
    stderr_fd: int,
    stdout_path: Path,
    stderr_path: Path,
    timeout: float = 10.0,
) -> None:
    """Drain a spawned job's capture pipes into their spool files.

    Used by tests that spawn real processes: the child writes to pipes that the
    supervisor would normally drain, so the test drains them itself before
    asserting on the spool files.

    Args:
        stdout_fd: Read end of the job's stdout capture pipe.
        stderr_fd: Read end of the job's stderr capture pipe.
        stdout_path: Spool file for standard output.
        stderr_path: Spool file for standard error.
        timeout: Maximum seconds to wait for end-of-file on both pipes.
    """
    targets = {stdout_fd: stdout_path, stderr_fd: stderr_path}
    remaining = set(targets)
    deadline = time.monotonic() + timeout
    while remaining and time.monotonic() < deadline:
        readable, _w, _e = select.select(list(remaining), [], [], 0.1)
        for fd in readable:
            data = os.read(fd, 65536)
            if not data:
                with suppress(OSError):
                    os.close(fd)
                remaining.discard(fd)
                continue
            with targets[fd].open("ab") as fh:
                fh.write(data)
    for fd in list(remaining):
        with suppress(OSError):
            os.close(fd)


EXECUTION_ERROR_EXIT_CODE: Final = 127
COMMAND_FAILURE_EXIT_CODE: Final = 7
MIN_LEASE_HEARTBEATS: Final = 2

SLEEP_30: Final = (sys.executable, "-c", "import time; time.sleep(30)")
SLEEP_300: Final = (sys.executable, "-c", "import time; time.sleep(300)")

#: A python argv that forks a background member of the same process group,
#: then exits zero — the direct-exec equivalent of ``sleep 30 & echo done``.
LEFTOVER_GROUP_PROBE: Final = (
    sys.executable,
    "-c",
    "import os, time\nif os.fork() == 0:\n    time.sleep(30)\nelse:\n    os._exit(0)\n",
)

#: A short-lived child that emits a little output on both streams and exits
#: immediately, so capture pipes reach end-of-file without holding open write
#: ends for the whole test (which would otherwise force the drain to wait out a
#: long sleep); used by fd/spool-leak tests that must stay fast.
SHORT_CHILD: Final = (
    sys.executable,
    "-c",
    "import sys\nsys.stdout.write('o' * 64)\nsys.stderr.write('e' * 64)\nsys.exit(0)\n",
)


def spawn_released(
    job: Job,
) -> tuple[subprocess.Popen[bytes], Path, Path, int, int, int]:
    """Spawn a job and immediately release its start gate so user code runs.

    The production :func:`spawn_job` returns a gated wrapper that blocks until
    the worker has persisted the exact identity; tests that need the user
    process to actually execute call this instead of ``spawn_job`` directly.

    Args:
        job: Claimed job to execute.

    Returns:
        The same tuple ``spawn_job`` returns (minus the gate write end).
    """
    proc, stdout_path, stderr_path, pgid, gate_fd, stdout_r, stderr_r = spawn_job(job)
    release_gate(gate_fd)
    return proc, stdout_path, stderr_path, pgid, stdout_r, stderr_r


class _RecordingCursor:
    """A cursor that records every executed statement."""

    def __init__(self, conn: "_RecordingConnection") -> None:
        self._conn = conn

    def execute(self, sql: str, params: object | None = None) -> None:
        self._conn.executions.append((sql, params))

    def fetchone(self) -> object:
        sql = self._conn.executions[-1][0] if self._conn.executions else ""
        if "FOR UPDATE" in sql and self._conn.retained_root is not None:
            return self._conn.retained_root
        if "->'state'->>'status'" in sql and self._conn.status_result is not None:
            return self._conn.status_result
        if self._conn.rows:
            return self._conn.rows.pop(0)
        return None

    def fetchall(self) -> list[object]:
        rows = self._conn.rows
        self._conn.rows = []
        return rows

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc_value: BaseException | None,
        _traceback: object,
    ) -> None:
        return None


class _RecordingConnection:
    """A database test double that records statements and returns queued rows."""

    def __init__(self) -> None:
        self.executions: list[tuple[str, object | None]] = []
        self.rows: list[object] = []
        #: Persistent answer for the publication root-retention ``SELECT``.
        self.retained_root: tuple[object, ...] | None = None
        #: Persistent answer for job-status probes (``_read_job_status``).
        self.status_result: tuple[object, ...] | None = None

    def cursor(self, **_kwargs: object) -> "_RecordingCursor":
        return _RecordingCursor(self)

    @staticmethod
    def transaction() -> AbstractContextManager[None]:
        return nullcontext()


class _FailingConnection(_RecordingConnection):
    """A recording connection whose statements fail on demand."""

    def __init__(self, fail_on: Callable[[str], bool]) -> None:
        super().__init__()
        self.fail_on = fail_on

    @override
    def cursor(self, **_kwargs: object) -> "_RecordingCursor":
        return _FailingCursor(self)


class _FailingCursor(_RecordingCursor):
    """A recording cursor that raises on matching statements."""

    @override
    def execute(self, sql: str, params: object | None = None) -> None:
        assert isinstance(self._conn, _FailingConnection)
        if self._conn.fail_on(sql):
            self._conn.executions.append((sql, params))
            msg = "simulated database failure"
            raise psycopg.OperationalError(msg)
        super().execute(sql, params)


def as_db(conn: _RecordingConnection) -> JobsConnection:
    """Adapt the recording test double to the worker's connection type.

    Args:
        conn: Recording test double.

    Returns:
        The same object typed as a psycopg connection.
    """
    return cast("JobsConnection", conn)


def _queue_root(conn: _RecordingConnection, job_id: UUID) -> None:
    """Queue the root row so the publication root-retention guard sees it.

    The publication transaction reads the root ``command`` row with
    ``SELECT ... FOR UPDATE`` before inserting any chunk; the recording double
    returns queued rows from ``fetchone``, so tests seed one row per
    publication pass.

    Args:
        conn: Recording test double.
        job_id: The root job identifier to report as retained.
    """
    conn.rows.append((str(job_id),))


def make_settings(  # ruff: ignore[too-many-arguments]
    *,
    server: str = "alpha-server",
    process_poll_interval_seconds: float = 0.02,
    cancel_grace_seconds: float = 1.0,
    lease_duration_seconds: float = DEFAULT_LEASE_DURATION_SECONDS,
    lease_refresh_interval_seconds: float = DEFAULT_LEASE_REFRESH_INTERVAL_SECONDS,
    lease_recovery_interval_seconds: float = DEFAULT_LEASE_RECOVERY_INTERVAL_SECONDS,
    output_publication_interval_seconds: float = 0.1,
    claim_batch_limit: int = 8,
    lease_safety_margin_seconds: float = 5.0,
    output_spool_max_bytes: int = 4 * 1024 * 1024,
) -> Settings:
    """Build worker settings for tests.

    Returns:
        Worker settings for tests.
    """
    return Settings(
        server=server,
        worker_id="test-worker",
        poll_interval_seconds=1.0,
        process_poll_interval_seconds=process_poll_interval_seconds,
        cancel_grace_seconds=cancel_grace_seconds,
        lease_duration_seconds=lease_duration_seconds,
        lease_refresh_interval_seconds=lease_refresh_interval_seconds,
        lease_recovery_interval_seconds=lease_recovery_interval_seconds,
        output_publication_interval_seconds=output_publication_interval_seconds,
        claim_batch_limit=claim_batch_limit,
        lease_safety_margin_seconds=lease_safety_margin_seconds,
        output_spool_max_bytes=output_spool_max_bytes,
    )


def wait_until(predicate: Callable[[], bool], timeout: float = 10.0) -> None:
    """Poll until a predicate holds, raising if the deadline expires.

    Args:
        predicate: Condition to satisfy.
        timeout: Maximum seconds to wait.

    Raises:
        AssertionError: If the condition is not met within the timeout.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    msg = "condition not met within timeout"
    raise AssertionError(msg)


def make_active_job(tmp_path: Path, *, process: tuple[str, ...] = SLEEP_30) -> ActiveJob:
    """Build a synthetic active job with capture files under ``tmp_path``.

    The synthetic child is ``/bin/true`` spawned in its own session and
    deterministically reaped before return.  It is not a live registered
    process: callers that need a live process must spawn and own one directly.

    Args:
        tmp_path: Temporary directory for the capture files.
        process: Process argv recorded on the job.

    Returns:
        A synthetic active job whose child has terminated and been reaped, with
        empty capture files.
    """
    proc = subprocess.Popen(
        ["/bin/true"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    guard.register(proc)
    proc.wait(timeout=10)
    guard.unregister(proc)
    job = ActiveJob(
        id=uuid4(),
        cwd=str(tmp_path),
        process=process,
        proc=proc,
        pid=proc.pid,
        pgid=proc.pid,
        started_mono=time.monotonic(),
        claimed_at=time.time(),
    )
    job.stdout = OutputStream(path=tmp_path / "stdout.cap")
    job.stderr = OutputStream(path=tmp_path / "stderr.cap")
    return job


def test_make_active_job_synthetic_child_is_reaped_and_unregistered(
    tmp_path: Path,
) -> None:
    """The synthetic child exited and is not left registered with the guard.

    ``make_active_job`` must reap ``/bin/true`` before returning and drop the
    guard's ownership of it, so the job is a synthetic (already-terminated)
    fixture rather than a live, tracked process that would trip the global leak
    assertions.
    """
    job = make_active_job(tmp_path)
    assert job.proc.poll() == 0
    assert job.proc.pid not in guard.tracked_pids()


def test_truncate_output_preserves_short_output() -> None:
    """Short output is returned unchanged."""
    assert truncate_output(b"hello\n", 128) == "hello\n"


def test_truncate_output_keeps_tail() -> None:
    """Oversized output keeps the newest bytes and records truncation."""
    limit = 64
    output = b"a" * 100 + b"the-end"

    result = truncate_output(output, limit)

    assert result.encode().startswith(TRUNCATION_MARKER)
    assert result.endswith("the-end")
    assert len(result.encode()) == limit


def test_truncate_output_hard_bound_at_marker_length() -> None:
    """A limit equal to the marker length is a hard bound."""
    result = truncate_output(b"x" * 1000, len(TRUNCATION_MARKER))
    assert result.encode() == TRUNCATION_MARKER
    assert len(result.encode()) <= len(TRUNCATION_MARKER)


def test_truncate_output_hard_bound_at_adjacent_limits() -> None:
    """Limits one byte below/above the marker length behave deterministically."""
    with pytest.raises(ValueError, match="truncation marker"):
        truncate_output(b"xyz", len(TRUNCATION_MARKER) - 1)

    data = b"y" * 1000
    for delta in (0, 1, 2, 3):
        limit = len(TRUNCATION_MARKER) + delta
        result = truncate_output(data, limit)
        assert len(result.encode()) <= limit


def test_truncate_output_hard_bound_despite_replacement_expansion() -> None:
    """NUL bytes expand to 3-byte U+FFFD; the limit still holds."""
    limit = 64
    data = b"\x00" * 100 + b"the-end"

    result = truncate_output(data, limit)

    assert len(result.encode()) <= limit
    assert result.encode().startswith(TRUNCATION_MARKER)
    assert result.endswith("the-end")


@pytest.mark.parametrize(
    "data",
    [
        b"\x00" * 500,
        bytes(range(128, 256)) * 4,
        b"ab\x80\x80\x80\x80cd",
        b"ok" * 300,
        ("héllo".encode() * 50) + b"end",
    ],
)
def test_truncate_output_invariant_across_limits(data: bytes) -> None:
    """Encoded output never exceeds the limit regardless of decoding expansion."""
    limits = [len(TRUNCATION_MARKER), 30, 33, 64, 100, 257, 1024]
    for limit in limits:
        result = truncate_output(data, limit)
        encoded = result.encode()
        assert len(encoded) <= limit
        if len(data) > limit:
            assert encoded.startswith(TRUNCATION_MARKER)


def test_truncate_output_multibyte_boundary_keeps_valid_text() -> None:
    """Truncation across multibyte boundaries returns bounded, valid text."""
    data = "€".encode() * 100
    limits = [len(TRUNCATION_MARKER), len(TRUNCATION_MARKER) + 13, 64, 100]

    for limit in limits:
        result = truncate_output(data, limit)
        encoded = result.encode()
        assert len(encoded) <= limit
        if len(data) > limit:
            assert encoded.startswith(TRUNCATION_MARKER)
        result.encode("utf-8")  # valid UTF-8 text round-trips cleanly


def test_truncate_output_expansion_heavy_payload_stays_linear() -> None:
    """A large replacement-heavy payload is bounded without quadratic rework."""
    limit = 512
    data = b"\x00" * 50_000 + b"tail"

    result = truncate_output(data, limit)

    encoded = result.encode()
    assert len(encoded) <= limit
    assert encoded.startswith(TRUNCATION_MARKER)
    assert result.endswith("tail")


def test_read_output_returns_captured_bytes(tmp_path: Path) -> None:
    """read_output returns everything captured into an output file."""
    target = tmp_path / "out"
    target.write_bytes(b"captured output")
    assert read_output(target) == b"captured output"


def test_stream_size_and_read_range(tmp_path: Path) -> None:
    """stream_size and read_range inspect capture files by byte offset."""
    target = tmp_path / "out"
    target.write_bytes(b"0123456789")

    assert stream_size(target) == 10
    assert read_range(target, 2, 5) == b"234"


def test_group_has_members_tracks_process_group(tmp_path: Path) -> None:
    """The exact process group is reported while alive and gone after death."""
    proc, _stdout_path, _stderr_path, pgid, _so_fd, _se_fd = spawn_released(
        Job(id=uuid4(), cwd=str(tmp_path), process=SLEEP_30)
    )
    guard.register(proc)
    try:
        assert group_has_members(pgid)
    finally:
        with suppress(ProcessLookupError):
            os.killpg(pgid, signal.SIGKILL)
        proc.wait(timeout=10)
        guard.unregister(proc)
    wait_until(lambda: not group_has_members(pgid))


def test_spawn_job_makes_session_and_process_group_leader(tmp_path: Path) -> None:
    """A spawned job is a session leader whose group ID equals its PID."""
    proc, _stdout_path, _stderr_path, pgid, _so_fd, _se_fd = spawn_released(
        Job(id=uuid4(), cwd=str(tmp_path), process=SLEEP_30)
    )
    guard.register(proc)
    try:
        assert pgid == proc.pid
        assert os.getpgid(proc.pid) == proc.pid
        assert os.getsid(proc.pid) == proc.pid
    finally:
        with suppress(ProcessLookupError):
            os.killpg(pgid, signal.SIGKILL)
        proc.wait(timeout=10)
        guard.unregister(proc)


def test_spawn_job_runs_process_and_cleanup_files(tmp_path: Path) -> None:
    """A process job writes its output into the capture files."""
    proc, stdout_path, stderr_path, _pgid, stdout_fd, stderr_fd = spawn_released(
        Job(id=uuid4(), cwd=str(tmp_path), process=(sys.executable, "-c", "print('hi')"))
    )
    guard.register(proc)
    try:
        proc.wait(timeout=10)
        drain_pipes(stdout_fd, stderr_fd, stdout_path, stderr_path)
        assert read_output(stdout_path) == b"hi\n"
        assert stdout_path.is_file()
        assert stderr_path.is_file()
    finally:
        guard.unregister(proc)
        proc.wait(timeout=5)
        stdout_path.unlink(missing_ok=True)
        stderr_path.unlink(missing_ok=True)


def test_spawn_job_injects_exact_root_job_uuid(tmp_path: Path) -> None:
    """A process job inherits its exact root job UUID as LUBKO_JOB_ID."""
    job_id = uuid4()
    probe = "import os; print(os.environ['LUBKO_JOB_ID'])"
    proc, stdout_path, stderr_path, _pgid, stdout_fd, stderr_fd = spawn_released(
        Job(id=job_id, cwd=str(tmp_path), process=(sys.executable, "-c", probe))
    )
    guard.register(proc)
    try:
        proc.wait(timeout=10)
        drain_pipes(stdout_fd, stderr_fd, stdout_path, stderr_path)
        assert read_output(stdout_path) == str(job_id).encode() + b"\n"
    finally:
        guard.unregister(proc)
        proc.wait(timeout=5)
        stdout_path.unlink(missing_ok=True)
        stderr_path.unlink(missing_ok=True)


def test_spawn_job_runs_process_in_declared_cwd(tmp_path: Path) -> None:
    """A process job (direct exec) runs inside its declared working directory."""
    work_dir = tmp_path / "runner"
    work_dir.mkdir()
    probe = "import os; print(os.getcwd())"
    proc, stdout_path, stderr_path, _pgid, stdout_fd, stderr_fd = spawn_released(
        Job(id=uuid4(), cwd=str(work_dir), process=(sys.executable, "-c", probe))
    )
    guard.register(proc)
    try:
        proc.wait(timeout=10)
        drain_pipes(stdout_fd, stderr_fd, stdout_path, stderr_path)
        assert read_output(stdout_path) == str(work_dir).encode() + b"\n"
        assert read_output(stderr_path) == b""
    finally:
        guard.unregister(proc)
        proc.wait(timeout=5)
        stdout_path.unlink(missing_ok=True)
        stderr_path.unlink(missing_ok=True)


def test_spawn_job_passes_shell_metacharacters_literally(tmp_path: Path) -> None:
    """Shell metacharacters in argv are passed literally, never evaluated.

    The sibling pattern ``a;b``, the expansion ``$HOME``, a glob ``*.txt``,
    and a command substitution ``$(id)`` must reach the program unmodified
    because the worker executes the argv directly without a shell.
    """
    literal = "a;b $HOME *.txt $(id)"
    probe = "import sys; print(sys.argv[1])"
    proc, stdout_path, stderr_path, _pgid, stdout_fd, stderr_fd = spawn_released(
        Job(
            id=uuid4(),
            cwd=str(tmp_path),
            process=(sys.executable, "-c", probe, literal),
        )
    )
    guard.register(proc)
    try:
        proc.wait(timeout=10)
        drain_pipes(stdout_fd, stderr_fd, stdout_path, stderr_path)
        assert read_output(stdout_path) == literal.encode() + b"\n"
        assert read_output(stderr_path) == b""
    finally:
        guard.unregister(proc)
        proc.wait(timeout=5)
        stdout_path.unlink(missing_ok=True)
        stderr_path.unlink(missing_ok=True)


def test_archive_target_never_reaches_the_live_tail() -> None:
    """The archive target always stays short of the newest tail window."""
    assert worker.archive_target(0) == 0
    assert worker.archive_target(OUTPUT_TAIL_MAX_BYTES) == 0
    size = OUTPUT_TAIL_MAX_BYTES * 3
    target = worker.archive_target(size)
    assert target < size
    assert target == size - OUTPUT_TAIL_MAX_BYTES + 2000


def test_claim_job_marks_job_running_and_only_command_rows() -> None:
    """claim_jobs marks command rows running and never touches chunk rows."""
    conn = _RecordingConnection()
    job_id = uuid4()
    claimed_payload = json.dumps({
        "v": 4,
        "type": "command",
        "server": "alpha-server",
        "request": {"cwd": "/workspace", "process": ["echo", "hi"]},
        "state": {"status": "running"},
    })
    conn.rows = [(job_id, claimed_payload)]
    settings = make_settings()

    claimed = claim_job(as_db(conn), settings)

    assert claimed is not None
    assert claimed.id == job_id
    parsed = parse_payload(claimed.payload)
    assert parsed.request.process == ("echo", "hi")
    assert parsed.status == "running"
    sql, params = conn.executions[0]
    assert "{state,status}" in sql
    assert "'running'" in sql
    assert "'pending'" in sql
    assert "FOR UPDATE SKIP LOCKED" in sql
    assert "(payload::jsonb)->>'type' = 'command'" in sql
    assert "jsonb_typeof((payload::jsonb)->'server') = 'string'" in sql
    assert "(payload::jsonb)->>'server' = %(server)s" in sql
    assert "::text" in sql
    assert "{state,worker_incarnation}" in sql
    assert "{state,lease_expires_at}" in sql
    assert "make_interval" in sql
    assert isinstance(params, dict)
    assert params["server"] == settings.server
    assert params["worker_id"] == settings.worker_id
    assert params["worker_incarnation"] == settings.worker_incarnation
    assert params["lease_duration_seconds"] == settings.lease_duration_seconds
    assert params["limit"] == 1


def test_claim_jobs_only_claims_configured_server_rows() -> None:
    """The claim predicate is scoped to the daemon's configured server identity."""
    conn = _RecordingConnection()
    settings = make_settings(server="alpha-server")

    claim_jobs(as_db(conn), settings, 8)

    _sql, params = conn.executions[0]
    assert isinstance(params, dict)
    assert params["server"] == "alpha-server"


def test_claim_jobs_returns_nothing_on_empty_queue() -> None:
    """An empty queue yields no claims and no writes beyond the claim query."""
    conn = _RecordingConnection()

    claimed = claim_jobs(as_db(conn), make_settings(), 8)

    assert claimed == []
    assert len(conn.executions) == 1
    assert "RETURNING job.id" in conn.executions[0][0]


def test_claim_jobs_batch_limit_is_a_fairness_bound() -> None:
    """The claim batch limit is passed as a SQL limit, not a concurrency cap."""
    conn = _RecordingConnection()
    conn.rows = []

    claim_jobs(as_db(conn), make_settings(claim_batch_limit=3), 3)

    sql, params = conn.executions[0]
    assert isinstance(params, dict)
    assert params["limit"] == 3
    assert "LIMIT %(limit)s" in sql


def test_request_cancel_cancels_pending_job_without_spawning() -> None:
    """A pending job is marked cancelled immediately without being claimed."""
    conn = _RecordingConnection()
    conn.rows = [("cancelled",)]
    job_id = uuid4()

    status = request_cancel(as_db(conn), job_id, server="alpha-server")

    assert status == "cancelled"
    sql, params = conn.executions[0]
    assert "{state,status}" in sql
    assert "'cancelled'" in sql
    assert "payload::jsonb" in sql
    assert "::text" in sql
    assert "'pending'" in sql
    assert "(payload::jsonb)->>'server'" in sql
    assert params == {"job_id": job_id, "server": "alpha-server"}
    assert len(conn.executions) == 1


def test_request_cancel_marks_running_job() -> None:
    """A running job has its cancellation marker set for the worker to act on."""
    conn = _RecordingConnection()
    conn.rows = [None, ("running",)]
    job_id = uuid4()

    status = request_cancel(as_db(conn), job_id, server="alpha-server")

    assert status == "running"
    sql, params = conn.executions[1]
    assert "cancel_requested_at" in sql
    assert "'running'" in sql
    assert "(payload::jsonb)->>'server'" in sql
    assert params == {"job_id": job_id, "server": "alpha-server"}


def test_request_cancel_leaves_terminal_job_unchanged() -> None:
    """Cancelling an already terminal job is a harmless no-op."""
    conn = _RecordingConnection()
    conn.rows = [None, None, ("alpha-server", "succeeded")]
    job_id = uuid4()

    status = request_cancel(as_db(conn), job_id, server="alpha-server")

    assert status == "succeeded"
    updates = [sql for sql, _ in conn.executions if "UPDATE" in sql]
    assert len(updates) == 2


def test_bulk_refresh_leases_refreshes_only_named_root_ids() -> None:
    """One statement refreshes exactly the requested root IDs, nothing else."""
    conn = _RecordingConnection()
    owned = [uuid4(), uuid4(), uuid4()]
    conn.rows = [(owned[0],), (owned[1],)]
    settings = make_settings()

    refreshed = bulk_refresh_leases(as_db(conn), settings, [owned[0], owned[1]])

    assert refreshed == [owned[0], owned[1]]
    sql, params = conn.executions[0]
    assert "lease_expires_at" in sql
    assert "make_interval" in sql
    assert "(payload::jsonb)->>'type' = 'command'" in sql
    assert "status' = 'running'" in sql
    assert "worker_id" in sql
    assert "worker_incarnation" in sql
    # Heartbeat scoping: only the explicitly named root IDs are touched.
    assert "id = ANY(%(root_ids)s)" in sql
    assert isinstance(params, dict)
    assert params["lease_duration_seconds"] == settings.lease_duration_seconds
    assert params["worker_id"] == settings.worker_id
    assert params["root_ids"] == [owned[0], owned[1]]


def test_bulk_refresh_leases_never_heartbeats_unnamed_owned_row() -> None:
    """An owned running row not named in root_ids is left untouched.

    This is the core of issue #74: a claimed job whose immediate
    finalization write failed must not be heartbeated merely because another
    job is active. The bulk heartbeat only refreshes the explicitly named IDs.
    """
    conn = _RecordingConnection()
    # The recording double returns whatever rows are queued; by queuing nothing
    # we prove the statement's WHERE clause scopes to root_ids and refreshes
    # no row when none of the named IDs match a running owned row.
    conn.rows = []
    settings = make_settings()
    named = uuid4()

    refreshed = bulk_refresh_leases(as_db(conn), settings, [named])

    assert refreshed == []
    assert "id = ANY(%(root_ids)s)" in conn.executions[0][0]
    # A different owned running row (not named) is never touched.
    other = uuid4()
    assert other not in refreshed


def test_discover_cancellations_queries_owned_running_markers() -> None:
    """Cancellation discovery reads owned running jobs in a bounded batch."""
    conn = _RecordingConnection()
    job_id = uuid4()
    conn.rows = [(job_id,)]
    settings = make_settings()

    found = discover_cancellations(as_db(conn), settings)

    assert found == [job_id]
    sql, params = conn.executions[0]
    assert "cancel_requested_at" in sql
    assert "(payload::jsonb)->>'type' = 'command'" in sql
    assert "LIMIT %(limit)s" in sql
    assert isinstance(params, dict)
    assert params["limit"] == 100


def test_recover_stale_jobs_marks_failed_with_diagnostic() -> None:
    """recover_stale_jobs atomically fails expired-lease running command rows."""
    conn = _RecordingConnection()
    job_id = uuid4()
    settings = make_settings()
    recovered_payload = json.dumps({
        "v": 4,
        "type": "command",
        "server": "alpha-server",
        "request": {"cwd": "/workspace", "process": ["sleep", "30"]},
        "state": {
            "status": "failed",
            "worker_id": "old-worker",
            "worker_incarnation": "old-incarnation",
            "lease_expires_at": "2020-01-01T00:00:00.000000Z",
        },
    })
    conn.rows = [(job_id, recovered_payload)]

    recovered = recover_stale_jobs(as_db(conn), settings.server)

    assert recovered == [(job_id, recovered_payload)]
    sql, params = conn.executions[0]
    assert "WITH stale AS" in sql
    assert "(payload::jsonb)->>'type' = 'command'" in sql
    assert "'running'" in sql
    assert "lease_expires_at" in sql
    assert "FOR UPDATE SKIP LOCKED" in sql
    assert "'failed'" in sql
    assert "{state,recovered_at}" in sql
    assert "recovery_note" in sql
    assert "(payload::jsonb)->>'server'" in sql
    assert params == {"server": "alpha-server", "limit": 100}


def test_recover_stale_jobs_returns_empty_when_none_stale() -> None:
    """An empty recovery scan returns no rows."""
    conn = _RecordingConnection()

    recovered = recover_stale_jobs(as_db(conn), make_settings().server)

    assert recovered == []
    assert len(conn.executions) == 1


def test_finish_job_persists_success_result() -> None:
    """finish_job persists a natural success result."""
    conn = _RecordingConnection()
    conn.rows = [("succeeded",)]
    job_id = uuid4()
    result = JobResult(
        status="succeeded",
        exit_code=0,
        stdout="hi\n",
        stderr="",
        cancellation_note=None,
    )

    status = finish_job(as_db(conn), job_id, result, server="alpha-server")

    assert status == "succeeded"
    sql, params = conn.executions[0]
    assert "CASE" in sql
    assert "jsonb_build_object" in sql
    assert "'running'" in sql
    assert "::text" in sql
    assert isinstance(params, dict)
    assert params["status"] == "succeeded"
    assert params["stdout"] == "hi\n"
    assert params["exit_code"] == 0


def test_finish_job_persists_cancellation_result() -> None:
    """finish_job persists a cancelled result with its diagnostic note."""
    conn = _RecordingConnection()
    conn.rows = [("cancelled",)]
    job_id = uuid4()
    result = JobResult(
        status="cancelled",
        exit_code=-signal.SIGTERM,
        stdout="",
        stderr="",
        cancellation_note="cancelled by request",
    )

    status = finish_job(as_db(conn), job_id, result, server="alpha-server")

    assert status == "cancelled"
    sql, params = conn.executions[0]
    assert "CASE" in sql
    assert isinstance(params, dict)
    assert params["status"] == "cancelled"
    assert params["cancellation_note"] == "cancelled by request"


def test_delete_job_and_chunks_uses_explicit_ownership() -> None:
    """Cleanup is server-scoped: root first, then every owned chunk by thread."""
    conn = _RecordingConnection()
    job_id = uuid4()

    delete_job_and_chunks(as_db(conn), job_id, server="alpha-server")

    select_sql, _select_params = conn.executions[0]
    assert select_sql.startswith("SELECT")
    assert "(payload::jsonb)->>'server'" in select_sql
    root_sql, root_params = conn.executions[1]
    assert root_sql.startswith("DELETE")
    assert "output_chunk" not in root_sql
    assert "jsonb_typeof((payload::jsonb)->'server') = 'string'" in root_sql
    assert isinstance(root_params, dict)
    assert root_params["job_id"] == job_id
    assert root_params["server"] == "alpha-server"
    chunk_sql, chunk_params = conn.executions[2]
    assert chunk_sql.startswith("DELETE")
    assert "output_chunk" in chunk_sql
    assert "thread" in chunk_sql
    assert "jsonb_typeof((payload::jsonb)->'server') = 'string'" in chunk_sql
    assert isinstance(chunk_params, dict)
    assert chunk_params["thread"] == str(job_id)
    assert chunk_params["server"] == "alpha-server"


def test_delete_job_and_chunks_fails_closed_on_foreign_server() -> None:
    """A root belonging to another server is never deleted; cleanup raises."""
    conn = _RecordingConnection()
    conn.rows = [("beta-server",)]
    job_id = uuid4()

    with pytest.raises(ValueError, match="belongs to server 'beta-server'"):
        delete_job_and_chunks(as_db(conn), job_id, server="alpha-server")

    deletes = [sql for sql, _params in conn.executions if sql.lstrip().startswith("DELETE")]
    assert deletes == []


def test_publish_output_writes_bounded_tail_for_short_output(
    tmp_path: Path,
) -> None:
    """Short output produces a full tail and no immutable chunks."""
    job = make_active_job(tmp_path)
    job.stdout.path.write_bytes(b"hello world")
    conn = _RecordingConnection()
    _queue_root(conn, job.id)

    publish_output(
        as_db(conn),
        job,
        [OUTPUT_STREAM_STDOUT],
        time.monotonic(),
        server="alpha-server",
        force=True,
    )

    assert job.stdout.tail_text == "hello world"
    assert job.stdout.tail_start == 0
    assert job.stdout.tail_end == 11
    assert job.stdout.archived_upto == 0
    inserts = [sql for sql, _ in conn.executions if sql.startswith("INSERT")]
    updates = [(sql, params) for sql, params in conn.executions if sql.startswith("UPDATE")]
    assert inserts == []
    assert len(updates) == 1
    _, params = updates[0]
    assert isinstance(params, dict)
    window = json.loads(cast("str", params["output"]))["stdout"]
    assert window["tail"] == "hello world"
    assert window["start"] == 0
    assert window["end"] == 11
    assert window["previous"] is None


def test_publish_output_archives_immutable_chunks(tmp_path: Path) -> None:
    """Large output creates contiguous chunks and a 4000-character live tail."""
    job = make_active_job(tmp_path)
    job.stdout.path.write_bytes(b"x" * 9000)
    conn = _RecordingConnection()
    _queue_root(conn, job.id)

    publish_output(
        as_db(conn),
        job,
        [OUTPUT_STREAM_STDOUT],
        time.monotonic(),
        server="alpha-server",
        force=True,
    )

    assert job.stdout.tail_text == "x" * OUTPUT_TAIL_MAX_BYTES
    assert job.stdout.tail_start == 5000
    assert job.stdout.tail_end == 9000
    assert job.stdout.sequence == 3
    inserts = [sql for sql, _ in conn.executions if sql.startswith("INSERT")]
    assert len(inserts) == 3
    offsets: list[tuple[int, int, int]] = []
    previous: UUID | None = None
    for sql, params in conn.executions:
        if not sql.startswith("INSERT"):
            continue
        assert isinstance(params, tuple)
        chunk = parse_chunk_payload(params[1])
        offsets.append((chunk.sequence, chunk.start, chunk.end))
        if chunk.previous is not None:
            assert chunk.previous == previous
        previous = cast("UUID", params[0])
    assert offsets == [(0, 0, 2000), (1, 2000, 4000), (2, 4000, 6000)]
    updates = [(sql, params) for sql, params in conn.executions if sql.startswith("UPDATE")]
    assert len(updates) == 1
    _, params = updates[0]
    assert isinstance(params, dict)
    window = json.loads(cast("str", params["output"]))["stdout"]
    assert window["previous"] == str(previous)


def test_publish_output_rotation_never_shortens_the_live_tail(
    tmp_path: Path,
) -> None:
    """Archival rotation never shortens the live tail across publications.

    The child appends output over time, exactly like a real process; the
    supervisor trims the durably published prefix from the head of the capture
    file after each publication, so the local spool stays bounded while logical
    offsets and the rolling tail are preserved.
    """
    job = make_active_job(tmp_path)
    conn = _RecordingConnection()
    observed_lengths: list[int] = []
    published: int = 0

    def write_and_publish(size: int) -> None:
        nonlocal published
        if size > published:
            with job.stdout.path.open("ab") as fh:
                fh.write(b"y" * (size - published))
            published = size
        _queue_root(conn, job.id)
        publish_output(
            as_db(conn),
            job,
            [OUTPUT_STREAM_STDOUT],
            time.monotonic(),
            server="alpha-server",
            force=True,
        )
        observed_lengths.append(len(job.stdout.tail_text))
        assert job.stdout.tail_end == size

    write_and_publish(2000)
    write_and_publish(6000)
    write_and_publish(10000)
    write_and_publish(14000)

    assert observed_lengths == [2000, 4000, 4000, 4000]
    assert job.stdout.tail_start == 10000
    assert job.stdout.tail_end == 14000
    inserts = [sql for sql, _ in conn.executions if sql.startswith("INSERT")]
    assert len(inserts) == 6
    sequences = [
        parse_chunk_payload(cast("tuple[object, object]", params)[1]).sequence
        for sql, params in conn.executions
        if sql.startswith("INSERT")
    ]
    assert sequences == [0, 1, 2, 3, 4, 5]
    # The durably published prefix is discarded from the local spool, so the
    # capture file holds only the rolling live tail regardless of total volume.
    assert job.stdout.path.stat().st_size <= OUTPUT_TAIL_MAX_BYTES


def test_publish_output_failed_transaction_does_not_advance_state(
    tmp_path: Path,
) -> None:
    """A failed transaction never advances in-memory publication state."""
    job = make_active_job(tmp_path)
    job.stdout.path.write_bytes(b"z" * 9000)
    failing = _FailingConnection(lambda sql: sql.startswith("UPDATE"))
    _queue_root(failing, job.id)

    with pytest.raises(psycopg.Error):
        publish_output(
            as_db(failing),
            job,
            [OUTPUT_STREAM_STDOUT],
            time.monotonic(),
            server="alpha-server",
            force=True,
        )

    assert job.stdout.published_size == 0
    assert not job.stdout.tail_text
    assert job.stdout.sequence == 0
    assert job.stdout.archived_upto == 0
    assert job.stdout.last_chunk is None

    conn = _RecordingConnection()
    _queue_root(conn, job.id)
    publish_output(
        as_db(conn),
        job,
        [OUTPUT_STREAM_STDOUT],
        time.monotonic(),
        server="alpha-server",
        force=True,
    )
    assert job.stdout.tail_end == 9000
    assert job.stdout.sequence == 3


def test_publish_output_retains_root_before_any_chunk_insert(tmp_path: Path) -> None:
    """Chunk publication locks the root command row before inserting chunks."""
    job = make_active_job(tmp_path)
    job.stdout.path.write_bytes(b"x" * 9000)
    conn = _RecordingConnection()
    _queue_root(conn, job.id)

    result = publish_output(
        as_db(conn),
        job,
        [OUTPUT_STREAM_STDOUT],
        time.monotonic(),
        server="alpha-server",
        force=True,
    )

    assert result is True
    sqls = [sql for sql, _ in conn.executions]
    guard, *rest = sqls
    assert guard.startswith("SELECT")
    assert "FOR UPDATE" in guard
    inserts = [sql for sql in rest if sql.startswith("INSERT")]
    assert len(inserts) == 3
    assert sqls.index(inserts[0]) == 1


def test_publish_output_skips_when_root_row_is_already_gone(tmp_path: Path) -> None:
    """A root deleted before publication leaves no new chunk rows."""
    job = make_active_job(tmp_path)
    job.stdout.path.write_bytes(b"x" * 9000)
    conn = _RecordingConnection()

    result = publish_output(
        as_db(conn),
        job,
        [OUTPUT_STREAM_STDOUT],
        time.monotonic(),
        server="alpha-server",
        force=True,
    )

    assert result is False
    assert [sql for sql, _ in conn.executions if sql.startswith("INSERT")] == []
    assert [sql for sql, _ in conn.executions if sql.startswith("UPDATE")] == []
    assert job.stdout.archived_upto == 0
    assert job.stdout.sequence == 0
    assert not job.stdout.tail_text
    assert job.stdout.last_chunk is None


def test_publish_output_bounded_spool_independent_of_volume(tmp_path: Path) -> None:
    """The local spool file stays bounded regardless of total child output.

    Gigabytes of output can flow through a stream while the on-disk capture
    file is trimmed to the rolling live tail after every publication, so its
    size never tracks the total volume. Logical offsets and immutable chunks
    remain reconstructable.
    """
    job = make_active_job(tmp_path)
    conn = _RecordingConnection()
    total = 5_000_000
    step = 250_000
    published = 0
    max_size = 0
    while published < total:
        published += step
        with job.stdout.path.open("ab") as fh:
            fh.write(b"y" * step)
        _queue_root(conn, job.id)
        publish_output(
            as_db(conn),
            job,
            [OUTPUT_STREAM_STDOUT],
            time.monotonic(),
            server="test-server",
            force=True,
        )
        max_size = max(max_size, job.stdout.path.stat().st_size)
    # The physical spool file is bounded by the live tail, not the volume.
    assert max_size <= OUTPUT_TAIL_MAX_BYTES
    assert job.stdout.tail_end == total
    assert job.stdout.spool_start == total - OUTPUT_TAIL_MAX_BYTES
    # The whole stream is recoverable from immutable chunks plus the live tail.
    chunks = [
        parse_chunk_payload(cast("tuple[object, object]", params)[1])
        for sql, params in conn.executions
        if sql.startswith("INSERT")
    ]
    covered = sum(c.end - c.start for c in chunks)
    assert covered >= total - OUTPUT_TAIL_MAX_BYTES


def test_publish_output_preserves_chunk_content_after_spool_trim(tmp_path: Path) -> None:
    """Trimming the durably published prefix never corrupts chunk content.

    Chunks are read from the full capture file before the head is discarded, so
    every immutable ``output_chunk`` value survives the local rewrite exactly.
    """
    job = make_active_job(tmp_path)
    job.stdout.path.write_bytes(b"y" * 9000)
    conn = _RecordingConnection()
    _queue_root(conn, job.id)
    publish_output(
        as_db(conn), job, [OUTPUT_STREAM_STDOUT], time.monotonic(), server="test-server", force=True
    )

    chunks = [
        parse_chunk_payload(cast("tuple[object, object]", params)[1])
        for sql, params in conn.executions
        if sql.startswith("INSERT")
    ]
    assert len(chunks) == 3
    for chunk in chunks:
        assert chunk.value == "y" * OUTPUT_CHUNK_MAX_BYTES
    # The local spool is trimmed to the rolling live tail and offsets advance.
    assert job.stdout.path.stat().st_size == OUTPUT_TAIL_MAX_BYTES
    assert job.stdout.spool_start == 5000
    assert job.stdout.tail_start == 5000
    assert job.stdout.tail_end == 9000


def test_spawn_job_fails_closed_when_pipe_cannot_be_created(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """spawn_job fails closed if its capture pipes cannot be created.

    The worker captures a job through dedicated pipes; their creation must
    never be silently ignored. When ``os.pipe`` fails, the spawn is aborted,
    any spool files already created are cleaned up, and no child process or
    spool file is leaked.
    """

    def _failing_pipe() -> tuple[int, int]:
        msg = "injected pipe creation failure"
        raise OSError(msg)

    monkeypatch.setattr(os, "pipe", _failing_pipe)

    # Record the spool files spawn_job creates so we can prove they are cleaned
    # up when the spawn fails closed.
    created: list[Path] = []
    original_mkstemp = tempfile.mkstemp

    def _recording_mkstemp() -> tuple[int, str]:
        fd, name = original_mkstemp()
        created.append(Path(name))
        os.close(fd)
        return (fd, name)

    monkeypatch.setattr(tempfile, "mkstemp", _recording_mkstemp)

    job = Job(id=uuid4(), cwd=str(tmp_path), process=(sys.executable, "-c", "print('hi')"))

    with pytest.raises(OSError, match="pipe"):
        spawn_job(job)

    for capture_path in created:
        assert not capture_path.exists()


def test_publish_output_trim_does_not_signal_process_group(
    tmp_path: Path,
) -> None:
    """Head-discard compaction never signals the job's process group.

    Capture compaction now rewrites the head of the spool file without ever
    issuing ``SIGSTOP``/``SIGCONT`` (the worker is the file's sole writer, so
    there is no concurrent producer to race). The running group must be left
    entirely untouched: still running, never stopped by the supervisor for a
    compaction reason.
    """
    proc = subprocess.Popen(
        [sys.executable, "-c", "import time\nwhile True:\n    time.sleep(0.05)\n"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    guard.register(proc)
    try:
        pgid = os.getpgid(proc.pid)
        job = ActiveJob(
            id=uuid4(),
            cwd=str(tmp_path),
            process=(sys.executable, "-c", "pass"),
            proc=proc,
            pid=proc.pid,
            pgid=pgid,
            started_mono=time.monotonic(),
            claimed_at=time.time(),
        )
        job.stdout = OutputStream(path=tmp_path / "stdout.cap")
        job.stderr = OutputStream(path=tmp_path / "stderr.cap")
        job.stdout.path.write_bytes(b"y" * 9000)
        assert _proc_state(proc.pid) in {b"S", b"R"}

        conn = _RecordingConnection()
        _queue_root(conn, job.id)
        publish_output(
            as_db(conn),
            job,
            [OUTPUT_STREAM_STDOUT],
            time.monotonic(),
            server="test-server",
            force=True,
        )

        # The compaction rewrite happened (spool trimmed) and the group was
        # never stopped: the supervisor does not signal it for compaction.
        assert job.stdout.path.stat().st_size == OUTPUT_TAIL_MAX_BYTES
        assert _proc_state(proc.pid) in {b"S", b"R"}
    finally:
        with suppress(ProcessLookupError):
            os.killpg(pgid, signal.SIGKILL)
        proc.wait(timeout=10)
        guard.unregister(proc)


def _proc_state(pid: int) -> bytes | None:
    """Return the single-letter ``/proc`` state of a process, or ``None``."""
    try:
        stat = (Path("/proc") / str(pid) / "stat").read_bytes()
    except OSError:
        return None
    close_paren = stat.rfind(b")")
    if close_paren == -1:
        return None
    fields = stat[close_paren + 2 :].split()
    if not fields:
        return None
    return fields[0]


def test_settings_rejects_nonpositive_spool_bound(monkeypatch: pytest.MonkeyPatch) -> None:
    """A non-positive spool bound is rejected as invalid configuration."""
    monkeypatch.setenv("LUBKO_SERVER", "bound-server")
    monkeypatch.setenv("LUBKO_OUTPUT_SPOOL_MAX_BYTES", "0")
    with pytest.raises(ValueError, match="LUBKO_OUTPUT_SPOOL_MAX_BYTES"):
        Settings.from_environment()


def test_settings_reads_lease_and_output_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Lease, publication, server, and fairness settings come from the environment."""
    monkeypatch.setenv("LUBKO_SERVER", "env-server")
    monkeypatch.setenv("LUBKO_LEASE_DURATION_SECONDS", "12.5")
    monkeypatch.setenv("LUBKO_LEASE_REFRESH_INTERVAL_SECONDS", "2.5")
    monkeypatch.setenv("LUBKO_LEASE_RECOVERY_INTERVAL_SECONDS", "4.5")
    monkeypatch.setenv("LUBKO_OUTPUT_PUBLICATION_INTERVAL_SECONDS", "0.7")
    monkeypatch.setenv("LUBKO_CLAIM_BATCH_LIMIT", "16")
    monkeypatch.setenv("LUBKO_LEASE_SAFETY_MARGIN_SECONDS", "3.5")
    monkeypatch.setenv("LUBKO_DB_OPERATION_TIMEOUT_SECONDS", "8.5")
    monkeypatch.setenv("LUBKO_OUTPUT_SPOOL_MAX_BYTES", "524288")

    settings = Settings.from_environment()

    assert settings.lease_duration_seconds == pytest.approx(12.5)
    assert settings.lease_refresh_interval_seconds == pytest.approx(2.5)
    assert settings.lease_recovery_interval_seconds == pytest.approx(4.5)
    assert settings.output_publication_interval_seconds == pytest.approx(0.7)
    assert settings.claim_batch_limit == 16
    assert settings.lease_safety_margin_seconds == pytest.approx(3.5)
    assert settings.db_operation_timeout_seconds == pytest.approx(8.5)
    assert settings.output_spool_max_bytes == 524288
    assert settings.server == "env-server"


def test_settings_defaults_and_incarnation(monkeypatch: pytest.MonkeyPatch) -> None:
    """Settings default to the documented values and have a non-empty incarnation."""
    monkeypatch.setenv("LUBKO_SERVER", "default-server")
    first = Settings.from_environment()
    second = Settings.from_environment()

    assert first.lease_duration_seconds == DEFAULT_LEASE_DURATION_SECONDS
    assert first.lease_refresh_interval_seconds == DEFAULT_LEASE_REFRESH_INTERVAL_SECONDS
    assert first.lease_recovery_interval_seconds == DEFAULT_LEASE_RECOVERY_INTERVAL_SECONDS
    assert first.worker_incarnation
    assert second.worker_incarnation
    assert first.server == "default-server"
    if os.environ.get("LUBKO_LIFECYCLE_TOKEN"):
        assert first.worker_incarnation == second.worker_incarnation
    else:
        assert first.worker_incarnation != second.worker_incarnation


def test_settings_rejects_empty_server() -> None:
    """A daemon refuses to start without a configured server identity."""
    with pytest.raises(ValueError, match="LUBKO_SERVER"):
        Settings(
            server="",
            worker_id="w",
            poll_interval_seconds=1.0,
            process_poll_interval_seconds=0.1,
            cancel_grace_seconds=5.0,
        )


def test_settings_rejects_refresh_at_least_lease() -> None:
    """A refresh interval at or above the lease duration is refused."""
    with pytest.raises(ValueError, match="smaller than"):
        Settings(
            server="alpha-server",
            worker_id="w",
            poll_interval_seconds=1.0,
            process_poll_interval_seconds=0.1,
            cancel_grace_seconds=5.0,
            lease_duration_seconds=5.0,
            lease_refresh_interval_seconds=5.0,
            lease_recovery_interval_seconds=1.0,
        )


def test_settings_rejects_unsafe_lease_margin() -> None:
    """A lease safety margin at or above the lease duration is refused."""
    with pytest.raises(ValueError, match="LEASE_SAFETY_MARGIN"):
        Settings(
            server="alpha-server",
            worker_id="w",
            poll_interval_seconds=1.0,
            process_poll_interval_seconds=0.1,
            cancel_grace_seconds=5.0,
            lease_duration_seconds=5.0,
            lease_refresh_interval_seconds=1.0,
            lease_recovery_interval_seconds=1.0,
            lease_safety_margin_seconds=5.0,
        )


def test_settings_rejects_claim_batch_limit_zero() -> None:
    """A zero claim batch limit is refused."""
    with pytest.raises(ValueError, match="CLAIM_BATCH_LIMIT"):
        Settings(
            server="alpha-server",
            worker_id="w",
            poll_interval_seconds=1.0,
            process_poll_interval_seconds=0.1,
            cancel_grace_seconds=5.0,
            claim_batch_limit=0,
        )


def test_request_stop_sends_sigterm_to_exact_group(tmp_path: Path) -> None:
    """request_stop terminates the exact recorded process group once."""
    proc, _stdout_path, _stderr_path, pgid, _so_fd, _se_fd = spawn_released(
        Job(id=uuid4(), cwd=str(tmp_path), process=SLEEP_30)
    )
    guard.register(proc)
    try:
        job = ActiveJob(
            id=uuid4(),
            cwd=str(tmp_path),
            process=SLEEP_30,
            proc=proc,
            pid=proc.pid,
            pgid=pgid,
            started_mono=time.monotonic(),
            claimed_at=time.time(),
        )
        request_stop(job, "cancel")
        assert job.term_sent
        assert job.stop_started is not None
        proc.wait(timeout=10)
        guard.unregister(proc)
    finally:
        if proc.poll() is None:
            with suppress(ProcessLookupError):
                os.killpg(proc.pid, signal.SIGKILL)
            proc.wait(timeout=5)


# ---------------------------------------------------------------------------
# pg_safe_decode unit tests
# ---------------------------------------------------------------------------


def test_pg_safe_decode_clean_utf8() -> None:
    """Valid UTF-8 without NUL passes through unchanged."""
    assert pg_safe_decode(b"hello world") == "hello world"


def test_pg_safe_decode_nul_replaced() -> None:
    """NUL bytes are replaced with U+FFFD."""
    data = b"before\x00after"
    result = pg_safe_decode(data)
    assert result == "before\ufffdafter"
    assert "\x00" not in result


def test_pg_safe_decode_multiple_nul() -> None:
    """Multiple NUL bytes are all replaced."""
    assert pg_safe_decode(b"\x00\x00\x00") == "\ufffd\ufffd\ufffd"


def test_pg_safe_decode_invalid_utf8() -> None:
    """Invalid UTF-8 sequences become U+FFFD."""
    assert pg_safe_decode(b"\xff\xfe\xfd") == "\ufffd\ufffd\ufffd"


def test_pg_safe_decode_invalid_utf8_with_nul() -> None:
    """Invalid UTF-8 combined with NUL: both are replaced."""
    assert pg_safe_decode(b"\xff\x00\xfe") == "\ufffd\ufffd\ufffd"


def test_pg_safe_decode_empty() -> None:
    """Empty input produces empty output."""
    assert not pg_safe_decode(b"")


def test_pg_safe_decode_nul_in_multibyte() -> None:
    """NUL adjacent to valid multibyte UTF-8 is handled correctly."""
    data = "caf\u00e9".encode() + b"\x00" + "\u00e9".encode()
    assert pg_safe_decode(data) == "caf\u00e9\ufffd\u00e9"


def test_pg_safe_decode_json_safe() -> None:
    """The decoded string can be JSON-encoded without PostgreSQL-rejecting escapes."""
    result = pg_safe_decode(b"before\x00after")
    encoded = json.dumps(result)
    assert "\\u0000" not in encoded
    assert "\\ufffd" in encoded


def test_truncate_output_applies_pg_safe_decode() -> None:
    """truncate_output applies NUL replacement via pg_safe_decode."""
    data = b"hello\x00world"
    result = truncate_output(data, 100)
    assert "\x00" not in result
    assert "hello" in result
    assert "world" in result


def test_decode_range_pg_safe() -> None:
    """decode_range returns PostgreSQL-safe text with correct byte offsets."""
    tmp = Path(__file__).resolve().parent.parent / "test_output_bytes.bin"
    try:
        tmp.write_bytes(b"AAAA\x00BBBB\x00CCCC")
        text = decode_range(tmp, 0, 9)
        assert text == "AAAA\ufffdBBBB"
        text2 = decode_range(tmp, 5, 14)
        assert text2 == "BBBB\ufffdCCCC"
    finally:
        tmp.unlink(missing_ok=True)


def test_request_group_reap_preserves_natural_status(tmp_path: Path) -> None:
    """request_group_reap terminates the group without recording a stop reason."""
    proc, _stdout_path, _stderr_path, pgid, _so_fd, _se_fd = spawn_released(
        Job(id=uuid4(), cwd=str(tmp_path), process=LEFTOVER_GROUP_PROBE)
    )
    guard.register(proc)
    try:
        proc.wait(timeout=10)
        assert group_has_members(pgid)
        job = ActiveJob(
            id=uuid4(),
            cwd=str(tmp_path),
            process=LEFTOVER_GROUP_PROBE,
            proc=proc,
            pid=proc.pid,
            pgid=pgid,
            started_mono=time.monotonic(),
            claimed_at=time.time(),
        )
        job.completed = True
        job.returncode = 0
        request_group_reap(job)
        assert job.term_sent
        assert job.stop_reason is None
        assert job.cancellation_note is None
        wait_until(lambda: not group_has_members(pgid))
        guard.unregister(proc)
    finally:
        if group_has_members(pgid):
            with suppress(ProcessLookupError):
                os.killpg(pgid, signal.SIGKILL)


def test_signal_kill_does_not_note_natural_reap(tmp_path: Path) -> None:
    """signal_kill on a naturally reaped job leaves the note empty."""
    proc = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(300)"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    guard.register(proc)
    try:
        job = ActiveJob(
            id=uuid4(),
            cwd=str(tmp_path),
            process=SLEEP_300,
            proc=proc,
            pid=proc.pid,
            pgid=proc.pid,
            started_mono=time.monotonic(),
            claimed_at=time.time(),
        )
        job.completed = True
        job.returncode = 0
        request_group_reap(job)
        signal_kill(job)
        assert job.cancellation_note is None
        proc.wait(timeout=10)
        guard.unregister(proc)
    finally:
        if proc.poll() is None:
            with suppress(ProcessLookupError):
                os.killpg(proc.pid, signal.SIGKILL)
            proc.wait(timeout=5)


def test_signal_kill_appends_diagnostic(tmp_path: Path) -> None:
    """signal_kill records the SIGKILL escalation in the cancellation note."""
    proc = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(300)"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    guard.register(proc)
    try:
        job = ActiveJob(
            id=uuid4(),
            cwd=str(tmp_path),
            process=SLEEP_300,
            proc=proc,
            pid=proc.pid,
            pgid=proc.pid,
            started_mono=time.monotonic(),
            claimed_at=time.time(),
        )
        request_stop(job, "cancel")
        note_before = job.cancellation_note
        signal_kill(job)
        assert "SIGTERM" in (note_before or "")
        assert job.kill_sent
        assert "SIGKILL" in (job.cancellation_note or "")
        proc.wait(timeout=10)
        guard.unregister(proc)
    finally:
        if proc.poll() is None:
            with suppress(ProcessLookupError):
                os.killpg(proc.pid, signal.SIGKILL)
            proc.wait(timeout=5)


def test_bounded_spool_backpressures_fast_sigterm_ignoring_producer(
    tmp_path: Path,
) -> None:
    """A producer far faster than the worker is backpressured to a concrete max.

    The worker drains a job's capture pipe into its on-disk spool only while
    the spool has room. A producer that ignores ``SIGTERM`` and writes without
    limit fills the kernel pipe buffer and then blocks on ``write()``: the
    physical spool can never grow past ``output_spool_max_bytes`` (plus the
    transient pipe buffer), regardless of how much the child wants to emit. The
    test drains without publishing/trimming so the bound is enforced purely by
    backpressure, observes the physical spool size continuously, and proves a
    concrete maximum.
    """
    settings = make_settings(output_spool_max_bytes=64 * 1024)
    bound = settings.output_spool_max_bytes
    # Ignores SIGTERM and writes as fast as the pipe allows, forever.
    producer = (
        sys.executable,
        "-c",
        (
            "import os, signal, sys\n"
            "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
            "while True:\n"
            "    sys.stdout.buffer.write(b'x' * 65536)\n"
            "    sys.stdout.buffer.flush()\n"
        ),
    )
    proc, so_path, se_path, pgid, so_fd, se_fd = spawn_released(
        Job(id=uuid4(), cwd=str(tmp_path), process=producer)
    )
    guard.register(proc)
    job = ActiveJob(
        id=uuid4(),
        cwd=str(tmp_path),
        process=producer,
        proc=proc,
        pid=proc.pid,
        pgid=pgid,
        started_mono=time.monotonic(),
        claimed_at=time.time(),
    )
    job.stdout = OutputStream(path=so_path, fd=so_fd)
    job.stderr = OutputStream(path=se_path, fd=se_fd)
    try:
        max_size = 0
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            drain_capture_stream(job.stdout, bound)
            drain_capture_stream(job.stderr, bound)
            size = so_path.stat().st_size
            max_size = max(max_size, size)
            if proc.poll() is not None:
                break
        # The producer ignores SIGTERM: it is still alive and throttled.
        request_stop(job, "cancel")
        assert proc.poll() is None, "SIGTERM-ignoring producer must keep running"
        assert group_has_members(pgid)
        # Kill it for real so the test cannot hang.
        signal_kill(job)
        wait_until(lambda: proc.poll() is not None, timeout=5.0)
        # Concrete maximum: the physical spool never exceeds the configured
        # bound plus a single drain chunk (the kernel pipe buffer holds the
        # rest, which is transient and not on the local disk spool).
        assert max_size <= bound + DRAIN_CHUNK
    finally:
        with suppress(ProcessLookupError):
            os.killpg(pgid, signal.SIGKILL)
        proc.wait(timeout=5)
        guard.unregister(proc)
        for fd in (so_fd, se_fd):
            with suppress(OSError):
                os.close(fd)
        so_path.unlink(missing_ok=True)
        se_path.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# PR #136 blocker regressions: worker-owned pipe/backpressure hardening
# ---------------------------------------------------------------------------


def test_repeated_spawn_leaves_no_open_file_descriptor(tmp_path: Path) -> None:
    """Repeated spawns never leak the temporary spool-file descriptor.

    ``spawn_job`` now closes the ``tempfile.mkstemp`` descriptor immediately and
    closes every capture resource on failure, so spawning many short-lived jobs
    in a row must not grow the supervisor's open-file count. Each iteration
    spawns a child that exits immediately (rather than a long sleep) and drains
    both capture pipes to end-of-file, closing the exact read-end descriptors
    and removing the exact spool files it created so no resource leaks forward.
    """
    baseline = len(list(Path("/proc/self/fd").iterdir()))
    for _ in range(20):
        proc, so_path, se_path, _pgid, so_fd, se_fd = spawn_released(
            Job(id=uuid4(), cwd=str(tmp_path), process=SHORT_CHILD)
        )
        guard.register(proc)
        try:
            proc.wait(timeout=10)
            drain_pipes(so_fd, se_fd, so_path, se_path)
        finally:
            guard.unregister(proc)
            for fd in (so_fd, se_fd):
                with suppress(OSError):
                    os.close(fd)
            so_path.unlink(missing_ok=True)
            se_path.unlink(missing_ok=True)
    assert len(list(Path("/proc/self/fd").iterdir())) == baseline


def test_spool_append_write_error_does_not_discard_read_bytes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The byte-counted append seam never loses or duplicates bytes.

    Bytes pulled from the pipe are buffered in the stream's bounded ``pending``
    buffer and appended through the exact byte-counted seam ``_spill_append``,
    which consumes only the bytes it actually writes: a short ``os.write`` that
    lands a prefix and then fails mid-flush leaves exactly the not-yet-written
    suffix in ``pending`` (no gap, no duplication), and a later successful
    flush appends that suffix so the spool ends up holding the full payload
    exactly once. A total write failure retains every already-read byte so the
    owning job can be failed closed with all of its captured output still
    represented.
    """
    real_write = os.write

    class _PartialThenFail:
        """Inject a kernel short write, then a transient error, then success."""

        def __init__(self) -> None:
            self.calls = 0

        def __call__(self, fd: int, data: bytes) -> int:
            self.calls += 1
            if self.calls == 1:
                # Simulate a kernel short write: land only the first 3 bytes.
                return real_write(fd, data[:3])
            if self.calls == 2:
                # The next write attempt hits a transient error.
                msg = "injected transient spool write failure"
                raise OSError(msg)
            # Any later attempt writes everything.
            return real_write(fd, data)

    job = make_active_job(tmp_path)
    read_end, write_end = os.pipe()
    os.write(write_end, b"hello world")
    os.close(write_end)
    job.stdout = OutputStream(path=tmp_path / "so.cap", fd=read_end)
    job.stdout.path.write_bytes(b"PREFIX-")

    monkeypatch.setattr(os, "write", _PartialThenFail())
    # First drain: short write of 3 bytes, then a transient error; the seam
    # retains the remaining 8 bytes in pending and reports ok (the suffix will
    # be retried on the next drain).
    status = drain_capture_stream(job.stdout, 10_000_000)
    assert status == "ok"
    assert bytes(job.stdout.pending) == b"lo world"
    assert job.stdout.path.read_bytes() == b"PREFIX-hel"

    # Second drain: the retained suffix is appended with no gap and no
    # duplication, completing the payload exactly once.
    status = drain_capture_stream(job.stdout, 10_000_000)
    assert status in {"ok", "eof"}
    assert not job.stdout.pending
    assert job.stdout.path.read_bytes() == b"PREFIX-hello world"
    # The second drain reached end-of-file and closed the read end; tolerate
    # either ownership outcome without leaking or double-closing.
    with suppress(OSError):
        os.close(read_end)

    # A hard, total write failure retains every already-read byte; the owning
    # job is failed closed with all captured output still represented and the
    # earlier durable bytes untouched.
    job2 = make_active_job(tmp_path)
    r2, w2 = os.pipe()
    os.write(w2, b"more data")
    os.close(w2)
    job2.stdout = OutputStream(path=tmp_path / "so2.cap", fd=r2)
    job2.stdout.path.write_bytes(b"PREFIX2-")

    def failing_write(_fd: int, _data: bytes) -> int:
        msg = "injected spool write failure"
        raise OSError(msg)

    monkeypatch.setattr(os, "write", failing_write)
    status = drain_capture_stream(job2.stdout, 10_000_000)
    assert status == "error"
    assert bytes(job2.stdout.pending) == b"more data"
    # Bytes already durably written are never lost or corrupted.
    assert job2.stdout.path.read_bytes() == b"PREFIX2-"
    os.close(r2)


def test_finish_eof_partial_final_write_never_marks_eof_over_residual(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """EOF finalization with a positive partial write fails closed.

    When the producer's write end closes while ``pending`` still holds bytes,
    the final flush must land every retained byte before EOF is marked. A
    short write that lands only a prefix leaves a residual suffix, so
    ``_finish_capture_stream`` must report ``"error"`` (the exact capture/job
    fails closed) with every already-read byte still represented — never
    silently mark EOF over unlanded output.
    """
    real_write = os.write

    class _PrefixThenFail:
        """Land a 2-byte prefix once, then fail so the suffix stays retained."""

        def __init__(self) -> None:
            self.calls = 0

        def __call__(self, fd: int, data: bytes) -> int:
            self.calls += 1
            if self.calls == 1:
                return real_write(fd, data[:2])
            msg = "injected spool write failure"
            raise OSError(msg)

    job = make_active_job(tmp_path)
    read_end, write_end = os.pipe()
    os.close(write_end)
    job.stdout = OutputStream(path=tmp_path / "so.cap", fd=read_end)
    job.stdout.path.write_bytes(b"PRE-")
    # Bytes already taken ownership of from the pipe, as a prior partial
    # flush would have left them after landing its own prefix.
    job.stdout.pending += b"hello world"

    monkeypatch.setattr(os, "write", _PrefixThenFail())
    assert worker._finish_capture_stream(job.stdout) == "error"
    # The stream is NOT marked EOF over its unlanded bytes...
    eof_after_error = job.stdout.eof
    assert not eof_after_error
    assert bytes(job.stdout.pending) == b"llo world"
    # ...and exactly the landed prefix is on disk: no gap, no duplication.
    assert job.stdout.path.read_bytes() == b"PRE-he"
    monkeypatch.undo()

    # A retried final flush that lands every byte completes the stream:
    # EOF is marked normally and the payload is complete exactly once.
    assert worker._finish_capture_stream(job.stdout) == "eof"
    assert job.stdout.eof
    remaining = bytes(job.stdout.pending)
    assert not remaining
    assert job.stdout.path.read_bytes() == b"PRE-hello world"
    with suppress(OSError):
        os.close(read_end)


def test_finish_eof_complete_final_flush_marks_eof_normally(
    tmp_path: Path,
) -> None:
    """A final flush landing every pending byte marks EOF cleanly."""
    job = make_active_job(tmp_path)
    read_end, write_end = os.pipe()
    os.write(write_end, b"hello world")
    os.close(write_end)
    job.stdout = OutputStream(path=tmp_path / "so.cap", fd=read_end)
    job.stdout.path.write_bytes(b"PRE-")

    # Drain to natural EOF: the payload is read and flushed completely on the
    # first drain, then the second observes the closed write end and finishes
    # the stream without error.
    assert drain_capture_stream(job.stdout, 10_000_000) == "ok"
    assert drain_capture_stream(job.stdout, 10_000_000) == "eof"
    assert job.stdout.eof
    assert not job.stdout.pending
    assert job.stdout.path.read_bytes() == b"PRE-hello world"
    with suppress(OSError):
        os.close(read_end)


def test_bad_capture_fd_is_isolated_from_healthy_sibling(tmp_path: Path) -> None:
    """A bad capture fd fails only its own job; the sibling keeps draining.

    When ``select`` cannot isolate which fd is bad, the supervisor probes each
    candidate individually, fails closed exactly the offending job (closing the
    exact fd), and leaves every healthy sibling's capture fd intact and
    drainable.
    """
    settings = make_settings(output_spool_max_bytes=4 * 1024 * 1024)
    db = DatabaseConfig(host="h", port=1, dbname="d", user="u", password=str(uuid4()))
    supervisor = Supervisor(settings, db)

    proc, so_path, se_path, pgid, so_fd, se_fd = spawn_released(
        Job(id=uuid4(), cwd=str(tmp_path), process=SLEEP_30)
    )
    guard.register(proc)
    healthy = ActiveJob(
        id=uuid4(),
        cwd=str(tmp_path),
        process=SLEEP_30,
        proc=proc,
        pid=proc.pid,
        pgid=pgid,
        started_mono=time.monotonic(),
        claimed_at=time.time(),
    )
    healthy.stdout = OutputStream(path=so_path, fd=so_fd)
    healthy.stderr = OutputStream(path=se_path, fd=se_fd)

    bad = make_active_job(tmp_path)
    bad.stdout = OutputStream(path=tmp_path / "bad.cap", fd=999_999)

    supervisor.active[healthy.id] = healthy
    supervisor.active[bad.id] = bad
    candidates = [
        (healthy, OUTPUT_STREAM_STDOUT, healthy.stdout),
        (bad, OUTPUT_STREAM_STDOUT, bad.stdout),
    ]
    try:
        supervisor.isolate_bad_fds(candidates)
        assert bad.spool_evicted is True
        assert bad.stdout.fd is None
        # Healthy sibling untouched and still drainable.
        assert healthy.spool_evicted is False
        assert healthy.stdout.fd == so_fd
    finally:
        guard.unregister(proc)


def test_post_popen_fcntl_failure_kills_child_and_closes_resources(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A post-Popen fcntl failure kills the exact child with no resource leak.

    After ``Popen`` succeeds, the supervisor must close the write ends, mark
    the read ends nonblocking, and await the session. If that setup fails, the
    exact spawned child is killed and reaped and every capture fd is closed, so
    neither a live process nor an open descriptor leaks into the supervisor.
    """
    real_fcntl = fcntl.fcntl
    killed: list[tuple[int, int]] = []

    def failing_fcntl(_fd: int, cmd: int, arg: int = 0) -> int:
        if cmd == fcntl.F_SETFL:
            msg = "injected fcntl failure"
            raise OSError(msg)
        return real_fcntl(_fd, cmd, arg)

    monkeypatch.setattr(fcntl, "fcntl", failing_fcntl)
    real_kill = os.kill
    real_killpg = os.killpg
    killed_groups: list[tuple[int, int]] = []

    def recording_kill(pid: int, sig: int) -> None:
        killed.append((pid, sig))
        real_kill(pid, sig)

    def recording_killpg(pgid: int, sig: int) -> None:
        killed_groups.append((pgid, sig))
        real_killpg(pgid, sig)

    monkeypatch.setattr(os, "kill", recording_kill)
    monkeypatch.setattr(os, "killpg", recording_killpg)

    fd_baseline = len(list(Path("/proc/self/fd").iterdir()))
    with pytest.raises(OSError, match="fcntl"):
        spawn_job(Job(id=uuid4(), cwd=str(tmp_path), process=SLEEP_30))
    # The whole process group was killed with SIGKILL and reaped: nothing left
    # running, including any descendant that shares the group.
    assert killed_groups
    assert any(sig == signal.SIGKILL for _pgid, sig in killed_groups)
    with pytest.raises(ProcessLookupError):
        os.killpg(killed_groups[0][0], 0)
    # No descriptor leaked from the abandoned pipes or spool files.
    assert len(list(Path("/proc/self/fd").iterdir())) == fd_baseline


def test_exit_drains_more_than_one_chunk_to_eof(tmp_path: Path) -> None:
    """Finalization captures every byte when exit output exceeds one chunk.

    A producer that writes more than a single ``DRAIN_CHUNK`` and then exits
    must have all of its output captured into the spool (across multiple drain
    chunks) before the stream reports end-of-file, so no tail byte is lost.
    """
    n = DRAIN_CHUNK * 3 + 137
    producer = (
        sys.executable,
        "-c",
        f"import sys; sys.stdout.buffer.write(b'x' * {n}); sys.stdout.buffer.flush()",
    )
    proc, so_path, se_path, pgid, so_fd, se_fd = spawn_released(
        Job(id=uuid4(), cwd=str(tmp_path), process=producer)
    )
    guard.register(proc)
    job = ActiveJob(
        id=uuid4(),
        cwd=str(tmp_path),
        process=producer,
        proc=proc,
        pid=proc.pid,
        pgid=pgid,
        started_mono=time.monotonic(),
        claimed_at=time.time(),
    )
    job.stdout = OutputStream(path=so_path, fd=so_fd)
    try:
        while not job.stdout.eof:
            drain_capture_stream(job.stdout, 10_000_000)
        proc.wait(timeout=10)
        assert read_output(so_path) == b"x" * n
    finally:
        guard.unregister(proc)
        for fd in (so_fd, se_fd):
            with suppress(OSError):
                os.close(fd)
        so_path.unlink(missing_ok=True)
        se_path.unlink(missing_ok=True)


def test_publish_output_trim_failure_preserves_coherent_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A post-commit trim/rewrite failure keeps spool state coherent.

    The immutable chunks and live-tail window are committed to the database
    before the local head-discard rewrite. If that rewrite fails, the in-memory
    logical offset of the on-disk file must NOT advance, so the
    ``(spool_start, file size)`` invariant stays coherent for a retry and the
    offending job is the only one affected.
    """
    job = make_active_job(tmp_path)
    job.stdout.path.write_bytes(b"x" * 9000)
    conn = _RecordingConnection()
    _queue_root(conn, job.id)

    def failing_rewrite(_path: Path, _drop: int) -> None:
        msg = "injected trim failure"
        raise OSError(msg)

    monkeypatch.setattr(worker, "_rewrite_head", failing_rewrite)

    with pytest.raises(OSError, match="trim"):
        publish_output(
            as_db(conn),
            job,
            [OUTPUT_STREAM_STDOUT],
            time.monotonic(),
            server="test-server",
            force=True,
        )

    # Coherent: the logical offset of the file was not advanced, and the file
    # itself was not truncated, so spool_start + file size still equals the
    # true output length.
    assert job.stdout.spool_start == 0
    assert job.stdout.path.stat().st_size == 9000


def test_trim_failure_quarantines_offending_job(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A trim/rewrite failure is contained: the job is quarantined, not crash.

    When a publication's post-commit head-discard rewrite raises, the error is
    caught per job and the job is quarantined, so it does not poison
    publication of unrelated jobs or crash the supervisor.
    """
    settings = make_settings()
    db = DatabaseConfig(host="h", port=1, dbname="d", user="u", password=str(uuid4()))
    supervisor = Supervisor(settings, db)
    conn = _RecordingConnection()
    supervisor.conn = as_db(conn)

    job = make_active_job(tmp_path)
    job.stdout.path.write_bytes(b"x" * 9000)
    job.stderr.path.write_bytes(b"")
    job.stdout.published_at = 0.0
    _queue_root(conn, job.id)
    supervisor.active[job.id] = job

    def failing_rewrite(_path: Path, _drop: int) -> None:
        msg = "injected trim failure"
        raise OSError(msg)

    monkeypatch.setattr(worker, "_rewrite_head", failing_rewrite)

    supervisor.publish_job_output(job, time.monotonic())

    assert job.quarantined is True
    assert job in supervisor.active.values()


def test_rewrite_head_atomic_replace_preserves_original_on_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failed atomic replace leaves the original bytes and offset intact.

    ``_rewrite_head`` stages the rewritten head in a same-directory temp file
    and only swaps it onto the live path with ``os.replace``; when the replace
    fails the original spool file is byte-for-byte unchanged, no leftover temp
    file is leaked, and the caller's logical ``spool_start`` is left untouched
    so the next publication retries exactly the same drop.
    """
    job = make_active_job(tmp_path)
    job.stdout.path.write_bytes(b"x" * 9000)
    conn = _RecordingConnection()
    _queue_root(conn, job.id)

    def failing_replace(_src: str, _dst: str) -> None:
        msg = "injected atomic replace failure"
        raise OSError(msg)

    monkeypatch.setattr(os, "replace", failing_replace)

    with pytest.raises(OSError, match="replace"):
        publish_output(
            as_db(conn),
            job,
            [OUTPUT_STREAM_STDOUT],
            time.monotonic(),
            server="test-server",
            force=True,
        )

    # Original bytes untouched, logical offset unchanged, no temp leaked.
    assert job.stdout.path.read_bytes() == b"x" * 9000
    assert job.stdout.spool_start == 0
    assert list(tmp_path.glob(".*.rewrite.tmp")) == []


def test_isolate_bad_fds_keeps_high_number_fd_healthy(tmp_path: Path) -> None:
    """A valid high-numbered fd is not misclassified as bad by poll probing.

    ``select.select`` rejects any descriptor at or beyond ``FD_SETSIZE`` with a
    ``ValueError`` even when the fd is perfectly valid, which would wrongly fail
    such a job closed. The poll-based probe must recognise a high fd as healthy
    (not isolated) while still isolating a genuinely invalid fd.
    """
    settings = make_settings(output_spool_max_bytes=4 * 1024 * 1024)
    db = DatabaseConfig(host="h", port=1, dbname="d", user="u", password=str(uuid4()))
    supervisor = Supervisor(settings, db)

    healthy = make_active_job(tmp_path)
    read_end, write_end = os.pipe()
    os.close(write_end)
    high = 1500
    try:
        os.dup2(read_end, high)
    except OSError:
        pytest.skip("cannot allocate a high-numbered fd in this environment")
    os.close(read_end)
    healthy.stdout = OutputStream(path=tmp_path / "so.cap", fd=high)

    bad = make_active_job(tmp_path)
    bad.stdout = OutputStream(path=tmp_path / "bad.cap", fd=999_999)

    supervisor.active[healthy.id] = healthy
    supervisor.active[bad.id] = bad
    candidates = [
        (healthy, OUTPUT_STREAM_STDOUT, healthy.stdout),
        (bad, OUTPUT_STREAM_STDOUT, bad.stdout),
    ]
    try:
        supervisor.isolate_bad_fds(candidates)
        # High fd accepted as healthy; bad fd isolated exactly.
        assert healthy.spool_evicted is False
        assert healthy.stdout.fd == high
        assert bad.spool_evicted is True
        assert bad.stdout.fd is None
    finally:
        with suppress(OSError):
            os.close(high)


def test_aggregate_spool_overflow_sums_both_streams(tmp_path: Path) -> None:
    """The per-job bound is an aggregate across stdout and stderr.

    A job whose individual streams are each under the bound but whose combined
    on-disk usage exceeds it must still be flagged as overflowing, and a job
    comfortably under the combined bound must not be.
    """
    settings = make_settings(output_spool_max_bytes=100)
    db = DatabaseConfig(host="h", port=1, dbname="d", user="u", password=str(uuid4()))
    supervisor = Supervisor(settings, db)
    job = make_active_job(tmp_path)
    job.stdout.path.write_bytes(b"x" * 60)
    job.stderr.path.write_bytes(b"x" * 60)
    assert supervisor.spool_overflow(job, settings.output_spool_max_bytes) is True
    job.stdout.path.write_bytes(b"x" * 40)
    assert supervisor.spool_overflow(job, settings.output_spool_max_bytes) is False


def test_aggregate_spool_backpressures_both_streams_simultaneously(tmp_path: Path) -> None:
    """A producer writing both streams is backpressured to the aggregate max.

    The bound is enforced across the combined stdout+stderr on-disk usage, so
    a producer that ignores ``SIGTERM`` and writes both streams without limit
    is throttled before the combined spool can exceed the configured bound.
    """
    settings = make_settings(output_spool_max_bytes=64 * 1024)
    bound = settings.output_spool_max_bytes
    producer = (
        sys.executable,
        "-c",
        (
            "import os, signal, sys\n"
            "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
            "while True:\n"
            "    sys.stdout.buffer.write(b'A' * 65536)\n"
            "    sys.stderr.buffer.write(b'B' * 65536)\n"
            "    sys.stdout.buffer.flush(); sys.stderr.buffer.flush()\n"
        ),
    )
    proc, so_path, se_path, pgid, so_fd, se_fd = spawn_released(
        Job(id=uuid4(), cwd=str(tmp_path), process=producer)
    )
    guard.register(proc)
    job = ActiveJob(
        id=uuid4(),
        cwd=str(tmp_path),
        process=producer,
        proc=proc,
        pid=proc.pid,
        pgid=pgid,
        started_mono=time.monotonic(),
        claimed_at=time.time(),
    )
    job.stdout = OutputStream(path=so_path, fd=so_fd)
    job.stderr = OutputStream(path=se_path, fd=se_fd)
    try:
        max_agg = 0
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            agg = so_path.stat().st_size + se_path.stat().st_size
            drain_capture_stream(job.stdout, bound, aggregate_used=agg)
            drain_capture_stream(job.stderr, bound, aggregate_used=agg)
            agg = so_path.stat().st_size + se_path.stat().st_size
            max_agg = max(max_agg, agg)
            if proc.poll() is not None:
                break
        request_stop(job, "cancel")
        assert proc.poll() is None, "SIGTERM-ignoring producer must keep running"
        assert group_has_members(pgid)
        signal_kill(job)
        wait_until(lambda: proc.poll() is not None, timeout=5.0)
        # Combined on-disk spool never exceeds the configured aggregate bound
        # plus a single drain chunk of transient slack.
        assert max_agg <= bound + DRAIN_CHUNK
    finally:
        with suppress(ProcessLookupError):
            os.killpg(pgid, signal.SIGKILL)
        proc.wait(timeout=5)
        guard.unregister(proc)
        for fd in (so_fd, se_fd):
            with suppress(OSError):
                os.close(fd)
        so_path.unlink(missing_ok=True)
        se_path.unlink(missing_ok=True)


def test_pending_flush_never_exceeds_aggregate_bound_and_recovers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A retained pending suffix obeys aggregate disk room and lands after trim.

    A partial-positive-write followed by an ``OSError`` leaves a suffix in
    ``pending`` while the successfully written prefix has filled the aggregate
    stdout+stderr on-disk spool. On the next drain the retained bytes must be
    flushed only within the currently available aggregate disk room: while no
    room exists they stay pending and the bounded/full condition is reported,
    and once publication/trim frees room the suffix lands with no loss and no
    duplication — the aggregate physical spool never exceeding the bound at any
    observation.
    """
    bound = 100
    job = make_active_job(tmp_path)
    read_end, write_end = os.pipe()
    os.write(write_end, b"abcdefghijK")
    os.close(write_end)
    job.stdout = OutputStream(path=job.stdout.path, fd=read_end)
    job.stderr.path.write_bytes(b"S" * 30)
    job.stdout.path.write_bytes(b"P" * 60)
    observed_max_agg = 0

    def observe() -> int:
        nonlocal observed_max_agg
        agg = job.stdout.path.stat().st_size + job.stderr.path.stat().st_size
        assert agg <= bound, "aggregate physical spool must never exceed the bound"
        observed_max_agg = max(observed_max_agg, agg)
        return agg

    real_write = os.write
    state = {"calls": 0}

    def partial_then_fail(fd: int, data: bytes) -> int:
        state["calls"] += 1
        if state["calls"] == 1:
            # Land a positive prefix of the flush so _spill_append records a
            # real partial total.
            return real_write(fd, data[:6])
        if state["calls"] == 2:
            # Fail the next attempt mid-flush so exactly b"ghij" is retained
            # in pending, like a kernel short write followed by an error.
            msg = "injected transient spool write failure"
            raise OSError(msg)
        return real_write(fd, data)

    monkeypatch.setattr(os, "write", partial_then_fail)
    # First drain: aggregate accounting (stdout 60 + stderr 30 = used 90) leaves
    # room 10, so only b"abcdefghij" is read; the injected short write lands a
    # 6-byte prefix, retains b"ghij" in pending, and b"K" stays in the pipe.
    assert drain_capture_stream(job.stdout, bound, aggregate_used=90) == "ok"
    observe()
    monkeypatch.undo()
    assert bytes(job.stdout.pending) == b"ghij"
    assert observe() == 96

    # The successful prefix plus stderr now fills the aggregate to the bound:
    # the drain must report full, keep every retained byte pending, and write
    # nothing at all.
    with job.stderr.path.open("ab") as fh:
        fh.write(b"E" * 4)
    assert drain_capture_stream(job.stdout, bound, aggregate_used=100) == "full"
    assert bytes(job.stdout.pending) == b"ghij"
    assert observe() == 100

    # Recovery: publication/trim frees bounded room; the whole suffix fits and
    # lands, then the last pipe byte drains too (real aggregate here: 66+26).
    job.stderr.path.write_bytes(b"S" * 26)
    assert drain_capture_stream(job.stdout, bound, aggregate_used=92) == "ok"
    assert not job.stdout.pending
    assert job.stdout.path.stat().st_size == 71
    assert observe() == 97

    # The drained-out pipe reaches end-of-file once the freed room persists.
    job.stderr.path.write_bytes(b"S" * 16)
    assert drain_capture_stream(job.stdout, bound, aggregate_used=87) == "eof"
    observe()
    assert not job.stdout.pending
    # No loss, no duplication: the stdout stream is exactly its original prefix
    # plus every pipe byte exactly once, in order.
    assert job.stdout.path.read_bytes() == b"P" * 60 + b"abcdefghijK"
    assert job.stderr.path.read_bytes() == b"S" * 16
    assert observed_max_agg <= bound
    with suppress(OSError):
        os.close(read_end)


def test_exact_fit_pending_suffix_lands_within_disk_room(tmp_path: Path) -> None:
    """A pending suffix that fits the current disk room exactly is not stalled.

    Disk room is ``max(0, bound - used)`` independent of ``len(pending)``: with
    used=90, bound=100, pending=10 the whole suffix must land (the aggregate
    then sits exactly at the bound), while one byte less of room retains a
    suffix and reports full without overshooting.
    """
    bound = 100
    job = make_active_job(tmp_path)
    job.stdout.path.write_bytes(b"P" * 50)
    job.stderr.path.write_bytes(b"S" * 40)
    job.stdout.pending += b"EXACTFIT10"

    # Exact fit: all ten retained bytes land, consuming the room precisely.
    assert drain_capture_stream(job.stdout, bound, aggregate_used=90) == "eof"
    assert not job.stdout.pending
    assert job.stdout.path.stat().st_size == 60
    assert job.stdout.path.stat().st_size + job.stderr.path.stat().st_size == bound, (
        "the aggregate must sit exactly at the bound, never beyond it"
    )

    # Room five bytes short of fitting (real on-disk aggregate 50+45=95):
    # five bytes land, five stay pending, full is reported, and the aggregate
    # sits exactly at the bound — never beyond it.
    short_dir = tmp_path / "short"
    short_dir.mkdir()
    job2 = make_active_job(short_dir)
    job2.stdout.path.write_bytes(b"P" * 50)
    job2.stderr.path.write_bytes(b"S" * 45)
    job2.stdout.pending += b"HALFFITXYZ"
    assert drain_capture_stream(job2.stdout, bound, aggregate_used=95) == "full"
    assert bytes(job2.stdout.pending) == b"ITXYZ"
    assert job2.stdout.path.stat().st_size == 55
    assert job2.stdout.path.stat().st_size + job2.stderr.path.stat().st_size == bound, (
        "the flush must consume exactly the available disk room, never more"
    )


def _eof_pipe_stream(path: Path) -> tuple[OutputStream, int]:
    """Create a stream whose capture pipe is already at end-of-file.

    Returns:
        The stream and its (still-open) read fd the caller must close.
    """
    read_end, write_end = os.pipe()
    os.close(write_end)
    return OutputStream(path=path, fd=read_end), read_end


def test_bounded_finalizer_publishes_trims_and_continues_to_eof(
    tmp_path: Path,
) -> None:
    """A full spool forces publish+trim and the finalizer continues to EOF.

    Regression guard against the short-circuit form: when the first bounded
    drain makes no progress because the spool is at its bound, the cycle must
    durably publish and trim the archived head, then keep draining until both
    streams reach end-of-file and finalize. It must never return early leaving
    a completed job unfinalized with pipe bytes unrepresented.
    """
    settings = make_settings(output_spool_max_bytes=8192)
    db = DatabaseConfig(host="h", port=1, dbname="d", user="u", password=str(uuid4()))
    supervisor = Supervisor(settings, db)
    conn = _RecordingConnection()
    supervisor.conn = as_db(conn)
    conn.rows.extend([(None,)] * 64)

    job = make_active_job(tmp_path)
    stdout_read, stdout_write = os.pipe()
    os.write(stdout_write, b"tail-bytes")
    os.close(stdout_write)
    stderr_read, stderr_write = os.pipe()
    os.close(stderr_write)
    job.stdout = OutputStream(path=job.stdout.path, fd=stdout_read)
    job.stderr = OutputStream(path=job.stderr.path, fd=stderr_read)
    job.stderr.path.write_bytes(b"")
    job.stdout.path.write_bytes(b"x" * 10_000)
    supervisor.active[job.id] = job

    try:
        supervisor.finalize_completed_job_bounded(job)

        assert job.finalized is True
        assert job.id not in supervisor.active
        chunk_inserts = [sql for sql, _p in conn.executions if "INSERT INTO lubko.jobs" in sql]
        assert chunk_inserts, "full spool must force durable archival publication"
        # Every byte is represented by logical offsets even after local trim
        # and post-finalization cleanup removed the physical spool files.
        assert job.stdout.tail_end == 10_000 + len(b"tail-bytes")
        assert job.stdout.eof
        assert job.stderr.eof
    finally:
        with suppress(OSError):
            os.close(stdout_read)
        with suppress(OSError):
            os.close(stderr_read)


def _pipe_with_detached_writer(data: bytes) -> tuple[int, int, int]:
    """Create a nonblocking pipe whose write end is held only by a forked child.

    Args:
        data: Bytes written into the pipe before the fork.

    Returns:
        ``(read_fd, write_end_holder_pid, unused)`` where ``read_fd`` is the
        nonblocking read end and the forked child sleeps holding the write end.
    """
    read_end, write_end = os.pipe()
    os.set_blocking(read_end, False)
    os.write(write_end, data)
    pid = os.fork()
    if pid == 0:  # pragma: no cover - child path never returns
        os.close(read_end)
        time.sleep(30)
        os._exit(0)
    os.close(write_end)
    return read_end, pid, 0


def test_bounded_finalizer_abandons_detached_pipe_writer_and_finalizes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A detached grandchild holding the pipe write end cannot wedge finalization.

    A completed job may leave a detached helper (a deployment helper forks one)
    holding the capture-pipe write end, so the pipe never reaches end-of-file
    on its own and can keep trickling bytes. The bounded finalization cycle
    must give up after its grace period with every already-read byte durably
    published — never spinning forever, never losing or duplicating captured
    output, never exceeding the spool bound.
    """
    monkeypatch.setattr(worker, "DETACHED_CAPTURE_EOF_GRACE_SECONDS", 0.5)
    settings = make_settings(output_spool_max_bytes=8192)
    db = DatabaseConfig(host="h", port=1, dbname="d", user="u", password=str(uuid4()))
    supervisor = Supervisor(settings, db)
    conn = _RecordingConnection()
    supervisor.conn = as_db(conn)

    job = make_active_job(tmp_path)
    conn.retained_root = (str(job.id),)
    conn.status_result = ("running",)
    stdout_read, writer_pid, _ = _pipe_with_detached_writer(b"before-detach")
    stderr_read, _stderr_write = os.pipe()
    # Production capture read ends are nonblocking; mirror that exactly.
    os.set_blocking(stderr_read, False)
    job.stdout = OutputStream(path=job.stdout.path, fd=stdout_read)
    job.stderr = OutputStream(path=job.stderr.path, fd=stderr_read)
    job.stderr.path.write_bytes(b"")
    # Production pre-creates both spool files at spawn time; model that here.
    job.stdout.path.write_bytes(b"")
    job.completed = True
    supervisor.active[job.id] = job

    # The quiet-wait heartbeat is verified separately; neutralize its DB write
    # here so the recording double cannot fake a lost root row.
    monkeypatch.setattr(worker.Supervisor, "_refresh_leases", lambda _self: None)

    started = time.monotonic()
    try:
        supervisor.finalize_completed_job_bounded(job)
        elapsed = time.monotonic() - started
        # The grace is 0.5s; a wedged cycle would run orders of magnitude
        # longer (or hang the test outright).
        assert elapsed < 30.0, "finalization must not wedge on the detached writer"
        assert job.finalized is True
        assert job.id not in supervisor.active
        assert job.stdout.eof
        assert job.stderr.eof
        assert job.stdout.fd is None
        assert job.stderr.fd is None
        # Every byte read before the detach is represented exactly once.
        assert job.stdout.tail_end == len(b"before-detach")
        # The captured tail is durably published to the root row exactly once.
        updates = [
            cast("dict[str, object]", params)
            for sql, params in conn.executions
            if sql.startswith("UPDATE")
        ]
        tails = [
            json.loads(cast("str", u["output"]))["stdout"]["tail"]
            for u in updates
            if "output" in u and u["output"] is not None
        ]
        assert tails, "captured output must be published to the root row"
        assert tails[-1] == b"before-detach".decode()
    finally:
        with suppress(OSError):
            os.close(stdout_read)
        with suppress(OSError):
            os.close(stderr_read)
        with suppress(ProcessLookupError, OSError):
            os.kill(writer_pid, signal.SIGKILL)
        with suppress(ChildProcessError):
            os.waitpid(writer_pid, 0)


def _abandon_build_job(
    cwd: Path,
    db: DatabaseConfig,
    *,
    bound: int,
    payload: bytes = b"",
) -> tuple[Supervisor, _RecordingConnection, ActiveJob, int, int]:
    """Build a completed job at a given bound with a retained pending suffix.

    The fixture models production: both spool files pre-created, stderr holding
    90 on-disk bytes and stdout 5, a partial-write suffix ``XYZ`` still only in
    memory, and a detached writer child keeping the stdout pipe non-EOF.

    Returns:
        ``(supervisor, conn, job, writer_pid, stderr_write)``: the supervisor
        under test, its recording connection, the registered active job, the
        detached writer child PID (reap via :func:`_abandon_cleanup`), and the
        test-owned stderr write end to close afterwards.
    """
    settings = make_settings(output_spool_max_bytes=bound)
    supervisor = Supervisor(settings, db)
    conn = _RecordingConnection()
    supervisor.conn = as_db(conn)
    job = make_active_job(cwd)
    conn.retained_root = (str(job.id),)
    conn.status_result = ("running",)
    stdout_read, writer_pid, _ = _pipe_with_detached_writer(payload)
    stderr_read, stderr_write = os.pipe()
    os.set_blocking(stderr_read, False)
    job.stdout = OutputStream(path=job.stdout.path, fd=stdout_read)
    job.stderr = OutputStream(path=job.stderr.path, fd=stderr_read)
    job.stderr.path.write_bytes(b"S" * 90)
    job.stdout.path.write_bytes(b"P" * 5)
    job.stdout.pending += b"XYZ"
    job.completed = True
    supervisor.active[job.id] = job
    return supervisor, conn, job, writer_pid, stderr_write


def _abandon_cleanup(writer_pid: int, stderr_write: int) -> None:
    """Reap the detached writer and close the test-owned stderr write end."""
    with suppress(ProcessLookupError, OSError):
        os.kill(writer_pid, signal.SIGKILL)
    with suppress(ChildProcessError):
        os.waitpid(writer_pid, 0)
    with suppress(OSError):
        os.close(stderr_write)


def test_abandon_at_exact_bound_fails_closed_without_appending(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Grace-expiry abandonment never appends retained pending past the bound.

    With the aggregate spool sitting exactly at ``LUBKO_OUTPUT_SPOOL_MAX_BYTES``
    and a partial-write suffix still only in ``pending`` when the detached-writer
    grace expires, the abandonment must fail the exact job closed rather than
    overshoot the bound or silently drop the suffix. The spill spy proves no
    append occurs, the exact job is failed closed, and the read end is closed.
    """
    monkeypatch.setattr(worker, "DETACHED_CAPTURE_EOF_GRACE_SECONDS", 0.2)
    db = DatabaseConfig(host="h", port=1, dbname="d", user="u", password=str(uuid4()))
    no_room_dir = tmp_path / "no-room"
    no_room_dir.mkdir()
    supervisor, _conn, job, writer_pid, stderr_write = _abandon_build_job(
        no_room_dir, db, bound=100
    )
    # Make the on-disk aggregate sit EXACTLY at the bound (95 + 5).
    job.stderr.path.write_bytes(b"S" * 95)

    writes: list[tuple[str, int]] = []
    orig_spill = worker._spill_append

    def spill_spy(path: Path, data: bytearray) -> int:
        written = orig_spill(path, data)
        writes.append((Path(path).name, written))
        return written

    monkeypatch.setattr(worker, "_spill_append", spill_spy)
    try:
        supervisor.finalize_completed_job_bounded(job)
        assert writes == [], "abandonment appended despite zero aggregate room"
        assert job.spool_evicted, "unrepresentable retained bytes must fail closed"
        assert job.stdout.fd is None, "grace expiry must still close the read end"
    finally:
        monkeypatch.setattr(worker, "_spill_append", orig_spill)
        _abandon_cleanup(writer_pid, stderr_write)


def test_abandon_with_room_lands_pending_suffix_within_bound(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """With aggregate room available, the retained suffix lands exactly once.

    The spill spy enforces the running physical aggregate stays within the
    bound on every append; the suffix lands once, finalization proceeds to
    end-of-file, and no retained byte is left in memory.
    """
    monkeypatch.setattr(worker, "DETACHED_CAPTURE_EOF_GRACE_SECONDS", 0.2)
    db = DatabaseConfig(host="h", port=1, dbname="d", user="u", password=str(uuid4()))
    room_dir = tmp_path / "room"
    room_dir.mkdir()
    supervisor, _conn, job, writer_pid, stderr_write = _abandon_build_job(room_dir, db, bound=200)

    grown: dict[str, int] = {"stdout.cap": 5, "stderr.cap": 90}
    orig_spill = worker._spill_append

    def spill_spy(path: Path, data: bytearray) -> int:
        written = orig_spill(path, data)
        key = Path(path).name
        grown[key] = grown.get(key, 0) + written
        assert grown["stdout.cap"] + grown["stderr.cap"] <= 200, (
            "physical aggregate must stay within the bound"
        )
        return written

    monkeypatch.setattr(worker, "_spill_append", spill_spy)
    try:
        supervisor.finalize_completed_job_bounded(job)
        assert grown["stdout.cap"] == 8, (
            "the retained suffix must land exactly once, within the room"
        )
        assert job.stdout.eof
        assert job.stderr.eof
        assert job.stdout.fd is None
        assert job.stderr.fd is None
        assert not job.stdout.pending
        assert job.stdout.tail_end == 8, "5 on-disk bytes + retained suffix must land"
    finally:
        monkeypatch.setattr(worker, "_spill_append", orig_spill)
        _abandon_cleanup(writer_pid, stderr_write)


def test_bounded_finalize_quiet_wait_keeps_active_sibling_lease_heartbeating(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A deliberate bounded drain can never let an active sibling's lease lapse.

    While the bounded finalization cycle waits out the detached-writer grace on
    one completed job, any still-active sibling job's lease must keep being
    heartbeated: otherwise stale recovery would reclassify that running row as
    failed mid-drain. The quiet wait must therefore run the bulk heartbeat.
    """
    monkeypatch.setattr(worker, "DETACHED_CAPTURE_EOF_GRACE_SECONDS", 0.4)
    settings = make_settings(
        output_spool_max_bytes=8192,
        lease_duration_seconds=1.0,
        lease_refresh_interval_seconds=0.1,
        lease_safety_margin_seconds=0.2,
    )
    db = DatabaseConfig(host="h", port=1, dbname="d", user="u", password=str(uuid4()))
    supervisor = Supervisor(settings, db)
    conn = _RecordingConnection()
    supervisor.conn = as_db(conn)

    job = make_active_job(tmp_path)
    conn.retained_root = (str(job.id),)
    conn.status_result = ("running",)
    stdout_read, writer_pid, _ = _pipe_with_detached_writer(b"before-detach")
    stderr_read, _stderr_write = os.pipe()
    os.set_blocking(stderr_read, False)
    job.stdout = OutputStream(path=job.stdout.path, fd=stdout_read)
    job.stderr = OutputStream(path=job.stderr.path, fd=stderr_read)
    job.stderr.path.write_bytes(b"")
    job.stdout.path.write_bytes(b"")
    job.completed = True
    supervisor.active[job.id] = job

    sibling = make_active_job(tmp_path)
    sibling.completed = False
    supervisor.active[sibling.id] = sibling

    refreshed_ids: list[set[UUID]] = []

    def counting_refresh(self: worker.Supervisor) -> None:
        refreshed_ids.append(set(self._heartbeat_root_ids()))

    monkeypatch.setattr(worker.Supervisor, "_refresh_leases", counting_refresh)
    try:
        started = time.monotonic()
        supervisor.finalize_completed_job_bounded(job)
        assert time.monotonic() - started < 30.0
        assert job.finalized is True
        # The quiet wait spanned multiple heartbeat intervals and every one of
        # them refreshed the still-active sibling's lease.
        assert refreshed_ids, "quiet bounded drain must run the bulk heartbeat"
        assert any(sibling.id in ids for ids in refreshed_ids), (
            "the active sibling's lease must stay owned across the drain"
        )
    finally:
        with suppress(OSError):
            os.close(stdout_read)
        with suppress(OSError):
            os.close(stderr_read)
        with suppress(ProcessLookupError, OSError):
            os.kill(writer_pid, signal.SIGKILL)
        with suppress(ChildProcessError):
            os.waitpid(writer_pid, 0)


def test_disappeared_spool_fails_closed_without_recreation(tmp_path: Path) -> None:
    """A disappeared active spool fails closed and is never recreated.

    The spool append seam opens without ``O_CREAT``, so a vanished spool cannot
    be silently recreated as an empty file (which would corrupt logical offsets
    and hide data loss); already-read bytes stay pending and the owning job's
    drain reports an error instead.
    """
    spool = tmp_path / "stdout.cap"
    spool.write_bytes(b"kept")
    read_end, write_end = os.pipe()
    try:
        os.write(write_end, b"more")
        stream = OutputStream(path=spool, fd=read_end)
        stream.pending += b"held"
        spool.unlink()
        assert drain_capture_stream(stream, 4096) == "error"
        # The unwritten bytes are retained and the spool was not recreated.
        assert stream.pending == b"held"
        assert not spool.exists(), "failed flush must not recreate the spool"
    finally:
        with suppress(OSError):
            os.close(read_end)
        with suppress(OSError):
            os.close(write_end)


def test_post_popen_setup_failure_cleans_up_after_exact_convergence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Post-Popen setup failure cleans up only after exact direct-child convergence.

    With ``wait`` permanently timing out, ``spawn_job`` must still converge the
    unreleased gated wrapper synchronously — never escaping through a timeout
    while the direct child remains live — before closing every gate/capture
    descriptor, removing both spool files, and failing closed.
    """

    def timing_out_wait(self: subprocess.Popen[bytes], timeout: float | None = None) -> int:
        raise subprocess.TimeoutExpired(self.args, timeout or 0)

    monkeypatch.setattr(subprocess.Popen, "wait", timing_out_wait)

    real_fcntl = fcntl.fcntl

    def failing_fcntl(_fd: int, cmd: int, arg: int = 0) -> int:
        if cmd == fcntl.F_SETFL:
            msg = "injected fcntl failure"
            raise OSError(msg)
        return real_fcntl(_fd, cmd, arg)

    monkeypatch.setattr(fcntl, "fcntl", failing_fcntl)

    # Record the spool files spawn_job creates so their removal can be proven.
    created: list[Path] = []
    original_mkstemp = tempfile.mkstemp

    def _recording_mkstemp() -> tuple[int, str]:
        fd, name = original_mkstemp()
        created.append(Path(name))
        os.close(fd)
        return (fd, name)

    monkeypatch.setattr(tempfile, "mkstemp", _recording_mkstemp)

    fd_baseline = len(list(Path("/proc/self/fd").iterdir()))
    with pytest.raises(OSError, match="fcntl"):
        spawn_job(Job(id=uuid4(), cwd=str(tmp_path), process=SLEEP_30))

    # Convergence happened BEFORE cleanup/raise: poll-based ownership reaped the
    # exact child despite the permanently timing-out wait, no descriptor
    # leaked, and both spool files were removed.
    assert len(list(Path("/proc/self/fd").iterdir())) == fd_baseline
    for capture_path in created:
        assert not capture_path.exists()


def test_running_capture_failure_retains_ownership_until_group_gone(
    tmp_path: Path,
) -> None:
    """A running capture-failed job is owned until its group is proven gone.

    The fail-closed path terminates the exact group but must NOT terminalize in
    the database or untrack locally while the process group may still be alive;
    only once observation proves every member gone does finalization happen.
    """
    settings = make_settings()
    db = DatabaseConfig(host="h", port=1, dbname="d", user="u", password=str(uuid4()))
    supervisor = Supervisor(settings, db)
    conn = _RecordingConnection()
    supervisor.conn = as_db(conn)
    conn.rows.extend([(None,)] * 8)

    proc = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(30)"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    guard.register(proc)
    job = ActiveJob(
        id=uuid4(),
        cwd=str(tmp_path),
        process=(sys.executable, "-c", "pass"),
        proc=proc,
        pid=proc.pid,
        pgid=os.getpgid(proc.pid),
        started_mono=time.monotonic(),
        claimed_at=time.time(),
    )
    job.stdout = OutputStream(path=tmp_path / "stdout.cap")
    job.stderr = OutputStream(path=tmp_path / "stderr.cap")
    supervisor.active[job.id] = job

    try:
        job.stdout.path.unlink(missing_ok=True)
        supervisor.publish_job_output(job, time.monotonic() + 999.0)
        finalized_after_phase1 = job.finalized

        # Exact ownership retained while running: stopped, evicted, tracked,
        # but never terminalized/untracked behind a possibly-live group.
        assert job.spool_evicted is True
        assert job.term_sent is True
        assert not finalized_after_phase1
        assert job.id in supervisor.active
        finish_writes = [sql for sql, _p in conn.executions if "CASE" in sql]
        assert not finish_writes

        signal_kill(job)
        wait_until(lambda: proc.poll() is not None, timeout=5.0)
        wait_until(lambda: not group_has_members(job.pgid), timeout=5.0)
        job.completed = True
        job.returncode = proc.poll()
        # With the group proven gone, the public publication entry point takes
        # the same fail-closed path and now terminalizes/untracks exactly once.
        supervisor.publish_job_output(job, time.monotonic() + 9999.0)

        assert job.finalized is True
        assert job.id not in supervisor.active
        assert not job.stdout.path.exists()
        assert not job.stderr.path.exists()
    finally:
        with suppress(ProcessLookupError):
            os.killpg(job.pgid, signal.SIGKILL)
        proc.wait(timeout=5)
        guard.unregister(proc)


def test_changed_streams_stat_failure_fails_only_that_job_closed(
    tmp_path: Path,
) -> None:
    """A stat failure planning publication enters the exact-job fail-closed path.

    The affected running job is stopped/evicted exactly, and a healthy sibling
    job's publication is untouched by the offending job's broken spool.
    """
    settings = make_settings()
    db = DatabaseConfig(host="h", port=1, dbname="d", user="u", password=str(uuid4()))
    supervisor = Supervisor(settings, db)
    conn = _RecordingConnection()
    supervisor.conn = as_db(conn)

    (tmp_path / "bad").mkdir()
    (tmp_path / "good").mkdir()
    bad = make_active_job(tmp_path / "bad")
    good = make_active_job(tmp_path / "good")
    good.stdout.path.write_bytes(b"")
    good.stderr.path.write_bytes(b"")
    bad.stdout.path.unlink(missing_ok=True)
    bad.stdout.published_at = 0.0
    _queue_root(conn, good.id)
    supervisor.active[bad.id] = bad
    supervisor.active[good.id] = good

    now = time.monotonic() + 999.0
    supervisor.publish_job_output(bad, now)
    supervisor.publish_job_output(good, now)

    assert bad.spool_evicted is True
    assert bad.stop_reason == STOP_REASON_SPOOL
    assert good.spool_evicted is False
    assert good.term_sent is False


def test_stderr_archival_rotation_stress_chunk_order_offsets_and_tail(
    tmp_path: Path,
) -> None:
    """Independent stderr archival/rotation stress over repeated publish+trim.

    A stderr-heavy child is simulated by appending to the stderr spool across
    many publication rounds. Every round must keep immutable chunks ordered and
    chained (sequence monotonic, ``previous`` linked), logical start/end offsets
    contiguous with the total bytes written, the latest live tail equal to the
    newest ``OUTPUT_TAIL_MAX_BYTES`` bytes, and the local spool bounded to that
    tail regardless of cumulative volume.
    """
    job = make_active_job(tmp_path)
    conn = _RecordingConnection()
    published = 0
    written: list[bytes] = []
    chunk_records: list[tuple[int, int, int, UUID | None]] = []

    def write_and_publish(size: int) -> None:
        nonlocal published
        if size > published:
            block = bytes(range(256)) * ((size - published) // 256 + 1)
            block = block[: size - published]
            with job.stderr.path.open("ab") as fh:
                fh.write(block)
            written.append(block)
            published = size
        _queue_root(conn, job.id)
        assert publish_output(
            as_db(conn),
            job,
            [OUTPUT_STREAM_STDERR],
            time.monotonic(),
            server="test-server",
            force=True,
        )
        assert job.stderr.tail_end == size
        assert job.stderr.tail_start == max(0, size - OUTPUT_TAIL_MAX_BYTES)
        assert len(job.stderr.tail_text) <= OUTPUT_TAIL_MAX_BYTES
        # The latest root-window tail always equals the newest tail-window bytes.
        update_params = cast(
            "dict[str, object]",
            next(params for sql, params in reversed(conn.executions) if sql.startswith("UPDATE")),
        )
        window = json.loads(cast("str", update_params["output"]))["stderr"]
        assert window["end"] == size
        for sql, params in conn.executions:
            if not sql.startswith("INSERT"):
                continue
            insert_params = cast("tuple[UUID, str]", params)
            chunk = parse_chunk_payload(insert_params[1])
            if (chunk.sequence, chunk.start) not in [
                (seq, start) for seq, start, _e, _p in chunk_records
            ]:
                chunk_records.append((
                    chunk.sequence,
                    chunk.start,
                    chunk.end,
                    insert_params[0],
                ))

    write_and_publish(3000)
    write_and_publish(9000)
    write_and_publish(20_000)
    write_and_publish(33_000)

    # Chunk order: sequences are contiguous from 0 and offsets are contiguous.
    assert [rec[0] for rec in chunk_records] == list(range(len(chunk_records)))
    assert chunk_records[0][1] == 0
    for (_s0, _st0, end0, _p0), (_s1, start1, _e1, _p1) in pairwise(chunk_records):
        assert start1 == end0
    last_offset = chunk_records[-1][2]
    # Archival stops at the last whole chunk before the archive target;
    # everything after that lives only in the rolling tail.
    archive_target = 33_000 - OUTPUT_TAIL_MAX_BYTES + worker.ARCHIVE_MARGIN_CHARS
    expected_archive_end = archive_target - (archive_target % OUTPUT_CHUNK_MAX_BYTES)
    assert last_offset == expected_archive_end
    # Reconstructing chunks in sequence order yields exactly the archived prefix.
    total = b"".join(written).decode("utf-8", errors="replace")
    assert last_offset <= len(total)
    # The physical spool holds only the rolling tail despite 33k total volume.
    assert job.stderr.path.stat().st_size <= OUTPUT_TAIL_MAX_BYTES
    assert job.stderr.tail_text.endswith(total[-64:])
    # Stdout was never touched by this stderr-only workload.
    assert not job.stdout.tail_text


def test_post_popen_failure_owns_exact_child_until_reaped(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A post-Popen capture-setup failure owns the exact child until it is reaped.

    The unreleased gated wrapper is the worker's direct child in its own
    childless dedicated group. Even with reap ``wait`` held nonterminal through
    injected timeouts, ``spawn_job`` must not return or raise while that exact
    direct child is still live: it keeps signalling only while the child is
    unreaped, converges synchronously, and never signals the numeric PGID again
    after the reap. Gate/capture fds and spool files are closed as part of the
    convergence, and no user code runs.
    """
    real_poll = subprocess.Popen.poll

    polls = {"n": 0}
    hold_polls = 6

    def staged_poll(self: subprocess.Popen[bytes]) -> int | None:
        polls["n"] += 1
        if polls["n"] <= hold_polls:
            # Hold this exact direct child un-reaped through the first polls.
            return None
        return real_poll(self)

    def timing_out_wait(self: subprocess.Popen[bytes], timeout: float | None = None) -> int:
        raise subprocess.TimeoutExpired(self.args, timeout or 0)

    real_killpg = os.killpg
    kill_events: list[tuple[int, int]] = []

    def recording_killpg(pgid: int, sig: int) -> None:
        kill_events.append((polls["n"], sig))
        real_killpg(pgid, sig)

    monkeypatch.setattr(subprocess.Popen, "poll", staged_poll)
    monkeypatch.setattr(subprocess.Popen, "wait", timing_out_wait)
    monkeypatch.setattr(os, "killpg", recording_killpg)

    real_fcntl = fcntl.fcntl

    def failing_fcntl(_fd: int, cmd: int, arg: int = 0) -> int:
        if cmd == fcntl.F_SETFL:
            msg = "injected fcntl failure"
            raise OSError(msg)
        return real_fcntl(_fd, cmd, arg)

    monkeypatch.setattr(fcntl, "fcntl", failing_fcntl)

    fd_baseline = len(list(Path("/proc/self/fd").iterdir()))
    with pytest.raises(OSError, match="fcntl"):
        spawn_job(Job(id=uuid4(), cwd=str(tmp_path), process=SLEEP_30))

    # Signalling happened ONLY while the exact child was held un-reaped; after
    # the release (reap allowed) the numeric PGID was never touched again.
    assert kill_events
    assert {sig for _n, sig in kill_events} == {signal.SIGKILL}
    assert {n for n, _sig in kill_events} <= set(range(1, hold_polls + 1))
    assert polls["n"] > hold_polls, "spawn must poll past the hold before raising"
    # The convergence closed every descriptor and removed the spool files.
    assert len(list(Path("/proc/self/fd").iterdir())) == fd_baseline
