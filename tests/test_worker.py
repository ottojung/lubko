"""Tests for the Lubko worker supervisor and its database operations."""

import json
import os
import shutil
import signal
import subprocess
import sys
import time
from collections.abc import Callable
from contextlib import AbstractContextManager, nullcontext, suppress
from pathlib import Path
from typing import TYPE_CHECKING, Final, Self, cast, override
from uuid import uuid4

import psycopg
import pytest

from lubko import worker
from lubko.protocol import OUTPUT_TAIL_MAX_BYTES, parse_chunk_payload, parse_payload
from lubko.worker import (
    DEFAULT_LEASE_DURATION_SECONDS,
    DEFAULT_LEASE_RECOVERY_INTERVAL_SECONDS,
    DEFAULT_LEASE_REFRESH_INTERVAL_SECONDS,
    OUTPUT_STREAM_STDOUT,
    TRUNCATION_MARKER,
    ActiveJob,
    Job,
    JobResult,
    JobsConnection,
    OutputStream,
    Settings,
    bulk_refresh_leases,
    claim_job,
    claim_jobs,
    delete_job_and_chunks,
    discover_cancellations,
    finish_job,
    group_has_members,
    publish_output,
    read_output,
    read_range,
    recover_stale_jobs,
    request_cancel,
    request_group_reap,
    request_stop,
    resolve_shell,
    signal_kill,
    spawn_job,
    stream_size,
    truncate_output,
)
from tests import _process_guard as guard

if TYPE_CHECKING:
    from uuid import UUID

EXECUTION_ERROR_EXIT_CODE: Final = 127
COMMAND_FAILURE_EXIT_CODE: Final = 7
MIN_LEASE_HEARTBEATS: Final = 2


class _RecordingCursor:
    """A cursor that records every executed statement."""

    def __init__(self, conn: "_RecordingConnection") -> None:
        self._conn = conn

    def execute(self, sql: str, params: object | None = None) -> None:
        self._conn.executions.append((sql, params))

    def fetchone(self) -> object:
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


def make_settings(  # ruff: ignore[too-many-arguments]
    *,
    process_poll_interval_seconds: float = 0.02,
    cancel_grace_seconds: float = 1.0,
    lease_duration_seconds: float = DEFAULT_LEASE_DURATION_SECONDS,
    lease_refresh_interval_seconds: float = DEFAULT_LEASE_REFRESH_INTERVAL_SECONDS,
    lease_recovery_interval_seconds: float = DEFAULT_LEASE_RECOVERY_INTERVAL_SECONDS,
    output_publication_interval_seconds: float = 0.1,
    claim_batch_limit: int = 8,
    lease_safety_margin_seconds: float = 5.0,
) -> Settings:
    """Build worker settings for tests.

    Returns:
        Worker settings for tests.
    """
    return Settings(
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


def make_active_job(tmp_path: Path, *, command: str = "echo hi") -> ActiveJob:
    """Build an active job with capture files under ``tmp_path``.

    Args:
        tmp_path: Temporary directory for the capture files.
        command: Command recorded on the job.

    Returns:
        A registered active job with empty capture files.
    """
    proc = subprocess.Popen(
        ["/bin/true"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    guard.register(proc)
    job = ActiveJob(
        id=uuid4(),
        cwd=str(tmp_path),
        command=command,
        args=None,
        proc=proc,
        pid=proc.pid,
        pgid=proc.pid,
        started_mono=time.monotonic(),
    )
    job.stdout = OutputStream(path=tmp_path / "stdout.cap")
    job.stderr = OutputStream(path=tmp_path / "stderr.cap")
    return job


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


def test_resolve_shell_finds_bash() -> None:
    """resolve_shell locates an installed bash executable."""
    assert resolve_shell() == shutil.which("bash")
    assert resolve_shell() is not None


def test_group_has_members_tracks_process_group(tmp_path: Path) -> None:
    """The exact process group is reported while alive and gone after death."""
    shell = resolve_shell()
    assert shell is not None
    proc, _stdout_path, _stderr_path, pgid = spawn_job(
        Job(id=uuid4(), cwd=str(tmp_path), command="sleep 30", args=None), shell
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
    shell = resolve_shell()
    assert shell is not None
    proc, _stdout_path, _stderr_path, pgid = spawn_job(
        Job(id=uuid4(), cwd=str(tmp_path), command="sleep 30", args=None), shell
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


def test_spawn_job_runs_command_and_cleanup_files(tmp_path: Path) -> None:
    """A command job writes its output into the capture files."""
    shell = resolve_shell()
    assert shell is not None
    proc, stdout_path, stderr_path, _pgid = spawn_job(
        Job(id=uuid4(), cwd=str(tmp_path), command="echo hi", args=None), shell
    )
    guard.register(proc)
    try:
        proc.wait(timeout=10)
        assert read_output(stdout_path) == b"hi\n"
        assert stdout_path.is_file()
        assert stderr_path.is_file()
    finally:
        guard.unregister(proc)
        proc.wait(timeout=5)
        stdout_path.unlink(missing_ok=True)
        stderr_path.unlink(missing_ok=True)


def test_spawn_job_injects_exact_root_job_uuid(tmp_path: Path) -> None:
    """A shell command job inherits its exact root job UUID as LUBKO_JOB_ID."""
    shell = resolve_shell()
    assert shell is not None
    job_id = uuid4()
    proc, stdout_path, stderr_path, _pgid = spawn_job(
        Job(id=job_id, cwd=str(tmp_path), command='printf "%s" "$LUBKO_JOB_ID"', args=None),
        shell,
    )
    guard.register(proc)
    try:
        proc.wait(timeout=10)
        assert read_output(stdout_path) == str(job_id).encode()
    finally:
        guard.unregister(proc)
        proc.wait(timeout=5)
        stdout_path.unlink(missing_ok=True)
        stderr_path.unlink(missing_ok=True)


def test_spawn_job_injects_exact_root_job_uuid_into_args_environment(
    tmp_path: Path,
) -> None:
    """An argv job (direct exec) inherits its exact root job UUID as LUBKO_JOB_ID."""
    shell = resolve_shell()
    assert shell is not None
    job_id = uuid4()
    probe = "import os; print(os.environ['LUBKO_JOB_ID'])"
    proc, stdout_path, stderr_path, _pgid = spawn_job(
        Job(id=job_id, cwd=str(tmp_path), command=None, args=(sys.executable, "-c", probe)),
        shell,
    )
    guard.register(proc)
    try:
        proc.wait(timeout=10)
        assert read_output(stdout_path) == str(job_id).encode() + b"\n"
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
        "v": 2,
        "type": "command",
        "request": {"cwd": "/workspace", "command": "echo hi"},
        "state": {"status": "running"},
    })
    conn.rows = [(job_id, claimed_payload)]
    settings = make_settings()

    claimed = claim_job(as_db(conn), settings)

    assert claimed is not None
    assert claimed.id == job_id
    parsed = parse_payload(claimed.payload)
    assert parsed.request.command == "echo hi"
    assert parsed.status == "running"
    sql, params = conn.executions[0]
    assert "{state,status}" in sql
    assert "'running'" in sql
    assert "'pending'" in sql
    assert "FOR UPDATE SKIP LOCKED" in sql
    assert "(payload::jsonb)->>'type' = 'command'" in sql
    assert "payload::jsonb" in sql
    assert "::text" in sql
    assert "{state,worker_incarnation}" in sql
    assert "{state,lease_expires_at}" in sql
    assert "make_interval" in sql
    assert isinstance(params, dict)
    assert params["worker_id"] == settings.worker_id
    assert params["worker_incarnation"] == settings.worker_incarnation
    assert params["lease_duration_seconds"] == settings.lease_duration_seconds
    assert params["limit"] == 1


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

    status = request_cancel(as_db(conn), job_id)

    assert status == "cancelled"
    sql, params = conn.executions[0]
    assert "{state,status}" in sql
    assert "'cancelled'" in sql
    assert "payload::jsonb" in sql
    assert "::text" in sql
    assert "'pending'" in sql
    assert params == (job_id,)
    assert len(conn.executions) == 1


def test_request_cancel_marks_running_job() -> None:
    """A running job has its cancellation marker set for the worker to act on."""
    conn = _RecordingConnection()
    conn.rows = [None, ("running",)]
    job_id = uuid4()

    status = request_cancel(as_db(conn), job_id)

    assert status == "running"
    sql, params = conn.executions[1]
    assert "cancel_requested_at" in sql
    assert "'running'" in sql
    assert params == (job_id,)


def test_request_cancel_leaves_terminal_job_unchanged() -> None:
    """Cancelling an already terminal job is a harmless no-op."""
    conn = _RecordingConnection()
    conn.rows = [None, None, ("succeeded",)]
    job_id = uuid4()

    status = request_cancel(as_db(conn), job_id)

    assert status == "succeeded"
    updates = [sql for sql, _ in conn.executions if "UPDATE" in sql]
    assert len(updates) == 2


def test_bulk_refresh_leases_refreshes_owned_running_rows() -> None:
    """One statement refreshes every owned running command row."""
    conn = _RecordingConnection()
    job_ids = [uuid4(), uuid4()]
    conn.rows = [(job_ids[0],), (job_ids[1],)]
    settings = make_settings()

    refreshed = bulk_refresh_leases(as_db(conn), settings)

    assert refreshed == job_ids
    sql, params = conn.executions[0]
    assert "lease_expires_at" in sql
    assert "make_interval" in sql
    assert "(payload::jsonb)->>'type' = 'command'" in sql
    assert "status' = 'running'" in sql
    assert "worker_id" in sql
    assert "worker_incarnation" in sql
    assert isinstance(params, dict)
    assert params["lease_duration_seconds"] == settings.lease_duration_seconds
    assert params["worker_id"] == settings.worker_id


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
    recovered_payload = json.dumps({
        "v": 2,
        "type": "command",
        "request": {"cwd": "/workspace", "command": "sleep 30"},
        "state": {
            "status": "failed",
            "worker_id": "old-worker",
            "worker_incarnation": "old-incarnation",
            "lease_expires_at": "2020-01-01T00:00:00.000000Z",
        },
    })
    conn.rows = [(job_id, recovered_payload)]

    recovered = recover_stale_jobs(as_db(conn))

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
    assert params == {"limit": 100}


def test_recover_stale_jobs_returns_empty_when_none_stale() -> None:
    """An empty recovery scan returns no rows."""
    conn = _RecordingConnection()

    recovered = recover_stale_jobs(as_db(conn))

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

    status = finish_job(as_db(conn), job_id, result)

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

    status = finish_job(as_db(conn), job_id, result)

    assert status == "cancelled"
    sql, params = conn.executions[0]
    assert "CASE" in sql
    assert isinstance(params, dict)
    assert params["status"] == "cancelled"
    assert params["cancellation_note"] == "cancelled by request"


def test_delete_job_and_chunks_uses_explicit_ownership() -> None:
    """Cleanup deletes the root and every explicitly owned chunk by thread."""
    conn = _RecordingConnection()
    job_id = uuid4()

    delete_job_and_chunks(as_db(conn), job_id)

    sql, params = conn.executions[0]
    assert "output_chunk" in sql
    assert "thread" in sql
    assert isinstance(params, dict)
    assert params["job_id"] == job_id
    assert params["thread"] == str(job_id)


def test_publish_output_writes_bounded_tail_for_short_output(
    tmp_path: Path,
) -> None:
    """Short output produces a full tail and no immutable chunks."""
    job = make_active_job(tmp_path)
    job.stdout.path.write_bytes(b"hello world")
    conn = _RecordingConnection()

    publish_output(as_db(conn), job, [OUTPUT_STREAM_STDOUT], time.monotonic(), force=True)

    assert job.stdout.tail_text == "hello world"
    assert job.stdout.tail_start == 0
    assert job.stdout.tail_end == 11
    assert job.stdout.archived_upto == 0
    inserts = [sql for sql, _ in conn.executions if sql.startswith("INSERT")]
    updates = [(sql, params) for sql, params in conn.executions if "UPDATE" in sql]
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

    publish_output(as_db(conn), job, [OUTPUT_STREAM_STDOUT], time.monotonic(), force=True)

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
    updates = [(sql, params) for sql, params in conn.executions if "UPDATE" in sql]
    assert len(updates) == 1
    _, params = updates[0]
    assert isinstance(params, dict)
    window = json.loads(cast("str", params["output"]))["stdout"]
    assert window["previous"] == str(previous)


def test_publish_output_rotation_never_shortens_the_live_tail(
    tmp_path: Path,
) -> None:
    """Archival rotation never shortens the live tail across publications."""
    job = make_active_job(tmp_path)
    conn = _RecordingConnection()
    observed_lengths: list[int] = []

    def write_and_publish(size: int) -> None:
        job.stdout.path.write_bytes(b"y" * size)
        publish_output(as_db(conn), job, [OUTPUT_STREAM_STDOUT], time.monotonic(), force=True)
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


def test_publish_output_failed_transaction_does_not_advance_state(
    tmp_path: Path,
) -> None:
    """A failed transaction never advances in-memory publication state."""
    job = make_active_job(tmp_path)
    job.stdout.path.write_bytes(b"z" * 9000)
    failing = _FailingConnection(lambda sql: sql.startswith("UPDATE"))

    with pytest.raises(psycopg.Error):
        publish_output(as_db(failing), job, [OUTPUT_STREAM_STDOUT], time.monotonic(), force=True)

    assert job.stdout.published_size == 0
    assert not job.stdout.tail_text
    assert job.stdout.sequence == 0
    assert job.stdout.archived_upto == 0
    assert job.stdout.last_chunk is None

    conn = _RecordingConnection()
    publish_output(as_db(conn), job, [OUTPUT_STREAM_STDOUT], time.monotonic(), force=True)
    assert job.stdout.tail_end == 9000
    assert job.stdout.sequence == 3


def test_settings_reads_lease_and_output_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Lease, publication, and fairness settings come from the environment."""
    monkeypatch.setenv("LUBKO_LEASE_DURATION_SECONDS", "12.5")
    monkeypatch.setenv("LUBKO_LEASE_REFRESH_INTERVAL_SECONDS", "2.5")
    monkeypatch.setenv("LUBKO_LEASE_RECOVERY_INTERVAL_SECONDS", "4.5")
    monkeypatch.setenv("LUBKO_OUTPUT_PUBLICATION_INTERVAL_SECONDS", "0.7")
    monkeypatch.setenv("LUBKO_CLAIM_BATCH_LIMIT", "16")
    monkeypatch.setenv("LUBKO_LEASE_SAFETY_MARGIN_SECONDS", "3.5")
    monkeypatch.setenv("LUBKO_DB_OPERATION_TIMEOUT_SECONDS", "8.5")

    settings = Settings.from_environment()

    assert settings.lease_duration_seconds == pytest.approx(12.5)
    assert settings.lease_refresh_interval_seconds == pytest.approx(2.5)
    assert settings.lease_recovery_interval_seconds == pytest.approx(4.5)
    assert settings.output_publication_interval_seconds == pytest.approx(0.7)
    assert settings.claim_batch_limit == 16
    assert settings.lease_safety_margin_seconds == pytest.approx(3.5)
    assert settings.db_operation_timeout_seconds == pytest.approx(8.5)


def test_settings_defaults_and_unique_incarnation() -> None:
    """Settings default to the documented values and unique incarnations."""
    first = Settings.from_environment()
    second = Settings.from_environment()

    assert first.lease_duration_seconds == DEFAULT_LEASE_DURATION_SECONDS
    assert first.lease_refresh_interval_seconds == DEFAULT_LEASE_REFRESH_INTERVAL_SECONDS
    assert first.lease_recovery_interval_seconds == DEFAULT_LEASE_RECOVERY_INTERVAL_SECONDS
    assert first.worker_incarnation
    assert first.worker_incarnation != second.worker_incarnation


def test_settings_rejects_refresh_at_least_lease() -> None:
    """A refresh interval at or above the lease duration is refused."""
    with pytest.raises(ValueError, match="smaller than"):
        Settings(
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
            worker_id="w",
            poll_interval_seconds=1.0,
            process_poll_interval_seconds=0.1,
            cancel_grace_seconds=5.0,
            claim_batch_limit=0,
        )


def test_request_stop_sends_sigterm_to_exact_group(tmp_path: Path) -> None:
    """request_stop terminates the exact recorded process group once."""
    shell = resolve_shell()
    assert shell is not None
    proc, _stdout_path, _stderr_path, pgid = spawn_job(
        Job(id=uuid4(), cwd=str(tmp_path), command="sleep 30", args=None), shell
    )
    guard.register(proc)
    try:
        job = ActiveJob(
            id=uuid4(),
            cwd=str(tmp_path),
            command="sleep 30",
            args=None,
            proc=proc,
            pid=proc.pid,
            pgid=pgid,
            started_mono=time.monotonic(),
        )
        request_stop(job, "cancel")
        assert job.term_sent
        assert job.stop_started is not None
        proc.wait(timeout=10)
        guard.unregister(proc)
    finally:
        if proc.poll() is None:
            with suppress(ProcessLookupError):
                os.killpg(pgid, signal.SIGKILL)
            proc.wait(timeout=5)


def test_request_group_reap_preserves_natural_status(tmp_path: Path) -> None:
    """request_group_reap terminates the group without recording a stop reason."""
    shell = resolve_shell()
    assert shell is not None
    proc, _stdout_path, _stderr_path, pgid = spawn_job(
        Job(
            id=uuid4(),
            cwd=str(tmp_path),
            command="sleep 30 & echo done",
            args=None,
        ),
        shell,
    )
    guard.register(proc)
    try:
        proc.wait(timeout=10)
        assert group_has_members(pgid)
        job = ActiveJob(
            id=uuid4(),
            cwd=str(tmp_path),
            command="sleep 30 & echo done",
            args=None,
            proc=proc,
            pid=proc.pid,
            pgid=pgid,
            started_mono=time.monotonic(),
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
        ["/bin/sleep", "300"],
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
            command="sleep 300",
            args=None,
            proc=proc,
            pid=proc.pid,
            pgid=proc.pid,
            started_mono=time.monotonic(),
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
        ["/bin/sleep", "300"],
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
            command="sleep 300",
            args=None,
            proc=proc,
            pid=proc.pid,
            pgid=proc.pid,
            started_mono=time.monotonic(),
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
