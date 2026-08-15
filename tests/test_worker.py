"""Tests for the Lubko worker."""

import json
import os
import shutil
import signal
import subprocess
import threading
import time
from collections.abc import Callable
from contextlib import AbstractContextManager, nullcontext, suppress
from pathlib import Path
from typing import Final, Self, cast
from uuid import UUID, uuid4

import pytest

from lubko.protocol import parse_payload
from lubko.worker import (
    DEFAULT_LEASE_DURATION_SECONDS,
    DEFAULT_LEASE_RECOVERY_INTERVAL_SECONDS,
    DEFAULT_LEASE_REFRESH_INTERVAL_SECONDS,
    PROTOCOL_ERROR_EXIT_CODE,
    TRUNCATION_MARKER,
    Job,
    JobResult,
    JobsConnection,
    Settings,
    claim_and_process_one,
    claim_job,
    finish_job,
    group_has_members,
    process_jobs,
    read_output,
    recover_stale_jobs,
    request_cancel,
    resolve_shell,
    run_job,
    spawn_job,
    truncate_output,
)
from tests import _process_guard as guard

EXECUTION_ERROR_EXIT_CODE: Final = 127
COMMAND_FAILURE_EXIT_CODE: Final = 7
CANCEL_UPDATE_STATEMENTS: Final = 2
CLAIM_AND_FINISH_UPDATE_STATEMENTS: Final = 2
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


def as_db(conn: _RecordingConnection) -> JobsConnection:
    """Adapt the recording test double to the worker's connection type.

    Args:
        conn: Recording test double.

    Returns:
        The same object typed as a psycopg connection.
    """
    return cast("JobsConnection", conn)


def make_settings(
    *,
    process_poll_interval_seconds: float = 0.02,
    cancel_grace_seconds: float = 1.0,
    lease_duration_seconds: float = DEFAULT_LEASE_DURATION_SECONDS,
    lease_refresh_interval_seconds: float = DEFAULT_LEASE_REFRESH_INTERVAL_SECONDS,
    lease_recovery_interval_seconds: float = DEFAULT_LEASE_RECOVERY_INTERVAL_SECONDS,
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
        max_output_bytes=256 * 1024,
        lease_duration_seconds=lease_duration_seconds,
        lease_refresh_interval_seconds=lease_refresh_interval_seconds,
        lease_recovery_interval_seconds=lease_recovery_interval_seconds,
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


def pid_alive(pid: int) -> bool:
    """Return whether a process is alive, ignoring unreaped zombies.

    Args:
        pid: Process ID to probe.

    Returns:
        ``True`` when a running process with that ID exists.
    """
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    try:
        content = Path(f"/proc/{pid}/stat").read_bytes()
    except OSError:
        return True
    close_paren = content.rfind(b")")
    if close_paren == -1:
        return True
    fields = content[close_paren + 2 :].split()
    if not fields:
        return True
    return fields[0] != b"Z"


def install_cancel_harness(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[threading.Event, threading.Event, list[tuple[UUID, int, int]]]:
    """Patch the worker's database hooks for cancellation tests.

    Args:
        monkeypatch: Pytest monkeypatch fixture.

    Returns:
        An event set once the process identity is recorded, an event that
        toggles the observed cancellation request, and the recorded
        (job id, pid, pgid) tuples.
    """
    persisted_event = threading.Event()
    cancel_event = threading.Event()
    persisted: list[tuple[UUID, int, int]] = []

    def fake_persist(_conn: object, job_id: UUID, pid: int, pgid: int) -> None:
        persisted.append((job_id, pid, pgid))
        persisted_event.set()

    def fake_cancel_check(_conn: object, _job_id: UUID) -> bool:
        return cancel_event.is_set()

    monkeypatch.setattr("lubko.worker._persist_process", fake_persist)
    monkeypatch.setattr("lubko.worker._is_cancel_requested", fake_cancel_check)
    return persisted_event, cancel_event, persisted


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


def test_resolve_shell_finds_bash() -> None:
    """resolve_shell locates an installed bash executable."""
    assert resolve_shell() == shutil.which("bash")
    assert resolve_shell() is not None


def test_group_has_members_tracks_process_group(tmp_path: Path) -> None:
    """The exact process group is reported while alive and gone after death."""
    shell = resolve_shell()
    assert shell is not None
    run = spawn_job(Job(id=uuid4(), cwd=str(tmp_path), command="sleep 30", args=None), shell)
    guard.register(run.proc)
    try:
        assert group_has_members(run.pgid)
    finally:
        with suppress(ProcessLookupError):
            os.killpg(run.pgid, signal.SIGKILL)
        run.proc.wait(timeout=10)
        guard.unregister(run.proc)
    wait_until(lambda: not group_has_members(run.pgid))


def test_spawn_job_makes_session_and_process_group_leader(tmp_path: Path) -> None:
    """A spawned job is a session leader whose group ID equals its PID."""
    shell = resolve_shell()
    assert shell is not None
    run = spawn_job(Job(id=uuid4(), cwd=str(tmp_path), command="sleep 30", args=None), shell)
    guard.register(run.proc)
    try:
        assert run.pgid == run.pid
        assert os.getpgid(run.pid) == run.pid
        assert os.getsid(run.pid) == run.pid
    finally:
        with suppress(ProcessLookupError):
            os.killpg(run.pgid, signal.SIGKILL)
        run.proc.wait(timeout=10)
        guard.unregister(run.proc)


def test_run_job_runs_directly_without_docker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A job executes directly without any Docker executable or lookup."""
    original_which = shutil.which
    monkeypatch.setattr(
        shutil,
        "which",
        lambda name: original_which(name) if name != "docker" else None,
    )

    job = Job(id=uuid4(), cwd=str(tmp_path), command="echo direct", args=None)
    result = run_job(as_db(_RecordingConnection()), job, make_settings())

    assert result.status == "succeeded"
    assert result.exit_code == 0
    assert result.stdout.strip() == "direct"
    assert not result.stderr


def test_run_job_honors_cwd(tmp_path: Path) -> None:
    """A job runs from the requested working directory."""
    job = Job(id=uuid4(), cwd=str(tmp_path), command="pwd", args=None)

    result = run_job(as_db(_RecordingConnection()), job, make_settings())

    assert result.exit_code == 0
    assert result.stdout.strip() == os.path.realpath(str(tmp_path))
    assert not result.stderr


def test_run_job_success(tmp_path: Path) -> None:
    """A successful job reports a zero exit code and its stdout."""
    job = Job(id=uuid4(), cwd=str(tmp_path), command="echo hello world", args=None)

    result = run_job(as_db(_RecordingConnection()), job, make_settings())

    assert result.exit_code == 0
    assert result.stdout.strip() == "hello world"
    assert not result.stderr


def test_run_job_executes_args_directly(tmp_path: Path) -> None:
    """An argv-style job runs directly without a shell."""
    args = [shutil.which("pwd") or "pwd"]
    job = Job(id=uuid4(), cwd=str(tmp_path), command=None, args=tuple(args))

    result = run_job(as_db(_RecordingConnection()), job, make_settings())

    assert result.status == "succeeded"
    assert result.exit_code == 0
    assert result.stdout.strip() == os.path.realpath(str(tmp_path))
    assert not result.stderr


def test_run_job_rejects_request_without_command_or_args(tmp_path: Path) -> None:
    """A job request with neither command nor args fails with a clear error."""
    job = Job(id=uuid4(), cwd=str(tmp_path), command=None, args=None)

    result = run_job(as_db(_RecordingConnection()), job, make_settings())

    assert result.status == "failed"
    assert result.exit_code == EXECUTION_ERROR_EXIT_CODE
    assert "neither command nor args" in result.stderr


def test_run_job_reports_command_failure(tmp_path: Path) -> None:
    """A failing command preserves its exit code and output."""
    job = Job(id=uuid4(), cwd=str(tmp_path), command="echo oops >&2; exit 7", args=None)

    result = run_job(as_db(_RecordingConnection()), job, make_settings())

    assert result.status == "failed"
    assert result.exit_code == COMMAND_FAILURE_EXIT_CODE
    assert not result.stdout
    assert "oops" in result.stderr


def test_run_job_reports_missing_cwd(tmp_path: Path) -> None:
    """A missing working directory produces a useful error."""
    missing = tmp_path / "missing"
    job = Job(id=uuid4(), cwd=str(missing), command="echo hi", args=None)

    result = run_job(as_db(_RecordingConnection()), job, make_settings())

    assert result.status == "failed"
    assert result.exit_code == EXECUTION_ERROR_EXIT_CODE
    assert not result.stdout
    assert "working directory" in result.stderr


def test_run_job_reports_non_directory_cwd(tmp_path: Path) -> None:
    """A working directory that is a regular file produces a useful error."""
    target = tmp_path / "file"
    target.write_text("not a directory")
    job = Job(id=uuid4(), cwd=str(target), command="echo hi", args=None)

    result = run_job(as_db(_RecordingConnection()), job, make_settings())

    assert result.status == "failed"
    assert result.exit_code == EXECUTION_ERROR_EXIT_CODE
    assert not result.stdout
    assert "working directory" in result.stderr


def test_run_job_reports_missing_shell(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A missing shell executable produces a useful error."""
    original_which = shutil.which
    monkeypatch.setattr(
        shutil,
        "which",
        lambda name: original_which(name) if name != "bash" else None,
    )

    job = Job(id=uuid4(), cwd=str(tmp_path), command="echo hi", args=None)

    result = run_job(as_db(_RecordingConnection()), job, make_settings())

    assert result.status == "failed"
    assert result.exit_code == EXECUTION_ERROR_EXIT_CODE
    assert not result.stdout
    assert "shell" in result.stderr


def test_run_job_persists_process_identity(tmp_path: Path) -> None:
    """The worker records the exact PID and PGID of the spawned shell."""
    conn = _RecordingConnection()
    job = Job(id=uuid4(), cwd=str(tmp_path), command="echo hi", args=None)

    run_job(as_db(conn), job, make_settings())

    persist_sqls = [(sql, params) for sql, params in conn.executions if "process_pgid" in sql]
    assert len(persist_sqls) == 1
    sql, params = persist_sqls[0]
    assert "{state,process_pid}" in sql
    assert "{state,process_pgid}" in sql
    assert "payload::jsonb" in sql
    assert "::text" in sql
    assert isinstance(params, tuple)
    pid, pgid, recorded_job_id = params
    assert recorded_job_id == job.id
    assert pid == pgid


def test_cancellation_kills_shell_and_spawned_child(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cancellation terminates the shell and its spawned child process."""
    child_pid_path = tmp_path / "child.pid"
    command = f"sleep 30 & child=$!; echo $child > {child_pid_path}; wait"
    job = Job(id=uuid4(), cwd=str(tmp_path), command=command, args=None)
    persisted_event, cancel_event, persisted = install_cancel_harness(monkeypatch)
    result_box: list[JobResult] = []

    def run_in_thread() -> None:
        result_box.append(run_job(as_db(_RecordingConnection()), job, make_settings()))

    thread = threading.Thread(target=run_in_thread)
    thread.start()
    try:
        assert persisted_event.wait(timeout=10)
        recorded_job_id, pid, pgid = persisted[0]
        assert recorded_job_id == job.id
        assert pgid == pid

        wait_until(child_pid_path.exists)
        child_pid = int(child_pid_path.read_text().strip())

        cancel_event.set()
        thread.join(timeout=15)
        assert not thread.is_alive()

        result = result_box[0]
        assert result.status == "cancelled"
        assert result.exit_code < 0
        assert "SIGTERM" in (result.cancellation_note or "")

        wait_until(lambda: not pid_alive(pid))
        wait_until(lambda: not pid_alive(child_pid))
    finally:
        cancel_event.set()
        thread.join(timeout=5)


def test_cancellation_leaves_unrelated_process_group_untouched(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cancellation never signals processes outside the tracked process group."""
    sleep = shutil.which("sleep")
    assert sleep is not None
    unrelated = subprocess.Popen([sleep, "30"], start_new_session=True)
    guard.register(unrelated)
    job = Job(id=uuid4(), cwd=str(tmp_path), command="sleep 30", args=None)
    persisted_event, cancel_event, _ = install_cancel_harness(monkeypatch)
    result_box: list[JobResult] = []

    def run_in_thread() -> None:
        result_box.append(run_job(as_db(_RecordingConnection()), job, make_settings()))

    thread = threading.Thread(target=run_in_thread)
    thread.start()
    try:
        assert persisted_event.wait(timeout=10)
        cancel_event.set()
        thread.join(timeout=15)
        assert not thread.is_alive()

        result = result_box[0]
        assert result.status == "cancelled"
        assert unrelated.poll() is None
    finally:
        cancel_event.set()
        thread.join(timeout=5)
        unrelated.kill()
        unrelated.wait(timeout=10)
        guard.unregister(unrelated)


def test_cancellation_sigkills_term_ignoring_shell(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A shell that ignores SIGTERM is force-killed after the grace period."""
    ready_path = tmp_path / "ready"
    command = f"trap '' TERM; echo ready > {ready_path}; while true; do sleep 1; done"
    job = Job(id=uuid4(), cwd=str(tmp_path), command=command, args=None)
    persisted_event, cancel_event, persisted = install_cancel_harness(monkeypatch)
    settings = make_settings(process_poll_interval_seconds=0.02, cancel_grace_seconds=0.3)
    result_box: list[JobResult] = []

    def run_in_thread() -> None:
        result_box.append(run_job(as_db(_RecordingConnection()), job, settings))

    thread = threading.Thread(target=run_in_thread)
    thread.start()
    try:
        assert persisted_event.wait(timeout=10)
        pid = persisted[0][1]
        wait_until(ready_path.exists)
        cancel_event.set()
        thread.join(timeout=20)
        assert not thread.is_alive()

        result = result_box[0]
        assert result.status == "cancelled"
        assert result.exit_code == -signal.SIGKILL
        assert "SIGKILL" in (result.cancellation_note or "")
        wait_until(lambda: not pid_alive(pid))
    finally:
        cancel_event.set()
        thread.join(timeout=5)


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
    assert "payload::jsonb" in sql
    assert params == (job_id,)


def test_request_cancel_leaves_terminal_job_unchanged() -> None:
    """Cancelling an already terminal job is a harmless no-op."""
    conn = _RecordingConnection()
    conn.rows = [None, None, ("succeeded",)]
    job_id = uuid4()

    status = request_cancel(as_db(conn), job_id)

    assert status == "succeeded"
    updates = [sql for sql, _ in conn.executions if "UPDATE" in sql]
    assert len(updates) == CANCEL_UPDATE_STATEMENTS
    assert "'pending'" in updates[0]
    assert "'running'" in updates[1]


def test_claim_job_marks_job_running() -> None:
    """claim_job marks the job running and returns it for execution."""
    conn = _RecordingConnection()
    job_id = uuid4()
    claimed_payload = json.dumps({
        "v": 1,
        "type": "command",
        "request": {"cwd": "/workspace", "command": "echo hi"},
        "state": {"status": "running"},
    })
    conn.rows = [(job_id, claimed_payload)]
    settings = make_settings()

    claimed = claim_job(as_db(conn), settings)

    assert claimed is not None
    assert claimed.id == job_id
    assert isinstance(claimed.payload, str)
    parsed = parse_payload(claimed.payload)
    assert parsed.request.cwd == "/workspace"
    assert parsed.request.command == "echo hi"
    assert parsed.status == "running"
    sql, params = conn.executions[0]
    assert "{state,status}" in sql
    assert "'running'" in sql
    assert "'pending'" in sql
    assert "FOR UPDATE SKIP LOCKED" in sql
    assert "payload::jsonb" in sql
    assert "::text" in sql
    assert "{state,worker_incarnation}" in sql
    assert "{state,lease_expires_at}" in sql
    assert "make_interval" in sql
    assert params == (
        settings.worker_id,
        settings.worker_incarnation,
        settings.lease_duration_seconds,
    )


def test_recover_stale_jobs_marks_failed_with_diagnostic() -> None:
    """recover_stale_jobs atomically fails expired-lease running jobs."""
    conn = _RecordingConnection()
    job_id = uuid4()
    recovered_payload = json.dumps({
        "v": 1,
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
    assert "'running'" in sql
    assert "lease_expires_at" in sql
    assert "FOR UPDATE SKIP LOCKED" in sql
    assert "'failed'" in sql
    assert "{state,recovered_at}" in sql
    assert "recovery_note" in sql
    assert "payload::jsonb" in sql
    assert "::text" in sql
    assert params == {"limit": 100}


def test_recover_stale_jobs_returns_empty_when_none_stale() -> None:
    """An empty recovery scan returns no rows."""
    conn = _RecordingConnection()

    recovered = recover_stale_jobs(as_db(conn))

    assert recovered == []
    assert len(conn.executions) == 1


def test_process_jobs_runs_recovery_pass(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The processing loop runs a recovery pass before claiming."""
    conn = _RecordingConnection()
    calls: list[str] = []

    def recording_recover(_conn: object) -> list[object]:
        calls.append("recover")
        return []

    def stop(_conn: object, _settings: Settings) -> bool:
        calls.append("claim")
        raise SystemExit(0)

    monkeypatch.setattr("lubko.worker.recover_stale_jobs", recording_recover)
    monkeypatch.setattr("lubko.worker.claim_and_process_one", stop)

    with pytest.raises(SystemExit):
        process_jobs(as_db(conn), make_settings(lease_recovery_interval_seconds=1.0))

    assert calls == ["recover", "claim"]


def test_settings_reads_lease_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The lease timing is configurable through environment variables."""
    monkeypatch.setenv("LUBKO_LEASE_DURATION_SECONDS", "12.5")
    monkeypatch.setenv("LUBKO_LEASE_REFRESH_INTERVAL_SECONDS", "2.5")
    monkeypatch.setenv("LUBKO_LEASE_RECOVERY_INTERVAL_SECONDS", "4.5")

    settings = Settings.from_environment()

    assert settings.lease_duration_seconds == pytest.approx(12.5)
    assert settings.lease_refresh_interval_seconds == pytest.approx(2.5)
    assert settings.lease_recovery_interval_seconds == pytest.approx(4.5)


def test_settings_defaults_and_unique_incarnation() -> None:
    """Settings default to the documented lease values and unique incarnations."""
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
            max_output_bytes=1024,
            lease_duration_seconds=5.0,
            lease_refresh_interval_seconds=5.0,
            lease_recovery_interval_seconds=1.0,
        )


def test_running_job_refreshes_its_lease(tmp_path: Path) -> None:
    """A running job heartbeats its lease until it finishes."""
    conn = _RecordingConnection()
    job = Job(id=uuid4(), cwd=str(tmp_path), command="sleep 0.5", args=None)
    settings = make_settings(
        process_poll_interval_seconds=0.02,
        lease_refresh_interval_seconds=0.05,
    )

    result = run_job(as_db(conn), job, settings)

    assert result.status == "succeeded"
    lease_updates = [(sql, params) for sql, params in conn.executions if "lease_expires_at" in sql]
    assert len(lease_updates) >= MIN_LEASE_HEARTBEATS
    for sql, params in lease_updates:
        assert "make_interval" in sql
        assert params == (settings.lease_duration_seconds, job.id)


def test_claim_and_process_one_fails_unparseable_payload() -> None:
    """A claimed job with an invalid payload is failed without executing it."""
    conn = _RecordingConnection()
    job_id = uuid4()
    conn.rows = [(job_id, "{not json"), ("failed",)]

    processed = claim_and_process_one(as_db(conn), make_settings())

    assert processed is True
    updates = [(sql, params) for sql, params in conn.executions if "UPDATE" in sql]
    assert len(updates) == CLAIM_AND_FINISH_UPDATE_STATEMENTS
    finish_params = updates[1][1]
    assert isinstance(finish_params, dict)
    assert finish_params["status"] == "failed"
    assert finish_params["exit_code"] == PROTOCOL_ERROR_EXIT_CODE
    assert "invalid job payload" in finish_params["stderr"]


def test_claim_and_process_one_returns_false_when_queue_empty() -> None:
    """An empty queue is reported without any writes past the claim query."""
    conn = _RecordingConnection()

    processed = claim_and_process_one(as_db(conn), make_settings())

    assert processed is False
    assert len(conn.executions) == 1
    assert "RETURNING job.id" in conn.executions[0][0]


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
    assert "payload::jsonb" in sql
    assert "::text" in sql
    assert isinstance(params, dict)
    assert params["status"] == "succeeded"
    assert params["stdout"] == "hi\n"
    assert not params["stderr"]
    assert params["exit_code"] == 0
    assert params["cancellation_note"] is None


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
        cancellation_note="cancelled: sent SIGTERM to process group",
    )

    status = finish_job(as_db(conn), job_id, result)

    assert status == "cancelled"
    sql, params = conn.executions[0]
    assert "CASE" in sql
    assert "jsonb_build_object" in sql
    assert "payload::jsonb" in sql
    assert "::text" in sql
    assert isinstance(params, dict)
    assert params["status"] == "cancelled"
    assert params["exit_code"] == -signal.SIGTERM
    assert params["cancellation_note"] == "cancelled: sent SIGTERM to process group"


def test_finish_job_builds_result_atomically_as_json_object() -> None:
    """finish_job assembles the whole result in one jsonb_build_object.

    A per-field jsonb_set path ending in ``to_jsonb(NULL)`` would make the
    whole update SQL NULL and violate the payload NOT NULL constraint. The
    result parent must also be created when absent, so only the ``{result}``
    path is written.
    """
    conn = _RecordingConnection()
    conn.rows = [("succeeded",)]
    job_id = uuid4()
    result = JobResult(
        status="succeeded",
        exit_code=0,
        stdout="out\n",
        stderr="err\n",
        cancellation_note=None,
    )

    finish_job(as_db(conn), job_id, result)

    result_path = "'{" + "result" + "}'"
    field_paths = [
        "'{" + "result,stdout" + "}'",
        "'{" + "result,stderr" + "}'",
        "'{" + "result,exit_code" + "}'",
        "'{" + "result,cancellation_note" + "}'",
    ]
    sql, params = conn.executions[0]
    assert "jsonb_build_object" in sql
    assert result_path in sql
    for path in field_paths:
        assert path not in sql
    assert "::text" in sql
    assert isinstance(params, dict)
    assert params["cancellation_note"] is None
