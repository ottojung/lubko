"""Capture and publication need no persistent-filesystem capacity.

These invariants hold the capture path to anonymous memory only: spawning,
draining, trimming, and publication complete while every persistent-filesystem
mutation fails, buffer exhaustion fails exactly the offending job, and siblings
keep making progress. No path assumes any mount is tmpfs or memory-backed.
"""

from __future__ import annotations

import dataclasses
import errno
import fcntl
import json
import os
import subprocess
import tempfile
import time
from contextlib import contextmanager, suppress
from pathlib import Path
from typing import TYPE_CHECKING, Any, Self, cast
from uuid import uuid4

import pytest

from lubko import worker
from lubko.protocol import OUTPUT_CHUNK_MAX_BYTES, OUTPUT_TAIL_MAX_BYTES, PROTOCOL_VERSION
from lubko.worker import (
    EXECUTION_ERROR_EXIT_CODE,
    ActiveJob,
    Job,
    OutputStream,
    Settings,
    Supervisor,
    _spawn_result_from_tuple,
    _SpawnFuture,
    _StartAttempt,
)

if TYPE_CHECKING:
    from collections.abc import Iterator


def _exhausted(*_args: object, **_kwargs: object) -> None:
    """Simulate an exhausted persistent filesystem for any mutation seam.

    Raises:
        OSError: Always, with ``ENOSPC``.
    """
    raise OSError(errno.ENOSPC, "No space left on device")


def _block_persistent_filesystem(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fail every persistent-filesystem mutation while allowing pipes/memory."""
    monkeypatch.setattr(os, "open", _exhausted)
    monkeypatch.setattr(os, "mkdir", _exhausted)
    monkeypatch.setattr(os, "makedirs", _exhausted)
    monkeypatch.setattr(os, "replace", _exhausted)
    monkeypatch.setattr(os, "rename", _exhausted)
    monkeypatch.setattr(tempfile, "mkstemp", _exhausted)
    monkeypatch.setattr(tempfile, "mkdtemp", _exhausted)
    monkeypatch.setattr(Path, "open", _exhausted)
    monkeypatch.setattr(Path, "write_bytes", _exhausted)
    monkeypatch.setattr(Path, "mkdir", _exhausted)
    monkeypatch.setattr(Path, "unlink", _exhausted)
    monkeypatch.setattr(Path, "touch", _exhausted)


class _FakeCursor:
    """Record INSERT/UPDATE statements of one publication transaction."""

    def __init__(self, recorder: _Recorder) -> None:
        self._recorder = recorder

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, query: str, params: object = None) -> None:
        if query.lstrip().startswith("INSERT"):
            self._recorder.inserts.append(params)
        elif "UPDATE lubko.jobs" in query:
            self._recorder.updates.append(params)

    @staticmethod
    def fetchone() -> tuple[str] | None:
        return ("row",)


class _Recorder:
    """Collect the chunk inserts and root updates of publications."""

    def __init__(self) -> None:
        self.inserts: list[Any] = []
        self.updates: list[Any] = []


class _FakeConn:
    """Connection double that retains the root row for every publication."""

    def __init__(self, recorder: _Recorder) -> None:
        self._recorder = recorder

    @staticmethod
    @contextmanager
    def transaction() -> Iterator[None]:
        yield None

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self._recorder)


def _pipe_with(content: bytes) -> tuple[int, int]:
    """Create a pipe holding ``content`` with the write end closed (EOF after data).

    Returns:
        The read and (already closed) write file descriptors.
    """
    read_fd, write_fd = os.pipe()
    os.write(write_fd, content)
    os.close(write_fd)
    flags = fcntl.fcntl(read_fd, fcntl.F_GETFL)
    fcntl.fcntl(read_fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)
    return read_fd, write_fd


def _drain_fully(stream: OutputStream, bound: int) -> None:
    """Drain one stream to EOF within the bound.

    Raises:
        AssertionError: If the drain stalls or never reaches EOF.
    """
    for _ in range(10_000):
        status = worker.drain_capture_stream(stream, bound)
        if status == "eof":
            return
        assert status == "ok", f"drain stalled with status {status!r}"
    msg = "drain did not reach EOF within its bound"
    raise AssertionError(msg)


def _active_job() -> ActiveJob:
    """Build a structurally complete job entry with empty memory buffers.

    Returns:
        An active-job registry entry with no live child process.
    """
    job = object.__new__(ActiveJob)
    for f in dataclasses.fields(ActiveJob):
        if f.default is not dataclasses.MISSING:
            setattr(job, f.name, f.default)
        elif f.default_factory is not dataclasses.MISSING:
            setattr(job, f.name, f.default_factory())
    job.id = uuid4()
    job.cwd = "/var/empty"
    job.process = ("true",)
    job.pid = -1
    job.pgid = -1
    job.started_mono = 0.0
    job.claimed_at = 0.0
    job.version = PROTOCOL_VERSION
    job.stdout = OutputStream()
    job.stderr = OutputStream()
    return job


def _bare_supervisor() -> Supervisor:
    """Build a supervisor with real settings and no connection.

    Returns:
        An unstarted supervisor suitable for method-level tests.
    """
    supervisor = object.__new__(Supervisor)
    supervisor.settings = Settings.from_environment(server="srv")
    supervisor.active = {}
    supervisor.conn = None
    return supervisor


def test_capture_publish_trim_cycle_needs_no_filesystem(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Drain, chunk, publish, and trim complete with zero free filesystem blocks."""
    _block_persistent_filesystem(monkeypatch)
    bound = worker.DEFAULT_OUTPUT_SPOOL_MAX_BYTES
    content = b"o" * (OUTPUT_CHUNK_MAX_BYTES * 2 + OUTPUT_TAIL_MAX_BYTES + 500)
    job = _active_job()
    read_fd, _ = _pipe_with(content)
    job.stdout.fd = read_fd
    try:
        _drain_fully(job.stdout, bound)
    finally:
        with suppress(OSError):
            os.close(read_fd)
        job.stdout.fd = None
    assert bytes(job.stdout.data) == content

    recorder = _Recorder()
    conn = cast("worker.JobsConnection", _FakeConn(recorder))
    published = worker.publish_output(
        conn, job, ["stdout", "stderr"], time.monotonic(), server="srv", force=True
    )
    assert published
    assert recorder.updates, "the root live-tail update was not issued"
    assert recorder.inserts, "historical output was not archived into chunks"

    payloads = [json.loads(params[1]) for params in recorder.inserts]
    sequences = [parsed["sequence"] for parsed in payloads]
    assert sequences == list(range(len(payloads))), "chunks are not ordered"
    offset = 0
    for parsed in payloads:
        assert parsed["start"] == offset, "chunks are not contiguous"
        assert 0 < parsed["end"] - parsed["start"] <= OUTPUT_CHUNK_MAX_BYTES
        assert parsed["value"] == content[offset : parsed["end"]].decode()
        offset = parsed["end"]
    stdout_stream = job.stdout
    assert stdout_stream.archived_upto >= stdout_stream.tail_start, "a gap precedes the tail"
    assert content[stdout_stream.tail_start : stdout_stream.tail_end].decode() == (
        stdout_stream.tail_text
    )
    assert bytes(stdout_stream.data) == content[stdout_stream.spool_start :]
    assert stdout_stream.spool_start == stdout_stream.tail_start

    # A second publication with no new output issues no new chunks.
    second = _Recorder()
    conn2 = cast("worker.JobsConnection", _FakeConn(second))
    assert worker.publish_output(conn2, job, ["stdout", "stderr"], time.monotonic(), server="srv")
    assert not second.inserts


def test_buffer_allocation_failure_fails_only_the_offending_spawn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An exhausted capture buffer fails its own spawn without forking."""

    def fail_allocate() -> OutputStream:
        msg = "No space left on device"
        raise OSError(errno.ENOSPC, msg)

    monkeypatch.setattr(worker, "_new_output_stream", fail_allocate)

    def forbid_fork(*_args: object, **_kwargs: object) -> object:
        """Forbid forking when no capture buffer exists.

        Raises:
            AssertionError: Always; no child may be spawned without a buffer.
        """
        msg = "no child may be spawned without a capture buffer"
        raise AssertionError(msg)

    monkeypatch.setattr(subprocess, "Popen", forbid_fork)

    with pytest.raises(OSError, match="No space left on device") as caught:
        worker.spawn_job(Job(id=uuid4(), cwd="/var/empty", process=("true",)))
    assert caught.value.errno == errno.ENOSPC


def test_failed_spawn_finalizes_its_job_and_spares_its_sibling(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A spawn failure is a per-job terminal result; the worker keeps serving."""
    supervisor = _bare_supervisor()
    supervisor.conn = cast("worker.JobsConnection", object())
    sibling = _active_job()
    supervisor.active[sibling.id] = sibling
    finalized: list[tuple[Any, Any]] = []
    monkeypatch.setattr(
        supervisor, "_finalize_immediate", lambda jid, result: finalized.append((jid, result))
    )

    victim_id = uuid4()
    future = _SpawnFuture(callback=None)
    future.set_result(OSError(errno.ENOSPC, "No space left on device"))
    supervisor._pending_starts = {}
    attempt = _StartAttempt(
        job_id=victim_id,
        job_spec=Job(id=victim_id, cwd="/var/empty", process=("true",)),
        claim_mono=time.monotonic(),
        version=PROTOCOL_VERSION,
        submitted_at=time.monotonic(),
        future=future,
    )
    supervisor._pending_starts[victim_id] = attempt
    supervisor._handle_completed_attempt(attempt)

    assert len(finalized) == 1
    assert finalized[0][0] == victim_id
    assert finalized[0][1].status == "failed"
    assert finalized[0][1].exit_code == EXECUTION_ERROR_EXIT_CODE
    assert supervisor.active[sibling.id] is sibling


def test_buffer_append_failure_fails_exactly_one_job(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A buffer that cannot be extended fails its own job; its sibling drains."""
    supervisor = _bare_supervisor()
    victim = _active_job()
    sibling = _active_job()
    victim_read, _ = _pipe_with(b"victim-output")
    sibling_read, _ = _pipe_with(b"sibling-output")
    victim.stdout.fd = victim_read
    sibling.stdout.fd = sibling_read
    supervisor.active[victim.id] = victim
    supervisor.active[sibling.id] = sibling

    real_append = worker._spool_append

    def fail_victim(stream: OutputStream, data: bytearray) -> int:
        if stream is victim.stdout:
            raise OSError(errno.ENOSPC, "No space left on device")
        return real_append(stream, data)

    monkeypatch.setattr(worker, "_spool_append", fail_victim)
    stops: list[tuple[ActiveJob, str]] = []
    monkeypatch.setattr(worker, "request_stop", lambda job, reason: stops.append((job, reason)))

    try:
        supervisor._drain_captures()
    finally:
        with suppress(OSError):
            os.close(victim_read)
        with suppress(OSError):
            os.close(sibling_read)

    assert victim.spool_evicted
    assert (victim, worker.STOP_REASON_SPOOL) in [(job, reason) for job, reason in stops]
    assert not sibling.spool_evicted
    assert bytes(sibling.stdout.data) == b"sibling-output"
    assert not any(job is sibling for job, _reason in stops)


def test_full_spool_backpressures_instead_of_reading(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A spool at its bound withholds reads until publication frees room."""
    _block_persistent_filesystem(monkeypatch)
    bound = 16
    stream = OutputStream(data=bytearray(b"0" * bound))
    read_fd, write_fd = os.pipe()
    try:
        os.write(write_fd, b"producer-data")
        stream.fd = read_fd
        assert worker.drain_capture_stream(stream, bound) == "full"
        assert bytes(stream.data) == b"0" * bound
        assert not stream.pending

        # Publish+trim frees bounded room without any filesystem use.
        stream.spool_start = 0
        worker._drop_head(stream, 8)
        stream.spool_start = 8
        assert worker.drain_capture_stream(stream, bound) == "ok"
        assert len(stream.data) <= bound
    finally:
        os.close(read_fd)
        os.close(write_fd)


def test_failed_spawn_closes_every_pipe(monkeypatch: pytest.MonkeyPatch) -> None:
    """A spawn that fails after piping leaks no file descriptors."""
    before = sum(1 for _entry in Path("/proc/self/fd").iterdir())

    def fail_exec(*_args: object, **_kwargs: object) -> object:
        """Simulate an exec failure after the capture pipes were created.

        Raises:
            OSError: Always, with ``ENOENT``.
        """
        msg = "No such file or directory"
        raise OSError(errno.ENOENT, msg)

    monkeypatch.setattr(subprocess, "Popen", fail_exec)
    with pytest.raises(OSError, match="No such file or directory"):
        worker.spawn_job(Job(id=uuid4(), cwd="/var/empty", process=("true",)))
    assert sum(1 for _entry in Path("/proc/self/fd").iterdir()) == before


def test_spawn_result_carries_memory_buffers() -> None:
    """Spawn results hand memory buffers (not paths) to activation."""
    proc = cast("Any", object())
    stdout = OutputStream(data=bytearray(b"out"))
    stderr = OutputStream(data=bytearray(b"err"))
    result = _spawn_result_from_tuple((proc, stdout, stderr, 7, 8, 9, 10))
    assert result.stdout is stdout
    assert result.stderr is stderr
    assert result.pgid == 7
