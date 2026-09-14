"""Deterministic invariants for asynchronous non-blocking spawn attempts.

The worker supervisor must never synchronously call ``spawn_job``/``subprocess.Popen``
in the tick loop because that seam can block forever.  Each spawn attempt runs in
a bounded daemon pool; the main loop polls completed futures each tick, enforces a
bounded start deadline, and fails the DB row on timeout.  One permanently blocked
start must never consume the only start lane: later queue work must still progress.
"""

from __future__ import annotations

import queue
import threading
import time
from typing import TYPE_CHECKING, Any, cast, override
from unittest.mock import MagicMock
from uuid import uuid4

from lubko.config import DatabaseConfig
from lubko.worker import (
    ActiveJob,
    OutputStream,
    Settings,
    Supervisor,
    _spawn_result_from_tuple,
    _SpawnExecutor,
    _SpawnFuture,
    _SpawnResult,
    _StartAttempt,
)

if TYPE_CHECKING:
    from pathlib import Path
    from uuid import UUID

    import pytest

    from lubko.worker import JobsConnection, _SpawnTuple

_ANON_DIR = "/var/empty"


class _FakeConn:
    """Fake connection that supports the operation_deadline attribute."""

    operation_deadline: float = 0.0


def _settings(**kwargs: object) -> Settings:
    """Build a valid baseline Settings with optional overrides.

    Returns:
        Configured Settings instance.
    """
    defaults: dict[str, object] = {
        "worker_id": "worker",
        "server": "server",
        "poll_interval_seconds": 0.1,
        "process_poll_interval_seconds": 0.1,
        "cancel_grace_seconds": 1.0,
    }
    defaults.update(kwargs)
    return Settings(**defaults)  # type: ignore[arg-type]


def _supervisor(settings: Settings | None = None) -> Supervisor:
    """Build a Supervisor with a fake connection.

    Returns:
        A Supervisor instance.
    """
    s = Supervisor(
        settings or _settings(),
        DatabaseConfig(host="host", port=5432, dbname="db", user="user", password=""),
    )
    s.conn = cast("JobsConnection", _FakeConn())
    return s


def _active_job(job_id: UUID | None = None) -> ActiveJob:
    """Build a minimal ActiveJob for testing.

    Returns:
        An ActiveJob with fake process handles.
    """
    if job_id is None:
        job_id = uuid4()
    proc = MagicMock()
    proc.poll.return_value = None
    proc.pid = 42000
    job = ActiveJob(
        id=job_id,
        cwd=_ANON_DIR,
        process=("/bin/true",),
        proc=proc,
        pid=42000,
        pgid=42000,
        started_mono=time.monotonic(),
        claimed_at=time.monotonic(),
        version=1,
    )
    job.stdout = OutputStream(path=MagicMock(), fd=None, eof=True)
    job.stderr = OutputStream(path=MagicMock(), fd=None, eof=True)
    job.last_heartbeat_at = time.monotonic()
    return job


class _BlockingSpawn:
    """Callable that blocks its caller forever until released.

    Used to simulate ``spawn_job``/``subprocess.Popen`` blocking forever in
    a daemon worker thread.  When released, returns a valid spawn tuple so the
    callback can abort/reap it.
    """

    def __init__(self) -> None:
        self._gate = threading.Event()
        self._start_count = 0

    def __call__(self, *_args: object, **_kwargs: object) -> _SpawnTuple:
        self._start_count += 1
        self._gate.wait()
        fake_proc = MagicMock()
        fake_proc.pid = 99999
        return (
            fake_proc,
            MagicMock(),
            MagicMock(),
            99999,
            -1,
            -1,
            -1,
        )

    def release(self) -> None:
        self._gate.set()

    @property
    def start_count(self) -> int:
        return self._start_count


def _completed_future(result: _SpawnTuple) -> _SpawnFuture:
    """Build a _SpawnFuture with the result already set.

    Returns:
        A _SpawnFuture with the result already set.
    """
    f = _SpawnFuture(callback=None)
    f.set_result(_spawn_result_from_tuple(result))
    return f


def test_blocked_spawn_does_not_starve_unrelated_active_jobs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A forever-blocked spawn_job must not prevent heartbeat/stop of active jobs."""
    blocker = _BlockingSpawn()
    supervisor = _supervisor(_settings(spawn_deadline_seconds=300.0))
    job = _active_job()
    supervisor.active[job.id] = job
    supervisor._stopping = False

    calls: list[str] = []

    monkeypatch.setattr("lubko.worker.spawn_job", blocker)
    monkeypatch.setattr(supervisor, "_service_processes", lambda: calls.append("svc"))
    monkeypatch.setattr(supervisor, "_drain_captures", lambda *_a: calls.append("drain"))
    monkeypatch.setattr(supervisor, "_enforce_spool_bounds", lambda: calls.append("spool"))
    monkeypatch.setattr(supervisor, "_db_phase", lambda _now: supervisor._claim_batch())
    monkeypatch.setattr(supervisor, "_publish_health_force", lambda: None)
    monkeypatch.setattr(
        "lubko.worker.claim_jobs",
        lambda _conn, _settings, _limit: [_make_claimed()],
    )
    monkeypatch.setattr("lubko.worker.parse_payload", lambda _payload: _FakePayload())
    monkeypatch.setattr("lubko.worker._preflight_failure", lambda _spec: None)

    supervisor._tick(time.monotonic())

    time.sleep(0.05)

    assert "svc" in calls, "_service_processes was called"
    assert "drain" in calls, "_drain_captures was called"
    assert "spool" in calls, "_enforce_spool_bounds was called"
    assert len(supervisor._pending_starts) == 1, "one spawn attempt is pending"
    assert blocker.start_count == 1, "spawn_job was called once in the pool"


def test_spawn_deadline_fails_row_within_bounded_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A timed-out spawn attempt's DB row is failed within the configured deadline."""
    blocker = _BlockingSpawn()
    job_id = uuid4()
    supervisor = _supervisor(_settings(spawn_deadline_seconds=0.01))

    finalized: list[tuple[UUID, str]] = []

    def fake_finalize(jid: UUID, result: object) -> None:
        finalized.append((jid, cast("Any", result).status))

    monkeypatch.setattr("lubko.worker.spawn_job", blocker)
    monkeypatch.setattr(supervisor, "_finalize_immediate", fake_finalize)
    monkeypatch.setattr(supervisor, "_db_phase", lambda _now: supervisor._claim_batch())
    monkeypatch.setattr(supervisor, "_publish_health_force", lambda: None)
    monkeypatch.setattr(
        "lubko.worker.claim_jobs",
        lambda _conn, _settings, _limit: [_make_claimed(job_id)],
    )
    monkeypatch.setattr("lubko.worker.parse_payload", lambda _payload: _FakePayload())
    monkeypatch.setattr("lubko.worker._preflight_failure", lambda _spec: None)

    supervisor._tick(time.monotonic())
    assert len(supervisor._pending_starts) == 1

    time.sleep(0.05)
    supervisor._poll_pending_starts(time.monotonic())

    assert len(finalized) == 1, "the timed-out row was finalized"
    assert finalized[0][1] == "failed"
    assert blocker.start_count == 1


def test_late_spawn_completion_aborted_without_executing_user_code(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If a blocked worker returns GatedSpawn after timeout, it is aborted/reaped."""
    blocker = _BlockingSpawn()
    job_id = uuid4()
    supervisor = _supervisor(_settings(spawn_deadline_seconds=0.01))

    finalized: list[UUID] = []
    aborted: list[UUID] = []

    def fake_finalize(jid: UUID, _result: object) -> None:
        finalized.append(jid)

    def fake_abort_late(_gated: object, jid: UUID) -> None:
        aborted.append(jid)

    monkeypatch.setattr("lubko.worker.spawn_job", blocker)
    monkeypatch.setattr(supervisor, "_finalize_immediate", fake_finalize)
    monkeypatch.setattr(Supervisor, "_abort_and_reap_late", staticmethod(fake_abort_late))
    monkeypatch.setattr(supervisor, "_db_phase", lambda _now: supervisor._claim_batch())
    monkeypatch.setattr(supervisor, "_publish_health_force", lambda: None)
    monkeypatch.setattr(
        "lubko.worker.claim_jobs",
        lambda _conn, _settings, _limit: [_make_claimed(job_id)],
    )
    monkeypatch.setattr("lubko.worker.parse_payload", lambda _payload: _FakePayload())
    monkeypatch.setattr("lubko.worker._preflight_failure", lambda _spec: None)

    supervisor._tick(time.monotonic())
    assert len(supervisor._pending_starts) == 1

    time.sleep(0.05)
    supervisor._poll_pending_starts(time.monotonic())
    assert len(finalized) == 1, "the row was failed"

    blocker.release()
    time.sleep(0.1)

    assert len(aborted) >= 1, "the late GatedSpawn was aborted"
    assert blocker.start_count == 1


def test_later_job_progresses_past_blocked_start(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A second claim batch still starts new jobs while an earlier spawn is blocked."""
    blocker = _BlockingSpawn()
    supervisor = _supervisor(_settings(spawn_deadline_seconds=300.0))

    attempt_count = 0
    job1_id = uuid4()
    job2_id = uuid4()

    def fake_claim(_conn: object, _settings: object, _limit: int) -> list[object]:
        nonlocal attempt_count
        attempt_count += 1
        if attempt_count == 1:
            return [_make_claimed(job1_id)]
        return [_make_claimed(job2_id)]

    monkeypatch.setattr("lubko.worker.spawn_job", blocker)
    monkeypatch.setattr(supervisor, "_publish_health_force", lambda: None)
    monkeypatch.setattr(supervisor, "_db_phase", lambda _now: supervisor._claim_batch())
    monkeypatch.setattr("lubko.worker.claim_jobs", fake_claim)
    monkeypatch.setattr("lubko.worker.parse_payload", lambda _payload: _FakePayload())
    monkeypatch.setattr("lubko.worker._preflight_failure", lambda _spec: None)
    monkeypatch.setattr(supervisor, "_finalize_immediate", lambda _jid, _result: None)

    supervisor._tick(time.monotonic())
    assert len(supervisor._pending_starts) == 1
    assert job1_id in supervisor._pending_starts

    supervisor._tick(time.monotonic())
    assert len(supervisor._pending_starts) == 2
    assert job2_id in supervisor._pending_starts

    time.sleep(0.05)

    assert blocker.start_count == 2, "both spawns ran in separate pool threads"


def test_cleanup_pending_starts_does_not_join_blocked_threads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """_shutdown fails pending starts immediately without joining blocked threads."""
    blocker = _BlockingSpawn()
    supervisor = _supervisor(_settings(spawn_deadline_seconds=300.0))

    finalized: list[UUID] = []

    def fake_finalize(jid: UUID, _result: object) -> None:
        finalized.append(jid)

    monkeypatch.setattr("lubko.worker.spawn_job", blocker)
    monkeypatch.setattr(supervisor, "_finalize_immediate", fake_finalize)
    monkeypatch.setattr(supervisor, "_db_phase", lambda _now: supervisor._claim_batch())
    monkeypatch.setattr(supervisor, "_publish_health_force", lambda: None)
    monkeypatch.setattr(
        "lubko.worker.claim_jobs",
        lambda _conn, _settings, _limit: [_make_claimed()],
    )
    monkeypatch.setattr("lubko.worker.parse_payload", lambda _payload: _FakePayload())
    monkeypatch.setattr("lubko.worker._preflight_failure", lambda _spec: None)

    supervisor._tick(time.monotonic())
    assert len(supervisor._pending_starts) == 1

    start = time.monotonic()
    supervisor._cleanup_pending_starts()
    elapsed = time.monotonic() - start

    assert elapsed < 0.5, f"_cleanup_pending_starts took {elapsed:.2f}s (must not join)"
    assert len(finalized) == 1, "the pending start was finalized"
    assert len(supervisor._pending_starts) == 0


def test_poll_pending_starts_activates_completed_attempt(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A completed spawn attempt is activated: identity persisted, gate released."""
    supervisor = _supervisor(_settings(spawn_deadline_seconds=300.0))

    fake_proc = MagicMock()
    fake_proc.pid = 12345
    spawn_tuple: _SpawnTuple = (
        fake_proc,
        tmp_path / "stdout",
        tmp_path / "stderr",
        12345,
        99,
        -1,
        -1,
    )

    activated: list[UUID] = []
    monkeypatch.setattr(supervisor, "_publish_health_force", lambda: None)

    def _fake_activate(
        _conn: object, jid: UUID, _spec: object, _gated: object, **_kw: object
    ) -> ActiveJob:
        activated.append(jid)
        return _make_active_job(jid)

    monkeypatch.setattr(supervisor, "_activate_gated_job", _fake_activate)

    job_id = uuid4()
    attempt = _StartAttempt(
        job_id=job_id,
        job_spec=MagicMock(id=job_id, cwd=_ANON_DIR, process=("/bin/true",)),
        claim_mono=time.monotonic(),
        version=1,
        submitted_at=time.monotonic(),
        future=_completed_future(spawn_tuple),
    )
    supervisor._pending_starts[job_id] = attempt

    supervisor._poll_pending_starts(time.monotonic())

    assert len(activated) == 1, "the attempt was activated"
    assert job_id in supervisor.active, "the job is now active"


def test_repeated_blocked_starts_cannot_grow_threads_without_bound() -> None:
    """Repeated blocked spawns are bounded by lane count; overflow is rejected.

    The pool has ``NUM_START_LANES`` daemon worker threads and a queue of
    ``SPAWN_LANE_QUEUE_SIZE`` slots.  Submitting more blocking attempts
    than the combined lane+queue capacity must fail with ``queue.Full``
    rather than grow threads without bound.  Every admitted spawn runs in
    exactly one pool thread; rejected spawns never execute ``spawn_job``.
    """
    blocker = _BlockingSpawn()
    lanes = 2
    qsize = 2
    pool = _SpawnExecutor(num_lanes=lanes, queue_size=qsize)

    both_blocked = threading.Event()
    block_lock = threading.Lock()
    block_count = 0

    def blocking_with_event() -> _SpawnTuple:
        nonlocal block_count
        with block_lock:
            block_count += 1
            if block_count == lanes:
                both_blocked.set()
        blocker._gate.wait()
        fake_proc = MagicMock()
        fake_proc.pid = 99999
        return (fake_proc, MagicMock(), MagicMock(), 99999, -1, -1, -1)

    pool_threads_before = len(pool._threads)
    max_admitted = lanes + qsize

    lane_admitted = 0
    for _ in range(lanes):
        future = _SpawnFuture(callback=None)
        pool.submit(uuid4(), blocking_with_event, future)
        lane_admitted += 1
    both_blocked.wait(timeout=1.0)

    queue_admitted = 0
    queue_rejected = 0
    for _ in range(max_admitted):
        future = _SpawnFuture(callback=None)
        jid = uuid4()
        try:
            pool.submit(jid, blocking_with_event, future)
        except queue.Full:
            queue_rejected += 1
            future.set_result(OSError("spawn pool full"))
        else:
            queue_admitted += 1

    time.sleep(0.02)
    pool_threads_after = len(pool._threads)
    blocker.release()
    pool.shutdown()

    assert pool_threads_before <= lanes
    assert pool_threads_after <= lanes, (
        f"pool has {pool_threads_after} threads but num_lanes={lanes}"
    )
    assert lane_admitted + queue_admitted == max_admitted
    assert queue_rejected == qsize


def test_spawn_result_from_tuple_roundtrip() -> None:
    """_spawn_result_from_tuple correctly wraps a spawn_job return tuple."""
    fake_proc = MagicMock()
    fake_proc.pid = 42
    t: _SpawnTuple = (fake_proc, MagicMock(), MagicMock(), 42, 7, 8, 9)
    result = _spawn_result_from_tuple(t)
    assert result.proc is fake_proc
    assert result.pgid == 42
    assert result.gate_fd == 7
    assert result.stdout_read_fd == 8
    assert result.stderr_read_fd == 9


def test_callback_installation_atomic_with_completion() -> None:
    """install_callback is atomic with future completion.

    Regression: timeout observed done()=False, then spawn completed before
    callback was installed, so set_result saw callback=None and the late
    gated child was never cleaned up.  Every interleaving must invoke the
    callback exactly once.
    """
    for _ in range(200):
        future = _SpawnFuture(callback=None)
        invoked: list[str] = []

        def cb(_f: _SpawnFuture, inv: list[str] = invoked) -> None:
            inv.append("cb")

        fake_proc = MagicMock()
        fake_proc.pid = 1
        result = _SpawnResult(
            proc=fake_proc,
            stdout_path=MagicMock(),
            stderr_path=MagicMock(),
            pgid=1,
            gate_fd=-1,
            stdout_read_fd=-1,
            stderr_read_fd=-1,
        )

        def _install(
            f: _SpawnFuture = future,
            c: Any = cb,  # ruff: ignore[any-type]
        ) -> None:
            f.install_callback(c)

        def _set(f: _SpawnFuture = future, r: _SpawnResult = result) -> None:
            f.set_result(r)

        installer = threading.Thread(target=_install)
        setter = threading.Thread(target=_set)

        installer.start()
        setter.start()
        installer.join()
        setter.join()

        assert len(invoked) == 1, f"callback invoked {len(invoked)} times (expected exactly 1)"


def test_install_callback_already_done_invokes_immediately() -> None:
    """install_callback on an already-done future invokes the callback immediately."""
    future = _SpawnFuture(callback=None)
    fake_proc = MagicMock()
    fake_proc.pid = 1
    future.set_result(
        _SpawnResult(
            proc=fake_proc,
            stdout_path=MagicMock(),
            stderr_path=MagicMock(),
            pgid=1,
            gate_fd=-1,
            stdout_read_fd=-1,
            stderr_read_fd=-1,
        )
    )

    invoked: list[str] = []

    def cb(_f: _SpawnFuture) -> None:
        invoked.append("cb")

    was_already_done = future.install_callback(cb)
    assert was_already_done is True
    assert invoked == ["cb"]


def test_install_callback_result_reading_callback_does_not_deadlock() -> None:
    """A callback that calls future.result() (production shape) does not self-deadlock.

    Regression: install_callback invoked cb(self) while holding the
    non-reentrant lock when already done; the production late-completion
    callback immediately calls future.result(), causing self-deadlock.
    The callback must be invoked outside the lock.
    """
    future = _SpawnFuture(callback=None)
    fake_proc = MagicMock()
    fake_proc.pid = 1
    result = _SpawnResult(
        proc=fake_proc,
        stdout_path=MagicMock(),
        stderr_path=MagicMock(),
        pgid=1,
        gate_fd=-1,
        stdout_read_fd=-1,
        stderr_read_fd=-1,
    )
    future.set_result(result)

    read_result: list[object] = []

    def production_callback(f: _SpawnFuture) -> None:
        """Simulates the real late-completion callback that reads result()."""
        r = f.result()
        read_result.append(r)

    was_already_done = future.install_callback(production_callback)
    assert was_already_done is True
    assert len(read_result) == 1
    assert read_result[0] is result


def test_set_result_result_reading_callback_does_not_deadlock() -> None:
    """set_result invoking a callback that calls result() does not self-deadlock."""
    future = _SpawnFuture(callback=None)

    read_result: list[object] = []

    def production_callback(f: _SpawnFuture) -> None:
        r = f.result()
        read_result.append(r)

    future.install_callback(production_callback)

    fake_proc = MagicMock()
    fake_proc.pid = 1
    result = _SpawnResult(
        proc=fake_proc,
        stdout_path=MagicMock(),
        stderr_path=MagicMock(),
        pgid=1,
        gate_fd=-1,
        stdout_read_fd=-1,
        stderr_read_fd=-1,
    )
    future.set_result(result)

    assert len(read_result) == 1
    assert read_result[0] is result


def test_timed_out_callable_skipped_without_execution() -> None:
    """A cancelled future's callable is skipped; an in-flight spawn still fires the callback.

    When ``_handle_timed_out_attempt`` calls ``cancel()``, the worker thread
    must skip the stale callable rather than invoking ``spawn_job``, freeing
    the lane for fresh work.  If the spawn was already executing when
    ``cancel()`` was called, its result still publishes through the callback
    so the late physical completion is properly aborted/reaped.
    """
    pool = _SpawnExecutor(num_lanes=2, queue_size=2)
    gate = threading.Event()
    both_blocked = threading.Event()
    block_lock = threading.Lock()
    block_count = 0

    def lane_spawn() -> _SpawnTuple:
        nonlocal block_count
        with block_lock:
            block_count += 1
            if block_count == 2:
                both_blocked.set()
        gate.wait()
        fake_proc = MagicMock()
        fake_proc.pid = 99999
        return (fake_proc, MagicMock(), MagicMock(), 99999, -1, -1, -1)

    for _ in range(2):
        pool.submit(uuid4(), lane_spawn, _SpawnFuture(callback=None))

    both_blocked.wait(timeout=1.0)

    queue_executed = threading.Event()

    def queue_spawn() -> _SpawnTuple:
        queue_executed.set()
        fake_proc = MagicMock()
        fake_proc.pid = 88888
        return (fake_proc, MagicMock(), MagicMock(), 88888, -1, -1, -1)

    timed_out_future = _SpawnFuture(callback=None)
    pool.submit(uuid4(), queue_spawn, timed_out_future)
    pool.submit(uuid4(), queue_spawn, _SpawnFuture(callback=None))

    timed_out_future.cancel()

    gate.set()
    queue_executed.wait(timeout=1.0)
    time.sleep(0.02)

    assert queue_executed.is_set(), "the non-cancelled queued callable ran"
    assert timed_out_future.done()
    assert timed_out_future.cancelled()

    pool.shutdown()


def test_real_timeout_cancels_queued_callable_and_frees_lane(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The production timeout path cancels the future so the worker skips the callable.

    When ``_poll_pending_starts`` detects a timeout, it calls
    ``_handle_timed_out_attempt`` which cancels the future.  The worker
    thread must then skip the stale callable if it has not started yet,
    freeing the lane for a fresh submission.
    """
    blocker = _BlockingSpawn()
    supervisor = _supervisor(_settings(spawn_deadline_seconds=0.01))

    finalized: list[tuple[UUID, str]] = []

    def fake_finalize(jid: UUID, result: object) -> None:
        finalized.append((jid, cast("Any", result).status))

    monkeypatch.setattr("lubko.worker.spawn_job", blocker)
    monkeypatch.setattr(supervisor, "_finalize_immediate", fake_finalize)
    monkeypatch.setattr(supervisor, "_db_phase", lambda _now: supervisor._claim_batch())
    monkeypatch.setattr(supervisor, "_publish_health_force", lambda: None)
    monkeypatch.setattr(
        "lubko.worker.claim_jobs",
        lambda _conn, _settings, _limit: [_make_claimed()],
    )
    monkeypatch.setattr("lubko.worker.parse_payload", lambda _payload: _FakePayload())
    monkeypatch.setattr("lubko.worker._preflight_failure", lambda _spec: None)

    supervisor._tick(time.monotonic())
    assert len(supervisor._pending_starts) == 1

    job_id = next(iter(supervisor._pending_starts))
    future = supervisor._pending_starts[job_id].future

    time.sleep(0.05)
    supervisor._poll_pending_starts(time.monotonic())

    assert len(finalized) == 1, "the timed-out row was finalized"
    assert finalized[0][1] == "failed"
    assert future.done(), "future must be done after timeout"
    assert future.cancelled(), "future must be cancelled after timeout"
    blocker.release()


class _RacingExecutor(_SpawnExecutor):
    """Executor subclass that pauses between dequeue and try_claim_execution.

    The pause lets the test force ``cancel()`` during the exact handoff
    window, proving the atomic claim prevents stale callable execution.
    """

    def __init__(self, **kw: object) -> None:
        super().__init__(**kw)  # type: ignore[arg-type]
        self._claim_pause = threading.Event()
        self._claim_resume = threading.Event()

    @override
    def _worker_loop(self) -> None:
        while True:
            try:
                _job_id, fn, future = self._queue.get(timeout=0.5)
            except queue.Empty:
                continue
            if self._shutdown:
                future.set_result(OSError("spawn pool shut down"))
                continue
            # --- HANDOFF WINDOW: cancel() can arrive here ---
            self._claim_pause.set()
            self._claim_resume.wait(timeout=1.0)
            # ------------------------------------------------
            if not future.try_claim_execution():
                continue
            try:
                raw = fn()
            except BaseException as exc:  # ruff: ignore[blind-except]
                future.set_result(exc)
            else:
                if raw is None:
                    continue
                result = _spawn_result_from_tuple(cast("_SpawnTuple", raw))
                future.set_result(result)


def test_cancel_between_dequeue_and_claim_skips_callable() -> None:
    """Cancellation during the dequeue→claim window is atomically visible.

    Regression: ``cancelled()`` and ``mark_executed()`` were separate lock
    acquisitions.  ``cancel()`` arriving between them left
    ``_cancelled=True`` but ``_executed=True`` too, so the worker still
    ran ``fn()``.  The atomic ``try_claim_execution()`` makes the
    handshake indivisible: cancellation that wins the race means the
    callable is never invoked.
    """
    gate = threading.Event()
    cancelled_ran = threading.Event()
    sibling_ran = threading.Event()

    pool = _RacingExecutor(num_lanes=1, queue_size=2)

    def cancelled_callable() -> _SpawnTuple:
        cancelled_ran.set()
        fake_proc = MagicMock()
        fake_proc.pid = 11111
        return (fake_proc, MagicMock(), MagicMock(), 11111, -1, -1, -1)

    def sibling_callable() -> _SpawnTuple:
        sibling_ran.set()
        gate.wait()
        fake_proc = MagicMock()
        fake_proc.pid = 22222
        return (fake_proc, MagicMock(), MagicMock(), 22222, -1, -1, -1)

    cancelled_future = _SpawnFuture(callback=None)
    pool.submit(uuid4(), cancelled_callable, cancelled_future)

    sibling_future = _SpawnFuture(callback=None)
    pool.submit(uuid4(), sibling_callable, sibling_future)

    # Wait for the worker to dequeue the cancelled item and reach the pause
    pool._claim_pause.wait(timeout=1.0)
    # Worker is now between dequeue and try_claim_execution.
    # Cancel the future before the worker resumes.
    cancelled_future.cancel()
    # Release the worker to call try_claim_execution (which must return False).
    pool._claim_resume.set()

    gate.set()
    time.sleep(0.2)

    pool.shutdown()

    assert cancelled_future.done()
    assert cancelled_future.cancelled()
    assert not cancelled_ran.is_set(), "cancelled callable must never have been invoked"
    assert sibling_ran.is_set(), "sibling callable must have executed"


def test_shutdown_drains_queue_and_cancels_pending(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """shutdown() drains the queue and delivers OSError to every pending future."""
    supervisor = _supervisor(_settings(spawn_deadline_seconds=300.0))

    # Submit a blocking spawn that will never complete
    blocker = _BlockingSpawn()
    monkeypatch.setattr("lubko.worker.spawn_job", blocker)

    job_id = uuid4()
    future = _SpawnFuture(callback=None)
    supervisor._spawn_pool.submit(job_id, blocker, future)

    # Now shut down the pool
    supervisor._spawn_pool.shutdown()

    # The future must have been failed (drained from queue)
    assert future.done()
    result = future.result()
    assert isinstance(result, OSError)
    assert "shut down" in str(result)

    # A new submit after shutdown must also fail immediately
    future2 = _SpawnFuture(callback=None)
    supervisor._spawn_pool.submit(uuid4(), blocker, future2)
    assert future2.done()
    result2 = future2.result()
    assert isinstance(result2, OSError)

    # The blocking spawn was never actually called
    assert blocker.start_count == 0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _FakePayload:
    """Minimal parsed payload for test claims."""

    server = "server"
    version = 1
    request = MagicMock(cwd=_ANON_DIR, process=("/bin/true",))


class _FakeClaimed:
    """Minimal claimed job for test claims."""

    def __init__(self, job_id: UUID | None = None) -> None:
        self.id = job_id or uuid4()
        self.payload = "{}"


def _make_claimed(job_id: UUID | None = None) -> _FakeClaimed:
    """Build a minimal ClaimedJob.

    Returns:
        A fake ClaimedJob.
    """
    return _FakeClaimed(job_id)


def _make_active_job(job_id: UUID) -> ActiveJob:
    """Build a minimal ActiveJob.

    Returns:
        An ActiveJob with fake process handles.
    """
    proc = MagicMock()
    proc.pid = 12345
    proc.poll.return_value = None
    job = ActiveJob(
        id=job_id,
        cwd=_ANON_DIR,
        process=("/bin/true",),
        proc=proc,
        pid=12345,
        pgid=12345,
        started_mono=time.monotonic(),
        claimed_at=time.monotonic(),
        version=1,
    )
    job.stdout = OutputStream(path=MagicMock(), fd=None, eof=True)
    job.stderr = OutputStream(path=MagicMock(), fd=None, eof=True)
    job.last_heartbeat_at = time.monotonic()
    return job
