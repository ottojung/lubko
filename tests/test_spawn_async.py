"""Deterministic invariants for asynchronous non-blocking spawn attempts.

The worker supervisor must never synchronously call ``spawn_job``/``subprocess.Popen``
in the tick loop because that seam can block forever.  Each spawn attempt runs in
a bounded daemon pool; the main loop polls completed futures each tick, enforces a
bounded start deadline, and fails the DB row on timeout.  One permanently blocked
start must never consume the only start lane: later queue work must still progress.
"""

from __future__ import annotations

import threading
import time
from typing import TYPE_CHECKING, Any, cast
from unittest.mock import MagicMock
from uuid import uuid4

from lubko.config import DatabaseConfig
from lubko.worker import (
    NUM_START_LANES,
    ActiveJob,
    OutputStream,
    Settings,
    Supervisor,
    _spawn_result_from_tuple,
    _SpawnFuture,
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
    """Repeated blocked spawn attempts are bounded by the thread pool size.

    The pool has ``NUM_START_LANES`` daemon worker threads.  Submitting
    more blocking attempts than pool workers must NOT create additional
    threads: the excess attempts queue inside the pool.  This proves the
    bounded resource model.
    """
    blocker = _BlockingSpawn()
    supervisor = _supervisor(_settings(spawn_deadline_seconds=300.0))

    pool_threads_before = len(supervisor._spawn_pool._threads)

    num_attempts = NUM_START_LANES * 3
    for _ in range(num_attempts):
        future = _SpawnFuture(callback=None)
        jid = uuid4()
        supervisor._spawn_pool.submit(jid, blocker, future)
        supervisor._pending_starts[jid] = _StartAttempt(
            job_id=jid,
            job_spec=MagicMock(id=jid, cwd=_ANON_DIR, process=("/bin/true",)),
            claim_mono=time.monotonic(),
            version=1,
            submitted_at=time.monotonic(),
            future=future,
        )

    time.sleep(0.05)
    pool_threads_after = len(supervisor._spawn_pool._threads)

    assert pool_threads_before <= NUM_START_LANES
    assert pool_threads_after <= NUM_START_LANES, (
        f"pool has {pool_threads_after} threads but NUM_START_LANES={NUM_START_LANES}"
    )
    assert len(supervisor._pending_starts) == num_attempts
    assert blocker.start_count == NUM_START_LANES


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
