"""Hard client deadline for established database operations.

Invariants: an established operation that outlives its lease-safety-derived
client deadline is abandoned and classified as a connectivity failure, only
deadline-capable supervisor connections accept a deadline, and settings fail
closed unless the configured timeout fits strictly inside the lease-safety
budget.
"""

import dataclasses
import os
import selectors as selectors_module
import time
from collections.abc import Generator
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Final, Self, cast
from uuid import uuid4

import pytest

from lubko import worker
from lubko.config import DatabaseConfig
from lubko.worker import (
    DEFAULT_LEASE_DURATION_SECONDS,
    STOP_REASON_LEASE,
    ActiveJob,
    DbOperationDeadlineError,
    Settings,
    Supervisor,
    install_operation_deadline,
    operation_deadline_at,
    wait_with_deadline,
)

DEADLINE_TEST_TIMEOUT_SECONDS = 5.0


if TYPE_CHECKING:
    import subprocess


def make_settings(
    *,
    process_poll_interval_seconds: float = 0.0,
    lease_duration_seconds: float = 30.0,
    lease_safety_margin_seconds: float = 5.0,
    lease_refresh_interval_seconds: float = 5.0,
    db_operation_timeout_seconds: float | None = None,
) -> Settings:
    """Build validated settings with explicit lease/deadline knobs.

    Args:
        process_poll_interval_seconds: Process observation cadence.
        lease_duration_seconds: Lease window granted by each heartbeat.
        lease_safety_margin_seconds: Safety margin before lease expiry.
        lease_refresh_interval_seconds: Heartbeat cadence.
        db_operation_timeout_seconds: Hard client deadline cap; ``None`` keeps
            a value strictly inside the lease-safety budget.

    Returns:
        Validated worker settings.
    """
    if db_operation_timeout_seconds is None:
        # Strictly inside the fail-closed budget duration - margin - refresh.
        db_operation_timeout_seconds = (
            lease_duration_seconds
            - lease_safety_margin_seconds
            - lease_refresh_interval_seconds
            - 1.0
        )
    return Settings(
        worker_id="w-test",
        poll_interval_seconds=0.0,
        process_poll_interval_seconds=process_poll_interval_seconds,
        cancel_grace_seconds=1.0,
        server="srv-test",
        lease_duration_seconds=lease_duration_seconds,
        lease_safety_margin_seconds=lease_safety_margin_seconds,
        lease_refresh_interval_seconds=lease_refresh_interval_seconds,
        db_operation_timeout_seconds=db_operation_timeout_seconds,
    )


class _InstantSelector:
    """A selector stand-in whose waits never block and never report readiness."""

    @staticmethod
    def register(_fileno: int, _events: int) -> None:
        return None

    @staticmethod
    def modify(_fileno: int, _events: int) -> None:
        return None

    @staticmethod
    def select(*, timeout: float) -> list[tuple[int, int]]:
        del timeout
        return []

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc_value: BaseException | None,
        _traceback: object,
    ) -> None:
        return None


#: Readiness event mask; matches what selectors deliver and psycopg sends.
READY_EVENT: Final = 1


class _CompletingGen:
    """A libpq-shaped generator that becomes ready once and then commits."""

    def __init__(self) -> None:
        self.waited = False

    def __iter__(self) -> Self:
        return self

    def __next__(self) -> int:
        if not self.waited:
            self.waited = True
            return READY_EVENT
        result = "committed"
        raise StopIteration(result)

    def send(self, _value: object) -> int:
        return next(self)


def completing_gen() -> Generator[int, object, str]:
    """A libpq-shaped generator that becomes ready once and then commits.

    Returns:
        A generator that yields one readiness event, then commits.
    """
    return cast("Generator[int, object, str]", iter(_CompletingGen()))


def hung_gen() -> Generator[int, object, None]:
    """A libpq-shaped generator that yields readiness forever and never ends.

    Yields:
        Readiness requests that are never satisfied by data.
    """
    while True:
        yield READY_EVENT


def test_wait_with_deadline_returns_completed_operation() -> None:
    """An operation whose socket becomes ready completes and returns its value."""
    read_fd, write_fd = os.pipe()
    os.write(write_fd, b"x")
    try:
        result: str = wait_with_deadline(
            cast("Any", completing_gen()), read_fd, time.monotonic() + DEADLINE_TEST_TIMEOUT_SECONDS
        )
        assert result == "committed"
    finally:
        os.close(read_fd)
        os.close(write_fd)


def test_wait_with_deadline_raises_immediately_on_expired_deadline() -> None:
    """A deadline already in the past fails the operation before any waiting."""
    read_fd, write_fd = os.pipe()
    os.close(write_fd)
    try:
        with pytest.raises(DbOperationDeadlineError):
            wait_with_deadline(cast("Any", completing_gen()), read_fd, time.monotonic() - 1.0)
    finally:
        os.close(read_fd)


def test_wait_with_deadline_abandons_hung_socket_at_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A socket that never becomes ready is abandoned exactly at the deadline.

    The selector is replaced by a nonblocking fake and the monotonic clock by
    an injected incrementing sequence, so hang detection is fully deterministic.
    """
    monkeypatch.setattr(selectors_module, "DefaultSelector", _InstantSelector)
    clock = {"now": 99.0}

    def ticking_monotonic() -> float:
        clock["now"] += 1.0
        return clock["now"]

    monkeypatch.setattr(time, "monotonic", ticking_monotonic)

    read_fd, write_fd = os.pipe()
    try:
        with pytest.raises(DbOperationDeadlineError):
            wait_with_deadline(cast("Any", hung_gen()), read_fd, deadline=101.0)
        assert clock["now"] == pytest.approx(101.0)
    finally:
        os.close(read_fd)
        os.close(write_fd)


def test_install_operation_deadline_capability_contract() -> None:
    """Only deadline-capable connection types accept a deadline.

    A ``None`` connection is a no-op; a connection type without the
    ``operation_deadline`` capability marker fails closed, so a production
    plain connection can never silently operate unbounded; a capable type
    accepts the deadline.
    """

    class _CapableDouble:
        operation_deadline: float = 0.0

    class _PlainDouble:
        pass

    install_operation_deadline(None, 5.0)
    with pytest.raises(TypeError, match="deadline"):
        install_operation_deadline(cast("worker.JobsConnection", _PlainDouble()), 5.0)
    capable = _CapableDouble()
    install_operation_deadline(cast("worker.JobsConnection", capable), 7.25)
    assert capable.operation_deadline == pytest.approx(7.25)


def test_db_operation_timeout_fail_closed_ordering() -> None:
    """Settings enforce the lease-safety deadline ordering invariant.

    A timeout that can outlive ``duration - margin - refresh`` is refused;
    one strictly inside the budget starts cleanly.
    """
    with pytest.raises(ValueError, match="LUBKO_DB_OPERATION_TIMEOUT_SECONDS"):
        make_settings(db_operation_timeout_seconds=25.0)
    settings = make_settings(db_operation_timeout_seconds=14.999)
    assert settings.db_operation_timeout_seconds == pytest.approx(14.999)


def test_operation_deadline_at_derivation() -> None:
    """The deadline is the timeout cap, tightened by the earliest safety instant."""
    settings = make_settings(db_operation_timeout_seconds=15.0)
    assert operation_deadline_at(100.0, [], settings) == pytest.approx(115.0)
    # last heartbeat 80 + duration 30 - margin 5 caps below the cap.
    assert operation_deadline_at(100.0, [80.0], settings) == pytest.approx(105.0)


def make_active_job(tmp_path: Path, *, heartbeat_at: float) -> ActiveJob:
    """Build a structurally complete registry entry without spawning a process.

    Returns:
        An active-job registry entry with no live child process.
    """
    job = object.__new__(ActiveJob)
    # Populate every field from its dataclass default so the fixture stays
    # structurally complete even as the registry grows new bookkeeping fields.
    for f in dataclasses.fields(ActiveJob):
        if f.default is not dataclasses.MISSING:
            setattr(job, f.name, f.default)
        elif f.default_factory is not dataclasses.MISSING:
            setattr(job, f.name, f.default_factory())
    job.id = uuid4()
    job.cwd = str(tmp_path)
    job.process = ("true",)
    job.proc = cast("subprocess.Popen[bytes]", object())
    job.pid = -1
    # A signal target that safely absorbs stray SIGTERMs: this test process
    # itself, where the default handler is never replaced during the test.
    job.pgid = os.getpid()
    job.started_mono = 0.0
    job.claimed_at = 0.0
    job.stdout = worker.OutputStream(tmp_path / "out")
    job.stderr = worker.OutputStream(tmp_path / "err")
    job.completed = False
    job.term_sent = False
    job.last_heartbeat_at = heartbeat_at
    return job


def test_supervisor_operation_deadline_ignores_non_live_groups(tmp_path: Path) -> None:
    """Only live owned groups with committed heartbeats tighten the deadline."""
    supervisor = Supervisor(
        make_settings(),
        DatabaseConfig(host="h", port=1, dbname="d", user="u", password=str(uuid4())),
    )
    live = make_active_job(tmp_path, heartbeat_at=1000.0)
    done = make_active_job(tmp_path, heartbeat_at=500.0)
    done.completed = True
    stopping = make_active_job(tmp_path, heartbeat_at=400.0)
    stopping.term_sent = True
    fresh = make_active_job(tmp_path, heartbeat_at=0.0)
    for job in (live, done, stopping, fresh):
        supervisor.active[job.id] = job
    deadline = supervisor._operation_deadline(now_mono=1010.0)
    assert deadline == pytest.approx(1000.0 + DEFAULT_LEASE_DURATION_SECONDS - 5.0)


class _CapableConn:
    """A deadline-capable connection double with a trivial work seam."""

    operation_deadline: float = 0.0
    queries: ClassVar[list[str]] = []

    @classmethod
    def connect(cls, conninfo: str, **_kwargs: object) -> "_CapableConn":
        cls.queries.append(conninfo)
        return cls()

    def execute(self, query: str) -> None:
        self.queries.append(query)


def test_connect_selects_the_production_deadline_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Production ``_connect`` cannot fall back to a plain connection.

    The lazily bound driver class is replaced by a recording capable double;
    ``_connect`` must construct exactly that class, install a fresh deadline
    on the resulting live connection, and publish it as the supervisor's
    connection. A plain connection could never pass
    :func:`install_operation_deadline` (see the capability contract), so this
    wiring is the fail-closed production path.
    """
    supervisor = Supervisor(
        make_settings(db_operation_timeout_seconds=3.0),
        DatabaseConfig(host="h", port=1, dbname="d", user="u", password=str(uuid4())),
    )
    monkeypatch.setattr(worker, "verify_jobs_table_invariant", lambda _conn: None)
    monkeypatch.setattr(worker, "verify_protocol_schema", lambda _conn: None)
    monkeypatch.setattr(worker, "verify_server_isolation", lambda _conn: None)
    monkeypatch.setattr(worker, "verify_server_identity", lambda _conn, _server: None)
    # Bind the recording double into the module globals without tripping the
    # lazy ``__getattr__`` loader: setattr would resolve the real driver-bound
    # class just to check for its existence.
    monkeypatch.setitem(vars(worker), "DeadlineConnection", _CapableConn)
    # Wiring-only: skip durable health publication side effects.
    monkeypatch.setattr(supervisor, "_publish_health_force", lambda: None)
    monkeypatch.setattr(supervisor, "_publish_health", lambda **_kwargs: None)
    before = time.monotonic()
    supervisor._connect()
    conn = cast("Any", supervisor.conn)
    assert isinstance(conn, _CapableConn)
    assert conn.operation_deadline >= before + 3.0 - 1e-9
    # A deadline-capable connection accepts the per-turn deadline directly.
    install_operation_deadline(cast("worker.JobsConnection", conn), 42.0)
    assert conn.operation_deadline == pytest.approx(42.0)


def test_run_converges_to_outage_and_lease_enforcement_on_deadline_breach(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A hung database operation converges through outage to lease enforcement.

    When an established operation breaches its hard client deadline, the run
    loop must enter outage handling and immediately terminate any owned group
    that can no longer be refreshed safely, in the very same turn rather than
    after sleeping.
    """
    settings = make_settings(process_poll_interval_seconds=0.0)
    supervisor = Supervisor(
        settings,
        DatabaseConfig(host="h", port=1, dbname="d", user="u", password=str(uuid4())),
    )
    job = make_active_job(tmp_path, heartbeat_at=time.monotonic() - settings.lease_duration_seconds)
    # The turn under test must reach the database phase: give both capture
    # spools their expected on-disk state so no earlier fail-closed path fires.
    job.stdout.path.touch()
    job.stderr.path.touch()
    supervisor.active[job.id] = job
    monkeypatch.setattr(supervisor, "_service_processes", lambda: None)

    breach_message = "hung established connection"

    def hung_db_phase(_now: float) -> None:
        raise DbOperationDeadlineError(breach_message)

    _CapableConn.queries.clear()
    reconnects = {"n": 0}

    def failing_connect() -> None:
        reconnects["n"] += 1
        supervisor.conn = cast("worker.JobsConnection", _CapableConn())

    def stop_when_converged(*_args: object, **_kwargs: object) -> None:
        # Stop once the breach has evicted the unsafe group and a fresh
        # capable connection has been restored by a subsequent turn.
        if job.lease_evicted and supervisor.conn is not None:
            supervisor._stopping = True

    monkeypatch.setattr(supervisor, "_db_phase", hung_db_phase)
    monkeypatch.setattr(supervisor, "_connect", failing_connect)
    monkeypatch.setattr(supervisor, "_publish_health", stop_when_converged)
    monkeypatch.setattr(supervisor, "_shutdown", lambda: None)
    supervisor.run()
    assert reconnects["n"] >= 1
    assert job.lease_evicted
    assert job.stop_reason == STOP_REASON_LEASE
    assert job.term_sent
    # Restored connectivity: a fresh capable connection carries new work.
    restored = cast("_CapableConn", supervisor.conn)
    restored.execute("SELECT 1")
    assert "SELECT 1" in _CapableConn.queries
