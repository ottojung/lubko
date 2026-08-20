"""Process-level regressions for GitHub issue #75.

Planned worker retirement must never force-kill or forget a worker before every
owned command process group is dead/reaped/finalized or safely transferred. The
outer lifecycle authority must observe the worker's explicit safe-to-reap
boundary (a drain acknowledgement) rather than race two equal-duration timers,
and a wedged worker's surviving command groups must be recovered by exact
process-group identity rather than silently orphaned.

The first two tests are pure process-level (no PostgreSQL) and exercise the
drain/stop protocol and the exact-ownership recovery directly. The remaining
tests run the same scenarios through a real maintained worker and through the
external supervisor restart path; they require a local PostgreSQL cluster and
are skipped automatically when one is unavailable.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from contextlib import contextmanager, suppress
from pathlib import Path
from typing import TYPE_CHECKING, Final, Self
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from psycopg import connect

from lubko import lifecycle
from lubko import worker as worker_mod
from lubko.supervisor import OwnedGroupRecoveryError, SupervisorDaemon
from tests import _process_guard as guard
from tests.test_lifecycle import identity_of

if TYPE_CHECKING:
    from collections.abc import Iterator

SLEEP_BIN: Final = __import__("shutil").which("sleep") or "/bin/sleep"
REPAIR_WORKER_ID: Final = "repair-worker"
REPAIR_TIMINGS: Final = {
    "LUBKO_POLL_INTERVAL_SECONDS": "0.05",
    "LUBKO_PROCESS_POLL_INTERVAL_SECONDS": "0.01",
    "LUBKO_CANCEL_GRACE_SECONDS": "0.5",
    "LUBKO_LEASE_DURATION_SECONDS": "2.0",
    "LUBKO_LEASE_REFRESH_INTERVAL_SECONDS": "0.15",
    "LUBKO_LEASE_RECOVERY_INTERVAL_SECONDS": "0.2",
    "LUBKO_LEASE_SAFETY_MARGIN_SECONDS": "0.3",
    "LUBKO_OUTPUT_PUBLICATION_INTERVAL_SECONDS": "0.1",
    "LUBKO_CLAIM_BATCH_LIMIT": "16",
}

#: A scripted worker that mimics the real worker's drain contract: it owns one
#: command process group (which ignores SIGTERM), and on SIGTERM it drains
#: that group (SIGTERM, wait the cancel grace, SIGKILL), writes the
#: per-incarnation drain sentinel, and exits. This lets the no-PG tests prove
#: the outer lifecycle authority waits for the drain boundary instead of
#: racing it.
WORKER_SCRIPT: Final = """
import os, sys, time, signal, subprocess
from contextlib import suppress

token = sys.argv[1]
child_pgid_file = sys.argv[2]
cancel_grace = float(sys.argv[3])
state = os.environ["XDG_STATE_HOME"]
sentinel = os.path.join(state, "lubko/worker/drain", token + ".drained")


def spawn_child():
    return subprocess.Popen(
        [
            "/bin/sh", "-c",
            'trap "" TERM; exec %s -c "import signal,time; '
            'signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(30)  '
            '# lubko_issue75_child"' % sys.executable,
        ],
        start_new_session=True,
    )


child = spawn_child()
pgid = os.getpgid(child.pid)
with open(child_pgid_file, "w") as fh:
    fh.write(str(pgid))


def handle(signum, frame):
    os.killpg(pgid, signal.SIGTERM)
    time.sleep(cancel_grace)
    os.killpg(pgid, signal.SIGKILL)
    with suppress(OSError):
        os.makedirs(os.path.dirname(sentinel), exist_ok=True)
        with open(sentinel, "w") as fh:
            fh.write(token + "\\n")
    os._exit(0)


signal.signal(signal.SIGTERM, handle)
while True:
    time.sleep(0.2)
"""


def _spawn_scripted_worker(
    tmp_path: Path, token: str, cancel_grace: float, *, env_token: str | None = None
) -> subprocess.Popen[bytes]:
    """Spawn the scripted drain worker, registered with the process guard.

    Args:
        tmp_path: Test temporary directory.
        token: Lifecycle token (incarnation) for the worker.
        cancel_grace: Seconds the worker waits before SIGKILLing its group.
        env_token: The ``LUBKO_LIFECYCLE_TOKEN`` to place in the worker's
            environment. Defaults to ``token`` so the worker is recognized as
            owned; a divergent value simulates a live, unowned (wrong-token)
            process that must not be signalled.

    Returns:
        The spawned worker process.
    """
    script = tmp_path / "scripted_worker.py"
    script.write_text(WORKER_SCRIPT, encoding="utf-8")
    child_pgid_file = tmp_path / "child_pgid"
    env = dict(os.environ)
    env["LUBKO_LIFECYCLE_TOKEN"] = env_token if env_token is not None else token
    proc = subprocess.Popen(
        [sys.executable, str(script), token, str(child_pgid_file), str(cancel_grace)],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        close_fds=True,
        env=env,
    )
    guard.register(proc)
    return proc


def _wait_child_pgid(child_pgid_file: Path, timeout: float = 5.0) -> int:
    """Read the child process group id the scripted worker recorded.

    Args:
        child_pgid_file: File the worker writes the pgid to.
        timeout: Maximum seconds to wait.

    Returns:
        The recorded process group id.

    Raises:
        AssertionError: If the worker never publishes a process group id.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if child_pgid_file.exists():
            with suppress(ValueError, OSError):
                return int(child_pgid_file.read_text(encoding="utf-8").strip())
        time.sleep(0.02)
    msg = "scripted worker never published its child process group id"
    raise AssertionError(msg)


def _meta_for_live(
    proc: subprocess.Popen[bytes], tmp_path: Path, token: str
) -> lifecycle.WorkerMeta:
    """Build running metadata for a live worker process.

    Args:
        proc: Live worker process.
        tmp_path: Repository path to record.
        token: Lifecycle token (incarnation) for the worker.

    Returns:
        Running metadata for the process.
    """
    identity = identity_of(proc)
    return lifecycle.WorkerMeta(
        schema_version=1,
        state=lifecycle.STATE_RUNNING,
        pid=identity.pid,
        pgid=identity.pgid,
        sid=identity.sid,
        start_time_ticks=identity.start_time_ticks,
        token=token,
        repo=str(tmp_path),
        git_commit="a" * 40,
        worker_id="test-worker",
        log_path=str(lifecycle.worker_log_path()),
        started_at=time.time(),
        stopped_at=None,
    )


def test_stop_worker_respects_drain_of_sigterm_ignoring_group(tmp_path: Path) -> None:
    """The outer authority must not kill the worker before its group is drained.

    A command whose process group ignores SIGTERM survives until the worker's
    inner cancel grace boundary. The outer ``stop_worker`` must wait for the
    worker's drain acknowledgement and must not SIGKILL the worker while the
    command group is still alive, must leave no descendant process, and must see
    the command reach a terminal (gone) state.
    """
    token = "issue75-drain-token"  # ruff: ignore[hardcoded-password-string]
    cancel_grace = 1.0
    proc = _spawn_scripted_worker(tmp_path, token, cancel_grace)
    meta = _meta_for_live(proc, tmp_path, token)
    child_pgid = _wait_child_pgid(tmp_path / "child_pgid")
    assert worker_mod.group_has_members(child_pgid)

    # While the command group is still alive, the outer authority must not have
    # force-killed the worker: that would be the forbidden equal-timeout race.
    outer_deadline = time.monotonic() + 15.0
    killed_early = False
    while worker_mod.group_has_members(child_pgid) and time.monotonic() < outer_deadline:
        if proc.poll() is not None:
            killed_early = True
        time.sleep(0.02)
    assert not killed_early, "outer authority force-killed the worker before its group was drained"

    stopped = lifecycle.stop_worker(meta, 5.0, cancel_grace_seconds=cancel_grace)
    assert stopped
    assert proc.poll() is not None
    assert not worker_mod.group_has_members(child_pgid)
    # The drain sentinel proves the worker reached its safe-to-reap boundary.
    sentinel = worker_mod.drain_sentinel_path(token)
    assert sentinel.exists()
    assert sentinel.read_text(encoding="utf-8").strip() == token


def test_stop_worker_equal_timeouts_do_not_race_drain(tmp_path: Path) -> None:
    """Equal outer/inner timeouts must no longer race worker cleanup.

    With ``stop_grace == cancel_grace``, the worker still drains its group
    before the outer authority may treat it as wedged, because the outer wait
    floor is the worker's cancel grace plus finalization slack.
    """
    token = "issue75-equal-timeout"  # ruff: ignore[hardcoded-password-string]
    cancel_grace = 1.0
    proc = _spawn_scripted_worker(tmp_path, token, cancel_grace)
    meta = _meta_for_live(proc, tmp_path, token)
    child_pgid = _wait_child_pgid(tmp_path / "child_pgid")
    stopped = lifecycle.stop_worker(meta, cancel_grace, cancel_grace_seconds=cancel_grace)
    assert stopped
    assert proc.poll() is not None
    assert not worker_mod.group_has_members(child_pgid)
    assert worker_mod.drain_sentinel_path(token).exists()


class _FakeCursor:
    """Minimal cursor double returning a fixed row set for recovery queries."""

    def __init__(self, rows: list[tuple[object, ...]]) -> None:
        self._rows = rows
        self.executed: list[tuple[str, object]] = []

    def __enter__(self) -> Self:
        """Enter the cursor context.

        Returns:
            Self, so the ``with`` target is the cursor itself.
        """
        return self

    def __exit__(self, _exc_type: object, _exc: object, _tb: object) -> None:
        """Exit the cursor context (no-op)."""
        return

    def execute(self, sql: str, params: object = None) -> None:
        """Record the query (ignored)."""
        self.executed.append((sql, params))

    def fetchall(self) -> list[tuple[object, ...]]:
        """Return the fixed rows."""
        return self._rows


class _FakeConn:
    """Minimal connection double for :func:`recover_owned_job_groups`."""

    def __init__(self, rows: list[tuple[object, ...]]) -> None:
        self._rows = rows

    @contextmanager
    def transaction(self) -> Iterator[_FakeConn]:
        """Yield self as the transaction context."""
        yield self

    def cursor(self, row_factory: object = None) -> _FakeCursor:  # ruff: ignore[unused-method-argument]
        """Return a fake cursor."""
        return _FakeCursor(self._rows)


def test_recover_owned_job_groups_kills_exact_groups_only() -> None:
    """Emergency recovery must terminate only exact owned groups, never by name.

    A wedged worker can leave a command group alive after it is SIGKILLed. The
    recovery targets the exact process-group id persisted in the job row for the
    retired incarnation and never touches other groups. It is also PID-reuse
    safe and fail-closed: a persisted group id that has since been recycled by
    an unrelated process (different start-time ticks) is never signalled even
    though it is still alive, and the unverifiable group is reported as
    ``unresolved`` so the orchestrator blocks rather than clearing authority.
    """
    unrelated = subprocess.Popen(
        [SLEEP_BIN, "300"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        close_fds=True,
    )
    guard.register(unrelated)
    owned = subprocess.Popen(
        [
            "/bin/sh",
            "-c",
            'trap "" TERM; exec ' + sys.executable + ' -c "import signal,time; '
            'signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(30)"',
        ],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        close_fds=True,
    )
    guard.register(owned)
    owned_pgid = os.getpgid(owned.pid)
    # A stale, reused group id: it points at the still-alive ``unrelated``
    # process but carries start-time ticks that do NOT match that process, so
    # recovery must treat it as a recycled id, leave it untouched, and report it
    # as unresolved (a durable blocking obligation).
    reused_pgid = os.getpgid(unrelated.pid)
    reused_ticks = (lifecycle.proc_start_ticks(unrelated.pid) or 0) + 1
    try:
        incarnation = "issue75-wedged-incarnation"
        conn = _FakeConn([
            (uuid4(), str(owned_pgid), str(lifecycle.proc_start_ticks(owned.pid))),
            (uuid4(), str(reused_pgid), str(reused_ticks)),
        ])
        result = worker_mod.recover_owned_job_groups(conn, incarnation, 0.5)  # type: ignore[arg-type]
        assert result.reaped == [owned_pgid]
        assert result.surviving == []
        assert result.unresolved == [reused_pgid]
        assert not worker_mod.group_has_members(owned_pgid)
        # The reused id was NOT acted on: the unrelated process is untouched.
        assert worker_mod.group_has_members(reused_pgid)
    finally:
        if owned.poll() is None:
            with suppress(ProcessLookupError):
                os.killpg(owned_pgid, signal.SIGKILL)
        if unrelated.poll() is None:
            with suppress(ProcessLookupError):
                os.killpg(os.getpgid(unrelated.pid), signal.SIGKILL)
        for p in (owned, unrelated):
            with suppress(Exception):
                p.wait(timeout=5)
            guard.unregister(p)


def _spawn_real_worker(
    db_conf: Path, *, worker_id: str = REPAIR_WORKER_ID
) -> subprocess.Popen[bytes]:
    """Spawn a real queue-consuming worker registered with the process guard.

    Args:
        db_conf: Database configuration file for the worker.
        worker_id: Worker identifier the worker records on claims.

    Returns:
        The spawned worker process.
    """
    env = dict(os.environ)
    env["LUBKO_DATABASE_CONFIG"] = str(db_conf)
    env["LUBKO_WORKER_ID"] = worker_id
    env.update(REPAIR_TIMINGS)
    proc = subprocess.Popen(
        [sys.executable, "-m", "lubko.worker"],
        cwd=Path(__file__).resolve().parent.parent,
        env=env,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        close_fds=True,
    )
    guard.register(proc)
    return proc


def _db_conf_from_conninfo(conninfo: str, tmp_path: Path) -> Path:
    """Write a database config file the worker can read from a conninfo string.

    Args:
        conninfo: A psycopg-style conninfo string.
        tmp_path: Directory for the config file.

    Returns:
        The config file path.
    """
    host = port = ""
    for part in conninfo.split():
        if part.startswith("host="):
            host = part.split("=", 1)[1]
        elif part.startswith("port="):
            port = part.split("=", 1)[1]
    conf = tmp_path / "database.conf"
    conf.write_text(
        f"host={host}\nport={port}\ndbname=postgres\nuser=postgres\npassword=local-trust\n",
        encoding="utf-8",
    )
    conf.chmod(0o600)
    return conf


def _insert_pending_job(conninfo: str, cwd: str, command: str) -> object:
    """Insert a protocol v3 pending command job running a shell snippet.

    Args:
        conninfo: PostgreSQL connection string.
        cwd: Working directory for the job.
        command: Shell snippet, executed by an explicit ``/bin/sh -c`` argv.

    Returns:
        The job identifier.
    """
    payload = json.dumps({
        "v": 3,
        "type": "command",
        "request": {"cwd": cwd, "process": ["/bin/sh", "-c", command]},
        "state": {"status": "pending"},
    })
    with connect(conninfo) as conn:
        row = conn.execute(
            "INSERT INTO lubko.jobs (payload) VALUES (%s) RETURNING id",
            (payload,),
        ).fetchone()
    assert row is not None
    return row[0]


def _read_job_field(conninfo: str, job_id: object, field_sql: str) -> object:
    """Read one JSON field from a job's payload.

    Args:
        conninfo: PostgreSQL connection string.
        job_id: Job identifier.
        field_sql: SQL expression selecting the JSON field.

    Returns:
        The decoded field value, or ``None``.
    """
    with connect(conninfo) as conn:
        row = conn.execute(
            f"SELECT {field_sql} FROM lubko.jobs WHERE id = %s",  # ruff: ignore[hardcoded-sql-expression]
            (job_id,),
        ).fetchone()
    if row is None or row[0] is None:
        return None
    return row[0]


def _wait_for_claim(jobs_db: str, job_id: object, tmp_path: Path) -> tuple[int, str]:
    """Wait for the worker to claim the job and publish its process group.

    Args:
        jobs_db: PostgreSQL connection string.
        job_id: The pending job identifier.
        tmp_path: Repository path recorded in metadata.

    Returns:
        The claimed ``(child_pgid, incarnation)``.
    """
    del tmp_path
    status_sql = "(payload::jsonb)->'state'->>'status'"
    pgid_sql = "(payload::jsonb)->'state'->>'process_pgid'"
    inc_sql = "(payload::jsonb)->'state'->>'worker_incarnation'"
    deadline = time.monotonic() + 15.0
    child_pgid: int | None = None
    incarnation: object = None
    while time.monotonic() < deadline:
        status = _read_job_field(jobs_db, job_id, status_sql)
        if status == "running":
            pgid = _read_job_field(jobs_db, job_id, pgid_sql)
            incarnation = _read_job_field(jobs_db, job_id, inc_sql)
            if pgid is not None and incarnation is not None:
                child_pgid = int(str(pgid))
                break
        time.sleep(0.05)
    assert child_pgid is not None
    assert incarnation is not None
    assert worker_mod.group_has_members(child_pgid)
    return child_pgid, str(incarnation)


def _assert_root_terminal(jobs_db: str, job_id: object) -> None:
    """Assert the root job reaches a deterministic terminal state.

    Args:
        jobs_db: PostgreSQL connection string.
        job_id: The job identifier.
    """
    status_sql = "(payload::jsonb)->'state'->>'status'"
    status_deadline = time.monotonic() + 10.0
    status = None
    while time.monotonic() < status_deadline:
        status = _read_job_field(jobs_db, job_id, status_sql)
        if status in {"succeeded", "failed", "cancelled"}:
            break
        time.sleep(0.05)
    assert status == "cancelled"


def test_planned_replacement_drains_sigterm_ignoring_command(jobs_db: str, tmp_path: Path) -> None:
    """A planned replacement must drain a SIGTERM-ignoring command group.

    Runs a real maintained worker, claims a command whose process group ignores
    SIGTERM, requests an intentional replacement via ``stop_worker``, and proves
    the outer authority does not kill/forget the worker before the command group
    is SIGKILLed and gone, no descendant remains, the root job reaches a terminal
    state, and a replacement worker is the sole consumer only after the old
    execution ownership is safe.
    """
    db_conf = _db_conf_from_conninfo(jobs_db, tmp_path)
    worker = _spawn_real_worker(db_conf)
    py = sys.executable
    command = (
        'trap "" TERM; exec ' + py + ' -c "import signal,time; '
        'signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(30)"'
    )
    job_id = _insert_pending_job(jobs_db, str(tmp_path), command)
    incarnation: object = None
    try:
        child_pgid, incarnation = _wait_for_claim(jobs_db, job_id, tmp_path)
        meta = _meta_for_live(worker, tmp_path, str(incarnation))

        # While the command group is alive the outer authority must not kill
        # the worker: that is the forbidden equal-timeout race.
        race_deadline = time.monotonic() + 15.0
        killed_early = False
        while worker_mod.group_has_members(child_pgid) and time.monotonic() < race_deadline:
            if worker.poll() is not None:
                killed_early = True
            time.sleep(0.02)
        assert not killed_early

        stopped = lifecycle.stop_worker(meta, 5.0)
        assert stopped
        assert worker.poll() is not None
        assert not worker_mod.group_has_members(child_pgid)

        _assert_root_terminal(jobs_db, job_id)

        # A replacement is the sole consumer only after old execution ownership
        # is safe: no old-incarnation group may be alive, and the replacement
        # genuinely consumes the queue.
        assert not worker_mod.group_has_members(child_pgid)
        replacement = _spawn_real_worker(db_conf, worker_id="replacement-worker")
        try:
            ready = lifecycle.verify_worker_consumes_queue(
                "replacement-worker", str(tmp_path), replacement.pid, 15.0
            )
            assert ready
        finally:
            if replacement.poll() is None:
                lifecycle.stop_worker(_meta_for_live(replacement, tmp_path, "repl-token"), 5.0)
    finally:
        if worker.poll() is None:
            lifecycle.stop_worker(_meta_for_live(worker, tmp_path, str(incarnation or "x")), 5.0)
        _recover_incarnation(jobs_db, incarnation)


@pytest.fixture(autouse=True)
def _issue75_leak_proof() -> Iterator[None]:
    """Deterministically reap every test-owned worker and child group.

    Every #75 focused test spawns a scripted worker (and, transitively, a
    SIGTERM-ignoring command group) that must never reparent to PID 1. Even on
    an assertion/failure path this fixture kills any surviving process whose
    command line names our scripted worker or the exact child marker, by exact
    process group, then asserts zero survivors remain.
    """
    yield
    _kill_issue75_orphans()
    _assert_no_issue75_processes()


ISSUE75_WORKER_MARKER: Final = "scripted_worker.py"
ISSUE75_CHILD_MARKER: Final = "lubko_issue75_child"


def _issue75_survivors() -> list[tuple[int, int]]:
    """Return ``(pid, pgid)`` of any still-live issue75 test process.

    Returns:
        Live processes whose command line names the scripted worker or the
        exact command-group child marker.
    """
    survivors: list[tuple[int, int]] = []
    proc_dir = Path("/proc")
    if proc_dir.is_dir():
        for entry in proc_dir.iterdir():
            if not entry.name.isdigit():
                continue
            try:
                cmdline = (entry / "cmdline").read_bytes().replace(b"\0", b" ")
            except OSError:
                continue
            text = cmdline.decode("utf-8", "replace")
            if ISSUE75_WORKER_MARKER in text or ISSUE75_CHILD_MARKER in text:
                try:
                    pid = int(entry.name)
                    pgid = os.getpgid(pid)
                except (ProcessLookupError, OSError):
                    continue
                survivors.append((pid, pgid))
    return survivors


def _kill_issue75_orphans() -> None:
    """SIGKILL any surviving issue75 test process by exact process group."""
    for _pid, pgid in _issue75_survivors():
        with suppress(ProcessLookupError, OSError):
            os.killpg(pgid, signal.SIGKILL)


def _assert_no_issue75_processes() -> None:
    """Assert no issue75 test process survives; kill then re-check if needed.

    Raises:
        AssertionError: If any issue75 test process is still running after a
            kill-and-recheck cycle.
    """
    survivors = _issue75_survivors()
    if survivors:
        _kill_issue75_orphans()
        time.sleep(0.2)
        survivors = _issue75_survivors()
    if survivors:
        msg = "issue75 test processes still alive after focused tests: " + ", ".join(
            f"pid={pid}/pgid={pgid}" for pid, pgid in survivors
        )
        raise AssertionError(msg)


def test_stop_worker_rejects_live_unowned_wrong_token(tmp_path: Path) -> None:
    """stop_worker must not signal or report success for a live unowned worker.

    A live process may match the recorded identity (PID/PGID/SID/start ticks)
    yet carry a lifecycle token that is not ours (or none). Such a live,
    unowned process must never be signalled, and stop_worker must report
    failure rather than claiming retirement -- so authority is never handed off.
    """
    token = "issue75-wrong-token-ctx"  # ruff: ignore[hardcoded-password-string]
    # The process environment carries a DIFFERENT token, so it is live but not
    # owned by the recorded incarnation.
    proc = _spawn_scripted_worker(
        tmp_path,
        token,
        1.0,
        env_token="issue75-other-incarnation",  # ruff: ignore[hardcoded-password-func-arg]
    )
    meta = _meta_for_live(proc, tmp_path, token)
    try:
        assert lifecycle.process_identity(proc.pid) is not None
        assert lifecycle.process_has_token(proc.pid, token) is False
        stopped = lifecycle.stop_worker(meta, 5.0, cancel_grace_seconds=1.0)
        assert stopped is False
        # The worker must not have been signalled: still alive, command group
        # intact.
        assert proc.poll() is None
        assert worker_mod.group_has_members(os.getpgid(proc.pid))
    finally:
        # Deterministic reap so teardown sees nothing live; the worker's own
        # drain handler terminates its child group.
        with suppress(ProcessLookupError, OSError):
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        try:
            proc.wait(timeout=5)
        except Exception:  # ruff: ignore[blind-except]
            with suppress(ProcessLookupError, OSError):
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            with suppress(Exception):
                proc.wait(timeout=5)
        guard.unregister(proc)


def test_recover_owned_groups_db_failure_blocks(monkeypatch: pytest.MonkeyPatch) -> None:
    """Recovery DB/config failure is a durable blocking obligation.

    When the database configuration is missing the supervisor's owned-group
    recovery must raise :class:`OwnedGroupRecoveryError` rather than silently
    skipping, so the retired child and sole-consumer authority are preserved
    and no replacement is spawned alongside stale groups.
    """
    monkeypatch.setattr(
        "lubko.supervisor.load_database_config",
        lambda: (_ for _ in ()).throw(ValueError("no database configuration available")),
    )
    with pytest.raises(OwnedGroupRecoveryError):
        SupervisorDaemon._recover_owned_groups(  # ruff: ignore[private-member-access]
            "issue75-blocked-incarnation"
        )


def test_recover_owned_groups_survivor_blocks(monkeypatch: pytest.MonkeyPatch) -> None:
    """A surviving verified-ours group keeps recovery a blocking obligation.

    When emergency recovery acts on a group it proved is ours but the group
    cannot be reaped within the cancel grace, the supervisor must raise
    :class:`OwnedGroupRecoveryError` rather than reporting success. The caller
    then preserves the retired child and does not spawn a replacement alongside
    a still-live side-effecting group.
    """
    surviving_pgid = 424242
    monkeypatch.setattr(
        worker_mod,
        "recover_owned_job_groups",
        lambda *_, **__: worker_mod.ReclaimedGroups(
            reaped=[], surviving=[surviving_pgid], unresolved=[]
        ),
    )
    fake_db = MagicMock()
    fake_db.conninfo.return_value = ""
    monkeypatch.setattr("lubko.supervisor.load_database_config", lambda: fake_db)
    monkeypatch.setattr("lubko.supervisor.psycopg.connect", lambda *_, **__: MagicMock())
    with pytest.raises(OwnedGroupRecoveryError):
        SupervisorDaemon._recover_owned_groups(  # ruff: ignore[private-member-access]
            "issue75-survivor-incarnation"
        )


def test_recover_owned_job_groups_reports_survivors(monkeypatch: pytest.MonkeyPatch) -> None:
    """An unreapable verified-ours group is reported as surviving, never hidden.

    When a command group that recovery verified is ours (matching start-time
    ticks, live members) cannot be reaped, the recovery pass must report it as
    ``surviving`` so the orchestrator blocks clearing the retired worker's
    authority. The unreapable outcome is injected through the termination seam
    (``_terminate_one_group`` returning ``False``) so the test is deterministic
    and does not race the kernel's SIGKILL→reap window.
    """
    owned = subprocess.Popen(
        [
            "/bin/sh",
            "-c",
            'trap "" TERM; exec ' + sys.executable + ' -c "import signal,time; '
            'signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(30)"',
        ],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        close_fds=True,
    )
    guard.register(owned)
    owned_pgid = os.getpgid(owned.pid)
    monkeypatch.setattr(worker_mod, "_terminate_one_group", lambda *_, **__: False)
    try:
        conn = _FakeConn([(uuid4(), str(owned_pgid), str(lifecycle.proc_start_ticks(owned.pid)))])
        result = worker_mod.recover_owned_job_groups(conn, "issue75-survivor", 0.5)  # type: ignore[arg-type]
        assert result.surviving == [owned_pgid]
        assert result.reaped == []
        assert result.unresolved == []
    finally:
        with suppress(ProcessLookupError, OSError):
            os.killpg(owned_pgid, signal.SIGKILL)
        with suppress(Exception):
            owned.wait(timeout=5)
        guard.unregister(owned)


def test_recover_owned_job_groups_missing_ticks_live_unrelated_unresolved() -> None:
    """Legacy/missing ticks over a live unrelated group: no signal, blocks.

    A persisted row whose ``process_start_time_ticks`` is missing (a legacy row
    that never recorded them) but whose ``process_pgid`` points at a still-live
    unrelated process must NOT be signalled. It is nonetheless reported as
    ``unresolved`` so the orchestrator treats it as a durable blocking
    obligation and holds rather than clearing authority or starting a
    replacement.
    """
    unrelated = subprocess.Popen(
        [SLEEP_BIN, "300"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        close_fds=True,
    )
    guard.register(unrelated)
    unrelated_pgid = os.getpgid(unrelated.pid)
    try:
        # Ticks column is ``None`` (missing/legacy) while the group is alive.
        conn = _FakeConn([(uuid4(), str(unrelated_pgid), None)])
        result = worker_mod.recover_owned_job_groups(conn, "issue75-missing-ticks")  # type: ignore[arg-type]
        assert result.unresolved == [unrelated_pgid]
        assert result.reaped == []
        assert result.surviving == []
        # The unrelated process is completely untouched.
        assert worker_mod.group_has_members(unrelated_pgid)
        assert unrelated.poll() is None
    finally:
        if unrelated.poll() is None:
            with suppress(ProcessLookupError):
                os.killpg(unrelated_pgid, signal.SIGKILL)
        with suppress(Exception):
            unrelated.wait(timeout=5)
        guard.unregister(unrelated)


def test_recover_owned_job_groups_mismatched_ticks_live_group_unresolved() -> None:
    """Mismatched persisted ticks over a live group: no signal, blocks.

    A persisted row whose ``process_pgid`` points at a live group but whose
    ``process_start_time_ticks`` does not match the live leader's ticks (the id
    was recycled by an unrelated process) must NOT be signalled. It is reported
    as ``unresolved`` so the orchestrator holds rather than risk killing a
    stranger-owned group.
    """
    unrelated = subprocess.Popen(
        [SLEEP_BIN, "300"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        close_fds=True,
    )
    guard.register(unrelated)
    unrelated_pgid = os.getpgid(unrelated.pid)
    # Ticks that are valid/positive but do not match the live leader.
    wrong_ticks = (lifecycle.proc_start_ticks(unrelated.pid) or 0) + 1
    try:
        conn = _FakeConn([(uuid4(), str(unrelated_pgid), str(wrong_ticks))])
        result = worker_mod.recover_owned_job_groups(conn, "issue75-mismatch-ticks")  # type: ignore[arg-type]
        assert result.unresolved == [unrelated_pgid]
        assert result.reaped == []
        assert result.surviving == []
        assert worker_mod.group_has_members(unrelated_pgid)
        assert unrelated.poll() is None
    finally:
        if unrelated.poll() is None:
            with suppress(ProcessLookupError):
                os.killpg(unrelated_pgid, signal.SIGKILL)
        with suppress(Exception):
            unrelated.wait(timeout=5)
        guard.unregister(unrelated)


def test_recover_owned_job_groups_missing_ticks_no_members_converges() -> None:
    """Missing ticks over a gone group converges safely (no signal, no block).

    When the persisted group id has no live members the recovery has already
    converged: even with missing/malformed ticks the group is not signalled and
    is not reported as an unresolved blocking obligation.
    """
    dead = subprocess.Popen(
        [SLEEP_BIN, "0.01"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        close_fds=True,
    )
    guard.register(dead)
    dead_pgid = os.getpgid(dead.pid)
    dead.wait(timeout=5)
    guard.unregister(dead)
    # After the process exits the group has no live members.
    assert not worker_mod.group_has_members(dead_pgid)
    conn = _FakeConn([(uuid4(), str(dead_pgid), None)])
    result = worker_mod.recover_owned_job_groups(conn, "issue75-gone-missing-ticks")  # type: ignore[arg-type]
    assert result.reaped == []
    assert result.surviving == []
    assert result.unresolved == []


def test_recover_owned_groups_unresolved_blocks(monkeypatch: pytest.MonkeyPatch) -> None:
    """Unresolved (unverifiable) groups keep recovery a blocking obligation.

    When emergency recovery reports a live group whose exact identity could not
    be proven (missing/mismatched ticks) it must raise
    :class:`OwnedGroupRecoveryError` so the orchestrator preserves the retired
    child and does not spawn a replacement alongside a possibly stranger-owned
    group.
    """
    unresolved_pgid = 515151
    monkeypatch.setattr(
        worker_mod,
        "recover_owned_job_groups",
        lambda *_, **__: worker_mod.ReclaimedGroups(
            reaped=[], surviving=[], unresolved=[unresolved_pgid]
        ),
    )
    fake_db = MagicMock()
    fake_db.conninfo.return_value = ""
    monkeypatch.setattr("lubko.supervisor.load_database_config", lambda: fake_db)
    monkeypatch.setattr("lubko.supervisor.psycopg.connect", lambda *_, **__: MagicMock())
    with pytest.raises(OwnedGroupRecoveryError):
        SupervisorDaemon._recover_owned_groups(  # ruff: ignore[private-member-access]
            "issue75-unresolved-incarnation"
        )


def _recover_incarnation(conninfo: str, incarnation: object) -> None:
    """Recover any surviving command group owned by an incarnation.

    Args:
        conninfo: PostgreSQL connection string.
        incarnation: Worker incarnation whose groups to recover.
    """
    if not incarnation:
        return

    with connect(conninfo) as conn:
        worker_mod.recover_owned_job_groups(conn, str(incarnation), 0.5)
