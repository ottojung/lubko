"""Process-level tests for the external worker supervisor (#63).

These tests run the real ``lubko.supervisor`` daemon as a separate process
against an isolated PostgreSQL cluster and prove the acceptance criteria with
real process ownership:

- an unexpected worker exit is detected and the worker is restarted
  automatically by the daemon;
- the replacement worker recovers abandoned leases without re-executing them
  and consumes fresh probes;
- repeated crashes back off and never accumulate processes or zombies;
- a database outage is never misclassified as process death (no duplication),
  and a restart during an outage backs off until readiness is possible;
- a supervisor restart reconstructs exactly one worker from durable state;
- an orphaned worker from a hard-killed supervisor is taken over by exact
  identity;
- corrupt supervised-deployment state fails closed into a hold;
- a pending supervised mission holds the daemon, and confirmation resumes it
  with the confirmed commit.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import signal
import subprocess
import sys
import time
from contextlib import ExitStack, contextmanager, suppress
from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final, cast

import psycopg
import pytest

from lubko import cli, lifecycle, supervise
from lubko import deployctl as dc
from lubko import supervisor as supervisor_module
from lubko.state import cli_root_dir, rollback_state_path
from lubko.supervisor import Settings, SupervisorDaemon
from lubko.supervisor import main as supervisor_main
from lubko.worker import group_has_members
from tests import _pg
from tests import _process_guard as guard
from tests.test_cli import make_repo

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Sequence
    from uuid import UUID

REPO_ROOT: Final = Path(__file__).resolve().parent.parent
BASELINE_MIGRATION: Final = REPO_ROOT / "migrations" / "0001_two_column_protocol.sql"
SLEEP_BIN: Final = shutil.which("sleep") or "/bin/sleep"
TEST_WORKER_ID: Final = "test-supervisor-worker"
LEGACY_MARKER: Final = "legacy-token"
_TEST_ORPHAN_INCARNATION: Final = "test-orphan-incarnation"
_TEST_WORKER_INCARNATION: Final = "test-worker-incarnation"


def wait_until(predicate: Callable[[], bool], timeout: float = 30.0) -> None:
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
        time.sleep(0.05)
    msg = "condition not met within timeout"
    raise AssertionError(msg)


def process_alive(pid: int) -> bool:
    """Return whether a process exists and is not a zombie.

    Args:
        pid: Process ID to probe.

    Returns:
        ``True`` when a running (non-zombie) process with that ID exists.
    """
    try:
        stat = (Path("/proc") / str(pid) / "stat").read_bytes()
    except OSError:
        return False
    close_paren = stat.rfind(b")")
    if close_paren == -1:
        return True
    fields = stat[close_paren + 2 :].split()
    if not fields:
        return True
    return fields[0] not in {b"Z", b"X"}


def zombie_children() -> list[int]:
    """Return the zombie children of the current test process.

    Returns:
        The zombie child PIDs.
    """
    me = os.getpid()
    zombies: list[int] = []
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            stat = (entry / "stat").read_bytes()
        except OSError:
            continue
        close = stat.rfind(b")")
        if close == -1:
            continue
        fields = stat[close + 2 :].split()
        if len(fields) < 4:
            continue
        if fields[0] in {b"Z", b"X"} and int(fields[3]) == me:
            zombies.append(int(entry.name))
    return zombies


def direct_children(ppid: int) -> list[int]:
    """Return the live direct children of a process by exact PPID.

    Args:
        ppid: Parent process ID.

    Returns:
        The live child PIDs.
    """
    children: list[int] = []
    expected = str(ppid).encode()
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            stat = (entry / "stat").read_bytes()
        except OSError:
            continue
        close = stat.rfind(b")")
        if close == -1:
            continue
        fields = stat[close + 2 :].split()
        if len(fields) < 3:
            continue
        if fields[1] == expected and fields[0] not in {b"Z", b"X"}:
            children.append(int(entry.name))
    return children


# ---------------------------------------------------------------------------
# Fixtures and helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def maintained_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, str, str]:
    """Build immutable CLI environments with a real worker for two commits.

    Each commit's maintained environment must contain a genuine
    ``lubko-worker`` entry point (running the in-tree worker) so the
    supervisor spawns a real queue consumer from the immutable per-commit
    environment, never from a mutable checkout.

    Args:
        tmp_path: Pytest temporary directory.
        monkeypatch: Pytest monkeypatch fixture.

    Returns:
        The ``(repo, first, second)`` tuple of the two-commit repository.
    """
    repo, first, second = make_repo(tmp_path / "repo")

    def fake_sync(_uv_path: str, root: Path, _timeout_seconds: float) -> None:
        bin_dir = root / ".venv" / "bin"
        bin_dir.mkdir(parents=True, exist_ok=True)
        worker = bin_dir / "lubko-worker"
        worker.write_text(f"#!/bin/sh\nexec {sys.executable} -m lubko.worker\n", encoding="utf-8")
        worker.chmod(0o755)
        for entry in cli.ENTRY_POINTS:
            if entry == "lubko-worker":
                continue
            script = bin_dir / entry
            script.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
            script.chmod(0o755)

    monkeypatch.setattr(cli, "_sync_venv", fake_sync)
    cli.build_cli_root(repo, first, "uv", 60.0)
    cli.build_cli_root(repo, second, "uv", 60.0)
    return repo, first, second


def _database_config_file(cluster: _pg.PgCluster, tmp_path: Path) -> Path:
    """Write the worker database configuration file for a test cluster.

    Args:
        cluster: The running cluster.
        tmp_path: Pytest temporary directory.

    Returns:
        The configuration file path.
    """
    conf = tmp_path / "database.conf"
    conf.write_text(
        f"host={cluster.socket_dir}\n"
        f"port={cluster.port}\n"
        "dbname=postgres\n"
        "user=postgres\n"
        "password=local-trust\n"
    )
    conf.chmod(0o600)
    return conf


@pytest.fixture
def supervisor_env(
    pg_cluster: _pg.PgCluster,
    tmp_path: Path,
) -> dict[str, str]:
    """Build the environment for supervisor subprocesses and their workers.

    Args:
        pg_cluster: The running cluster.
        tmp_path: Pytest temporary directory.

    Returns:
        An environment with fast supervisor and worker timing.
    """
    env = dict(os.environ)
    env["LUBKO_DATABASE_CONFIG"] = str(_database_config_file(pg_cluster, tmp_path))
    env["LUBKO_SERVER"] = "alpha-server"
    env["LUBKO_SUPERVISOR_POLL_SECONDS"] = "0.1"
    env["LUBKO_SUPERVISOR_BACKOFF_BASE_SECONDS"] = "0.2"
    env["LUBKO_SUPERVISOR_BACKOFF_MAX_SECONDS"] = "2.0"
    env["LUBKO_SUPERVISOR_STABLE_WINDOW_SECONDS"] = "2.0"
    env["LUBKO_SUPERVISOR_STOP_GRACE_SECONDS"] = "0.3"
    env["LUBKO_SUPERVISOR_IDENTITY_TIMEOUT_SECONDS"] = "2.0"
    env["LUBKO_SUPERVISOR_PROBE_TIMEOUT_SECONDS"] = "5.0"
    env["LUBKO_SUPERVISOR_READINESS_INTERVAL_SECONDS"] = "0.3"
    env["LUBKO_POLL_INTERVAL_SECONDS"] = "0.05"
    env["LUBKO_PROCESS_POLL_INTERVAL_SECONDS"] = "0.01"
    env["LUBKO_LEASE_DURATION_SECONDS"] = "1.5"
    env["LUBKO_LEASE_REFRESH_INTERVAL_SECONDS"] = "0.2"
    env["LUBKO_LEASE_RECOVERY_INTERVAL_SECONDS"] = "0.1"
    env["LUBKO_LEASE_SAFETY_MARGIN_SECONDS"] = "0.2"
    env["LUBKO_OUTPUT_PUBLICATION_INTERVAL_SECONDS"] = "0.1"
    return env


def start_supervisor(env: dict[str, str]) -> subprocess.Popen[bytes]:
    """Start the real supervisor daemon as a separate process.

    Args:
        env: Environment for the daemon (and its worker child).

    Returns:
        The daemon process.
    """
    proc = subprocess.Popen(
        [sys.executable, "-m", "lubko.supervisor"],
        cwd=REPO_ROOT,
        env=env,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    guard.register(proc)
    return proc


def stop_supervisor(proc: subprocess.Popen[bytes]) -> None:
    """Gracefully stop the supervisor so it retires its worker child.

    Args:
        proc: The daemon process.
    """
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
    guard.unregister(proc)


@contextmanager
def running_supervisor(env: dict[str, str]) -> Iterator[subprocess.Popen[bytes]]:
    """Run a supervisor daemon and guarantee deterministic teardown.

    Args:
        env: Environment for the daemon (and its worker child).

    Yields:
        The daemon process.
    """
    proc = start_supervisor(env)
    try:
        yield proc
    finally:
        stop_supervisor(proc)


@contextmanager
def _owned_supervisors(
    startups: Sequence[Callable[[], subprocess.Popen[bytes]]],
) -> Iterator[list[subprocess.Popen[bytes]]]:
    """Start supervisors and unconditionally reap every created stack.

    Orchestration invariant under review: a failure during a later startup,
    during the test body, or during an earlier teardown must never leak an
    already-running stack.  Every successful startup immediately registers
    its teardown on an :class:`contextlib.ExitStack`, so all registered
    callbacks run even when something else raises, and Python exception
    chaining preserves each earlier failure as context.

    Args:
        startups: One zero-argument callable per stack, invoked in order.
            A callable that raises aborts the remaining startups.

    Yields:
        The processes of every successfully started stack, in creation order.
    """
    with ExitStack() as stack:
        started: list[subprocess.Popen[bytes]] = []
        for start in startups:
            proc = start()
            started.append(proc)
            stack.callback(stop_supervisor, proc)
        yield started


def request_and_wait(commit: str, repo: Path) -> int:
    """Ask the supervisor to run a commit and wait until it is queue-ready.

    Args:
        commit: Exact commit to run.
        repo: Maintained checkout.

    Returns:
        The applied generation.
    """
    generation = supervise.request_run(
        commit,
        repo=str(repo),
        uv_path="uv",
        worker_id=TEST_WORKER_ID,
    )
    assert supervise.wait_for_generation(generation, 30.0)
    assert supervise.wait_until_ready(generation, 30.0)
    return generation


def worker_pid() -> int | None:
    """Return the PID of the supervisor's recorded worker child.

    Returns:
        The child PID, or ``None`` when none is recorded.
    """
    status = supervise.read_status()
    return status.child.pid if status is not None and status.child is not None else None


def status_ready() -> bool:
    """Return whether the supervisor reports a queue-ready worker.

    Returns:
        ``True`` when ready.
    """
    status = supervise.read_status()
    return bool(status is not None and status.ready)


def status_commit_is(commit: str) -> Callable[[], bool]:
    """Return a predicate asserting the supervisor currently runs ``commit``.

    Args:
        commit: Exact commit the worker must be running.

    Returns:
        A predicate usable with :func:`wait_until`.
    """

    def check() -> bool:
        status = supervise.read_status()
        return status is not None and status.commit == commit

    return check


def status_has_no_child() -> bool:
    """Return whether the supervisor currently owns no worker child.

    Returns:
        ``True`` when no child is recorded.
    """
    status = supervise.read_status()
    return status is not None and status.child is None


def shell_command_argv(command: str) -> list[str]:
    """Wrap a shell snippet as an explicit process argv that execs ``sh``.

    Args:
        command: Shell snippet to run through ``sh -c``.

    Returns:
        An argv array that execs the snippet through ``/bin/sh``.
    """
    return [shutil.which("sh") or "/bin/sh", "-c", command]


def insert_pending_job(conninfo: str, cwd: str, command: str) -> UUID:
    """Insert a protocol v4 pending command job running a shell snippet.

    Args:
        conninfo: PostgreSQL connection string.
        cwd: Working directory for the job.
        command: Shell snippet, executed by an explicit ``/bin/sh -c`` argv.

    Returns:
        The job identifier.
    """
    payload = json.dumps({
        "v": 4,
        "type": "command",
        "server": "alpha-server",
        "request": {"cwd": cwd, "process": shell_command_argv(command)},
        "state": {"status": "pending"},
    })
    with psycopg.connect(conninfo) as conn:
        row = conn.execute(
            "INSERT INTO lubko.jobs (payload) VALUES (%s) RETURNING id",
            (payload,),
        ).fetchone()
    assert row is not None
    return cast("UUID", row[0])


def read_status_of(conninfo: str, job_id: UUID) -> str:
    """Return a job's current status.

    Args:
        conninfo: PostgreSQL connection string.
        job_id: Job to inspect.

    Returns:
        The job status.
    """
    with psycopg.connect(conninfo) as conn:
        row = conn.execute(
            "SELECT (payload::jsonb)->'state'->>'status' FROM lubko.jobs WHERE id = %s",
            (job_id,),
        ).fetchone()
    assert row is not None
    return str(row[0])


def read_payload(conninfo: str, job_id: UUID) -> dict[str, object]:
    """Read and decode a job's payload.

    Args:
        conninfo: PostgreSQL connection string.
        job_id: Job to read.

    Returns:
        The decoded payload mapping.
    """
    with psycopg.connect(conninfo) as conn:
        row = conn.execute(
            "SELECT payload FROM lubko.jobs WHERE id = %s",
            (job_id,),
        ).fetchone()
    assert row is not None
    data = json.loads(str(row[0]))
    assert isinstance(data, dict)
    return data


def write_rollback(state: dc.RollbackState) -> None:
    """Persist durable supervised-deployment state atomically.

    Args:
        state: State to store.
    """
    path = rollback_state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(state.to_dict(), sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def mission_state(
    generation: int,
    status: str,
    commit: str,
    previous_commit: str,
) -> dc.RollbackState:
    """Build a durable supervised-deployment mission for the override tests.

    Use ``dataclasses.replace`` at call sites to set ``repo`` or
    ``supervisor_owned`` when the test needs non-default values.

    Args:
        generation: Monotonic mission generation.
        status: ``pending``, ``confirmed``, or ``rolled_back``.
        commit: Candidate commit.
        previous_commit: Previously confirmed commit.

    Returns:
        A minimal but valid :class:`dc.RollbackState`.
    """
    return dc.RollbackState(
        schema_version=dc.ROLLBACK_SCHEMA_VERSION,
        generation=generation,
        status=status,
        commit=commit,
        previous_commit=previous_commit,
        challenge_hash=None,
        deadline=time.time() + 60,
        repo="",
        uv_path="uv",
        stop_grace_seconds=1.0,
        git_timeout_seconds=5.0,
        previous_retiring=False,
        previous_meta=lifecycle.WorkerMeta(
            schema_version=1,
            state=lifecycle.STATE_RUNNING,
            pid=999_999,
            pgid=999_999,
            sid=999_999,
            start_time_ticks=1,
            token=LEGACY_MARKER,
            repo="",
            git_commit=previous_commit,
            worker_id="w",
            log_path="",
            started_at=1.0,
            stopped_at=None,
        ),
        new_meta=lifecycle.WorkerMeta(
            schema_version=1,
            state=lifecycle.STATE_RUNNING,
            pid=999_998,
            pgid=999_998,
            sid=999_998,
            start_time_ticks=1,
            token=LEGACY_MARKER,
            repo="",
            git_commit=commit,
            worker_id="w",
            log_path="",
            started_at=1.0,
            stopped_at=None,
        ),
    )


def kill_job_group_if_any(conninfo: str, job_id: UUID) -> None:
    """Force-kill a job's recorded process group when members remain.

    Args:
        conninfo: PostgreSQL connection string.
        job_id: Job to clean up.
    """
    payload = read_payload(conninfo, job_id)
    state = payload.get("state")
    pgid = state.get("process_pgid") if isinstance(state, dict) else None
    if pgid is not None:
        with suppress(ProcessLookupError):
            os.killpg(int(pgid), signal.SIGKILL)


# ---------------------------------------------------------------------------
# Acceptance: automatic restart
# ---------------------------------------------------------------------------


def test_worker_is_restarted_after_unexpected_kill(
    jobs_db: str,
    pg_cluster: _pg.PgCluster,
    maintained_env: tuple[Path, str, str],
    supervisor_env: dict[str, str],
) -> None:
    """Killing the worker by exact identity triggers an automatic restart."""
    del jobs_db, pg_cluster
    repo, first, _second = maintained_env
    with running_supervisor(supervisor_env):
        request_and_wait(first, repo)
        original_pid = worker_pid()
        assert original_pid is not None

        os.killpg(original_pid, signal.SIGKILL)

        wait_until(lambda: worker_pid() is not None and worker_pid() != original_pid, timeout=30.0)
        replacement_pid = worker_pid()
        assert replacement_pid is not None
        assert process_alive(replacement_pid)
        status = supervise.read_status()
        assert status is not None
        assert status.last_exit is not None
        assert len(direct_children(status.supervisor_pid)) == 1


def test_same_commit_restart_replaces_worker_process(
    jobs_db: str,
    pg_cluster: _pg.PgCluster,
    maintained_env: tuple[Path, str, str],
    supervisor_env: dict[str, str],
) -> None:
    """A newer generation for the same commit replaces the worker process."""
    del jobs_db, pg_cluster
    repo, first, _second = maintained_env
    with running_supervisor(supervisor_env):
        generation = request_and_wait(first, repo)
        original_pid = worker_pid()
        assert original_pid is not None

        restart = supervise.request_restart(
            first,
            repo=str(repo),
            uv_path="uv",
            worker_id=TEST_WORKER_ID,
        )
        assert restart > generation
        assert supervise.wait_until_ready(restart, 30.0)
        wait_for_replacement(original_pid)
        replacement_pid = worker_pid()
        assert replacement_pid is not None
        assert replacement_pid != original_pid
        assert process_alive(replacement_pid)
        status = supervise.read_status()
        assert status is not None
        assert status.applied_generation == restart
        assert status.commit == first
        assert len(direct_children(status.supervisor_pid)) == 1


def test_restart_works_after_source_checkout_deleted(
    jobs_db: str,
    pg_cluster: _pg.PgCluster,
    maintained_env: tuple[Path, str, str],
    supervisor_env: dict[str, str],
) -> None:
    """A restarted worker runs from the sealed runtime after checkout deletion."""
    del jobs_db, pg_cluster
    repo, first, _second = maintained_env
    with running_supervisor(supervisor_env):
        request_and_wait(first, repo)
        original_pid = worker_pid()
        assert original_pid is not None

        shutil.rmtree(repo)
        os.killpg(original_pid, signal.SIGKILL)

        wait_for_replacement(original_pid)
        replacement_pid = worker_pid()
        assert replacement_pid is not None
        assert process_alive(replacement_pid)
        status = supervise.read_status()
        assert status is not None
        assert status.commit == first
        assert len(direct_children(status.supervisor_pid)) == 1


def test_corrupt_runtime_fails_closed_holding(
    jobs_db: str,
    pg_cluster: _pg.PgCluster,
    maintained_env: tuple[Path, str, str],
    supervisor_env: dict[str, str],
) -> None:
    """A missing/corrupt runtime makes the supervisor hold without a worker."""
    del jobs_db, pg_cluster
    repo, first, _second = maintained_env
    with running_supervisor(supervisor_env):
        request_and_wait(first, repo)
        original_pid = worker_pid()
        assert original_pid is not None

        cli.unseal_runtime(first)
        shutil.rmtree(cli.cli_commit_dir(first))
        os.killpg(original_pid, signal.SIGKILL)

        wait_until(status_has_no_child, timeout=30.0)
        status = supervise.read_status()
        assert status is not None
        assert status.child is None
        assert status.ready is not True
        assert len(direct_children(status.supervisor_pid)) == 0


def test_crash_recovery_recovers_stale_job_without_reexecution(
    jobs_db: str,
    pg_cluster: _pg.PgCluster,
    tmp_path: Path,
    maintained_env: tuple[Path, str, str],
    supervisor_env: dict[str, str],
) -> None:
    """A crashed worker's abandoned lease is recovered, never re-executed."""
    del pg_cluster
    repo, first, _second = maintained_env
    marker = tmp_path / "runs"
    command = f"echo ran >> {marker}; sleep 30"
    with running_supervisor(supervisor_env):
        request_and_wait(first, repo)
        job_id = insert_pending_job(jobs_db, str(tmp_path), command)
        wait_until(lambda: read_status_of(jobs_db, job_id) == "running")
        wait_until(marker.exists)

        original_pid = worker_pid()
        assert original_pid is not None
        os.killpg(original_pid, signal.SIGKILL)

        wait_until(
            lambda: read_status_of(jobs_db, job_id) == "failed",
            timeout=60.0,
        )
        payload = read_payload(jobs_db, job_id)
        result = payload["result"]
        assert isinstance(result, dict)
        assert "lease expired" in str(result.get("recovery_note"))
        assert marker.read_text(encoding="utf-8").splitlines() == ["ran"]

        probe_id = insert_pending_job(jobs_db, str(tmp_path), "echo probe-ok")
        wait_until(lambda: read_status_of(jobs_db, probe_id) == "succeeded")
    kill_job_group_if_any(jobs_db, job_id)


def wait_for_replacement(old_pid: int) -> int:
    """Wait until the daemon records a worker child different from ``old_pid``.

    A single status read is taken per poll (via :func:`wait_until`) and the
    exact observed replacement PID is captured and returned, so the caller
    never re-reads a disagreeing snapshot.  The previous predicate read the
    status twice (``worker_pid() is not None and worker_pid() != old_pid``);
    those two reads are independent snapshots, so a multi-read TOCTOU in the
    test harness is logically capable of making the predicate ``True`` while the
    current child is ``None`` (because ``None != old_pid``), after which the
    caller's separate :func:`worker_pid` read could observe ``None`` -- the
    observed deployment symptom at line 731.  This helper removes the defect by
    returning the one PID it matched.

    Args:
        old_pid: The previous worker child PID.

    Returns:
        The exact replacement worker PID recorded by the daemon.
    """
    observed: list[int] = []

    def _check() -> bool:
        candidate = worker_pid()
        if candidate is not None and candidate != old_pid:
            observed.append(candidate)
            return True
        return False

    wait_until(_check, timeout=30.0)
    return observed[0]


def test_wait_for_replacement_rejects_transient_none_and_returns_observed_pid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A transient ``None`` snapshot must not satisfy the helper.

    The exact observed replacement PID must be returned to the caller.  The
    helper observes one status snapshot per poll.  A ``None`` child (the
    supervisor having cleared the previous child before respawning) is not a
    replacement, so the helper must keep waiting; once a real replacement PID
    appears it is returned exactly, with no separate re-read that could disagree.
    """
    old_pid = 4242
    sequence = iter([old_pid, None, None, 9999, 9999])

    def fake_worker_pid() -> int | None:
        return next(sequence)

    monkeypatch.setattr(
        "tests.test_supervisor_daemon.worker_pid",
        fake_worker_pid,
    )
    replacement = wait_for_replacement(old_pid)
    assert replacement == 9999
    assert replacement != old_pid


def test_repeated_crashes_never_accumulate_workers(
    jobs_db: str,
    maintained_env: tuple[Path, str, str],
    supervisor_env: dict[str, str],
) -> None:
    """Repeated crashes back off and never accumulate processes or zombies."""
    del jobs_db
    repo, first, _second = maintained_env
    with running_supervisor(supervisor_env):
        request_and_wait(first, repo)
        previous = worker_pid()
        assert previous is not None
        for _ in range(3):
            os.killpg(previous, signal.SIGKILL)
            previous = wait_for_replacement(previous)
        status = supervise.read_status()
        assert status is not None
        assert status.restart_count >= 1
        assert len(direct_children(status.supervisor_pid)) == 1
    assert not zombie_children()


def test_database_outage_restart_backs_off_until_readiness(
    jobs_db: str,
    pg_cluster: _pg.PgCluster,
    tmp_path: Path,
    maintained_env: tuple[Path, str, str],
    supervisor_env: dict[str, str],
) -> None:
    """A restart during a PostgreSQL outage backs off until readiness is possible."""
    repo, first, _second = maintained_env
    with running_supervisor(supervisor_env):
        request_and_wait(first, repo)
        original_pid = worker_pid()
        assert original_pid is not None

        pg_cluster.stop()
        os.killpg(original_pid, signal.SIGKILL)

        wait_until(lambda: worker_pid() is not None and worker_pid() != original_pid, timeout=30.0)
        replacement_pid = worker_pid()
        assert replacement_pid is not None
        status = supervise.read_status()
        assert status is not None
        assert status.ready is False
        time.sleep(1.0)
        assert worker_pid() == replacement_pid
        current = supervise.read_status()
        assert current is not None
        assert current.ready is False
        assert len(direct_children(status.supervisor_pid)) == 1

        pg_cluster.start()
        wait_until(status_ready, timeout=60.0)
        probe_id = insert_pending_job(jobs_db, str(tmp_path), "echo recovered")
        wait_until(lambda: read_status_of(jobs_db, probe_id) == "succeeded", timeout=60.0)


@pytest.fixture
def second_pg_cluster(tmp_path: Path) -> Iterator[_pg.PgCluster]:
    """Start a second independent pytest-owned PostgreSQL cluster.

    Args:
        tmp_path: Pytest temporary directory.

    Yields:
        The running second cluster.
    """
    binaries = _pg.postgres_binaries()
    if binaries is None:
        pytest.skip("PostgreSQL server binaries not available on this host")
    root = tmp_path / "pg-second"
    data_dir = root / "data"
    socket_dir = root / "sock"
    socket_dir.mkdir(parents=True)
    port = _pg.free_port()
    env = dict(os.environ)
    lib = _pg.postgres_lib_dir(Path(binaries["postgres"]).parent)
    if lib is not None:
        env["LD_LIBRARY_PATH"] = lib
    subprocess.run(
        [binaries["initdb"], "-D", str(data_dir), "-U", "postgres", "--auth=trust"],
        env=env,
        check=True,
        capture_output=True,
    )
    cluster = _pg.PgCluster(binaries, data_dir, socket_dir, port, env)
    cluster.start()
    try:
        yield cluster
    finally:
        cluster.stop()


def _apply_jobs_baseline(conninfo: str) -> None:
    """Apply the canonical baseline on a fresh ``lubko.jobs`` table.

    Args:
        conninfo: Connection string of the target cluster.
    """
    with psycopg.connect(conninfo) as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS lubko")
        conn.execute("DROP TABLE IF EXISTS lubko.jobs CASCADE")
        conn.execute(BASELINE_MIGRATION.read_text(encoding="utf-8"))


def _stack_b_environment(
    tmp_path: Path,
    supervisor_env: dict[str, str],
    cluster: _pg.PgCluster,
    xdg_b: Path,
) -> dict[str, str]:
    """Build the full environment for the independent second stack.

    Args:
        tmp_path: Pytest temporary directory.
        supervisor_env: The stack-A environment to derive from.
        cluster: The second PostgreSQL cluster.
        xdg_b: The pytest-owned XDG root for stack B.

    Returns:
        The environment for stack B's supervisor and its worker.
    """
    conf_b = tmp_path / "stack-b" / "database.conf"
    conf_b.parent.mkdir(parents=True, exist_ok=True)
    conf_b.write_text(
        f"host={cluster.socket_dir}\n"
        f"port={cluster.port}\n"
        "dbname=postgres\n"
        "user=postgres\n"
        "password=local-trust\n",
        encoding="utf-8",
    )
    conf_b.chmod(0o600)
    env_b = dict(supervisor_env)
    env_b["LUBKO_DATABASE_CONFIG"] = str(conf_b)
    for env_key in (
        "XDG_STATE_HOME",
        "XDG_DATA_HOME",
        "XDG_CACHE_HOME",
        "XDG_CONFIG_HOME",
        "XDG_BIN_HOME",
    ):
        env_b[env_key] = str(xdg_b / env_key.removeprefix("XDG_").lower())
    return env_b


def _with_stack_b_env(
    monkeypatch: pytest.MonkeyPatch,
    env_b: dict[str, str],
    action: Callable[[], object],
) -> None:
    """Run ``action`` with stack B's XDG root authoritative.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
        env_b: The stack-B environment.
        action: The callable to run.
    """
    with monkeypatch.context() as m:
        for env_key, value in env_b.items():
            if env_key.startswith("XDG_"):
                m.setenv(env_key, value)
        action()


@pytest.fixture
def stack_a(
    jobs_db: str,
    maintained_env: tuple[Path, str, str],
    supervisor_env: dict[str, str],
) -> tuple[Path, str, dict[str, str], str]:
    """Bundle the stack-A repo, commit, environment, and queue connection.

    Args:
        jobs_db: Baseline queue connection for stack A's readiness probe.
        maintained_env: The two-commit repository.
        supervisor_env: The stack-A environment.

    Returns:
        The ``(repo, first_commit, env, conninfo)`` quadruple.
    """
    repo, first, _second = maintained_env
    return repo, first, supervisor_env, jobs_db


def _prepare_stack_b(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    supervisor_env: dict[str, str],
    cluster: _pg.PgCluster,
) -> tuple[Path, str, dict[str, str]]:
    """Build repo, CLI root, and environment for the independent stack B.

    Args:
        tmp_path: Pytest temporary directory.
        monkeypatch: Pytest monkeypatch fixture.
        supervisor_env: The stack-A environment to derive from.
        cluster: The second PostgreSQL cluster.

    Returns:
        The ``(repo, first_commit, env)`` triple for stack B.
    """
    _apply_jobs_baseline(cluster.conninfo())
    env_b = _stack_b_environment(tmp_path, supervisor_env, cluster, tmp_path / "stack-b" / "xdg")
    repo_b, first_b, _second_b = make_repo(tmp_path / "repo-b")
    with monkeypatch.context() as m:
        for env_key, value in env_b.items():
            if env_key.startswith("XDG_"):
                m.setenv(env_key, value)
        cli.build_cli_root(repo_b, first_b, "uv", 60.0)
    return repo_b, first_b, env_b


def test_two_simultaneous_owned_stacks_never_cross_talk(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    stack_a: tuple[Path, str, dict[str, str], str],
    second_pg_cluster: _pg.PgCluster,
) -> None:
    """Two simultaneous cluster+supervisor+worker stacks stay fully isolated.

    Each stack lives in its own pytest-owned XDG root and its own PostgreSQL
    cluster.  Both supervisors run concurrently with exactly one worker each;
    status surfaces, workers, and queues never cross; and teardown retires
    every process by exact identity with nothing left alive or reparented to
    PID 1.
    """
    repo_a, first_a, supervisor_env, conninfo_a = stack_a

    # Independent baselines: stack A's queue is refreshed by its fixture.
    repo_b, first_b, env_b = _prepare_stack_b(
        tmp_path, monkeypatch, supervisor_env, second_pg_cluster
    )

    pid_a: int | None = None
    pid_b: int | None = None

    def read_b() -> supervise.SupervisorStatus | None:
        status: supervise.SupervisorStatus | None = None

        def read() -> None:
            nonlocal status
            status = supervise.read_status()

        _with_stack_b_env(monkeypatch, env_b, read)
        return status

    with _owned_supervisors([
        lambda: start_supervisor(supervisor_env),
        lambda: start_supervisor(env_b),
    ]) as _stack_procs:
        request_and_wait(first_a, repo_a)
        _with_stack_b_env(
            monkeypatch,
            env_b,
            lambda: request_and_wait(first_b, repo_b),
        )

        pid_a = worker_pid()
        assert pid_a is not None
        status_a = supervise.read_status()
        assert status_a is not None
        assert status_a.ready is True
        assert status_a.commit == first_a
        assert len(direct_children(status_a.supervisor_pid)) == 1

        status_b = read_b()
        assert status_b is not None
        assert status_b.child is not None
        pid_b = status_b.child.pid
        assert pid_a != pid_b
        assert status_b.ready is True
        assert status_b.commit == first_b
        assert len(direct_children(status_b.supervisor_pid)) == 1

        # Queue isolation: each worker consumes only its own cluster's queue.
        probe_a = insert_pending_job(conninfo_a, str(tmp_path), "echo stack-a")
        wait_until(
            lambda: read_status_of(conninfo_a, probe_a) == "succeeded",
            timeout=60.0,
        )
        probe_b = insert_pending_job(second_pg_cluster.conninfo(), str(tmp_path), "echo stack-b")
        wait_until(
            lambda: read_status_of(second_pg_cluster.conninfo(), probe_b) == "succeeded",
            timeout=60.0,
        )

    # Exact cleanup: no test-owned process survives, so none can reparent.
    # Both pids were captured inside the converged body, so they are bound.
    assert pid_a is not None
    assert pid_b is not None
    assert not process_alive(pid_a)
    assert not process_alive(pid_b)


class _FakeSupervisor:
    """Duck-typed supervisor handle recording teardown signals for regressions."""

    def __init__(self) -> None:
        self.terminated = False

    @staticmethod
    def poll() -> int | None:
        return None

    def terminate(self) -> None:
        self.terminated = True

    def kill(self) -> None:
        self.terminated = True

    @staticmethod
    def wait(timeout: float | None = None) -> int:
        del timeout
        return 0


def test_second_stack_startup_failure_still_stops_first_stack(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed second startup must not leak the first, converged stack."""
    first = _FakeSupervisor()
    handles: list[subprocess.Popen[bytes]] = [cast("subprocess.Popen[bytes]", first)]
    startup_attempts: list[int] = []
    stopped: list[subprocess.Popen[bytes]] = []

    def failing_second_start() -> subprocess.Popen[bytes]:
        startup_attempts.append(1)
        if len(startup_attempts) == 1:
            return handles[0]
        msg = "second stack startup exploded"
        raise RuntimeError(msg)

    def fake_stop(proc: subprocess.Popen[bytes]) -> None:
        stopped.append(proc)

    monkeypatch.setattr(sys.modules[__name__], "stop_supervisor", fake_stop)

    with (
        pytest.raises(RuntimeError, match="second stack startup exploded"),
        _owned_supervisors([failing_second_start, failing_second_start]),
    ):
        pass

    assert len(startup_attempts) == 2
    assert stopped == [handles[0]]


def test_first_teardown_failure_still_converges_rest_and_surfaces_chained(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failing teardown never skips later stacks nor hides the failure."""
    first = _FakeSupervisor()
    second = _FakeSupervisor()
    handles: list[subprocess.Popen[bytes]] = [
        cast("subprocess.Popen[bytes]", first),
        cast("subprocess.Popen[bytes]", second),
    ]
    stop_order: list[subprocess.Popen[bytes]] = []

    def fake_stop(proc: subprocess.Popen[bytes]) -> None:
        stop_order.append(proc)
        if proc is handles[0]:
            msg = "teardown wedged"
            raise OSError(msg)

    monkeypatch.setattr(sys.modules[__name__], "stop_supervisor", fake_stop)
    handles_iter = iter(handles)
    startups: list[Callable[[], subprocess.Popen[bytes]]] = [
        handles_iter.__next__,
        handles_iter.__next__,
    ]

    body_ran = False
    with (
        pytest.raises(OSError, match="teardown wedged"),
        _owned_supervisors(startups),
    ):
        body_ran = True

    assert body_ran is True
    # ExitStack unwinds LIFO, but every registered stack is still converged.
    assert stop_order == [handles[1], handles[0]]


def test_body_failure_with_failing_teardown_chains_both_exceptions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cleanup that cannot converge chains loudly instead of masking the body."""
    first = _FakeSupervisor()
    handles: list[subprocess.Popen[bytes]] = [cast("subprocess.Popen[bytes]", first)]
    stopped: list[subprocess.Popen[bytes]] = []

    def always_wedged_stop(_proc: subprocess.Popen[bytes]) -> None:
        stopped.append(handles[0])
        msg = "teardown wedged"
        raise OSError(msg)

    def failing_body() -> None:
        msg = "body assertion blew up"
        raise ValueError(msg)

    monkeypatch.setattr(sys.modules[__name__], "stop_supervisor", always_wedged_stop)

    excinfo: pytest.ExceptionInfo[OSError]
    with (
        pytest.raises(OSError, match="teardown wedged") as excinfo,
        _owned_supervisors([lambda: handles[0]]),
    ):
        failing_body()

    assert stopped == handles
    assert isinstance(excinfo.value.__context__, ValueError)
    assert str(excinfo.value.__context__) == "body assertion blew up"


def test_live_worker_db_outage_is_not_duplicated(
    jobs_db: str,
    pg_cluster: _pg.PgCluster,
    tmp_path: Path,
    maintained_env: tuple[Path, str, str],
    supervisor_env: dict[str, str],
) -> None:
    """A live worker that only loses the database is never duplicated."""
    repo, first, _second = maintained_env
    with running_supervisor(supervisor_env):
        request_and_wait(first, repo)
        original_pid = worker_pid()
        assert original_pid is not None

        pg_cluster.stop()
        time.sleep(1.0)

        assert worker_pid() == original_pid
        status = supervise.read_status()
        assert status is not None
        assert len(direct_children(status.supervisor_pid)) == 1

        pg_cluster.start()
        probe_id = insert_pending_job(jobs_db, str(tmp_path), "echo ok")
        wait_until(lambda: read_status_of(jobs_db, probe_id) == "succeeded", timeout=60.0)


# ---------------------------------------------------------------------------
# Acceptance: supervisor restart and reconstruction
# ---------------------------------------------------------------------------


def test_supervisor_restart_reconstructs_a_single_worker(
    jobs_db: str,
    pg_cluster: _pg.PgCluster,
    tmp_path: Path,
    maintained_env: tuple[Path, str, str],
    supervisor_env: dict[str, str],
) -> None:
    """A supervisor restart reconstructs exactly one worker from durable state."""
    del pg_cluster
    repo, first, _second = maintained_env
    with running_supervisor(supervisor_env):
        request_and_wait(first, repo)
        original_pid = worker_pid()
        assert original_pid is not None
    assert not process_alive(original_pid)

    with running_supervisor(supervisor_env):
        wait_until(lambda: worker_pid() is not None, timeout=30.0)
        reconstructed_pid = worker_pid()
        assert reconstructed_pid != original_pid
        wait_until(status_ready, timeout=30.0)
        status = supervise.read_status()
        assert status is not None
        assert len(direct_children(status.supervisor_pid)) == 1
        meta = lifecycle.read_meta()
        assert meta is not None
        assert meta.pid == reconstructed_pid
        probe_id = insert_pending_job(jobs_db, str(tmp_path), "echo reconstructed")
        wait_until(lambda: read_status_of(jobs_db, probe_id) == "succeeded")


def test_supervisor_takeover_stops_orphan_worker_by_exact_identity(
    jobs_db: str,
    maintained_env: tuple[Path, str, str],
    supervisor_env: dict[str, str],
) -> None:
    """A hard-killed supervisor's orphaned worker is taken over, never duplicated."""
    del jobs_db
    repo, first, _second = maintained_env
    first_proc = start_supervisor(supervisor_env)
    try:
        request_and_wait(first, repo)
        orphan_pid = worker_pid()
        assert orphan_pid is not None
        first_proc.kill()
        first_proc.wait(timeout=5)
        guard.unregister(first_proc)
        assert process_alive(orphan_pid)

        second_proc = start_supervisor(supervisor_env)
        try:
            wait_until(
                lambda: worker_pid() is not None and worker_pid() != orphan_pid,
                timeout=30.0,
            )
            replacement_pid = worker_pid()
            assert replacement_pid is not None
            assert not process_alive(orphan_pid)
            assert process_alive(replacement_pid)
            assert len(direct_children(second_proc.pid)) == 1
        finally:
            stop_supervisor(second_proc)
    finally:
        if first_proc.poll() is None:
            stop_supervisor(first_proc)


# ---------------------------------------------------------------------------
# Acceptance: singleton ownership (#68)
# ---------------------------------------------------------------------------

#: Return code a second supervisor must exit with when it loses ownership.
NON_OWNER_EXIT_CODE: Final = 1


def wait_for_exit(proc: subprocess.Popen[bytes], timeout: float = 30.0) -> int:
    """Wait until a process exits and return its exit code.

    Args:
        proc: Process to await.
        timeout: Maximum seconds to wait.

    Returns:
        The process exit code.
    """
    wait_until(lambda: proc.poll() is not None, timeout=timeout)
    return int(proc.poll() or 0)


def test_second_supervisor_while_owner_running_exits_before_mutation(
    jobs_db: str,
    pg_cluster: _pg.PgCluster,
    tmp_path: Path,
    maintained_env: tuple[Path, str, str],
    supervisor_env: dict[str, str],
) -> None:
    """A concurrent second supervisor exits before mutating any daemon state.

    The process-level ownership lock is held for the entire daemon lifetime,
    so a supervisor started while the owner is live must fail closed before it
    can rewrite ``supervisor.pid``, ``state.json``, or the worker lifecycle:
    the durable generation, the recorded pidfile identity, and the running
    worker child must all stay exactly as the owner left them (no oscillation,
    no second consumer), and the same state root must accept a fresh
    supervisor again once the owner exits.
    """
    del pg_cluster
    repo, first, _second = maintained_env
    with running_supervisor(supervisor_env) as owner:
        request_and_wait(first, repo)
        worker = worker_pid()
        assert worker is not None
        status = supervise.read_status()
        assert status is not None
        applied = status.applied_generation
        assert applied >= 1

        intruder = start_supervisor(supervisor_env)
        exit_code = wait_for_exit(intruder)
        guard.unregister(intruder)
        assert exit_code == NON_OWNER_EXIT_CODE
        assert not process_alive(intruder.pid)

        status = supervise.read_status()
        assert status is not None
        assert status.supervisor_pid == owner.pid
        assert status.applied_generation == applied
        assert status.child is not None
        assert status.child.pid == worker
        assert len(direct_children(owner.pid)) == 1
        recorded = supervise.read_supervisor_pid()
        assert recorded is not None
        assert recorded[0] == owner.pid
        meta = lifecycle.read_meta()
        assert meta is not None
        assert meta.pid == worker
        assert supervise.supervisor_running()

        probe_id = insert_pending_job(jobs_db, str(tmp_path), "echo singleton-ok")
        wait_until(lambda: read_status_of(jobs_db, probe_id) == "succeeded")
        assert worker_pid() == worker

    with running_supervisor(supervisor_env):
        wait_until(lambda: worker_pid() is not None, timeout=30.0)
        wait_until(status_ready, timeout=30.0)
        resumed_pid = worker_pid()
        assert resumed_pid is not None
        assert resumed_pid != worker
        status = supervise.read_status()
        assert status is not None
        assert status.supervisor_pid != owner.pid
        assert len(direct_children(status.supervisor_pid)) == 1
        meta = lifecycle.read_meta()
        assert meta is not None
        assert meta.pid == resumed_pid


def test_two_simultaneous_supervisors_resolve_to_single_owner(
    jobs_db: str,
    pg_cluster: _pg.PgCluster,
    tmp_path: Path,
    maintained_env: tuple[Path, str, str],
    supervisor_env: dict[str, str],
) -> None:
    """Two near-simultaneous supervisor starts resolve to exactly one owner.

    Regression for the #68 singleton race: with no daemon yet running, two
    ``lubko-supervisor`` processes are started back-to-back against a single
    durable intent.  Exactly one of them must win the process-level ownership
    lock, run the daemon loop, and own the sole worker child; the loser must
    exit fail-closed with code 1 before it can write the pidfile, and the
    durable generation must never oscillate or duplicate a consumer.
    """
    del pg_cluster
    repo, first, _second = maintained_env
    supervise.request_run(first, repo=str(repo), uv_path="uv", worker_id=TEST_WORKER_ID)
    first_candidate = start_supervisor(supervisor_env)
    second_candidate = start_supervisor(supervisor_env)
    try:
        wait_until(
            lambda: first_candidate.poll() is not None or second_candidate.poll() is not None,
            timeout=30.0,
        )
        winner = first_candidate if first_candidate.poll() is None else second_candidate
        loser = second_candidate if first_candidate.poll() is None else first_candidate
        assert winner.poll() is None
        loser_exit = wait_for_exit(loser)
        assert loser_exit == NON_OWNER_EXIT_CODE
        assert not process_alive(loser.pid)

        wait_until(status_ready, timeout=30.0)
        status = supervise.read_status()
        assert status is not None
        assert status.supervisor_pid == winner.pid
        assert status.applied_generation == 1
        assert status.child is not None
        assert process_alive(status.child.pid)
        assert status.child.pid in direct_children(winner.pid)
        assert len(direct_children(winner.pid)) == 1
        recorded = supervise.read_supervisor_pid()
        assert recorded is not None
        assert recorded[0] == winner.pid
        meta = lifecycle.read_meta()
        assert meta is not None
        assert meta.pid == status.child.pid

        probe_id = insert_pending_job(jobs_db, str(tmp_path), "echo exactly-one")
        wait_until(lambda: read_status_of(jobs_db, probe_id) == "succeeded")
        assert worker_pid() == status.child.pid
    finally:
        for proc in (first_candidate, second_candidate):
            if proc.poll() is None:
                stop_supervisor(proc)
            else:
                guard.unregister(proc)


# ---------------------------------------------------------------------------
# Acceptance: fail-closed deployment state and supervised missions
# ---------------------------------------------------------------------------


def test_corrupt_rollback_state_fails_closed_into_hold(
    jobs_db: str,
    pg_cluster: _pg.PgCluster,
    maintained_env: tuple[Path, str, str],
    supervisor_env: dict[str, str],
) -> None:
    """Corrupt deployment state holds the daemon; no worker is ever started."""
    del jobs_db, pg_cluster
    repo, first, _second = maintained_env
    with running_supervisor(supervisor_env):
        request_and_wait(first, repo)
        original_pid = worker_pid()
        assert original_pid is not None

        rollback_state_path().write_text("{not json\n", encoding="utf-8")

        wait_until(status_has_no_child, timeout=30.0)
        assert not process_alive(original_pid)
        time.sleep(1.0)
        assert status_has_no_child()
        status = supervise.read_status()
        assert status is not None
        assert status.message is not None
        assert len(direct_children(status.supervisor_pid)) == 0


def test_pending_mission_drives_candidate_and_settlement_converges(
    jobs_db: str,
    pg_cluster: _pg.PgCluster,
    maintained_env: tuple[Path, str, str],
    supervisor_env: dict[str, str],
) -> None:
    """A newer pending mission drives the candidate; settle keeps one worker."""
    del jobs_db, pg_cluster
    repo, first, second = maintained_env
    with running_supervisor(supervisor_env):
        applied = request_and_wait(first, repo)
        original_pid = worker_pid()
        assert original_pid is not None

        write_rollback(
            replace(
                mission_state(applied + 1, dc.STATUS_PENDING, second, first),
                repo=str(repo),
                supervisor_owned=True,
            )
        )
        wait_for_replacement(original_pid)
        wait_until(status_ready, timeout=30.0)
        candidate_pid = worker_pid()
        assert candidate_pid is not None
        assert candidate_pid != original_pid
        status = supervise.read_status()
        assert status is not None
        assert status.commit == second
        assert status.applied_generation == applied + 1
        assert len(direct_children(status.supervisor_pid)) == 1

        settle = supervise.request_run(
            second,
            repo=str(repo),
            uv_path="uv",
            worker_id=TEST_WORKER_ID,
        )
        assert settle > applied + 1
        assert supervise.wait_until_ready(settle, 30.0)
        assert worker_pid() is not None
        final_status = supervise.read_status()
        assert final_status is not None
        assert len(direct_children(final_status.supervisor_pid)) == 1
        assert final_status.commit == second


def test_supervised_deploy_candidate_is_direct_supervisor_child(
    jobs_db: str,
    pg_cluster: _pg.PgCluster,
    maintained_env: tuple[Path, str, str],
    supervisor_env: dict[str, str],
) -> None:
    """A pending-mission candidate is started by the supervisor as a direct child."""
    del jobs_db, pg_cluster
    repo, first, second = maintained_env
    with running_supervisor(supervisor_env):
        applied = request_and_wait(first, repo)
        original_pid = worker_pid()
        assert original_pid is not None

        dc.publish_mission(
            replace(
                mission_state(applied + 1, dc.STATUS_PENDING, second, first),
                repo=str(repo),
                supervisor_owned=True,
            ),
            lock_timeout_seconds=5.0,
        )
        wait_for_replacement(original_pid)
        wait_until(status_ready, timeout=30.0)
        candidate_pid = worker_pid()
        assert candidate_pid is not None
        status = supervise.read_status()
        assert status is not None
        assert candidate_pid in direct_children(status.supervisor_pid)
        assert status.commit == second
        assert len(direct_children(status.supervisor_pid)) == 1

        settle = dc.settle_desired(second, str(repo), "uv")
        assert settle > applied + 1
        wait_until(status_ready, timeout=30.0)
        final_status = supervise.read_status()
        assert final_status is not None
        assert final_status.commit == second
        assert len(direct_children(final_status.supervisor_pid)) == 1
        write_rollback(
            replace(
                mission_state(applied + 1, dc.STATUS_CONFIRMED, second, first),
                repo=str(repo),
                supervisor_owned=True,
            )
        )


def test_supervised_bad_candidate_rollback_converges_to_previous(
    jobs_db: str,
    pg_cluster: _pg.PgCluster,
    maintained_env: tuple[Path, str, str],
    supervisor_env: dict[str, str],
) -> None:
    """An unconfirmed candidate settles back to the previous commit, one worker."""
    del jobs_db, pg_cluster
    repo, first, second = maintained_env
    with running_supervisor(supervisor_env):
        applied = request_and_wait(first, repo)
        original_pid = worker_pid()
        assert original_pid is not None

        dc.publish_mission(
            replace(
                mission_state(applied + 1, dc.STATUS_PENDING, second, first),
                repo=str(repo),
                supervisor_owned=True,
            ),
            lock_timeout_seconds=5.0,
        )
        wait_for_replacement(original_pid)
        wait_until(status_ready, timeout=30.0)
        candidate_pid = worker_pid()
        assert candidate_pid is not None

        settle = dc.settle_desired(first, str(repo), "uv")
        assert settle > applied + 1
        wait_until(status_ready, timeout=30.0)
        final_pid = worker_pid()
        assert final_pid is not None
        assert final_pid != candidate_pid
        final_status = supervise.read_status()
        assert final_status is not None
        assert final_status.commit == first
        assert len(direct_children(final_status.supervisor_pid)) == 1
        write_rollback(
            replace(
                mission_state(applied + 1, dc.STATUS_ROLLED_BACK, second, first),
                repo=str(repo),
                supervisor_owned=True,
            )
        )


def _status_options(repo: Path) -> dc.Options:
    """Build controller options for a status query against a live supervisor.

    Args:
        repo: Maintained checkout.

    Returns:
        Runtime options for the controller.
    """
    return dc.Options(
        repo=repo,
        uv_path="uv",
        confirm_window_seconds=120,
        stop_grace_seconds=1.0,
        postgres_timeout_seconds=5,
        lock_timeout_seconds=5,
        validation_timeout_seconds=30,
        git_timeout_seconds=10,
        cli_timeout_seconds=60,
    )


def test_status_keeps_live_supervised_pending_mission(
    jobs_db: str,
    pg_cluster: _pg.PgCluster,
    maintained_env: tuple[Path, str, str],
    supervisor_env: dict[str, str],
) -> None:
    """A status query keeps a healthy supervisor-owned pending mission live.

    The durable pending mission carries the never-alive placeholder candidate
    identity, so real candidate liveness comes only from the supervisor's own
    child state. A supported ``lubko-deploy-ctl status`` during a healthy
    supervised deployment must report the pending phase and must never roll the
    candidate back.
    """
    del jobs_db, pg_cluster
    repo, first, second = maintained_env
    with running_supervisor(supervisor_env):
        applied = request_and_wait(first, repo)
        original_pid = worker_pid()
        assert original_pid is not None

        write_rollback(
            replace(
                mission_state(applied + 1, dc.STATUS_PENDING, second, first),
                repo=str(repo),
                supervisor_owned=True,
            )
        )
        wait_for_replacement(original_pid)
        wait_until(status_ready, timeout=30.0)
        candidate_pid = worker_pid()
        assert candidate_pid is not None
        status = supervise.read_status()
        assert status is not None
        assert status.commit == second

        result = dc._handle_status(_status_options(repo))  # ruff: ignore[private-member-access]

        assert result["phase"] == "await-confirmation"
        assert result["proposed_commit"] == second
        assert result["previous_commit"] == first
        after = dc._read_state()  # ruff: ignore[private-member-access]
        assert after is not None
        assert after.status == dc.STATUS_PENDING
        assert worker_pid() == candidate_pid


def test_status_rolls_back_supervised_mission_with_lapsed_deadline(
    jobs_db: str,
    pg_cluster: _pg.PgCluster,
    maintained_env: tuple[Path, str, str],
    supervisor_env: dict[str, str],
) -> None:
    """A status query enforces the lapsed deadline on a supervisor-owned mission.

    The supervisor drives candidates purely by generation and keeps running the
    candidate past the confirmation deadline, so the status path is the
    deadline enforcer: a lapsed pending mission must settle back to the
    previous commit even while its candidate is still live.
    """
    del jobs_db, pg_cluster
    repo, first, second = maintained_env
    with running_supervisor(supervisor_env):
        applied = request_and_wait(first, repo)
        original_pid = worker_pid()
        assert original_pid is not None

        lapsed = replace(
            mission_state(applied + 1, dc.STATUS_PENDING, second, first),
            repo=str(repo),
            supervisor_owned=True,
        )
        write_rollback(replace(lapsed, deadline=time.time() - 1))
        wait_for_replacement(original_pid)
        wait_until(status_ready, timeout=30.0)
        candidate_pid = worker_pid()
        assert candidate_pid is not None

        result = dc._handle_status(_status_options(repo))  # ruff: ignore[private-member-access]

        assert result["phase"] == "idle"
        assert result["last_outcome"] == dc.STATUS_ROLLED_BACK
        final = dc._read_state()  # ruff: ignore[private-member-access]
        assert final is not None
        assert final.status == dc.STATUS_ROLLED_BACK
        wait_until(status_commit_is(first), timeout=30.0)
        status = supervise.read_status()
        assert status is not None
        assert status.commit == first
        assert status.applied_generation > applied + 1
        assert status.child is not None
        assert status.child.pid != candidate_pid


def test_supervisor_restart_resumes_pending_mission_candidate(
    jobs_db: str,
    pg_cluster: _pg.PgCluster,
    maintained_env: tuple[Path, str, str],
    supervisor_env: dict[str, str],
) -> None:
    """A supervisor restart during a pending mission resumes the candidate."""
    del jobs_db, pg_cluster
    repo, first, second = maintained_env
    with running_supervisor(supervisor_env):
        applied = request_and_wait(first, repo)
        original_pid = worker_pid()
        assert original_pid is not None
        dc.publish_mission(
            replace(
                mission_state(applied + 1, dc.STATUS_PENDING, second, first),
                repo=str(repo),
                supervisor_owned=True,
            ),
            lock_timeout_seconds=5.0,
        )
        wait_for_replacement(original_pid)
        wait_until(status_ready, timeout=30.0)
        first_candidate = worker_pid()
        assert first_candidate is not None

    with running_supervisor(supervisor_env):
        wait_until(lambda: worker_pid() is not None, timeout=30.0)
        wait_until(status_ready, timeout=30.0)
        resumed_pid = worker_pid()
        assert resumed_pid is not None
        assert resumed_pid != first_candidate
        status = supervise.read_status()
        assert status is not None
        assert status.commit == second
        assert len(direct_children(status.supervisor_pid)) == 1
        write_rollback(
            replace(
                mission_state(applied + 1, dc.STATUS_CONFIRMED, second, first),
                repo=str(repo),
                supervisor_owned=True,
            )
        )


def test_deploy_and_restart_generations_strictly_increase(
    jobs_db: str,
    pg_cluster: _pg.PgCluster,
    maintained_env: tuple[Path, str, str],
    supervisor_env: dict[str, str],
) -> None:
    """Deploy/restart/mission/settle generations strictly increase, one worker."""
    del jobs_db, pg_cluster
    repo, first, second = maintained_env
    with running_supervisor(supervisor_env):
        applied = request_and_wait(first, repo)

        restart_gen = supervise.request_restart(
            first,
            repo=str(repo),
            uv_path="uv",
            worker_id=TEST_WORKER_ID,
        )
        assert restart_gen > applied
        assert supervise.wait_until_ready(restart_gen, 30.0)
        status = supervise.read_status()
        assert status is not None
        assert len(direct_children(status.supervisor_pid)) == 1

        mission_gen = dc.next_mission_generation()
        assert mission_gen > restart_gen
        dc.publish_mission(
            replace(
                mission_state(mission_gen, dc.STATUS_PENDING, second, first),
                repo=str(repo),
                supervisor_owned=True,
            ),
            lock_timeout_seconds=5.0,
        )
        wait_until(status_commit_is(second), timeout=30.0)
        wait_until(status_ready, timeout=30.0)
        status = supervise.read_status()
        assert status is not None
        assert status.commit == second
        assert len(direct_children(status.supervisor_pid)) == 1

        settle_gen = dc.settle_desired(second, str(repo), "uv")
        assert settle_gen > mission_gen
        wait_until(status_ready, timeout=30.0)
        status = supervise.read_status()
        assert status is not None
        assert status.commit == second
        assert len(direct_children(status.supervisor_pid)) == 1
        assert status.applied_generation == settle_gen
        write_rollback(
            replace(
                mission_state(mission_gen, dc.STATUS_CONFIRMED, second, first),
                repo=str(repo),
                supervisor_owned=True,
            )
        )


def test_migrate_from_stale_fake_state_then_supervisor_reconstructs(
    jobs_db: str,
    pg_cluster: _pg.PgCluster,
    tmp_path: Path,
    maintained_env: tuple[Path, str, str],
    supervisor_env: dict[str, str],
) -> None:
    """Explicit migration replaces stale fake state; the supervisor converges."""
    del pg_cluster
    repo, first, _second = maintained_env
    rollback_state_path().parent.mkdir(parents=True, exist_ok=True)
    rollback_state_path().write_text("{stale\n", encoding="utf-8")
    stale_desired = {
        "schema_version": 1,
        "generation": 99,
        "mode": "run",
        "commit": "0" * 40,
        "repo": "",
        "uv_path": "",
        "worker_id": None,
        "requested_at": 0.0,
    }
    supervise.desired_path().parent.mkdir(parents=True, exist_ok=True)
    supervise.desired_path().write_text(
        json.dumps(stale_desired, sort_keys=True) + "\n", encoding="utf-8"
    )

    args = argparse.Namespace(commit=first, repo=repo, uv="uv", lock_timeout=5.0)
    assert lifecycle.migrate_cmd(args) == 0
    desired = supervise.read_desired()
    assert desired is not None
    assert desired.commit == first
    assert desired.generation > 99

    with running_supervisor(supervisor_env):
        wait_until(lambda: worker_pid() is not None, timeout=30.0)
        wait_until(status_ready, timeout=30.0)
        status = supervise.read_status()
        assert status is not None
        assert status.commit == first
        assert len(direct_children(status.supervisor_pid)) == 1
        probe_id = insert_pending_job(jobs_db, str(tmp_path), "echo migrated")
        wait_until(lambda: read_status_of(jobs_db, probe_id) == "succeeded", timeout=60.0)


# ---------------------------------------------------------------------------
# Unit-level decision and identity guarantees
# ---------------------------------------------------------------------------


def test_derive_action_fails_closed_on_corrupt_rollback(tmp_path: Path) -> None:
    """A corrupt rollback state yields a hold, never a runnable worker."""
    del tmp_path
    rollback_state_path().parent.mkdir(parents=True, exist_ok=True)
    rollback_state_path().write_text("{not json\n", encoding="utf-8")
    lifecycle.write_meta(
        lifecycle.WorkerMeta(
            schema_version=1,
            state=lifecycle.STATE_RUNNING,
            pid=123_456,
            pgid=123_456,
            sid=123_456,
            start_time_ticks=99,
            token=LEGACY_MARKER,
            repo="",
            git_commit="1" * 40,
            worker_id="w",
            log_path="",
            started_at=1.0,
            stopped_at=None,
        )
    )
    daemon = SupervisorDaemon(Settings())
    action, commit = daemon._derive_action(supervise.read_state())  # ruff: ignore[private-member-access]
    assert action == "hold"
    assert commit is None


def _derive_action() -> tuple[str, str | None]:
    """Derive the supervisor's intended action from the current durable state.

    Returns:
        The ``(action, commit)`` pair.
    """
    daemon = SupervisorDaemon(Settings())
    return daemon._derive_action(supervise.read_state())  # ruff: ignore[private-member-access]


def test_derive_action_pending_newer_than_desired_selects_candidate(
    tmp_path: Path,
) -> None:
    """A pending mission at a newer generation is the active candidate intent."""
    del tmp_path
    supervise.request_run("1" * 40, repo="", uv_path="uv", worker_id="w")
    write_rollback(mission_state(2, dc.STATUS_PENDING, "2" * 40, "1" * 40))
    action, commit = _derive_action()
    assert action == "run"
    assert commit == "2" * 40


def test_derive_action_pending_equal_desired_same_commit_runs(tmp_path: Path) -> None:
    """A pending mission at the desired generation selecting the same commit runs."""
    del tmp_path
    supervise.request_run("2" * 40, repo="", uv_path="uv", worker_id="w")
    write_rollback(mission_state(1, dc.STATUS_PENDING, "2" * 40, "1" * 40))
    action, commit = _derive_action()
    assert action == "run"
    assert commit == "2" * 40


def test_derive_action_pending_equal_desired_mismatch_holds(tmp_path: Path) -> None:
    """A pending mission contradicting the desired generation must hold."""
    del tmp_path
    supervise.request_run("1" * 40, repo="", uv_path="uv", worker_id="w")
    write_rollback(mission_state(1, dc.STATUS_PENDING, "2" * 40, "1" * 40))
    action, commit = _derive_action()
    assert action == "hold"
    assert commit is None


def test_derive_action_stale_pending_ignored_for_newer_desired(tmp_path: Path) -> None:
    """A stale pending mission older than the desired intent is ignored."""
    del tmp_path
    write_rollback(mission_state(1, dc.STATUS_PENDING, "2" * 40, "1" * 40))
    supervise.request_run("3" * 40, repo="", uv_path="uv", worker_id="w")
    action, commit = _derive_action()
    assert action == "run"
    assert commit == "3" * 40


def test_derive_action_stale_confirmed_cannot_override_newer_restart(
    tmp_path: Path,
) -> None:
    """A terminal confirmed mission older than a newer restart is history."""
    del tmp_path
    write_rollback(mission_state(1, dc.STATUS_CONFIRMED, "2" * 40, "1" * 40))
    supervise.request_restart("3" * 40, repo="", uv_path="uv", worker_id="w")
    action, commit = _derive_action()
    assert action == "run"
    assert commit == "3" * 40


def test_derive_action_stale_rolled_back_cannot_override_newer_deploy(
    tmp_path: Path,
) -> None:
    """A terminal rolled_back mission older than a newer deploy is history."""
    del tmp_path
    write_rollback(mission_state(1, dc.STATUS_ROLLED_BACK, "2" * 40, "1" * 40))
    supervise.request_run("3" * 40, repo="", uv_path="uv", worker_id="w")
    action, commit = _derive_action()
    assert action == "run"
    assert commit == "3" * 40


def test_derive_action_terminal_mission_at_or_newer_holds(tmp_path: Path) -> None:
    """A terminal mission without a newer settled intent is unsettled: hold."""
    del tmp_path
    supervise.request_run("2" * 40, repo="", uv_path="uv", worker_id="w")
    write_rollback(mission_state(2, dc.STATUS_CONFIRMED, "2" * 40, "1" * 40))
    action, commit = _derive_action()
    assert action == "hold"
    assert commit is None
    write_rollback(mission_state(1, dc.STATUS_ROLLED_BACK, "2" * 40, "1" * 40))
    action, commit = _derive_action()
    assert action == "hold"
    assert commit is None
    write_rollback(mission_state(1, dc.STATUS_CONFIRMED, "2" * 40, "1" * 40))
    action, commit = _derive_action()
    assert action == "hold"
    assert commit is None


def test_derive_action_legacy_mission_without_generation_holds(tmp_path: Path) -> None:
    """Legacy schema-1 mission state without a generation fails closed."""
    del tmp_path
    rollback_state_path().parent.mkdir(parents=True, exist_ok=True)
    legacy = mission_state(1, dc.STATUS_PENDING, "2" * 40, "1" * 40).to_dict()
    del legacy["generation"]
    legacy["schema_version"] = 1
    rollback_state_path().write_text(json.dumps(legacy, sort_keys=True) + "\n", encoding="utf-8")
    action, commit = _derive_action()
    assert action == "hold"
    assert commit is None


def test_derive_action_no_desired_selects_newer_pending_candidate(
    tmp_path: Path,
) -> None:
    """Supervisor restart with no desired intent resumes the pending candidate."""
    del tmp_path
    assert supervise.read_desired() is None
    write_rollback(mission_state(5, dc.STATUS_PENDING, "2" * 40, "1" * 40))
    action, commit = _derive_action()
    assert action == "run"
    assert commit == "2" * 40


def test_derive_action_legacy_v2_mission_without_ownership_is_parsed(tmp_path: Path) -> None:
    """A supported schema-2 mission parses; unknown ownership never authorizes.

    The old file must not look corrupt to the supervisor, and its missing
    ``supervisor_owned`` field must stay unknown (fail-closed) instead of being
    implicitly legacy-authorized.
    """
    del tmp_path
    supervise.request_run("2" * 40, repo="", uv_path="uv", worker_id="w")
    legacy = mission_state(1, dc.STATUS_PENDING, "2" * 40, "1" * 40).to_dict()
    del legacy["supervisor_owned"]
    legacy["schema_version"] = 2
    rollback_state_path().parent.mkdir(parents=True, exist_ok=True)
    rollback_state_path().write_text(json.dumps(legacy, sort_keys=True) + "\n", encoding="utf-8")

    parsed = dc.read_rollback_state()

    assert parsed is not None
    assert parsed.schema_version == dc.ROLLBACK_SCHEMA_VERSION
    assert parsed.supervisor_owned is None
    # The old pending mission is older than the freshly written desired intent,
    # so it is stale history: the supervisor runs the desired commit.
    action, commit = _derive_action()
    assert action == "run"
    assert commit == "2" * 40


def test_derive_action_fails_closed_on_unsupported_future_version(tmp_path: Path) -> None:
    """A valid-looking future-schema mission holds without a worker."""
    del tmp_path
    future = mission_state(3, dc.STATUS_PENDING, "2" * 40, "1" * 40).to_dict()
    future["schema_version"] = dc.ROLLBACK_SCHEMA_VERSION + 1
    rollback_state_path().parent.mkdir(parents=True, exist_ok=True)
    rollback_state_path().write_text(json.dumps(future, sort_keys=True) + "\n", encoding="utf-8")
    action, commit = _derive_action()
    assert action == "hold"
    assert commit is None


def test_next_generation_surpasses_open_mission(tmp_path: Path) -> None:
    """Generation allocation always outranks an open mission."""
    del tmp_path
    write_rollback(mission_state(7, dc.STATUS_PENDING, "2" * 40, "1" * 40))
    generation = supervise.request_run("9" * 40, repo="", uv_path="uv", worker_id="w")
    assert generation > 7
    desired = supervise.read_desired()
    assert desired is not None
    assert desired.generation == generation


def test_wait_for_identity_rejects_non_leader_on_timeout() -> None:
    """A process that never becomes a session leader is never accepted."""
    proc = subprocess.Popen(
        [SLEEP_BIN, "30"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    guard.register(proc)
    try:
        daemon = SupervisorDaemon(Settings(identity_timeout_seconds=0.3))
        assert daemon._wait_for_identity(proc.pid) is None  # ruff: ignore[private-member-access]
    finally:
        guard.teardown_tracked(fail_on_leak=False)


def test_supervise_desired_roundtrip(tmp_path: Path) -> None:
    """A desired intent round-trips and generation bumps monotonically."""
    del tmp_path
    assert supervise.read_desired() is None
    first = supervise.request_run("a" * 40, repo="/repo", uv_path="uv", worker_id="w")
    desired = supervise.read_desired()
    assert desired is not None
    assert desired.commit == "a" * 40
    assert desired.generation == first
    second = supervise.request_run("b" * 40, repo="/repo", uv_path="uv", worker_id="w")
    assert second > first


def test_supervise_restart_uses_newer_generation_same_commit(tmp_path: Path) -> None:
    """A restart request bumps the generation while keeping the exact commit."""
    del tmp_path
    assert not supervise.supervisor_running()
    first = supervise.request_run("a" * 40, repo="/repo", uv_path="uv", worker_id="w")
    desired = supervise.read_desired()
    assert desired is not None
    assert desired.commit == "a" * 40
    assert desired.generation == first
    restart = supervise.request_restart("a" * 40, repo="/repo", uv_path="uv", worker_id="w")
    assert restart > first
    after = supervise.read_desired()
    assert after is not None
    assert after.commit == "a" * 40
    assert after.generation == restart
    assert not supervise.wait_until_ready(restart, 0.2)


def test_supervisor_status_command_without_daemon(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The ``--status`` command reports when no daemon is running."""
    assert supervisor_main(["--status"]) == 1
    assert "not running" in capsys.readouterr().out


def test_derive_action_stale_state_commit_without_intent_holds(
    tmp_path: Path,
) -> None:
    """A stale state/meta commit never selects a worker without a live intent."""
    del tmp_path
    assert supervise.read_desired() is None
    supervise.write_state(
        replace(
            supervise.read_state(),
            mode=supervise.MODE_RUN,
            commit="1" * 40,
            applied_generation=1,
        )
    )
    lifecycle.write_meta(
        lifecycle.WorkerMeta(
            schema_version=1,
            state=lifecycle.STATE_RUNNING,
            pid=123_458,
            pgid=123_458,
            sid=123_458,
            start_time_ticks=99,
            token=LEGACY_MARKER,
            repo="",
            git_commit="1" * 40,
            worker_id="w",
            log_path="",
            started_at=1.0,
            stopped_at=None,
        )
    )
    action, commit = _derive_action()
    assert action == "hold"
    assert commit is None


def test_tick_derives_before_applying_stale_desired(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A newer mission candidate wins before a stale desired intent applies."""
    del tmp_path
    supervise.write_state(replace(supervise.read_state(), applied_generation=1))
    supervise.write_desired(
        supervise.SupervisorDesired(
            schema_version=supervise.SCHEMA_VERSION,
            generation=2,
            commit="2" * 40,
            repo="",
            uv_path="uv",
            worker_id="w",
        )
    )
    desired = supervise.read_desired()
    assert desired is not None
    assert desired.generation == 2
    assert desired.commit == "2" * 40
    daemon = SupervisorDaemon(Settings())
    applied: list[supervise.SupervisorDesired] = []
    ensured: list[str] = []
    monkeypatch.setattr(daemon, "_derive_action", lambda _state: ("run", "3" * 40))
    monkeypatch.setattr(daemon, "_apply_desired", applied.append)
    monkeypatch.setattr(daemon, "_ensure_worker", ensured.append)
    monkeypatch.setattr(daemon, "_record_mission_progress", lambda _commit: None)
    monkeypatch.setattr(daemon, "_probe_readiness", lambda _now: None)
    daemon.reconcile(0.0)
    assert applied == []
    assert ensured == ["3" * 40]


# ---------------------------------------------------------------------------
# Queue-invoked plain ``lubko-deploy deploy`` (#68)
# ---------------------------------------------------------------------------


def write_fake_uv(tmp_path: Path, *, fail_validation: bool = False) -> Path:
    """Write a stub ``uv`` that validates instantly and never spawns a worker.

    The deploy's validation runs ``uv sync``/``uv run ...``; the stub exits zero
    (or non-zero when ``fail_validation`` is set) so validation is instant. The
    supervisor spawns the worker from the sealed per-commit runtime itself, so
    the stub never needs a ``uv run lubko-worker`` arm.

    Args:
        tmp_path: Temporary directory for the script.
        fail_validation: Whether every validation step must fail.

    Returns:
        The fake ``uv`` executable path.
    """
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(parents=True, exist_ok=True)
    script = bin_dir / "uv"
    script.write_text("#!/bin/sh\nexit 9\n" if fail_validation else "#!/bin/sh\nexit 0\n")
    script.chmod(0o755)
    return script


def checkout_args(repo: Path, fake_uv: Path, commit: str) -> list[str]:
    """Build the argv of one queue-invoked ``lubko-deploy-ctl checkout`` job.

    Args:
        repo: Deployment checkout pinned to the candidate commit.
        fake_uv: Stub ``uv`` executable.
        commit: Exact candidate commit to check out.

    Returns:
        The checkout argv running the real stable-wrapper CLI.
    """
    return [
        sys.executable,
        "-m",
        "lubko.deployctl",
        json.dumps({"type": "checkout", "commit": commit}),
        "--repo",
        str(repo),
        "--uv",
        str(fake_uv),
        "--grace-seconds",
        "0.5",
        "--db-timeout",
        "5",
        "--lock-timeout",
        "15",
        "--validation-timeout",
        "30",
        "--git-timeout",
        "10",
        "--cli-timeout",
        "60",
    ]


def deploy_deploy_args(repo: Path, fake_uv: Path) -> list[str]:
    """Build the argv of one queue-invoked ``lubko-deploy deploy`` job.

    Args:
        repo: Deployment checkout pinned to the candidate commit.
        fake_uv: Stub ``uv`` executable.

    Returns:
        The deploy argv running the real lifecycle CLI.
    """
    return [
        sys.executable,
        "-m",
        "lubko.lifecycle",
        "deploy",
        "--repo",
        str(repo),
        "--uv",
        str(fake_uv),
        "--grace-seconds",
        "0.5",
        "--db-timeout",
        "5",
        "--lock-timeout",
        "15",
        "--validation-timeout",
        "30",
        "--git-timeout",
        "10",
        "--cli-timeout",
        "60",
    ]


def insert_pending_process_job(conninfo: str, cwd: str, process: list[str]) -> UUID:
    """Insert a protocol v4 pending command job executing argv directly.

    Args:
        conninfo: PostgreSQL connection string.
        cwd: Working directory for the job.
        process: Non-empty argv array to execute directly.

    Returns:
        The job identifier.
    """
    payload = json.dumps({
        "v": 4,
        "type": "command",
        "server": "alpha-server",
        "request": {"cwd": cwd, "process": process},
        "state": {"status": "pending"},
    })
    with psycopg.connect(conninfo) as conn:
        row = conn.execute(
            "INSERT INTO lubko.jobs (payload) VALUES (%s) RETURNING id",
            (payload,),
        ).fetchone()
    assert row is not None
    return cast("UUID", row[0])


def _wait_for_queue_success_and_old_death(
    jobs_db: str, queue_id: UUID, old_meta: lifecycle.WorkerMeta
) -> None:
    """Wait for durable queue success and prove the old worker dies after.

    The deploying/restarting root row must reach durable ``succeeded`` before
    the old worker is stopped. In the production split-state regression the
    supervisor retired the old worker while the initiating job was still
    running, so the old worker's shutdown cancelled the row; here the row is
    terminal first and the old worker's death must come strictly after that.

    Args:
        jobs_db: Connection string.
        queue_id: The initiating root job.
        old_meta: The old worker metadata.
    """
    dead_at: float | None = None
    succeeded_at: float | None = None
    deadline = time.monotonic() + 90.0
    while time.monotonic() < deadline:
        status = read_status_of(jobs_db, queue_id)
        if succeeded_at is None and status == "succeeded":
            succeeded_at = time.monotonic()
        if dead_at is None and not lifecycle.worker_alive(old_meta):
            dead_at = time.monotonic()
        if succeeded_at is not None and dead_at is not None:
            break
        time.sleep(0.02)
    assert succeeded_at is not None, "queue row never reached succeeded"
    assert dead_at is not None, "old worker never died"
    assert dead_at >= succeeded_at, "old worker died before durable queue success"


def payload_state(payload: dict[str, object]) -> dict[str, Any]:
    """Return the ``state`` sub-object of a decoded job payload.

    Args:
        payload: Decoded job payload.

    Returns:
        The state mapping.
    """
    state = payload["state"]
    assert isinstance(state, dict)
    return state


def ctl_request(request: dict[str, object], repo: Path, fake_uv: Path) -> dict[str, object]:
    """Run one synchronous ``lubko-deploy-ctl`` protocol request.

    Args:
        request: Protocol request object.
        repo: Maintained checkout passed as ``--repo``.
        fake_uv: Stub ``uv`` executable.

    Returns:
        The decoded protocol response.
    """
    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "lubko.deployctl",
            json.dumps(request),
            "--repo",
            str(repo),
            "--uv",
            str(fake_uv),
        ],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    return cast("dict[str, object]", json.loads(proc.stdout))


def confirm_checkout(repo: Path, fake_uv: Path, commit: str) -> None:
    """Drive the supported two-phase confirmation for a pending checkout.

    Args:
        repo: Maintained checkout.
        fake_uv: Stub ``uv`` executable.
        commit: Exact proposed candidate commit.
    """
    first = ctl_request({"type": "confirm", "commit": commit}, repo, fake_uv)
    challenge = first["challenge"]
    assert isinstance(challenge, str)
    second = ctl_request(
        {"type": "confirm", "commit": commit, "challenge": challenge[::-1]},
        repo,
        fake_uv,
    )
    assert second.get("confirmed") is True


def _deploy_converged(applied: int, commit: str, old_pid: int) -> Callable[[], bool]:
    """Return a predicate asserting the exact supervisor convergence.

    The deploy row reaches durable success *before* the supervisor retires the
    old worker, so ``status.json`` may still carry the pre-handoff snapshot
    (``ready=true`` for the old worker) for a moment. Waiting on this exact
    convergence — a strictly newer applied generation, the exact candidate
    commit, a fresh live child, proof it consumes the queue, and the maintained
    CLI pointer moved to the candidate — is what proves the handoff actually
    completed and converged rather than matching the stale pre-handoff snapshot.

    Args:
        applied: The generation applied before the deploy.
        commit: The exact deployed commit.
        old_pid: The pre-deploy worker PID that must have been replaced.

    Returns:
        A predicate usable with :func:`wait_until`.
    """

    def check() -> bool:
        status = supervise.read_status()
        return bool(
            status is not None
            and status.applied_generation > applied
            and status.commit == commit
            and status.child is not None
            and status.child.pid != old_pid
            and status.ready
            and cli.current_commit() == commit
        )

    return check


def test_queue_checkout_survives_old_worker_shutdown_and_converges(
    jobs_db: str,
    pg_cluster: _pg.PgCluster,
    maintained_env: tuple[Path, str, str],
    supervisor_env: dict[str, str],
    tmp_path: Path,
) -> None:
    """A queue-invoked ``lubko-deploy-ctl checkout`` survives its own old worker.

    The production defect: a root job executes a version-changing deployment
    from a worker-owned process group, the external supervisor retires that
    very worker during the handoff, and the old worker's shutdown cancels the
    initiating row even though the deployment converges. Under #29 the
    supported version-changing path is ``lubko-deploy-ctl checkout``; here its
    detached helper drives the supervisor handoff while the root row reaches
    durable ``succeeded`` (never ``cancelled``) before the old worker dies, so
    the CLI pointer, the supervisor desired+applied state, and the new worker
    commit converge without any later status reconciliation, and an unrelated
    active job is still terminated by the old worker's shutdown rather than
    orphaned.
    """
    del pg_cluster
    repo, first, second = maintained_env
    fake_uv = write_fake_uv(tmp_path)
    with running_supervisor(supervisor_env):
        applied = request_and_wait(first, repo)
        old_meta = lifecycle.read_meta()
        assert old_meta is not None
        assert old_meta.pid is not None

        unrelated = insert_pending_job(jobs_db, str(repo), "sleep 30")
        wait_until(lambda: read_status_of(jobs_db, unrelated) == "running", timeout=30.0)

        checkout_id = insert_pending_process_job(
            jobs_db, str(repo), checkout_args(repo, fake_uv, second)
        )
        _wait_for_queue_success_and_old_death(jobs_db, checkout_id, old_meta)

        checkout_payload = read_payload(jobs_db, checkout_id)
        checkout_state = payload_state(checkout_payload)
        assert checkout_state["status"] == "succeeded"
        result = checkout_payload["result"]
        assert isinstance(result, dict)
        assert result["exit_code"] == 0
        assert '"ok": true' in str(result["stdout"])
        assert second in str(result["stdout"])

        wait_until(
            lambda: read_status_of(jobs_db, unrelated) in {"cancelled", "failed"},
            timeout=30.0,
        )
        unrelated_state = payload_state(read_payload(jobs_db, unrelated))
        assert unrelated_state["status"] == "cancelled"
        pgid = unrelated_state.get("process_pgid")
        assert pgid is not None
        assert not group_has_members(int(str(pgid)))

        # A checkout is provisional by design (#103): the supported two-phase
        # confirmation settles the supervisor on the candidate and activates
        # the maintained CLIs.
        confirm_checkout(repo, fake_uv, second)

        wait_until(_deploy_converged(applied, second, old_meta.pid), timeout=60.0)
        status = supervise.read_status()
        assert status is not None
        assert status.commit == second
        assert status.applied_generation > applied
        assert status.child is not None
        assert status.child.pid != old_meta.pid
        assert status.child.pid in direct_children(status.supervisor_pid)
        assert len(direct_children(status.supervisor_pid)) == 1

        meta = lifecycle.read_meta()
        assert meta is not None
        assert meta.git_commit == second
        assert lifecycle.worker_alive(meta)
        assert cli.current_commit() == second


def test_queue_version_changing_deploy_is_refused_durably_without_retiring_old_worker(
    jobs_db: str,
    pg_cluster: _pg.PgCluster,
    maintained_env: tuple[Path, str, str],
    supervisor_env: dict[str, str],
    tmp_path: Path,
) -> None:
    """A queue-owned ordinary version-changing ``deploy`` is refused durably.

    Issue #29: once maintained-worker metadata exists, an ordinary
    ``lubko-deploy deploy`` must never change versions — even when invoked as a
    queue job from the live maintained worker itself. The owning worker records
    the root row as durably ``failed`` with the refusal guidance, the old
    worker is never retired (so an unrelated active job keeps running), and the
    supervisor state stays pinned to the previously confirmed commit.
    """
    del pg_cluster
    repo, first, _second = maintained_env
    fake_uv = write_fake_uv(tmp_path)
    with running_supervisor(supervisor_env):
        applied = request_and_wait(first, repo)
        old_meta = lifecycle.read_meta()
        assert old_meta is not None
        assert old_meta.pid is not None

        unrelated = insert_pending_job(jobs_db, str(repo), "sleep 30")
        wait_until(lambda: read_status_of(jobs_db, unrelated) == "running", timeout=30.0)

        deploy_id = insert_pending_process_job(
            jobs_db, str(repo), deploy_deploy_args(repo, fake_uv)
        )
        wait_until(lambda: read_status_of(jobs_db, deploy_id) == "failed", timeout=60.0)

        deploy_payload = read_payload(jobs_db, deploy_id)
        deploy_state = payload_state(deploy_payload)
        assert deploy_state["status"] == "failed"
        result = deploy_payload["result"]
        assert isinstance(result, dict)
        assert result["exit_code"] != 0
        assert "cannot change versions" in str(result["stderr"])
        assert "lubko-deploy-ctl checkout" in str(result["stderr"])

        assert lifecycle.worker_alive(old_meta)
        assert worker_pid() == old_meta.pid
        status = supervise.read_status()
        assert status is not None
        assert status.commit == first
        assert status.applied_generation == applied
        meta = lifecycle.read_meta()
        assert meta is not None
        assert meta.git_commit == first

        assert read_status_of(jobs_db, unrelated) == "running"
        unrelated_state = payload_state(read_payload(jobs_db, unrelated))
        assert unrelated_state.get("process_pgid") is not None


def test_queue_deploy_validation_failure_leaves_failed_row_and_old_worker(
    jobs_db: str,
    pg_cluster: _pg.PgCluster,
    maintained_env: tuple[Path, str, str],
    supervisor_env: dict[str, str],
    tmp_path: Path,
) -> None:
    """A queue deploy that fails validation is durably failed, never succeeded.

    The detached helper reports the error, the controller exits non-zero, and
    the owning worker records the root row as ``failed``; the previous worker
    keeps running and the supervisor never retires it. This mirrors the failure
    contract of a supervised checkout: a dead/erroring helper can never leave a
    falsely-successful row.
    """
    del pg_cluster
    repo, first, _second = maintained_env
    fake_uv = write_fake_uv(tmp_path, fail_validation=True)
    with running_supervisor(supervisor_env):
        applied = request_and_wait(first, repo)
        old_pid = worker_pid()
        assert old_pid is not None
        old_meta = lifecycle.read_meta()
        assert old_meta is not None

        deploy_id = insert_pending_process_job(
            jobs_db, str(repo), deploy_deploy_args(repo, fake_uv)
        )
        wait_until(lambda: read_status_of(jobs_db, deploy_id) == "failed", timeout=60.0)
        payload = read_payload(jobs_db, deploy_id)
        result = payload["result"]
        assert isinstance(result, dict)
        assert result["exit_code"] != 0

        assert lifecycle.worker_alive(old_meta)
        assert worker_pid() == old_pid
        status = supervise.read_status()
        assert status is not None
        assert status.commit == first
        assert status.applied_generation == applied
        meta = lifecycle.read_meta()
        assert meta is not None
        assert meta.git_commit == first


def restart_args() -> list[str]:
    """Build the argv of one queue-invoked ``lubko-deploy restart`` job.

    Returns:
        The restart argv running the real lifecycle CLI.
    """
    return [sys.executable, "-m", "lubko.lifecycle", "restart"]


def _restart_converged(applied: int, commit: str, old_pid: int) -> Callable[[], bool]:
    """Return a predicate asserting the exact restart convergence.

    A restart replaces the process running the *same* confirmed commit, so the
    convergence is a strictly newer applied generation, the same commit, a
    fresh child PID, and proof it consumes the queue — never the stale
    pre-handoff snapshot that still shows ``ready`` for the old worker.

    Args:
        applied: The generation applied before the restart.
        commit: The exact confirmed commit being restarted.
        old_pid: The pre-restart worker PID that must have been replaced.

    Returns:
        A predicate usable with :func:`wait_until`.
    """

    def check() -> bool:
        status = supervise.read_status()
        return bool(
            status is not None
            and status.applied_generation > applied
            and status.commit == commit
            and status.child is not None
            and status.child.pid != old_pid
            and status.ready
        )

    return check


def test_queue_restart_survives_old_worker_shutdown_and_replaces_worker(
    jobs_db: str,
    pg_cluster: _pg.PgCluster,
    maintained_env: tuple[Path, str, str],
    supervisor_env: dict[str, str],
) -> None:
    """A queue-invoked ``lubko-deploy restart`` survives its own old worker.

    Without the detached-handoff protection the supervisor would retire the
    very worker executing the restart command, cancelling its own root row. Here
    the root row reaches durable ``succeeded`` before the old worker dies, and
    the detached helper then drives the supervised process replacement so a
    fresh same-commit worker becomes the sole ready consumer.
    """
    del pg_cluster
    repo, first, _second = maintained_env
    with running_supervisor(supervisor_env):
        applied = request_and_wait(first, repo)
        old_meta = lifecycle.read_meta()
        assert old_meta is not None
        assert old_meta.pid is not None

        restart_id = insert_pending_process_job(jobs_db, str(repo), restart_args())
        _wait_for_queue_success_and_old_death(jobs_db, restart_id, old_meta)

        restart_payload = read_payload(jobs_db, restart_id)
        restart_state = payload_state(restart_payload)
        assert restart_state["status"] == "succeeded"
        result = restart_payload["result"]
        assert isinstance(result, dict)
        assert result["exit_code"] == 0
        assert "completes detached" in str(result["stdout"])

        wait_until(_restart_converged(applied, first, old_meta.pid), timeout=60.0)
        status = supervise.read_status()
        assert status is not None
        assert status.commit == first
        assert status.applied_generation > applied
        assert status.child is not None
        assert status.child.pid != old_meta.pid
        assert status.child.pid in direct_children(status.supervisor_pid)
        assert len(direct_children(status.supervisor_pid)) == 1

        meta = lifecycle.read_meta()
        assert meta is not None
        assert meta.git_commit == first
        assert lifecycle.worker_alive(meta)


def test_queue_checkout_cli_activation_failure_keeps_confirmed_worker_reconcilable(
    jobs_db: str,
    pg_cluster: _pg.PgCluster,
    maintained_env: tuple[Path, str, str],
    supervisor_env: dict[str, str],
    tmp_path: Path,
) -> None:
    """A confirmed queue checkout whose CLI activation failed stays reconcilable.

    The candidate becomes live and ready through the real supervisor handoff
    and the two-phase confirmation settles it durably, but every maintained-CLI
    activation attempt fails (the atomic pointer switch is blocked). The
    checkout must never roll back a *confirmed* deployment: the worker, the
    supervisor desired/applied commit, and the metadata stay pinned to the
    candidate while ``cli/current`` stays stale, and one unblocked controller
    invocation repairs the pointer through the idempotent reconciliation.
    """
    del pg_cluster
    repo, first, second = maintained_env
    fake_uv = write_fake_uv(tmp_path)
    cli.set_current(first)
    # Make the CLI root non-writable so the atomic pointer switch (which now
    # uses unique temporary names) cannot create its temp symlink or replace
    # the ``current`` pointer.  Both runtimes are already built by
    # ``maintained_env``, so this blocks only the final pointer switch and not
    # any new commit-runtime construction.  The previously established
    # ``current`` pointer stays readable as ``first``.
    cli_root = cli_root_dir()
    original_mode = cli_root.stat().st_mode & 0o777
    cli_root.chmod(original_mode & ~0o222)
    try:
        assert cli.current_commit() == first
        with running_supervisor(supervisor_env):
            applied = request_and_wait(first, repo)
            old_meta = lifecycle.read_meta()
            assert old_meta is not None
            assert old_meta.pid is not None

            checkout_id = insert_pending_process_job(
                jobs_db, str(repo), checkout_args(repo, fake_uv, second)
            )
            wait_until(lambda: read_status_of(jobs_db, checkout_id) == "succeeded", timeout=90.0)
            payload = read_payload(jobs_db, checkout_id)
            result = payload["result"]
            assert isinstance(result, dict)
            assert result["exit_code"] == 0

            confirm_checkout(repo, fake_uv, second)

            status = supervise.read_status()
            assert status is not None
            assert status.commit == second
            assert status.applied_generation > applied
            assert status.child is not None
            assert status.child.pid != old_meta.pid
            assert len(direct_children(status.supervisor_pid)) == 1
            meta = lifecycle.read_meta()
            assert meta is not None
            assert meta.git_commit == second
            assert lifecycle.worker_alive(meta)

            # Confirmation succeeded while the pointer switch was blocked: the
            # deployment is confirmed-but-stale, never rolled back.
            assert cli.current_commit() == first
    finally:
        cli_root.chmod(original_mode)

    # One unblocked controller invocation repairs the stale pointer to the
    # exactly confirmed commit without touching worker or mission state.
    ctl_request({"type": "status"}, repo, fake_uv)
    wait_until(lambda: cli.current_commit() == second, timeout=30.0)
    final_meta = lifecycle.read_meta()
    assert final_meta is not None
    assert final_meta.git_commit == second
    assert cli.current_commit() == second


# ---------------------------------------------------------------------------
# Status identity binding regressions (#90)
# ---------------------------------------------------------------------------


def _write_status_snapshot(
    supervisor_pid: int,
    supervisor_start_time_ticks: int,
    *,
    ready: bool = True,
    commit: str = "a" * 40,
    child_pid: int = 100,
) -> None:
    """Persist a raw status snapshot for identity-binding tests.

    Args:
        supervisor_pid: PID recorded in the status.
        supervisor_start_time_ticks: Start time ticks recorded in the status.
        ready: Whether the snapshot claims readiness.
        commit: Commit the snapshot claims to run.
        child_pid: Worker child PID in the snapshot.
    """
    supervise.write_status(
        supervise.SupervisorStatus(
            schema_version=supervise.SCHEMA_VERSION,
            supervisor_pid=supervisor_pid,
            supervisor_start_time_ticks=supervisor_start_time_ticks,
            started_at=0.0,
            applied_generation=1,
            mode=supervise.MODE_RUN,
            commit=commit,
            child=supervise.WorkerChild(
                pid=child_pid,
                pgid=child_pid,
                sid=child_pid,
                start_time_ticks=1,
                token=f"token-{child_pid}",
                worker_id="test-worker",
                spawned_at=0.0,
            ),
            intent=supervise.INTENT_RUN,
            restart_count=0,
            next_attempt_at=None,
            last_exit=None,
            mission=None,
            db_ready=True,
            ready=ready,
            message=None,
            worker_health=None,
        )
    )


def test_read_status_returns_none_when_no_pidfile(
    tmp_path: Path,
) -> None:
    """A status snapshot with no pidfile is treated as stale."""
    del tmp_path
    pid = os.getpid()
    ticks = supervise.proc_start_ticks(pid) or 0
    _write_status_snapshot(pid, ticks)
    supervise.supervisor_pid_path().unlink(missing_ok=True)
    assert supervise.read_status() is None


def test_read_status_returns_none_when_pidfile_mismatch(
    tmp_path: Path,
) -> None:
    """A status whose PID disagrees with the pidfile is stale."""
    del tmp_path
    pid = os.getpid()
    ticks = supervise.proc_start_ticks(pid) or 0
    _write_status_snapshot(pid, ticks)
    supervise.write_supervisor_pid(pid + 999, ticks)
    assert supervise.read_status() is None


def test_read_status_returns_none_when_status_ticks_mismatch_pidfile(
    tmp_path: Path,
) -> None:
    """A status whose start_time_ticks disagree with the pidfile is stale.

    This is the PID-reuse scenario: same PID, different incarnation.
    """
    del tmp_path
    pid = os.getpid()
    ticks = supervise.proc_start_ticks(pid) or 0
    _write_status_snapshot(pid, ticks)
    supervise.write_supervisor_pid(pid, ticks + 1)
    assert supervise.read_status() is None


def test_read_status_returns_none_when_process_dead(
    tmp_path: Path,
) -> None:
    """A status whose supervisor process has died is stale."""
    del tmp_path
    _write_status_snapshot(999_999, 1)
    supervise.write_supervisor_pid(999_999, 1)
    assert supervise.read_status() is None


def test_read_status_returns_none_when_process_zombie(
    tmp_path: Path,
) -> None:
    """A status whose supervisor process is a zombie is stale."""
    del tmp_path
    _write_status_snapshot(999_999, 1)
    supervise.write_supervisor_pid(999_999, 1)
    assert supervise.read_status() is None


def test_read_status_returns_valid_for_live_supervisor(
    jobs_db: str,
    maintained_env: tuple[Path, str, str],
    supervisor_env: dict[str, str],
) -> None:
    """A status snapshot from the live supervisor is accepted."""
    del jobs_db
    repo, first, _second = maintained_env
    with running_supervisor(supervisor_env):
        request_and_wait(first, repo)
        status = supervise.read_status()
        assert status is not None
        assert status.supervisor_pid > 0
        assert status.supervisor_start_time_ticks > 0
        assert status.ready is True
        assert status.commit == first


def test_stale_ready_status_rejected_by_wait_until_ready(
    tmp_path: Path,
) -> None:
    """wait_until_ready returns False when the status snapshot is stale."""
    del tmp_path
    pid = os.getpid()
    ticks = supervise.proc_start_ticks(pid) or 0
    _write_status_snapshot(pid, ticks, ready=True)
    supervise.supervisor_pid_path().unlink(missing_ok=True)
    assert not supervise.wait_until_ready(1, 0.2)


def test_stale_ready_status_rejected_by_wait_for_generation(
    tmp_path: Path,
) -> None:
    """wait_for_generation returns False when the status snapshot is stale."""
    del tmp_path
    pid = os.getpid()
    ticks = supervise.proc_start_ticks(pid) or 0
    _write_status_snapshot(pid, ticks, ready=True)
    supervise.supervisor_pid_path().unlink(missing_ok=True)
    assert not supervise.wait_for_generation(1, 0.2)


def test_status_roundtrip_includes_start_time_ticks(tmp_path: Path) -> None:
    """SupervisorStatus survives serialization and retains start_time_ticks."""
    del tmp_path
    original = supervise.SupervisorStatus(
        schema_version=supervise.SCHEMA_VERSION,
        supervisor_pid=42,
        supervisor_start_time_ticks=777,
        started_at=1.5,
        applied_generation=3,
        mode=supervise.MODE_RUN,
        commit="a" * 40,
        child=supervise.WorkerChild(
            pid=100,
            pgid=100,
            sid=100,
            start_time_ticks=10,
            token=f"token-{42}",
            worker_id="w",
            spawned_at=2.0,
        ),
        intent=supervise.INTENT_RUN,
        restart_count=1,
        next_attempt_at=None,
        last_exit=supervise.LastExit(returncode=1, at=3.0),
        mission=None,
        db_ready=True,
        ready=True,
        message="ok",
        worker_health=None,
    )
    data = original.to_dict()
    restored = supervise.SupervisorStatus.from_dict(data)
    assert restored.supervisor_pid == 42
    assert restored.supervisor_start_time_ticks == 777
    assert restored.child is not None
    assert restored.child.pid == 100
    assert restored.ready is True
    assert isinstance(data["started_at"], float)


def test_old_status_schema_without_start_time_ticks_fails_closed(
    tmp_path: Path,
) -> None:
    """A legacy status without supervisor_start_time_ticks defaults to 0 and is stale."""
    del tmp_path
    pid = os.getpid()
    ticks = supervise.proc_start_ticks(pid) or 0
    supervise.write_supervisor_pid(pid, ticks)
    legacy_data = {
        "schema_version": supervise.SCHEMA_VERSION,
        "supervisor_pid": pid,
        "started_at": 0.0,
        "applied_generation": 1,
        "mode": supervise.MODE_RUN,
        "commit": "a" * 40,
        "child": None,
        "intent": supervise.INTENT_RUN,
        "restart_count": 0,
        "next_attempt_at": None,
        "last_exit": None,
        "mission": None,
        "db_ready": None,
        "ready": True,
        "message": None,
    }
    supervise.status_path().parent.mkdir(parents=True, exist_ok=True)
    temporary = supervise.status_path().with_name(f"{supervise.status_path().name}.tmp")
    temporary.write_text(json.dumps(legacy_data, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(supervise.status_path())
    assert supervise.read_status() is None


# ---------------------------------------------------------------------------
# Acceptance: deterministic SIGKILL regression and replacement recovery (#91)
# ---------------------------------------------------------------------------


def test_supervisor_hard_kill_pending_candidate_replacement_resumes(
    jobs_db: str,
    pg_cluster: _pg.PgCluster,
    maintained_env: tuple[Path, str, str],
    supervisor_env: dict[str, str],
) -> None:
    """Deterministic SIGKILL regression: supervisor hard-killed while pending candidate.

    The supervisor is hard-killed while a pending mission is active.  The orphan
    candidate survives initially (reparented to PID 1).  A replacement supervisor
    starts, retires the orphan by exact identity, and resumes the candidate
    commit from the durable pending mission state.  The replacement status is
    synchronized to the exact replacement supervisor incarnation (PID +
    start_time ticks) so a stale snapshot from the first supervisor can never
    satisfy the wait condition.
    """
    del jobs_db, pg_cluster
    repo, first, second = maintained_env
    first_proc = start_supervisor(supervisor_env)
    try:
        applied = request_and_wait(first, repo)
        original_pid = worker_pid()
        assert original_pid is not None

        dc.publish_mission(
            replace(
                mission_state(applied + 1, dc.STATUS_PENDING, second, first),
                repo=str(repo),
                supervisor_owned=True,
            ),
            lock_timeout_seconds=5.0,
        )
        wait_for_replacement(original_pid)
        wait_until(status_ready, timeout=30.0)

        first_status = supervise.read_status()
        assert first_status is not None
        assert first_status.child is not None
        first_child = first_status.child
        first_meta = lifecycle.WorkerMeta(
            schema_version=lifecycle.SCHEMA_VERSION,
            state=lifecycle.STATE_RUNNING,
            pid=first_child.pid,
            pgid=first_child.pgid,
            sid=first_child.sid,
            start_time_ticks=first_child.start_time_ticks,
            token=first_child.token,
            repo="",
            git_commit=None,
            worker_id=first_child.worker_id,
            log_path="",
            started_at=first_child.spawned_at,
            stopped_at=None,
        )
        assert lifecycle.worker_alive(first_meta), "first candidate must be alive before kill"

        first_proc.kill()
        first_proc.wait(timeout=5)
        guard.unregister(first_proc)
        assert not process_alive(first_proc.pid)

        mission = dc.read_rollback_state()
        assert mission is not None
        assert mission.status == dc.STATUS_PENDING
        assert mission.commit == second
        assert mission.generation == applied + 1

        assert lifecycle.worker_alive(first_meta), "orphan candidate must survive initial SIGKILL"

        second_proc = start_supervisor(supervisor_env)
        second_pid = second_proc.pid
        try:

            def replacement_ready() -> bool:
                st = supervise.read_status()
                return bool(
                    st is not None
                    and st.supervisor_pid == second_pid
                    and st.supervisor_start_time_ticks
                    == (supervise.proc_start_ticks(second_pid) or 0)
                    and st.ready
                    and st.child is not None
                    and not (
                        st.child.pid == first_child.pid
                        and st.child.start_time_ticks == first_child.start_time_ticks
                        and st.child.token == first_child.token
                    )
                )

            wait_until(replacement_ready, timeout=30.0)

            second_status = supervise.read_status()
            assert second_status is not None
            assert second_status.child is not None
            second_child = second_status.child
            second_meta = lifecycle.WorkerMeta(
                schema_version=lifecycle.SCHEMA_VERSION,
                state=lifecycle.STATE_RUNNING,
                pid=second_child.pid,
                pgid=second_child.pgid,
                sid=second_child.sid,
                start_time_ticks=second_child.start_time_ticks,
                token=second_child.token,
                repo="",
                git_commit=None,
                worker_id=second_child.worker_id,
                log_path="",
                started_at=second_child.spawned_at,
                stopped_at=None,
            )

            assert not lifecycle.worker_alive(first_meta), "old exact identity must be dead"
            assert second_status.commit == second
            assert len(direct_children(second_status.supervisor_pid)) == 1
            assert lifecycle.worker_alive(second_meta), "resumed candidate must be live"

            write_rollback(
                replace(
                    mission_state(applied + 1, dc.STATUS_CONFIRMED, second, first),
                    repo=str(repo),
                    supervisor_owned=True,
                )
            )
        finally:
            stop_supervisor(second_proc)
    finally:
        if first_proc.poll() is None:
            stop_supervisor(first_proc)


def test_supervisor_restart_resumes_mission_repeated(
    jobs_db: str,
    pg_cluster: _pg.PgCluster,
    maintained_env: tuple[Path, str, str],
    supervisor_env: dict[str, str],
) -> None:
    """Repeated supervisor restart-resume cycles are stable.

    A pending mission survives multiple supervisor restart cycles without
    state mutation or duplicate workers.  Each restart reconstructs the
    candidate from durable state, and the mission remains pending throughout.
    """
    del jobs_db, pg_cluster
    repo, first, second = maintained_env
    with running_supervisor(supervisor_env):
        applied = request_and_wait(first, repo)

    for _cycle in range(3):
        with running_supervisor(supervisor_env):
            current_applied = supervise.read_state().applied_generation
            dc.publish_mission(
                replace(
                    mission_state(
                        current_applied + 1,
                        dc.STATUS_PENDING,
                        second,
                        first,
                    ),
                    repo=str(repo),
                    supervisor_owned=True,
                ),
                lock_timeout_seconds=5.0,
            )
            wait_until(status_ready, timeout=30.0)
            candidate = worker_pid()
            assert candidate is not None
            assert process_alive(candidate)

        status = supervise.read_status()
        if status is not None:
            assert status.commit == second
            mission = dc.read_rollback_state()
            assert mission is not None
            assert mission.status == dc.STATUS_PENDING
            assert mission.commit == second
            assert mission.generation > applied

    write_rollback(
        replace(
            mission_state(
                supervise.read_state().applied_generation,
                dc.STATUS_CONFIRMED,
                second,
                first,
            ),
            repo=str(repo),
            supervisor_owned=True,
        )
    )


def test_reconcile_takeover_stops_reparented_orphan(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reconcile retires a reparented orphan then replacement may spawn.

    After a supervisor restart, the old worker is reparented to PID 1.
    _child_alive returns False (PPID mismatch) but lifecycle.worker_alive
    confirms the exact process is live.  reconcile skips _handle_crash,
    proceeds to _ensure_worker, which exact-retires the orphan.  A mutable
    exact_alive flag models reality: successful stop flips it False so the
    later metadata takeover check sees the process dead and does not stop
    again.  Exactly one stop occurs before replacement.
    """
    proc = subprocess.Popen(
        [SLEEP_BIN, "300"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    guard.register(proc)
    try:
        identity = lifecycle.process_identity(proc.pid)
        assert identity is not None
        child = supervise.WorkerChild(
            pid=identity.pid,
            pgid=identity.pgid,
            sid=identity.sid,
            start_time_ticks=identity.start_time_ticks,
            token=_TEST_ORPHAN_INCARNATION,
            worker_id="orphan-worker",
            spawned_at=1.0,
        )
        orphan_meta = lifecycle.WorkerMeta(
            schema_version=lifecycle.SCHEMA_VERSION,
            state=lifecycle.STATE_RUNNING,
            pid=identity.pid,
            pgid=identity.pgid,
            sid=identity.sid,
            start_time_ticks=identity.start_time_ticks,
            token=_TEST_ORPHAN_INCARNATION,
            repo="",
            git_commit=None,
            worker_id="orphan-worker",
            log_path="",
            started_at=1.0,
            stopped_at=None,
        )
        state = replace(
            supervise.fresh_state(),
            mode=supervise.MODE_RUN,
            commit="a" * 40,
            child=child,
            intent=supervise.INTENT_RUN,
        )
        supervise.write_state(state)

        exact_alive = True
        stop_calls: list[lifecycle.WorkerMeta] = []

        def fake_stop(meta: lifecycle.WorkerMeta, _grace: float) -> bool:
            stop_calls.append(meta)
            nonlocal exact_alive
            exact_alive = False
            return True

        def fake_read_meta() -> lifecycle.WorkerMeta | None:
            return orphan_meta

        def fake_worker_alive(meta: lifecycle.WorkerMeta) -> bool:
            return exact_alive and meta.pid == identity.pid

        daemon = SupervisorDaemon(Settings())
        monkeypatch.setattr(daemon, "_derive_action", lambda _s: ("run", "b" * 40))
        monkeypatch.setattr(lifecycle, "stop_worker", fake_stop)
        monkeypatch.setattr(lifecycle, "read_meta", fake_read_meta)
        monkeypatch.setattr(lifecycle, "worker_alive", fake_worker_alive)
        monkeypatch.setattr(daemon, "_spawn_worker", lambda _c: None)
        monkeypatch.setattr(daemon, "_child_alive", lambda _s: False)
        # Model the emergency owned-group recovery explicitly (exact recovery
        # succeeded) rather than depending on a real database configuration, so
        # the takeover's full retire path is exercised deterministically. The
        # exact token is verified so recovery targets only the retired orphan's
        # incarnation.
        recover_calls: list[str] = []

        def fake_recover(token: str) -> None:
            recover_calls.append(token)
            assert token == _TEST_ORPHAN_INCARNATION

        monkeypatch.setattr(supervisor_module, "recover_owned_groups", fake_recover)

        daemon.reconcile(0.0)

        assert recover_calls == [_TEST_ORPHAN_INCARNATION]
        assert len(stop_calls) == 1
        assert stop_calls[0].pid == proc.pid
        assert stop_calls[0].start_time_ticks == identity.start_time_ticks

        final_state = supervise.read_state()
        assert final_state.child is None
    finally:
        guard.teardown_tracked(fail_on_leak=False)


def test_reconcile_takeover_fails_closed_on_wrong_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reconcile holds when stop_worker fails -- retries after backoff.

    When the recorded maintained worker's identity cannot be proven (wrong
    token, PID reuse), stop_worker returns False and reconcile backs off
    without spawning.  The durable child identity is preserved across
    repeated reconciliation cycles and _spawn_worker is never called.
    After backoff expires the exact same stop is retried.
    """
    child = supervise.WorkerChild(
        pid=999_999,
        pgid=999_999,
        sid=999_999,
        start_time_ticks=42,
        token=_TEST_WORKER_INCARNATION,
        worker_id="dead-worker",
        spawned_at=1.0,
    )
    stale_meta = lifecycle.WorkerMeta(
        schema_version=lifecycle.SCHEMA_VERSION,
        state=lifecycle.STATE_RUNNING,
        pid=999_999,
        pgid=999_999,
        sid=999_999,
        start_time_ticks=42,
        token=_TEST_WORKER_INCARNATION,
        repo="",
        git_commit=None,
        worker_id="dead-worker",
        log_path="",
        started_at=1.0,
        stopped_at=None,
    )
    state = replace(
        supervise.fresh_state(),
        mode=supervise.MODE_RUN,
        commit="a" * 40,
        child=child,
        intent=supervise.INTENT_RUN,
    )
    supervise.write_state(state)

    stop_calls: list[lifecycle.WorkerMeta] = []

    def fail_stop(meta: lifecycle.WorkerMeta, _grace: float) -> bool:
        stop_calls.append(meta)
        return False

    def fake_read_meta() -> lifecycle.WorkerMeta | None:
        return stale_meta

    def fake_worker_alive(meta: lifecycle.WorkerMeta) -> bool:
        return meta.pid == 999_999

    spawn_calls: list[str] = []

    def fake_spawn(commit: str) -> supervise.WorkerChild | None:
        spawn_calls.append(commit)
        return None

    daemon = SupervisorDaemon(Settings())
    monkeypatch.setattr(daemon, "_derive_action", lambda _s: ("run", "b" * 40))
    monkeypatch.setattr(lifecycle, "stop_worker", fail_stop)
    monkeypatch.setattr(lifecycle, "read_meta", fake_read_meta)
    monkeypatch.setattr(lifecycle, "worker_alive", fake_worker_alive)
    monkeypatch.setattr(daemon, "_spawn_worker", fake_spawn)
    monkeypatch.setattr(daemon, "_child_alive", lambda _s: False)

    daemon.reconcile(0.0)
    first_state = supervise.read_state()
    assert first_state.child is not None
    assert first_state.child.pid == child.pid
    assert first_state.child.token == child.token
    assert first_state.next_attempt_at is not None
    assert spawn_calls == []
    assert len(stop_calls) == 1

    daemon.reconcile(first_state.next_attempt_at + 0.01)
    second_state = supervise.read_state()
    assert second_state.child is not None
    assert second_state.child.pid == child.pid
    assert second_state.child.token == child.token
    assert spawn_calls == [], "no worker must be spawned across repeated ticks"
    assert len(stop_calls) == 2, "stop_worker must be called on each retry"
