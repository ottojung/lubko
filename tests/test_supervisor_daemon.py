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
from contextlib import contextmanager, suppress
from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING, Final, cast

import psycopg
import pytest

from lubko import cli, lifecycle, supervise
from lubko import deployctl as dc
from lubko.state import rollback_state_path
from lubko.supervisor import Settings, SupervisorDaemon
from lubko.supervisor import main as supervisor_main
from tests import _process_guard as guard
from tests.test_cli import make_repo

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from uuid import UUID

    from tests import _pg

REPO_ROOT: Final = Path(__file__).resolve().parent.parent
BASELINE_MIGRATION: Final = REPO_ROOT / "migrations" / "0001_two_column_protocol.sql"
SLEEP_BIN: Final = shutil.which("sleep") or "/bin/sleep"
TEST_WORKER_ID: Final = "test-supervisor-worker"
LEGACY_MARKER: Final = "legacy-token"


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
        env: Environment for the daemon.

    Yields:
        The daemon process.
    """
    proc = start_supervisor(env)
    try:
        yield proc
    finally:
        stop_supervisor(proc)


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


def insert_pending_job(conninfo: str, cwd: str, command: str) -> UUID:
    """Insert a protocol v2 pending command job.

    Args:
        conninfo: PostgreSQL connection string.
        cwd: Working directory for the job.
        command: Shell command to run.

    Returns:
        The job identifier.
    """
    payload = json.dumps({
        "v": 2,
        "type": "command",
        "request": {"cwd": cwd, "command": command},
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
    *,
    repo: str = "",
) -> dc.RollbackState:
    """Build a durable supervised-deployment mission for the override tests.

    Args:
        generation: Monotonic mission generation.
        status: ``pending``, ``confirmed``, or ``rolled_back``.
        commit: Candidate commit.
        previous_commit: Previously confirmed commit.
        repo: Maintained checkout, defaults to ``""``.

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
        repo=repo,
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


def wait_for_replacement(old_pid: int) -> None:
    """Wait until the daemon records a worker child different from ``old_pid``.

    Args:
        old_pid: The previous worker child PID.
    """
    wait_until(lambda: worker_pid() is not None and worker_pid() != old_pid, timeout=30.0)


def test_repeated_crashes_never_accumulate_workers(
    maintained_env: tuple[Path, str, str],
    supervisor_env: dict[str, str],
) -> None:
    """Repeated crashes back off and never accumulate processes or zombies."""
    repo, first, _second = maintained_env
    with running_supervisor(supervisor_env):
        request_and_wait(first, repo)
        previous = worker_pid()
        assert previous is not None
        for _ in range(3):
            os.killpg(previous, signal.SIGKILL)
            wait_for_replacement(previous)
            next_pid = worker_pid()
            assert next_pid is not None
            previous = next_pid
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
    maintained_env: tuple[Path, str, str],
    supervisor_env: dict[str, str],
) -> None:
    """A hard-killed supervisor's orphaned worker is taken over, never duplicated."""
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

        write_rollback(mission_state(applied + 1, dc.STATUS_PENDING, second, first, repo=str(repo)))
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
            mission_state(applied + 1, dc.STATUS_PENDING, second, first, repo=str(repo)),
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
            mission_state(applied + 1, dc.STATUS_CONFIRMED, second, first, repo=str(repo))
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
            mission_state(applied + 1, dc.STATUS_PENDING, second, first, repo=str(repo)),
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
            mission_state(applied + 1, dc.STATUS_ROLLED_BACK, second, first, repo=str(repo))
        )


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
            mission_state(applied + 1, dc.STATUS_PENDING, second, first, repo=str(repo)),
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
            mission_state(applied + 1, dc.STATUS_CONFIRMED, second, first, repo=str(repo))
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
            mission_state(mission_gen, dc.STATUS_PENDING, second, first, repo=str(repo)),
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
            mission_state(mission_gen, dc.STATUS_CONFIRMED, second, first, repo=str(repo))
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
    daemon._tick(0.0)  # ruff: ignore[private-member-access]
    assert applied == []
    assert ensured == ["3" * 40]
