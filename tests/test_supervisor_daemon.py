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
from typing import TYPE_CHECKING, Any, Final, cast

import psycopg
import pytest

from lubko import cli, lifecycle, supervise
from lubko import deployctl as dc
from lubko.state import cli_root_dir, rollback_state_path
from lubko.supervisor import Settings, SupervisorDaemon
from lubko.supervisor import main as supervisor_main
from lubko.worker import group_has_members
from tests import _isolation as isolation
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


def _assert_env_state_home_test_owned(env: dict[str, str]) -> None:
    """Fail closed unless ``env[XDG_STATE_HOME]`` is the current test-owned root.

    The check is on the caller-supplied ``env`` dict, not ``os.environ``, so
    a caller that passes an ambient environment is caught before any
    state-writing helper can use it.

    Args:
        env: Caller-supplied environment dictionary.

    Raises:
        AssertionError: If ``XDG_STATE_HOME`` is missing, unset, or resolves
            outside the current test's temporary directory.
    """
    if isolation.CURRENT_TEST_TMP is None:
        msg = "test state isolation was not established before starting a supervisor"
        raise AssertionError(msg)
    raw = env.get(isolation.STATE_HOME_ENV)
    if not raw:
        msg = (
            f"{isolation.STATE_HOME_ENV} is unset in the supplied env; "
            "state would resolve to the live user state root"
        )
        raise AssertionError(msg)
    resolved = Path(raw).resolve()
    test_tmp = isolation.CURRENT_TEST_TMP.resolve()
    if resolved != test_tmp and test_tmp not in resolved.parents:
        msg = (
            f"{isolation.STATE_HOME_ENV}={resolved} in the supplied env is not under "
            f"the current test's pytest-owned temporary directory {test_tmp}; "
            "refusing to launch a supervisor against non-test state"
        )
        raise AssertionError(msg)


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

    Refuses to launch when the caller-supplied ``env[XDG_STATE_HOME]`` does
    not resolve under the current pytest-owned temporary directory.  The check
    is on the *supplied* ``env`` dict, not ``os.environ``, so a caller that
    passes an ambient environment is caught before the daemon can write or
    read any state.

    Args:
        env: Environment for the daemon (and its worker child).

    Returns:
        The daemon process.
    """
    _assert_env_state_home_test_owned(env)
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


def _stop_orphaned_worker_children() -> None:
    """Stop any worker child the supervisor daemon owned.

    The supervisor's durable state records the exact worker child identity
    (PID, PGID, SID, start_time_ticks, token).  After the daemon exits,
    those worker processes are orphaned to PID 1 because they run in their
    own session.  This helper reads the state and stops the worker — but
    ONLY after verifying the recorded identity is the exact live process
    that was observed during this test execution.

    Safety contract:
    1.  ``assert_test_owned_state_root()`` — refuse to read durable state
        unless the resolved state root is the current pytest-owned root;
    2.  lifecycle.worker_alive — the recorded PID/start-time/tokens must
        match a live process right now, exactly like production;
    3.  group_has_members — the recorded PGID must still have members.

    If any check fails the helper aborts (fail-closed); a stale or forged
    child identity is never signalled.
    """
    isolation.assert_test_owned_state_root()
    state = supervise.read_state()
    child = state.child
    if child is None:
        return
    meta = lifecycle.WorkerMeta(
        schema_version=lifecycle.SCHEMA_VERSION,
        state=lifecycle.STATE_RUNNING,
        pid=child.pid,
        pgid=child.pgid,
        sid=child.sid,
        start_time_ticks=child.start_time_ticks,
        token=child.token,
        repo="",
        git_commit=None,
        worker_id=child.worker_id,
        log_path="",
        started_at=child.spawned_at,
        stopped_at=None,
    )
    if not lifecycle.worker_alive(meta):
        return
    if not group_has_members(child.pgid):
        return
    with suppress(ProcessLookupError):
        os.killpg(child.pgid, signal.SIGKILL)
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline and group_has_members(child.pgid):
        time.sleep(0.02)


def stop_supervisor(proc: subprocess.Popen[bytes]) -> None:
    """Gracefully stop the supervisor so it retires its worker child.

    After the supervisor process exits (gracefully or by force), any worker
    child the daemon owned is read from the durable supervisor state and
    stopped by its exact recorded identity.  This prevents a hard-killed
    supervisor from orphaning a separately-sessioned worker that PID 1 would
    otherwise reparent.

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
    _stop_orphaned_worker_children()
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

    Refuses to write when ``XDG_STATE_HOME`` is not the current test's
    pytest-owned root, so desired intent state can never escape into ambient
    state.

    Args:
        commit: Exact commit to run.
        repo: Maintained checkout.

    Returns:
        The applied generation.
    """
    isolation.assert_test_owned_state_root()
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

    Refuses to write when ``XDG_STATE_HOME`` is not the current test's
    pytest-owned root, so rollback state can never escape into ambient state.

    Args:
        state: State to store.
    """
    isolation.assert_test_owned_state_root()
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

        write_rollback(mission_state(applied + 1, dc.STATUS_PENDING, second, first, repo=str(repo)))
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

        lapsed = mission_state(applied + 1, dc.STATUS_PENDING, second, first, repo=str(repo))
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
    """Insert a protocol v3 pending command job executing argv directly.

    Args:
        conninfo: PostgreSQL connection string.
        cwd: Working directory for the job.
        process: Non-empty argv array to execute directly.

    Returns:
        The job identifier.
    """
    payload = json.dumps({
        "v": 3,
        "type": "command",
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


def test_queue_deploy_survives_old_worker_shutdown_and_converges(
    jobs_db: str,
    pg_cluster: _pg.PgCluster,
    maintained_env: tuple[Path, str, str],
    supervisor_env: dict[str, str],
    tmp_path: Path,
) -> None:
    """A queue-invoked ``lubko-deploy deploy`` survives its own old worker.

    The production defect: a root job executes ``lubko-deploy deploy`` from a
    worker-owned process group, the external supervisor retires that very
    worker during the handoff, and the old worker's shutdown cancels the
    initiating row even though the deployment converges. Here the deploy forks a
    detached handoff helper, the root row reaches durable ``succeeded`` (never
    ``cancelled``) before the old worker dies, the helper then drives the
    supervisor handoff and activates the maintained CLIs so the CLI pointer,
    the supervisor desired+applied state, and the new worker commit converge
    without any later status reconciliation, and an unrelated active job is
    still terminated by the old worker's shutdown rather than orphaned.
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

        deploy_id = insert_pending_process_job(
            jobs_db, str(repo), deploy_deploy_args(repo, fake_uv)
        )
        _wait_for_queue_success_and_old_death(jobs_db, deploy_id, old_meta)

        deploy_payload = read_payload(jobs_db, deploy_id)
        deploy_state = payload_state(deploy_payload)
        assert deploy_state["status"] == "succeeded"
        result = deploy_payload["result"]
        assert isinstance(result, dict)
        assert result["exit_code"] == 0
        assert second in str(result["stdout"])
        assert "converges detached" in str(result["stdout"])

        wait_until(
            lambda: read_status_of(jobs_db, unrelated) in {"cancelled", "failed"},
            timeout=30.0,
        )
        unrelated_state = payload_state(read_payload(jobs_db, unrelated))
        assert unrelated_state["status"] == "cancelled"
        pgid = unrelated_state.get("process_pgid")
        assert pgid is not None
        assert not group_has_members(int(str(pgid)))

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


def _deploy_rollback_converged(applied: int, commit: str) -> Callable[[], bool]:
    """Return a predicate asserting the exact post-rollback coherence.

    After a queue deploy whose CLI activation failed, the helper settles the
    supervisor back to the previous confirmed commit and the maintained CLIs
    keep selecting it, so the live worker, the supervisor desired/applied
    commit, and ``cli/current`` all match.

    Args:
        applied: The generation applied before the deploy.
        commit: The exact previous confirmed commit to restore.

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
            and status.ready
            and cli.current_commit() == commit
        )

    return check


def test_queue_deploy_cli_activation_failure_rolls_back_and_converges(
    jobs_db: str,
    pg_cluster: _pg.PgCluster,
    maintained_env: tuple[Path, str, str],
    supervisor_env: dict[str, str],
    tmp_path: Path,
) -> None:
    """A queue deploy whose CLI activation fails rolls back to full coherence.

    The candidate becomes live and ready through the real supervisor handoff,
    but every maintained-CLI activation retry fails (the atomic pointer switch
    is blocked). The detached helper must not leave the candidate worker on the
    new commit with a stale ``cli/current``: it settles the supervisor back to
    the previous confirmed commit so the live worker, the supervisor
    desired/applied commit, and ``cli/current`` all match — with no manual
    ``lubko-deploy-ctl status`` reconciliation.
    """
    del pg_cluster
    repo, first, second = maintained_env
    fake_uv = write_fake_uv(tmp_path)
    cli.set_current(first)
    (cli_root_dir() / cli.CURRENT_TMP_NAME).mkdir()
    with running_supervisor(supervisor_env):
        applied = request_and_wait(first, repo)
        old_meta = lifecycle.read_meta()
        assert old_meta is not None
        assert old_meta.pid is not None

        deploy_id = insert_pending_process_job(
            jobs_db, str(repo), deploy_deploy_args(repo, fake_uv)
        )
        wait_until(lambda: read_status_of(jobs_db, deploy_id) == "succeeded", timeout=90.0)
        wait_until(_deploy_rollback_converged(applied, first), timeout=90.0)

        status = supervise.read_status()
        assert status is not None
        assert status.commit == first
        assert status.applied_generation > applied
        assert status.child is not None
        assert status.child.pid != old_meta.pid
        assert len(direct_children(status.supervisor_pid)) == 1
        assert cli.current_commit() == first

        meta = lifecycle.read_meta()
        assert meta is not None
        assert meta.git_commit == first
        assert lifecycle.worker_alive(meta)

        payload = read_payload(jobs_db, deploy_id)
        assert payload_state(payload)["status"] == "succeeded"
        result = payload["result"]
        assert isinstance(result, dict)
        assert result["exit_code"] == 0
        assert second in str(result["stdout"])


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
    maintained_env: tuple[Path, str, str],
    supervisor_env: dict[str, str],
) -> None:
    """A status snapshot from the live supervisor is accepted."""
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
