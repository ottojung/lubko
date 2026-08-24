"""Shared pytest fixtures enforcing deterministic process teardown.

The container runs under a real reaping PID 1 (tini), so this harness never
installs a reaper or calls ``waitpid(-1)``: after every test, any still-live
test-created process group is stopped by exact identity and the test fails
loudly if it leaked a process, and at the end of the session every tracked
group is asserted to be gone.

The suite is also fail-safe by default against the live Lubko worker: every
test runs with every XDG-backed Lubko state root redirected to a pytest-owned
temporary directory before any lifecycle path can resolve it, destructive test
helpers fail closed when the state root is not test-owned, and an ambient
"production-like" sentinel state tree and live process prove that nothing in
the suite ever mutates ambient state or signals an ambient process.
"""

from __future__ import annotations

import logging
import os
import shutil
import signal
import subprocess
from contextlib import suppress
from pathlib import Path
from typing import TYPE_CHECKING, Final

import pytest

from lubko import deployctl as dc
from lubko import lifecycle
from tests import _isolation as isolation
from tests import _pg
from tests import _process_guard as guard

if TYPE_CHECKING:
    from collections.abc import Iterator

LOGGER = logging.getLogger(__name__)

REPO_ROOT: Final = Path(__file__).resolve().parent.parent
BASELINE_MIGRATION: Final = REPO_ROOT / "migrations" / "0001_two_column_protocol.sql"
SLEEP_BIN: Final = shutil.which("sleep") or "/bin/sleep"

# The exact corruption signature from the 2026-08-15 incident: the deployment
# E2E helpers record this worker id into lifecycle metadata. The sentinel
# ambient worker deliberately carries it so any test that reads or writes
# ambient lifecycle state reproduces the incident signature.
AMBIENT_WORKER_ID: Final = "test-worker"
AMBIENT_TOKEN: Final = "ambient-sentinel-token"  # ruff: ignore[hardcoded-password-string] - test token


@pytest.fixture(scope="session", autouse=True)
def _record_test_temp_base(tmp_path_factory: pytest.TempPathFactory) -> Iterator[None]:
    """Record the pytest session temp base for the ownership guards.

    Args:
        tmp_path_factory: Pytest temporary path factory.

    Yields:
        Nothing while the suite runs.
    """
    isolation.TEST_BASETEMP = tmp_path_factory.getbasetemp()
    try:
        yield
    finally:
        isolation.TEST_BASETEMP = None


@pytest.fixture(autouse=True)
def _isolated_lubko_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Redirect every XDG-backed Lubko state root for the current test.

    All per-user XDG home variables that Lubko's code resolves are pointed at
    this test's pytest-owned temporary directory before any test code runs, so
    no lifecycle/deploy/CLI/toolchain/agent state can resolve to the live user
    state tree. Subprocesses spawned by tests inherit the isolated variables
    because they copy or inherit ``os.environ``.

    Args:
        tmp_path: Pytest temporary directory for the current test.
        monkeypatch: Pytest monkeypatch fixture.

    Returns:
        The pytest-owned XDG root for the current test.
    """
    root = tmp_path / "xdg"
    isolation.CURRENT_TEST_TMP = tmp_path
    monkeypatch.setenv("XDG_STATE_HOME", str(root / "state"))
    monkeypatch.setenv("XDG_DATA_HOME", str(root / "share"))
    monkeypatch.setenv("XDG_CACHE_HOME", str(root / "cache"))
    monkeypatch.setenv("XDG_CONFIG_HOME", str(root / "config"))
    monkeypatch.setenv("XDG_BIN_HOME", str(root / "bin"))
    # The suite can legitimately run from inside a Lubko job whose ambient
    # environment carries LUBKO_JOB_ID. Queue-ownership detection (deployctl
    # checkout, queue-invoked deploy) keys off that exact injected variable, so
    # the ambient job identity is cleared: a test is only queue-invoked when it
    # deliberately sets the variable itself.
    monkeypatch.delenv("LUBKO_JOB_ID", raising=False)
    # The same applies to the runner identity markers. A test-spawned process
    # inherits ``os.environ``, so ambient LUBKO_AGENT_ID / LUBKO_RUNNER_GEN
    # values (e.g. when the suite itself runs inside a Lubko job) would make
    # unrelated helper processes look like live runners of an arbitrary agent
    # and generation. Exact-identity tests must set these themselves.
    monkeypatch.delenv("LUBKO_AGENT_ID", raising=False)
    monkeypatch.delenv("LUBKO_RUNNER_GEN", raising=False)
    return root


def _spawn_ambient_sentinel() -> subprocess.Popen[bytes]:
    """Spawn the ambient "live worker" sentinel process.

    The process is a real session/process-group leader carrying a lifecycle
    token in its environment, exactly like a maintained worker. It is never
    registered with the process guard, so teardown never stops it; the session
    fixture alone asserts it survives the entire suite and then reaps it.

    Returns:
        The sentinel process.
    """
    env = dict(os.environ)
    env["LUBKO_LIFECYCLE_TOKEN"] = AMBIENT_TOKEN
    env["LUBKO_WORKER_ID"] = AMBIENT_WORKER_ID
    return subprocess.Popen(
        [SLEEP_BIN, "86400"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        close_fds=True,
        env=env,
    )


def _build_ambient_tree(root: Path, sentinel_pid: int) -> Path:
    """Build a production-like Lubko state tree under ``root``.

    The tree mirrors the ambient state the incident showed: lifecycle metadata
    naming the sentinel live worker as ``test-worker``, stale terminal rollback
    state, readiness markers, toolchain state, maintained CLI roots/pointer,
    deployment logs, and arbitrary future-looking files.

    Args:
        root: Base directory for the ambient tree.
        sentinel_pid: Exact process identity of the sentinel live worker.

    Returns:
        The ``.../lubko`` state root.
    """
    lubko_root = root / "lubko"
    worker_dir = lubko_root / "worker"
    worker_dir.mkdir(parents=True)
    meta = {
        "schema_version": 1,
        "state": "running",
        "pid": sentinel_pid,
        "pgid": sentinel_pid,
        "sid": sentinel_pid,
        "start_time_ticks": 424_242,
        "token": AMBIENT_TOKEN,
        "repo": "/workspace/.lubko-deployment",
        "git_commit": "7" * 40,
        "worker_id": AMBIENT_WORKER_ID,
        "log_path": str(worker_dir / "worker.log"),
        "started_at": 1_786_836_666.0,
        "stopped_at": None,
    }
    (worker_dir / "meta.json").write_text(
        __import__("json").dumps(meta, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    stale_rollback = {
        "schema_version": 1,
        "status": "confirmed",
        "commit": "8" * 40,
        "previous_commit": "9" * 40,
        "challenge_hash": "0" * 64,
        "deadline": 1_786_000_000.0,
        "repo": "/workspace/.lubko-deployment",
        "uv_path": "/nonexistent/uv",
        "stop_grace_seconds": 5.0,
        "git_timeout_seconds": 10.0,
        "previous_retiring": True,
        "previous_meta": {
            "schema_version": 1,
            "state": "running",
            "pid": 434_468,
            "pgid": 434_468,
            "sid": 434_468,
            "start_time_ticks": 169_233_112,
            "token": "stale-previous",
            "repo": "/workspace/.lubko-deployment",
            "git_commit": "9" * 40,
            "worker_id": "stale-worker",
            "log_path": str(worker_dir / "worker.log"),
            "started_at": 1_786_820_129.0,
            "stopped_at": None,
        },
        "new_meta": {
            "schema_version": 1,
            "state": "running",
            "pid": 553_520,
            "pgid": 553_520,
            "sid": 553_520,
            "start_time_ticks": 170_410_748,
            "token": "stale-new",
            "repo": "/workspace/.lubko-deployment",
            "git_commit": "8" * 40,
            "worker_id": "stale-worker",
            "log_path": str(worker_dir / "worker.log"),
            "started_at": 1_786_831_905.0,
            "stopped_at": None,
        },
    }
    (worker_dir / "rollback.json").write_text(
        __import__("json").dumps(stale_rollback, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (worker_dir / ".deploy.lock").write_bytes(b"")
    (worker_dir / "worker.log").write_text(
        "2026-08-15T23:31:06 sentinel worker log\n",
        encoding="utf-8",
    )
    (worker_dir / "deploy.log").write_text(
        "2026-08-15T23:31:06 deployed sentinel commit\n",
        encoding="utf-8",
    )
    (worker_dir / "deployctl.log").write_text("rollback: completed\n", encoding="utf-8")
    (worker_dir / "recovery-worker.log").write_text(
        "recovery bridge log\n",
        encoding="utf-8",
    )
    (worker_dir / f"ready-{AMBIENT_TOKEN}.json").write_text(
        __import__("json").dumps({
            "v": 1,
            "pid": sentinel_pid,
            "token": AMBIENT_TOKEN,
            "written_at": 1_786_836_666.0,
        })
        + "\n",
        encoding="utf-8",
    )
    (worker_dir / "ready-garbage.json").write_text("not json\n", encoding="utf-8")
    (worker_dir / "future-state.json").write_text('{"future": "field"}\n', encoding="utf-8")
    (lubko_root / "toolchain.json").write_text(
        __import__("json").dumps(
            {"schema_version": 1, "uv_path": "/nonexistent/uv"}, indent=2, sort_keys=True
        )
        + "\n",
        encoding="utf-8",
    )
    (lubko_root / "last.txt").write_text("42\n", encoding="utf-8")
    (lubko_root / "agents" / "deadbeef" / "meta.json").parent.mkdir(parents=True)
    (lubko_root / "agents" / "deadbeef" / "meta.json").write_text(
        '{"id": "deadbeef", "state": "succeeded"}\n',
        encoding="utf-8",
    )
    cli_dir = lubko_root / "cli"
    commit_root = cli_dir / ("a" * 40)
    (commit_root / ".venv" / "bin").mkdir(parents=True)
    (commit_root / ".venv" / "bin" / "lubko-agent").write_text(
        "#!/bin/sh\necho sentinel\n",
        encoding="utf-8",
    )
    (cli_dir / "current").symlink_to("a" * 40)
    cutover = lubko_root / "cutover"
    cutover.mkdir()
    (cutover / "bridge.log").write_text("sentinel cutover\n", encoding="utf-8")
    return lubko_root


@pytest.fixture(scope="session", autouse=True)
def _ambient_production_sentinel(
    tmp_path_factory: pytest.TempPathFactory,
) -> Iterator[None]:
    """Prove the whole suite never touches ambient state or processes.

    A production-like sentinel state tree and a real live worker process are
    created before the first test and left running for the entire session. Any
    test that escapes the XDG isolation and reads, writes, or signals this
    ambient state either fails its own assertions or trips the session-end
    check that the sentinel process is still alive and its tree is unchanged.

    Args:
        tmp_path_factory: Pytest temporary path factory.

    Yields:
        Nothing while the suite runs.

    Raises:
        AssertionError: If the ambient tree was mutated or the sentinel died
            during the session.
    """
    base = tmp_path_factory.mktemp("ambient-production")
    sentinel = _spawn_ambient_sentinel()
    isolation.AMBIENT_SENTINEL_PID = sentinel.pid
    isolation.AMBIENT_STATE_ROOT = _build_ambient_tree(base, sentinel.pid)
    ambient_root = isolation.AMBIENT_STATE_ROOT
    digest_before = isolation.snapshot_tree(ambient_root)
    try:
        yield
    finally:
        isolation.AMBIENT_SENTINEL_PID = None
        isolation.AMBIENT_STATE_ROOT = None
        with suppress(OSError):
            if sentinel.poll() is None:
                os.killpg(sentinel.pid, signal.SIGKILL)
            sentinel.wait(timeout=10)
        digest_after = isolation.snapshot_tree(ambient_root)
        if digest_before != digest_after:
            msg = "the suite mutated the ambient production-like state tree"
            raise AssertionError(msg)


@pytest.fixture(scope="session", autouse=True)
def _session_process_teardown() -> Iterator[None]:
    """Assert no test-created process survives the whole session.

    Yields:
        Nothing while the suite runs.
    """
    yield
    stopped = guard.teardown_tracked()
    guard.assert_no_live_tracked()
    if stopped:
        LOGGER.debug("session teardown stopped %d leaked process(es)", stopped)


@pytest.fixture(autouse=True)
def _process_teardown(
    _isolated_lubko_state: Path,
) -> Iterator[None]:
    """Own and deterministically stop every process a test creates.

    Takes an explicit parameter dependency on ``_isolated_lubko_state`` so
    pytest guarantees the test-owned XDG root is still authoritative during
    cleanup.  The ``yield`` is inside ``try/finally`` so cleanup always
    runs even when the test body raises.  After the guard stops tracked
    processes, a bounded loop reads test-owned lifecycle metadata and
    rollback state, verifies each recorded identity with
    ``lifecycle.worker_alive(meta)``, and signals only exact verified
    groups.  A late watchdog rollback candidate is caught in a second pass.

    Args:
        _isolated_lubko_state: The pytest-owned XDG root for this test.

    Yields:
        Nothing while one test runs.
    """
    try:
        yield
    finally:
        os.environ["XDG_STATE_HOME"] = str(_isolated_lubko_state / "state")
        stopped = guard.teardown_tracked()
        _cleanup_recorded_workers()
        if stopped:
            LOGGER.debug("test teardown stopped %d leaked process(es)", stopped)
        isolation.CURRENT_TEST_TMP = None


def _cleanup_recorded_workers() -> None:
    """Read test-owned lifecycle meta/rollback, verify and signal exact identities.

    Uses ``lifecycle.worker_alive(meta)`` which validates PID/start-ticks/
    PGID/SID/token, and ``lifecycle.stop_worker(meta)`` for exact TERM/KILL/
    group wait.  If a PENDING rollback mission exists it is disarmed before
    killing the candidate so its watchdog cannot spawn another replacement.
    """
    isolation.assert_test_owned_state_root()
    recorded: list[lifecycle.WorkerMeta] = []
    meta = lifecycle.read_meta()
    if meta is not None:
        recorded.append(meta)
    try:
        state = dc.read_rollback_state()
    except dc.DeployCtlError:
        state = None
    if state is not None:
        recorded.append(state.new_meta)
        if state.status == dc.STATUS_PENDING:
            dc.archive_mission(state, dc.STATUS_ROLLED_BACK)
    for rw in recorded:
        if rw.pid is None:
            continue
        if not lifecycle.worker_alive(rw):
            continue
        lifecycle.stop_worker(rw, 5.0)


@pytest.fixture(scope="module")
def pg_cluster(tmp_path_factory: pytest.TempPathFactory) -> Iterator[_pg.PgCluster]:
    """Start an isolated PostgreSQL cluster, or skip when unavailable.

    Args:
        tmp_path_factory: Pytest temporary path factory.

    Yields:
        The running cluster.
    """
    binaries = _pg.postgres_binaries()
    if binaries is None:
        pytest.skip("PostgreSQL server binaries not available on this host")
    root = tmp_path_factory.mktemp("lubko-pg")
    data_dir = root / "data"
    socket_dir = root / "sock"
    socket_dir.mkdir()
    port = _pg.free_port()
    env = dict(os.environ)
    lib = _pg.postgres_lib_dir(Path(binaries["postgres"]).parent)
    if lib is not None:
        env["LD_LIBRARY_PATH"] = lib
    subprocess.run(
        [
            binaries["initdb"],
            "-D",
            str(data_dir),
            "-U",
            "postgres",
            "--auth=trust",
        ],
        env=env,
        check=True,
        capture_output=True,
    )
    current = _pg.PgCluster(binaries, data_dir, socket_dir, port, env)
    current.start()
    try:
        yield current
    finally:
        current.stop()


@pytest.fixture
def jobs_db(pg_cluster: _pg.PgCluster) -> str:
    """Apply the canonical baseline on a fresh ``lubko.jobs`` table.

    Args:
        pg_cluster: The running PostgreSQL cluster.

    Returns:
        A connection string usable with :func:`psycopg.connect`.
    """
    with __import__("psycopg").connect(pg_cluster.conninfo()) as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS lubko")
        conn.execute("DROP TABLE IF EXISTS lubko.jobs CASCADE")
        conn.execute(BASELINE_MIGRATION.read_text(encoding="utf-8"))
    return pg_cluster.conninfo()


@pytest.fixture(autouse=True)
def _worker_server_config(
    _isolated_lubko_state: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Path:
    """Provide a valid private worker configuration for the current test.

    The worker and every deploy-side probe read the execution-server identity
    from the restricted worker configuration file. Each test gets a fresh
    ``worker.conf`` with a non-empty server; tests that need another identity
    (or a deliberately missing/corrupt config) rewrite or remove this file.
    ``LUBKO_WORKER_CONFIG`` pins the path so tests that switch
    ``XDG_CONFIG_HOME`` (e.g. dual-stack isolation tests) still resolve the
    per-test private configuration.

    Args:
        _isolated_lubko_state: The pytest-owned XDG root for this test.
        monkeypatch: Pytest monkeypatch fixture.

    Returns:
        The worker configuration file path.
    """
    config_dir = Path(os.environ["XDG_CONFIG_HOME"]) / "lubko"
    config_dir.mkdir(parents=True, exist_ok=True)
    config_file = config_dir / "worker.conf"
    config_file.write_text("server = alpha-server\n", encoding="utf-8")
    config_file.chmod(0o600)
    monkeypatch.setenv("LUBKO_WORKER_CONFIG", str(config_file))
    return config_file


def write_worker_server_config(server: str) -> Path:
    """Rewrite the isolated worker configuration with one server identity.

    Args:
        server: Non-empty execution-server identity to configure.

    Returns:
        The worker configuration file path.
    """
    config_file = Path(
        os.environ.get("LUBKO_WORKER_CONFIG")
        or Path(os.environ["XDG_CONFIG_HOME"]) / "lubko" / "worker.conf"
    )
    config_file.parent.mkdir(parents=True, exist_ok=True)
    config_file.write_text(f"server = {server}\n", encoding="utf-8")
    config_file.chmod(0o600)
    return config_file
