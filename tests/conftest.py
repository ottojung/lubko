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

Fixture ordering
----------------

``_isolated_lubko_state`` (autouse, per-test) sets ``CURRENT_TEST_TMP`` and
all XDG environment variables.  ``_process_teardown`` (autouse, per-test)
declares an explicit parameter dependency on ``_isolated_lubko_state`` so pytest
guarantees the XDG root is still authoritative during teardown; only after the
process guard has finished does ``CURRENT_TEST_TMP`` get cleared.  This ordering
is structurally enforced, never relying on same-scope autouse ordering.
"""

from __future__ import annotations

import logging
import os
import shutil
import signal
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING, Final

import pytest

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

    This fixture also records ``CURRENT_TEST_TMP`` so the ownership guard can
    verify state-root provenance.  The value is cleared by
    ``_process_teardown`` *after* teardown finishes, guaranteeing the isolated
    root remains authoritative throughout cleanup.

    Args:
        tmp_path: Pytest temporary directory for the current test.
        monkeypatch: Pytest monkeypatch fixture.

    Returns:
        The pytest-owned XDG root for the current test.
    """
    root = tmp_path / "xdg"
    isolation.RUNTIME.current_test_tmp = tmp_path
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

    The sentinel's PID, start-time ticks, and lifecycle token are all captured
    at spawn so ``ambient_sentinel_alive`` can verify the same process
    incarnation survived — not merely a reused PID.

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
    isolation.AMBIENT_SENTINEL_TOKEN = AMBIENT_TOKEN
    # Capture the sentinel's start time in clock ticks for incarnation proof.
    # If /proc is unreadable (for example a stripped container) the sentinel
    # cannot be incarnation-verified and must not silently fall back to a weak
    # identity — the suite fails immediately rather than accepting a reused PID.
    ticks = isolation.proc_start_ticks(sentinel.pid)
    if ticks is None:
        msg = (
            f"cannot read start-time ticks for sentinel pid {sentinel.pid}; "
            "incarnation proof requires /proc access"
        )
        raise AssertionError(msg)
    isolation.AMBIENT_SENTINEL_START_TICKS = ticks
    isolation.AMBIENT_STATE_ROOT = _build_ambient_tree(base, sentinel.pid)
    ambient_root = isolation.AMBIENT_STATE_ROOT
    digest_before = isolation.snapshot_tree(ambient_root)
    sentinel_survived = False
    try:
        yield
    finally:
        # Assert the sentinel survived the entire suite *as the same
        # incarnation* before cleaning it up.  If a test signalled the
        # ambient process or the PID was reused, this must fail loudly.
        sentinel_survived = isolation.ambient_sentinel_alive()
        isolation.AMBIENT_SENTINEL_PID = None
        isolation.AMBIENT_SENTINEL_START_TICKS = None
        isolation.AMBIENT_SENTINEL_TOKEN = None
        isolation.AMBIENT_STATE_ROOT = None
        # Always reap the sentinel, even after a failed assertion, so the
        # session never leaves an ambient sleep process behind.
        try:
            if sentinel.poll() is None:
                os.killpg(sentinel.pid, signal.SIGKILL)
            sentinel.wait(timeout=10)
        except OSError:
            pass
        digest_after = isolation.snapshot_tree(ambient_root)
        errors: list[str] = []
        if not sentinel_survived:
            errors.append("the ambient sentinel worker was killed during the suite")
        if digest_before != digest_after:
            errors.append("the suite mutated the ambient production-like state tree")
        if errors:
            raise AssertionError("; ".join(errors))


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
) -> Iterator[dict[int, int]]:
    """Own and deterministically stop every process a test creates.

    Takes an explicit parameter dependency on ``_isolated_lubko_state`` so
    pytest guarantees the test-owned XDG root is still authoritative during
    teardown.  Before teardown begins the fixture verifies that the active
    ``XDG_STATE_HOME`` is indeed a child of the isolated root, making the
    parameter use structurally mandatory rather than merely declared.

    After teardown completes and the external leak proof passes,
    ``CURRENT_TEST_TMP`` is cleared so the next test's fixture can set it
    fresh.

    The external process-list proof is *detection-only*: it compares the
    process table before and after and reports any new process whose command
    line matches a known leak marker.  It never signals any process; it is
    purely an assertion/detection proof.  The ambient sentinel and other
    higher-scope test resources are excluded from the ``allowed`` set only
    through the ``before`` snapshot (they existed before the test), never
    through a process-name allowlist.

    Args:
        _isolated_lubko_state: The pytest-owned XDG root for this test,
            returned by ``_isolated_lubko_state``.

    Yields:
        Nothing while one test runs.
    """
    # Structural gate: the isolated root must still be the active XDG state
    # home.  This is not merely a declared dependency — if monkeypatch
    # teardown or another fixture changed it, this assertion fires before
    # the process guard can observe stale state.
    state_home = Path(os.environ.get("XDG_STATE_HOME", ""))
    assert state_home.resolve().is_relative_to(_isolated_lubko_state.resolve()), (
        f"XDG_STATE_HOME={state_home} is not under the isolated root "
        f"{_isolated_lubko_state}; the ordering dependency is broken"
    )
    yield from isolation.teardown_generator()


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
    current.on_start = guard.register_persistent_fixture_incarnation
    current.on_stop = guard.unregister_persistent_fixture_incarnation
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
