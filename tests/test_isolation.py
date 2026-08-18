"""Regressions proving the whole test suite is hermetically isolated.

The validation suite must be safe to run from the same Unix user and container
as the live Lubko worker. These tests prove the default isolation is real:
state roots resolve under the current test's temporary directory, subprocesses
inherit the isolated root, the destructive lifecycle helpers fail closed when
state is not test-owned, and an ambient production-like state tree and live
worker are never mutated or signalled.
"""

from __future__ import annotations

import os
import shutil
import signal
import subprocess
import sys
import time
from contextlib import suppress
from pathlib import Path
from typing import Final

import pytest

from lubko import deployctl as dc
from lubko import lifecycle, supervise
from lubko.lifecycle import read_meta
from lubko.state import state_root
from tests import _isolation as isolation
from tests import _pg
from tests import _process_guard as guard
from tests.test_supervisor_daemon import (
    _stop_orphaned_worker_children,
    request_and_wait,
    start_supervisor,
    write_rollback,
)

SLEEP_BIN: Final = shutil.which("sleep") or "/bin/sleep"
_SENTINEL_TOKEN: Final = "sentinel-token"  # ruff: ignore[hardcoded-password-string] - test sentinel token


def _subprocess_state_home() -> str:
    """Return the XDG_STATE_HOME a plain inherited subprocess observes."""
    proc = subprocess.run(
        [sys.executable, "-c", "import os; print(os.environ.get('XDG_STATE_HOME', ''))"],
        capture_output=True,
        text=True,
        check=True,
    )
    return proc.stdout.strip()


def _subprocess_state_root() -> str:
    """Return the Lubko state root an inherited subprocess resolves."""
    proc = subprocess.run(
        [
            sys.executable,
            "-c",
            "from lubko.state import state_root; print(state_root())",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    return proc.stdout.strip()


def test_state_root_resolves_under_the_current_test_tmp(
    tmp_path: Path,
) -> None:
    """Lifecycle state resolves under the test-owned temporary root by default."""
    test_tmp = tmp_path.resolve()
    assert state_root().is_relative_to(test_tmp)
    raw = os.environ["XDG_STATE_HOME"]
    assert Path(raw).resolve().is_relative_to(test_tmp)
    assert test_tmp == isolation.CURRENT_TEST_TMP


def test_subprocesses_inherit_the_isolated_state_root() -> None:
    """Plain inherited subprocesses observe the same isolated XDG state root."""
    test_tmp = isolation.CURRENT_TEST_TMP
    assert test_tmp is not None
    expected_home = str(Path(os.environ["XDG_STATE_HOME"]).resolve())
    assert _subprocess_state_home() == expected_home
    resolved_root = Path(_subprocess_state_root()).resolve()
    assert resolved_root.is_relative_to(test_tmp.resolve())
    assert resolved_root == state_root().resolve()


def test_guard_fails_closed_when_state_home_is_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Without XDG_STATE_HOME the guard refuses instead of using live state."""
    monkeypatch.delenv("XDG_STATE_HOME", raising=False)
    with pytest.raises(AssertionError, match="unset"):
        isolation.assert_test_owned_state_root()


def test_guard_fails_closed_against_ambient_production_root(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A state root outside the current test's tmp dir is never accepted.

    This is the exact pre-fix failure mode: pointing the state root at a
    production-like tree (as the deployment E2E helpers did before isolation)
    must abort loudly rather than read, write, or signal it.
    """
    ambient = isolation.ambient_state_root().parent
    monkeypatch.setenv("XDG_STATE_HOME", str(ambient))
    with pytest.raises(AssertionError, match="not under the current test"):
        isolation.assert_test_owned_state_root()


def test_kill_recorded_workers_fails_closed_before_signal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The destructive helper refuses ambient metadata and never signals.

    The ambient production-like tree records the sentinel live worker under
    ``worker_id="test-worker"`` (the incident corruption signature). A helper
    bug that resolved state against that tree must raise before it can read or
    signal the recorded identity.
    """
    ambient = isolation.ambient_state_root().parent
    monkeypatch.setenv("XDG_STATE_HOME", str(ambient))
    with pytest.raises(AssertionError, match="not under the current test"):
        _kill_recorded_workers()
    assert isolation.ambient_sentinel_alive()
    tree = isolation.ambient_state_root()
    before = isolation.snapshot_tree(tree)
    with pytest.raises(AssertionError, match="not under the current test"):
        _kill_recorded_workers()
    assert isolation.snapshot_tree(tree) == before


def test_ambient_sentinel_survives_this_module() -> None:
    """The ambient live worker is still running after this module's tests."""
    assert isolation.ambient_sentinel_alive()


def test_supervisor_helper_refuses_ambient_state_root(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """start_supervisor fails closed when XDG_STATE_HOME points at ambient state.

    This is the exact #61 regression: the supervisor test helpers wrote
    synthetic rollback state (fake schema-3 with pid-like sentinel values)
    into the live user state tree because XDG_STATE_HOME was not verified
    before launching the daemon.
    """
    ambient = isolation.ambient_state_root().parent
    monkeypatch.setenv("XDG_STATE_HOME", str(ambient))
    with pytest.raises(AssertionError, match="not under the current test"):
        start_supervisor({"XDG_STATE_HOME": str(ambient)})


def test_supervisor_rollback_helper_refuses_ambient_state_root(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """write_rollback fails closed when XDG_STATE_HOME points at ambient state.

    Reproduces the exact corruption signature from the 2026-08-18 incident:
    synthetic a/b commits and pid-like sentinel values in a schema-3 rollback
    must never escape into the live user state tree.
    """
    ambient = isolation.ambient_state_root().parent
    monkeypatch.setenv("XDG_STATE_HOME", str(ambient))
    fake = dc.RollbackState(
        schema_version=dc.ROLLBACK_SCHEMA_VERSION,
        generation=999,
        status=dc.STATUS_PENDING,
        commit="a" * 40,
        previous_commit="b" * 40,
        challenge_hash=None,
        deadline=0.0,
        repo="",
        uv_path="",
        stop_grace_seconds=0.0,
        git_timeout_seconds=0.0,
        previous_retiring=False,
        previous_meta=None,
        new_meta=None,
    )
    with pytest.raises(AssertionError, match="not under the current test"):
        write_rollback(fake)
    tree = isolation.ambient_state_root()
    before = isolation.snapshot_tree(tree)
    with pytest.raises(AssertionError, match="not under the current test"):
        write_rollback(fake)
    assert isolation.snapshot_tree(tree) == before


def test_supervisor_request_and_wait_refuses_ambient_state_root(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """request_and_wait fails closed when XDG_STATE_HOME points at ambient state.

    The desired-intent JSON written by ``supervise.request_run`` must never
    escape into the ambient state tree through an unguarded helper.
    """
    ambient = isolation.ambient_state_root().parent
    monkeypatch.setenv("XDG_STATE_HOME", str(ambient))
    with pytest.raises(AssertionError, match="not under the current test"):
        request_and_wait("c" * 40, Path("/nonexistent"))


def _kill_recorded_workers() -> None:
    """Stand-in for the deployment helper: guard then record reads.

    Mirrors ``test_deployctl.kill_recorded_workers``: the ownership guard must
    run before any metadata is read or any recorded identity is signalled.
    """
    isolation.assert_test_owned_state_root()
    read_meta()


# ---------------------------------------------------------------------------
# Regression: forged child metadata must not cause signal of live sentinel
# ---------------------------------------------------------------------------


def test_stop_orphaned_children_refuses_forged_child_identity() -> None:
    """Durable child metadata with wrong start_time_ticks must not signal a live process.

    This is the exact #61 failure mode: the supervisor test wrote synthetic
    child state (pid-like sentinel values) into durable state, and teardown
    signalled the recorded identity without proving it was the actual test-
    owned process.  A forged identity must be rejected by the same exact-
    identity proof production uses (worker_alive checks PID, PGID, SID,
    start_time_ticks, and lifecycle token).
    """
    env = dict(os.environ)
    env["LUBKO_LIFECYCLE_TOKEN"] = _SENTINEL_TOKEN
    sentinel = subprocess.Popen(
        [SLEEP_BIN, "86400"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        close_fds=True,
        env=env,
    )
    guard.register(sentinel)
    try:
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            if os.getpgid(sentinel.pid) == sentinel.pid:
                break
            time.sleep(0.01)

        identity = lifecycle.process_identity(sentinel.pid)
        assert identity is not None

        forged_state = supervise.SupervisorState(
            schema_version=supervise.SCHEMA_VERSION,
            applied_generation=999,
            mode=supervise.MODE_RUN,
            commit="x" * 40,
            child=supervise.WorkerChild(
                pid=identity.pid,
                pgid=identity.pgid,
                sid=identity.sid,
                start_time_ticks=identity.start_time_ticks + 999,
                token="wrong-token",  # ruff: ignore[hardcoded-password-func-arg] - forged identity
                worker_id="forged-worker",
                spawned_at=1.0,
            ),
            intent=supervise.INTENT_RUN,
            restart_count=0,
            next_attempt_at=None,
            last_exit=None,
            last_spawn_at=None,
            ready=False,
            next_readiness_at=None,
        )
        supervise.write_state(forged_state)

        _stop_orphaned_worker_children()

        os.kill(sentinel.pid, 0)  # raises OSError if dead
        assert isolation.ambient_sentinel_alive()
    finally:
        if sentinel.poll() is None:
            with suppress(ProcessLookupError):
                os.killpg(sentinel.pid, signal.SIGKILL)
        sentinel.wait(timeout=5)
        guard.unregister(sentinel)


# ---------------------------------------------------------------------------
# Regression: external interruption must not leak postmaster
# ---------------------------------------------------------------------------


def _report_if_ready(pidfile: Path, writer_fd: int) -> None:
    """Report the postmaster PID through the pipe if it is live.

    Args:
        pidfile: Path to the ``postmaster.pid`` file.
        writer_fd: Pipe write descriptor.
    """
    try:
        pm_pid = int(pidfile.read_text(encoding="utf-8").splitlines()[0])
    except (OSError, ValueError, IndexError):
        return
    if _pg.process_live(pm_pid):
        os.write(writer_fd, str(pm_pid).encode())
        os.close(writer_fd)
        time.sleep(300)
        os._exit(0)


def _start_pg_in_child(
    root: Path,
    binaries: dict[str, str],
    writer_fd: int,
) -> None:
    """Start a PostgreSQL cluster in a child process and report the postmaster PID.

    Args:
        root: Working directory for the cluster.
        binaries: PostgreSQL binary paths.
        writer_fd: Pipe write descriptor to report the postmaster PID.
    """
    os.setsid()
    data_dir = root / "data"
    socket_dir = root / "sock"
    socket_dir.mkdir()
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
    shim = _pg._resolve_shim(root)  # ruff: ignore[private-member-access] - test harness internal
    log_path = root / "server.log"
    cmd = [shim, binaries["postgres"], "-D", str(data_dir), "-p", str(port), "-k", str(socket_dir)]
    with log_path.open("ab") as log:
        subprocess.Popen(
            cmd,
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            close_fds=True,
        )
    pidfile = data_dir / "postmaster.pid"
    deadline = time.monotonic() + 15.0
    while time.monotonic() < deadline:
        if pidfile.is_file():
            _report_if_ready(pidfile, writer_fd)
        time.sleep(0.1)
    os.write(writer_fd, b"0")
    os.close(writer_fd)
    os._exit(1)


def test_pdeathsig_shim_kills_postmaster_on_parent_death() -> None:
    """Killing the test-owner process causes the postmaster to disappear.

    Simulates the external-interruption failure mode (SIGKILL, OOM, timeout
    runner): a child process acts as the ``test owner``, starts a PostgreSQL
    cluster through the ``PR_SET_PDEATHSIG`` shim, and is then killed.  The
    postmaster must not survive.
    """
    binaries = _pg.postgres_binaries()
    if binaries is None:
        pytest.skip("PostgreSQL server binaries not available on this host")

    r_fd, w_fd = os.pipe()
    try:  # ruff: ignore[too-many-statements-in-try-clause] - fork cleanup
        child_pid = os.fork()
        if child_pid == 0:
            os.close(r_fd)
            try:
                root = Path("/tmp") / f"pg-pdeathsig-{os.getpid()}"  # ruff: ignore[hardcoded-temp-file]
                root.mkdir(parents=True, exist_ok=True)
                _start_pg_in_child(root, binaries, w_fd)
            except Exception:  # ruff: ignore[blind-except] - child cleanup
                with suppress(OSError):
                    os.write(w_fd, b"0")
                    os.close(w_fd)
                os._exit(1)

        os.close(w_fd)
        raw = os.read(r_fd, 32)
        os.close(r_fd)
        pm_pid = int(raw.decode().strip())
        assert pm_pid > 0
        assert _pg.process_live(pm_pid)

        os.kill(child_pid, signal.SIGKILL)
        os.waitpid(child_pid, 0)

        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline and _pg.process_live(pm_pid):
            time.sleep(0.1)

        assert not _pg.process_live(pm_pid), (
            f"postmaster pid {pm_pid} survived parent death — "
            "PR_SET_PDEATHSIG shim did not propagate SIGKILL"
        )
    except Exception:
        with suppress(OSError):
            os.close(r_fd)
        with suppress(OSError):
            os.close(w_fd)
        raise
