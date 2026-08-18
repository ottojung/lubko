"""Regression tests for the deterministic process-guard infrastructure.

These verify that the shared guard owns every test-created process with an
exact identity, stops leaked processes by their exact process group, never
signals the pytest/shared process group, and fails loudly when a test leaks.
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

import pytest

from tests import _pg
from tests import _process_guard as guard

SLEEP_BIN: str = shutil.which("sleep") or "/bin/sleep"


def spawn_sleep_leader() -> tuple[subprocess.Popen[bytes], int]:
    """Spawn an untracked long-lived session-leader ``sleep``.

    Returns:
        A ``(Popen, pid)`` pair where the process is deliberately NOT
        registered with the guard.
    """
    proc = subprocess.Popen(
        [SLEEP_BIN, "300"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        close_fds=True,
    )
    return proc, proc.pid


def pid_live(pid: int) -> bool:
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


def test_teardown_stops_leaked_registered_process() -> None:
    """A leaked registered process is stopped by exact group and reported."""
    proc, pid = spawn_sleep_leader()
    guard.register(proc)
    try:
        with pytest.raises(AssertionError, match="leaked"):
            guard.teardown_tracked()
        assert not pid_live(pid)
        assert pid not in guard.TRACKED
    finally:
        with pytest.raises(ProcessLookupError):
            os.killpg(pid, 0)


def test_teardown_is_clean_when_process_was_reaped() -> None:
    """A test that owns and reaps its process leaves a clean registry."""
    proc, pid = spawn_sleep_leader()
    guard.register(proc)
    os.killpg(pid, signal.SIGKILL)
    proc.wait(timeout=10)
    guard.unregister(proc)
    assert guard.teardown_tracked() == 0
    assert pid not in guard.TRACKED
    assert not pid_live(pid)


def test_assert_no_live_tracked_detects_leaks() -> None:
    """A live tracked process is detected after teardown."""
    proc, pid = spawn_sleep_leader()
    guard.register(proc)
    try:
        with pytest.raises(AssertionError, match="still live"):
            guard.assert_no_live_tracked()
    finally:
        guard.teardown_tracked(fail_on_leak=False)
        assert not pid_live(pid)


def test_teardown_never_signals_parent_process_group() -> None:
    """A non-leader child is stopped by exact PID, never its shared group.

    A tracked process that is not a session leader shares the pytest process
    group.  Teardown must signal only that exact PID; signalling the shared
    group would kill pytest and any sibling process in the same group.
    """
    sibling = subprocess.Popen(
        [SLEEP_BIN, "60"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    leaked = subprocess.Popen(
        [SLEEP_BIN, "60"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        assert os.getpgid(leaked.pid) == os.getpgrp()
        assert os.getpgid(sibling.pid) == os.getpgrp()
        guard.register(leaked)
        killed = guard.teardown_tracked(fail_on_leak=False)
        assert killed == 1
        assert not pid_live(leaked.pid)
        assert pid_live(sibling.pid)
        assert pid_live(os.getpid())
    finally:
        if sibling.poll() is None:
            sibling.kill()
            sibling.wait(timeout=10)


# ---------------------------------------------------------------------------
# Higher-scope incarnation registry regressions
# ---------------------------------------------------------------------------


def test_higher_scope_incarnation_allowed_during_test(tmp_path: Path) -> None:
    """A registered higher-scope incarnation is exempt from persistent-leak detection.

    A script is written under ``tmp_path`` and executed by absolute path so
    its argv references the pytest-owned directory.  The exact incarnation is
    registered, and ``assert_no_persistent_leaks`` must not flag it.
    """
    script = tmp_path / "holder.py"
    script.write_text("import time; time.sleep(600)\n", encoding="utf-8")
    before = guard.snapshot_incarnations()
    proc = subprocess.Popen(
        [sys.executable, str(script)],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    ticks = guard.proc_start_ticks(proc.pid)
    if ticks is None:
        proc.kill()
        proc.wait(timeout=10)
        pytest.fail("cannot read start-time ticks for spawned process")
    try:
        guard.register_persistent_fixture_incarnation(proc.pid, ticks)
        assert guard.process_alive(proc.pid)
        argv = guard.read_cmdline_bytes(proc.pid)
        assert guard.argv_references_path(argv, tmp_path)
        guard.assert_no_persistent_leaks(before, owned_paths={tmp_path})
    finally:
        guard.unregister_persistent_fixture_incarnation(proc.pid, ticks)
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=10)


def test_higher_scope_stale_ticks_not_exempt(tmp_path: Path) -> None:
    """A registered PID whose ticks are mismatched is NOT exempt.

    After registration with the correct ticks, the stored value is
    deliberately overwritten to a wrong tick via the public
    ``install_stale_ticks`` helper.  ``assert_no_persistent_leaks`` must
    then flag the process because the incarnation no longer matches.
    """
    script = tmp_path / "holder.py"
    script.write_text("import time; time.sleep(600)\n", encoding="utf-8")
    before = guard.snapshot_incarnations()
    proc = subprocess.Popen(
        [sys.executable, str(script)],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    ticks = guard.proc_start_ticks(proc.pid)
    if ticks is None:
        proc.kill()
        proc.wait(timeout=10)
        pytest.fail("cannot read start-time ticks for spawned process")
    try:
        guard.register_persistent_fixture_incarnation(proc.pid, ticks)
        assert proc.pid in guard.HIGHER_SCOPE_INCARNATIONS
        guard.install_stale_ticks(proc.pid, 1)
        argv = guard.read_cmdline_bytes(proc.pid)
        assert guard.argv_references_path(argv, tmp_path)
        with pytest.raises(AssertionError, match="persistent process leak"):
            guard.assert_no_persistent_leaks(before, owned_paths={tmp_path})
    finally:
        current_stored = guard.HIGHER_SCOPE_INCARNATIONS.get(proc.pid)
        if current_stored is not None:
            guard.unregister_persistent_fixture_incarnation(proc.pid, current_stored)
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=10)


def test_unregistered_new_process_under_owned_path_fails(tmp_path: Path) -> None:
    """An unregistered new process whose argv references owned paths is flagged."""
    script = tmp_path / "holder.py"
    script.write_text("import time; time.sleep(600)\n", encoding="utf-8")
    before = guard.snapshot_incarnations()
    proc = subprocess.Popen(
        [sys.executable, str(script)],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    try:
        assert guard.process_alive(proc.pid)
        argv = guard.read_cmdline_bytes(proc.pid)
        assert guard.argv_references_path(argv, tmp_path)
        with pytest.raises(AssertionError, match="persistent process leak"):
            guard.assert_no_persistent_leaks(before, owned_paths={tmp_path})
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=10)


def test_incarnation_is_preexisting_classifies_correctly() -> None:
    """Pure incarnation comparison: same PID+ticks => pre-existing, different => new."""
    before = {100: 42, 200: 99}
    assert guard.incarnation_is_preexisting(before, 100, 42)
    assert not guard.incarnation_is_preexisting(before, 100, 99)
    assert not guard.incarnation_is_preexisting(before, 300, 42)
    assert not guard.incarnation_is_preexisting(before, 100, 43)


def test_unregister_with_mismatched_ticks_is_noop() -> None:
    """Unregistering with wrong ticks does not remove a live registration."""
    proc = subprocess.Popen(
        [SLEEP_BIN, "300"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    try:
        ticks = guard.proc_start_ticks(proc.pid)
        assert ticks is not None
        guard.register_persistent_fixture_incarnation(proc.pid, ticks)
        assert proc.pid in guard.HIGHER_SCOPE_INCARNATIONS
        guard.unregister_persistent_fixture_incarnation(proc.pid, ticks + 999)
        assert proc.pid in guard.HIGHER_SCOPE_INCARNATIONS
        guard.unregister_persistent_fixture_incarnation(proc.pid, ticks)
        assert proc.pid not in guard.HIGHER_SCOPE_INCARNATIONS
    finally:
        guard.HIGHER_SCOPE_INCARNATIONS.pop(proc.pid, None)
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=10)


def test_pgcluster_on_start_failure_stops_postmaster(
    tmp_path: Path,
) -> None:
    """When on_start callback fails, PgCluster stops the new postmaster before re-raising.

    The cluster must not be left alive and unregistered.
    """
    binaries = _pg.postgres_binaries()
    if binaries is None:
        pytest.skip("PostgreSQL server binaries not available")
    root = tmp_path / "pg"
    root.mkdir()
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
    cluster = _pg.PgCluster(binaries, data_dir, socket_dir, port, env)
    captured: list[tuple[int, int]] = []

    def exploding_callback(pid: int, ticks: int) -> None:
        captured.append((pid, ticks))
        assert pid > 0
        assert ticks > 0
        msg = "simulated on_start failure"
        raise RuntimeError(msg)

    cluster.on_start = exploding_callback
    with pytest.raises(RuntimeError, match="simulated on_start failure"):
        cluster.start()
    if captured:
        pid, ticks = captured[0]
        alive = guard.process_alive(pid) and guard.proc_start_ticks(pid) == ticks
        assert not alive


def test_stop_popen_cleans_group_after_leader_exit() -> None:
    """When a leader exits but its group has surviving children, the group is SIGKILLed.

    A live session-leader shell spawns a background child that inherits
    SIGTERM ignored, prints the child PID, resets the leader TERM trap to
    exit, and loops.  ``_stop_popen`` is called while the leader is still
    alive so ``_process_group_of`` resolves pgid from a live leader.  After
    SIGTERM the leader dies but the child survives in the same group;
    ``_stop_popen`` must SIGKILL the surviving group and wait for it empty.
    """
    leader_script = (
        "import os, signal, sys, time\n"
        "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
        "if os.fork() == 0:\n"
        "    time.sleep(300)\n"
        "    sys.exit(0)\n"
        "signal.signal(signal.SIGTERM, signal.SIG_DFL)\n"
        "print(os.getpid())\n"
        "sys.stdout.flush()\n"
        "while True:\n"
        "    time.sleep(300)\n"
    )
    proc = subprocess.Popen(
        [sys.executable, "-c", leader_script],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    child_pid_line = proc.stdout.readline()  # type: ignore[union-attr]
    child_pid = int(child_pid_line.strip())
    child_ticks = guard.proc_start_ticks(child_pid)
    assert child_ticks is not None
    pgid = proc.pid
    try:
        assert proc.poll() is None, "leader must still be alive"
        assert guard.process_alive(child_pid)
        assert guard.group_has_members(pgid)
        guard._stop_popen(proc)  # ruff: ignore[private-member-access]
        assert proc.poll() is not None, "leader must be reaped"
        child_alive = (
            guard.process_alive(child_pid) and guard.proc_start_ticks(child_pid) == child_ticks
        )
        assert not child_alive, f"child pid {child_pid} (ticks={child_ticks}) still alive"
        assert not guard.group_has_members(pgid)
    finally:
        if guard.group_has_members(pgid):
            with suppress(ProcessLookupError):
                os.killpg(pgid, signal.SIGKILL)
            deadline = time.monotonic() + 2.0
            while time.monotonic() < deadline and guard.group_has_members(pgid):
                time.sleep(0.02)
