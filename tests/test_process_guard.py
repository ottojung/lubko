"""Regression tests for the deterministic process-guard infrastructure.

These verify that the shared guard owns every test-created process with an
exact identity, stops leaked processes by their exact process group, never
signals the pytest/shared process group, and fails loudly when a test leaks.
"""

from __future__ import annotations

import fcntl
import json
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import TYPE_CHECKING, Final

import pytest

from tests import _process_guard as guard

if TYPE_CHECKING:
    from collections.abc import Callable

SLEEP_BIN: str = shutil.which("sleep") or "/bin/sleep"

TERM_IGNORE_SCRIPT: Final = """
import os, signal, sys, time
from pathlib import Path

def handler(_signum: int, _frame: object) -> None:
    Path(sys.argv[1]).write_text(f"{os.getpid()}:term")

signal.signal(signal.SIGTERM, handler)
sys.stdout.write(f"ready {os.getpid()}\\n")
sys.stdout.flush()
time.sleep(300)
"""


def wait_until(predicate: Callable[[], bool], timeout: float = 10.0) -> None:
    """Wait until a predicate holds.

    Args:
        predicate: Condition to await.
        timeout: Maximum seconds to wait.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.05)
    failure = AssertionError("condition not met within timeout")
    raise failure


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


def verified_kill(proc: subprocess.Popen[bytes], spawn_ticks: int) -> None:
    """Force-kill a test-owned subject by its SPAWN-TIME exact identity.

    Revalidates the stored start ticks immediately before the signal; a
    reused occupant of the PID is never signalled.

    Args:
        proc: The test-owned subject.
        spawn_ticks: Start ticks captured right after spawn.
    """
    if proc.poll() is not None:
        return
    assert guard.proc_start_ticks(proc.pid) == spawn_ticks, (
        f"pid {proc.pid} identity stale/reused at cleanup; KILL refused"
    )
    assert guard.signal_identity_checked(proc.pid, spawn_ticks, signal.SIGKILL)
    proc.wait(timeout=10)


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


def test_teardown_never_signals_reused_pid_identity() -> None:
    """A registry entry whose recorded identity no longer matches is never signalled.

    Deterministically simulates kernel PID reuse: a live innocent process
    occupies a PID whose registry entry records different start ticks, as if
    the originally-registered process had died and the kernel reassigned its
    PID.  Teardown must refuse to signal the unverified new occupant and must
    report the stale identity loudly instead.
    """
    innocent = subprocess.Popen(
        [SLEEP_BIN, "60"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    innocent_spawn_ticks = guard.proc_start_ticks(innocent.pid)
    assert innocent_spawn_ticks is not None
    try:
        current = innocent_spawn_ticks
        guard.register(innocent, start_ticks=current - 1)
        with pytest.raises(AssertionError, match="never signalled"):
            guard.teardown_tracked()
        assert pid_live(innocent.pid)
    finally:
        if innocent.poll() is None:
            assert guard.signal_identity_checked(
                innocent.pid,
                innocent_spawn_ticks,
                signal.SIGKILL,
            )
            innocent.wait(timeout=10)
    assert innocent.pid not in guard.TRACKED


def test_register_fails_closed_for_live_process_without_ticks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A live registration whose ticks cannot be read is refused outright."""
    proc = subprocess.Popen(
        [SLEEP_BIN, "300"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    proc_spawn_ticks = guard.proc_start_ticks(proc.pid)
    assert proc_spawn_ticks is not None
    try:
        monkeypatch.setattr(guard, "proc_start_ticks", lambda _pid: None)
        with pytest.raises(AssertionError, match="unverifiable identity"):
            guard.register(proc)
        assert guard.tracked_pids() == ()
        # The live process was never owned and never signalled.
        assert pid_live(proc.pid)
    finally:
        # Undo the seam first so cleanup revalidation reads real ticks.
        monkeypatch.undo()
        verified_kill(proc, proc_spawn_ticks)


def test_teardown_never_kills_identity_reused_after_term(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A KILL is refused when ticks change between TERM and KILL.

    The subject is a dedicated process that ignores SIGTERM and records its
    delivery, so the escalation path is exercised deterministically.  The
    TERM-time identity check matches the registration identity (proving the
    TERM hit the original occupant); the tick seam then changes before the
    KILL revalidation.  Teardown must refuse the KILL and report the stale
    identity — never kill the changed/reused occupant — and the test cleans
    up explicitly afterwards.

    Args:
        tmp_path: Pytest temporary directory.
        monkeypatch: Pytest monkeypatch fixture.
    """
    term_marker = tmp_path / "term-delivered.marker"
    proc = subprocess.Popen(
        [sys.executable, "-c", TERM_IGNORE_SCRIPT, str(term_marker)],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        # Deliberately NOT a session/group leader: the escalation gate under
        # test authorizes a KILL of the live PID itself only while its
        # recorded start-ticks identity matches.  (A dedicated-group owner
        # escalates by proven group ownership instead — covered by the
        # leader-exit regression.)
    )
    subject_spawn_ticks = guard.proc_start_ticks(proc.pid)
    assert subject_spawn_ticks is not None
    try:
        # Wait until the subject has installed its SIGTERM handler, so the
        # TERM is deterministically delivered to a live ignoring occupant.
        assert proc.stdout is not None
        ready = proc.stdout.readline().decode()
        assert ready.startswith("ready "), f"subject never became ready: {ready!r}"
        real = guard.proc_start_ticks(proc.pid)
        assert real is not None
        guard.register(proc)
        calls = {"n": 0}

        def shifting_ticks(_pid: int) -> int | None:
            # First read authorizes the TERM against the true identity;
            # every later read simulates the PID having been reused.
            calls["n"] += 1
            return real if calls["n"] == 1 else real + 1

        monkeypatch.setattr(guard, "proc_start_ticks", shifting_ticks)
        with pytest.raises(AssertionError, match="never signalled"):
            guard.teardown_tracked()
        # TERM was delivered to the original identity...
        while not term_marker.exists():
            if proc.poll() is not None:
                break
            time.sleep(0.05)
        assert term_marker.exists(), "TERM was never delivered"
        assert term_marker.read_text(encoding="utf-8") == f"{proc.pid}:term"
        # ...and the KILL was never delivered to the changed identity.
        assert pid_live(proc.pid)
    finally:
        monkeypatch.undo()
        if proc.poll() is None:
            # Non-leader: exact-PID cleanup only, never the shared group.
            verified_kill(proc, subject_spawn_ticks)
        if proc.stdout is not None:
            proc.stdout.close()
    assert proc.pid not in guard.TRACKED


CHILD_IGNORES_TERM_SCRIPT: Final = """
import os, signal, sys, time
from pathlib import Path

def handler(_signum: int, _frame: object) -> None:
    Path(sys.argv[1] + ".child-ignored").write_text("ignored")

signal.signal(signal.SIGTERM, handler)
Path(sys.argv[1] + ".pid").write_text(str(os.getpid()))
time.sleep(300)
"""


def test_teardown_kills_owned_group_when_leader_exits_on_term(
    tmp_path: Path,
) -> None:
    """A dedicated group proven owned is escalated even after leader exit.

    The registered session/group leader exits on SIGTERM while an
    already-spawned child of its dedicated group ignores SIGTERM and
    survives.  Teardown must escalate the KILL to the owned group — the
    leader's ``/proc`` entry being gone must not abandon the surviving
    child.

    Args:
        tmp_path: Pytest temporary directory.
    """
    parent_script = f"""
import subprocess, sys, time
from pathlib import Path

child = subprocess.Popen(
    [sys.executable, "-c", {CHILD_IGNORES_TERM_SCRIPT!r}, {str(tmp_path / "child")!r}],
)
Path({str(tmp_path / "child-pid")!r}).write_text(str(child.pid))
time.sleep(300)
"""
    proc = subprocess.Popen(
        [sys.executable, "-c", parent_script],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    proc_spawn_ticks = guard.proc_start_ticks(proc.pid)
    assert proc_spawn_ticks is not None
    child_pid_path = tmp_path / "child-pid"
    try:
        deadline = time.monotonic() + 30.0
        while not child_pid_path.exists() and time.monotonic() < deadline:
            if proc.poll() is not None:
                break
            time.sleep(0.05)
        assert child_pid_path.exists(), "subject never spawned its group member"
        guard.register(proc)
        with pytest.raises(AssertionError, match="leaked"):
            guard.teardown_tracked()
        # The leader exited on TERM; the ignoring child was killed by the
        # escalation against the owned dedicated group.
        assert proc.poll() is not None
        child_pid = int(child_pid_path.read_text())
        wait_until(lambda: not pid_live(child_pid), timeout=10.0)
    finally:
        if proc.poll() is None:
            assert guard.signal_identity_checked(
                proc.pid,
                proc_spawn_ticks,
                signal.SIGKILL,
            )
            proc.wait(timeout=10)


def test_signal_identity_checked_never_signals_shared_group() -> None:
    """The public signal primitive is exact-PID for non-leaders.

    Two children share the test process's group; signalling one by its
    verified PID+start-ticks identity must leave the sibling and the test
    process alive.  Both subjects are cleaned up explicitly.

    """
    target = subprocess.Popen(
        [SLEEP_BIN, "300"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    sibling = subprocess.Popen(
        [SLEEP_BIN, "300"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    target_ticks = guard.proc_start_ticks(target.pid)
    sibling_ticks = guard.proc_start_ticks(sibling.pid)
    assert target_ticks is not None
    assert sibling_ticks is not None
    try:
        assert os.getpgid(target.pid) == os.getpgrp()
        assert os.getpgid(sibling.pid) == os.getpgrp()
        ticks = guard.proc_start_ticks(target.pid)
        assert ticks is not None
        delivered = guard.signal_identity_checked(target.pid, ticks, signal.SIGKILL)
        assert delivered is True
        wait_until(lambda: not pid_live(target.pid), timeout=10.0)
        # Exact-PID only: the shared-group sibling and pytest survive.
        assert pid_live(sibling.pid)
        assert pid_live(os.getpid())
        # A stale identity authorizes nothing.
        assert guard.signal_identity_checked(sibling.pid, (ticks or 0) + 1, signal.SIGKILL) is False
        assert pid_live(sibling.pid)
    finally:
        for proc, spawn_ticks in ((target, target_ticks), (sibling, sibling_ticks)):
            if proc.poll() is None:
                assert guard.signal_identity_checked(proc.pid, spawn_ticks, signal.SIGKILL)
                proc.wait(timeout=10)


def test_register_ignores_already_terminal_process() -> None:
    """An already-terminal (reaped) Popen is harmless and not registered."""
    proc = subprocess.Popen(
        [SLEEP_BIN, "0"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    assert proc.wait(timeout=10) == 0
    guard.register(proc)
    assert guard.tracked_pids() == ()


def test_owner_marker_record_is_complete_before_register_returns(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A short write must be retried so the full record is durable first.

    ``register`` appends the exact ``{pid, ticks}`` identity to the owner
    marker before returning.  Forcing the first ``os.write`` to return only a
    partial prefix proves record-before-return: a single partial write would
    leave a torn line (unproven coverage), so the helper must loop until every
    byte is appended, including the terminating newline, before ``register``
    succeeds.
    """
    marker = tmp_path / "owner.marker.json"
    subject = subprocess.Popen(
        [SLEEP_BIN, "60"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    subject_spawn_ticks = guard.proc_start_ticks(subject.pid)
    assert subject_spawn_ticks is not None

    real_write = os.write
    calls: dict[str, int] = {"n": 0, "short_bytes": 0}

    def short_then_complete_write(fd: int, data: bytes) -> int:
        if calls["n"] == 0:
            calls["n"] = 1
            calls["short_bytes"] = max(len(data) // 2, 1)
            return real_write(fd, data[: calls["short_bytes"]])
        return real_write(fd, data)

    try:
        monkeypatch.setenv(guard.OWNER_MARKER_ENV, str(marker))
        monkeypatch.setattr(os, "write", short_then_complete_write)
        guard.register(subject)
        raw = marker.read_text(encoding="utf-8")
        assert raw.endswith("\n")
        entries = [json.loads(line) for line in raw.splitlines() if line.strip()]
        assert entries == [{"pid": subject.pid, "ticks": subject_spawn_ticks}]
        assert calls["n"] == 1
        assert calls["short_bytes"] > 0, "seam must actually force a short write"
    finally:
        monkeypatch.undo()
        if subject.poll() is None:
            assert guard.signal_identity_checked(
                subject.pid,
                subject_spawn_ticks,
                signal.SIGKILL,
            )
            subject.wait(timeout=10)
    guard.unregister(subject)
    assert subject.pid not in guard.TRACKED


def assert_marker_has_exactly(
    marker: Path,
    registered: list[tuple[subprocess.Popen[bytes], int]],
) -> None:
    """Assert the marker holds exactly one complete record per registration.

    Args:
        marker: The owner-marker JSONL file.
        registered: The successfully-registered ``(proc, spawn_ticks)`` pairs.
    """
    lines = marker.read_text(encoding="utf-8").splitlines(keepends=True)
    assert len(lines) == len(registered), lines
    assert all(line.endswith("\n") for line in lines), lines
    entries = [json.loads(line) for line in lines]
    expected = sorted((proc.pid, guard.proc_start_ticks(proc.pid)) for proc, _ in registered)
    assert sorted((e["pid"], e["ticks"]) for e in entries) == expected


def make_torn_byte_writer(
    key: tuple[int, int],
    real_write: Callable[[int, bytes], int],
) -> Callable[[int, bytes], int]:
    """Build an ``os.write`` replacement emitting one byte per call.

    Args:
        key: ``(st_dev, st_ino)`` of the marker; other fds pass through.
        real_write: The real ``os.write`` to delegate to.

    Returns:
        A write function forcing many short writes on the marker fd only.
    """

    def torn_write(fd: int, data: bytes) -> int:
        stat = os.fstat(fd)
        if (stat.st_dev, stat.st_ino) != key:
            return real_write(fd, data)
        written = 0
        while written < len(data):
            written += real_write(fd, data[written : written + 1])
            time.sleep(0.0005)
        return written

    return torn_write


def kill_by_exact_identity(registered: list[tuple[subprocess.Popen[bytes], int]]) -> None:
    """Kill every still-live registered subject by its spawn-time identity.

    Args:
        registered: The ``(proc, spawn_ticks)`` pairs to clean up.
    """
    for proc, spawn_ticks in registered:
        if proc.poll() is None:
            assert guard.signal_identity_checked(
                proc.pid,
                spawn_ticks,
                signal.SIGKILL,
            )
            proc.wait(timeout=10)
    for proc, _ in registered:
        guard.unregister(proc)


def make_register_worker(
    barrier: threading.Barrier,
    registered: list[tuple[subprocess.Popen[bytes], int]],
    errors: list[Exception],
) -> Callable[[], None]:
    """Build a thread body that spawns a subject and registers it concurrently.

    Args:
        barrier: Rendezvous so both writers overlap mid-recording.
        registered: Collects ``(proc, spawn_ticks)`` for every spawned subject.
        errors: Collects any failure raised by the registration.

    Returns:
        The thread target.
    """

    def target() -> None:
        proc, _ = spawn_sleep_leader()
        try:
            barrier.wait(timeout=10)
            guard.register(proc)
        except (threading.BrokenBarrierError, AssertionError, OSError) as error:
            errors.append(error)
        finally:
            registered.append((proc, guard.proc_start_ticks(proc.pid) or 0))

    return target


def run_two_threads(target: Callable[[], None]) -> None:
    """Run ``target`` on two threads concurrently and await both.

    Args:
        target: The thread body to run twice.
    """
    threads = [threading.Thread(target=target) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)


def test_concurrent_registrations_never_interleave_owner_marker_records(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two concurrent registrations assembled from short writes never interleave.

    Two real subprocesses are registered concurrently while ``os.write`` is
    forced to emit only one byte per call, so each logical marker record is
    necessarily assembled from many partial writes and the writers overlap.
    With the cross-process serialization in place both registrations must
    succeed and the marker must contain exactly two complete, parseable,
    newline-terminated JSONL records with the expected pid/ticks pairs.

    Args:
        tmp_path: Pytest temporary directory.
        monkeypatch: Pytest monkeypatch fixture.
    """
    marker = tmp_path / "owner.marker.jsonl"
    marker.touch()
    key = (marker.stat().st_dev, marker.stat().st_ino)
    registered: list[tuple[subprocess.Popen[bytes], int]] = []
    errors: list[Exception] = []

    monkeypatch.setenv(guard.OWNER_MARKER_ENV, str(marker))
    monkeypatch.setattr(os, "write", make_torn_byte_writer(key, os.write))
    run_two_threads(make_register_worker(threading.Barrier(2), registered, errors))
    monkeypatch.undo()
    try:
        assert not errors, errors
        assert len({proc.pid for proc, _ in registered}) == 2
        assert_marker_has_exactly(marker, registered)
    finally:
        kill_by_exact_identity(registered)


def test_owner_marker_fails_closed_when_lock_cannot_be_established(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Recording fails loudly instead of appending unlocked.

    Args:
        tmp_path: Pytest temporary directory.
        monkeypatch: Pytest monkeypatch fixture.
    """
    marker = tmp_path / "owner.marker.jsonl"
    proc, pid = spawn_sleep_leader()
    proc_spawn_ticks = guard.proc_start_ticks(pid)
    assert proc_spawn_ticks is not None

    def broken_flock(_fd: int, _operation: int) -> None:
        msg = "no locks available"
        raise OSError(msg)

    monkeypatch.setenv(guard.OWNER_MARKER_ENV, str(marker))
    monkeypatch.setattr(fcntl, "flock", broken_flock)
    try:
        with pytest.raises(AssertionError, match="cannot lock owner marker"):
            guard.register(proc)
        assert not marker.read_text(encoding="utf-8")
    finally:
        monkeypatch.undo()
        if proc.poll() is None:
            verified_kill(proc, proc_spawn_ticks)
        guard.unregister(proc)
    assert pid not in guard.TRACKED


def test_teardown_never_signals_entries_without_valid_ticks() -> None:
    """A registry entry without valid recorded ticks is never signalled."""
    innocent = subprocess.Popen(
        [SLEEP_BIN, "60"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    innocent_spawn_ticks = guard.proc_start_ticks(innocent.pid)
    assert innocent_spawn_ticks is not None
    try:
        guard.register_unverifiable(innocent)
        with pytest.raises(AssertionError, match="never signalled"):
            guard.teardown_tracked()
        assert pid_live(innocent.pid)
    finally:
        if innocent.poll() is None:
            assert guard.signal_identity_checked(
                innocent.pid,
                innocent_spawn_ticks,
                signal.SIGKILL,
            )
            innocent.wait(timeout=10)
    assert innocent.pid not in guard.TRACKED


def test_teardown_stops_exact_identity_after_registration() -> None:
    """A registered process whose ticks still match is signalled exactly."""
    proc, pid = spawn_sleep_leader()
    guard.register(proc)
    assert guard.proc_start_ticks(pid) is not None
    with pytest.raises(AssertionError, match="leaked"):
        guard.teardown_tracked()
    assert not pid_live(pid)
    assert pid not in guard.TRACKED


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
    sibling_spawn_ticks = guard.proc_start_ticks(sibling.pid)
    assert sibling_spawn_ticks is not None
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
        verified_kill(sibling, sibling_spawn_ticks)
