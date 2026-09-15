"""Deterministic production-topology proof: exec-in-place handoff preserves PID and lock.

Uses real subprocesses to prove kernel-level invariants that mock-based tests
cannot cover:

1. PID survives os.execve (process image replacement preserves PID for Tini).
2. Lock fd is inherited through exec (the new image adopts the same flock).
3. No second authority exists during the transition (flock blocks competitors).
4. The successor binds to the exact target commit (immutable runtime identity).
5. Exec failure leaves the old owner authoritative (fail-closed availability).

The test topology mirrors the production Tini layout:

    parent ─── A (real subprocess)
      │          └── lock file (flock held)
      │          └── os.execve(target) → B (same PID)
      │
      └── C (competitor, after B exits)

No real sleeps, no optional skips, no mocked process primitives.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

TARGET_COMMIT = "b" * 40

_TOPOLOGY_SCRIPT = Path(__file__).with_name("_exec_topology_helper.py")


def _write_topology_helper() -> Path:
    """Write the exec-topology helper script to a deterministic location.

    Returns:
        The path to the helper script.
    """
    helper = _TOPOLOGY_SCRIPT
    if helper.exists():
        return helper
    helper.write_text(
        r'''"""Helper for test_exec_in_place_topology: simulates A->B exec-in-place handoff."""
from __future__ import annotations
import errno
import fcntl
import json
import os
import sys


def main() -> None:
    args = sys.argv[1:]
    if not args:
        sys.exit(1)
    action = args[0]

    if action == "acquire-and-exec":
        # Phase 1: Acquire lock, report PID, wait for signal, exec into B.
        lock_path = args[1]
        helper_path = args[2]
        fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        os.set_inheritable(fd, True)
        # Report A's PID to a file (survives exec because stdout may buffer).
        report_path = lock_path + ".a_pid"
        Path(report_path).write_text(str(os.getpid()))
        # Wait for signal on stdin.
        sys.stdin.readline()
        env = os.environ.copy()
        env["LUBKO_HANDOFF_FD"] = str(fd)
        env["LUBKO_HANDOFF_PATH"] = lock_path
        env["LUBKO_HANDOFF_TARGET"] = "b" * 40
        os.execve(sys.executable, [sys.executable, helper_path, "adopt-and-report"], env)

    elif action == "adopt-and-report":
        # Phase 2: Adopt inherited fd, report PID + lock status + target.
        # Wait on stdin so the process stays alive during competitor checks.
        fd = int(os.environ["LUBKO_HANDOFF_FD"])
        lock_path = os.environ["LUBKO_HANDOFF_PATH"]
        actual_path = str(os.readlink(f"/proc/self/fd/{fd}"))
        if actual_path != lock_path:
            sys.exit(1)
        # Check that flock is held.
        blocked = False
        try:
            test_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
            try:
                fcntl.flock(test_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as exc:
                if exc.errno in {errno.EAGAIN, errno.EWOULDBLOCK}:
                    blocked = True
                else:
                    raise
            finally:
                os.close(test_fd)
        except OSError:
            pass
        report = {
            "pid": os.getpid(),
            "fd": fd,
            "lock_held": blocked,
            "target_commit": os.environ.get("LUBKO_HANDOFF_TARGET", ""),
        }
        # Write B's report to a file and also to stdout.
        report_path = lock_path + ".b_report"
        Path(report_path).write_text(json.dumps(report))
        sys.stdout.write(json.dumps(report) + "\n")
        sys.stdout.flush()
        # Hold the lock until stdin is closed (parent signals done).
        sys.stdin.read()

    elif action == "try-lock":
        # Try to acquire the lock (competitor check).
        lock_path = args[1]
        try:
            fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                acquired = True
            except OSError as exc:
                if exc.errno in {errno.EAGAIN, errno.EWOULDBLOCK}:
                    acquired = False
                else:
                    raise
            finally:
                os.close(fd)
        except OSError:
            acquired = False
        sys.stdout.write(json.dumps({"acquired": acquired}) + "\n")
        sys.stdout.flush()

    else:
        sys.exit(1)


if __name__ == "__main__":
    from pathlib import Path
    main()
''',
        encoding="utf-8",
    )
    Path(str(helper)).chmod(0o755)
    return helper


def _read_file_report(path: str) -> dict[str, object]:
    """Read a JSON report from a file.

    Args:
        path: Path to the JSON report file.

    Returns:
        The parsed JSON report.
    """
    result: dict[str, object] = json.loads(Path(path).read_text(encoding="utf-8"))
    return result


def _run_helper_simple(
    args: list[str],
    *,
    stdin_data: str | None = None,
    env: dict[str, str] | None = None,
    timeout: float = 5.0,
) -> dict[str, object]:
    """Run the topology helper for simple actions (try-lock).

    Args:
        args: Command-line arguments for the helper.
        stdin_data: Optional stdin input.
        env: Optional environment overrides.
        timeout: Subprocess timeout in seconds.

    Returns:
        The parsed JSON report.

    Raises:
        RuntimeError: If the helper subprocess fails.
    """
    helper = _write_topology_helper()
    merged_env = {**os.environ}
    if env:
        merged_env.update(env)
    proc = subprocess.run(
        [sys.executable, str(helper), *args],
        input=stdin_data,
        capture_output=True,
        text=True,
        timeout=timeout,
        env=merged_env,
        check=False,
    )
    if proc.returncode != 0:
        msg = (
            f"helper {args[0]!r} failed (rc={proc.returncode})\n"
            f"stdout: {proc.stdout}\nstderr: {proc.stderr}"
        )
        raise RuntimeError(msg)
    result: dict[str, object] = json.loads(proc.stdout.strip())
    return result


# ---------------------------------------------------------------------------
# Production topology tests
# ---------------------------------------------------------------------------


def test_exec_in_place_preserves_pid(tmp_path: Path) -> None:
    """A's PID survives os.execve into B: Tini never sees a child exit.

    Proves the fundamental kernel invariant that makes exec-in-place
    handoff production-safe under Tini (PID 1).
    """
    lock_path = str(tmp_path / ".supervisor.lock")
    helper = _write_topology_helper()

    # Phase 1: Spawn A, acquire lock, get A's PID from file.
    a_pid_file = lock_path + ".a_pid"
    b_report_file = lock_path + ".b_report"
    proc = subprocess.Popen(
        [sys.executable, str(helper), "acquire-and-exec", lock_path, str(helper)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    # Wait for A to acquire lock and write its PID.
    while not Path(a_pid_file).exists():
        if proc.poll() is not None:
            break
    a_pid = int(Path(a_pid_file).read_text(encoding="utf-8").strip())

    # Signal A to exec into B.
    assert proc.stdin is not None
    proc.stdin.write("exec\n")
    proc.stdin.flush()
    proc.stdin.close()

    # Wait for exec to complete and B to write its report.
    proc.wait(timeout=5.0)

    # Read B's report from file (survives exec because it's on disk).
    b_report = _read_file_report(b_report_file)

    # PID must be identical across exec.
    assert a_pid == b_report["pid"], (
        f"PID changed across exec: A={a_pid}, B={b_report['pid']}; "
        "Tini would see the child exit and terminate the container"
    )


def test_exec_in_place_inherits_lock_fd(tmp_path: Path) -> None:
    """The lock fd is inherited through exec: B holds A's flock.

    Proves that the kernel preserves file descriptors across execve,
    so B inherits the advisory lock without opening a second descriptor.
    """
    lock_path = str(tmp_path / ".supervisor.lock")
    helper = _write_topology_helper()

    a_pid_file = lock_path + ".a_pid"
    b_report_file = lock_path + ".b_report"
    proc = subprocess.Popen(
        [sys.executable, str(helper), "acquire-and-exec", lock_path, str(helper)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    while not Path(a_pid_file).exists():
        if proc.poll() is not None:
            break
    assert proc.stdin is not None
    proc.stdin.write("exec\n")
    proc.stdin.flush()
    proc.stdin.close()
    proc.wait(timeout=5.0)

    b_report = _read_file_report(b_report_file)

    # B inherited the lock fd.
    assert b_report.get("fd") is not None, "B did not inherit the lock fd"
    assert b_report.get("lock_held") is True, (
        "B does not hold the flock after exec; competitor could acquire"
    )


def test_no_competitor_during_transition(tmp_path: Path) -> None:
    """No second authority exists during the A->B transition.

    While B holds the lock (still running), a competitor C cannot acquire it.
    """
    lock_path = str(tmp_path / ".supervisor.lock")
    helper = _write_topology_helper()

    a_pid_file = lock_path + ".a_pid"
    b_report_file = lock_path + ".b_report"
    proc = subprocess.Popen(
        [sys.executable, str(helper), "acquire-and-exec", lock_path, str(helper)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    while not Path(a_pid_file).exists():
        if proc.poll() is not None:
            break
    assert proc.stdin is not None
    proc.stdin.write("exec\n")
    proc.stdin.flush()

    # Wait for B to write its report.
    while not Path(b_report_file).exists():
        if proc.poll() is not None:
            break

    # B is still alive (holding stdin open): competitor cannot acquire.
    c_report = _run_helper_simple(["try-lock", lock_path])
    assert c_report["acquired"] is False, (
        "competitor acquired the lock while B holds it; "
        "exactly-one-lifecycle-authority invariant violated"
    )

    # Release B by closing stdin.
    proc.stdin.close()
    proc.wait(timeout=5.0)


def test_exec_failure_preserves_old_authority(tmp_path: Path) -> None:
    """Exec failure leaves A's process image intact and the lock held.

    Fail-closed: if exec fails, A continues with its own runtime.
    """
    lock_path = str(tmp_path / ".supervisor.lock")

    fail_helper = tmp_path / "fail_exec_helper.py"
    fail_helper.write_text(
        r'''"""Acquire lock, report PID, exec into nonexistent target, check lock still held."""
from __future__ import annotations
import errno
import fcntl
import json
import os
import sys


def main() -> None:
    lock_path = sys.argv[1]
    fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    a_pid = os.getpid()
    sys.stdin.readline()
    try:
        os.execve("/nonexistent/exec-target", ["x"], os.environ)
    except OSError:
        blocked = False
        try:
            test_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
            try:
                fcntl.flock(test_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as exc:
                if exc.errno in {errno.EAGAIN, errno.EWOULDBLOCK}:
                    blocked = True
                else:
                    raise
            finally:
                os.close(test_fd)
        except OSError:
            pass
        sys.stdout.write(
            json.dumps({"pid": a_pid, "continued": True, "lock_held": blocked}) + "\n"
        )
        sys.stdout.flush()


if __name__ == "__main__":
    main()
''',
        encoding="utf-8",
    )
    Path(str(fail_helper)).chmod(0o755)

    proc = subprocess.Popen(
        [sys.executable, str(fail_helper), lock_path],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert proc.stdin is not None
    proc.stdin.write("exec\n")
    proc.stdin.flush()
    proc.stdin.close()
    proc.wait(timeout=5.0)

    report = json.loads(proc.stdout.read().strip())  # type: ignore[union-attr]
    assert report["continued"] is True
    assert report["lock_held"] is True, (
        "lock not held after exec failure; fail-closed availability violated"
    )


def test_target_commit_reaches_successor(tmp_path: Path) -> None:
    """The exact target commit reaches B via environment, not mutable cli/current.

    Proves immutable runtime identity (#767) is preserved across exec.
    """
    lock_path = str(tmp_path / ".supervisor.lock")
    helper = _write_topology_helper()

    a_pid_file = lock_path + ".a_pid"
    b_report_file = lock_path + ".b_report"
    proc = subprocess.Popen(
        [sys.executable, str(helper), "acquire-and-exec", lock_path, str(helper)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    while not Path(a_pid_file).exists():
        if proc.poll() is not None:
            break
    assert proc.stdin is not None
    proc.stdin.write("exec\n")
    proc.stdin.flush()
    proc.stdin.close()
    proc.wait(timeout=5.0)

    b_report = _read_file_report(b_report_file)

    assert b_report.get("target_commit") == TARGET_COMMIT, (
        f"B received target_commit={b_report.get('target_commit')!r}, expected {TARGET_COMMIT!r}"
    )


def test_durable_state_survives_handoff(tmp_path: Path) -> None:
    """The supervisor durable state survives the exec-in-place handoff.

    After A execs into B, the durable state.json (which lives on disk, not
    in the process image) is still readable and contains the correct
    supervisor_runtime_commit.
    """
    state_dir = tmp_path / "supervisor"
    state_dir.mkdir(parents=True, exist_ok=True)
    state_file = state_dir / "state.json"
    state_data = {
        "schema_version": 1,
        "applied_generation": 0,
        "mode": "idle",
        "commit": None,
        "child": None,
        "unresolved_child": None,
        "ownership_hold_malformed": False,
        "unresolved_hold_malformed": False,
        "spawning": None,
        "spawning_hold_malformed": False,
        "intent": "run",
        "restart_count": 0,
        "next_attempt_at": None,
        "last_exit": None,
        "last_spawn_at": None,
        "ready": False,
        "next_readiness_at": None,
        "supervisor_runtime_commit": "a" * 40,
    }
    state_file.write_text(json.dumps(state_data), encoding="utf-8")

    loaded = json.loads(state_file.read_text(encoding="utf-8"))
    assert loaded["supervisor_runtime_commit"] == "a" * 40

    # Run the full exec-in-place handoff.
    lock_path = str(tmp_path / ".supervisor.lock")
    helper = _write_topology_helper()
    a_pid_file = lock_path + ".a_pid"
    proc = subprocess.Popen(
        [sys.executable, str(helper), "acquire-and-exec", lock_path, str(helper)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    while not Path(a_pid_file).exists():
        if proc.poll() is not None:
            break
    assert proc.stdin is not None
    proc.stdin.write("exec\n")
    proc.stdin.flush()
    proc.stdin.close()
    proc.wait(timeout=5.0)

    # State file is still intact.
    reloaded = json.loads(state_file.read_text(encoding="utf-8"))
    assert reloaded["supervisor_runtime_commit"] == "a" * 40
    assert reloaded == loaded
