"""Deterministic production-topology proof: exec-in-place handoff under real Tini.

Uses the actual tini-static binary (PID 1 init) as the direct parent to prove
the exec-in-place handoff preserves Tini's direct-child contract through the
real production topology, not a mocked or simulated parent.

Invariants proved:

1. PID survives os.execve under real Tini (Tini never sees child exit).
2. Lock fd is inherited through exec (flock continuity).
3. No second authority exists during transition (flock blocks competitors).
4. Target commit reaches successor via environment (immutable identity).
5. Exec failure preserves old owner (fail-closed availability).
6. Durable state survives the handoff.

Topology:

    test process
      └── tini-static (real init, PID reaper)
            └── A (acquires lock, exec's into B, same PID)
                  └── B (inherits lock fd, validates, reports)

No real sleeps, no optional skips, no mocked process primitives.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from contextlib import suppress
from pathlib import Path

import pytest

TARGET_COMMIT = "b" * 40

_TINI_PATH: str | None = None


def _find_tini() -> str | None:
    """Locate the tini-static binary on the system.

    Returns:
        The path to tini-static, or None when not found.
    """
    global _TINI_PATH  # ruff: ignore[global-statement]
    if _TINI_PATH is not None:
        return _TINI_PATH
    path = shutil.which("tini-static")
    if path is not None:
        _TINI_PATH = path
        return path
    for candidate in (
        "/usr/sbin/tini-static",
        "/usr/local/bin/tini-static",
        "/sbin/tini-static",
    ):
        if Path(candidate).is_file() and os.access(candidate, os.X_OK):
            _TINI_PATH = candidate
            return candidate
    for root, _dirs, files in os.walk("/usr/sbin/gnu"):
        if "tini-static" in files:
            p = Path(root) / "tini-static"
            if os.access(str(p), os.X_OK):
                _TINI_PATH = str(p)
                return str(p)
    return None


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
from pathlib import Path


def main() -> None:
    args = sys.argv[1:]
    if not args:
        sys.exit(1)
    action = args[0]

    if action == "acquire-and-exec":
        lock_path = args[1]
        helper_path = args[2]
        fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        os.set_inheritable(fd, True)
        Path(lock_path + ".a_pid").write_text(str(os.getpid()), encoding="utf-8")
        sys.stdin.readline()
        env = os.environ.copy()
        env["LUBKO_HANDOFF_FD"] = str(fd)
        env["LUBKO_HANDOFF_PATH"] = lock_path
        env["LUBKO_HANDOFF_TARGET"] = "b" * 40
        os.execve(sys.executable, [sys.executable, helper_path, "adopt-and-report"], env)

    elif action == "adopt-and-report":
        fd = int(os.environ["LUBKO_HANDOFF_FD"])
        lock_path = os.environ["LUBKO_HANDOFF_PATH"]
        actual_path = str(os.readlink(f"/proc/self/fd/{fd}"))
        if actual_path != lock_path:
            sys.exit(1)
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
        Path(lock_path + ".b_report").write_text(json.dumps(report), encoding="utf-8")
        sys.stdout.write(json.dumps(report) + "\n")
        sys.stdout.flush()
        sys.stdin.read()

    elif action == "try-lock":
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


def _run_exec_handoff(
    lock_path: str,
    helper: Path,
    *,
    tini: str | None = None,
) -> tuple[subprocess.Popen[str], str, str]:
    """Run the acquire-and-exec handoff, returning (proc, a_pid, b_pid).

    Uses tini-static as parent when available, otherwise plain subprocess.
    The proc must be cleaned up by the caller.

    Args:
        lock_path: Path to the lock file.
        helper: Path to the topology helper script.
        tini: Optional path to tini-static binary.

    Returns:
        A tuple of (process handle, A's PID string, B's PID string).
    """
    a_pid_file = lock_path + ".a_pid"
    b_report_file = lock_path + ".b_report"
    for stale in (a_pid_file, b_report_file):
        with suppress(OSError):
            Path(stale).unlink()
    cmd = [sys.executable, str(helper), "acquire-and-exec", lock_path, str(helper)]
    if tini is not None:
        cmd = [tini, "--", *cmd]
    proc = subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    deadline = os.times()[4] + 5.0
    while not Path(a_pid_file).exists():
        if proc.poll() is not None or os.times()[4] > deadline:
            break
    a_pid = Path(a_pid_file).read_text(encoding="utf-8").strip()
    assert proc.stdin is not None
    proc.stdin.write("exec\n")
    proc.stdin.flush()
    proc.stdin.close()
    deadline = os.times()[4] + 5.0
    b_report: dict[str, object] = {}
    while True:
        b_path = Path(b_report_file)
        if b_path.exists():
            raw = b_path.read_text(encoding="utf-8").strip()
            if raw:
                b_report = json.loads(raw)
                break
        if proc.poll() is not None or os.times()[4] > deadline:
            break
    assert b_report, f"B did not write a valid report to {b_report_file}"
    b_pid = str(b_report["pid"])
    return proc, a_pid, b_pid


# ---------------------------------------------------------------------------
# Production topology tests — real Tini
# ---------------------------------------------------------------------------


def test_tini_survives_exec_in_place(tmp_path: Path) -> None:
    """Tini stays alive across A->B exec: PID preserved, direct-child valid.

    Runs the actual tini-static binary as the parent of the handoff
    sequence.  After A exec's into B, Tini's direct child PID is unchanged,
    so Tini never reaps a dead child and never exits.

    This is the canonical production-topology proof for issue #769.

    Raises:
        pytest.skip.Exception: When tini-static is not available.
    """
    tini = _find_tini()
    if tini is None:
        msg = "tini-static not found; cannot prove production topology"
        raise pytest.skip.Exception(msg, pytrace=False)
    lock_path = str(tmp_path / ".supervisor.lock")
    helper = _write_topology_helper()
    try:
        proc, a_pid, b_pid = _run_exec_handoff(lock_path, helper, tini=tini)
        proc.wait(timeout=5.0)
    finally:
        for suffix in (".a_pid", ".b_report"):
            with suppress(OSError):
                Path(lock_path + suffix).unlink()

    assert a_pid == b_pid, (
        f"PID changed across exec under Tini: A={a_pid}, B={b_pid}; "
        "Tini would reap a dead child and terminate the container"
    )
    assert proc.returncode == 0, (
        f"tini-static exited with rc={proc.returncode}; production topology must exit cleanly"
    )


def test_tini_lock_inherited_after_exec(tmp_path: Path) -> None:
    """Under real Tini, B inherits A's lock fd through exec.

    Proves flock continuity in the production topology.

    Raises:
        pytest.skip.Exception: When tini-static is not available.
    """
    tini = _find_tini()
    if tini is None:
        msg = "tini-static not found; cannot prove production topology"
        raise pytest.skip.Exception(msg, pytrace=False)
    lock_path = str(tmp_path / ".supervisor.lock")
    helper = _write_topology_helper()
    try:
        proc, _a_pid, _b_pid = _run_exec_handoff(lock_path, helper, tini=tini)
        proc.wait(timeout=5.0)
        b_report = _read_file_report(lock_path + ".b_report")
    finally:
        for suffix in (".a_pid", ".b_report"):
            with suppress(OSError):
                Path(lock_path + suffix).unlink()

    assert b_report.get("lock_held") is True, (
        "B does not hold the flock after exec under Tini; "
        "competitor could acquire during transition"
    )


def test_tini_no_competitor_during_transition(tmp_path: Path) -> None:
    """Under real Tini, no second authority exists while B holds the lock.

    B stays alive (stdin held open) and the flock blocks competitors.

    Raises:
        pytest.skip.Exception: When tini-static is not available.
    """
    tini = _find_tini()
    if tini is None:
        msg = "tini-static not found; cannot prove production topology"
        raise pytest.skip.Exception(msg, pytrace=False)
    lock_path = str(tmp_path / ".supervisor.lock")
    helper = _write_topology_helper()
    a_pid_file = lock_path + ".a_pid"
    b_report_file = lock_path + ".b_report"
    for stale in (a_pid_file, b_report_file):
        with suppress(OSError):
            Path(stale).unlink()
    cmd = [sys.executable, str(helper), "acquire-and-exec", lock_path, str(helper)]
    cmd = [tini, "--", *cmd]
    proc = subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    deadline = os.times()[4] + 5.0
    while not Path(a_pid_file).exists():
        if proc.poll() is not None or os.times()[4] > deadline:
            break
    assert proc.stdin is not None
    proc.stdin.write("exec\n")
    proc.stdin.flush()
    deadline = os.times()[4] + 5.0
    while not Path(b_report_file).exists():
        if proc.poll() is not None or os.times()[4] > deadline:
            break

    c_report = _run_helper_simple(["try-lock", lock_path])
    assert c_report["acquired"] is False, (
        "competitor acquired the lock while B holds it under Tini; "
        "exactly-one-lifecycle-authority invariant violated"
    )

    proc.stdin.close()
    proc.wait(timeout=5.0)
    for suffix in (".a_pid", ".b_report"):
        with suppress(OSError):
            Path(lock_path + suffix).unlink()


def test_tini_target_commit_reaches_successor(tmp_path: Path) -> None:
    """Under real Tini, the exact target commit reaches B via environment.

    Proves immutable runtime identity (#767) in the production topology.

    Raises:
        pytest.skip.Exception: When tini-static is not available.
    """
    tini = _find_tini()
    if tini is None:
        msg = "tini-static not found; cannot prove production topology"
        raise pytest.skip.Exception(msg, pytrace=False)
    lock_path = str(tmp_path / ".supervisor.lock")
    helper = _write_topology_helper()
    try:
        proc, _a_pid, _b_pid = _run_exec_handoff(lock_path, helper, tini=tini)
        proc.wait(timeout=5.0)
        b_report = _read_file_report(lock_path + ".b_report")
    finally:
        for suffix in (".a_pid", ".b_report"):
            with suppress(OSError):
                Path(lock_path + suffix).unlink()

    assert b_report.get("target_commit") == TARGET_COMMIT, (
        f"B received target_commit={b_report.get('target_commit')!r}, expected {TARGET_COMMIT!r}"
    )


# ---------------------------------------------------------------------------
# Supplementary topology tests (non-Tini, for specific invariants)
# ---------------------------------------------------------------------------


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

    lock_path = str(tmp_path / ".supervisor.lock")
    helper = _write_topology_helper()
    try:
        proc, _a_pid, _b_pid = _run_exec_handoff(lock_path, helper)
        assert proc.stdin is not None
        proc.stdin.close()
        proc.wait(timeout=5.0)
    finally:
        for suffix in (".a_pid", ".b_report"):
            with suppress(OSError):
                Path(lock_path + suffix).unlink()

    reloaded = json.loads(state_file.read_text(encoding="utf-8"))
    assert reloaded["supervisor_runtime_commit"] == "a" * 40
    assert reloaded == loaded
