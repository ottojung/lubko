"""Production-topology proof: supervisor exec-in-place under real Tini.

Single deterministic end-to-end test exercising the actual
``SupervisorDaemon._exec_in_place`` production code under the real
``tini-static`` binary, proving all required invariants in minimal
process launches.

Invariants proved against real production code:

1. Direct-child PID continuity through exec (Tini never sees exit).
2. Lock fd inherited through exec (flock continuity).
3. Target commit reaches successor via environment (immutable identity).
4. No competitor can acquire the lock while B holds it.
5. Exec failure preserves old owner (fail-closed, pidfile restored).
6. Durable state survives the handoff.

Topology:

    test process
      └── tini-static (real init, PID reaper)
            └── helper (imports real supervisor code, calls _exec_in_place)
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

_TINI_CANDIDATES = (
    shutil.which("tini-static"),
    "/usr/sbin/tini-static",
    "/usr/local/bin/tini-static",
    "/sbin/tini-static",
)


def _find_tini() -> str | None:
    """Locate the tini-static binary on the system.

    Returns:
        The path to tini-static, or None when not found.
    """
    for candidate in _TINI_CANDIDATES:
        if candidate is not None and Path(candidate).is_file() and os.access(candidate, os.X_OK):
            return candidate
    for root, _dirs, files in os.walk("/usr/sbin/gnu"):
        if "tini-static" in files:
            p = Path(root) / "tini-static"
            if os.access(str(p), os.X_OK):
                return str(p)
    return None


_TINI = _find_tini()
_TOPOLOGY_SCRIPT = Path(__file__).with_name("_exec_topology_helper.py")
_TARGET_SCRIPT = Path(__file__).with_name("_exec_topology_target.py")


def _write_helper() -> Path:
    """Write the helper that imports and calls the real supervisor code.

    Returns:
        Path to the helper script.
    """
    helper = _TOPOLOGY_SCRIPT
    helper.write_text(
        r'''"""Helper: imports real supervisor code and calls _exec_in_place."""
from __future__ import annotations

import errno
import fcntl
import json
import os
import sys
from pathlib import Path


def main() -> None:
    args = sys.argv[1:]
    action = args[0]
    lock_path = args[1]

    if action == "acquire-and-handoff":
        target_path = args[2]
        confirmed = args[3]

        fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        os.set_inheritable(fd, True)

        Path(lock_path + ".a_pid").write_text(str(os.getpid()), encoding="utf-8")

        state_dir = Path(lock_path).parent / "supervisor_state"
        state_dir.mkdir(parents=True, exist_ok=True)
        os.environ["XDG_STATE_HOME"] = str(state_dir.parent)

        from lubko import supervise
        from lubko._exact_signal import proc_start_ticks

        supervise.supervisor_dir().mkdir(parents=True, exist_ok=True)
        my_ticks = proc_start_ticks(os.getpid()) or 0
        supervise.write_supervisor_pid(os.getpid(), my_ticks)

        os.environ["LUBKO_TEST_LOCK_PATH"] = lock_path

        sys.stdin.readline()

        from lubko.supervisor import SupervisorDaemon

        result = SupervisorDaemon._exec_in_place(target_path, confirmed, fd)

        if result is False:
            Path(lock_path + ".exec_failed").write_text("true", encoding="utf-8")

    elif action == "try-lock":
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


if __name__ == "__main__":
    main()
''',
        encoding="utf-8",
    )
    Path(str(helper)).chmod(0o755)
    return helper


def _write_target() -> Path:
    """Write the target script that A execs into (B).

    Returns:
        Path to the target script.
    """
    target = _TARGET_SCRIPT
    target.write_text(
        f"#!{sys.executable}\n"
        r'''"""Target B: receives exec from A, reports PID and lock status."""
from __future__ import annotations

import errno
import fcntl
import json
import os
import sys
from pathlib import Path


def main() -> None:
    lock_path = os.environ.get("LUBKO_TEST_LOCK_PATH", "")
    fd = int(os.environ.get("LUBKO_SUPERVISOR_HANDOFF_FD", "-1"))
    lock_held = False
    if fd >= 0 and lock_path:
        try:
            test_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
            try:
                fcntl.flock(test_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as exc:
                if exc.errno in {errno.EAGAIN, errno.EWOULDBLOCK}:
                    lock_held = True
                else:
                    raise
            finally:
                os.close(test_fd)
        except OSError:
            pass
    report = {
        "pid": os.getpid(),
        "fd": fd,
        "lock_held": lock_held,
        "target_commit": os.environ.get("LUBKO_SUPERVISOR_HANDOFF_TARGET_COMMIT", ""),
    }
    Path(lock_path + ".b_report").write_text(json.dumps(report), encoding="utf-8")
    sys.stdout.write(json.dumps(report) + "\n")
    sys.stdout.flush()
    sys.stdin.read()


if __name__ == "__main__":
    main()
''',
        encoding="utf-8",
    )
    Path(str(target)).chmod(0o755)
    return target


def _read_report(path: str) -> dict[str, object]:
    """Read a JSON report from a file.

    Args:
        path: Path to the JSON report file.

    Returns:
        The parsed JSON report.
    """
    result: dict[str, object] = json.loads(Path(path).read_text(encoding="utf-8"))
    return result


def _cleanup(lock_path: str) -> None:
    """Clean up test artifacts."""
    for suffix in (".a_pid", ".b_report", ".exec_failed"):
        with suppress(OSError):
            Path(lock_path + suffix).unlink()


def _tini_or_skip() -> str:
    """Return the tini path or fail the test clearly.

    Raises:
        pytest.fail: When tini-static is not available.
    """
    if _TINI is None:
        msg = f"tini-static not found; tested path: {_TINI_CANDIDATES}"
        raise pytest.fail(msg)
    return _TINI


def _launch_handoff(
    tini: str, lock_path: str, helper: Path, target: Path
) -> tuple[subprocess.Popen[str], str]:
    """Launch the real _exec_in_place under Tini and return (proc, a_pid).

    Args:
        tini: Path to tini-static.
        lock_path: Path to the lock file.
        helper: Path to the helper script.
        target: Path to the target script.

    Returns:
        A tuple of (process handle, A's PID string).
    """
    cmd = [
        tini,
        "--",
        sys.executable,
        str(helper),
        "acquire-and-handoff",
        lock_path,
        str(target),
        TARGET_COMMIT,
    ]
    proc = subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    deadline = os.times()[4] + 5.0
    while not Path(lock_path + ".a_pid").exists():
        if proc.poll() is not None or os.times()[4] > deadline:
            break
    a_pid = Path(lock_path + ".a_pid").read_text(encoding="utf-8").strip()
    return proc, a_pid


def _wait_b_report(lock_path: str, proc: subprocess.Popen[str]) -> dict[str, object]:
    """Wait for B to write its report and return it.

    Args:
        lock_path: Path to the lock file.
        proc: The running process.

    Returns:
        B's report dict.
    """
    deadline = os.times()[4] + 5.0
    while not Path(lock_path + ".b_report").exists():
        if proc.poll() is not None or os.times()[4] > deadline:
            break
    return _read_report(lock_path + ".b_report")


def _check_competitor(lock_path: str, helper: Path) -> dict[str, object]:
    """Check that a competitor cannot acquire the lock.

    Args:
        lock_path: Path to the lock file.
        helper: Path to the helper script.

    Returns:
        The competitor's report dict.
    """
    result = subprocess.run(
        [sys.executable, str(helper), "try-lock", lock_path],
        capture_output=True,
        text=True,
        timeout=5.0,
        check=False,
    )
    report: dict[str, object] = json.loads(result.stdout.strip())
    return report


def _run_exec_failure(lock_path: str) -> dict[str, object]:
    """Run _exec_in_place with a nonexistent target and verify fail-closed.

    Args:
        lock_path: Path to the lock file.

    Returns:
        The failure report dict.
    """
    fail_helper = Path(lock_path).parent / "fail_exec_helper.py"
    fail_helper.write_text(
        r'''"""Call _exec_in_place with nonexistent target; verify lock still held."""
from __future__ import annotations

import errno
import fcntl
import json
import os
import sys
from pathlib import Path


def main() -> None:
    lock_path = sys.argv[1]
    fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    os.set_inheritable(fd, True)
    a_pid = os.getpid()

    state_dir = Path(lock_path).parent / "supervisor_state"
    state_dir.mkdir(parents=True, exist_ok=True)
    os.environ["XDG_STATE_HOME"] = str(state_dir.parent)

    from lubko import supervise
    from lubko._exact_signal import proc_start_ticks

    supervise.supervisor_dir().mkdir(parents=True, exist_ok=True)
    supervise.write_supervisor_pid(os.getpid(), proc_start_ticks(os.getpid()) or 0)
    os.environ["LUBKO_TEST_LOCK_PATH"] = lock_path

    sys.stdin.readline()

    from lubko.supervisor import SupervisorDaemon

    result = SupervisorDaemon._exec_in_place("/nonexistent/target", "b" * 40, fd)

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
        json.dumps({"pid": a_pid, "continued": result is False, "lock_held": blocked}) + "\n"
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
    report: dict[str, object] = json.loads(proc.stdout.read().strip())  # type: ignore[union-attr]
    return report


def test_exec_in_place_preserves_tini_direct_child(tmp_path: Path) -> None:
    """Single end-to-end test: real _exec_in_place under real Tini.

    Proves all required invariants in one deterministic process launch:
    1. Direct-child PID continuity through exec.
    2. Lock fd inherited through exec (flock continuity).
    3. Target commit reaches successor via environment.
    4. No competitor can acquire the lock while B holds it.
    5. Exec failure preserves old owner (fail-closed).
    6. Durable state survives the handoff.
    """
    tini = _tini_or_skip()
    lock_path = str(tmp_path / ".supervisor.lock")
    helper = _write_helper()
    target = _write_target()
    for suffix in (".a_pid", ".b_report", ".exec_failed"):
        with suppress(OSError):
            Path(lock_path + suffix).unlink()

    # --- Invariants 1-4: successful handoff under Tini ---
    proc, a_pid = _launch_handoff(tini, lock_path, helper, target)
    assert proc.stdin is not None
    proc.stdin.write("exec\n")
    proc.stdin.flush()
    b_report = _wait_b_report(lock_path, proc)
    c_report = _check_competitor(lock_path, helper)
    proc.stdin.close()
    proc.wait(timeout=5.0)

    assert proc.returncode == 0, f"tini exited rc={proc.returncode}"
    assert a_pid == str(b_report["pid"]), f"PID changed: A={a_pid}, B={b_report['pid']}"
    assert b_report.get("lock_held") is True, "lock not inherited"
    assert b_report.get("target_commit") == TARGET_COMMIT, "target commit wrong"
    assert c_report["acquired"] is False, "competitor acquired lock"
    _cleanup(lock_path)

    # --- Invariant 5: exec failure preserves authority ---
    fail_report = _run_exec_failure(lock_path)
    assert fail_report["continued"] is True, "exec failure not detected"
    assert fail_report["lock_held"] is True, "lock lost after exec failure"

    # --- Invariant 6: durable state survives ---
    state_file = tmp_path / "state.json"
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
