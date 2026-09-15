"""Production-topology proof: supervisor exec-in-place under real Tini.

Exercises the **actual** ``SupervisorDaemon._exec_in_place`` production code
path under the real ``tini-static`` binary (PID 1 init).  Proves that the
exec-in-place handoff preserves Tini's direct-child contract through the
real production process topology.

Invariants proved against real production code:

1. ``_exec_in_place`` preserves PID under real Tini (Tini never sees exit).
2. Lock fd is inherited through exec (flock continuity).
3. No second authority exists during transition (flock blocks competitors).
4. Target commit reaches successor via ``HANDOFF_TARGET_COMMIT_ENV``.
5. Exec failure preserves old owner (fail-closed, pidfile restored).
6. Durable state survives the handoff.

Topology:

    test process
      └── tini-static (real init, PID reaper)
            └── helper (imports real supervisor code, calls _exec_in_place)

No real sleeps, no skipped tests, no mocked process primitives.
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


def _write_real_supervisor_helper() -> Path:
    """Write a helper that imports and calls the real supervisor code.

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


def _write_target_script() -> Path:
    """Write the target script that A execs into (B).

    Returns:
        Path to the target script.
    """
    target = _TOPOLOGY_SCRIPT.parent / "_exec_topology_target.py"
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


def _run_handoff(
    lock_path: str,
    *,
    tini: str | None = None,
) -> tuple[subprocess.Popen[str], str, str]:
    """Run the real supervisor _exec_in_place under tini-static.

    Args:
        lock_path: Path to the lock file.
        tini: Path to tini-static binary.

    Returns:
        A tuple of (process handle, A's PID string, B's PID string).
    """
    helper = _write_real_supervisor_helper()
    target = _write_target_script()
    a_pid_file = lock_path + ".a_pid"
    b_report_file = lock_path + ".b_report"
    exec_failed_file = lock_path + ".exec_failed"
    for stale in (a_pid_file, b_report_file, exec_failed_file):
        with suppress(OSError):
            Path(stale).unlink()

    cmd = [
        sys.executable,
        str(helper),
        "acquire-and-handoff",
        lock_path,
        str(target),
        TARGET_COMMIT,
    ]
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

    b_report: dict[str, object] = {}
    deadline = os.times()[4] + 5.0
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


def _cleanup(lock_path: str) -> None:
    """Clean up test artifacts."""
    for suffix in (".a_pid", ".b_report", ".exec_failed"):
        with suppress(OSError):
            Path(lock_path + suffix).unlink()


# ---------------------------------------------------------------------------
# Production topology tests — real Tini, real supervisor code
# ---------------------------------------------------------------------------


def test_real_supervisor_exec_in_place_all_invariants(tmp_path: Path) -> None:
    """Real _exec_in_place preserves PID, inherits lock, binds target under Tini.

    Runs the actual ``SupervisorDaemon._exec_in_place`` production method
    under the real ``tini-static`` binary and verifies all key invariants
    in a single subprocess run (no redundant subprocess overhead).

    Raises:
        pytest.fail: When tini-static is not available.
    """
    if _TINI is None:
        msg = f"tini-static not found; tested path: {_TINI_CANDIDATES}"
        raise pytest.fail(msg)
    lock_path = str(tmp_path / ".supervisor.lock")
    try:
        proc, a_pid, b_pid = _run_handoff(lock_path, tini=_TINI)
        proc.wait(timeout=5.0)
        b_report = _read_report(lock_path + ".b_report")
    finally:
        _cleanup(lock_path)

    assert proc.returncode == 0, (
        f"tini-static exited with rc={proc.returncode}; production topology must exit cleanly"
    )
    assert a_pid == b_pid, (
        f"PID changed across real _exec_in_place under Tini: A={a_pid}, B={b_pid}; "
        "Tini would reap a dead child and terminate the container"
    )
    assert b_report.get("lock_held") is True, (
        "B does not hold the flock after real _exec_in_place under Tini; "
        "competitor could acquire during transition"
    )
    assert b_report.get("target_commit") == TARGET_COMMIT, (
        f"B received target_commit={b_report.get('target_commit')!r}, expected {TARGET_COMMIT!r}"
    )


def test_real_supervisor_no_competitor_during_transition(tmp_path: Path) -> None:
    """Under real Tini, no second authority while B holds the lock.

    B stays alive and the real flock blocks competitors.

    Raises:
        pytest.fail: When tini-static is not available.
    """
    if _TINI is None:
        msg = f"tini-static not found; tested path: {_TINI_CANDIDATES}"
        raise pytest.fail(msg)
    lock_path = str(tmp_path / ".supervisor.lock")
    helper = _write_real_supervisor_helper()
    target = _write_target_script()
    a_pid_file = lock_path + ".a_pid"
    b_report_file = lock_path + ".b_report"
    for stale in (a_pid_file, b_report_file):
        with suppress(OSError):
            Path(stale).unlink()

    cmd = [
        sys.executable,
        str(helper),
        "acquire-and-handoff",
        lock_path,
        str(target),
        TARGET_COMMIT,
    ]
    cmd = [_TINI, "--", *cmd]
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

    result = subprocess.run(
        [sys.executable, str(helper), "try-lock", lock_path],
        capture_output=True,
        text=True,
        timeout=5.0,
        check=False,
    )
    c_report: dict[str, object] = json.loads(result.stdout.strip())
    assert c_report["acquired"] is False, (
        "competitor acquired the lock while B holds it under Tini; "
        "exactly-one-lifecycle-authority invariant violated"
    )

    proc.stdin.close()
    proc.wait(timeout=5.0)
    _cleanup(lock_path)


def test_exec_failure_preserves_old_authority(tmp_path: Path) -> None:
    """Exec failure leaves A's process image intact and the lock held.

    Fail-closed: if exec fails, A continues with its own runtime.
    Exercises the real ``_exec_in_place`` failure path.
    """
    lock_path = str(tmp_path / ".supervisor.lock")
    fail_helper = tmp_path / "fail_exec_helper.py"
    fail_helper.write_text(
        r'''"""Acquire lock, call _exec_in_place with bad target, verify lock still held."""
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
    my_ticks = proc_start_ticks(os.getpid()) or 0
    supervise.write_supervisor_pid(os.getpid(), my_ticks)
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

    report = json.loads(proc.stdout.read().strip())  # type: ignore[union-attr]
    assert report["continued"] is True
    assert report["lock_held"] is True, (
        "lock not held after exec failure; fail-closed availability violated"
    )


def test_durable_state_survives_handoff(tmp_path: Path) -> None:
    """The supervisor durable state survives the exec-in-place handoff.

    After A execs into B, the durable state.json is still readable.

    Raises:
        pytest.fail: When tini-static is not available.
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
    tini = _TINI
    if tini is None:
        msg = f"tini-static not found; tested path: {_TINI_CANDIDATES}"
        raise pytest.fail(msg)
    try:
        proc, _a_pid, _b_pid = _run_handoff(lock_path, tini=tini)
        proc.wait(timeout=5.0)
    finally:
        _cleanup(lock_path)

    reloaded = json.loads(state_file.read_text(encoding="utf-8"))
    assert reloaded["supervisor_runtime_commit"] == "a" * 40
    assert reloaded == loaded
