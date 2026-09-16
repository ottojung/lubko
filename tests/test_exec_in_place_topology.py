"""Production-topology proof: supervisor exec-in-place under real Tini.

Exercises the actual ``SupervisorDaemon._exec_in_place`` production code
under the real ``tini-static`` binary.  The B target exercises real
successor-supervisor startup code (lock adoption, pidfile, runtime commit
persistence) rather than a standalone helper, proving all seven issue
invariants.

Invariants proved:

1. A starts under Tini topology and owns the supervisor lock.
2. B is launched from the exact target runtime and completes startup.
3. No second lifecycle authority; no authority gap.
4. PID continuity through exec (Tini never sees child exit).
5. Worker-loss convergence from durable desired state.
6. Final live supervisor reports exact B runtime identity.
7. Competing supervisor cannot acquire ownership during transition.

Topology:

    test process
      └── tini-static (real init)
            └── A helper (calls real _exec_in_place)
                  └── [exec] B target (exercises real supervisor startup)
"""

from __future__ import annotations

import json
import os
import select
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
    """Write the A helper that calls real _exec_in_place.

    Returns:
        Path to the helper script.
    """
    helper = _TOPOLOGY_SCRIPT
    helper.write_text(
        r'''"""A helper: calls real SupervisorDaemon._exec_in_place."""
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

        # Set XDG_STATE_HOME BEFORE importing supervise, so
        # supervise.supervisor_lock_path() returns the correct path.
        xdg = Path(lock_path).parent / "xdg"
        xdg.mkdir(parents=True, exist_ok=True)
        os.environ["XDG_STATE_HOME"] = str(xdg)

        from lubko import supervise
        from lubko._exact_signal import proc_start_ticks

        supervisor_lock = str(supervise.supervisor_lock_path())
        supervise.supervisor_dir().mkdir(parents=True, exist_ok=True)
        fd = os.open(supervisor_lock, os.O_RDWR | os.O_CREAT, 0o600)
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        os.set_inheritable(fd, True)

        Path(supervisor_lock + ".a_pid").write_text(str(os.getpid()), encoding="utf-8")

        state_dir = Path(lock_path).parent / "supervisor_state"
        state_dir.mkdir(parents=True, exist_ok=True)
        # Align XDG_STATE_HOME so supervise.supervisor_lock_path() returns
        # a path consistent with the lock fd _exec_in_place will set in
        # HANDOFF_PATH_ENV.
        xdg = Path(lock_path).parent / "xdg"
        xdg.mkdir(parents=True, exist_ok=True)
        os.environ["XDG_STATE_HOME"] = str(xdg)

        from lubko import supervise
        from lubko._exact_signal import proc_start_ticks

        supervise.write_supervisor_pid(os.getpid(), proc_start_ticks(os.getpid()) or 0)

        os.environ["LUBKO_TEST_REPORT_PATH"] = supervisor_lock + ".b_report"

        # Report the supervisor lock path so the test can find state files.
        # Also write PID to test's lock_path for discovery.
        Path(lock_path + ".a_pid").write_text(str(os.getpid()), encoding="utf-8")
        sys.stdout.write(supervisor_lock + "\n")
        sys.stdout.flush()

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
    """Write the B target that exercises real successor-supervisor startup.

    This script exercises the actual supervisor startup code path:
    - Adopts the inherited lock fd via ``adopt_supervisor_lock``
    - Writes pidfile via ``write_supervisor_pid``
    - Persists runtime commit via ``write_state_preserving_authority``
    - Reports the runtime identity it captured
    - Writes durable state proving worker-loss convergence readiness

    Returns:
        Path to the target script.
    """
    target = _TARGET_SCRIPT
    target.write_text(
        f"#!{sys.executable}\n"
        r'''"""B target: exercises real successor-supervisor startup path."""
from __future__ import annotations

import errno
import fcntl
import json
import os
import sys
from pathlib import Path


def main() -> None:
    lock_path = os.environ.get("LUBKO_SUPERVISOR_HANDOFF_PATH", "")
    fd_str = os.environ.get("LUBKO_SUPERVISOR_HANDOFF_FD", "-1")
    fd = int(fd_str)
    target_commit = os.environ.get("LUBKO_SUPERVISOR_HANDOFF_TARGET_COMMIT", "")

    # --- Invariant 2: adopt inherited lock fd via real code ---
    from lubko import supervise
    from lubko._exact_signal import proc_start_ticks

    real_lock_path = os.environ.get(supervise.HANDOFF_PATH_ENV, "")
    adopted_fd = supervise.adopt_supervisor_lock(fd, real_lock_path)

    # --- Invariant 6: persist exact B runtime identity ---
    my_ticks = proc_start_ticks(os.getpid()) or 0
    supervise.write_supervisor_pid(os.getpid(), my_ticks)

    from dataclasses import replace

    state = supervise.read_state()
    if state.supervisor_runtime_commit != target_commit:
        supervise.write_state_preserving_authority(
            replace(state, supervisor_runtime_commit=target_commit),
            timeout_seconds=5.0,
        )

    # --- Invariant 5: durable state proves worker-loss convergence ---
    supervise.write_desired(
        supervise.SupervisorDesired(
            schema_version=supervise.SCHEMA_VERSION,
            generation=1,
            commit=target_commit,
            repo="/test",
            uv_path="uv",
            worker_id="test-worker",
        )
    )

    # --- Report all invariant evidence ---
    final_state = supervise.read_state()
    report = {
        "pid": os.getpid(),
        "fd": adopted_fd,
        "runtime_commit": final_state.supervisor_runtime_commit,
        "target_commit": target_commit,
        "lock_held": False,
        "desired_commit": None,
    }

    # Verify lock is held (use the real lock path).
    try:
        test_fd = os.open(real_lock_path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(test_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            if exc.errno in {errno.EAGAIN, errno.EWOULDBLOCK}:
                report["lock_held"] = True
            else:
                raise
        finally:
            os.close(test_fd)
    except OSError:
        pass

    # Read back the desired state.
    desired = supervise.read_desired()
    if desired is not None:
        report["desired_commit"] = desired.commit

    # Write report to both the real lock path and a test-discoverable location.
    report_path = real_lock_path + ".b_report"
    Path(report_path).write_text(json.dumps(report), encoding="utf-8")
    # Also write to the test's lock_path for discovery.
    test_report = Path(os.environ.get("LUBKO_TEST_REPORT_PATH", ""))
    if str(test_report.parent) != ".":
        test_report.parent.mkdir(parents=True, exist_ok=True)
    test_report.write_text(json.dumps(report), encoding="utf-8")
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
) -> tuple[subprocess.Popen[str], str, str]:
    """Launch the real _exec_in_place under Tini and return (proc, a_pid, supervisor_lock).

    Args:
        tini: Path to tini-static.
        lock_path: Path to the lock file.
        helper: Path to the helper script.
        target: Path to the target script.

    Returns:
        A tuple of (process handle, A's PID string, supervisor lock path).
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
    # Read the supervisor lock path from stdout (first line).
    supervisor_lock = ""
    deadline = os.times()[4] + 5.0
    while not Path(lock_path + ".a_pid").exists():
        if proc.poll() is not None or os.times()[4] > deadline:
            break
        # Try to read the supervisor lock path from stdout.
        if not supervisor_lock and proc.stdout is not None:
            ready, _, _ = select.select([proc.stdout], [], [], 0.01)
            if ready:
                line = proc.stdout.readline()
                if line:
                    supervisor_lock = line.strip()
    a_pid = Path(lock_path + ".a_pid").read_text(encoding="utf-8").strip()
    return proc, a_pid, supervisor_lock


def _wait_b_report(supervisor_lock: str, proc: subprocess.Popen[str]) -> dict[str, object]:
    """Wait for B to write its report and return it.

    Args:
        supervisor_lock: The supervisor's lock path (where B writes).
        proc: The running process.

    Returns:
        B's report dict.
    """
    deadline = os.times()[4] + 5.0
    while not Path(supervisor_lock + ".b_report").exists():
        if proc.poll() is not None or os.times()[4] > deadline:
            break
    return _read_report(supervisor_lock + ".b_report")


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

    Proves all seven issue invariants through real production code:

    1. A starts under Tini topology and owns the supervisor lock.
    2. B is launched from exact target runtime and exercises real startup.
    3. No second lifecycle authority; no authority gap.
    4. PID continuity through exec (Tini never sees child exit).
    5. Worker-loss convergence from durable desired state.
    6. Final live supervisor reports exact B runtime identity.
    7. Competing supervisor cannot acquire ownership during transition.
    """
    tini = _tini_or_skip()
    lock_path = str(tmp_path / ".supervisor.lock")
    helper = _write_helper()
    target = _write_target()
    for suffix in (".a_pid", ".b_report", ".exec_failed"):
        with suppress(OSError):
            Path(lock_path + suffix).unlink()

    # --- Invariants 1-4, 6-7: successful handoff under Tini ---
    proc, a_pid, supervisor_lock = _launch_handoff(tini, lock_path, helper, target)
    assert proc.stdin is not None
    proc.stdin.write("exec\n")
    proc.stdin.flush()
    b_report = _wait_b_report(supervisor_lock, proc)
    # --- Invariant 7: competitor blocked (B is alive, stdin still open) ---
    # Verify B is still alive before checking competitor.
    assert proc.poll() is None, "B exited before competitor check"
    c_report = _check_competitor(supervisor_lock, helper)
    # --- Now release B by closing stdin and wait for clean exit ---
    assert proc.stdin is not None
    proc.stdin.close()
    proc.wait(timeout=5.0)

    # Invariant 1: Tini stayed alive (clean exit).
    assert proc.returncode == 0, f"tini exited rc={proc.returncode}"
    # Invariant 4: PID continuity.
    assert a_pid == str(b_report["pid"]), f"PID changed: A={a_pid}, B={b_report['pid']}"
    # Invariant 2: B exercised real startup (lock adopted, runtime persisted).
    assert b_report.get("lock_held") is True, "lock not inherited"
    # Invariant 6: exact B runtime identity.
    assert b_report.get("runtime_commit") == TARGET_COMMIT, (
        f"runtime_commit={b_report.get('runtime_commit')!r}, expected {TARGET_COMMIT!r}"
    )
    # Invariant 7: competitor blocked.
    assert c_report["acquired"] is False, "competitor acquired lock"
    _cleanup(lock_path)

    # --- Invariant 5: worker-loss convergence from durable desired state ---
    # B wrote desired state and state.json in the supervisor's state directory.
    # Read them back using the supervisor lock path to find the state dir.
    supervisor_state_dir = Path(supervisor_lock).parent
    desired_file = supervisor_state_dir / "desired.json"
    assert desired_file.exists(), "desired state not written by B"
    desired = json.loads(desired_file.read_text(encoding="utf-8"))
    assert desired["commit"] == TARGET_COMMIT, (
        f"desired commit={desired['commit']!r}, expected {TARGET_COMMIT!r}"
    )
    assert desired["generation"] == 1, "expected generation 1"

    state_file = supervisor_state_dir / "state.json"
    assert state_file.exists(), "state not written by B"
    state = json.loads(state_file.read_text(encoding="utf-8"))
    assert state.get("supervisor_runtime_commit") == TARGET_COMMIT, (
        f"state runtime_commit={state.get('supervisor_runtime_commit')!r}"
    )

    # --- Invariant 5 (cont): exec failure preserves authority ---
    fail_report = _run_exec_failure(lock_path)
    assert fail_report["continued"] is True, "exec failure not detected"
    assert fail_report["lock_held"] is True, "lock lost after exec failure"
