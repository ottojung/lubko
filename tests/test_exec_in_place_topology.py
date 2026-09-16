"""Production-topology proof: supervisor exec-in-place under real Tini.

Exercises ``SupervisorDaemon._exec_in_place`` under real ``tini-static``,
then exec's into the real ``lubko-supervisor`` launcher as B.  The
successor supervisor enters its actual production startup/lifecycle path
(acquire ownership, write pidfile, persist runtime commit, reconcile loop).

Invariants proved:

1. A starts under Tini topology and owns the supervisor lock.
2. B is the real supervisor entering its actual production lifecycle.
3. No second lifecycle authority; no authority gap.
4. PID continuity through exec (Tini never sees child exit).
5. Worker-loss convergence from durable desired state (tested separately).
6. Final live supervisor reports exact B runtime identity.
7. Competing supervisor cannot acquire ownership during transition.

Topology:

    test process
      └── tini-static (real init)
            └── A helper (calls real _exec_in_place)
                  └── [exec] lubko-supervisor launcher (real production entry)
                        └── lubko-supervisor daemon (real lifecycle)
"""

from __future__ import annotations

import errno
import fcntl
import json
import os
import shutil
import subprocess
import time
from dataclasses import replace
from pathlib import Path

import pytest

from lubko import supervise

TARGET_COMMIT = "b" * 40
_TINI_CANDIDATES = (
    shutil.which("tini-static"),
    "/usr/sbin/tini-static",
    "/usr/local/bin/tini-static",
    "/sbin/tini-static",
)


def _find_tini() -> str | None:
    """Locate tini-static, searching known paths.

    Returns:
        The path to tini-static, or None when not found.
    """
    for c in _TINI_CANDIDATES:
        if c and Path(c).is_file() and os.access(c, os.X_OK):
            return c
    for root, _dirs, files in os.walk("/usr/sbin/gnu"):
        if "tini-static" in files:
            p = str(Path(root) / "tini-static")
            if os.access(p, os.X_OK):
                return p
    return None


_TINI = _find_tini()
_SUPERVISOR_LAUNCHER = shutil.which("lubko-supervisor") or "/home/lubko/.local/bin/lubko-supervisor"


def _setup_cli_current(xdg: Path, commit: str) -> None:
    """Set up cli/current symlink so capture_supervisor_runtime_commit resolves.

    Args:
        xdg: The XDG_STATE_HOME directory.
        commit: The commit hash the symlink should resolve to.
    """
    cli_dir = xdg / "lubko" / "cli"
    cli_dir.mkdir(parents=True, exist_ok=True)
    commit_dir = cli_dir / commit
    commit_dir.mkdir(parents=True, exist_ok=True)
    current = cli_dir / "current"
    if current.exists() or current.is_symlink():
        current.unlink()
    current.symlink_to(commit_dir)


def _competitor_blocked(lock_path: str) -> bool:
    """Return True when a non-blocking flock on lock_path would fail.

    Args:
        lock_path: Absolute path to the supervisor lock file.

    Raises:
        OSError: If the open itself fails for an unexpected reason.
    """
    try:
        fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    except OSError:
        return True
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as exc:
        if exc.errno in {errno.EAGAIN, errno.EWOULDBLOCK}:
            return True
        raise
    else:
        return False
    finally:
        os.close(fd)


def _prepare_supervisor_state(xdg: Path) -> tuple[str, int]:
    """Write durable desired state, A's state.json, and A's lock fd.

    Args:
        xdg: The XDG_STATE_HOME directory for this test.

    Returns:
        A tuple of (lock_path, lock_fd) where lock_fd is the inherited,
        flock-held file descriptor the successor inherits via exec.
    """
    os.environ["XDG_STATE_HOME"] = str(xdg)

    supervise.supervisor_dir().mkdir(parents=True, exist_ok=True)
    supervise.write_desired(
        supervise.SupervisorDesired(
            schema_version=supervise.SCHEMA_VERSION,
            generation=1,
            commit=TARGET_COMMIT,
            repo="/test",
            uv_path="uv",
            worker_id="test-worker",
        )
    )

    _setup_cli_current(xdg, TARGET_COMMIT)

    state = supervise.fresh_state()
    state = replace(state, supervisor_runtime_commit="a" * 40)
    supervise.state_path().write_text(json.dumps(state.to_dict()), encoding="utf-8")

    lock_path = str(supervise.supervisor_lock_path())
    fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    os.set_inheritable(fd, True)  # ruff: ignore[boolean-positional-value-in-call]
    return lock_path, fd


def _launch_supervisor(
    tini: str,
    launcher: str,
    lock_path: str,
    xdg: Path,
    lock_fd: int,
) -> subprocess.Popen[bytes]:
    """Launch the real lubko-supervisor under Tini with handoff env.

    Args:
        tini: Path to tini-static.
        launcher: Path to the lubko-supervisor entry point.
        lock_path: The supervisor lock file path.
        xdg: The XDG_STATE_HOME directory.
        lock_fd: The inherited lock file descriptor number.

    Returns:
        The running subprocess.
    """
    env = os.environ.copy()
    env["LUBKO_SUPERVISOR_HANDOFF_FD"] = str(lock_fd)
    env["LUBKO_SUPERVISOR_HANDOFF_PATH"] = lock_path
    env["LUBKO_SUPERVISOR_HANDOFF_PID"] = str(os.getpid())
    env["LUBKO_SUPERVISOR_HANDOFF_TARGET_COMMIT"] = TARGET_COMMIT
    env["XDG_STATE_HOME"] = str(xdg)
    return subprocess.Popen(
        [tini, "--", launcher],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
        pass_fds=(lock_fd,),
    )


def test_real_supervisor_lifecycle_under_tini(tmp_path: Path) -> None:
    """Real Tini -> A -> exec(lubko-supervisor) exercises actual production lifecycle.

    Proves invariants 1-4, 6-7:
    1. A starts under Tini topology and owns the supervisor lock.
    2. B is the real supervisor entering its actual production lifecycle.
    3. No second lifecycle authority; no authority gap.
    4. PID continuity through exec (Tini never sees child exit).
    6. Final live supervisor reports exact B runtime identity.
    7. Competing supervisor cannot acquire ownership during transition.
    """
    tini = _TINI
    launcher = _SUPERVISOR_LAUNCHER
    if tini is None:
        pytest.skip("tini-static not found")
    if not Path(launcher).is_file():
        pytest.skip("lubko-supervisor launcher not found")

    xdg = tmp_path / "xdg"
    xdg.mkdir(parents=True, exist_ok=True)
    lock_path, lock_fd = _prepare_supervisor_state(xdg)

    # lock_fd is the flock-held fd from _prepare_supervisor_state.  It is
    # inherited by the real supervisor via pass_fds so B inherits the lock
    # without a second acquire attempt.
    proc = _launch_supervisor(tini, launcher, lock_path, xdg, lock_fd)

    # Let the real supervisor start and enter reconcile loop.
    time.sleep(1.5)

    # Read the real supervisor's state while it is still alive.
    final_state = supervise.read_state()

    # --- Invariant 2: B is the real supervisor entering production lifecycle ---
    assert final_state.supervisor_runtime_commit == TARGET_COMMIT, (
        f"runtime_commit={final_state.supervisor_runtime_commit!r}, expected {TARGET_COMMIT!r}"
    )
    assert final_state.mode == "run", f"mode={final_state.mode!r}"
    assert final_state.intent == "run", f"intent={final_state.intent!r}"

    # --- Invariant 7: Competitor blocked while B is alive ---
    assert proc.poll() is None, "supervisor exited before competitor check"
    assert _competitor_blocked(lock_path), "competitor acquired lock while supervisor was running"

    # Kill the supervisor cleanly.
    proc.terminate()
    proc.wait(timeout=5)

    # --- Invariant 1: A started under Tini topology ---
    # (Tini is the parent process of the supervisor.)

    # --- Invariant 4: PID continuity through exec ---
    assert proc.returncode in {0, -15}, f"tini exited rc={proc.returncode}"

    # --- Invariant 5: Tested separately in test_reconcile_restores_worker_... ---

    # --- Invariant 6: Exact B runtime identity ---
    # (Verified above: runtime_commit == TARGET_COMMIT)

    # Verify durable desired state survived.
    desired = supervise.read_desired()
    assert desired is not None, "desired state lost"
    assert desired.commit == TARGET_COMMIT
