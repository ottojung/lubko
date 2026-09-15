"""Regression tests for the exec-in-place handoff architecture.

Proves:
- Preflight probe validates lock, signals READY, exits before any durable write.
- Preflight probe never enters reconcile, never writes pidfile/status/state.
- A retires its pidfile before exec-in-place.
- Failed exec restores A's pidfile and continues.
- READY failure kills/reaps probe and aborts handoff.
- Probe nonzero exit kills/reaps and aborts handoff.
- No second lifecycle authority exists at any handoff boundary.
- Exec-in-place preserves PID continuity and inherits lock fd (topology).
"""

from __future__ import annotations

import fcntl
import json
import os
import select
import subprocess
import sys
from contextlib import suppress
from dataclasses import dataclass
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pytest

from lubko import supervise
from lubko.supervisor import (
    Settings,
    SupervisorDaemon,
    _HandoffPipes,
)

if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture
def _state_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Isolated XDG_STATE_HOME with an empty supervisor directory."""
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
    supervise.supervisor_dir().mkdir(parents=True, exist_ok=True)


def _write_fresh_state() -> None:
    state_path = supervise.state_path()
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(
        json.dumps(supervise.fresh_state().to_dict()),
        encoding="utf-8",
    )


def _noop_invalidate(_self: object) -> None:
    pass


def _noop_normalize() -> None:
    pass


def _noop_signals(_self: object) -> None:
    pass


# ---------------------------------------------------------------------------
# Preflight probe: must touch NO durable-write or reconcile hooks
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("_state_dir")
def test_preflight_probe_exits_without_durable_writes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Preflight probe validates lock, signals READY, and returns.

    Must return before ANY durable write, status write, normalization,
    signal handler, or reconcile call.
    """
    _write_fresh_state()
    daemon = SupervisorDaemon(Settings())
    daemon._ownership_fd = -1

    touched: list[str] = []

    def _track_pidfile(_self: object) -> None:
        touched.append("pidfile")

    def _track_runtime(_self: object) -> None:
        touched.append("runtime_commit")

    def _track_status(_self: object, *_args: object) -> None:
        touched.append("status")

    def _track_normalize() -> None:
        touched.append("normalize")

    def _track_signals(_self: object) -> None:
        touched.append("signals")

    def _track_reconcile(_self: object, _now: float) -> None:
        touched.append("reconcile")

    def _track_shutdown(_self: object) -> None:
        touched.append("shutdown")

    monkeypatch.setattr(SupervisorDaemon, "_write_pidfile", _track_pidfile)
    monkeypatch.setattr(SupervisorDaemon, "_persist_runtime_commit", _track_runtime)
    monkeypatch.setattr(SupervisorDaemon, "_write_status", _track_status)
    monkeypatch.setattr(SupervisorDaemon, "_invalidate_stale_status", _noop_invalidate)
    monkeypatch.setattr(SupervisorDaemon, "reconcile", _track_reconcile)
    monkeypatch.setattr(SupervisorDaemon, "_shutdown", _track_shutdown)
    monkeypatch.setattr("lubko.supervisor.normalize_cross_boot_state", _track_normalize)
    monkeypatch.setattr(SupervisorDaemon, "_install_signal_handlers", _track_signals)
    monkeypatch.setattr("lubko.supervisor._durable_log_handlers", list)

    ready_r, ready_w = os.pipe()
    os.set_inheritable(ready_w, True)  # ruff: ignore[boolean-positional-value-in-call]
    monkeypatch.setenv(supervise.HANDOFF_READY_FD_ENV, str(ready_w))
    monkeypatch.setenv(supervise.HANDOFF_PREPARE_MODE_ENV, "1")

    daemon.run()

    os.close(ready_r)

    assert touched == [], (
        f"preflight probe must not touch any durable-write or reconcile hooks; touched: {touched}"
    )


@pytest.mark.usefixtures("_state_dir")
def test_preflight_probe_signals_ready_and_closes_lock_fd(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    r"""Preflight probe writes R\n on the readiness pipe and closes its lock fd."""
    _write_fresh_state()
    daemon = SupervisorDaemon(Settings())
    fd_r, fd_w = os.pipe()
    os.set_inheritable(fd_w, True)  # ruff: ignore[boolean-positional-value-in-call]
    daemon._ownership_fd = fd_r

    ready_r, ready_w = os.pipe()
    os.set_inheritable(ready_w, True)  # ruff: ignore[boolean-positional-value-in-call]

    monkeypatch.setattr(SupervisorDaemon, "_write_pidfile", lambda _self: None)
    monkeypatch.setattr(SupervisorDaemon, "_persist_runtime_commit", lambda _self: None)
    monkeypatch.setattr(SupervisorDaemon, "_invalidate_stale_status", _noop_invalidate)
    monkeypatch.setattr(SupervisorDaemon, "_install_signal_handlers", _noop_signals)
    monkeypatch.setattr(SupervisorDaemon, "_write_status", lambda _self, *_a: None)
    monkeypatch.setattr("lubko.supervisor.normalize_cross_boot_state", _noop_normalize)
    monkeypatch.setattr("lubko.supervisor._durable_log_handlers", list)

    monkeypatch.setenv(supervise.HANDOFF_READY_FD_ENV, str(ready_w))
    monkeypatch.setenv(supervise.HANDOFF_PREPARE_MODE_ENV, "1")

    daemon.run()

    buf = os.read(ready_r, 16)
    os.close(ready_r)
    assert buf == b"R\n", f"probe must signal READY; got {buf!r}"
    assert daemon._ownership_fd is None, "probe must close its lock fd copy"


@pytest.mark.usefixtures("_state_dir")
def test_preflight_probe_exits_without_reconcile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Preflight probe returns from run() before the reconcile loop starts."""
    _write_fresh_state()
    daemon = SupervisorDaemon(Settings())
    daemon._ownership_fd = -1

    reconcile_calls: list[str] = []

    def _track_reconcile(_self: object, _now: float) -> None:
        reconcile_calls.append("reconcile")

    monkeypatch.setattr(SupervisorDaemon, "reconcile", _track_reconcile)
    monkeypatch.setattr(SupervisorDaemon, "_write_pidfile", lambda _self: None)
    monkeypatch.setattr(SupervisorDaemon, "_persist_runtime_commit", lambda _self: None)
    monkeypatch.setattr(SupervisorDaemon, "_invalidate_stale_status", _noop_invalidate)
    monkeypatch.setattr(SupervisorDaemon, "_install_signal_handlers", _noop_signals)
    monkeypatch.setattr(SupervisorDaemon, "_write_status", lambda _self, *_a: None)
    monkeypatch.setattr("lubko.supervisor.normalize_cross_boot_state", _noop_normalize)
    monkeypatch.setattr("lubko.supervisor._durable_log_handlers", list)

    ready_r, ready_w = os.pipe()
    os.set_inheritable(ready_w, True)  # ruff: ignore[boolean-positional-value-in-call]
    monkeypatch.setenv(supervise.HANDOFF_READY_FD_ENV, str(ready_w))
    monkeypatch.setenv(supervise.HANDOFF_PREPARE_MODE_ENV, "1")

    daemon.run()

    os.close(ready_r)
    assert reconcile_calls == [], "preflight must never enter reconcile loop"


# ---------------------------------------------------------------------------
# READY failure: probe must be killed/reaped, handoff aborted
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("_state_dir")
def test_ready_failure_kills_probe_and_aborts_handoff() -> None:
    """When the probe does not signal READY, A kills/reaps it and aborts."""
    _write_fresh_state()
    daemon = SupervisorDaemon(Settings())
    daemon._ownership_fd = -1

    process = MagicMock()
    process.wait.return_value = 0

    ready_r, ready_w = os.pipe()
    os.close(ready_w)
    pipes = _HandoffPipes(ready_r=ready_r)

    result = daemon._await_probe_ready(
        process=process,
        pipes=pipes,
        confirmed="b" * 40,
        ownership_fd=-1,
    )

    assert result is False
    process.kill.assert_called_once()
    process.wait.assert_called_once()


# ---------------------------------------------------------------------------
# Probe nonzero exit: must be killed/reaped, handoff aborted
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("_state_dir")
def test_probe_nonzero_exit_aborts_handoff() -> None:
    """When the probe exits with rc != 0 after READY, handoff is aborted."""
    _write_fresh_state()
    daemon = SupervisorDaemon(Settings())
    daemon._ownership_fd = -1

    process = MagicMock()
    process.wait.return_value = 1

    ready_r, ready_w = os.pipe()
    os.write(ready_w, b"R\n")
    os.close(ready_w)
    pipes = _HandoffPipes(ready_r=ready_r)

    result = daemon._await_probe_ready(
        process=process,
        pipes=pipes,
        confirmed="b" * 40,
        ownership_fd=-1,
    )

    assert result is False, "nonzero probe exit must abort handoff"
    assert "failed (rc=1)" in daemon._message  # type: ignore[operator]


# ---------------------------------------------------------------------------
# Exec-in-place: pidfile retired before exec, exec uses os.execve
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("_state_dir")
def test_exec_in_place_retires_pidfile_before_exec(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A retires its own pidfile before exec-in-place."""
    _write_fresh_state()
    daemon = SupervisorDaemon(Settings())
    daemon._ownership_fd = -1

    retired: list[bool] = []

    def _fake_retire() -> tuple[int, int]:
        retired.append(True)
        return os.getpid(), 12345

    exec_calls: list[tuple[str, list[str], dict[str, str]]] = []
    msg = "simulated exec failure"

    def _fake_execve(path: str, argv: list[str], env: dict[str, str]) -> None:
        exec_calls.append((path, argv, env))
        raise OSError(msg)

    monkeypatch.setattr("lubko.supervise.retire_supervisor_pid", _fake_retire)
    monkeypatch.setattr(
        "lubko.supervise.restore_supervisor_pid",
        lambda _pid, _ticks: None,
    )
    monkeypatch.setattr("lubko.supervisor.os.execve", _fake_execve)
    monkeypatch.setattr("lubko.supervisor.os.set_inheritable", lambda _fd, _val: None)

    result = daemon._exec_in_place(
        target="/fake/target",
        confirmed="b" * 40,
        ownership_fd=-1,
    )

    assert result is False
    assert retired, "pidfile must be retired before exec"
    assert len(exec_calls) == 1, "execve must be called exactly once"
    path, argv, env = exec_calls[0]
    assert path == "/fake/target"
    assert argv == ["/fake/target"]
    assert env[supervise.HANDOFF_FD_ENV] == str(-1)
    assert env[supervise.HANDOFF_TARGET_COMMIT_ENV] == "b" * 40


@pytest.mark.usefixtures("_state_dir")
def test_exec_failure_restores_pidfile_and_continues(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When os.execve fails, A restores its pidfile and continues."""
    _write_fresh_state()
    daemon = SupervisorDaemon(Settings())
    daemon._ownership_fd = -1

    restored: list[tuple[int, int]] = []

    def _fake_retire() -> tuple[int, int]:
        return os.getpid(), 12345

    def _fake_restore(pid: int, ticks: int) -> None:
        restored.append((pid, ticks))

    fail_msg = "exec failed"

    def _fail_execve(_path: str, _argv: list[str], _env: dict[str, str]) -> None:
        raise OSError(fail_msg)

    monkeypatch.setattr("lubko.supervise.retire_supervisor_pid", _fake_retire)
    monkeypatch.setattr("lubko.supervise.restore_supervisor_pid", _fake_restore)
    monkeypatch.setattr("lubko.supervisor.os.execve", _fail_execve)
    monkeypatch.setattr("lubko.supervisor.os.set_inheritable", lambda _fd, _val: None)

    result = daemon._exec_in_place(
        target="/fake/target",
        confirmed="b" * 40,
        ownership_fd=-1,
    )

    assert result is False, "exec failure must return False (continue)"
    assert restored, "pidfile must be restored after exec failure"
    assert restored[0] == (os.getpid(), 12345)


# ---------------------------------------------------------------------------
# No second authority: probe + exec boundary
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("_state_dir")
def test_no_authority_overlap_at_probe_exec_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """At no point during probe->exec does a second reconciler exist.

    The probe exits before exec. The exec'd image enters normal startup only
    after the probe has fully exited and its lock fd copy is closed.
    """
    _write_fresh_state()
    daemon = SupervisorDaemon(Settings())
    daemon._ownership_fd = -1
    lifecycle_log: list[str] = []

    def _track_pidfile(_self: object) -> None:
        lifecycle_log.append("pidfile_write")

    def _track_reconcile(_self: object, _now: float) -> None:
        lifecycle_log.append("reconcile")

    original_preflight = SupervisorDaemon._preflight_handoff

    def _tracked_preflight(_self: SupervisorDaemon) -> None:
        lifecycle_log.append("preflight_start")
        original_preflight(_self)
        lifecycle_log.append("preflight_done")

    monkeypatch.setattr(SupervisorDaemon, "_preflight_handoff", _tracked_preflight)
    monkeypatch.setattr(SupervisorDaemon, "_write_pidfile", _track_pidfile)
    monkeypatch.setattr(SupervisorDaemon, "_persist_runtime_commit", lambda _self: None)
    monkeypatch.setattr(SupervisorDaemon, "_invalidate_stale_status", _noop_invalidate)
    monkeypatch.setattr(SupervisorDaemon, "reconcile", _track_reconcile)
    monkeypatch.setattr(SupervisorDaemon, "_install_signal_handlers", _noop_signals)
    monkeypatch.setattr(SupervisorDaemon, "_write_status", lambda _self, *_a: None)
    monkeypatch.setattr("lubko.supervisor.normalize_cross_boot_state", _noop_normalize)
    monkeypatch.setattr("lubko.supervisor._durable_log_handlers", list)

    ready_r, ready_w = os.pipe()
    os.set_inheritable(ready_w, True)  # ruff: ignore[boolean-positional-value-in-call]
    monkeypatch.setenv(supervise.HANDOFF_READY_FD_ENV, str(ready_w))
    monkeypatch.setenv(supervise.HANDOFF_PREPARE_MODE_ENV, "1")

    daemon.run()
    os.close(ready_r)

    assert lifecycle_log == [
        "preflight_start",
        "preflight_done",
    ], f"preflight must exit before pidfile/reconcile; log: {lifecycle_log}"


# ---------------------------------------------------------------------------
# Topology invariant: exec-in-place preserves PID, inherits lock fd, binds commit
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class _TopologyContext:
    """Live exec-in-place probe and the parent-side release control."""

    result_path: Path
    release_w: int
    proc: subprocess.Popen[bytes]

    @classmethod
    def create(cls, tmp_path: Path) -> _TopologyContext:
        """Create the real exec topology probe and wait for target readiness.

        Returns:
            A live topology context whose target has completed exec and is
            blocked on the release pipe while holding the supervisor lock.
        """
        state_dir = supervise.supervisor_dir()
        state_dir.mkdir(parents=True, exist_ok=True)
        result_path = tmp_path / "result.json"
        target_script = _write_topology_target(tmp_path, result_path)
        helper = _write_topology_helper(tmp_path)
        ready_r, ready_w = os.pipe()
        release_r, release_w = os.pipe()
        env = {
            **os.environ,
            "LUBKO_TOPOLOGY_READY_FD": str(ready_w),
            "LUBKO_TOPOLOGY_RELEASE_FD": str(release_r),
        }
        proc = subprocess.Popen(
            [sys.executable, str(helper), str(state_dir.parent.parent), str(target_script)],
            pass_fds=(ready_w, release_r),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
        )
        os.close(ready_w)
        os.close(release_r)
        ready_fds, _, _ = select.select([ready_r], [], [], 5.0)
        if not ready_fds:
            os.close(ready_r)
            os.close(release_w)
            _fail_topology_process(proc, "target did not signal READY within timeout")
        ready = os.read(ready_r, 16)
        os.close(ready_r)
        if ready != b"R\n":
            os.close(release_w)
            _fail_topology_process(proc, f"unexpected READY signal: {ready!r}")
        return cls(result_path=result_path, release_w=release_w, proc=proc)

    def assert_invariants(self) -> None:
        """Assert PID, liveness, inherited lock, identity, and lock exclusion."""
        data = json.loads(self.result_path.read_text(encoding="utf-8"))
        assert data["pid"] == self.proc.pid, (
            f"PID must be preserved; helper pid={self.proc.pid}, target pid={data['pid']}"
        )
        assert self.proc.poll() is None, "target must still be alive after exec-in-place"
        assert data["lock_fd_open"] is True, "lock fd must be inherited by exec'd target"
        assert data["target_commit"] == "b" * 40, "target commit must be present"
        _assert_lock_excludes_competitor()

    def release(self) -> int:
        """Release the blocked target and return its process exit code.

        Returns:
            The target process return code after bounded cleanup.
        """
        with suppress(OSError):
            os.write(self.release_w, b"G")
        os.close(self.release_w)
        try:
            self.proc.wait(timeout=5.0)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait(timeout=5.0)
        return self.proc.returncode if self.proc.returncode is not None else -1


def _fail_topology_process(proc: subprocess.Popen[bytes], message: str) -> None:
    """Stop a failed topology probe and fail with its captured diagnostics."""
    if proc.poll() is None:
        proc.kill()
    stdout, stderr = proc.communicate()
    pytest.fail(f"{message}; rc={proc.returncode}; stdout={stdout!r}; stderr={stderr!r}")


def _write_topology_target(tmp_path: Path, result_path: Path) -> Path:
    """Write the exact exec target used by the topology regression.

    Returns:
        Path to the executable target script.
    """
    target_script = tmp_path / "target.py"
    target_script.write_text(
        f"#!{sys.executable}\n"
        "import json, os\n"
        "lock_fd = int(os.environ['LUBKO_SUPERVISOR_HANDOFF_FD'])\n"
        "commit = os.environ.get('LUBKO_SUPERVISOR_HANDOFF_TARGET_COMMIT', '')\n"
        "ready_fd = int(os.environ['LUBKO_TOPOLOGY_READY_FD'])\n"
        "release_fd = int(os.environ['LUBKO_TOPOLOGY_RELEASE_FD'])\n"
        "result = {\n"
        "    'pid': os.getpid(),\n"
        "    'lock_fd_open': os.path.exists(f'/proc/self/fd/{lock_fd}'),\n"
        "    'target_commit': commit,\n"
        "}\n"
        f"with open({str(result_path)!r}, 'w', encoding='utf-8') as stream:\n"
        "    json.dump(result, stream)\n"
        "os.write(ready_fd, b'R\\n')\n"
        "os.close(ready_fd)\n"
        "release = os.read(release_fd, 1)\n"
        "os.close(release_fd)\n"
        "raise SystemExit(0 if release == b'G' else 2)\n",
        encoding="utf-8",
    )
    target_script.chmod(0o755)
    return target_script


def _write_topology_helper(tmp_path: Path) -> Path:
    """Write the pre-exec helper that owns the real supervisor lock.

    Returns:
        Path to the helper script.
    """
    helper = tmp_path / "helper.py"
    helper.write_text(
        "import fcntl, os, sys\n"
        "os.environ['XDG_STATE_HOME'] = sys.argv[1]\n"
        "sys.path.insert(0, 'src')\n"
        "for name in ('LUBKO_TOPOLOGY_READY_FD', 'LUBKO_TOPOLOGY_RELEASE_FD'):\n"
        "    os.set_inheritable(int(os.environ[name]), True)\n"
        "from lubko import supervise\n"
        "from lubko.supervisor import SupervisorDaemon\n"
        "lock_path = supervise.supervisor_lock_path()\n"
        "lock_path.parent.mkdir(parents=True, exist_ok=True)\n"
        "lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)\n"
        "fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)\n"
        "os.set_inheritable(lock_fd, True)\n"
        "pid = os.getpid()\n"
        "from lubko._exact_signal import proc_start_ticks\n"
        "from lubko.supervise import write_supervisor_pid\n"
        "write_supervisor_pid(pid, proc_start_ticks(pid) or 0)\n"
        "SupervisorDaemon._exec_in_place(sys.argv[2], 'b' * 40, lock_fd)\n",
        encoding="utf-8",
    )
    return helper


def _assert_lock_excludes_competitor() -> None:
    """Prove the exec'd target still exclusively owns the supervisor lock."""
    lock_path = supervise.supervisor_lock_path()
    competitor_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        with pytest.raises(BlockingIOError):
            fcntl.flock(competitor_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    finally:
        os.close(competitor_fd)


@pytest.mark.usefixtures("_state_dir")
def test_exec_in_place_preserves_pid_and_inherits_lock_fd(tmp_path: Path) -> None:
    """Real exec preserves Tini-child PID, lock authority, and target identity."""
    context = _TopologyContext.create(tmp_path)
    try:
        context.assert_invariants()
    finally:
        returncode = context.release()
    assert returncode == 0, f"target must exit cleanly; rc={returncode}"
