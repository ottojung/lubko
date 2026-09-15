"""Regression tests for the exec-in-place handoff architecture.

Proves:
- Preflight probe validates lock, signals READY, exits before any durable write.
- Preflight probe never enters reconcile, never writes pidfile/status/state.
- A retires its pidfile before exec-in-place.
- Failed exec restores A's pidfile and continues.
- READY failure kills/reaps probe and aborts handoff.
- Probe nonzero exit kills/reaps and aborts handoff.
- No second lifecycle authority exists at any handoff boundary.
"""

from __future__ import annotations

import fcntl
import json
import os
import select
import signal
import sys
from contextlib import suppress
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from lubko import supervise
from lubko.supervisor import (
    Settings,
    SupervisorDaemon,
    _HandoffPipes,
)


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
# Exec-in-place: real fork, real exec, real lock, identity continuity
# ---------------------------------------------------------------------------


def _exec_child_main(report_r: int, hold_w: int, target: str, target_commit: str) -> None:
    """Child entry point: set up lock and exec into target."""
    os.close(report_r)
    os.close(hold_w)
    os.environ["XDG_STATE_HOME"] = str(Path(target).parent / "state")
    os.set_inheritable(int(os.environ["LUBKO_TEST_REPORT_FD"]), True)  # ruff: ignore[boolean-positional-value-in-call]
    os.set_inheritable(int(os.environ["LUBKO_TEST_HOLD_FD"]), True)  # ruff: ignore[boolean-positional-value-in-call]
    supervise.supervisor_dir().mkdir(parents=True, exist_ok=True)
    lock_fd = supervise.acquire_supervisor_lock()
    ticks = supervise.proc_start_ticks(os.getpid())
    if ticks is None:
        report_fd = int(os.environ["LUBKO_TEST_REPORT_FD"])
        os.write(report_fd, b'{"error": "no ticks"}\n')
        os._exit(2)
    supervise.write_supervisor_pid(os.getpid(), ticks)
    result = SupervisorDaemon._exec_in_place(str(target), target_commit, lock_fd)
    if result is False:
        report_fd = int(os.environ["LUBKO_TEST_REPORT_FD"])
        os.write(report_fd, b'{"error": "exec_in_place returned False"}\n')
        os._exit(3)


def test_exec_in_place_preserves_pid_lock_and_target_identity(  # ruff: ignore[too-many-statements,too-many-locals]
    tmp_path: Path,
) -> None:
    """After exec-in-place the child holds the same lock and commit identity."""
    target_commit = "abcdef1234567890abcdef1234567890abcdef12"
    target = tmp_path / "exec-target"

    target.write_text(
        f"#!{sys.executable}\n"
        "import fcntl, json, os\n"
        'report_fd = int(os.environ["LUBKO_TEST_REPORT_FD"])\n'
        'hold_fd = int(os.environ["LUBKO_TEST_HOLD_FD"])\n'
        "lock_fd = int(os.environ['LUBKO_SUPERVISOR_HANDOFF_FD'])\n"
        "lock_path = os.environ['LUBKO_SUPERVISOR_HANDOFF_PATH']\n"
        "commit = os.environ['LUBKO_SUPERVISOR_HANDOFF_TARGET_COMMIT']\n"
        "fd_stat = os.fstat(lock_fd)\n"
        "report = json.dumps({\n"
        '    "pid": os.getpid(),\n'
        '    "lock_fd_open": True,\n'
        '    "lock_path": lock_path,\n'
        '    "target_commit": commit,\n'
        '}) + "\\n"\n'
        "os.write(report_fd, report.encode())\n"
        "os.read(hold_fd, 1)\n",
        encoding="utf-8",
    )
    target.chmod(0o700)

    report_r, report_w = os.pipe()
    hold_r, hold_w = os.pipe()

    os.environ["LUBKO_TEST_REPORT_FD"] = str(report_w)
    os.environ["LUBKO_TEST_HOLD_FD"] = str(hold_r)

    child_pid = os.fork()

    if child_pid == 0:
        try:
            _exec_child_main(report_r, hold_w, str(target), target_commit)
        except OSError:
            os._exit(4)
    else:
        child_reaped = False
        try:
            os.close(report_w)
            os.close(hold_r)

            ready, _, _ = select.select([report_r], [], [], 3.0)
            assert ready, "child did not send report within 3s"
            raw = os.read(report_r, 4096)
            assert raw, "empty report from child"
            report = json.loads(raw)
            assert "error" not in report, report.get("error")

            pid = report["pid"]
            assert pid == child_pid, f"expected pid {child_pid}, got {pid}"
            assert report["lock_fd_open"] is True
            commit = report["target_commit"]
            assert commit == target_commit, f"expected commit {target_commit}, got {commit}"
            lock_path = report["lock_path"]

            # Lock still held: second flock must fail.
            lock_fd2 = os.open(lock_path, os.O_RDWR)
            try:
                with pytest.raises(BlockingIOError):
                    fcntl.flock(lock_fd2, fcntl.LOCK_EX | fcntl.LOCK_NB)
            finally:
                os.close(lock_fd2)

            # Release the child so it exits.
            os.close(hold_w)
            hold_w = -1

            pid2, status = os.waitpid(child_pid, 0)
            assert pid2 == child_pid
            assert os.WIFEXITED(status)
            assert os.WEXITSTATUS(status) == 0
            child_reaped = True

            # Lock now free.
            lock_fd3 = os.open(lock_path, os.O_RDWR)
            try:
                fcntl.flock(lock_fd3, fcntl.LOCK_EX | fcntl.LOCK_NB)
                fcntl.flock(lock_fd3, fcntl.LOCK_UN)
            finally:
                os.close(lock_fd3)
        finally:
            os.close(report_r)
            if hold_w >= 0:
                os.close(hold_w)
            if not child_reaped:
                with suppress(ProcessLookupError):
                    os.kill(child_pid, signal.SIGKILL)
                with suppress(ChildProcessError):
                    os.waitpid(child_pid, 0)
