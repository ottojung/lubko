"""Regression tests for the two-phase handoff role separation.

Proves:
- B (successor) exits before durable writes on handoff failure.
- B (successor) writes durable state only after successful protocol.
- A (old) returns from run() after reconcile when _handoff_completed,
  before _write_status, sleep, or _shutdown.
- A retires its pidfile before sending TRANSFER.
- Failed TRANSFER restores A's pidfile.
- READY failure/timeout/EOF closes all pipe fds and reaps B.
- retire_supervisor_pid() failure closes all pipe fds and reaps B.
"""

from __future__ import annotations

import json
import os
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pytest

from lubko import supervise
from lubko.supervisor import (
    Settings,
    SupervisorDaemon,
    _HandoffPipes,  # ruff: ignore[import-private-name]
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


@pytest.mark.usefixtures("_state_dir")
def test_b_failure_exits_before_durable_writes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When handoff protocol fails, B returns from run() without any durable write."""
    _write_fresh_state()
    daemon = SupervisorDaemon(Settings())

    writes: list[str] = []

    def _track_pidfile(_self: object) -> None:
        writes.append("pidfile")

    def _track_runtime(_self: object) -> None:
        writes.append("runtime")

    def _track_status(_self: object, *_args: object) -> None:
        writes.append("status")

    monkeypatch.setattr(SupervisorDaemon, "_write_pidfile", _track_pidfile)
    monkeypatch.setattr(SupervisorDaemon, "_persist_runtime_commit", _track_runtime)
    monkeypatch.setattr(SupervisorDaemon, "_write_status", _track_status)
    monkeypatch.setattr(SupervisorDaemon, "_invalidate_stale_status", _noop_invalidate)
    monkeypatch.setattr("lubko.supervisor.normalize_cross_boot_state", _noop_normalize)
    monkeypatch.setattr(SupervisorDaemon, "_install_signal_handlers", _noop_signals)
    monkeypatch.setattr("lubko.supervisor._durable_log_handlers", list)
    monkeypatch.setattr(
        SupervisorDaemon,
        "_in_handoff_mode",
        staticmethod(lambda: True),
    )
    monkeypatch.setattr(
        SupervisorDaemon,
        "_run_handoff_protocol",
        lambda _self: False,
    )

    daemon.run()

    assert writes == []


@pytest.mark.usefixtures("_state_dir")
def test_b_success_proceeds_to_durable_writes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When handoff protocol succeeds, B writes pidfile and runtime_commit."""
    _write_fresh_state()
    daemon = SupervisorDaemon(Settings())
    daemon._stopping = True

    writes: list[str] = []

    def _track_pidfile(_self: object) -> None:
        writes.append("pidfile")

    def _track_runtime(_self: object) -> None:
        writes.append("runtime")

    monkeypatch.setattr(SupervisorDaemon, "_write_pidfile", _track_pidfile)
    monkeypatch.setattr(SupervisorDaemon, "_persist_runtime_commit", _track_runtime)
    monkeypatch.setattr(SupervisorDaemon, "_invalidate_stale_status", _noop_invalidate)
    monkeypatch.setattr("lubko.supervisor.normalize_cross_boot_state", _noop_normalize)
    monkeypatch.setattr(SupervisorDaemon, "_install_signal_handlers", _noop_signals)
    monkeypatch.setattr(SupervisorDaemon, "_write_status", lambda _self, *_a: None)
    monkeypatch.setattr("lubko.supervisor._durable_log_handlers", list)
    monkeypatch.setattr(
        SupervisorDaemon,
        "_in_handoff_mode",
        staticmethod(lambda: True),
    )
    monkeypatch.setattr(
        SupervisorDaemon,
        "_run_handoff_protocol",
        lambda _self: True,
    )

    daemon.run()

    assert "pidfile" in writes
    assert "runtime" in writes


@pytest.mark.usefixtures("_state_dir")
def test_a_handoff_completed_returns_before_status_sleep_shutdown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """After reconcile sets _handoff_completed, run() returns immediately."""
    _write_fresh_state()
    daemon = SupervisorDaemon(Settings())
    daemon._ownership_fd = -1

    calls: list[str] = []

    def _fake_reconcile(_self: object, _now: float) -> None:
        daemon._handoff_completed = True

    def _track_status(_self: object, *_args: object) -> None:
        calls.append("write_status")

    def _track_sleep(_seconds: float) -> None:
        calls.append("sleep")

    def _track_shutdown(_self: object) -> None:
        calls.append("shutdown")

    monkeypatch.setattr(SupervisorDaemon, "reconcile", _fake_reconcile)
    monkeypatch.setattr(SupervisorDaemon, "_write_status", _track_status)
    monkeypatch.setattr(SupervisorDaemon, "_shutdown", _track_shutdown)
    monkeypatch.setattr("lubko.supervisor.time.sleep", _track_sleep)
    monkeypatch.setattr("lubko.supervisor._durable_log_handlers", list)
    monkeypatch.setattr(SupervisorDaemon, "_write_pidfile", lambda _self: None)
    monkeypatch.setattr(SupervisorDaemon, "_persist_runtime_commit", lambda _self: None)
    monkeypatch.setattr(SupervisorDaemon, "_invalidate_stale_status", _noop_invalidate)
    monkeypatch.setattr("lubko.supervisor.normalize_cross_boot_state", _noop_normalize)
    monkeypatch.setattr(SupervisorDaemon, "_install_signal_handlers", _noop_signals)

    daemon._handoff_completed = False
    daemon._stopping = False
    daemon.run()

    assert "shutdown" not in calls
    assert "sleep" not in calls
    assert calls.count("write_status") <= 1


@pytest.mark.usefixtures("_state_dir")
def test_a_retires_pidfile_before_transfer(monkeypatch: pytest.MonkeyPatch) -> None:
    """A retires its own pidfile before sending TRANSFER.

    Regression: before this fix, A's pidfile survived until A exited, so B's
    _write_pidfile() could see A's live pid and raise SystemExit.
    """
    _write_fresh_state()
    daemon = SupervisorDaemon(Settings())
    daemon._ownership_fd = -1

    pidfile_removed: list[bool] = []

    def _fake_retire() -> tuple[int, int]:
        pidfile_removed.append(True)
        return os.getpid(), 12345

    monkeypatch.setattr("lubko.supervise.retire_supervisor_pid", _fake_retire)
    monkeypatch.setattr(
        "lubko.supervise.restore_supervisor_pid",
        lambda _pid, _ticks: None,
    )
    monkeypatch.setattr("lubko.supervisor.os.set_inheritable", lambda _fd, _val: None)

    original_close = os.close
    original_write = os.write

    def _safe_close(fd: int) -> None:
        if fd >= 0:
            original_close(fd)

    def _track_transfer_write(fd: int, data: bytes) -> int:
        assert pidfile_removed, "pidfile should be retired before TRANSFER write"
        return original_write(fd, data)

    monkeypatch.setattr("lubko.supervisor.os.write", _track_transfer_write)
    monkeypatch.setattr("lubko.supervisor.os.close", _safe_close)

    transfer_r, transfer_w = os.pipe()
    pipes = _HandoffPipes(ready_r=-1, transfer_r=transfer_r, transfer_w=transfer_w)
    daemon._send_handoff_transfer(
        process=__import__("subprocess").Popen(["true"]),
        pipes=pipes,
        confirmed="b" * 40,
        ownership_fd=-1,
    )


@pytest.mark.usefixtures("_state_dir")
def test_failed_transfer_restores_pidfile(monkeypatch: pytest.MonkeyPatch) -> None:
    """Failed TRANSFER write restores A's pidfile.

    Regression: before this fix, a failed TRANSFER left A without a pidfile,
    making A undiscoverable by CLIs.
    """
    _write_fresh_state()
    daemon = SupervisorDaemon(Settings())
    daemon._ownership_fd = -1

    restored_pid: list[tuple[int, int]] = []

    def _fake_retire() -> tuple[int, int]:
        return os.getpid(), 12345

    def _fake_restore(pid: int, ticks: int) -> None:
        restored_pid.append((pid, ticks))

    monkeypatch.setattr("lubko.supervise.retire_supervisor_pid", _fake_retire)
    monkeypatch.setattr("lubko.supervise.restore_supervisor_pid", _fake_restore)

    msg = "transfer pipe broken"

    def _fail_transfer_write(_fd: int, _data: bytes) -> int:
        raise OSError(msg)

    monkeypatch.setattr("lubko.supervisor.os.write", _fail_transfer_write)
    monkeypatch.setattr("lubko.supervisor.os.set_inheritable", lambda _fd, _val: None)

    original_close = os.close

    def _safe_close(fd: int) -> None:
        if fd >= 0:
            original_close(fd)

    monkeypatch.setattr("lubko.supervisor.os.close", _safe_close)

    transfer_r, transfer_w = os.pipe()
    pipes = _HandoffPipes(ready_r=-1, transfer_r=transfer_r, transfer_w=transfer_w)
    daemon._send_handoff_transfer(
        process=__import__("subprocess").Popen(["true"]),
        pipes=pipes,
        confirmed="b" * 40,
        ownership_fd=-1,
    )

    assert restored_pid, "pidfile should be restored after failed TRANSFER"
    assert restored_pid[0] == (os.getpid(), 12345)


@pytest.mark.usefixtures("_state_dir")
def test_ready_failure_closes_all_pipes_and_reaps_b(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """READY failure/timeout/EOF closes all pipe fds and reaps B.

    Regression: before this fix, _await_handoff_ready leaked transfer_r
    and transfer_w on abort, and did not close them when B was already
    spawned.
    """
    _write_fresh_state()
    daemon = SupervisorDaemon(Settings())
    daemon._ownership_fd = -1

    closed_fds: list[int] = []

    original_close = os.close

    def _track_close(fd: int) -> None:
        closed_fds.append(fd)
        if fd >= 0:
            original_close(fd)

    process = MagicMock()
    process.wait.return_value = 0

    monkeypatch.setattr("lubko.supervisor.os.close", _track_close)
    monkeypatch.setattr("lubko.supervisor.os.set_inheritable", lambda _fd, _val: None)

    ready_r, ready_w = os.pipe()
    os.close(ready_w)
    transfer_r, transfer_w = os.pipe()
    pipes = _HandoffPipes(ready_r=ready_r, transfer_r=transfer_r, transfer_w=transfer_w)

    result = daemon._await_handoff_ready(
        process=process,
        pipes=pipes,
        confirmed="b" * 40,
        ownership_fd=-1,
    )

    assert result is False
    process.kill.assert_called_once()
    process.wait.assert_called_once()
    assert transfer_r in closed_fds, "transfer_r should be closed"
    assert transfer_w in closed_fds, "transfer_w should be closed"


@pytest.mark.usefixtures("_state_dir")
def test_retire_pidfile_failure_closes_all_pipes_and_reaps_b(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """retire_supervisor_pid() failure closes all pipe fds and reaps B.

    Regression: before this fix, _send_handoff_transfer leaked transfer_r,
    transfer_w, and left B stuck when retire_supervisor_pid() failed.
    """
    _write_fresh_state()
    daemon = SupervisorDaemon(Settings())
    daemon._ownership_fd = -1

    closed_fds: list[int] = []

    original_close = os.close

    def _track_close(fd: int) -> None:
        closed_fds.append(fd)
        if fd >= 0:
            original_close(fd)

    process = MagicMock()
    process.wait.return_value = 0

    msg = "pidfile mismatch"

    def _fail_retire() -> tuple[int, int]:
        raise supervise.PidfileIdentityMismatchError(msg)

    monkeypatch.setattr("lubko.supervise.retire_supervisor_pid", _fail_retire)
    monkeypatch.setattr("lubko.supervisor.os.close", _track_close)
    monkeypatch.setattr("lubko.supervisor.os.set_inheritable", lambda _fd, _val: None)

    transfer_r, transfer_w = os.pipe()
    pipes = _HandoffPipes(ready_r=-1, transfer_r=transfer_r, transfer_w=transfer_w)

    daemon._send_handoff_transfer(
        process=process,
        pipes=pipes,
        confirmed="b" * 40,
        ownership_fd=-1,
    )

    process.kill.assert_called_once()
    process.wait.assert_called_once()
    assert transfer_r in closed_fds, "transfer_r should be closed"
    assert transfer_w in closed_fds, "transfer_w should be closed"
