"""Regression tests for the two-phase handoff role separation.

Proves:
- B (successor) exits before durable writes on handoff failure.
- B (successor) writes durable state only after successful protocol.
- A (old) returns from run() after reconcile when _handoff_completed,
  before _write_status, sleep, or _shutdown.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest

from lubko import supervise
from lubko.supervisor import Settings, SupervisorDaemon

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
