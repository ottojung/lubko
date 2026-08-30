"""Fail-closed tests for the durable supervisor identity record."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

import pytest

from lubko import startup_contract as sc
from lubko import supervise, supervisor


@pytest.fixture
def isolated_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Provide an isolated supervisor identity path.

    Returns:
        The isolated identity path.
    """
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
    supervise.supervisor_dir().mkdir(parents=True, exist_ok=True)
    return supervise.supervisor_pid_path()


def _write_raw(path: Path, value: object) -> str:
    raw = json.dumps(value)
    path.write_text(raw, encoding="utf-8")
    return raw


def test_supervisor_identity_round_trips_and_absence_is_distinct(isolated_state: Path) -> None:
    """Canonical identity round-trips while genuine absence remains None."""
    assert isolated_state.parent.exists()
    assert supervise.read_supervisor_pid() is None
    supervise.write_supervisor_pid(4242, 999)
    assert supervise.read_supervisor_pid() == (4242, 999)


@pytest.mark.parametrize(
    "record",
    [
        {"schema_version": "1", "pid": 4242, "start_time_ticks": 999},
        {"schema_version": 1.0, "pid": 4242, "start_time_ticks": 999},
        {"schema_version": True, "pid": 4242, "start_time_ticks": 999},
        {"schema_version": 2, "pid": 4242, "start_time_ticks": 999},
        {"schema_version": 1, "pid": "4242", "start_time_ticks": 999},
        {"schema_version": 1, "pid": 4242.9, "start_time_ticks": 999},
        {"schema_version": 1, "pid": True, "start_time_ticks": 999},
        {"schema_version": 1, "pid": 0, "start_time_ticks": 999},
        {"schema_version": 1, "pid": -1, "start_time_ticks": 999},
        {"schema_version": 1, "pid": 4242, "start_time_ticks": "999"},
        {"schema_version": 1, "pid": 4242, "start_time_ticks": 999.5},
        {"schema_version": 1, "pid": 4242, "start_time_ticks": False},
        {"schema_version": 1, "pid": 4242, "start_time_ticks": -1},
    ],
)
def test_malformed_supervisor_identity_fails_closed_without_mutation(
    isolated_state: Path, record: dict[str, object]
) -> None:
    """Malformed present authority is rejected without mutation."""
    raw = _write_raw(isolated_state, record)
    with pytest.raises(supervise.MalformedSupervisorIdentityError):
        supervise.read_supervisor_pid()
    assert supervise.supervisor_running() is False
    proof = sc.verify_live_topology()
    assert proof.ok is False
    assert "identity record is malformed" in proof.message
    assert isolated_state.read_text(encoding="utf-8") == raw


def test_daemon_refuses_to_overwrite_malformed_supervisor_identity(isolated_state: Path) -> None:
    """Daemon startup preserves malformed recovery authority and exits."""
    raw = _write_raw(
        isolated_state, {"schema_version": "1", "pid": "4242", "start_time_ticks": "999"}
    )
    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    with pytest.raises(SystemExit) as exc_info:
        daemon._write_pidfile()
    assert exc_info.value.code == 1
    assert isolated_state.read_text(encoding="utf-8") == raw
