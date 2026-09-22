"""Fail-closed tests for the supervisor identity cache record."""

from __future__ import annotations

import json
import os
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from pathlib import Path

    from lubko.worker import JobsConnection

import pytest

from lubko import supervise, supervisor
from tests._fake_authority_db import FakeAuthorityConnection


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
    assert isolated_state.read_text(encoding="utf-8") == raw


def test_daemon_recovers_malformed_identity_cache_from_the_row(
    isolated_state: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Daemon startup treats a torn cache as unusable and recovers from the row."""
    _write_raw(isolated_state, {"schema_version": "1", "pid": "4242", "start_time_ticks": "999"})
    with pytest.raises(supervise.MalformedSupervisorIdentityError):
        supervise.read_supervisor_pid()
    table = FakeAuthorityConnection()
    monkeypatch.setattr(supervisor, "load_worker_server", lambda: "srv-test")
    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    daemon._authority_conn_factory = lambda: cast("JobsConnection", table)
    daemon._write_pidfile()
    assert daemon._authority is not None
    assert daemon._authority.epoch == 1
    assert daemon._authority.pid == os.getpid()
    assert supervise.read_supervisor_pid() is not None
