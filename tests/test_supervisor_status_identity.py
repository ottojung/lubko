"""Supervisor status identity stays bound to one kernel-stable process."""

from __future__ import annotations

import os
from pathlib import Path  # ruff: ignore[typing-only-standard-library-import]

import pytest

from lubko import supervise


@pytest.fixture
def isolated_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Provide an isolated supervisor state directory.

    Returns:
        The temporary state root path.
    """
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
    supervise.supervisor_dir().mkdir(parents=True, exist_ok=True)
    return tmp_path


def _write_pid_and_status(pid: int, ticks: int, *, ready: bool = True) -> None:
    supervise.write_supervisor_pid(pid, ticks)
    status = supervise.SupervisorStatus(
        schema_version=supervise.SCHEMA_VERSION,
        supervisor_pid=pid,
        supervisor_start_time_ticks=ticks,
        started_at=1.0,
        applied_generation=7,
        mode=supervise.MODE_RUN,
        commit=None,
        child=None,
        intent=supervise.INTENT_RUN,
        restart_count=0,
        next_attempt_at=None,
        last_exit=None,
        mission=None,
        db_ready=None,
        ready=ready,
        message=None,
        worker_health=None,
    )
    supervise.write_status(status)


def test_reused_pid_between_proof_and_cmdline_is_stale(
    isolated_state: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Pinned liveness failure after reuse must reject stale ready status."""
    assert isolated_state.exists()
    pid = 4242
    ticks_a = 111
    _write_pid_and_status(pid, ticks_a, ready=True)

    # Numeric observations still look like the original supervisor (A) and a
    # supervisor-like cmdline after reuse. Old code would accept by combining
    # them; corrected code must re-prove the pinned original is still alive.
    monkeypatch.setattr(supervise, "_process_is_zombie", lambda _pid: False)
    monkeypatch.setattr(supervise, "proc_start_ticks", lambda _pid: ticks_a)
    monkeypatch.setattr(supervise, "_read_cmdline", lambda _pid: "lubko-supervisor --serve")

    def fake_open_pidfd(_pid: int) -> int:
        fd, _ = os.pipe()
        return fd

    def fake_send_gone(_pidfd: int, _sig: int) -> None:
        raise OSError(3, "No such process")

    monkeypatch.setattr(supervise, "_open_supervisor_pidfd", fake_open_pidfd)
    monkeypatch.setattr(supervise, "_pidfd_send_signal", fake_send_gone)

    assert supervise.read_status() is None
    assert supervise.wait_until_ready(7, timeout_seconds=0.05) is False


def test_current_supervisor_status_remains_readable(
    isolated_state: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Readable status while the exact supervisor stays live is accepted."""
    assert isolated_state.exists()
    pid = 4242
    ticks = 111
    _write_pid_and_status(pid, ticks, ready=True)

    monkeypatch.setattr(supervise, "_process_is_zombie", lambda _pid: False)
    monkeypatch.setattr(supervise, "proc_start_ticks", lambda _pid: ticks)
    monkeypatch.setattr(supervise, "_read_cmdline", lambda _pid: "lubko-supervisor --serve")

    def fake_open_pidfd(_pid: int) -> int:
        fd, _ = os.pipe()
        return fd

    monkeypatch.setattr(supervise, "_open_supervisor_pidfd", fake_open_pidfd)
    monkeypatch.setattr(supervise, "_pidfd_send_signal", lambda _pidfd, _sig: None)

    status = supervise.read_status()
    assert status is not None
    assert status.ready is True
    assert status.applied_generation == 7
    assert supervise.wait_until_ready(7, timeout_seconds=0.05) is True


def test_pidfd_unavailable_fails_closed(
    isolated_state: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Without a stable pin the status must be treated as stale."""
    assert isolated_state.exists()
    pid = 4242
    ticks = 111
    _write_pid_and_status(pid, ticks, ready=True)

    monkeypatch.setattr(supervise, "_process_is_zombie", lambda _pid: False)
    monkeypatch.setattr(supervise, "proc_start_ticks", lambda _pid: ticks)
    monkeypatch.setattr(supervise, "_read_cmdline", lambda _pid: "lubko-supervisor")

    def missing(_pid: int) -> int:
        raise OSError(2, "pidfd_open failed")

    monkeypatch.setattr(supervise, "_open_supervisor_pidfd", missing)

    assert supervise.read_status() is None
