"""Strict persisted agent-ID authority for process liveness."""

import os

import pytest

from lubko import agent


@pytest.mark.parametrize("bad_aid", [123, True, "", "AAAAAAAA", "not-hex"])
def test_invocation_liveness_rejects_malformed_persisted_agent_id(
    monkeypatch: pytest.MonkeyPatch,
    bad_aid: object,
) -> None:
    """Malformed IDs fail before invocation marker matching."""
    meta = agent.idle_meta("aaaaaaaa", str(os.environ["XDG_STATE_HOME"]), None)
    meta.update({"state": "running", "id": bad_aid, "pid": 4242, "start_time": 111})
    marker_checks: list[tuple[int, str]] = []

    monkeypatch.setattr(agent, "open_pidfd", lambda _pid: 77)
    monkeypatch.setattr(agent, "proc_start_ticks", lambda _pid: 111)

    def record_marker(pid: int, aid: str) -> bool:
        marker_checks.append((pid, aid))
        return True

    monkeypatch.setattr(agent, "env_has_marker", record_marker)
    monkeypatch.setattr(agent, "pidfd_send_signal", lambda _fd, _sig: None)
    monkeypatch.setattr(os, "close", lambda _fd: None)

    assert not agent.is_alive(meta)
    assert marker_checks == []


def test_invocation_liveness_accepts_canonical_persisted_agent_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Canonical IDs still permit exact invocation marker matching."""
    meta = agent.idle_meta("aaaaaaaa", str(os.environ["XDG_STATE_HOME"]), None)
    meta.update({"state": "running", "pid": 4242, "start_time": 111})
    marker_checks: list[tuple[int, str]] = []

    monkeypatch.setattr(agent, "open_pidfd", lambda _pid: 77)
    monkeypatch.setattr(agent, "proc_start_ticks", lambda _pid: 111)

    def record_marker(pid: int, aid: str) -> bool:
        marker_checks.append((pid, aid))
        return True

    monkeypatch.setattr(agent, "env_has_marker", record_marker)
    monkeypatch.setattr(agent, "pidfd_send_signal", lambda _fd, _sig: None)
    monkeypatch.setattr(os, "close", lambda _fd: None)

    assert agent.is_alive(meta)
    assert marker_checks == [(4242, "aaaaaaaa")]


@pytest.mark.parametrize("bad_aid", [123, True, "", "AAAAAAAA", "not-hex"])
def test_runner_liveness_rejects_malformed_persisted_agent_id(
    monkeypatch: pytest.MonkeyPatch,
    bad_aid: object,
) -> None:
    """Malformed IDs fail before runner marker matching."""
    meta = agent.idle_meta("aaaaaaaa", str(os.environ["XDG_STATE_HOME"]), None)
    meta.update({"id": bad_aid, "runner_pid": 4242, "runner_start_time": 111})
    marker_checks: list[tuple[int, str]] = []

    monkeypatch.setattr(agent, "open_pidfd", lambda _pid: 77)
    monkeypatch.setattr(agent, "proc_start_ticks", lambda _pid: 111)

    def record_marker(pid: int, aid: str) -> bool:
        marker_checks.append((pid, aid))
        return True

    monkeypatch.setattr(agent, "env_has_marker", record_marker)
    monkeypatch.setattr(agent, "pidfd_send_signal", lambda _fd, _sig: None)
    monkeypatch.setattr(os, "close", lambda _fd: None)

    assert not agent.runner_alive(meta)
    assert marker_checks == []


def test_runner_liveness_accepts_canonical_persisted_agent_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Canonical IDs still permit exact runner marker matching."""
    meta = agent.idle_meta("aaaaaaaa", str(os.environ["XDG_STATE_HOME"]), None)
    meta.update({"runner_pid": 4242, "runner_start_time": 111})
    marker_checks: list[tuple[int, str]] = []

    monkeypatch.setattr(agent, "open_pidfd", lambda _pid: 77)
    monkeypatch.setattr(agent, "proc_start_ticks", lambda _pid: 111)

    def record_marker(pid: int, aid: str) -> bool:
        marker_checks.append((pid, aid))
        return True

    monkeypatch.setattr(agent, "env_has_marker", record_marker)
    monkeypatch.setattr(agent, "pidfd_send_signal", lambda _fd, _sig: None)
    monkeypatch.setattr(os, "close", lambda _fd: None)

    assert agent.runner_alive(meta)
    assert marker_checks == [(4242, "aaaaaaaa")]
