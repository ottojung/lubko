"""Exact managed-invocation liveness regressions."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

from lubko import agent

if TYPE_CHECKING:
    import pytest


def _meta() -> dict[str, object]:
    return {
        "pid": 123,
        "start_time": 7,
        "id": "abc",
        "active_runner": False,
        "state": "running",
    }


def test_is_alive_fails_closed_when_pinned_process_exits_after_identity_proof(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A reused numeric PID cannot keep a dead invocation logically live."""
    meta = _meta()
    monkeypatch.setattr(agent, "open_pidfd", lambda _pid: 91)
    monkeypatch.setattr(agent, "proc_start_ticks", lambda _pid: 7)
    monkeypatch.setattr(agent, "env_has_marker", lambda _pid, _aid: True)
    monkeypatch.setattr(
        agent, "pidfd_send_signal", lambda _fd, _sig: (_ for _ in ()).throw(ProcessLookupError())
    )
    closed: list[int] = []
    monkeypatch.setattr(os, "close", closed.append)

    assert not agent.is_alive(meta)
    assert closed == [91]

    monkeypatch.setattr(agent, "runner_alive", lambda _meta: False)
    monkeypatch.setattr(agent, "reservation_in_flight", lambda _meta: False)
    decision: dict[str, object] = {}
    agent._apply_locked_transition(meta, decision, prompt="new work", steer=False, mode="continue")
    assert decision["action"] != "busy"


def test_is_alive_accepts_same_pinned_live_invocation(monkeypatch: pytest.MonkeyPatch) -> None:
    """A still-live exact invocation is accepted through its pinned pidfd."""
    meta = _meta()
    monkeypatch.setattr(agent, "open_pidfd", lambda _pid: 92)
    monkeypatch.setattr(agent, "proc_start_ticks", lambda _pid: 7)
    monkeypatch.setattr(agent, "env_has_marker", lambda _pid, _aid: True)
    delivered: list[tuple[int, int]] = []
    monkeypatch.setattr(agent, "pidfd_send_signal", lambda fd, sig: delivered.append((fd, sig)))
    monkeypatch.setattr(os, "close", lambda _fd: None)

    assert agent.is_alive(meta)
    assert delivered == [(92, 0)]
