"""Fail-closed stop/kill orchestration for malformed durable agent authority."""

from __future__ import annotations

import copy
from types import SimpleNamespace

import pytest

from lubko import agent


@pytest.mark.parametrize("command", [agent.cmd_stop, agent.cmd_kill])
@pytest.mark.parametrize("bad", [123, True, 1.5, [], {}, ""])
def test_stop_like_rejects_malformed_pending_prompt(
    monkeypatch: pytest.MonkeyPatch,
    command: object,
    bad: object,
) -> None:
    """Malformed pending-prompt authority returns a bounded failure unchanged."""
    meta: agent.Meta = {
        "id": "audit",
        "state": "running",
        "pending_prompt": bad,
        "runner_reservation": None,
    }
    before = copy.deepcopy(meta)
    monkeypatch.setattr(agent, "read_meta", lambda _aid: meta)
    monkeypatch.setattr(agent, "is_alive", lambda _m: False)
    monkeypatch.setattr(agent, "group_alive", lambda _m: False)
    signalled: list[object] = []

    def fake_signal(*args: object, **kwargs: object) -> int:
        signalled.append((args, kwargs))
        return agent.EXIT_OK

    monkeypatch.setattr(agent, "_signal_live_invocation", fake_signal)

    result = command(SimpleNamespace(agent_id="audit"))  # type: ignore[operator]

    assert result == agent.EXIT_ERROR
    assert meta == before
    assert signalled == []


@pytest.mark.parametrize("command", [agent.cmd_stop, agent.cmd_kill])
@pytest.mark.parametrize(
    "reservation",
    [
        {},
        {"state": "bogus"},
        {"state": 1},
    ],
)
def test_stop_like_rejects_malformed_runner_reservation(
    monkeypatch: pytest.MonkeyPatch,
    command: object,
    reservation: object,
) -> None:
    """Malformed runner-reservation authority fails without signals or mutation."""
    meta: agent.Meta = {
        "id": "audit",
        "state": "running",
        "pending_prompt": None,
        "runner_reservation": reservation,
    }
    before = copy.deepcopy(meta)
    monkeypatch.setattr(agent, "read_meta", lambda _aid: meta)
    monkeypatch.setattr(agent, "is_alive", lambda _m: False)
    monkeypatch.setattr(agent, "group_alive", lambda _m: False)
    signalled: list[object] = []

    def fake_signal(*args: object, **kwargs: object) -> int:
        signalled.append((args, kwargs))
        return agent.EXIT_OK

    monkeypatch.setattr(agent, "_signal_live_invocation", fake_signal)

    result = command(SimpleNamespace(agent_id="audit"))  # type: ignore[operator]

    assert result == agent.EXIT_ERROR
    assert meta == before
    assert signalled == []
