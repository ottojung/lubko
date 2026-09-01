"""Strict durable native-session continuation authority."""

from __future__ import annotations

import pytest

from lubko import agent


@pytest.mark.parametrize("value", [123, True, False, 1.5, [], {}, ""])
def test_resolve_session_mode_rejects_malformed_persisted_identity(
    monkeypatch: pytest.MonkeyPatch, value: object
) -> None:
    """Malformed durable state cannot authorize continuation via rediscovery."""
    monkeypatch.setattr(agent, "discover_session_id", lambda _aid: "discovered")
    meta: agent.Meta = {"id": "aaaaaaaa", "native_session_id": value}

    with pytest.raises(ValueError, match="native_session_id is malformed"):
        agent._resolve_session_mode(meta)


@pytest.mark.parametrize("value", [123, True, False, 1.5, [], {}, ""])
def test_build_agent_command_rejects_malformed_persisted_identity(
    monkeypatch: pytest.MonkeyPatch, value: object
) -> None:
    """Malformed durable state never becomes or overrides an argv session ID."""
    monkeypatch.setattr(agent, "discover_session_id", lambda _aid: "discovered")
    meta: agent.Meta = {
        "id": "aaaaaaaa",
        "cwd": "/workspace/exact-agent-tree",
        "native_session_id": value,
    }

    with pytest.raises(ValueError, match="native_session_id is malformed"):
        agent.build_agent_command(meta, "do work", is_continue=True)


def test_none_and_discovered_only_continuation_remain_valid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Genuine absence may still use canonical rediscovered continuation state."""
    monkeypatch.setattr(agent, "discover_session_id", lambda _aid: "discovered")
    meta: agent.Meta = {
        "id": "aaaaaaaa",
        "cwd": "/workspace/exact-agent-tree",
        "native_session_id": None,
    }

    assert agent._resolve_session_mode(meta) == "continue"
    command = agent.build_agent_command(meta, "do work", is_continue=True)
    assert command is not None
    assert command[command.index("--session") + 1] == "discovered"


def test_valid_recorded_but_missing_session_still_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A valid recorded identity that vanished must not start a new session."""
    monkeypatch.setattr(agent, "discover_session_id", lambda _aid: None)
    meta: agent.Meta = {"id": "aaaaaaaa", "native_session_id": "recorded"}

    assert agent._resolve_session_mode(meta) is None


def test_valid_recorded_identity_is_preserved_in_continue_command(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Canonical recorded continuation state remains authoritative."""
    monkeypatch.setattr(agent, "discover_session_id", lambda _aid: "discovered")
    meta: agent.Meta = {
        "id": "aaaaaaaa",
        "cwd": "/workspace/exact-agent-tree",
        "native_session_id": "recorded",
    }

    assert agent._resolve_session_mode(meta) == "continue"
    command = agent.build_agent_command(meta, "do work", is_continue=True)
    assert command is not None
    assert command[command.index("--session") + 1] == "recorded"
