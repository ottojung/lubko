"""Strict durable managed-agent variant configuration."""

from __future__ import annotations

import pytest

from lubko import agent


def _meta(**extra: object) -> agent.Meta:
    return {"id": "aaaaaaaa", "cwd": "/workspace/exact-agent-tree", **extra}


def test_absent_variant_uses_canonical_default() -> None:
    """Absent durable configuration uses the canonical default."""
    command = agent.build_agent_command(_meta(), "do work", is_continue=False)
    assert command is not None
    assert command[command.index("--variant") + 1] == agent.DEFAULT_VARIANT


def test_valid_variant_preserved_new() -> None:
    """Valid durable variants remain unchanged for new sessions."""
    command = agent.build_agent_command(_meta(variant="custom"), "do work", is_continue=False)
    assert command is not None
    assert command[command.index("--variant") + 1] == "custom"


def test_valid_variant_preserved_continue(monkeypatch: pytest.MonkeyPatch) -> None:
    """Valid durable variants remain unchanged for continuation."""
    monkeypatch.setattr(agent, "discover_session_id", lambda _aid: "discovered")
    command = agent.build_agent_command(_meta(variant="custom"), "do work", is_continue=True)
    assert command is not None
    assert command[command.index("--variant") + 1] == "custom"


@pytest.mark.parametrize("value", [None, "", 0, 123, True, False, 1.5, [], {}])
def test_malformed_present_variant_fails_closed_new(value: object) -> None:
    """Malformed present values cannot affect new-session argv."""
    with pytest.raises(ValueError, match="managed-agent variant is malformed"):
        agent.build_agent_command(_meta(variant=value), "do work", is_continue=False)


@pytest.mark.parametrize("value", [None, "", 0, 123, True, False, 1.5, [], {}])
def test_malformed_present_variant_fails_closed_continue(
    monkeypatch: pytest.MonkeyPatch, value: object
) -> None:
    """Malformed present values cannot affect continuation argv."""
    monkeypatch.setattr(agent, "discover_session_id", lambda _aid: "discovered")
    with pytest.raises(ValueError, match="managed-agent variant is malformed"):
        agent.build_agent_command(_meta(variant=value), "do work", is_continue=True)
