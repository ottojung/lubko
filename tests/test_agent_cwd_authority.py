"""Strict durable working-directory authority for managed agents."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from lubko import agent

if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture(autouse=True)
def state_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Isolate managed-agent durable state.

    Returns:
        The isolated state directory.
    """
    state = tmp_path / "state"
    monkeypatch.setenv("XDG_STATE_HOME", str(state))
    return state


def test_build_agent_command_preserves_valid_persisted_cwd() -> None:
    """Pass the exact durable working directory to opencode."""
    cwd = "/workspace/exact-agent-tree"
    command = agent.build_agent_command(
        {"id": "aaaaaaaa", "cwd": cwd},
        "do work",
        is_continue=False,
    )

    assert command is not None
    assert command[command.index("--dir") + 1] == cwd


@pytest.mark.parametrize("cwd", ["", 0, False, None, []])
def test_build_agent_command_rejects_malformed_persisted_cwd(cwd: object) -> None:
    """Reject malformed durable cwd values instead of normalizing them."""
    meta: agent.Meta = {"id": "aaaaaaaa", "cwd": cwd}

    with pytest.raises(ValueError, match="managed-agent cwd is malformed"):
        agent.build_agent_command(meta, "do work", is_continue=False)


def test_runner_malformed_cwd_fails_before_spawn_and_aborts_cleanly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Abort a claimed runner before spawn when durable cwd is malformed."""
    aid = "aaaaaaaa"
    meta: agent.Meta = {
        "id": aid,
        "cwd": "",
        "state": "idle",
        "runner_reservation": {"state": "reserved", "gen": 1},
    }
    agent.write_meta(aid, meta)
    monkeypatch.setenv("LUBKO_RUNNER_GEN", "1")
    monkeypatch.setattr(
        agent,
        "_runner_loop",
        lambda *_a, **_kw: pytest.fail("underlying runner started with malformed cwd"),
    )
    monkeypatch.setattr(agent, "send_signal_group", lambda _m, _sig: None)
    monkeypatch.setattr(agent, "wait_group_dead", lambda _m, _timeout: True)

    with pytest.raises(ValueError, match="managed-agent cwd is malformed"):
        agent.runner(aid, "new")

    final = agent.read_meta(aid)
    assert final is not None
    assert final["state"] == "failed"
    assert final["active_runner"] is False
