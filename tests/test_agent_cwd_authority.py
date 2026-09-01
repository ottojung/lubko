"""Managed-agent persisted working-directory authority invariants."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

import pytest

from lubko import agent

if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture(autouse=True)
def state_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Isolate durable managed-agent state for each test.

    Returns:
        The isolated state directory.
    """
    state = tmp_path / "state"
    monkeypatch.setenv("XDG_STATE_HOME", str(state))
    return state


def test_agent_command_uses_exact_persisted_working_directory(tmp_path: Path) -> None:
    """Valid durable cwd is passed to opencode unchanged."""
    cwd = str(tmp_path / "worktree")
    meta = agent.idle_meta("aaaaaaaa", cwd, None)

    command = agent.build_agent_command(meta, "work", is_continue=False)

    assert command is not None
    assert command[command.index("--dir") + 1] == cwd


@pytest.mark.parametrize("cwd", ["", 0, False, None, [], {}])
def test_agent_command_rejects_malformed_persisted_working_directory(cwd: object) -> None:
    """Malformed durable cwd cannot become command execution authority."""
    meta = agent.idle_meta("aaaaaaaa", "/valid", None)
    meta["cwd"] = cwd

    with pytest.raises(ValueError, match="managed-agent cwd is malformed"):
        agent.build_agent_command(meta, "work", is_continue=False)


def test_runner_malformed_working_directory_fails_before_invocation_spawn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Runner aborts cleanly before invocation spawn for malformed cwd."""
    aid = "aaaaaaaa"
    meta = agent.idle_meta(aid, "/valid", None)
    meta["cwd"] = ""
    meta["state"] = "running"
    meta["active_runner"] = True
    meta["runner_gen"] = 1
    meta["runner_reservation"] = {
        "gen": 1,
        "owner_pid": os.getpid(),
        "owner_start_ticks": agent.proc_start_ticks(os.getpid()),
        "state": "reserved",
        "mode": "new",
    }
    agent.write_meta(aid, meta)
    monkeypatch.setenv("LUBKO_RUNNER_GEN", "1")

    spawned = False

    def must_not_run(*_args: object, **_kwargs: object) -> None:
        nonlocal spawned
        spawned = True

    monkeypatch.setattr(agent, "_runner_loop", must_not_run)

    with pytest.raises(ValueError, match="managed-agent cwd is malformed"):
        agent.runner(aid, "new")

    assert spawned is False
    final = agent.read_meta(aid)
    assert final is not None
    assert final["state"] == "failed"
    assert final["active_runner"] is False
    assert final.get("pid") is None
