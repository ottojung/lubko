"""Retention clean lifecycle safety.

Retention ``clean`` must never raw-delete state for an agent that became
promptable/live after candidate enumeration: it must go through the same
tombstone/convergence/removal machinery as ``delete``, skip live candidates,
and keep dry-run strictly observational.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from typing import TYPE_CHECKING

import pytest

from lubko import agent

if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture(autouse=True)
def isolated_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point the agent state root at a throwaway directory.

    Returns:
        The isolated Lubko state root.
    """
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state"))
    monkeypatch.delenv("LUBKO_AGENT_RETENTION_DAYS", raising=False)
    return tmp_path / "state" / "lubko"


def write_meta(aid: str, **fields: object) -> None:
    """Persist an agent metadata document."""
    directory = agent.agent_dir(aid)
    directory.mkdir(parents=True, exist_ok=True)
    payload: dict[str, object] = {
        "id": aid,
        "state": "succeeded",
        "finished_at": time.time() - 86400 * 30,
    }
    payload.update(fields)
    (directory / "meta.json").write_text(json.dumps(payload), encoding="utf-8")


def clean_args(*, dry_run: bool) -> argparse.Namespace:
    """Build a parsed ``clean`` command namespace.

    Args:
        dry_run: Whether observation-only mode is requested.

    Returns:
        The parsed namespace.
    """
    return argparse.Namespace(days=14, dry_run=dry_run)


def test_dry_run_is_observation_only(capsys: pytest.CaptureFixture[str]) -> None:
    """Dry-run reports candidates but never mutates any state."""
    write_meta("aa")
    assert agent.cmd_clean(clean_args(dry_run=True)) == agent.EXIT_OK
    assert (agent.agent_dir("aa") / "meta.json").is_file()
    meta = agent.read_meta("aa")
    assert meta is not None
    assert not meta.get("delete_pending")
    assert "would remove agent aa" in capsys.readouterr().out


def test_clean_removes_terminal_agents(capsys: pytest.CaptureFixture[str]) -> None:
    """Terminal agents past the cutoff are removed through the safe path."""
    write_meta("aa")
    write_meta("bb")
    assert agent.cmd_clean(clean_args(dry_run=False)) == agent.EXIT_OK
    out = capsys.readouterr().out
    assert not agent.agent_dir("aa").exists()
    assert not agent.agent_dir("bb").exists()
    assert "(2 agent(s))" in out


def test_candidate_becomes_live_after_enumeration_is_skipped(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A candidate that turns promptable/live after enumeration keeps its state."""
    write_meta("aa")

    original = agent._clean_candidates

    def racing(days: int) -> list[str]:
        ids = original(days)
        # The candidate becomes promptable/live after enumeration but before
        # removal, exactly as a concurrent prompt/runner claim would.
        current = agent.read_meta("aa")
        assert current is not None
        agent.write_meta("aa", {**current, "state": "running", "started_at": time.time()})
        return ids

    monkeypatch.setattr(agent, "_clean_candidates", racing)
    assert agent.cmd_clean(clean_args(dry_run=False)) == agent.EXIT_OK
    out = capsys.readouterr().out
    assert agent.agent_dir("aa").is_dir()
    meta = agent.read_meta("aa")
    assert meta is not None
    assert not meta.get("delete_pending")
    assert "removed agent aa" not in out
    assert "skipped agent aa" in out


def test_reserved_runner_blocks_removal_at_prestart_boundary(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A reserved-but-unclaimed runner blocks retention removal entirely."""
    aid = "aa"
    write_meta(
        aid,
        state="succeeded",
        active_runner=True,
        runner_gen=1,
        runner_reservation={
            "state": "reserved",
            "gen": 1,
            "owner_pid": os.getpid(),
            "mode": "new",
            "owner_start_ticks": agent.proc_start_ticks(os.getpid()),
        },
    )
    reserved = agent.read_meta(aid)
    assert reserved is not None
    assert agent.reservation_in_flight(reserved)
    assert agent.cmd_clean(clean_args(dry_run=False)) == agent.EXIT_OK
    out = capsys.readouterr().out
    assert agent.agent_dir(aid).is_dir()
    meta = agent.read_meta(aid)
    assert meta is not None
    assert not meta.get("delete_pending")
    assert "removed agent aa" not in out
