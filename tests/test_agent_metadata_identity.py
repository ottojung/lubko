"""Managed-agent metadata identity binding invariants."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pathlib import Path

from lubko import agent


def _write_meta(base: Path, aid: str, persisted_id: object = "aaaa") -> None:
    directory = base / aid
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "meta.json").write_text(
        json.dumps({
            "id": persisted_id,
            "cwd": "/workspace/exact-agent-tree",
            "variant": "low",
            "native_session_id": None,
        })
    )


def test_matching_persisted_identity_remains_session_authority(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A canonical ID bound to its addressed directory keeps normal behavior."""
    aid = "aaaa"
    _write_meta(tmp_path, aid)
    monkeypatch.setattr(agent, "agent_dir", lambda value: tmp_path / value)
    calls: list[str] = []

    def discover(value: str) -> str:
        calls.append(value)
        return "sess-a"

    monkeypatch.setattr(agent, "discover_session_id", discover)

    meta = agent.read_meta(aid)
    assert meta is not None
    continued = agent.build_agent_command(meta, "work", is_continue=True)
    assert continued is not None
    assert continued[continued.index("--session") + 1] == "sess-a"
    assert calls == [aid]

    fresh = agent.build_agent_command(meta, "work", is_continue=False)
    assert fresh is not None
    assert fresh[fresh.index("--title") + 1] == "lubko-aaaa"


def test_read_meta_rejects_unbound_or_malformed_persisted_identity(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Durable identity cannot remap or malformedly identify an agent record."""
    aid = "aaaa"
    monkeypatch.setattr(agent, "agent_dir", lambda value: tmp_path / value)

    for persisted_id in ("bbbb", "AAAA", "", 123, True, None):
        _write_meta(tmp_path, aid, persisted_id)
        assert agent.read_meta(aid) is None


def test_read_meta_requires_present_persisted_identity(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Identity absence is corruption, not permission to infer from the path."""
    aid = "aaaa"
    directory = tmp_path / aid
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "meta.json").write_text(json.dumps({"cwd": "/workspace/exact-agent-tree"}))
    monkeypatch.setattr(agent, "agent_dir", lambda value: tmp_path / value)

    assert agent.read_meta(aid) is None


def test_command_construction_rejects_malformed_unbound_identity() -> None:
    """Command argv never normalizes malformed durable ID into authority."""
    for persisted_id in (None, 123, True, "", "AAAA", "not-hex"):
        meta: agent.Meta = {
            "id": persisted_id,
            "cwd": "/workspace/exact-agent-tree",
            "variant": "low",
            "native_session_id": None,
        }

        with pytest.raises(ValueError, match="managed-agent id is malformed"):
            agent.build_agent_command(meta, "work", is_continue=True)
        with pytest.raises(ValueError, match="managed-agent id is malformed"):
            agent.build_agent_command(meta, "work", is_continue=False)


def test_noncanonical_address_cannot_select_metadata(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The caller/path side of the binding must itself be canonical."""
    _write_meta(tmp_path, "AAAA", "AAAA")
    monkeypatch.setattr(agent, "agent_dir", lambda value: tmp_path / value)

    assert agent.read_meta("AAAA") is None
