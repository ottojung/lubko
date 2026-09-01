"""Regression coverage for strict managed-agent list summary metadata."""

from __future__ import annotations

from argparse import Namespace

import pytest

from lubko import agent


def _args(**overrides: object) -> Namespace:
    values: dict[str, object] = {
        "running": False,
        "finished": False,
        "succeeded": False,
        "failed": False,
        "stopped": False,
        "killed": False,
        "limit": 0,
    }
    values.update(overrides)
    return Namespace(**values)


def test_list_valid_summary_and_ordering_are_preserved(monkeypatch: pytest.MonkeyPatch) -> None:
    """Canonical metadata retains list ordering and JSON output."""
    metas = {
        "older": {"created_at": 10, "prompt_count": 0, "cwd": "/old", "title": "old"},
        "newer": {"created_at": 20.5, "prompt_count": 2, "cwd": "/new", "title": "new"},
    }
    monkeypatch.setattr(agent, "_agent_ids", lambda: ["older", "newer"])
    monkeypatch.setattr(agent, "read_meta", lambda aid: metas[aid])
    monkeypatch.setattr(agent, "derive_state", lambda _meta: "idle")

    entries = agent._list_entries(_args())
    assert [entry[0] for entry in entries] == ["newer", "older"]
    assert agent._entry_json(*entries[0]) == {
        "id": "newer",
        "state": "idle",
        "prompts": 2,
        "cwd": "/new",
        "title": "new",
        "created_at": 20.5,
        "last_activity_at": None,
        "finished_at": None,
    }


def test_list_malformed_created_at_cannot_break_mixed_sorting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Malformed creation times fail closed during mixed-entry sorting."""
    metas = {"bad": {"created_at": {}}, "good": {"created_at": 12.0}}
    monkeypatch.setattr(agent, "_agent_ids", lambda: ["bad", "good"])
    monkeypatch.setattr(agent, "read_meta", lambda aid: metas[aid])
    monkeypatch.setattr(agent, "derive_state", lambda _meta: "idle")

    entries = agent._list_entries(_args())
    assert [entry[0] for entry in entries] == ["good", "bad"]
    bad = agent._entry_json(*entries[1])
    assert bad["created_at"] is None
    assert bad["metadata_errors"] == ["created_at"]


@pytest.mark.parametrize(("field", "value"), [("cwd", {}), ("title", 7)])
def test_list_malformed_text_fields_are_diagnostic_not_crashes(
    field: str, value: object, capsys: pytest.CaptureFixture[str]
) -> None:
    """Malformed text fields remain visible without crashing."""
    meta: agent.Meta = {"created_at": 1.0, "prompt_count": 0, "cwd": "/ok", "title": "ok"}
    meta[field] = value
    agent._print_agent_table([("abc", "idle", meta)])
    output = capsys.readouterr().out
    assert "abc" in output
    assert "<invalid>" in output
    entry = agent._entry_json("abc", "idle", meta)
    assert entry[field] is None
    assert entry["metadata_errors"] == [field]


def test_falsey_malformed_prompt_count_is_not_canonical_zero(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Falsey corrupt prompt counts stay distinct from canonical zero."""
    meta: agent.Meta = {"prompt_count": False}
    entry = agent._entry_json("bad", "idle", meta)
    assert entry["prompts"] is None
    assert entry["metadata_errors"] == ["prompt_count"]

    agent._print_agent_table([("bad", "idle", meta)])
    assert "<invalid>" in capsys.readouterr().out


def test_one_malformed_entry_does_not_hide_healthy_entry(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A corrupt entry cannot hide an unrelated healthy agent."""
    healthy: agent.Meta = {"created_at": 2.0, "prompt_count": 1, "cwd": "/ok", "title": "healthy"}
    corrupt: agent.Meta = {"created_at": "bad", "prompt_count": [], "cwd": 1, "title": {}}
    agent._print_agent_table([("good", "idle", healthy), ("bad", "idle", corrupt)])
    output = capsys.readouterr().out
    assert "good" in output
    assert "healthy" in output
    assert "bad" in output
    assert "<invalid>" in output

    bad_json = agent._entry_json("bad", "idle", corrupt)
    assert bad_json["metadata_errors"] == ["created_at", "prompt_count", "cwd", "title"]
    assert bad_json["created_at"] is None
    assert bad_json["prompts"] is None
    assert bad_json["cwd"] is None
    assert bad_json["title"] is None
