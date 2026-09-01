"""Regression coverage for strict summary metadata status rendering."""

from __future__ import annotations

import argparse

import pytest

from lubko import agent


def _status(meta: agent.Meta) -> agent.Meta:
    return agent._status_json("abc123", meta, "idle", alive=False)


def test_status_json_preserves_canonical_summary_metadata() -> None:
    """Canonical summary metadata remains unchanged in JSON status."""
    meta: agent.Meta = {
        "created_at": 1.0,
        "started_at": 2,
        "finished_at": 3.5,
        "prompt_count": 0,
        "cwd": "/ok",
        "title": "ok",
    }
    status = _status(meta)
    assert status["created_at"] == pytest.approx(1.0)
    assert status["started_at"] == 2
    assert status["finished_at"] == pytest.approx(3.5)
    assert status["prompts"] == 0
    assert status["cwd"] == "/ok"
    assert status["title"] == "ok"
    assert "metadata_errors" not in status


@pytest.mark.parametrize(
    ("field", "value", "status_field"),
    [
        ("created_at", [], "created_at"),
        ("started_at", "2", "started_at"),
        ("started_at", float("nan"), "started_at"),
        ("finished_at", False, "finished_at"),
        ("finished_at", float("inf"), "finished_at"),
        ("prompt_count", False, "prompts"),
        ("cwd", 0, "cwd"),
        ("title", {}, "title"),
    ],
)
def test_status_json_sanitizes_malformed_summary_metadata(
    field: str, value: object, status_field: str
) -> None:
    """Malformed summary values are sanitized and reported."""
    meta: agent.Meta = {
        "created_at": 1.0,
        "started_at": 2.0,
        "finished_at": 3.0,
        "prompt_count": 1,
        "cwd": "/ok",
        "title": "ok",
    }
    meta[field] = value
    status = _status(meta)
    assert status[status_field] is None
    assert status["metadata_errors"] == [field]


def test_status_text_diagnoses_malformed_summary_without_crashing(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Text status diagnoses malformed summary values without crashing."""
    meta: agent.Meta = {
        "created_at": [],
        "started_at": "bad",
        "finished_at": False,
        "prompt_count": False,
        "cwd": 0,
        "title": {},
    }
    monkeypatch.setattr(agent, "reconcile_meta", lambda _aid: None)
    monkeypatch.setattr(agent, "read_meta", lambda _aid: meta)
    monkeypatch.setattr(agent, "derive_state", lambda _meta: "idle")
    monkeypatch.setattr(agent, "is_alive", lambda _meta: False)
    monkeypatch.setattr(agent, "log_excerpt", lambda _path, _count: [])

    args = argparse.Namespace(id="abc123", agent_id=None, json=False)
    assert agent.cmd_status(args) == agent.EXIT_OK
    output = capsys.readouterr().out
    assert output.count("<invalid>") == 6
    assert "malformed persisted summary metadata" in output
    for field in (
        "created_at",
        "prompt_count",
        "cwd",
        "title",
        "started_at",
        "finished_at",
    ):
        assert field in output
