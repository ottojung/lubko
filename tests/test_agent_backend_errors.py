"""Regression coverage for managed-agent backend diagnostics."""

from __future__ import annotations

from typing import TYPE_CHECKING

from lubko import agent

if TYPE_CHECKING:
    from pathlib import Path

    import pytest


def test_classifies_backend_server_error_from_current_invocation(tmp_path: Path) -> None:
    """A recognized server error becomes bounded structured diagnostics."""
    log = tmp_path / "output.log"
    log.write_text("old failure\n", encoding="utf-8")
    start = log.stat().st_size
    message = (
        '{"name":"UnknownError","data":{"message":"Unexpected server error. '
        'Check server logs for details.","ref":"err_abc123"}}\n'
    )
    with log.open("a", encoding="utf-8") as fh:
        fh.write(message)

    error = agent._classify_backend_failure(log, start, 1)

    assert error is not None
    assert error["classification"] == "transient_backend_server_error"
    assert error["reference"] == "err_abc123"
    assert error["transient"] is True
    assert error["automatic_retry_safe"] is False
    assert error["diagnostic_bytes"] <= agent.BACKEND_DIAGNOSTIC_MAX_BYTES


def test_unrecognized_failure_is_not_misclassified(tmp_path: Path) -> None:
    """Ordinary task failures do not become backend outages."""
    log = tmp_path / "output.log"
    log.write_text("deterministic task failure\n", encoding="utf-8")
    assert agent._classify_backend_failure(log, 0, 1) is None


def test_success_clears_previous_backend_error() -> None:
    """A later successful invocation clears recoverable backend state."""
    meta: agent.Meta = {
        "state": "running",
        "intent": None,
        "stop_reason": None,
        "backend_error": {"classification": "transient_backend_server_error"},
    }
    agent._finalize_after(0)(meta)
    assert meta["state"] == "succeeded"
    assert meta["backend_error"] is None


def test_retryable_backend_failure_recovers(monkeypatch: pytest.MonkeyPatch) -> None:
    """A positively replay-safe transient failure gets bounded retry."""
    calls = 0
    sleeps: list[float] = []
    meta: agent.Meta = {
        "backend_error": {
            "classification": "fake_transient",
            "transient": True,
            "automatic_retry_safe": True,
        }
    }

    def run_once() -> int:
        nonlocal calls
        calls += 1
        return 1 if calls == 1 else 0

    monkeypatch.setattr(agent, "read_meta", lambda _aid: meta)
    monkeypatch.setattr("lubko.agent.time.sleep", sleeps.append)
    assert agent._run_with_backend_retries("abc123", run_once) == 0
    assert calls == 2
    assert sleeps == [agent.BACKEND_RETRY_BASE_SECONDS]


def test_retryable_backend_failure_exhausts_budget(monkeypatch: pytest.MonkeyPatch) -> None:
    """Repeated replay-safe failures stop after the bounded retry budget."""
    calls = 0
    sleeps: list[float] = []
    meta: agent.Meta = {
        "backend_error": {
            "classification": "fake_transient",
            "transient": True,
            "automatic_retry_safe": True,
        }
    }

    def run_once() -> int:
        nonlocal calls
        calls += 1
        return 1

    monkeypatch.setattr(agent, "read_meta", lambda _aid: meta)
    monkeypatch.setattr("lubko.agent.time.sleep", sleeps.append)
    assert agent._run_with_backend_retries("abc123", run_once) == 1
    assert calls == agent.BACKEND_RETRY_MAX_ATTEMPTS + 1
    assert sleeps == [
        agent.BACKEND_RETRY_BASE_SECONDS,
        agent.BACKEND_RETRY_BASE_SECONDS * 2,
    ]


def test_ambiguous_backend_acceptance_is_never_replayed(monkeypatch: pytest.MonkeyPatch) -> None:
    """Transient classification alone never authorizes duplicate work."""
    calls = 0
    meta: agent.Meta = {
        "backend_error": {
            "classification": "transient_backend_server_error",
            "transient": True,
            "automatic_retry_safe": False,
        }
    }

    def run_once() -> int:
        nonlocal calls
        calls += 1
        return 1

    monkeypatch.setattr(agent, "read_meta", lambda _aid: meta)
    monkeypatch.setattr(
        "lubko.agent.time.sleep", lambda _delay: (_ for _ in ()).throw(AssertionError())
    )
    assert agent._run_with_backend_retries("abc123", run_once) == 1
    assert calls == 1
