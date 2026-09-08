"""Regression coverage for managed-agent backend diagnostics."""

from __future__ import annotations

import argparse
import subprocess
from typing import TYPE_CHECKING

from lubko import agent

if TYPE_CHECKING:
    from pathlib import Path

    import pytest


def _stub_opencode_executable(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> str:
    executable = str(tmp_path / "opencode")

    def which(_name: str, *, path: str | None = None) -> str:
        del path
        return executable

    monkeypatch.setattr("lubko.agent.shutil.which", which)
    return executable


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
    assert error["request_boundary"] == "fresh_session"
    assert error["fresh_session_useful"] is False
    assert error["backend_scope"] == "unknown"
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


def test_continuation_backend_error_keeps_fresh_session_help_unknown(tmp_path: Path) -> None:
    """Continuation failures do not invent evidence that a fresh session helps."""
    log = tmp_path / "output.log"
    log.write_text("Unexpected server error\n", encoding="utf-8")
    error = agent._classify_backend_failure(log, 0, 1, is_continue=True)
    assert error is not None
    assert error["request_boundary"] == "continuation"
    assert error["fresh_session_useful"] is None


def test_retry_interruption_never_starts_another_attempt(monkeypatch: pytest.MonkeyPatch) -> None:
    """An interruption propagates immediately instead of duplicating an invocation."""
    calls = 0

    def run_once() -> int:
        nonlocal calls
        calls += 1
        raise KeyboardInterrupt

    monkeypatch.setattr(
        "lubko.agent.time.sleep",
        lambda _delay: (_ for _ in ()).throw(AssertionError("must not sleep")),
    )
    interrupted = False
    try:
        agent._run_with_backend_retries("abc123", run_once)
    except KeyboardInterrupt:
        interrupted = True
    assert interrupted is True
    assert calls == 1


def test_status_sanitizes_backend_diagnostics() -> None:
    """Status exposes actionable bounded fields without arbitrary persisted data."""
    meta: agent.Meta = {
        "backend_error": {
            "classification": "transient_backend_server_error",
            "provider": "opencode",
            "model": "model",
            "request_boundary": "continuation",
            "reference": "err_abc",
            "transient": True,
            "automatic_retry_safe": False,
            "fresh_session_useful": None,
            "backend_scope": "unknown",
            "diagnostic_bytes": 42,
            "secret": "must-not-escape",
        }
    }
    status = agent._status_json("abc123", meta, "failed", alive=False)
    backend = status["backend_error"]
    assert isinstance(backend, dict)
    assert backend["request_boundary"] == "continuation"
    assert backend["backend_scope"] == "unknown"
    assert "secret" not in backend


def test_configured_model_catalog_presence(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """A successful catalog positively proves the configured model is available."""
    calls: list[tuple[object, ...]] = []

    def run(*args: object, **kwargs: object) -> object:
        calls.append(args)
        assert kwargs["timeout"] == agent.MODEL_CATALOG_TIMEOUT_SECONDS
        return subprocess.CompletedProcess(
            ["opencode", "models"], 0, stdout=f"other/model\n{agent.AGENT_MODEL}\n"
        )

    executable = _stub_opencode_executable(monkeypatch, tmp_path)
    monkeypatch.setattr("lubko.agent.subprocess.run", run)
    assert agent._configured_model_available({"HOME": str(tmp_path)}) is True
    assert calls == [([executable, "models"],)]


def test_configured_model_catalog_absence_is_authoritative(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A successful catalog proves an unlisted configured model unavailable."""
    executable = _stub_opencode_executable(monkeypatch, tmp_path)
    monkeypatch.setattr(
        "lubko.agent.subprocess.run",
        lambda *_args, **_kwargs: subprocess.CompletedProcess(
            [executable, "models"], 0, stdout="opencode/other-model\n"
        ),
    )
    assert agent._configured_model_available({}) is False


def test_configured_model_catalog_failure_is_inconclusive(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Catalog transport failure never hides the ordinary backend failure path."""
    executable = _stub_opencode_executable(monkeypatch, tmp_path)
    monkeypatch.setattr(
        "lubko.agent.subprocess.run",
        lambda *_args, **_kwargs: subprocess.CompletedProcess([executable, "models"], 1, stdout=""),
    )
    assert agent._configured_model_available({}) is None


def test_unavailable_configured_model_rejects_without_mutating_agent(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Known model absence rejects a prompt before durable invocation authority changes."""
    meta = agent.idle_meta("abc123", str(tmp_path), "model unavailable")
    before = dict(meta)
    monkeypatch.setattr(agent, "read_meta", lambda _aid: meta)
    monkeypatch.setattr(agent, "_configured_model_available", lambda _env: False)

    def must_not_dispatch(*_args: object, **_kwargs: object) -> int:
        raise AssertionError

    monkeypatch.setattr(agent, "_dispatch_invocation", must_not_dispatch)
    args = argparse.Namespace(
        id="abc123",
        prompt_text=None,
        prompt="do work",
        steer=False,
    )

    assert agent.cmd_prompt(args) == agent.EXIT_ERROR
    assert meta == before
