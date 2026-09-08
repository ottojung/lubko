"""Regression coverage for managed-agent backend diagnostics."""

from __future__ import annotations

from typing import TYPE_CHECKING

from lubko import agent

if TYPE_CHECKING:
    from pathlib import Path


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
