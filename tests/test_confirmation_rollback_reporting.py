"""Confirmation must not report rollback success unless rollback terminalized."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest

import lubko.cli
import lubko.deployctl as dc
import lubko.supervise
from lubko import lifecycle_state

cli = lubko.cli
supervise = lubko.supervise

COMMIT = "2" * 40
OTHER_COMMIT = "3" * 40


def _pending_state() -> dc.RollbackState:
    """Return the minimal pending mission shape used by confirmation helpers."""
    return cast(
        "dc.RollbackState",
        SimpleNamespace(
            status=dc.STATUS_PENDING, commit=COMMIT, repo="/workspace/Lubko", uv_path="uv"
        ),
    )


def test_expired_confirmation_reports_successful_rollback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An expired confirmation may say rolled back after rollback succeeds."""
    state = _pending_state()
    monkeypatch.setattr(dc, "_read_state", lambda: state)
    monkeypatch.setattr(dc, "_pending_mission_rollback_due", lambda _state: True)
    monkeypatch.setattr(dc, "_rollback_locked", lambda _state: True)

    with pytest.raises(dc.DeployCtlError, match="rolled back"):
        dc._confirmation_state({"type": "confirm", "commit": COMMIT})


def test_expired_confirmation_reports_pending_when_rollback_does_not_terminalize(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An expired confirmation must expose an unresolved rollback truthfully."""
    state = _pending_state()
    monkeypatch.setattr(dc, "_read_state", lambda: state)
    monkeypatch.setattr(dc, "_pending_mission_rollback_due", lambda _state: True)
    monkeypatch.setattr(dc, "_rollback_locked", lambda _state: False)

    with pytest.raises(dc.DeployCtlError, match="remains pending"):
        dc._confirmation_state({"type": "confirm", "commit": COMMIT})

    assert state.status == dc.STATUS_PENDING


def test_wrong_confirmation_commit_reports_pending_when_rollback_does_not_terminalize(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A wrong commit must not falsely claim terminal rollback."""
    state = _pending_state()
    monkeypatch.setattr(dc, "_read_state", lambda: state)
    monkeypatch.setattr(dc, "_pending_mission_rollback_due", lambda _state: False)
    monkeypatch.setattr(dc, "_rollback_locked", lambda _state: False)

    with pytest.raises(dc.DeployCtlError, match="remains pending"):
        dc._confirmation_state({"type": "confirm", "commit": OTHER_COMMIT})

    assert state.status == dc.STATUS_PENDING


def test_candidate_failure_reports_pending_when_rollback_does_not_terminalize(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Candidate failure must report unresolved rollback rather than success."""
    state = _pending_state()
    monkeypatch.setattr(dc, "_pending_mission_rollback_due", lambda _state: True)
    monkeypatch.setattr(dc, "_rollback_locked", lambda _state: False)

    with pytest.raises(dc.DeployCtlError, match="remains pending"):
        dc._authorize_confirmation(state)

    assert state.status == dc.STATUS_PENDING


def test_authority_failure_reports_pending_when_rollback_does_not_terminalize(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Authority refusal must preserve truthful nonterminal reporting."""
    state = _pending_state()
    monkeypatch.setattr(dc, "_pending_mission_rollback_due", lambda _state: False)
    monkeypatch.setattr(dc, "_mission_authority_facts", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(lifecycle_state, "authorize_mission_confirm", lambda _facts: False)
    monkeypatch.setattr(dc, "_rollback_locked", lambda _state: False)

    with pytest.raises(dc.DeployCtlError, match="remains pending"):
        dc._authorize_confirmation(state)

    assert state.status == dc.STATUS_PENDING


def test_cli_preparation_failure_reports_pending_when_rollback_does_not_terminalize(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CLI preparation failure must not falsely claim terminal rollback."""
    state = _pending_state()
    monkeypatch.setattr(supervise, "supervisor_running", lambda: False)
    monkeypatch.setattr(
        cli,
        "build_cli_root",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(cli.CliError("build failed")),
    )
    monkeypatch.setattr(dc, "_rollback_locked", lambda _state: False)

    with pytest.raises(dc.DeployCtlError, match="remains pending"):
        dc._prepare_confirmation_candidate(
            state,
            dc.Options(
                repo=Path("/workspace/Lubko"),
                uv_path="uv",
                confirm_window_seconds=1.0,
                stop_grace_seconds=1.0,
                postgres_timeout_seconds=1.0,
                lock_timeout_seconds=1.0,
                validation_timeout_seconds=1.0,
                git_timeout_seconds=1.0,
                cli_timeout_seconds=1.0,
            ),
        )

    assert state.status == dc.STATUS_PENDING
