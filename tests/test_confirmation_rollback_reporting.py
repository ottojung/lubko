"""Confirmation must not report rollback success unless rollback terminalized."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

import lubko.deployctl as dc
import lubko.lifecycle_state as lifecycle_state

COMMIT = "2" * 40
OTHER_COMMIT = "3" * 40


def _pending_state() -> SimpleNamespace:
    """Return the minimal pending mission shape used by confirmation helpers."""
    return SimpleNamespace(status=dc.STATUS_PENDING, commit=COMMIT)


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
        dc._authorize_confirmation(state)  # type: ignore[arg-type]

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
        dc._authorize_confirmation(state)  # type: ignore[arg-type]

    assert state.status == dc.STATUS_PENDING
