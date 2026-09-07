"""Pending checkout cleanup must share supervised rollback policy without changing legacy gates."""

from __future__ import annotations

import time
from types import SimpleNamespace
from typing import cast
from unittest.mock import MagicMock

import pytest

import lubko.deployctl as dc


def _pending_state(*, supervisor_owned: bool) -> dc.RollbackState:
    """Return the minimal pending mission shape needed by cleanup policy."""
    return cast(
        "dc.RollbackState",
        SimpleNamespace(
            status=dc.STATUS_PENDING,
            supervisor_owned=supervisor_owned,
            deadline=time.time() + 60.0,
        ),
    )


def test_supervised_cleanup_preserves_canonically_pending_mission(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A supervised mission that the shared policy keeps pending blocks new checkout."""
    state = _pending_state(supervisor_owned=True)
    due = MagicMock(return_value=False)
    rollback = MagicMock(return_value=True)
    candidate_alive = MagicMock(return_value=False)
    monkeypatch.setattr(dc, "_read_state", lambda: state)
    monkeypatch.setattr(dc, "_pending_mission_rollback_due", due)
    monkeypatch.setattr(dc, "_rollback_locked", rollback)
    monkeypatch.setattr(dc, "_mission_candidate_alive", candidate_alive)

    with pytest.raises(dc.DeployCtlError, match="still pending confirmation"):
        dc._cleanup_pending_locked()

    due.assert_called_once_with(state)
    rollback.assert_not_called()
    candidate_alive.assert_not_called()


def test_supervised_cleanup_rolls_back_only_when_shared_policy_is_due(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A supervised mission may be cleaned up once the canonical policy says rollback is due."""
    state = _pending_state(supervisor_owned=True)
    due = MagicMock(return_value=True)
    rollback = MagicMock(return_value=True)
    monkeypatch.setattr(dc, "_read_state", lambda: state)
    monkeypatch.setattr(dc, "_pending_mission_rollback_due", due)
    monkeypatch.setattr(dc, "_rollback_locked", rollback)

    dc._cleanup_pending_locked()

    due.assert_called_once_with(state)
    rollback.assert_called_once_with(state)


def test_legacy_cleanup_keeps_recorded_candidate_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Legacy-owned cleanup still blocks while its recorded candidate is alive before deadline."""
    state = _pending_state(supervisor_owned=False)
    due = MagicMock(return_value=True)
    rollback = MagicMock(return_value=True)
    candidate_alive = MagicMock(return_value=True)
    monkeypatch.setattr(dc, "_read_state", lambda: state)
    monkeypatch.setattr(dc, "_pending_mission_rollback_due", due)
    monkeypatch.setattr(dc, "_rollback_locked", rollback)
    monkeypatch.setattr(dc, "_mission_candidate_alive", candidate_alive)

    with pytest.raises(dc.DeployCtlError, match="still pending confirmation"):
        dc._cleanup_pending_locked()

    due.assert_not_called()
    candidate_alive.assert_called_once_with(state)
    rollback.assert_not_called()
