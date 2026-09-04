"""Confirmation terminalization must preserve durable supervisor authority."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest

import lubko.cli as cli
import lubko.deployctl as dc
import lubko.supervise as supervise

COMMIT = "2" * 40
PREVIOUS_COMMIT = "1" * 40


def _pending_state(supervisor_owned: bool | None) -> dc.RollbackState:
    """Return a minimal pending mission for confirmation finalization tests."""
    return cast(
        "dc.RollbackState",
        SimpleNamespace(
            status=dc.STATUS_PENDING,
            commit=COMMIT,
            previous_commit=PREVIOUS_COMMIT,
            repo="/workspace/Lubko",
            uv_path="uv",
            supervisor_owned=supervisor_owned,
        ),
    )


def _options() -> dc.Options:
    """Return deterministic confirmation options for direct helper tests."""
    return dc.Options(
        repo=Path("/workspace/Lubko"),
        uv_path="uv",
        confirm_window_seconds=1.0,
        stop_grace_seconds=1.0,
        postgres_timeout_seconds=1.0,
        lock_timeout_seconds=1.0,
        validation_timeout_seconds=1.0,
        git_timeout_seconds=1.0,
        cli_timeout_seconds=1.0,
    )


@pytest.mark.parametrize("supervisor_owned", [True, None])
def test_supervised_confirmation_requires_live_supervisor_at_terminalization(
    monkeypatch: pytest.MonkeyPatch,
    supervisor_owned: bool | None,
) -> None:
    """Supervised or unknown ownership cannot degrade to legacy confirmation."""
    state = _pending_state(supervisor_owned)
    writes: list[object] = []
    pointer_updates: list[str] = []
    monkeypatch.setattr(supervise, "supervisor_running", lambda: False)
    monkeypatch.setattr(dc, "_write_state", writes.append)
    monkeypatch.setattr(cli, "set_current", pointer_updates.append)

    with pytest.raises(dc.DeployCtlError, match="live supervisor"):
        dc._finalize_confirmation(state)

    assert state.status == dc.STATUS_PENDING
    assert writes == []
    assert pointer_updates == []


def test_explicit_legacy_confirmation_can_terminalize_without_supervisor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Explicit legacy ownership retains direct no-supervisor confirmation."""
    state = _pending_state(False)
    writes: list[dc.RollbackState] = []
    pointer_updates: list[str] = []
    monkeypatch.setattr(supervise, "supervisor_running", lambda: False)
    monkeypatch.setattr(dc, "_write_state", writes.append)
    monkeypatch.setattr(cli, "set_current", pointer_updates.append)
    monkeypatch.setattr(cli, "gc_cli_roots", lambda _commits: None)
    monkeypatch.setattr(dc, "append_deploy_log", lambda _message: None)

    terminal = dc._finalize_confirmation(state)

    assert terminal.status == dc.STATUS_CONFIRMED
    assert writes == [terminal]
    assert pointer_updates == [COMMIT]


def test_confirmation_fails_closed_if_supervisor_disappears_after_preparation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A live preparation observation cannot authorize later legacy fallback."""
    state = _pending_state(True)
    liveness = iter([True, False])
    settled: list[tuple[str, str, str]] = []
    writes: list[object] = []
    pointer_updates: list[str] = []

    monkeypatch.setattr(dc, "_confirmation_state", lambda _request: state)
    monkeypatch.setattr(dc, "_authorize_confirmation", lambda _state: None)
    monkeypatch.setattr(supervise, "supervisor_running", lambda: next(liveness))
    monkeypatch.setattr(
        dc,
        "settle_desired",
        lambda commit, repo, uv_path: settled.append((commit, repo, uv_path)),
    )
    monkeypatch.setattr(dc, "_write_state", writes.append)
    monkeypatch.setattr(cli, "set_current", pointer_updates.append)

    with pytest.raises(dc.DeployCtlError, match="live supervisor"):
        dc._confirm_locked({"type": "confirm", "commit": COMMIT}, _options())

    assert settled == [(COMMIT, state.repo, state.uv_path)]
    assert state.status == dc.STATUS_PENDING
    assert writes == []
    assert pointer_updates == []
