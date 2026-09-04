"""Confirmation terminalization must preserve durable supervisor authority."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

import lubko.deployctl as dc
from lubko import cli, lifecycle, supervise

COMMIT = "2" * 40
PREVIOUS_COMMIT = "1" * 40


def _meta(commit: str, pid: int) -> lifecycle.WorkerMeta:
    """Return a minimal valid worker authority record."""
    return lifecycle.WorkerMeta(
        schema_version=lifecycle.SCHEMA_VERSION,
        state=lifecycle.STATE_RUNNING,
        pid=pid,
        pgid=pid,
        sid=pid,
        start_time_ticks=pid,
        token=f"token-{pid}",
        repo="/workspace/Lubko",
        git_commit=commit,
        worker_id="w",
        log_path="",
        started_at=1.0,
        stopped_at=None,
    )


def _pending_state(*, supervisor_owned: bool | None) -> dc.RollbackState:
    """Return a valid pending mission for confirmation finalization tests."""
    return dc.RollbackState(
        schema_version=dc.ROLLBACK_SCHEMA_VERSION,
        generation=1,
        status=dc.STATUS_PENDING,
        commit=COMMIT,
        previous_commit=PREVIOUS_COMMIT,
        deadline=10.0,
        repo="/workspace/Lubko",
        uv_path="uv",
        stop_grace_seconds=1.0,
        git_timeout_seconds=1.0,
        previous_retiring=False,
        previous_meta=_meta(PREVIOUS_COMMIT, 100),
        new_meta=_meta(COMMIT, 200),
        supervisor_owned=supervisor_owned,
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


def _assert_supervisor_authority_requires_liveness(
    monkeypatch: pytest.MonkeyPatch,
    *,
    supervisor_owned: bool | None,
) -> None:
    """Assert durable supervisor authority cannot degrade to legacy confirmation."""
    state = _pending_state(supervisor_owned=supervisor_owned)
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


def test_supervisor_owned_confirmation_requires_live_supervisor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Supervisor-owned confirmation cannot degrade to legacy confirmation."""
    _assert_supervisor_authority_requires_liveness(monkeypatch, supervisor_owned=True)


def test_unknown_confirmation_ownership_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unknown confirmation ownership is never inferred from liveness."""
    state = _pending_state(supervisor_owned=None)
    monkeypatch.setattr(supervise, "supervisor_running", lambda: False)

    with pytest.raises(dc.DeployCtlError, match="unknown supervisor ownership"):
        dc._finalize_confirmation(state)


def test_explicit_legacy_confirmation_can_terminalize_without_supervisor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Explicit legacy ownership retains direct no-supervisor confirmation."""
    state = _pending_state(supervisor_owned=False)
    writes: list[dc.RollbackState] = []
    pointer_updates: list[str] = []
    closed: list[int] = []
    monkeypatch.setattr(supervise, "acquire_supervisor_lock", lambda: 41)
    monkeypatch.setattr(os, "close", closed.append)
    monkeypatch.setattr(dc, "_write_state", writes.append)
    monkeypatch.setattr(cli, "set_current", pointer_updates.append)
    monkeypatch.setattr(cli, "gc_cli_roots", lambda _commits: None)
    monkeypatch.setattr(dc, "append_deploy_log", lambda _message: None)

    terminal = dc._finalize_confirmation(state)

    assert terminal.status == dc.STATUS_CONFIRMED
    assert writes == [terminal]
    assert pointer_updates == [COMMIT]
    assert closed == [41]


def test_confirmation_fails_closed_if_supervisor_disappears_after_preparation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A live preparation observation cannot authorize later legacy fallback."""
    state = _pending_state(supervisor_owned=True)
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


def test_legacy_confirmation_rolls_back_if_supervisor_appears_before_preparation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A live supervisor cannot adopt a legacy candidate during confirmation."""
    state = _pending_state(supervisor_owned=False)
    settled: list[tuple[str, str, str]] = []
    rolled_back: list[dc.RollbackState] = []
    monkeypatch.setattr(supervise, "supervisor_running", lambda: True)
    monkeypatch.setattr(
        dc,
        "settle_desired",
        lambda commit, repo, uv_path: settled.append((commit, repo, uv_path)),
    )

    def rollback(s: dc.RollbackState) -> bool:
        rolled_back.append(s)
        return True

    monkeypatch.setattr(dc, "_rollback_locked", rollback)

    with pytest.raises(dc.DeployCtlError, match="deployment was rolled back"):
        dc._prepare_confirmation_candidate(state, _options())

    assert settled == []
    assert rolled_back == [state]


def test_legacy_confirmation_rolls_back_if_supervisor_owns_terminalization_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A late supervisor wins atomically over prepared legacy confirmation."""
    state = _pending_state(supervisor_owned=False)
    rolled_back: list[dc.RollbackState] = []
    writes: list[object] = []
    pointer_updates: list[str] = []

    def lock_contended() -> int:
        raise BlockingIOError

    def rollback(s: dc.RollbackState) -> bool:
        rolled_back.append(s)
        return True

    monkeypatch.setattr(supervise, "acquire_supervisor_lock", lock_contended)
    monkeypatch.setattr(supervise, "supervisor_running", lambda: True)
    monkeypatch.setattr(dc, "_rollback_locked", rollback)
    monkeypatch.setattr(dc, "_write_state", writes.append)
    monkeypatch.setattr(cli, "set_current", pointer_updates.append)

    with pytest.raises(dc.DeployCtlError, match="deployment was rolled back"):
        dc._finalize_confirmation(state)

    assert rolled_back == [state]
    assert writes == []
    assert pointer_updates == []


def test_legacy_confirmation_lock_failure_without_live_supervisor_stays_pending(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An ambiguous ownership-lock failure never authorizes direct confirmation."""
    state = _pending_state(supervisor_owned=False)
    rolled_back: list[dc.RollbackState] = []

    def lock_failed() -> int:
        message = "lock unavailable"
        raise OSError(message)

    def rollback(s: dc.RollbackState) -> bool:
        rolled_back.append(s)
        return True

    monkeypatch.setattr(supervise, "acquire_supervisor_lock", lock_failed)
    monkeypatch.setattr(supervise, "supervisor_running", lambda: False)
    monkeypatch.setattr(dc, "_rollback_locked", rollback)

    with pytest.raises(dc.DeployCtlError, match="absence cannot be proven"):
        dc._finalize_confirmation(state)

    assert rolled_back == []


def test_unknown_confirmation_ownership_is_not_inferred_from_live_supervisor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unknown ownership fails closed before any supervisor settlement is published."""
    state = _pending_state(supervisor_owned=None)
    settled: list[tuple[str, str, str]] = []
    monkeypatch.setattr(supervise, "supervisor_running", lambda: True)
    monkeypatch.setattr(
        dc,
        "settle_desired",
        lambda commit, repo, uv_path: settled.append((commit, repo, uv_path)),
    )

    with pytest.raises(dc.DeployCtlError, match="unknown supervisor ownership"):
        dc._prepare_confirmation_candidate(state, _options())

    assert settled == []
