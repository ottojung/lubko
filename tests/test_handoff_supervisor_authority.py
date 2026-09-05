"""Handoff authority must come from durable mission ownership."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from lubko import deployctl as dc
from lubko import lifecycle, supervise

COMMIT = "a" * 40
PREVIOUS_COMMIT = "b" * 40


def _meta(commit: str, pid: int) -> lifecycle.WorkerMeta:
    """Return a deterministic valid worker identity."""
    return lifecycle.WorkerMeta(
        schema_version=lifecycle.SCHEMA_VERSION,
        state=lifecycle.STATE_RUNNING,
        pid=pid,
        pgid=pid,
        sid=pid,
        start_time_ticks=pid * 10,
        token=f"{pid:032x}",
        repo="/workspace/Lubko",
        git_commit=commit,
        worker_id="worker",
        log_path="/tmp/lubko.log",
        started_at=1.0,
        stopped_at=None,
    )


def _state(*, supervisor_owned: bool | None) -> dc.RollbackState:
    """Return one pending mission with explicit durable ownership."""
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
    """Return deterministic handoff options."""
    return dc.Options(
        repo=Path("/workspace/Lubko"),
        uv_path="uv",
        confirm_window_seconds=3.0,
        stop_grace_seconds=1.0,
        postgres_timeout_seconds=1.0,
        lock_timeout_seconds=2.0,
        validation_timeout_seconds=1.0,
        git_timeout_seconds=1.0,
        cli_timeout_seconds=1.0,
    )


def _gated(state: dc.RollbackState) -> dc.GatedWorker:
    """Return a harmless gated-candidate test record."""
    assert state.new_meta is not None
    return dc.GatedWorker(proc=MagicMock(), gate_writer=9, meta=state.new_meta)


def test_supervisor_owned_handoff_remains_supervised_when_daemon_is_temporarily_absent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Prepared supervisor authority publishes durably without reclassifying."""
    state = _state(supervisor_owned=True)
    published: list[tuple[dc.RollbackState, float]] = []
    waited: list[tuple[dc.RollbackState, float]] = []
    live = replace(state, deadline=20.0)
    monkeypatch.setattr(
        supervise,
        "supervisor_running",
        lambda: pytest.fail("supervised handoff must not reclassify from liveness"),
    )
    monkeypatch.setattr(dc, "publish_mission", lambda s, t: published.append((s, t)))

    def wait(s: dc.RollbackState, window: float) -> dc.RollbackState:
        waited.append((s, window))
        return live

    monkeypatch.setattr(dc, "_wait_for_supervised_candidate", wait)

    result = dc._complete_handoff(state, _gated(state), _options())

    assert result == live
    assert published == [(state, _options().lock_timeout_seconds)]
    assert waited == [(state, _options().confirm_window_seconds)]


def test_legacy_handoff_releases_gate_only_for_explicit_legacy_ownership(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Only explicit legacy ownership may operate the deployctl-owned gate."""
    state = _state(supervisor_owned=False)
    gated = _gated(state)
    released: list[dc.GatedWorker] = []
    monkeypatch.setattr(dc, "_release_gate", released.append)
    monkeypatch.setattr(dc, "_write_state", lambda _state: None)
    monkeypatch.setattr(dc, "_fork_watchdog", lambda _timeout: None)

    result = dc._complete_handoff(state, gated, _options())

    assert result == state
    assert released == [gated]


def test_unknown_handoff_ownership_fails_closed_without_touching_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unknown durable ownership cannot operate either authority path."""
    state = _state(supervisor_owned=None)
    gated = _gated(state)
    monkeypatch.setattr(
        dc,
        "_release_gate",
        lambda _gated: pytest.fail("unknown ownership must not release legacy gate"),
    )
    monkeypatch.setattr(
        dc,
        "publish_mission",
        lambda _state, _timeout: pytest.fail("unknown ownership must not publish supervised mission"),
    )

    with pytest.raises(dc.DeployCtlError, match="ownership"):
        dc._complete_handoff(state, gated, _options())
