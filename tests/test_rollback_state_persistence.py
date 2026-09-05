"""Rollback-state persistence must agree with its own strict parser."""

from __future__ import annotations

import time
from contextlib import nullcontext
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from lubko import cli, deployctl, lifecycle, supervise

OLD = "1" * 40
NEW = "2" * 40


def _meta(commit: str, pid: int) -> lifecycle.WorkerMeta:
    """Return a valid worker identity for persistence tests."""
    token = f"{pid:032x}"
    return lifecycle.WorkerMeta(
        schema_version=lifecycle.SCHEMA_VERSION,
        state=lifecycle.STATE_RUNNING,
        pid=pid,
        pgid=pid,
        sid=pid,
        start_time_ticks=pid * 10,
        token=token,
        repo="/r",
        git_commit=commit,
        worker_id="w",
        log_path="/l",
        started_at=1.0,
        stopped_at=None,
    )


def _identityless_supervisor_sentinel() -> dict[str, object]:
    """Return the exact sentinel emitted by earlier supervisor-aware controllers."""
    return {
        "schema_version": lifecycle.SCHEMA_VERSION,
        "state": lifecycle.STATE_RUNNING,
        "pid": 0,
        "pgid": 0,
        "sid": 0,
        "start_time_ticks": 0,
        "token": None,
        "repo": "/r",
        "git_commit": NEW,
        "worker_id": "",
        "log_path": "",
        "started_at": None,
        "stopped_at": None,
    }


def _options() -> deployctl.Options:
    """Return bounded deployment options for an isolated test checkout."""
    return deployctl.Options(
        repo=Path("/r"),
        uv_path="uv",
        confirm_window_seconds=60.0,
        stop_grace_seconds=1.0,
        postgres_timeout_seconds=1.0,
        lock_timeout_seconds=1.0,
        validation_timeout_seconds=1.0,
        git_timeout_seconds=1.0,
        cli_timeout_seconds=1.0,
    )


def _supervised_mission() -> deployctl.RollbackState:
    """Return a supervisor-owned mission without duplicated candidate identity."""
    return deployctl.RollbackState(
        schema_version=deployctl.ROLLBACK_SCHEMA_VERSION,
        generation=1,
        status=deployctl.STATUS_PENDING,
        commit=NEW,
        previous_commit=OLD,
        deadline=time.time() + 60.0,
        repo="/r",
        uv_path="uv",
        stop_grace_seconds=1.0,
        git_timeout_seconds=1.0,
        previous_retiring=False,
        previous_meta=_meta(OLD, 1),
        new_meta=None,
        supervisor_owned=True,
    )


@pytest.fixture(autouse=True)
def state_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Isolate every durable authority surface."""
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state"))
    supervise.state_path().parent.mkdir(parents=True, exist_ok=True)


def test_supervised_prepare_state_stays_readable_through_status_and_confirmation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Prepared supervisor state round-trips before and after child publication."""
    previous = _meta(OLD, 1)
    lifecycle.write_meta(previous)
    monkeypatch.setattr(deployctl, "worker_alive", lambda meta: meta == previous)
    monkeypatch.setattr(deployctl, "_require_exact_commit", lambda *_, **__: None)
    monkeypatch.setattr(deployctl, "_require_clean_checkout", lambda *_, **__: None)
    monkeypatch.setattr(deployctl, "_checkout", lambda *_, **__: True)
    monkeypatch.setattr(
        deployctl,
        "run_validation",
        lambda *_, **__: SimpleNamespace(ok=True),
    )
    monkeypatch.setattr(cli, "build_cli_root", lambda *_, **__: None)
    monkeypatch.setattr(deployctl, "check_postgres", lambda *_, **__: True)
    monkeypatch.setattr(deployctl, "_fork_watchdog", lambda _timeout: None)

    state, gated = deployctl._prepare_locked(_options(), NEW, supervised=True)
    assert gated is None
    assert state.new_meta is None

    deployctl.publish_mission(state, 1.0)
    before_child = deployctl.read_rollback_state()
    assert before_child == state
    assert before_child.new_meta is None

    monkeypatch.setattr(supervise, "supervisor_running", lambda: True)
    supervise.write_state(
        replace(
            supervise.read_state(),
            commit=NEW,
            applied_generation=state.generation,
            child=None,
            ready=False,
        )
    )
    monkeypatch.setattr(deployctl, "_reconcile_cli", lambda _state: None)
    status = deployctl._handle_status(_options())
    assert status["phase"] == "await-confirmation"

    candidate = _meta(NEW, 2)
    lifecycle.write_meta(candidate)
    child = supervise.WorkerChild(
        pid=candidate.pid,
        pgid=candidate.pgid,
        sid=candidate.sid,
        start_time_ticks=candidate.start_time_ticks,
        token=candidate.token or "",
        worker_id=candidate.worker_id,
        spawned_at=1.0,
    )
    supervise.write_state(
        replace(
            supervise.read_state(),
            commit=NEW,
            applied_generation=state.generation,
            child=child,
            ready=True,
        )
    )
    monkeypatch.setattr(supervise, "child_alive", lambda _child: True)

    after_child = deployctl.read_rollback_state()
    assert after_child is not None
    assert after_child.new_meta is None
    assert deployctl._handle_status(_options())["phase"] == "await-confirmation"

    supervise.write_desired(
        supervise.SupervisorDesired(
            schema_version=supervise.SCHEMA_VERSION,
            generation=state.generation,
            commit=NEW,
            repo=state.repo,
            uv_path=state.uv_path,
            worker_id=candidate.worker_id,
        )
    )
    monkeypatch.setattr(deployctl, "settle_desired", lambda *_, **__: state.generation)
    monkeypatch.setattr(supervise, "generation_lock", nullcontext)
    monkeypatch.setattr(
        supervise,
        "read_status",
        lambda: SimpleNamespace(
            applied_generation=state.generation,
            commit=NEW,
            ready=True,
            holding=False,
        ),
    )
    monkeypatch.setattr(cli, "set_current", lambda _commit: None)
    monkeypatch.setattr(cli, "gc_cli_roots", lambda _commits: None)
    monkeypatch.setattr(deployctl, "append_deploy_log", lambda _line: None)

    response = deployctl._confirm_locked({"type": "confirm", "commit": NEW}, _options())
    assert response["confirmed"] is True
    terminal = deployctl.read_rollback_state()
    assert terminal is not None
    assert terminal.status == deployctl.STATUS_CONFIRMED
    assert terminal.new_meta is None


@pytest.mark.parametrize("supervisor_owned", [False, None])
def test_missing_candidate_identity_requires_explicit_supervisor_ownership(
    supervisor_owned: object,
) -> None:
    """Legacy or unknown ownership cannot omit the candidate process identity."""
    payload = _supervised_mission().to_dict()
    payload["supervisor_owned"] = supervisor_owned
    with pytest.raises(deployctl.DeployCtlError, match="malformed"):
        deployctl.RollbackState.from_dict(payload)


def test_exact_identityless_supervisor_sentinel_normalizes_to_absent_identity() -> None:
    """Previously persisted supervisor sentinels remain recoverable after upgrade."""
    payload = _supervised_mission().to_dict()
    payload["new_meta"] = _identityless_supervisor_sentinel()
    parsed = deployctl.RollbackState.from_dict(payload)
    assert parsed.new_meta is None


def test_near_miss_identityless_supervisor_sentinel_fails_closed() -> None:
    """Only the exact historical sentinel bypasses strict worker identity parsing."""
    payload = _supervised_mission().to_dict()
    sentinel = _identityless_supervisor_sentinel()
    sentinel["worker_id"] = "unexpected"
    payload["new_meta"] = sentinel
    with pytest.raises(deployctl.DeployCtlError, match="malformed"):
        deployctl.RollbackState.from_dict(payload)


def test_malformed_candidate_identity_still_fails_closed() -> None:
    """Explicit supervisor ownership does not make malformed identity dictionaries valid."""
    payload = _supervised_mission().to_dict()
    invalid = _meta(NEW, 2).to_dict()
    invalid["pid"] = 0
    payload["new_meta"] = invalid
    with pytest.raises(deployctl.DeployCtlError, match="malformed"):
        deployctl.RollbackState.from_dict(payload)


def test_legacy_candidate_identity_still_round_trips() -> None:
    """The emergency legacy path retains its exact stored candidate identity."""
    legacy = replace(
        _supervised_mission(),
        new_meta=_meta(NEW, 2),
        supervisor_owned=False,
    )
    assert deployctl.RollbackState.from_dict(legacy.to_dict()) == legacy
