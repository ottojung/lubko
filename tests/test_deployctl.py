"""Focused tests for the supervised deployment controller."""

from __future__ import annotations

import time
from dataclasses import replace
from pathlib import Path

import pytest

from lubko import deployctl as dc
from lubko.lifecycle import SCHEMA_VERSION, STATE_RUNNING, WorkerMeta


def worker_meta(commit: str, *, pid: int = 100) -> WorkerMeta:
    """Build deterministic maintained-worker metadata for controller tests.

    Args:
        commit: Exact commit represented by the worker.
        pid: Synthetic process identity.

    Returns:
        Worker metadata suitable for rollback-state tests.
    """
    return WorkerMeta(
        schema_version=SCHEMA_VERSION,
        state=STATE_RUNNING,
        pid=pid,
        pgid=pid,
        sid=pid,
        start_time_ticks=pid * 10,
        token=f"token-{pid}",
        repo="/workspace/Lubko",
        git_commit=commit,
        worker_id="test-worker",
        log_path="/workspace/worker.log",
        started_at=1.0,
        stopped_at=None,
    )


def pending_state() -> dc.RollbackState:
    """Return a live pending deployment state.

    Returns:
        A pending rollback state with distinct old/new commits.
    """
    old = "1" * 40
    new = "2" * 40
    return dc.RollbackState(
        schema_version=dc.ROLLBACK_SCHEMA_VERSION,
        status=dc.STATUS_PENDING,
        commit=new,
        previous_commit=old,
        challenge_hash=None,
        deadline=time.time() + 60,
        repo="/workspace/Lubko",
        uv_path="/usr/bin/uv",
        stop_grace_seconds=1.0,
        git_timeout_seconds=1.0,
        previous_meta=worker_meta(old, pid=100),
        new_meta=worker_meta(new, pid=200),
    )


def test_rollback_state_round_trip(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Persisted rollback state round-trips without losing process identity."""
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
    state = pending_state()

    dc._write_state(state)

    assert dc._read_state() == state


def test_first_confirmation_persists_only_challenge_digest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The first confirmation returns a challenge but stores only its digest."""
    state = pending_state()
    written: list[dc.RollbackState] = []
    monkeypatch.setattr(dc, "_read_state", lambda: state)
    monkeypatch.setattr(dc, "worker_alive", lambda _meta: True)
    monkeypatch.setattr(dc, "_write_state", written.append)

    response = dc._confirm_locked({"type": "confirm", "commit": state.commit})

    challenge = response["challenge"]
    assert isinstance(challenge, str)
    assert challenge
    assert written[-1].challenge_hash == dc._challenge_digest(challenge)
    assert challenge not in written[-1].to_dict().values()


def test_second_confirmation_writes_meta_before_terminal_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Successful confirmation records candidate metadata before terminal state."""
    state = pending_state()
    challenge = "challenge-value"
    challenged = replace(state, challenge_hash=dc._challenge_digest(challenge))
    events: list[str] = []
    written: list[dc.RollbackState] = []

    monkeypatch.setattr(dc, "_read_state", lambda: challenged)
    monkeypatch.setattr(dc, "worker_alive", lambda _meta: True)

    def record_meta(_meta: WorkerMeta) -> None:
        events.append("meta")

    def record_state(value: dc.RollbackState) -> None:
        events.append("state")
        written.append(value)

    monkeypatch.setattr(dc, "write_meta", record_meta)
    monkeypatch.setattr(dc, "_write_state", record_state)
    monkeypatch.setattr(dc, "append_deploy_log", lambda _message: None)

    response = dc._confirm_locked({
        "type": "confirm",
        "commit": state.commit,
        "challenge": challenge[::-1],
    })

    assert response["confirmed"] is True
    assert events == ["meta", "state"]
    assert written[-1].status == dc.STATUS_CONFIRMED


def test_wrong_challenge_rolls_back(monkeypatch: pytest.MonkeyPatch) -> None:
    """An incorrect second factor immediately invokes rollback."""
    state = pending_state()
    challenged = replace(state, challenge_hash=dc._challenge_digest("expected"))
    rollbacks: list[dc.RollbackState] = []
    monkeypatch.setattr(dc, "_read_state", lambda: challenged)
    monkeypatch.setattr(dc, "worker_alive", lambda _meta: True)

    def rollback(value: dc.RollbackState) -> bool:
        rollbacks.append(value)
        return True

    monkeypatch.setattr(dc, "_rollback_locked", rollback)

    with pytest.raises(dc.DeployCtlError, match="incorrect"):
        dc._confirm_locked({
            "type": "confirm",
            "commit": state.commit,
            "challenge": "wrong",
        })

    assert rollbacks == [challenged]


def test_watchdog_rollback_condition_uses_deadline_or_candidate_death(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Status lazily rolls back a dead pending candidate under the same lock."""
    state = pending_state()
    rolled_back = replace(state, status=dc.STATUS_ROLLED_BACK)
    states = iter((state, state, rolled_back))
    calls: list[dc.RollbackState] = []

    class FakeLock:
        """Minimal deployment-lock context for the status test."""

        def __enter__(self) -> None:
            return None

        def __exit__(self, *_args: object) -> None:
            return None

    monkeypatch.setattr(dc, "deploy_lock", lambda _timeout: FakeLock())
    monkeypatch.setattr(dc, "_read_state", lambda: next(states))
    monkeypatch.setattr(dc, "worker_alive", lambda _meta: False)
    monkeypatch.setattr(dc, "read_meta", lambda: state.previous_meta)

    def rollback(value: dc.RollbackState) -> bool:
        calls.append(value)
        return True

    monkeypatch.setattr(dc, "_rollback_locked", rollback)

    result = dc._handle_status(
        dc.Options(
            repo=Path("/workspace/Lubko"),
            uv_path="/usr/bin/uv",
            confirm_window_seconds=120,
            stop_grace_seconds=5,
            postgres_timeout_seconds=5,
            lock_timeout_seconds=5,
            validation_timeout_seconds=5,
            git_timeout_seconds=5,
        )
    )

    assert calls == [state]
    assert result["phase"] == "idle"
    assert result["last_outcome"] == dc.STATUS_ROLLED_BACK
