"""Supervised restart backoff must not trigger premature rollback.

Confirm, status, readiness waits, and the watchdog must agree: under a live
supervisor whose durable state still targets the mission's exact commit and
generation, a transient ``child=None`` restart-backoff observation is retryable
until the mission deadline. Superseded or contradictory supervisor authority,
and an expired deadline without a live candidate, fail closed.
"""

from __future__ import annotations

import time
from contextlib import nullcontext
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

import lubko.cli
import lubko.deployctl
import lubko.lifecycle
import lubko.supervise

cli = lubko.cli
dc = lubko.deployctl
lifecycle = lubko.lifecycle
supervise = lubko.supervise

OLD_COMMIT = "1" * 40
NEW_COMMIT = "2" * 40
OTHER_COMMIT = "3" * 40
GENERATION = 5


def _worker_meta(commit: str, *, pid: int) -> lifecycle.WorkerMeta:
    return lifecycle.WorkerMeta(
        schema_version=lifecycle.SCHEMA_VERSION,
        state=lifecycle.STATE_RUNNING,
        pid=pid,
        pgid=pid,
        sid=pid,
        start_time_ticks=pid * 10,
        token=f"token-{pid}",
        repo="/workspace/Lubko",
        git_commit=commit,
        worker_id="test-worker",
        log_path="worker.log",
        started_at=1.0,
        stopped_at=None,
    )


def _options() -> dc.Options:
    """Return runtime options for the deployment handlers.

    Returns:
        Runtime options.
    """
    return dc.Options(
        repo=Path("/workspace/Lubko"),
        uv_path="uv",
        confirm_window_seconds=60.0,
        stop_grace_seconds=1.0,
        postgres_timeout_seconds=1.0,
        lock_timeout_seconds=1.0,
        validation_timeout_seconds=1.0,
        git_timeout_seconds=1.0,
        cli_timeout_seconds=1.0,
    )


def _supervisor_state(**overrides: object) -> supervise.SupervisorState:
    """Return a supervisor snapshot in ordinary restart backoff.

    Args:
        overrides: Field values replacing the defaults.

    Returns:
        The supervisor state snapshot.
    """
    defaults: dict[str, object] = {
        "schema_version": supervise.SCHEMA_VERSION,
        "applied_generation": GENERATION,
        "mode": supervise.MODE_RUN,
        "commit": NEW_COMMIT,
        "child": None,
        "unresolved_child": None,
        "ownership_hold_malformed": False,
        "unresolved_hold_malformed": False,
        "spawning": None,
        "spawning_hold_malformed": False,
        "intent": supervise.INTENT_RUN,
        "restart_count": 1,
        "next_attempt_at": time.time() + 2.0,
        "last_exit": None,
        "last_spawn_at": None,
        "ready": False,
        "next_readiness_at": None,
        "boot_id": None,
    }
    defaults.update(overrides)
    return supervise.SupervisorState(**defaults)  # type: ignore[arg-type]


def _supervisor_status(
    *, child: supervise.WorkerChild | None, ready: bool
) -> supervise.SupervisorStatus:
    """Return a supervisor status observation.

    Args:
        child: Worker child currently recorded, if any.
        ready: Whether the worker is queue-ready.

    Returns:
        The supervisor status observation.
    """
    return supervise.SupervisorStatus(
        schema_version=supervise.SCHEMA_VERSION,
        supervisor_pid=4242,
        supervisor_start_time_ticks=111,
        started_at=1.0,
        applied_generation=GENERATION,
        mode=supervise.MODE_RUN,
        commit=NEW_COMMIT,
        child=child,
        intent=supervise.INTENT_RUN,
        restart_count=1,
        next_attempt_at=None if child is not None else time.time() + 2.0,
        last_exit=None,
        mission=None,
        db_ready=None,
        ready=ready,
        message=None,
        worker_health=None,
    )


@pytest.fixture
def mission(monkeypatch: pytest.MonkeyPatch) -> dc.RollbackState:
    """Install a pending supervised mission backed by an in-memory store.

    Returns:
        The pending mission.
    """
    state = dc.RollbackState(
        schema_version=dc.ROLLBACK_SCHEMA_VERSION,
        generation=GENERATION,
        status=dc.STATUS_PENDING,
        commit=NEW_COMMIT,
        previous_commit=OLD_COMMIT,
        deadline=time.time() + 3600.0,
        repo="/workspace/Lubko",
        uv_path="uv",
        stop_grace_seconds=1.0,
        git_timeout_seconds=5.0,
        previous_retiring=False,
        previous_meta=_worker_meta(OLD_COMMIT, pid=100),
        new_meta=_worker_meta(NEW_COMMIT, pid=200),
        supervisor_owned=True,
    )
    store: dict[str, dc.RollbackState] = {"state": state}
    monkeypatch.setattr(dc, "_read_state", lambda: store.get("state"))
    monkeypatch.setattr(dc, "_write_state", lambda value: store.__setitem__("state", value))
    return state


@pytest.fixture
def live_supervisor(monkeypatch: pytest.MonkeyPatch) -> list[supervise.SupervisorState]:
    """Pretend a live supervisor daemon is running with a settable snapshot.

    Returns:
        The single-element mutable supervisor snapshot list.
    """
    snapshot: list[supervise.SupervisorState] = [_supervisor_state()]
    children: dict[supervise.WorkerChild, bool] = {}
    monkeypatch.setattr(supervise, "supervisor_running", lambda: True)
    monkeypatch.setattr(supervise, "read_state", lambda: snapshot[0])
    monkeypatch.setattr(supervise, "child_alive", lambda child: children.get(child, False))
    return snapshot


@pytest.fixture
def settlement(monkeypatch: pytest.MonkeyPatch) -> dict[str, int]:
    """Record supervised settlements instead of talking to a real daemon.

    Returns:
        Mapping of settled commit to settlement count.
    """
    recorded: dict[str, int] = {}
    settled: dict[str, str | int] = {"commit": NEW_COMMIT, "generation": GENERATION}

    def record(commit: str, _repo: str, _uv_path: str) -> int:
        generation = GENERATION + 10
        recorded[commit] = recorded.get(commit, 0) + 1
        settled["commit"] = commit
        settled["generation"] = generation
        return generation

    monkeypatch.setattr(dc, "settle_desired", record)
    monkeypatch.setattr(supervise, "generation_lock", nullcontext)
    monkeypatch.setattr(
        supervise,
        "read_desired_strict",
        lambda: SimpleNamespace(commit=settled["commit"], generation=settled["generation"]),
    )
    monkeypatch.setattr(
        supervise,
        "read_status",
        lambda: SimpleNamespace(
            applied_generation=settled["generation"],
            commit=settled["commit"],
            ready=True,
            holding=False,
        ),
    )
    return recorded


@pytest.fixture
def cli_stubs(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep CLI side effects off the filesystem."""
    monkeypatch.setattr(cli, "remove_cli_root", lambda _commit: None)
    monkeypatch.setattr(cli, "reconcile_pointer", lambda _commit: True)
    monkeypatch.setattr(cli, "set_current", lambda _commit: None)
    monkeypatch.setattr(cli, "gc_cli_roots", lambda _commits: None)
    monkeypatch.setattr(dc, "append_deploy_log", lambda _line: None)


@pytest.fixture
def status_env(mission: dc.RollbackState, monkeypatch: pytest.MonkeyPatch) -> None:
    """Stub metadata reads used only by the status handler."""
    del mission
    monkeypatch.setattr(dc, "read_meta", lambda: None)
    monkeypatch.setattr(dc, "_reconcile_cli", lambda _state: None)


def test_status_survives_restart_backoff(
    mission: dc.RollbackState,
    live_supervisor: list[supervise.SupervisorState],
    settlement: dict[str, int],
    cli_stubs: None,
    status_env: None,
) -> None:
    """Status reporting must not roll back during ordinary restart backoff."""
    del cli_stubs, status_env, live_supervisor, mission
    response = dc._handle_status(_options())

    assert response["phase"] == "await-confirmation"
    assert settlement == {}
    current = dc._read_state()
    assert current is not None
    assert current.status == dc.STATUS_PENDING


def test_expired_deadline_rolls_back_despite_matching_authority(
    live_supervisor: list[supervise.SupervisorState],
    settlement: dict[str, int],
    cli_stubs: None,
    status_env: None,
) -> None:
    """Once the mission deadline passes without a live candidate, roll back."""
    del cli_stubs, status_env, live_supervisor
    current = dc._read_state()
    assert current is not None
    dc._write_state(replace(current, deadline=time.time() - 1.0))

    response = dc._handle_status(_options())

    assert response["phase"] == "idle"
    assert response["last_outcome"] == dc.STATUS_ROLLED_BACK
    assert settlement == {OLD_COMMIT: 1}


def _live_candidate() -> supervise.WorkerChild:
    """Return one stable live worker identity for deadline tests."""
    return supervise.WorkerChild(
        pid=300,
        pgid=300,
        sid=300,
        start_time_ticks=3000,
        token=f"worker-auth-{300}",
        worker_id="worker",
        spawned_at=1.0,
    )


def test_expired_deadline_rolls_back_live_but_not_ready_candidate(
    live_supervisor: list[supervise.SupervisorState],
    settlement: dict[str, int],
    cli_stubs: None,
    status_env: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An expired mission requires queue readiness, not only a live child."""
    del cli_stubs, status_env
    child = _live_candidate()
    live_supervisor[0] = replace(live_supervisor[0], child=child)
    monkeypatch.setattr(supervise, "child_alive", lambda candidate: candidate == child)

    def status() -> SimpleNamespace:
        rolled_back = OLD_COMMIT in settlement
        return SimpleNamespace(
            applied_generation=GENERATION + 10 if rolled_back else GENERATION,
            commit=OLD_COMMIT if rolled_back else NEW_COMMIT,
            ready=rolled_back,
            holding=False,
        )

    monkeypatch.setattr(supervise, "read_status", status)
    current = dc._read_state()
    assert current is not None
    dc._write_state(replace(current, deadline=time.time() - 1.0))

    response = dc._handle_status(_options())

    assert response["phase"] == "idle"
    assert response["last_outcome"] == dc.STATUS_ROLLED_BACK
    assert settlement == {OLD_COMMIT: 1}


def test_expired_deadline_rolls_back_compatible_newer_live_but_not_ready_candidate(
    live_supervisor: list[supervise.SupervisorState],
    settlement: dict[str, int],
    cli_stubs: None,
    status_env: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A newer same-commit generation must still be queue-ready after the deadline."""
    del cli_stubs, status_env
    child = _live_candidate()
    generation = GENERATION + 1
    live_supervisor[0] = replace(live_supervisor[0], applied_generation=generation, child=child)
    monkeypatch.setattr(supervise, "child_alive", lambda candidate: candidate == child)

    def desired() -> SimpleNamespace:
        rolled_back = OLD_COMMIT in settlement
        return SimpleNamespace(
            commit=OLD_COMMIT if rolled_back else NEW_COMMIT,
            generation=GENERATION + 10 if rolled_back else generation,
        )

    def status() -> SimpleNamespace:
        rolled_back = OLD_COMMIT in settlement
        return SimpleNamespace(
            applied_generation=GENERATION + 10 if rolled_back else generation,
            commit=OLD_COMMIT if rolled_back else NEW_COMMIT,
            ready=rolled_back,
            holding=False,
        )

    monkeypatch.setattr(supervise, "read_desired_strict", desired)
    monkeypatch.setattr(supervise, "read_status", status)
    current = dc._read_state()
    assert current is not None
    dc._write_state(replace(current, deadline=time.time() - 1.0))

    response = dc._handle_status(_options())

    assert response["phase"] == "idle"
    assert response["last_outcome"] == dc.STATUS_ROLLED_BACK
    assert settlement == {OLD_COMMIT: 1}


def test_future_deadline_keeps_live_but_not_ready_candidate_pending(
    live_supervisor: list[supervise.SupervisorState],
    settlement: dict[str, int],
    cli_stubs: None,
    status_env: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Before the deadline, live non-readiness remains retryable."""
    del cli_stubs, status_env
    child = _live_candidate()
    live_supervisor[0] = replace(live_supervisor[0], child=child)
    monkeypatch.setattr(supervise, "child_alive", lambda candidate: candidate == child)
    monkeypatch.setattr(
        supervise,
        "read_status",
        lambda: SimpleNamespace(
            applied_generation=GENERATION,
            commit=NEW_COMMIT,
            ready=False,
            holding=False,
        ),
    )

    response = dc._handle_status(_options())

    assert response["phase"] == "await-confirmation"
    assert settlement == {}


def test_expired_deadline_keeps_queue_ready_live_candidate_pending(
    live_supervisor: list[supervise.SupervisorState],
    settlement: dict[str, int],
    cli_stubs: None,
    status_env: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An authoritative queue-ready live candidate remains confirmable after the deadline."""
    del cli_stubs, status_env
    child = _live_candidate()
    live_supervisor[0] = replace(live_supervisor[0], child=child)
    monkeypatch.setattr(supervise, "child_alive", lambda candidate: candidate == child)
    current = dc._read_state()
    assert current is not None
    dc._write_state(replace(current, deadline=time.time() - 1.0))

    response = dc._handle_status(_options())

    assert response["phase"] == "await-confirmation"
    assert settlement == {}


def test_superseded_authority_fails_closed_before_deadline(
    mission: dc.RollbackState,
    live_supervisor: list[supervise.SupervisorState],
    settlement: dict[str, int],
    cli_stubs: None,
    status_env: None,
) -> None:
    """A supervisor snapshot naming another commit fails closed immediately."""
    del cli_stubs, status_env
    live_supervisor[0] = replace(live_supervisor[0], commit=OTHER_COMMIT)

    response = dc._handle_status(_options())

    assert response["phase"] == "idle"
    assert response["last_outcome"] == dc.STATUS_ROLLED_BACK
    assert settlement == {OLD_COMMIT: 1}

    dc._write_state(mission)
    settlement.clear()
    with pytest.raises(dc.DeployCtlError, match="rolled back"):
        dc._confirm_locked({"type": "confirm", "commit": NEW_COMMIT}, _options())
    assert settlement == {OLD_COMMIT: 1}


def test_newer_generation_same_commit_authority_fails_closed(
    mission: dc.RollbackState,
    live_supervisor: list[supervise.SupervisorState],
    settlement: dict[str, int],
    cli_stubs: None,
    status_env: None,
) -> None:
    """A higher applied generation supersedes the mission even for the same commit."""
    del cli_stubs, status_env
    live_supervisor[0] = replace(live_supervisor[0], applied_generation=GENERATION + 1)

    response = dc._handle_status(_options())

    assert response["phase"] == "idle"
    assert response["last_outcome"] == dc.STATUS_ROLLED_BACK
    assert settlement == {OLD_COMMIT: 1}

    dc._write_state(mission)
    settlement.clear()
    with pytest.raises(dc.DeployCtlError, match="rolled back"):
        dc._confirm_locked({"type": "confirm", "commit": NEW_COMMIT}, _options())
    assert settlement == {OLD_COMMIT: 1}


def test_newer_generation_same_commit_compatible_authority_stays_pending(
    live_supervisor: list[supervise.SupervisorState],
    settlement: dict[str, int],
    cli_stubs: None,
    status_env: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A compatible newer same-commit lifecycle generation does not trigger rollback."""
    del cli_stubs, status_env
    generation = GENERATION + 1
    live_supervisor[0] = replace(live_supervisor[0], applied_generation=generation)
    monkeypatch.setattr(
        supervise,
        "read_desired_strict",
        lambda: SimpleNamespace(commit=NEW_COMMIT, generation=generation),
    )
    monkeypatch.setattr(
        supervise,
        "read_status",
        lambda: SimpleNamespace(
            applied_generation=generation,
            commit=NEW_COMMIT,
            ready=True,
            holding=False,
        ),
    )

    response = dc._handle_status(_options())

    assert response["phase"] == "await-confirmation"
    assert settlement == {}
    current = dc._read_state()
    assert current is not None
    assert current.status == dc.STATUS_PENDING


def test_newer_generation_same_commit_restart_backoff_stays_pending(
    live_supervisor: list[supervise.SupervisorState],
    settlement: dict[str, int],
    cli_stubs: None,
    status_env: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A compatible same-commit restart backoff remains retryable before deadline."""
    del cli_stubs, status_env
    generation = GENERATION + 1
    live_supervisor[0] = replace(live_supervisor[0], applied_generation=generation, child=None)
    monkeypatch.setattr(
        supervise,
        "read_desired_strict",
        lambda: SimpleNamespace(commit=NEW_COMMIT, generation=generation),
    )
    monkeypatch.setattr(
        supervise,
        "read_status",
        lambda: SimpleNamespace(
            applied_generation=generation,
            commit=NEW_COMMIT,
            ready=False,
            holding=False,
        ),
    )

    response = dc._handle_status(_options())

    assert response["phase"] == "await-confirmation"
    assert settlement == {}
    current = dc._read_state()
    assert current is not None
    assert current.status == dc.STATUS_PENDING


def test_wait_until_ready_polls_through_transient_child_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One ``child=None`` observation must not abort a readiness wait."""
    child = supervise.WorkerChild(
        pid=300,
        pgid=300,
        sid=300,
        start_time_ticks=3000,
        token=f"worker-auth-{300}",
        worker_id="worker",
        spawned_at=1.0,
    )
    observations = [
        _supervisor_status(child=None, ready=False),
        _supervisor_status(child=child, ready=False),
        _supervisor_status(child=child, ready=True),
    ]
    monkeypatch.setattr(supervise, "read_status", lambda: observations.pop(0))
    monkeypatch.setattr(time, "sleep", lambda _seconds: None)

    assert supervise.wait_until_ready(GENERATION, timeout_seconds=10.0) is True
    assert observations == []


def test_wait_until_ready_rejects_superseding_different_commit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A newer ready generation for another commit cannot satisfy the wait."""
    observation = replace(
        _supervisor_status(child=None, ready=True),
        applied_generation=GENERATION + 1,
        commit=OTHER_COMMIT,
    )
    monkeypatch.setattr(supervise, "read_status", lambda: observation)
    assert supervise.wait_until_ready(GENERATION, timeout_seconds=10.0, commit=NEW_COMMIT) is False


def test_wait_until_ready_accepts_newer_same_commit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A newer generation remains valid when it proves the same exact commit ready."""
    observation = replace(
        _supervisor_status(child=None, ready=True),
        applied_generation=GENERATION + 1,
        commit=NEW_COMMIT,
    )
    monkeypatch.setattr(supervise, "read_status", lambda: observation)
    assert supervise.wait_until_ready(GENERATION, timeout_seconds=10.0, commit=NEW_COMMIT) is True


def test_wait_until_ready_times_out_without_readiness(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A never-ready candidate bounds the wait by its timeout."""
    ticks = iter(range(100))
    monkeypatch.setattr(
        time,
        "monotonic",
        lambda: next(ticks) * 0.5,
    )
    # The fake monotonic clock advances on every read, so the poll loop
    # terminates deterministically; no real sleeping is needed.
    monkeypatch.setattr(time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(supervise, "REQUEST_POLL_SECONDS", 0.5)
    monkeypatch.setattr(
        supervise,
        "read_status",
        lambda: _supervisor_status(child=None, ready=False),
    )

    assert supervise.wait_until_ready(GENERATION, timeout_seconds=2.0) is False


def _desired(generation: int, commit: str) -> supervise.SupervisorDesired:
    """Return one exact desired supervisor intent."""
    return supervise.SupervisorDesired(
        schema_version=supervise.SCHEMA_VERSION,
        generation=generation,
        commit=commit,
        repo="/workspace/Lubko",
        uv_path="uv",
        worker_id="test-worker",
        requested_at=1.0,
    )
