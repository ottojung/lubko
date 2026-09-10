"""Supervisor authority recovery through the independent confirmed runtime anchor."""

from __future__ import annotations

import pytest

from lubko import cli, deployctl, lifecycle, supervise, supervisor


def _malformed_desired() -> supervise.SupervisorDesired | None:
    message = "malformed desired authority"
    raise supervise.DesiredIntentError(message)


def _malformed_mission() -> deployctl.RollbackState | None:
    message = "malformed candidate mission"
    raise deployctl.DeployCtlError(message)


def test_derive_action_restores_confirmed_before_mission_on_malformed_desired(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Malformed desired state cannot hide the independently confirmed runtime."""
    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    confirmed = "a" * 40
    mission_reads: list[bool] = []
    monkeypatch.setattr(supervise, "read_desired_strict", _malformed_desired)
    monkeypatch.setattr(deployctl, "read_rollback_state", lambda: mission_reads.append(True))
    monkeypatch.setattr(cli, "current_commit", lambda: confirmed)
    monkeypatch.setattr(cli, "runtime_is_usable", lambda commit: commit == confirmed)

    action = daemon._derive_action(supervise.SupervisorState.from_dict({}))

    assert action == ("run", confirmed)
    assert mission_reads == []


def test_reconcile_restores_confirmed_on_malformed_desired(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reconciliation keeps service available from the independent confirmed anchor."""
    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    confirmed = "a" * 40
    ensured: list[str] = []
    monkeypatch.setattr(supervise, "read_desired_strict", _malformed_desired)
    monkeypatch.setattr(cli, "current_commit", lambda: confirmed)
    monkeypatch.setattr(cli, "runtime_is_usable", lambda commit: commit == confirmed)
    monkeypatch.setattr(supervisor, "read_state", lambda: supervise.SupervisorState.from_dict({}))
    monkeypatch.setattr(daemon, "_ensure_worker", ensured.append)
    monkeypatch.setattr(daemon, "_maybe_reset_backoff", lambda _state, _now: None)
    monkeypatch.setattr(daemon, "_record_mission_progress", lambda _commit: None)
    monkeypatch.setattr(daemon, "_probe_readiness", lambda _now: None)
    monkeypatch.setattr(daemon, "_complete_cold_migration", lambda: None)
    monkeypatch.setattr(daemon, "_converge_startup_artifacts", lambda: None)

    daemon.reconcile(0.0)

    assert ensured == [confirmed]
    assert daemon._message is not None
    assert "independently confirmed runtime" in daemon._message


def test_corrupt_candidate_mission_restores_confirmed_not_desired_candidate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unreadable candidate state cannot outrank the independently confirmed runtime."""
    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    confirmed = "a" * 40
    candidate = _desired("b" * 40, generation=7)
    monkeypatch.setattr(supervise, "read_desired_strict", lambda: candidate)
    monkeypatch.setattr(deployctl, "read_rollback_state", _malformed_mission)
    monkeypatch.setattr(cli, "current_commit", lambda: confirmed)
    monkeypatch.setattr(cli, "runtime_is_usable", lambda commit: commit == confirmed)

    action = daemon._derive_action(supervise.SupervisorState.from_dict({}))

    assert action == ("run", confirmed)
    assert daemon._message is not None
    assert "restoring independently confirmed runtime" in daemon._message


def test_malformed_desired_cannot_advance_pending_mission_generation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Mission progress cannot advance when desired generation authority is malformed."""
    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    mission_reads: list[bool] = []
    state_writes: list[object] = []
    monkeypatch.setattr(supervise, "read_desired_strict", _malformed_desired)
    monkeypatch.setattr(
        deployctl,
        "read_rollback_state",
        lambda: mission_reads.append(True),
    )
    monkeypatch.setattr(daemon, "_write_state_authority_safe", state_writes.append)

    daemon._record_mission_progress("a" * 40)

    assert mission_reads == []
    assert state_writes == []


def test_malformed_desired_cannot_clear_cold_migration_authority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cold-migration completion cannot clear authority when desired state is malformed."""
    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    lock_entries: list[bool] = []
    monkeypatch.setattr(supervise, "read_desired_strict", _malformed_desired)
    monkeypatch.setattr(
        lifecycle,
        "deploy_lock",
        lambda _timeout: lock_entries.append(True),
    )

    daemon._complete_cold_migration()

    assert lock_entries == []


def _desired(commit: str, generation: int = 1) -> supervise.SupervisorDesired:
    """Return one valid desired run intent for spawn-boundary tests."""
    return supervise.SupervisorDesired(
        schema_version=supervise.SCHEMA_VERSION,
        generation=generation,
        commit=commit,
        repo="/workspace/Lubko",
        uv_path="/usr/bin/uv",
        worker_id=None,
    )


def test_pre_spawn_revalidation_restores_confirmed_on_malformed_desired(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The final generation-locked spawn gate still selects confirmed A."""
    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    confirmed = "a" * 40
    spawned: list[str] = []
    monkeypatch.setattr(supervise, "read_desired_strict", _malformed_desired)
    monkeypatch.setattr(cli, "current_commit", lambda: confirmed)
    monkeypatch.setattr(cli, "runtime_is_usable", lambda commit: commit == confirmed)
    monkeypatch.setattr(supervisor, "read_state", lambda: supervise.SupervisorState.from_dict({}))
    monkeypatch.setattr(supervisor, "write_state", lambda _state: None)

    def spawn(commit: str) -> None:
        spawned.append(commit)

    monkeypatch.setattr(daemon, "_spawn_worker", spawn)

    daemon._spawn_and_publish(confirmed)

    assert spawned == [confirmed]


def test_malformed_authority_holds_without_usable_confirmed_anchor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Recovery remains fail-closed when there is no trusted confirmed runtime."""
    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    monkeypatch.setattr(supervise, "read_desired_strict", _malformed_desired)
    monkeypatch.setattr(cli, "current_commit", lambda: None)

    action = daemon._derive_action(supervise.SupervisorState.from_dict({}))

    assert action == ("hold", None)
    assert daemon._message is not None
    assert "no usable confirmed runtime" in daemon._message


def test_pre_spawn_revalidation_blocks_superseded_commit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A newer durable commit cannot permit a stale commit selected earlier."""
    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    newer = _desired("b" * 40, generation=2)
    monkeypatch.setattr(supervise, "read_desired_strict", lambda: newer)
    monkeypatch.setattr(deployctl, "read_rollback_state", lambda: None)
    monkeypatch.setattr(
        daemon,
        "_spawn_worker",
        lambda _commit: pytest.fail("superseded commit reached _spawn_worker"),
    )

    daemon._spawn_and_publish("a" * 40)

    assert daemon._message is not None
    assert "intent changed" in daemon._message


def test_pre_spawn_revalidation_preserves_unchanged_desired(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unchanged valid desired commit still crosses the final spawn gate."""
    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    desired = _desired("a" * 40)
    spawned: list[str] = []
    monkeypatch.setattr(supervise, "read_desired_strict", lambda: desired)
    monkeypatch.setattr(deployctl, "read_rollback_state", lambda: None)
    monkeypatch.setattr(
        daemon,
        "_spawn_worker",
        spawned.append,
    )

    daemon._spawn_and_publish("a" * 40)

    assert spawned == ["a" * 40]
