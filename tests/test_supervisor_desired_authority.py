"""Fail-closed supervisor handling of malformed desired authority (#521)."""

from __future__ import annotations

from typing import TYPE_CHECKING

from lubko import deployctl, supervise, supervisor

if TYPE_CHECKING:
    import pytest


def _malformed_desired() -> supervise.SupervisorDesired | None:
    message = "malformed desired authority"
    raise supervise.DesiredIntentError(message)


def test_derive_action_holds_before_mission_precedence_on_malformed_desired(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Malformed desired authority blocks mission precedence before mission reads."""
    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    mission_reads: list[bool] = []
    monkeypatch.setattr(supervise, "read_desired_strict", _malformed_desired)
    monkeypatch.setattr(
        deployctl,
        "read_rollback_state",
        lambda: mission_reads.append(True),
    )

    action = daemon._derive_action(supervise.SupervisorState.from_dict({}))

    assert action == ("hold", None)
    assert mission_reads == []


def test_reconcile_holds_before_reading_mutable_worker_state_on_malformed_desired(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reconciliation treats malformed desired authority as a hold."""
    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    held: list[bool] = []
    monkeypatch.setattr(supervise, "read_desired_strict", _malformed_desired)
    monkeypatch.setattr(
        supervisor,
        "read_state",
        lambda: supervise.SupervisorState.from_dict({}),
    )
    monkeypatch.setattr(daemon, "_ensure_held", lambda: held.append(True))
    monkeypatch.setattr(daemon, "_maybe_reset_backoff", lambda _state, _now: None)

    daemon.reconcile(0.0)

    assert held == [True]
    assert daemon._message is not None
    assert "corrupt desired supervisor state" in daemon._message


def test_reconcile_converges_hold_if_desired_corrupts_between_reads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A mid-tick desired corruption cannot leave a selected worker running."""
    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    desired = supervise.SupervisorDesired(
        schema_version=supervise.SCHEMA_VERSION,
        generation=1,
        mode=supervise.MODE_RUN,
        commit="a" * 40,
        repo="/workspace/Lubko",
        uv_path="/usr/bin/uv",
        worker_id=None,
        requested_at=1.0,
    )
    reads = iter((desired,))

    def changing_desired() -> supervise.SupervisorDesired | None:
        try:
            return next(reads)
        except StopIteration:
            return _malformed_desired()

    held: list[bool] = []
    monkeypatch.setattr(supervise, "read_desired_strict", changing_desired)
    monkeypatch.setattr(deployctl, "read_rollback_state", lambda: None)
    monkeypatch.setattr(
        supervisor,
        "read_state",
        lambda: supervise.SupervisorState.from_dict({}),
    )
    monkeypatch.setattr(daemon, "_ensure_held", lambda: held.append(True))

    daemon.reconcile(0.0)

    assert held == [True]
    assert daemon._message is not None
    assert "corrupt desired supervisor state" in daemon._message


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
        supervisor.lifecycle,
        "deploy_lock",
        lambda _timeout: lock_entries.append(True),
    )

    daemon._complete_cold_migration()

    assert lock_entries == []
