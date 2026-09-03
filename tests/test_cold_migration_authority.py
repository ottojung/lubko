"""Cold-migration authority convergence invariants.

A cold lifecycle migration publishes the migrated target commit and its
completion obligation as one atomically durable desired intent. Until the
supervisor proves the migrated worker queue-ready, CLI reconciliation holds
fail-closed on the previous confirmed authority; once ready, completion
converges pointer and deployctl authority under the deployment lock and
never clobbers a strictly newer mission.
"""

from __future__ import annotations

import json
from dataclasses import replace
from typing import TYPE_CHECKING

import pytest

from lubko import cli, lifecycle, supervise
from lubko import deployctl as dc
from lubko.state import rollback_state_path
from lubko.supervisor import Settings, SupervisorDaemon

if TYPE_CHECKING:
    from pathlib import Path

OLD = "1" * 40
NEW = "2" * 40
NEWER = "3" * 40


def meta(commit: str, pid: int) -> lifecycle.WorkerMeta:
    """Return a minimal valid WorkerMeta for ``commit``."""
    return lifecycle.WorkerMeta(
        schema_version=lifecycle.SCHEMA_VERSION,
        state=lifecycle.STATE_RUNNING,
        pid=pid,
        pgid=pid,
        sid=pid,
        start_time_ticks=pid,
        token=f"token-{pid}",
        repo="/workspace/repo",
        git_commit=commit,
        worker_id="w",
        log_path="",
        started_at=1.0,
        stopped_at=None,
    )


def mission(generation: int, status: str, commit: str) -> dc.RollbackState:
    """Return a minimal mission record."""
    return dc.RollbackState(
        schema_version=dc.ROLLBACK_SCHEMA_VERSION,
        generation=generation,
        status=status,
        commit=commit,
        previous_commit=OLD,
        challenge_hash=None,
        deadline=0.0,
        repo="/workspace/repo",
        uv_path="uv",
        stop_grace_seconds=1.0,
        git_timeout_seconds=5.0,
        previous_retiring=False,
        previous_meta=meta(OLD, 100),
        new_meta=meta(commit, 200),
        supervisor_owned=True,
    )


def migration_intent(generation: int, commit: str) -> None:
    """Publish the durable cold-migration desired intent."""
    supervise.write_desired(
        supervise.SupervisorDesired(
            schema_version=supervise.SCHEMA_VERSION,
            generation=generation,
            commit=commit,
            repo="/workspace/repo",
            uv_path="uv",
            worker_id=None,
            migration=True,
        )
    )


def ready_state(generation: int, commit: str) -> None:
    """Record a proven queue-ready daemon state for the migrated commit."""
    supervise.write_state(
        replace(
            supervise.fresh_state(),
            mode="run",
            applied_generation=generation,
            commit=commit,
            ready=True,
        )
    )


@pytest.fixture
def isolated(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point every state surface at an isolated per-test directory.

    Returns:
        The isolated state root (unused directly; env-driven paths only).
    """
    root = tmp_path / "state"
    monkeypatch.setenv("XDG_STATE_HOME", str(root))
    return root


def test_completion_holds_deployment_lock(isolated: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Authority mutations are serialized under the deployctl deployment lock."""
    del isolated
    migration_intent(6, NEW)
    ready_state(6, NEW)
    lock_held_during_mutation = False

    def prove_exclusion(_repo: Path, _commit: str, _uv_path: str, _timeout_seconds: float) -> Path:
        nonlocal lock_held_during_mutation
        try:
            with lifecycle.deploy_lock(0.0):
                pass
        except lifecycle.LockTimeoutError:
            lock_held_during_mutation = True
        return cli.cli_commit_dir(_commit)

    monkeypatch.setattr(cli, "build_cli_root", prove_exclusion)
    daemon = SupervisorDaemon(Settings())
    daemon._complete_cold_migration()
    assert lock_held_during_mutation


def test_newer_mission_survives_and_owns_authority_over_stale_migration(
    isolated: Path,
) -> None:
    """A newer published mission outranks the stale migration completion."""
    del isolated
    migration_intent(6, NEW)
    dc._write_state(mission(7, dc.STATUS_PENDING, NEWER))
    ready_state(6, NEW)
    daemon = SupervisorDaemon(Settings())
    daemon._complete_cold_migration()
    survivor = dc.read_rollback_state()
    assert survivor is not None
    assert survivor.generation == 7
    desired = supervise.read_desired()
    assert desired is not None
    assert desired.migration is False


@pytest.mark.parametrize(
    "malformed",
    ["{broken", "0", "true", "[]", "{}", '"unsupported"'],
)
def test_malformed_mission_holds_cold_migration_completion(
    isolated: Path, monkeypatch: pytest.MonkeyPatch, malformed: str
) -> None:
    """Malformed mission authority is preserved and repeatedly blocks settlement."""
    del isolated
    migration_intent(6, NEW)
    ready_state(6, NEW)
    path = rollback_state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(malformed, encoding="utf-8")
    cli_calls: list[str] = []
    monkeypatch.setattr(
        cli,
        "build_cli_root",
        lambda *_args, **_kwargs: cli_calls.append("build"),
    )

    daemon = SupervisorDaemon(Settings())
    for _ in range(2):
        daemon._complete_cold_migration()
        assert path.read_text(encoding="utf-8") == malformed
        desired = supervise.read_desired_strict()
        assert desired is not None
        assert desired.migration is True
        assert cli_calls == []
        assert daemon._message == (
            "corrupt supervised deployment state; cold-migration completion is held"
        )


def test_in_flight_migration_holds_cli_reconciliation(isolated: Path) -> None:
    """Reconciliation never targets the unproven migrated commit."""
    del isolated
    migration_intent(6, NEW)
    assert dc._cli_target_commit(None) is None
    assert dc._cli_target_commit(mission(5, dc.STATUS_CONFIRMED, OLD)) is None


def test_newer_mission_resumes_normal_reconciliation(isolated: Path) -> None:
    """A strictly newer mission supersedes the hold and owns the pointer."""
    del isolated
    migration_intent(6, NEW)
    dc._write_state(mission(7, dc.STATUS_CONFIRMED, NEWER))
    assert dc._cli_target_commit(dc.read_rollback_state()) == NEWER


@pytest.mark.parametrize("malformed", [1, "true", None, {}, []])
def test_present_non_boolean_migration_flag_fails_closed(malformed: object) -> None:
    """A present ``migration`` value must be a real JSON boolean or fail closed."""
    payload: dict[str, object] = {
        "schema_version": 1,
        "generation": 1,
        "commit": OLD,
        "migration": malformed,
    }
    with pytest.raises((TypeError, ValueError), match="malformed"):
        supervise.SupervisorDesired.from_dict(payload)


def test_absent_migration_flag_remains_backward_compatible_false() -> None:
    """Legacy intents without a ``migration`` key parse as migration=False."""
    desired = supervise.SupervisorDesired.from_dict({
        "schema_version": 1,
        "generation": 1,
        "commit": OLD,
    })
    assert desired.migration is False


def test_corrupt_migration_intent_holds_instead_of_trusting_meta(
    isolated: Path,
) -> None:
    """A malformed authoritative intent never falls back to live meta."""
    del isolated
    supervise.desired_path().parent.mkdir(parents=True, exist_ok=True)
    corrupt = {
        "schema_version": 1,
        "generation": 6,
        "commit": NEW,
        "repo": "/workspace/repo",
        "uv_path": "uv",
        "migration": 1,
    }
    supervise.desired_path().write_text(json.dumps(corrupt), encoding="utf-8")
    with pytest.raises(supervise.DesiredIntentError):
        supervise.read_desired_strict()
    assert dc._cli_target_commit(None) is None


def test_unreadable_intent_fails_closed(isolated: Path) -> None:
    """A present but unreadable authority file is corruption, not absence."""
    del isolated
    desired_dir = supervise.desired_path()
    desired_dir.parent.mkdir(parents=True, exist_ok=True)
    desired_dir.mkdir()  # a directory where the authority file must be
    with pytest.raises(supervise.DesiredIntentError):
        supervise.read_desired_strict()
    assert dc._cli_target_commit(None) is None


def test_genuinely_absent_intent_still_reconciles_to_live_meta(
    isolated: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """True absence stays backward compatible with live-worker reconciliation."""
    del isolated
    monkeypatch.setattr(dc, "read_meta", lambda: meta(NEW, 300))
    assert supervise.read_desired_strict() is None
    assert supervise.read_desired() is None
    assert dc._cli_target_commit(None) == NEW
