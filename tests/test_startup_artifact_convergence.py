"""Crash-safe startup-artifact convergence invariants.

Verifies that repository-owned startup artifacts (contract, launcher, definition)
are atomically staged by candidate B, promoted only at the confirmation boundary,
and idempotently retried by the supervisor.  Stale staging from a different commit
is never promoted.  Rollback retains the snapshot until all artifacts are restored.
"""

from __future__ import annotations

import json
from contextlib import nullcontext
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from lubko import cli, lifecycle, supervise, supervisor
from lubko import deployctl as dc
from lubko import startup_contract as sc
from lubko import state as _state_mod
from lubko.deployctl import STATUS_CONFIRMED, RollbackState
from lubko.durable import (
    FSYNC_STAGE_DIR,
    DurabilityError,
    set_one_shot_fsync_failure_injector,
)
from lubko.lifecycle import SCHEMA_VERSION, STATE_RUNNING, WorkerMeta
from lubko.startup_contract import (
    CONTRACT_SCHEMA_VERSION,
    CURRENT_CONTRACT,
    STARTUP_DEFINITION_SCHEMA_VERSION,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

STALE_CONTRACT = {
    "schema_version": CONTRACT_SCHEMA_VERSION,
    "init_command": ["sleep"],
    "supervisor_command": ["infinity"],
    "required_state_dirs": ["old"],
    "required_config_files": [],
    "required_environment": [],
}

STALE_DEFINITION = {
    "schema_version": STARTUP_DEFINITION_SCHEMA_VERSION,
    "command": ["sleep", "infinity"],
    "required_state_dirs": ["old"],
    "required_config_files": [],
    "required_environment": [],
}


def _write_stale_contract(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(STALE_CONTRACT), encoding="utf-8")


def _write_stale_definition(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(STALE_DEFINITION), encoding="utf-8")


def _setup(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    monkeypatch.setattr(sc, "state_root", lambda: tmp_path)
    monkeypatch.setattr(dc, "state_root", lambda: tmp_path)
    monkeypatch.setattr(_state_mod, "state_root", lambda: tmp_path)
    for name in CURRENT_CONTRACT.required_state_dirs:
        (tmp_path / name).mkdir(mode=0o700, exist_ok=True)
    (tmp_path / "deploy").mkdir(mode=0o700, exist_ok=True)
    bin_home = tmp_path / "bin"
    bin_home.mkdir(mode=0o700)
    return bin_home


def _manifest_path(tmp_path: Path) -> Path:
    return tmp_path / "deploy" / "staging-manifest.json"


def _fake_meta() -> RollbackState:
    """Return a minimal RollbackState for testing confirmed fast path."""
    return RollbackState(
        schema_version=4,
        generation=1,
        status=STATUS_CONFIRMED,
        commit="commit-B",
        previous_commit="commit-A",
        deadline=999999.0,
        repo="/repo",
        uv_path="uv",
        stop_grace_seconds=5.0,
        git_timeout_seconds=10.0,
        previous_retiring=False,
        previous_meta=type("M", (), {"to_dict": lambda _s: {}, "commit": "commit-A"})(),
        new_meta=None,
        supervisor_owned=True,
    )


def _raise_os_error(*_a: object, **_k: object) -> str:
    """Callable that always raises OSError.

    Raises:
        OSError: Always.
    """
    msg = "no home"
    raise OSError(msg)


# ---------------------------------------------------------------------------
# Manifest validation
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("supervisor_token")
def test_manifest_well_formed_rejects_missing_fields(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A manifest missing required fields is rejected as corrupt."""
    bin_home = _setup(monkeypatch, tmp_path)
    sc.write_contract()
    sc.write_startup_definition()
    sc.write_startup_launcher(bin_home)
    sc.stage_startup_artifacts(bin_home)
    _manifest_path(tmp_path).write_text(
        json.dumps({"commit": "abc", "contract_hash": "sha256:x"}), encoding="utf-8"
    )
    result = sc.promote_staged_artifacts("abc", "abc", bin_home)
    assert result is not None
    assert "corrupt" in result


@pytest.mark.usefixtures("supervisor_token")
def test_manifest_rejects_wrong_commit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Staging for commit-B is not promoted when commit-C is requested."""
    bin_home = _setup(monkeypatch, tmp_path)
    sc.write_contract()
    sc.write_startup_definition()
    sc.write_startup_launcher(bin_home)
    sc.stage_startup_artifacts(bin_home)
    sc.write_staging_manifest("commit-B", bin_home)
    result = sc.promote_staged_artifacts("commit-C", "commit-C", bin_home)
    assert result is not None
    assert "does not match" in result


@pytest.mark.usefixtures("supervisor_token")
def test_manifest_rejects_commit_mismatch_with_confirmed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Staging for commit-B is not promoted when commit-C is confirmed."""
    bin_home = _setup(monkeypatch, tmp_path)
    sc.write_contract()
    sc.write_startup_definition()
    sc.write_startup_launcher(bin_home)
    sc.stage_startup_artifacts(bin_home)
    sc.write_staging_manifest("commit-B", bin_home)
    result = sc.promote_staged_artifacts("commit-B", "commit-C", bin_home)
    assert result is not None
    assert "does not match confirmed" in result


# ---------------------------------------------------------------------------
# Staging / promotion round-trip
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("supervisor_token")
def test_promote_all_three_artifacts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """All three artifacts are promoted from staging to active paths."""
    bin_home = _setup(monkeypatch, tmp_path)
    sc.write_contract()
    sc.write_startup_definition()
    sc.write_startup_launcher(bin_home)
    sc.stage_startup_artifacts(bin_home)
    sc.write_staging_manifest("commit-B", bin_home)
    result = sc.promote_staged_artifacts("commit-B", "commit-B", bin_home)
    assert result is None
    assert sc.assess_recorded_contract().state == "current"
    assert sc.validate_startup_launcher(bin_home) is True
    assert sc.validate_startup_definition().ok is True
    # Manifest retained after promotion (caller cleans up after receipt)
    assert sc.read_staging_manifest() is not None
    sc.cleanup_staging(bin_home)
    assert not _manifest_path(tmp_path).is_file()


@pytest.mark.usefixtures("supervisor_token")
def test_promote_is_idempotent(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Artifacts remain correct after promotion; manifest is cleaned up."""
    bin_home = _setup(monkeypatch, tmp_path)
    sc.write_contract()
    sc.write_startup_definition()
    sc.write_startup_launcher(bin_home)
    sc.stage_startup_artifacts(bin_home)
    sc.write_staging_manifest("commit-B", bin_home)
    assert sc.promote_staged_artifacts("commit-B", "commit-B", bin_home) is None
    assert sc.assess_recorded_contract().state == "current"
    assert sc.validate_startup_launcher(bin_home) is True
    assert sc.validate_startup_definition().ok is True


@pytest.mark.usefixtures("supervisor_token")
def test_promote_verifies_launcher_executable_mode(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Promoted launcher has the required executable mode."""
    bin_home = _setup(monkeypatch, tmp_path)
    sc.write_contract()
    sc.write_startup_definition()
    sc.write_startup_launcher(bin_home)
    sc.stage_startup_artifacts(bin_home)
    sc.write_staging_manifest("commit-B", bin_home)
    assert sc.promote_staged_artifacts("commit-B", "commit-B", bin_home) is None
    launcher = bin_home / sc.STARTUP_LAUNCHER_NAME
    assert launcher.stat().st_mode & 0o777 == sc.STARTUP_LAUNCHER_MODE


# ---------------------------------------------------------------------------
# Stale staging from C not promoted when B is confirmed
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("supervisor_token")
def test_stale_staging_from_c_not_promoted_for_b(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Staging for commit-C is never promoted when commit-B is confirmed."""
    bin_home = _setup(monkeypatch, tmp_path)
    sc.write_contract()
    sc.write_startup_definition()
    sc.write_startup_launcher(bin_home)
    sc.stage_startup_artifacts(bin_home)
    sc.write_staging_manifest("commit-C", bin_home)
    result = sc.promote_staged_artifacts("commit-C", "commit-B", bin_home)
    assert result is not None
    assert "does not match confirmed" in result


@pytest.mark.usefixtures("supervisor_token")
def test_corrupt_manifest_never_yields_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A corrupt manifest is rejected and never yields promotion success."""
    bin_home = _setup(monkeypatch, tmp_path)
    _manifest_path(tmp_path).write_text("not json", encoding="utf-8")
    result = sc.promote_staged_artifacts("commit-B", "commit-B", bin_home)
    assert result is not None


# ---------------------------------------------------------------------------
# Supervisor A cannot revert confirmed B artifacts
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("supervisor_token")
def test_supervisor_validation_only_does_not_overwrite_b(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Supervisor validation reports mismatch but never writes from own code."""
    bin_home = _setup(monkeypatch, tmp_path)
    _write_stale_contract(sc.contract_path())
    _write_stale_definition(sc.startup_definition_path())
    error = sc.validate_startup_artifacts(bin_home)
    assert error is not None
    assert "startup contract" in error
    assert sc.assess_recorded_contract().state == "mismatch"


# ---------------------------------------------------------------------------
# Rollback: snapshot retained on failure, deleted on success
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("supervisor_token")
def test_rollback_restores_all_artifacts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Rollback restores contract, definition, and launcher from snapshot."""
    bin_home = _setup(monkeypatch, tmp_path)
    sc.write_contract()
    sc.write_startup_definition()
    sc.write_startup_launcher(bin_home)
    snapshot = sc.snapshot_startup_artifacts(bin_home)
    snapshot_path = tmp_path / "deploy" / "pre-confirmation-startup-artifacts.json"
    snapshot_path.write_text(json.dumps(snapshot), encoding="utf-8")
    _write_stale_contract(sc.contract_path())
    _write_stale_definition(sc.startup_definition_path())
    restored = dc._restore_pre_confirmation_artifacts(bin_home)
    assert restored is True
    assert sc.assess_recorded_contract().state == "current"
    assert sc.validate_startup_launcher(bin_home) is True


@pytest.mark.usefixtures("supervisor_token")
def test_rollback_retains_snapshot_on_corrupt_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Snapshot is retained when the file is corrupt and restore cannot run."""
    _setup(monkeypatch, tmp_path)
    snapshot_path = tmp_path / "deploy" / "pre-confirmation-startup-artifacts.json"
    snapshot_path.write_text("{bad json", encoding="utf-8")
    restored = dc._restore_pre_confirmation_artifacts(tmp_path / "bin")
    assert restored is False
    assert snapshot_path.is_file()


# ---------------------------------------------------------------------------
# Confirmation flow: promotion failure = non-success
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("supervisor_token")
def test_confirmed_idempotent_response_fails_on_stale_artifacts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Fast path returns ok:false when artifacts are stale and promotion fails."""
    bin_home = _setup(monkeypatch, tmp_path)
    monkeypatch.setattr(lifecycle, "_resolve_bin_home", lambda: bin_home)
    state = _fake_meta()
    response = dc._confirmed_idempotent_response(state)
    assert response["ok"] is False
    assert "promotion incomplete" in str(response.get("error", ""))


@pytest.mark.usefixtures("supervisor_token")
def test_confirmed_idempotent_fails_on_unresolvable_bin_home(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fast path fails closed when bin_home cannot be resolved."""
    monkeypatch.setattr(lifecycle, "_resolve_bin_home", _raise_os_error)
    state = _fake_meta()
    response = dc._confirmed_idempotent_response(state)
    assert response["ok"] is False
    assert "bin home" in str(response.get("error", ""))


# ---------------------------------------------------------------------------
# Crash recovery: supervisor can retry promotion
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("supervisor_token")
def test_supervisor_promotes_staged_bytes_without_synthesizing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Supervisor A promotes opaque staged B bytes — never generates from own code."""
    bin_home = _setup(monkeypatch, tmp_path)
    sc.write_contract()
    sc.write_startup_definition()
    sc.write_startup_launcher(bin_home)
    sc.stage_startup_artifacts(bin_home)
    sc.write_staging_manifest("commit-B", bin_home)
    _write_stale_contract(sc.contract_path())
    _write_stale_definition(sc.startup_definition_path())
    assert sc.assess_recorded_contract().state == "mismatch"
    error = sc.promote_staged_artifacts("commit-B", "commit-B", bin_home)
    assert error is None
    assert sc.assess_recorded_contract().state == "current"


# ---------------------------------------------------------------------------
# Post-terminalization exception retains recovery data
# ---------------------------------------------------------------------------


def _confirm_opts() -> dc.Options:
    return dc.Options(
        repo=Path("/repo"),
        uv_path="uv",
        confirm_window_seconds=120.0,
        stop_grace_seconds=5.0,
        postgres_timeout_seconds=5.0,
        lock_timeout_seconds=30.0,
        validation_timeout_seconds=1200.0,
        git_timeout_seconds=10.0,
        cli_timeout_seconds=30.0,
    )


@pytest.mark.usefixtures("supervisor_token")
def test_post_terminalization_exception_retains_recovery_data(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Invoke _confirm_locked; cause durable STATUS_CONFIRMED then raise.

    Proves that after _finalize_supervised_confirmation durably writes
    STATUS_CONFIRMED, a later exception (e.g. cli.gc_cli_roots) retains
    the staging manifest and pre-confirmation snapshot.  Retry via the
    STATUS_CONFIRMED fast path then proves convergence.
    """
    bin_home = _setup(monkeypatch, tmp_path)
    # Write current A artifacts and take a snapshot
    sc.write_contract()
    sc.write_startup_definition()
    sc.write_startup_launcher(bin_home)
    snapshot = sc.snapshot_startup_artifacts(bin_home)
    snap_path = tmp_path / "deploy" / "pre-confirmation-startup-artifacts.json"
    snap_path.write_text(json.dumps(snapshot), encoding="utf-8")
    # Stage real B artifacts
    sc.stage_startup_artifacts(bin_home)
    sc.write_staging_manifest("b" * 40, bin_home)
    manifest_path = _manifest_path(tmp_path)
    assert manifest_path.is_file(), "manifest must exist before confirmation"

    # Mutable current_state: _confirmation_state and read_rollback_state return it;
    # _write_state transitions it.
    commit_b = "b" * 40
    commit_a = "a" * 40
    current_state = dc.RollbackState(
        schema_version=4,
        generation=1,
        status=dc.STATUS_PENDING,
        commit=commit_b,
        previous_commit=commit_a,
        deadline=999999.0,
        repo="/repo",
        uv_path="uv",
        stop_grace_seconds=5.0,
        git_timeout_seconds=10.0,
        previous_retiring=False,
        previous_meta=type("M", (), {"to_dict": lambda _s: {}, "commit": commit_a})(),
        new_meta=None,
        supervisor_owned=True,
    )

    def _get_state(_req: object) -> dc.RollbackState:
        return current_state

    def _write_mutable(s: dc.RollbackState) -> None:
        nonlocal current_state
        current_state = s

    def _read_state() -> dc.RollbackState | None:
        return current_state

    monkeypatch.setattr(dc, "_confirmation_state", _get_state)
    monkeypatch.setattr(dc, "_authorize_confirmation", lambda _s: None)
    monkeypatch.setattr(dc, "_prepare_confirmation_candidate", lambda _s, _o: 1)
    # Staging already done manually; skip the B-CLI subprocess call
    monkeypatch.setattr(dc, "_stage_candidate_startup_artifacts", lambda _c: None)
    monkeypatch.setattr(dc, "_write_state", _write_mutable)
    monkeypatch.setattr(dc, "read_rollback_state", _read_state)
    monkeypatch.setattr(supervise, "generation_lock", nullcontext)
    monkeypatch.setattr(
        supervise,
        "read_desired_strict",
        lambda: SimpleNamespace(commit=commit_b, generation=1),
    )
    monkeypatch.setattr(
        supervise,
        "read_status",
        lambda: SimpleNamespace(
            applied_generation=1,
            commit=commit_b,
            ready=True,
            holding=False,
            child=SimpleNamespace(marker="child"),
        ),
    )
    monkeypatch.setattr(supervise, "supervisor_running", lambda: True)
    monkeypatch.setattr(dc, "_supervised_terminalization_authority_matches", lambda *_a: True)
    monkeypatch.setattr(cli, "set_current", lambda _c: None)
    monkeypatch.setattr(lifecycle, "_resolve_bin_home", lambda: bin_home)
    # cli.gc_cli_roots raises AFTER _write_state(terminal) has durably written
    monkeypatch.setattr(cli, "gc_cli_roots", lambda _c: (_ for _ in ()).throw(OSError("boom")))
    monkeypatch.setattr(dc, "append_deploy_log", lambda _l: None)

    # First call: terminalization succeeds, then gc_cli_roots raises
    with pytest.raises(OSError, match="boom"):
        dc._confirm_locked({"type": "confirm", "commit": commit_b}, _confirm_opts())

    # The mutable state was transitioned to STATUS_CONFIRMED by _write_state
    assert current_state.status == dc.STATUS_CONFIRMED
    # Recovery data retained: manifest + snapshot survive the exception
    assert manifest_path.is_file(), "manifest must survive post-terminalization exception"
    assert snap_path.is_file(), "snapshot must survive post-terminalization exception"

    # Disable the fault: gc_cli_roots now succeeds
    monkeypatch.setattr(cli, "gc_cli_roots", lambda _c: None)

    # Second call: STATUS_CONFIRMED fast path retries idempotent promotion
    response = dc._confirm_locked({"type": "confirm", "commit": commit_b}, _confirm_opts())
    assert response["ok"] is True
    assert response["confirmed"] is True
    # Snapshot cleaned up after successful convergence
    assert not snap_path.is_file(), "snapshot removed after successful convergence"


# ---------------------------------------------------------------------------
# First-install rollback: absence restored
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("supervisor_token")
def test_first_install_rollback_removes_artifacts_that_were_absent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """On first install, snapshot records absence; rollback removes B's artifacts."""
    bin_home = _setup(monkeypatch, tmp_path)
    assert not sc.contract_path().is_file()
    # Snapshot BEFORE artifacts exist — records absence as None
    snapshot = sc.snapshot_startup_artifacts(bin_home)
    assert snapshot["contract"] is None
    assert snapshot["definition"] is None
    assert snapshot["launcher"] is None
    # B installs its artifacts
    sc.write_contract()
    sc.write_startup_definition()
    sc.write_startup_launcher(bin_home)
    assert sc.assess_recorded_contract().state == "current"
    # Rollback restores absence — durably removes B's artifacts
    sc.restore_startup_artifacts(snapshot, bin_home)
    assert not sc.contract_path().is_file(), "contract removed on rollback to absence"
    assert not sc.startup_definition_path().is_file(), "definition removed on rollback to absence"
    assert not (bin_home / sc.STARTUP_LAUNCHER_NAME).is_file(), (
        "launcher removed on rollback to absence"
    )


# ---------------------------------------------------------------------------
# Malformed snapshot bytes parsing
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("supervisor_token")
def test_read_pre_confirmation_snapshot_rejects_malformed_data(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A snapshot with invalid values is rejected."""
    _setup(monkeypatch, tmp_path)
    snap_path = tmp_path / "deploy" / "pre-confirmation-startup-artifacts.json"
    # Out-of-range byte value
    snap_path.write_text(
        json.dumps({"contract": [0, 300], "definition": None, "launcher": None}),
        encoding="utf-8",
    )
    assert dc._read_pre_confirmation_snapshot() is None
    # Non-integer values
    snap_path.write_text(
        json.dumps({"contract": ["not", "integers"], "definition": None, "launcher": None}),
        encoding="utf-8",
    )
    assert dc._read_pre_confirmation_snapshot() is None
    # Boolean values (which are ints in Python)
    snap_path.write_text(
        json.dumps({"contract": [True, False], "definition": None, "launcher": None}),
        encoding="utf-8",
    )
    assert dc._read_pre_confirmation_snapshot() is None
    # Valid snapshot with None (absent) and list (present)
    snap_path.write_text(
        json.dumps({"contract": [0, 1, 2], "definition": None, "launcher": [4, 5]}),
        encoding="utf-8",
    )
    result = dc._read_pre_confirmation_snapshot()
    assert result is not None
    assert result["contract"] == [0, 1, 2]
    assert result["definition"] is None
    assert result["launcher"] == [4, 5]


@pytest.mark.usefixtures("supervisor_token")
def test_read_pre_confirmation_snapshot_rejects_missing_key(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A snapshot missing a required key is malformed."""
    _setup(monkeypatch, tmp_path)
    snap_path = tmp_path / "deploy" / "pre-confirmation-startup-artifacts.json"
    # Missing 'launcher' key
    snap_path.write_text(
        json.dumps({"contract": [0], "definition": None}),
        encoding="utf-8",
    )
    assert dc._read_pre_confirmation_snapshot() is None
    # Empty dict
    snap_path.write_text(json.dumps({}), encoding="utf-8")
    assert dc._read_pre_confirmation_snapshot() is None


@pytest.mark.usefixtures("supervisor_token")
def test_read_pre_confirmation_snapshot_rejects_unknown_key(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A snapshot with an unknown key is malformed."""
    _setup(monkeypatch, tmp_path)
    snap_path = tmp_path / "deploy" / "pre-confirmation-startup-artifacts.json"
    # Extra unknown key
    snap_path.write_text(
        json.dumps({
            "contract": [0],
            "definition": None,
            "launcher": None,
            "extra_key": [1, 2],
        }),
        encoding="utf-8",
    )
    assert dc._read_pre_confirmation_snapshot() is None


@pytest.mark.usefixtures("supervisor_token")
def test_snapshot_propagates_read_error_on_existing_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If an existing artifact cannot be read, OSError propagates."""
    bin_home = _setup(monkeypatch, tmp_path)
    # Create a contract file that exists on disk
    good_contract = sc.contract_path()
    good_contract.write_bytes(b"content")
    # Monkeypatch Path.read_bytes to fail only for the contract path
    original_read = Path.read_bytes

    def _selective_read(self: Path) -> bytes:
        if self == good_contract:
            msg = "Permission denied"
            raise OSError(msg)
        return original_read(self)

    monkeypatch.setattr(Path, "read_bytes", _selective_read)
    with pytest.raises(OSError, match="Permission denied"):
        sc.snapshot_startup_artifacts(bin_home)
    # Contract still exists — was not silently deleted
    assert good_contract.is_file()


# ---------------------------------------------------------------------------
# Snapshot write failure prevents terminalization
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("supervisor_token")
def test_snapshot_write_failure_prevents_terminalization(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Snapshot write failure prevents terminalization.

    If the pre-confirmation snapshot cannot be durably recorded, confirmation
    fails before terminalization so the mission remains pending.
    """
    bin_home = _setup(monkeypatch, tmp_path)
    sc.write_contract()
    sc.write_startup_definition()
    sc.write_startup_launcher(bin_home)
    sc.stage_startup_artifacts(bin_home)
    sc.write_staging_manifest("b" * 40, bin_home)

    current_state = dc.RollbackState(
        schema_version=4,
        generation=1,
        status=dc.STATUS_PENDING,
        commit="b" * 40,
        previous_commit="a" * 40,
        deadline=999999.0,
        repo="/repo",
        uv_path="uv",
        stop_grace_seconds=5.0,
        git_timeout_seconds=10.0,
        previous_retiring=False,
        previous_meta=type("M", (), {"to_dict": lambda _s: {}, "commit": "a" * 40})(),
        new_meta=None,
        supervisor_owned=True,
    )

    def _get_state(_req: object) -> dc.RollbackState:
        return current_state

    def _fail_snapshot(_b: Path) -> None:
        msg = "disk full"
        raise DurabilityError(msg)

    monkeypatch.setattr(dc, "_confirmation_state", _get_state)
    monkeypatch.setattr(dc, "_snapshot_pre_confirmation_artifacts", _fail_snapshot)
    monkeypatch.setattr(dc, "_stage_candidate_startup_artifacts", lambda _c: None)
    monkeypatch.setattr(dc, "_prepare_confirmation_candidate", lambda _s, _o: 1)
    monkeypatch.setattr(dc, "_authorize_confirmation", lambda _s: None)
    monkeypatch.setattr(dc, "append_deploy_log", lambda _l: None)
    monkeypatch.setattr(lifecycle, "_resolve_bin_home", lambda: bin_home)

    with pytest.raises(dc.DeployCtlError, match="snapshot"):
        dc._confirm_locked({"type": "confirm", "commit": "b" * 40}, _confirm_opts())

    assert current_state.status == dc.STATUS_PENDING


# ---------------------------------------------------------------------------
# Supervisor maintained recovery: genuine reconciliation path
# ---------------------------------------------------------------------------


def _make_confirmed_mission(commit: str) -> RollbackState:
    """Build a minimal confirmed rollback state for testing.

    Returns:
        A RollbackState with STATUS_CONFIRMED for the given commit.
    """
    previous_meta = WorkerMeta(
        schema_version=SCHEMA_VERSION,
        state=STATE_RUNNING,
        pid=1,
        pgid=1,
        sid=1,
        start_time_ticks=10,
        token="a" * 32,
        repo="/repo",
        git_commit="a" * 40,
        worker_id="w",
        log_path="/l",
        started_at=1.0,
        stopped_at=None,
    )
    return RollbackState(
        schema_version=4,
        generation=1,
        status=STATUS_CONFIRMED,
        commit=commit,
        previous_commit="a" * 40,
        deadline=999999.0,
        repo="/repo",
        uv_path="uv",
        stop_grace_seconds=5.0,
        git_timeout_seconds=10.0,
        previous_retiring=False,
        previous_meta=previous_meta,
        new_meta=None,
        supervisor_owned=True,
    )


@pytest.mark.usefixtures("supervisor_token")
def test_supervisor_reconcile_promotes_b_when_b_confirmed_and_staging_retained(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Strongest crash boundary: durable B confirmed, active artifacts still wholly A.

    B manifest/staging retained.  One Supervisor reconcile call must promote B
    without explicit confirm retry.  The supervisor must NOT gate on
    validate_startup_artifacts (which would see A as current and skip).
    Recovery is driven by durable authority only: STATUS_CONFIRMED + manifest
    bound to mission.commit.
    """
    bin_home = _setup(monkeypatch, tmp_path)
    # Write A artifacts (simulating old supervisor A's code)
    sc.write_contract()
    sc.write_startup_definition()
    sc.write_startup_launcher(bin_home)
    assert sc.assess_recorded_contract().state == "current"

    # Stage B artifacts with manifest bound to commit-B
    commit_b = "b" * 40
    sc.stage_startup_artifacts(bin_home)
    sc.write_staging_manifest(commit_b, bin_home)
    assert sc.read_staging_manifest() is not None

    # Overwrite active artifacts with stale A versions to simulate the crash boundary
    _write_stale_contract(sc.contract_path())
    _write_stale_definition(sc.startup_definition_path())
    assert sc.assess_recorded_contract().state == "mismatch"

    # Write durable confirmed mission for commit-B
    mission = _make_confirmed_mission(commit_b)
    dc._write_state(mission)

    # Set up SupervisorDaemon — do NOT mock _converge_startup_artifacts
    desired_ns = SimpleNamespace(commit=commit_b, generation=2, restart=False)
    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    monkeypatch.setattr(
        supervise,
        "read_desired_strict",
        lambda: desired_ns,
    )
    monkeypatch.setattr(cli, "current_commit", lambda: commit_b)
    monkeypatch.setattr(cli, "runtime_is_usable", lambda _c: False)
    applied_state = replace(supervise.SupervisorState.from_dict({}), applied_generation=2)
    monkeypatch.setattr(supervisor, "read_state", lambda: applied_state)
    monkeypatch.setattr(daemon, "_ensure_worker", lambda _c: None)
    monkeypatch.setattr(daemon, "_maybe_reset_backoff", lambda _s, _n: None)
    monkeypatch.setattr(daemon, "_record_mission_progress", lambda _c: None)
    monkeypatch.setattr(daemon, "_probe_readiness", lambda _n: None)
    monkeypatch.setattr(daemon, "_complete_cold_migration", lambda: None)
    monkeypatch.setattr(lifecycle, "_resolve_bin_home", lambda: bin_home)

    daemon.reconcile(0.0)

    # B artifacts must have been promoted — A artifacts overwritten
    assert sc.assess_recorded_contract().state == "current"
    # Manifest retained after promotion (deployctl cleans up after receipt)
    assert sc.read_staging_manifest() is not None


@pytest.mark.usefixtures("supervisor_token")
def test_supervisor_reconcile_skips_when_no_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No manifest: supervisor does not mutate artifacts or infer B staleness."""
    bin_home = _setup(monkeypatch, tmp_path)
    sc.write_contract()
    sc.write_startup_definition()
    sc.write_startup_launcher(bin_home)
    assert sc.assess_recorded_contract().state == "current"

    commit_b = "b" * 40
    mission = _make_confirmed_mission(commit_b)
    dc._write_state(mission)

    desired_ns = SimpleNamespace(commit=commit_b, generation=2, restart=False)
    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    monkeypatch.setattr(
        supervise,
        "read_desired_strict",
        lambda: desired_ns,
    )
    monkeypatch.setattr(cli, "current_commit", lambda: commit_b)
    monkeypatch.setattr(cli, "runtime_is_usable", lambda _c: False)
    applied_state = replace(supervise.SupervisorState.from_dict({}), applied_generation=2)
    monkeypatch.setattr(supervisor, "read_state", lambda: applied_state)
    monkeypatch.setattr(daemon, "_ensure_worker", lambda _c: None)
    monkeypatch.setattr(daemon, "_maybe_reset_backoff", lambda _s, _n: None)
    monkeypatch.setattr(daemon, "_record_mission_progress", lambda _c: None)
    monkeypatch.setattr(daemon, "_probe_readiness", lambda _n: None)
    monkeypatch.setattr(daemon, "_complete_cold_migration", lambda: None)
    monkeypatch.setattr(lifecycle, "_resolve_bin_home", lambda: bin_home)

    daemon.reconcile(0.0)

    # Active artifacts unchanged — no manifest means no promotion
    assert sc.assess_recorded_contract().state == "current"


@pytest.mark.usefixtures("supervisor_token")
def test_supervisor_reconcile_skips_wrong_commit_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Wrong-commit manifest: supervisor does not mutate artifacts."""
    bin_home = _setup(monkeypatch, tmp_path)
    sc.write_contract()
    sc.write_startup_definition()
    sc.write_startup_launcher(bin_home)

    commit_b = "b" * 40
    # Stage with wrong commit (commit-C ≠ mission commit-B)
    sc.stage_startup_artifacts(bin_home)
    sc.write_staging_manifest("c" * 40, bin_home)

    mission = _make_confirmed_mission(commit_b)
    dc._write_state(mission)

    desired_ns = SimpleNamespace(commit=commit_b, generation=2, restart=False)
    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    monkeypatch.setattr(
        supervise,
        "read_desired_strict",
        lambda: desired_ns,
    )
    monkeypatch.setattr(cli, "current_commit", lambda: commit_b)
    monkeypatch.setattr(cli, "runtime_is_usable", lambda _c: False)
    applied_state = replace(supervise.SupervisorState.from_dict({}), applied_generation=2)
    monkeypatch.setattr(supervisor, "read_state", lambda: applied_state)
    monkeypatch.setattr(daemon, "_ensure_worker", lambda _c: None)
    monkeypatch.setattr(daemon, "_maybe_reset_backoff", lambda _s, _n: None)
    monkeypatch.setattr(daemon, "_record_mission_progress", lambda _c: None)
    monkeypatch.setattr(daemon, "_probe_readiness", lambda _n: None)
    monkeypatch.setattr(daemon, "_complete_cold_migration", lambda: None)
    monkeypatch.setattr(lifecycle, "_resolve_bin_home", lambda: bin_home)

    daemon.reconcile(0.0)

    # Active artifacts unchanged — wrong-commit manifest was not promoted
    assert sc.assess_recorded_contract().state == "current"
    # Staging manifest still present (promotion rejected, not cleaned up)
    assert sc.read_staging_manifest() is not None


# ---------------------------------------------------------------------------
# Repeat confirm idempotency via durable receipt
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("supervisor_token")
def test_repeat_confirm_after_successful_promotion_returns_ok_true(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Repeat confirm of a fully-promoted deployment returns ok:true.

    After the first confirm succeeds, the staging manifest is removed.
    A second confirm must still return ok:true by checking the durable
    confirmation receipt rather than failing on missing manifest.
    """
    bin_home = _setup(monkeypatch, tmp_path)
    sc.write_contract()
    sc.write_startup_definition()
    sc.write_startup_launcher(bin_home)
    sc.stage_startup_artifacts(bin_home)
    sc.write_staging_manifest("b" * 40, bin_home)

    commit_b = "b" * 40
    current_state = dc.RollbackState(
        schema_version=4,
        generation=1,
        status=dc.STATUS_CONFIRMED,
        commit=commit_b,
        previous_commit="a" * 40,
        deadline=999999.0,
        repo="/repo",
        uv_path="uv",
        stop_grace_seconds=5.0,
        git_timeout_seconds=10.0,
        previous_retiring=False,
        previous_meta=type("M", (), {"to_dict": lambda _s: {}, "commit": "a" * 40})(),
        new_meta=None,
        supervisor_owned=True,
    )

    monkeypatch.setattr(dc, "_confirmation_state", lambda _r: current_state)
    monkeypatch.setattr(lifecycle, "_resolve_bin_home", lambda: bin_home)

    # First confirm: promotion succeeds, receipt written
    response = dc._confirm_locked({"type": "confirm", "commit": commit_b}, _confirm_opts())
    assert response["ok"] is True
    assert sc.read_staging_manifest() is None
    receipt = dc._read_confirmation_receipt()
    assert receipt is not None
    assert receipt.get("commit") == commit_b

    # Second confirm: receipt found, active artifacts verified, returns ok:true
    response2 = dc._confirm_locked({"type": "confirm", "commit": commit_b}, _confirm_opts())
    assert response2["ok"] is True
    assert response2["confirmed"] is True


@pytest.mark.usefixtures("supervisor_token")
def test_repeat_confirm_wrong_commit_receipt_is_fail_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Receipt for a different commit does not grant success."""
    bin_home = _setup(monkeypatch, tmp_path)
    sc.write_contract()
    sc.write_startup_definition()
    sc.write_startup_launcher(bin_home)

    # Write receipt for a different commit
    dc._write_confirmation_receipt("x" * 40, {"commit": "x" * 40})

    current_state = dc.RollbackState(
        schema_version=4,
        generation=1,
        status=dc.STATUS_CONFIRMED,
        commit="b" * 40,
        previous_commit="a" * 40,
        deadline=999999.0,
        repo="/repo",
        uv_path="uv",
        stop_grace_seconds=5.0,
        git_timeout_seconds=10.0,
        previous_retiring=False,
        previous_meta=type("M", (), {"to_dict": lambda _s: {}, "commit": "a" * 40})(),
        new_meta=None,
        supervisor_owned=True,
    )
    monkeypatch.setattr(dc, "_confirmation_state", lambda _r: current_state)
    monkeypatch.setattr(lifecycle, "_resolve_bin_home", lambda: bin_home)

    response = dc._confirm_locked({"type": "confirm", "commit": "b" * 40}, _confirm_opts())
    assert response["ok"] is False


@pytest.mark.usefixtures("supervisor_token")
def test_corrupt_confirmation_receipt_is_fail_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Corrupt receipt is treated as absent — does not grant false success."""
    bin_home = _setup(monkeypatch, tmp_path)
    sc.write_contract()
    sc.write_startup_definition()
    sc.write_startup_launcher(bin_home)

    # Write corrupt receipt
    dc._confirmation_receipt_path().write_text("not json", encoding="utf-8")

    current_state = dc.RollbackState(
        schema_version=4,
        generation=1,
        status=dc.STATUS_CONFIRMED,
        commit="b" * 40,
        previous_commit="a" * 40,
        deadline=999999.0,
        repo="/repo",
        uv_path="uv",
        stop_grace_seconds=5.0,
        git_timeout_seconds=10.0,
        previous_retiring=False,
        previous_meta=type("M", (), {"to_dict": lambda _s: {}, "commit": "a" * 40})(),
        new_meta=None,
        supervisor_owned=True,
    )
    monkeypatch.setattr(dc, "_confirmation_state", lambda _r: current_state)
    monkeypatch.setattr(lifecycle, "_resolve_bin_home", lambda: bin_home)

    # Corrupt receipt treated as absent — confirm proceeds but promotion
    # fails (no staging manifest), so ok:false
    response = dc._confirm_locked({"type": "confirm", "commit": "b" * 40}, _confirm_opts())
    assert response["ok"] is False


@pytest.mark.usefixtures("supervisor_token")
def test_rollback_removes_confirmation_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Rollback removes the confirmation receipt so stale receipts don't persist."""
    bin_home = _setup(monkeypatch, tmp_path)
    sc.write_contract()
    sc.write_startup_definition()
    sc.write_startup_launcher(bin_home)

    # Write a receipt with content authority
    dc._write_confirmation_receipt("b" * 40, {"commit": "b" * 40})
    assert dc._read_confirmation_receipt() is not None

    # Simulate rollback by calling the receipt removal
    dc._remove_confirmation_receipt()
    assert dc._read_confirmation_receipt() is None


@pytest.mark.usefixtures("supervisor_token")
def test_repeat_confirm_drift_returns_ok_false(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """After receipt creation, if active artifacts drift, repeat confirm returns ok:false.

    Receipt commit matches but artifacts drifted — no staging manifest to
    repair with, so promotion fails.
    """
    bin_home = _setup(monkeypatch, tmp_path)
    sc.write_contract()
    sc.write_startup_definition()
    sc.write_startup_launcher(bin_home)
    sc.stage_startup_artifacts(bin_home)
    sc.write_staging_manifest("b" * 40, bin_home)

    commit_b = "b" * 40
    current_state = dc.RollbackState(
        schema_version=4,
        generation=1,
        status=dc.STATUS_CONFIRMED,
        commit=commit_b,
        previous_commit="a" * 40,
        deadline=999999.0,
        repo="/repo",
        uv_path="uv",
        stop_grace_seconds=5.0,
        git_timeout_seconds=10.0,
        previous_retiring=False,
        previous_meta=type("M", (), {"to_dict": lambda _s: {}, "commit": "a" * 40})(),
        new_meta=None,
        supervisor_owned=True,
    )
    monkeypatch.setattr(dc, "_confirmation_state", lambda _r: current_state)
    monkeypatch.setattr(lifecycle, "_resolve_bin_home", lambda: bin_home)

    # First confirm: promotion succeeds, receipt written
    response = dc._confirm_locked({"type": "confirm", "commit": commit_b}, _confirm_opts())
    assert response["ok"] is True
    assert sc.read_staging_manifest() is None

    # Simulate artifact drift: overwrite contract with different content
    # (same schema version but different init_command to get "mismatch" not "corrupt")
    sc.contract_path().write_text(
        json.dumps({
            "schema_version": CONTRACT_SCHEMA_VERSION,
            "init_command": ["drifted"],
            "supervisor_command": list(CURRENT_CONTRACT.supervisor_command),
            "required_state_dirs": list(CURRENT_CONTRACT.required_state_dirs),
            "required_config_files": list(CURRENT_CONTRACT.required_config_files),
            "required_environment": list(CURRENT_CONTRACT.required_environment),
        }),
        encoding="utf-8",
    )
    assert sc.assess_recorded_contract().state == "mismatch"

    # Repeat confirm: receipt matches but artifacts drifted, no manifest to repair → ok:false
    response2 = dc._confirm_locked({"type": "confirm", "commit": commit_b}, _confirm_opts())
    assert response2["ok"] is False


@pytest.mark.usefixtures("supervisor_token")
def test_repeat_confirm_repair_via_staging_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When receipt is stale but valid staging manifest exists, promotion repairs.

    Artifacts and repeat confirm succeeds.
    """
    bin_home = _setup(monkeypatch, tmp_path)
    sc.write_contract()
    sc.write_startup_definition()
    sc.write_startup_launcher(bin_home)
    sc.stage_startup_artifacts(bin_home)
    sc.write_staging_manifest("b" * 40, bin_home)

    commit_b = "b" * 40
    current_state = dc.RollbackState(
        schema_version=4,
        generation=1,
        status=dc.STATUS_CONFIRMED,
        commit=commit_b,
        previous_commit="a" * 40,
        deadline=999999.0,
        repo="/repo",
        uv_path="uv",
        stop_grace_seconds=5.0,
        git_timeout_seconds=10.0,
        previous_retiring=False,
        previous_meta=type("M", (), {"to_dict": lambda _s: {}, "commit": "a" * 40})(),
        new_meta=None,
        supervisor_owned=True,
    )
    monkeypatch.setattr(dc, "_confirmation_state", lambda _r: current_state)
    monkeypatch.setattr(lifecycle, "_resolve_bin_home", lambda: bin_home)

    # Write receipt for wrong commit (simulating stale receipt)
    dc._write_confirmation_receipt("x" * 40, {"commit": "x" * 40})

    # Artifact drift
    sc.contract_path().write_text(
        json.dumps({
            "schema_version": CONTRACT_SCHEMA_VERSION,
            "init_command": ["drifted"],
            "supervisor_command": list(CURRENT_CONTRACT.supervisor_command),
            "required_state_dirs": list(CURRENT_CONTRACT.required_state_dirs),
            "required_config_files": list(CURRENT_CONTRACT.required_config_files),
            "required_environment": list(CURRENT_CONTRACT.required_environment),
        }),
        encoding="utf-8",
    )

    # Repeat confirm: wrong-commit receipt, drift → repair via manifest
    response = dc._confirm_locked({"type": "confirm", "commit": commit_b}, _confirm_opts())
    assert response["ok"] is True
    assert sc.assess_recorded_contract().state == "current"
    # Receipt updated to commit-B
    receipt = dc._read_confirmation_receipt()
    assert receipt is not None
    assert receipt.get("commit") == commit_b


# ---------------------------------------------------------------------------
# Crash-safety: receipt-write failure after promotion
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("supervisor_token")
def test_receipt_write_failure_after_promotion_retains_staging_for_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If receipt write fails after successful promotion, staging is retained.

    This prevents the crash-safety hole: STATUS_CONFIRMED with neither
    staging manifest nor receipt would leave repeat confirm unable to
    prove or repair startup artifacts.
    """
    bin_home = _setup(monkeypatch, tmp_path)
    sc.write_contract()
    sc.write_startup_definition()
    sc.write_startup_launcher(bin_home)
    sc.stage_startup_artifacts(bin_home)
    sc.write_staging_manifest("b" * 40, bin_home)

    commit_b = "b" * 40
    current_state = dc.RollbackState(
        schema_version=4,
        generation=1,
        status=dc.STATUS_CONFIRMED,
        commit=commit_b,
        previous_commit="a" * 40,
        deadline=999999.0,
        repo="/repo",
        uv_path="uv",
        stop_grace_seconds=5.0,
        git_timeout_seconds=10.0,
        previous_retiring=False,
        previous_meta=type("M", (), {"to_dict": lambda _s: {}, "commit": "a" * 40})(),
        new_meta=None,
        supervisor_owned=True,
    )
    monkeypatch.setattr(dc, "_confirmation_state", lambda _r: current_state)
    monkeypatch.setattr(lifecycle, "_resolve_bin_home", lambda: bin_home)

    # Make receipt write fail (simulate crash during receipt durability)
    original_receipt_write = dc._write_confirmation_receipt

    def _fail_receipt_write(_commit: str, _manifest: dict[str, object]) -> None:
        msg = "disk full"
        raise OSError(msg)

    monkeypatch.setattr(dc, "_write_confirmation_receipt", _fail_receipt_write)

    # First confirm: promotion succeeds, receipt write fails → staging retained
    response = dc._confirm_locked({"type": "confirm", "commit": commit_b}, _confirm_opts())
    assert response["ok"] is False
    # Staging manifest retained (not cleaned up because receipt write failed)
    assert sc.read_staging_manifest() is not None
    # No receipt written
    assert dc._read_confirmation_receipt() is None

    # Restore the real receipt write function, retry confirm → succeeds
    monkeypatch.setattr(dc, "_write_confirmation_receipt", original_receipt_write)
    response2 = dc._confirm_locked({"type": "confirm", "commit": commit_b}, _confirm_opts())
    assert response2["ok"] is True
    assert sc.read_staging_manifest() is None
    receipt = dc._read_confirmation_receipt()
    assert receipt is not None
    assert receipt.get("commit") == commit_b


@pytest.mark.usefixtures("supervisor_token")
def test_receipt_durability_failure_retains_staging_for_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Real DurabilityError (fsync failure) during receipt write retains staging.

    Uses the durable module's fault injector to simulate a real storage
    confirmation failure.  Proves: promotion succeeds; receipt durability
    fails; confirm returns controlled ok:false; staging manifest remains;
    receipt is not accepted as durable; clearing the fault and retrying
    succeeds, writes/verifies receipt, then cleans staging.
    """
    bin_home = _setup(monkeypatch, tmp_path)
    sc.write_contract()
    sc.write_startup_definition()
    sc.write_startup_launcher(bin_home)
    sc.stage_startup_artifacts(bin_home)
    sc.write_staging_manifest("b" * 40, bin_home)

    commit_b = "b" * 40
    current_state = dc.RollbackState(
        schema_version=4,
        generation=1,
        status=dc.STATUS_CONFIRMED,
        commit=commit_b,
        previous_commit="a" * 40,
        deadline=999999.0,
        repo="/repo",
        uv_path="uv",
        stop_grace_seconds=5.0,
        git_timeout_seconds=10.0,
        previous_retiring=False,
        previous_meta=type("M", (), {"to_dict": lambda _s: {}, "commit": "a" * 40})(),
        new_meta=None,
        supervisor_owned=True,
    )
    monkeypatch.setattr(dc, "_confirmation_state", lambda _r: current_state)
    monkeypatch.setattr(lifecycle, "_resolve_bin_home", lambda: bin_home)

    # Install one-shot fsync failure injector on the deploy directory
    # (the directory fsync fires on the parent of the receipt file)
    deploy_dir = dc._confirmation_receipt_path().parent
    set_one_shot_fsync_failure_injector(stage=FSYNC_STAGE_DIR, path=deploy_dir)

    # First confirm: promotion succeeds, receipt durability fails → staging retained
    response = dc._confirm_locked({"type": "confirm", "commit": commit_b}, _confirm_opts())
    assert response["ok"] is False
    assert "receipt" in str(response.get("error", ""))
    # Staging manifest retained
    assert sc.read_staging_manifest() is not None
    # Receipt file was NOT durably written (injector cleared after firing)
    assert not dc._confirmation_receipt_path().is_file()

    # Second confirm: fault cleared, retry succeeds
    response2 = dc._confirm_locked({"type": "confirm", "commit": commit_b}, _confirm_opts())
    assert response2["ok"] is True
    assert response2["confirmed"] is True
    # Staging cleaned up after successful receipt
    assert sc.read_staging_manifest() is None
    # Receipt durably written and verified
    receipt = dc._read_confirmation_receipt()
    assert receipt is not None
    assert receipt.get("commit") == commit_b


# ---------------------------------------------------------------------------
# Rollback artifact-restoration invariants
# ---------------------------------------------------------------------------


def _rollback_mission(
    *,
    status: str = dc.STATUS_PENDING,
    generation: int = 5,
    commit: str = "b" * 40,
    previous_commit: str = "a" * 40,
    supervisor_owned: bool = True,
) -> dc.RollbackState:
    """Return a minimal rollback mission for supervised/legacy rollback tests.

    Returns:
        A valid rollback state in the given status.
    """

    def _meta(commit_ref: str, pid: int) -> WorkerMeta:
        return WorkerMeta(
            schema_version=SCHEMA_VERSION,
            state=STATE_RUNNING,
            pid=pid,
            pgid=pid,
            sid=pid,
            start_time_ticks=pid * 10,
            token=f"tok-{pid}",
            repo="/repo",
            git_commit=commit_ref,
            worker_id="w",
            log_path="/l",
            started_at=1.0,
            stopped_at=None,
        )

    return dc.RollbackState(
        schema_version=dc.ROLLBACK_SCHEMA_VERSION,
        generation=generation,
        status=status,
        commit=commit,
        previous_commit=previous_commit,
        deadline=999999.0,
        repo="/repo",
        uv_path="uv",
        stop_grace_seconds=1.0,
        git_timeout_seconds=1.0,
        previous_retiring=False,
        previous_meta=_meta(previous_commit, 100),
        new_meta=None if supervisor_owned else _meta(commit, 200),
        supervisor_owned=supervisor_owned,
    )


def _setup_supervised_rollback(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> tuple[Path, dc.RollbackState]:
    """Set up shared state for supervised rollback tests.

    Returns:
        A tuple of (bin_home path, rollback mission state).
    """
    bin_home = _setup(monkeypatch, tmp_path)
    mission = _rollback_mission()
    child = SimpleNamespace(marker="child")
    monkeypatch.setattr(supervise, "supervisor_running", lambda: True)
    monkeypatch.setattr(supervise, "generation_lock", nullcontext)
    monkeypatch.setattr(
        supervise,
        "read_desired_strict",
        lambda: SimpleNamespace(commit=mission.previous_commit, generation=mission.generation + 1),
    )
    monkeypatch.setattr(
        supervise,
        "read_status",
        lambda: SimpleNamespace(
            commit=mission.previous_commit,
            applied_generation=mission.generation + 1,
            ready=True,
            holding=False,
            child=child,
        ),
    )
    monkeypatch.setattr(
        supervise,
        "read_state",
        lambda: SimpleNamespace(
            commit=mission.previous_commit,
            applied_generation=mission.generation + 1,
            ready=True,
            child=child,
        ),
    )
    monkeypatch.setattr(supervise, "child_alive", lambda _c: True)
    monkeypatch.setattr(supervise, "is_holding", lambda _s: False)
    monkeypatch.setattr(cli, "remove_cli_root", lambda _c: None)
    monkeypatch.setattr(cli, "reconcile_pointer", lambda _c: True)
    monkeypatch.setattr(dc, "append_deploy_log", lambda _m: None)
    monkeypatch.setattr(lifecycle, "_resolve_bin_home", lambda: bin_home)
    return bin_home, mission


@pytest.mark.usefixtures("supervisor_token")
def test_supervised_rollback_never_invokes_legacy_worker_restoration(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Terminal supervised rollback uses settle_desired only, never _restart_previous.

    Regression: _finalize_supervised_rollback must NOT call
    _restore_previous_locked() or _restart_previous().  The supervised path
    relies on settle_desired() having proved the external supervisor converged
    a fresh previous-commit worker.  Calling the legacy direct-restore would
    launch a second worker and mix legacy authority with supervisor ownership.
    """
    bin_home, mission = _setup_supervised_rollback(monkeypatch, tmp_path)
    sc.write_contract()
    sc.write_startup_definition()
    sc.write_startup_launcher(bin_home)
    sc.stage_startup_artifacts(bin_home)
    sc.write_staging_manifest(mission.previous_commit, bin_home)

    restart_called = [False]
    original_restart = dc._restart_previous

    def track_restart(state: dc.RollbackState) -> WorkerMeta | None:
        restart_called[0] = True
        return original_restart(state)

    monkeypatch.setattr(dc, "_restart_previous", track_restart)
    restore_locked_called = [False]
    original_restore = dc._restore_previous_locked

    def track_restore(state: dc.RollbackState) -> tuple[bool, bool]:
        restore_locked_called[0] = True
        return original_restore(state)

    monkeypatch.setattr(dc, "_restore_previous_locked", track_restore)

    terminal = dc._finalize_supervised_rollback(mission, mission.generation + 1)
    assert terminal.status == dc.STATUS_ROLLED_BACK
    assert not restart_called[0], "_restart_previous must not be called by supervised rollback"
    assert not restore_locked_called[0], (
        "_restore_previous_locked must not be called by supervised rollback"
    )


@pytest.mark.usefixtures("supervisor_token")
def test_bin_home_resolution_failure_keeps_rollback_nonterminal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When _resolve_bin_home() fails, _restore_cli_and_startup_artifacts returns failure.

    Terminal ``rolled_back`` status must NOT be written: the previous startup
    authority was not verified.  Staging, snapshot, and receipt evidence are
    retained so retry can recover.
    """
    bin_home, mission = _setup_supervised_rollback(monkeypatch, tmp_path)
    sc.write_contract()
    sc.write_startup_definition()
    sc.write_startup_launcher(bin_home)
    sc.stage_startup_artifacts(bin_home)
    sc.write_staging_manifest(mission.previous_commit, bin_home)

    snapshot_path = dc._pre_confirmation_artifacts_path()
    raw = sc.snapshot_startup_artifacts(bin_home)
    snapshot_path.write_text(json.dumps(dict(raw.items())), encoding="utf-8")
    assert snapshot_path.is_file()

    receipt_path = dc._confirmation_receipt_path()
    receipt_path.write_text(json.dumps({"commit": mission.commit}), encoding="utf-8")
    assert receipt_path.is_file()

    monkeypatch.setattr(lifecycle, "_resolve_bin_home", _raise_os_error)

    success, snapshot_restored = dc._restore_cli_and_startup_artifacts(
        mission.commit, mission.previous_commit
    )
    assert success is False
    assert snapshot_restored is False
    assert snapshot_path.is_file(), "pre-confirmation snapshot must be retained"
    assert receipt_path.is_file(), "confirmation receipt must be retained"
    assert sc.read_staging_manifest() is not None, "staging manifest must be retained"


@pytest.mark.usefixtures("supervisor_token")
def test_missing_snapshot_retains_evidence_and_succeeds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When no pre-confirmation snapshot and no receipt exist, rollback succeeds.

    Absence of both receipt and snapshot means confirmation never completed,
    so there is no previous startup authority to restore.  The restore
    function cleans up staging and receipt inline (no evidence to defer).
    """
    bin_home, mission = _setup_supervised_rollback(monkeypatch, tmp_path)
    sc.write_contract()
    sc.write_startup_definition()
    sc.write_startup_launcher(bin_home)
    sc.stage_startup_artifacts(bin_home)
    sc.write_staging_manifest(mission.previous_commit, bin_home)

    assert not dc._pre_confirmation_artifacts_path().is_file()
    assert not dc._confirmation_receipt_path().is_file()

    success, snapshot_restored = dc._restore_cli_and_startup_artifacts(
        mission.commit, mission.previous_commit
    )
    assert success is True
    assert snapshot_restored is False
    assert sc.read_staging_manifest() is None, "staging cleaned up when no snapshot exists"


@pytest.mark.usefixtures("supervisor_token")
def test_missing_snapshot_with_receipt_is_restoration_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A missing pre-confirmation snapshot when a receipt exists is restoration failure.

    The receipt proves confirmation happened, so the missing restoration
    authority must keep rollback nonterminal.  All evidence (receipt,
    staging) is retained for retry.
    """
    bin_home, mission = _setup_supervised_rollback(monkeypatch, tmp_path)
    sc.write_contract()
    sc.write_startup_definition()
    sc.write_startup_launcher(bin_home)
    sc.stage_startup_artifacts(bin_home)
    sc.write_staging_manifest(mission.previous_commit, bin_home)

    assert not dc._pre_confirmation_artifacts_path().is_file()
    receipt_path = dc._confirmation_receipt_path()
    receipt_path.write_text(json.dumps({"commit": mission.commit}), encoding="utf-8")

    success, snapshot_restored = dc._restore_cli_and_startup_artifacts(
        mission.commit, mission.previous_commit
    )
    assert success is False
    assert snapshot_restored is False
    assert receipt_path.is_file(), "confirmation receipt is retained"
    assert sc.read_staging_manifest() is not None, "staging manifest is retained"


@pytest.mark.usefixtures("supervisor_token")
def test_corrupt_snapshot_retains_evidence_and_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A corrupt pre-confirmation snapshot with a receipt is restoration failure.

    The receipt proves confirmation happened, so the missing restoration
    authority must keep rollback nonterminal.  The corrupt snapshot file is
    retained for later diagnosis.  Staging, receipt, and snapshot evidence
    are all retained so retry can recover.
    """
    bin_home, mission = _setup_supervised_rollback(monkeypatch, tmp_path)
    sc.write_contract()
    sc.write_startup_definition()
    sc.write_startup_launcher(bin_home)
    sc.stage_startup_artifacts(bin_home)
    sc.write_staging_manifest(mission.previous_commit, bin_home)

    snapshot_path = dc._pre_confirmation_artifacts_path()
    snapshot_path.write_text("{corrupt", encoding="utf-8")

    receipt_path = dc._confirmation_receipt_path()
    receipt_path.write_text(json.dumps({"commit": mission.commit}), encoding="utf-8")

    success, snapshot_restored = dc._restore_cli_and_startup_artifacts(
        mission.commit, mission.previous_commit
    )
    assert success is False
    assert snapshot_restored is False
    assert snapshot_path.is_file(), "corrupt snapshot file is retained"
    assert receipt_path.is_file(), "confirmation receipt is retained"
    assert sc.read_staging_manifest() is not None, "staging manifest is retained"


@pytest.mark.usefixtures("supervisor_token")
def test_supervised_rollback_generation_race_rejects_stale_terminalization(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A newer desired generation winning during restoration preserves evidence.

    Regression: because the initial generation check is separated from the
    later terminal write, a newer desired generation that wins during
    restoration must not be overwritten by a stale terminal rollback.
    Evidence (snapshot, staging, receipt) must be retained so retry can
    recover.  _finalize_supervised_rollback revalidates the generation
    under the lock before writing terminal state.
    """
    bin_home, mission = _setup_supervised_rollback(monkeypatch, tmp_path)
    sc.write_contract()
    sc.write_startup_definition()
    sc.write_startup_launcher(bin_home)
    sc.stage_startup_artifacts(bin_home)
    sc.write_staging_manifest(mission.previous_commit, bin_home)

    snapshot_path = dc._pre_confirmation_artifacts_path()
    raw = sc.snapshot_startup_artifacts(bin_home)
    snapshot_path.write_text(json.dumps(dict(raw.items())), encoding="utf-8")

    receipt_path = dc._confirmation_receipt_path()
    receipt_path.write_text(json.dumps({"commit": mission.commit}), encoding="utf-8")

    written: list[dc.RollbackState] = []
    monkeypatch.setattr(dc, "_write_state", written.append)

    desired_reads = [0]
    new_commit = "c" * 40
    new_gen = mission.generation + 99
    superseded_desired = SimpleNamespace(commit=new_commit, generation=new_gen)
    superseded_status = SimpleNamespace(
        commit=new_commit,
        applied_generation=new_gen,
        ready=True,
        holding=False,
        child=SimpleNamespace(marker="new-child"),
    )
    original_desired_fn = supervise.read_desired_strict
    original_status_fn = supervise.read_status

    def _supersede_on_second_read() -> object:
        desired_reads[0] += 1
        if desired_reads[0] >= 2:
            return superseded_desired
        return original_desired_fn()

    def _supersede_status_on_second_read() -> object:
        if desired_reads[0] >= 2:
            return superseded_status
        return original_status_fn()

    monkeypatch.setattr(supervise, "read_desired_strict", _supersede_on_second_read)
    monkeypatch.setattr(supervise, "read_status", _supersede_status_on_second_read)

    with pytest.raises(dc.DeployCtlError, match="superseded before rollback"):
        dc._finalize_supervised_rollback(mission, mission.generation + 1)

    assert not any(s.status == dc.STATUS_ROLLED_BACK for s in written), (
        "terminal rolled_back must not be written when generation was superseded"
    )
    assert snapshot_path.is_file(), "pre-confirmation snapshot retained after superseded rollback"
    assert receipt_path.is_file(), "confirmation receipt retained after superseded rollback"
    assert sc.read_staging_manifest() is not None, (
        "staging manifest retained after superseded rollback"
    )


@pytest.mark.usefixtures("supervisor_token")
def test_successful_supervised_rollback_through_real_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Full supervised rollback path: settle → authority check → restore artifacts → terminalize.

    Exercises the complete _rollback_locked() → settle_desired() →
    _finalize_supervised_rollback() path with real artifact restoration.
    Verifies terminal state, staging cleanup, receipt removal, and that
    the previous startup authority was verified before terminalization.
    """
    bin_home, mission = _setup_supervised_rollback(monkeypatch, tmp_path)
    sc.write_contract()
    sc.write_startup_definition()
    sc.write_startup_launcher(bin_home)
    sc.stage_startup_artifacts(bin_home)
    sc.write_staging_manifest(mission.previous_commit, bin_home)

    snapshot_path = dc._pre_confirmation_artifacts_path()
    raw = sc.snapshot_startup_artifacts(bin_home)
    snapshot_path.write_text(json.dumps(dict(raw.items())), encoding="utf-8")

    written: list[dc.RollbackState] = []
    monkeypatch.setattr(dc, "_write_state", written.append)
    monkeypatch.setattr(dc, "settle_desired", lambda *_a, **_k: mission.generation + 1)

    assert dc._rollback_locked(mission) is True

    assert len(written) == 1
    terminal = written[0]
    assert terminal.status == dc.STATUS_ROLLED_BACK
    assert not snapshot_path.is_file(), "snapshot cleaned up after successful rollback"
    assert sc.read_staging_manifest() is None, "staging cleaned up after successful rollback"
    assert not dc._confirmation_receipt_path().is_file(), (
        "receipt removed after successful rollback"
    )
    assert sc.assess_recorded_contract().state == "current", "previous contract authority restored"
    assert sc.validate_startup_launcher(bin_home), "previous launcher restored"
    assert sc.validate_startup_definition().ok, "previous definition restored"
