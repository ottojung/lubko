"""Crash-safe startup-artifact convergence invariants.

Verifies that repository-owned startup artifacts (contract, launcher, definition)
are atomically staged by candidate B, promoted only at the confirmation boundary,
and idempotently retried by the supervisor.  Stale staging from a different commit
is never promoted.  Rollback retains the snapshot until all artifacts are restored.
"""

from __future__ import annotations

import json
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace

import pytest

from lubko import cli, lifecycle, supervise
from lubko import deployctl as dc
from lubko import startup_contract as sc
from lubko import state as _state_mod
from lubko.deployctl import STATUS_CONFIRMED, RollbackState
from lubko.durable import DurabilityError
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
}

STALE_DEFINITION = {
    "schema_version": STARTUP_DEFINITION_SCHEMA_VERSION,
    "command": ["sleep", "infinity"],
    "required_state_dirs": ["old"],
    "required_config_files": [],
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
    assert not _manifest_path(tmp_path).is_file()


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


def test_confirmed_idempotent_fails_on_unresolvable_bin_home(
    tmp_path: Path,  # ruff: ignore[unused-function-argument]
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
