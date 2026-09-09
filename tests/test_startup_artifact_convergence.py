"""Crash-safe startup-artifact convergence invariants.

Verifies that repository-owned startup artifacts (contract, launcher, definition)
are atomically staged by candidate B, promoted only at the confirmation boundary,
and idempotently retried by the supervisor.  Stale staging from a different commit
is never promoted.  Rollback retains the snapshot until all artifacts are restored.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from lubko import deployctl as dc
from lubko import lifecycle
from lubko import startup_contract as sc
from lubko import state as _state_mod
from lubko.deployctl import STATUS_CONFIRMED, RollbackState
from lubko.startup_contract import (
    CONTRACT_SCHEMA_VERSION,
    CURRENT_CONTRACT,
    STARTUP_DEFINITION_SCHEMA_VERSION,
)

if TYPE_CHECKING:
    from pathlib import Path

    import pytest


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
    serializable = {k: list(v) for k, v in snapshot.items()}
    snapshot_path = tmp_path / "deploy" / "pre-confirmation-startup-artifacts.json"
    snapshot_path.write_text(json.dumps(serializable), encoding="utf-8")
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
