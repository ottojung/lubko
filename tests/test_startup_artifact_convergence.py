"""Crash-safe startup-artifact convergence invariants.

Verifies that repository-owned startup artifacts (contract, launcher, definition)
are atomically updated during confirmation, preserved across rollback, and
idempotently converged by the supervisor recovery loop.
"""

from __future__ import annotations

import json
from contextlib import suppress
from typing import TYPE_CHECKING

from lubko import deployctl as dc
from lubko import lifecycle
from lubko import startup_contract as sc
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


def _write_stale_contract(path: Path) -> None:
    """Write a deliberately stale (different content) contract artifact.

    Uses the current schema version so the strict reader does not classify
    it as corrupt; the semantic difference produces a ``mismatch``.
    """
    stale = {
        "schema_version": CONTRACT_SCHEMA_VERSION,
        "init_command": ["sleep"],
        "supervisor_command": ["infinity"],
        "required_state_dirs": ["old"],
        "required_config_files": [],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(stale), encoding="utf-8")


def _write_stale_definition(path: Path) -> None:
    """Write a deliberately stale startup definition artifact."""
    stale = {
        "schema_version": STARTUP_DEFINITION_SCHEMA_VERSION,
        "command": ["sleep", "infinity"],
        "required_state_dirs": ["old"],
        "required_config_files": [],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(stale), encoding="utf-8")


def _read_json(path: Path) -> dict[str, object]:
    """Read and decode a JSON file.

    Returns:
        The decoded JSON mapping.
    """
    result: dict[str, object] = json.loads(path.read_text(encoding="utf-8"))
    return result


def _setup_artifact_dirs(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Redirect state_root and create required state dirs."""
    monkeypatch.setattr(sc, "state_root", lambda: tmp_path)
    for name in CURRENT_CONTRACT.required_state_dirs:
        (tmp_path / name).mkdir(mode=0o700, exist_ok=True)
    (tmp_path / "deploy").mkdir(mode=0o700, exist_ok=True)


# ---------------------------------------------------------------------------
# converge_startup_artifacts unit tests
# ---------------------------------------------------------------------------


def test_converge_writes_all_artifacts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """converge_startup_artifacts writes contract, launcher, and definition."""
    _setup_artifact_dirs(monkeypatch, tmp_path)
    bin_home = tmp_path / "bin"
    bin_home.mkdir(mode=0o700)
    error = sc.converge_startup_artifacts(bin_home)
    assert error is None
    assert sc.assess_recorded_contract().state == "current"
    assert sc.validate_startup_launcher(bin_home) is True
    assert sc.validate_startup_definition().ok is True


def test_converge_is_idempotent(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Running converge twice produces identical artifacts."""
    _setup_artifact_dirs(monkeypatch, tmp_path)
    bin_home = tmp_path / "bin"
    bin_home.mkdir(mode=0o700)
    assert sc.converge_startup_artifacts(bin_home) is None
    contract_before = _read_json(sc.contract_path())
    definition_before = _read_json(sc.startup_definition_path())
    assert sc.converge_startup_artifacts(bin_home) is None
    assert _read_json(sc.contract_path()) == contract_before
    assert _read_json(sc.startup_definition_path()) == definition_before


def test_converge_repairs_stale_artifacts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """converge_startup_artifacts overwrites stale/mismatched artifacts."""
    _setup_artifact_dirs(monkeypatch, tmp_path)
    bin_home = tmp_path / "bin"
    bin_home.mkdir(mode=0o700)
    # Write stale artifacts
    _write_stale_contract(sc.contract_path())
    _write_stale_definition(sc.startup_definition_path())
    assert sc.assess_recorded_contract().state == "mismatch"
    assert sc.validate_startup_definition().ok is False
    # Converge repairs them
    error = sc.converge_startup_artifacts(bin_home)
    assert error is None
    assert sc.assess_recorded_contract().state == "current"
    assert sc.validate_startup_definition().ok is True


def test_converge_returns_error_on_missing_bin_home(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """converge_startup_artifacts returns an error when bin_home is missing."""
    _setup_artifact_dirs(monkeypatch, tmp_path)
    bin_home = tmp_path / "nonexistent"
    error = sc.converge_startup_artifacts(bin_home)
    assert error is not None
    assert "startup launcher" in error


# ---------------------------------------------------------------------------
# Snapshot / restore round-trip
# ---------------------------------------------------------------------------


def test_snapshot_restore_round_trip(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Snapshot captures current artifacts and restore writes them back."""
    _setup_artifact_dirs(monkeypatch, tmp_path)
    bin_home = tmp_path / "bin"
    bin_home.mkdir(mode=0o700)
    # Write current artifacts
    sc.write_contract()
    sc.write_startup_definition()
    sc.write_startup_launcher(bin_home)
    # Snapshot
    snapshot = sc.snapshot_startup_artifacts()
    assert "contract" in snapshot
    assert "definition" in snapshot
    # Overwrite with stale
    _write_stale_contract(sc.contract_path())
    _write_stale_definition(sc.startup_definition_path())
    assert sc.assess_recorded_contract().state == "mismatch"
    # Restore
    sc.restore_startup_artifacts(snapshot)
    assert sc.assess_recorded_contract().state == "current"
    assert sc.validate_startup_definition().ok is True


def test_snapshot_empty_when_no_artifacts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Snapshot returns empty dict when no artifacts exist on disk."""
    monkeypatch.setattr(sc, "state_root", lambda: tmp_path)
    snapshot = sc.snapshot_startup_artifacts()
    assert snapshot == {}


def test_restore_skips_missing_snapshot_keys(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Restore only writes artifacts present in the snapshot."""
    _setup_artifact_dirs(monkeypatch, tmp_path)
    # Write contract only (no definition)
    sc.write_contract()
    snapshot = sc.snapshot_startup_artifacts()
    assert "contract" in snapshot
    assert "definition" not in snapshot
    # Overwrite contract with stale
    _write_stale_contract(sc.contract_path())
    # Restore should restore contract but not touch definition (which doesn't exist)
    sc.restore_startup_artifacts(snapshot)
    assert sc.assess_recorded_contract().state == "current"


# ---------------------------------------------------------------------------
# Confirmation integration: artifacts updated after confirmation
# ---------------------------------------------------------------------------


def test_confirmation_updates_startup_artifacts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """After confirmation, startup artifacts match the current code contract."""
    _setup_artifact_dirs(monkeypatch, tmp_path)
    bin_home = tmp_path / "bin"
    bin_home.mkdir(mode=0o700)
    # Write stale artifacts to simulate pre-confirmation state
    _write_stale_contract(sc.contract_path())
    _write_stale_definition(sc.startup_definition_path())
    assert sc.assess_recorded_contract().state == "mismatch"
    # Simulate the confirmation flow: snapshot, converge, cleanup
    dc._snapshot_pre_confirmation_artifacts()
    assert dc._pre_confirmation_artifacts_path().is_file()
    dc._converge_startup_artifacts()
    assert sc.assess_recorded_contract().state == "current"
    assert sc.validate_startup_definition().ok is True
    dc._remove_pre_confirmation_artifacts()
    assert not dc._pre_confirmation_artifacts_path().is_file()


def test_pre_confirmation_snapshot_preserves_stale_artifacts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The pre-confirmation snapshot captures the stale artifacts before convergence."""
    _setup_artifact_dirs(monkeypatch, tmp_path)
    bin_home = tmp_path / "bin"
    bin_home.mkdir(mode=0o700)
    # Write stale artifacts
    _write_stale_contract(sc.contract_path())
    _write_stale_definition(sc.startup_definition_path())
    stale_contract = sc.contract_path().read_bytes()
    stale_definition = sc.startup_definition_path().read_bytes()
    # Snapshot
    dc._snapshot_pre_confirmation_artifacts()
    # Converge (overwrites on-disk artifacts)
    dc._converge_startup_artifacts()
    assert sc.assess_recorded_contract().state == "current"
    # The snapshot file should contain the stale content
    snapshot_path = dc._pre_confirmation_artifacts_path()
    raw = json.loads(snapshot_path.read_text(encoding="utf-8"))
    assert bytes(raw["contract"]) == stale_contract
    assert bytes(raw["definition"]) == stale_definition
    # Cleanup
    dc._remove_pre_confirmation_artifacts()


# ---------------------------------------------------------------------------
# Rollback integration: artifacts restored to pre-confirmation state
# ---------------------------------------------------------------------------


def test_rollback_restores_pre_confirmation_artifacts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Rollback restores startup artifacts to their pre-confirmation state."""
    _setup_artifact_dirs(monkeypatch, tmp_path)
    bin_home = tmp_path / "bin"
    bin_home.mkdir(mode=0o700)
    monkeypatch.setattr(lifecycle, "_resolve_bin_home", lambda: bin_home)
    # Write current artifacts, snapshot, then converge (simulating confirmation)
    sc.write_contract()
    sc.write_startup_definition()
    sc.write_startup_launcher(bin_home)
    dc._snapshot_pre_confirmation_artifacts()
    dc._converge_startup_artifacts()
    # Now overwrite with a "new version" that has different contract content
    sc.contract_path().write_text(
        json.dumps({
            "schema_version": CONTRACT_SCHEMA_VERSION,
            "init_command": ["different"],
            "supervisor_command": ["lubko-supervisor"],
            "required_state_dirs": list(CURRENT_CONTRACT.required_state_dirs),
            "required_config_files": list(CURRENT_CONTRACT.required_config_files),
        }),
        encoding="utf-8",
    )
    assert sc.assess_recorded_contract().state == "mismatch"
    # Rollback should restore the pre-confirmation snapshot
    dc._restore_pre_confirmation_artifacts()
    assert sc.assess_recorded_contract().state == "current"
    dc._remove_pre_confirmation_artifacts()


def test_rollback_without_snapshot_is_harmless(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Rollback with no snapshot file does not crash or modify artifacts."""
    _setup_artifact_dirs(monkeypatch, tmp_path)
    sc.write_contract()
    assert sc.assess_recorded_contract().state == "current"
    # No snapshot was written; restore should be a no-op
    dc._restore_pre_confirmation_artifacts()
    assert sc.assess_recorded_contract().state == "current"


# ---------------------------------------------------------------------------
# Crash boundary: supervisor convergence after incomplete confirmation
# ---------------------------------------------------------------------------


def test_supervisor_convergence_repairs_stale_artifacts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If a crash left startup artifacts stale, supervisor convergence repairs them."""
    _setup_artifact_dirs(monkeypatch, tmp_path)
    bin_home = tmp_path / "bin"
    bin_home.mkdir(mode=0o700)
    # Simulate a crash: write stale artifacts directly (no converge)
    _write_stale_contract(sc.contract_path())
    _write_stale_definition(sc.startup_definition_path())
    assert sc.assess_recorded_contract().state == "mismatch"
    # Supervisor convergence should repair them
    error = sc.converge_startup_artifacts(bin_home)
    assert error is None
    assert sc.assess_recorded_contract().state == "current"
    assert sc.validate_startup_definition().ok is True


def test_snapshot_converge_crash_between_them_leaves_convergence_retriable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A crash between snapshot and converge leaves artifacts retriable on recovery."""
    _setup_artifact_dirs(monkeypatch, tmp_path)
    bin_home = tmp_path / "bin"
    bin_home.mkdir(mode=0o700)
    # Write stale artifacts
    _write_stale_contract(sc.contract_path())
    _write_stale_definition(sc.startup_definition_path())
    # Pre-confirmation snapshot
    dc._snapshot_pre_confirmation_artifacts()
    # Simulate crash: on-disk artifacts are still stale
    assert sc.assess_recorded_contract().state == "mismatch"
    # Recovery: converge should repair them
    error = sc.converge_startup_artifacts(bin_home)
    assert error is None
    assert sc.assess_recorded_contract().state == "current"
    # Cleanup
    with suppress(FileNotFoundError, OSError):
        dc._pre_confirmation_artifacts_path().unlink()
