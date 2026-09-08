"""Deterministic startup-contract artifact invariants."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pytest

from lubko import lifecycle, supervise
from lubko import startup_contract as sc
from lubko.startup_contract import (
    CONTRACT_SCHEMA_VERSION,
    CURRENT_CONTRACT,
    ContractAssessment,
    ContractPathValidation,
    StartupContract,
    StartupContractError,
)


def test_canonical_startup_command() -> None:
    """The canonical versioned startup command is tini-static -- lubko-supervisor."""
    assert sc.canonical_startup_command() == ["tini-static", "--", "lubko-supervisor"]


def test_startup_launcher_round_trip(tmp_path: Path) -> None:
    """The versioned launcher is written exactly and validates against itself."""
    sc.write_startup_launcher(tmp_path)
    assert sc.validate_startup_launcher(tmp_path) is True
    assert (tmp_path / sc.STARTUP_LAUNCHER_NAME).stat().st_mode & 0o111


def test_startup_launcher_missing_is_invalid(tmp_path: Path) -> None:
    """A missing or divergent launcher fails validation."""
    assert sc.validate_startup_launcher(tmp_path) is False


def test_validate_contract_paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The contract's required state directories are validated under state_root."""
    monkeypatch.setattr(sc, "state_root", lambda: tmp_path)
    for name in CURRENT_CONTRACT.required_state_dirs:
        (tmp_path / name).mkdir(mode=0o700)
    assert sc.validate_contract_paths().ok is True
    (tmp_path / "deploy").rmdir()
    result = sc.validate_contract_paths()
    assert result.ok is False
    assert "deploy" in result.missing


def test_validate_contract_paths_mode(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A required directory missing the needed permission bits is reported."""
    monkeypatch.setattr(sc, "state_root", lambda: tmp_path)
    (tmp_path / "supervisor").mkdir(mode=0o600)
    result = sc.validate_contract_paths()
    assert result.ok is False
    assert "supervisor" in result.mode_mismatched


def test_contract_round_trip_and_version_check(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The contract artifact round-trips and corruption is hidden as absence."""
    monkeypatch.setattr(sc, "contract_path", lambda: tmp_path / "startup-contract.json")
    sc.write_contract()
    loaded = sc.read_contract()
    assert loaded is not None
    assert loaded == CURRENT_CONTRACT
    assert loaded.schema_version == CONTRACT_SCHEMA_VERSION
    (tmp_path / "startup-contract.json").write_text("{not json", encoding="utf-8")
    assert sc.read_contract() is None


def test_contract_version_mismatch_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An unsupported contract version fails closed on the strict reader."""
    monkeypatch.setattr(sc, "contract_path", lambda: tmp_path / "startup-contract.json")
    (tmp_path / "startup-contract.json").write_text(
        '{"schema_version": 999, "init_command": ["tini-static", "--"], '
        '"supervisor_command": ["lubko-supervisor"], '
        '"required_state_dirs": ["supervisor"]}',
        encoding="utf-8",
    )
    with pytest.raises(StartupContractError, match="unsupported startup contract version 999"):
        sc.read_contract_strict()


def test_contract_malformed_fails_closed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A malformed contract artifact fails closed on the strict reader."""
    monkeypatch.setattr(sc, "contract_path", lambda: tmp_path / "startup-contract.json")
    (tmp_path / "startup-contract.json").write_text('{"schema_version": "one"}', encoding="utf-8")
    with pytest.raises(StartupContractError):
        sc.read_contract_strict()


def test_contract_legacy_keys_ignored(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Legacy schema-v1 keys are silently ignored."""
    monkeypatch.setattr(sc, "contract_path", lambda: tmp_path / "startup-contract.json")
    config_files_json = json.dumps(list(CURRENT_CONTRACT.required_config_files))
    (tmp_path / "startup-contract.json").write_text(
        '{"schema_version": 1, '
        '"init_markers": ["tini-static", "tini"], '
        '"init_command": ["tini-static", "--"], '
        '"supervisor_markers": ["lubko-supervisor", "lubko.supervisor"], '
        '"supervisor_command": ["lubko-supervisor"], '
        '"worker_relationship": "direct-child", '
        '"required_state_dirs": ["supervisor", "worker", "deploy"], '
        f'"required_config_files": {config_files_json}'
        "}",
        encoding="utf-8",
    )
    contract = sc.read_contract_strict()
    assert contract is not None
    assert contract == CURRENT_CONTRACT


def test_contract_semantic_mismatch_is_distinct(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A valid but divergent contract is a distinct MISMATCH, not equal to current."""
    monkeypatch.setattr(sc, "contract_path", lambda: tmp_path / "startup-contract.json")
    divergent = StartupContract(
        schema_version=CONTRACT_SCHEMA_VERSION,
        init_command=CURRENT_CONTRACT.init_command,
        supervisor_command=CURRENT_CONTRACT.supervisor_command,
        required_state_dirs=("supervisor",),
        required_config_files=CURRENT_CONTRACT.required_config_files,
    )
    sc.write_contract(divergent)
    assessment = sc.assess_recorded_contract()
    assert assessment.state == "mismatch"
    assert sc.contract_matches_current(divergent) is False
    assert sc.contract_matches_current(CURRENT_CONTRACT) is True


def test_assess_recorded_contract_states(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Missing, corrupt, and current states are classified distinctly."""
    monkeypatch.setattr(sc, "contract_path", lambda: tmp_path / "startup-contract.json")
    assert sc.assess_recorded_contract().state == "missing"
    (tmp_path / "startup-contract.json").write_text("}{", encoding="utf-8")
    assert sc.assess_recorded_contract().state == "corrupt"
    sc.write_contract()
    assert sc.assess_recorded_contract().state == "current"


def test_startup_definition_round_trip(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The installed startup definition matches the current contract exactly."""
    monkeypatch.setattr(sc, "state_root", lambda: tmp_path)
    sc.write_startup_definition()
    assert sc.validate_startup_definition().ok is True
    definition = sc.read_startup_definition()
    assert definition == sc.generate_startup_definition()
    assert definition is not None
    assert definition["command"] == ["tini-static", "--", "lubko-supervisor"]
    assert definition["schema_version"] == sc.STARTUP_DEFINITION_SCHEMA_VERSION
    (tmp_path / "deploy" / sc.STARTUP_DEFINITION_NAME).write_text(
        '{"schema_version": 1, "command": ["sleep", "infinity"]}', encoding="utf-8"
    )
    assert sc.validate_startup_definition().ok is False


def test_install_creates_required_state_dirs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A fresh install creates the contract's private state dirs before validating."""
    monkeypatch.setattr(sc, "state_root", lambda: tmp_path)
    monkeypatch.setattr(sc, "write_startup_launcher", lambda _b: None)
    monkeypatch.setattr(sc, "validate_startup_launcher", lambda _b: True)
    assert sc.install_and_validate_startup_definition(tmp_path / "bin") is None
    for relative in CURRENT_CONTRACT.required_state_dirs:
        directory = tmp_path / relative
        assert directory.is_dir()
        mode = directory.stat().st_mode & 0o777
        assert mode == sc.DEFAULT_STATE_DIR_MODE


def test_validate_contract_paths_rejects_insecure_modes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Group/world-accessible state directories fail the private-mode contract."""
    monkeypatch.setattr(sc, "state_root", lambda: tmp_path)
    (tmp_path / "supervisor").mkdir(mode=0o755)
    (tmp_path / "worker").mkdir(mode=0o777)
    (tmp_path / "deploy").mkdir(mode=0o700)
    result = sc.validate_contract_paths()
    assert result.ok is False
    assert "supervisor" in result.mode_mismatched
    assert "worker" in result.mode_mismatched


def test_validate_contract_config_private_permissions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Private config files must exist with no group/world access bits."""
    private_a = tmp_path / "database.conf"
    private_b = tmp_path / "worker.conf"
    private_a.write_text("", encoding="utf-8")
    private_b.write_text("", encoding="utf-8")
    Path(private_a).chmod(0o600)
    Path(private_b).chmod(0o640)
    monkeypatch.setattr(sc, "private_config_paths", lambda: (private_a, private_b))
    result = sc.validate_contract_config()
    assert result.ok is False
    assert str(private_b) in result.mode_mismatched
    assert str(private_a) not in result.mode_mismatched
    Path(private_b).chmod(0o600)
    assert sc.validate_contract_config().ok is True
    private_a.unlink()
    assert "missing" in sc.validate_contract_config().message


def test_contract_is_frozen_and_current_matches_version() -> None:
    """The shipped contract is frozen and carries the current schema version."""
    assert isinstance(CURRENT_CONTRACT, StartupContract)
    assert CURRENT_CONTRACT.schema_version == CONTRACT_SCHEMA_VERSION
    assert "tini-static" in CURRENT_CONTRACT.init_command
    assert "lubko-supervisor" in CURRENT_CONTRACT.supervisor_command


# --- Status / startup-contract command tests (no topology) ---


def _patch_status_surface(monkeypatch: pytest.MonkeyPatch) -> None:
    """Stub every deployment-seam read used by the status surface."""
    monkeypatch.setattr(
        sc,
        "assess_recorded_contract",
        lambda: ContractAssessment("current", CURRENT_CONTRACT, "ok"),
    )
    monkeypatch.setattr(sc, "validate_startup_launcher", lambda _b: True)
    monkeypatch.setattr(
        sc,
        "validate_contract_paths",
        lambda *_a, **_k: ContractPathValidation(
            ok=True, missing=(), mode_mismatched=(), message="ok"
        ),
    )
    monkeypatch.setattr(
        sc,
        "validate_startup_definition",
        lambda: ContractPathValidation(ok=True, missing=(), mode_mismatched=(), message="ok"),
    )
    monkeypatch.setattr(
        sc,
        "validate_contract_config",
        lambda: ContractPathValidation(ok=True, missing=(), mode_mismatched=(), message="ok"),
    )


def test_status_command_reports_contract(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """'lubko-deploy status' surfaces the contract, launcher, paths, and definition."""
    _patch_status_surface(monkeypatch)
    monkeypatch.setattr(supervise, "supervisor_running", lambda: True)
    monkeypatch.setattr(supervise, "read_status", lambda: None)
    monkeypatch.setattr(lifecycle, "read_meta", lambda: None)
    monkeypatch.setattr(lifecycle, "worker_state", lambda _meta: "stopped")
    assert lifecycle.status_cmd() == lifecycle.EXIT_OK
    out = capsys.readouterr().out
    assert "startup contract: current" in out
    assert "startup launcher (lubko-startup): installed" in out
    assert "startup definition: OK" in out


def test_status_command_surfaces_corruption(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """'lubko-deploy status' distinguishes a missing contract distinctly."""
    monkeypatch.setattr(
        sc,
        "assess_recorded_contract",
        lambda: ContractAssessment("missing", None, "no startup contract is recorded"),
    )
    monkeypatch.setattr(sc, "validate_startup_launcher", lambda _b: False)
    monkeypatch.setattr(
        sc,
        "validate_contract_paths",
        lambda *_a, **_k: ContractPathValidation(
            ok=False, missing=("deploy",), mode_mismatched=(), message="missing"
        ),
    )
    monkeypatch.setattr(supervise, "supervisor_running", lambda: False)
    monkeypatch.setattr(supervise, "read_status", lambda: None)
    monkeypatch.setattr(lifecycle, "read_meta", lambda: None)
    monkeypatch.setattr(lifecycle, "worker_state", lambda _meta: "stopped")
    assert lifecycle.status_cmd() == lifecycle.EXIT_OK
    out = capsys.readouterr().out
    assert "startup contract: MISSING" in out
    assert "startup launcher (lubko-startup): MISSING" in out


def test_startup_contract_command_writes_and_validates(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """'lubko-deploy startup-contract --write' publishes and validates the contract."""
    monkeypatch.setattr(sc, "contract_path", lambda: tmp_path / "startup-contract.json")
    monkeypatch.setattr(sc, "write_startup_launcher", lambda _b: None)
    monkeypatch.setattr(sc, "validate_startup_launcher", lambda _b: True)
    monkeypatch.setattr(sc, "write_startup_definition", lambda: None)
    monkeypatch.setattr(
        sc,
        "validate_startup_definition",
        lambda: ContractPathValidation(ok=True, missing=(), mode_mismatched=(), message="ok"),
    )
    monkeypatch.setattr(
        sc,
        "validate_contract_paths",
        lambda *_a, **_k: ContractPathValidation(
            ok=True, missing=(), mode_mismatched=(), message="ok"
        ),
    )
    monkeypatch.setattr(
        sc,
        "validate_contract_config",
        lambda: ContractPathValidation(ok=True, missing=(), mode_mismatched=(), message="ok"),
    )
    assert lifecycle.startup_contract_cmd(argparse.Namespace(write=True)) == lifecycle.EXIT_OK
    assert (tmp_path / "startup-contract.json").is_file()
    out = capsys.readouterr().out
    assert "startup contract version" in out


def test_startup_contract_command_fails_on_missing_contract(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A missing contract makes 'lubko-deploy startup-contract' fail."""
    monkeypatch.setattr(sc, "contract_path", lambda: tmp_path / "startup-contract.json")
    monkeypatch.setattr(sc, "validate_startup_launcher", lambda _b: True)
    monkeypatch.setattr(
        sc,
        "validate_startup_definition",
        lambda: ContractPathValidation(ok=True, missing=(), mode_mismatched=(), message="ok"),
    )
    monkeypatch.setattr(
        sc,
        "validate_contract_paths",
        lambda *_a, **_k: ContractPathValidation(
            ok=True, missing=(), mode_mismatched=(), message="ok"
        ),
    )
    monkeypatch.setattr(
        sc,
        "validate_contract_config",
        lambda: ContractPathValidation(ok=True, missing=(), mode_mismatched=(), message="ok"),
    )
    assert lifecycle.startup_contract_cmd(argparse.Namespace(write=False)) == lifecycle.EXIT_ERROR


def _patch_startup_contract_command_green(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    launcher_ok: bool = True,
    paths_ok: bool = True,
) -> None:
    """Force every startup-contract boundary green except the one under test."""
    monkeypatch.setattr(sc, "state_root", lambda: tmp_path)
    monkeypatch.setattr(sc, "write_startup_launcher", lambda _b: None)
    monkeypatch.setattr(sc, "validate_startup_launcher", lambda _b: launcher_ok)
    monkeypatch.setattr(
        sc,
        "validate_startup_definition",
        lambda: ContractPathValidation(ok=True, missing=(), mode_mismatched=(), message="ok"),
    )
    monkeypatch.setattr(
        sc,
        "validate_contract_paths",
        lambda *_a, **_k: ContractPathValidation(
            ok=paths_ok, missing=(), mode_mismatched=(), message="ok"
        ),
    )
    monkeypatch.setattr(
        sc,
        "validate_contract_config",
        lambda: ContractPathValidation(ok=True, missing=(), mode_mismatched=(), message="ok"),
    )
    sc.write_contract()
    sc.write_startup_definition()


def test_startup_contract_command_fails_on_launcher_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A missing/drifted launcher forces EXIT_ERROR with every other boundary green."""
    _patch_startup_contract_command_green(monkeypatch, tmp_path, launcher_ok=False)
    assert lifecycle.startup_contract_cmd(argparse.Namespace(write=False)) == lifecycle.EXIT_ERROR


def test_startup_contract_command_fails_on_state_path_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A missing/insecure state path forces EXIT_ERROR with every other boundary green."""
    _patch_startup_contract_command_green(monkeypatch, tmp_path, paths_ok=False)
    assert lifecycle.startup_contract_cmd(argparse.Namespace(write=False)) == lifecycle.EXIT_ERROR
