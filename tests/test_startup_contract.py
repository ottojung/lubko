"""Deterministic startup-contract and live topology proof invariants."""

from __future__ import annotations

import argparse
from typing import TYPE_CHECKING

import pytest

from lubko import lifecycle, supervise
from lubko import startup_contract as sc
from lubko.startup_contract import (
    CONTRACT_SCHEMA_VERSION,
    CURRENT_CONTRACT,
    ContractAssessment,
    ContractPathValidation,
    ProcessInfo,
    RestartAuthorityProof,
    StartupContract,
    StartupContractError,
    TopologyProof,
    TopologyTargets,
)

if TYPE_CHECKING:
    from pathlib import Path


def _proc(pid: int, ppid: int, cmdline: str, *, zombie: bool = False) -> ProcessInfo:
    """Build an exact process-info fixture.

    Returns:
        The constructed process identity.
    """
    return ProcessInfo(
        pid=pid,
        ppid=ppid,
        cmdline=cmdline,
        start_time_ticks=1000 + pid,
        zombie=zombie,
    )


def _valid_processes() -> dict[int, ProcessInfo | None]:
    """A process table satisfying the supported tini -> supervisor -> worker chain.

    Returns:
        A mapping of PID to its exact process identity.
    """
    return {
        1: _proc(1, 0, "/usr/bin/tini-static -- lubko-supervisor"),
        10: _proc(10, 1, "/usr/local/bin/uv run lubko-supervisor --serve"),
        20: _proc(20, 10, "/usr/local/bin/uv run lubko-worker"),
    }


def _prove(worker_pid: int | None = 20) -> TopologyProof:
    """Prove the valid topology, binding exact recorded start ticks.

    Returns:
        The structured topology proof for the valid process table.
    """
    return sc.evaluate_topology(
        TopologyTargets(
            init_pid=1,
            supervisor_pid=10,
            worker_pid=worker_pid,
            supervisor_start_ticks=1010,
            worker_start_ticks=1020 if worker_pid is not None else None,
        ),
        _valid_processes(),
    )


def test_valid_topology_proves_full_chain() -> None:
    """A supported tini -> supervisor -> worker chain proves the contract."""
    proof = _prove()
    assert proof.ok is True
    assert proof.init_is_tini is True
    assert proof.supervisor_under_init is True
    assert proof.supervisor_is_contract_binary is True
    assert proof.supervisor_identity_matches is True
    assert proof.worker_identity_matches is True
    assert proof.uses_sleep_placeholder is False
    assert proof.worker_is_direct_child is True
    assert "tini -> supervisor -> worker" in proof.message


def test_topology_without_worker_is_ok() -> None:
    """No worker is claimed yet still satisfies the contract."""
    proof = _prove(worker_pid=None)
    assert proof.ok is True
    assert proof.worker_pid is None
    assert proof.worker_is_direct_child is False
    assert "no worker claimed yet" in proof.message


def test_init_not_tini_rejected() -> None:
    """A non-Tini init leaves the supervisor without reaper/signal-forwarding."""
    processes = _valid_processes()
    processes[1] = _proc(1, 0, "/sbin/init")
    proof = sc.evaluate_topology(
        TopologyTargets(
            init_pid=1,
            supervisor_pid=10,
            worker_pid=20,
            supervisor_start_ticks=1010,
            worker_start_ticks=1020,
        ),
        processes,
    )
    assert proof.ok is False
    assert proof.init_is_tini is False
    assert "not a supported Tini" in proof.message


def test_sleep_infinity_placeholder_rejected() -> None:
    """The legacy 'sleep infinity' placeholder is never accepted as the supervisor."""
    processes = _valid_processes()
    processes[10] = _proc(10, 1, "/usr/bin/tini-static -- sleep infinity")
    proof = sc.evaluate_topology(
        TopologyTargets(
            init_pid=1,
            supervisor_pid=10,
            worker_pid=20,
            supervisor_start_ticks=1010,
            worker_start_ticks=1020,
        ),
        processes,
    )
    assert proof.ok is False
    assert proof.uses_sleep_placeholder is True
    assert proof.supervisor_is_contract_binary is False
    assert "sleep infinity" in proof.message


def test_supervisor_not_under_init_rejected() -> None:
    """The supervisor must be the direct child of the Tini init process."""
    processes = _valid_processes()
    processes[10] = _proc(10, 5, "/usr/local/bin/uv run lubko-supervisor")
    proof = sc.evaluate_topology(
        TopologyTargets(
            init_pid=1,
            supervisor_pid=10,
            worker_pid=20,
            supervisor_start_ticks=1010,
            worker_start_ticks=1020,
        ),
        processes,
    )
    assert proof.ok is False
    assert proof.supervisor_under_init is False
    assert "not the direct child of the Tini init" in proof.message


def test_worker_not_direct_child_rejected() -> None:
    """The worker must be the supervisor's direct child, not reparented."""
    processes = _valid_processes()
    processes[20] = _proc(20, 7, "/usr/local/bin/uv run lubko-worker")
    proof = sc.evaluate_topology(
        TopologyTargets(
            init_pid=1,
            supervisor_pid=10,
            worker_pid=20,
            supervisor_start_ticks=1010,
            worker_start_ticks=1020,
        ),
        processes,
    )
    assert proof.ok is False
    assert proof.worker_is_direct_child is False
    assert "not the direct child of the supervisor" in proof.message


def test_zombie_supervisor_not_present() -> None:
    """A zombie supervisor cannot satisfy the contract."""
    processes = _valid_processes()
    processes[10] = _proc(10, 1, "/usr/local/bin/uv run lubko-supervisor", zombie=True)
    proof = sc.evaluate_topology(
        TopologyTargets(
            init_pid=1,
            supervisor_pid=10,
            worker_pid=20,
            supervisor_start_ticks=1010,
            worker_start_ticks=1020,
        ),
        processes,
    )
    assert proof.ok is False
    assert proof.supervisor_is_contract_binary is False
    assert "absent or a zombie" in proof.message


def test_unknown_supervisor_binary_rejected() -> None:
    """A non-supervisor binary under Tini fails the contract."""
    processes = _valid_processes()
    processes[10] = _proc(10, 1, "/usr/bin/some-other-daemon")
    proof = sc.evaluate_topology(
        TopologyTargets(
            init_pid=1,
            supervisor_pid=10,
            worker_pid=20,
            supervisor_start_ticks=1010,
            worker_start_ticks=1020,
        ),
        processes,
    )
    assert proof.ok is False
    assert proof.supervisor_is_contract_binary is False
    assert "not the supported lubko-supervisor binary" in proof.message


def test_supervisor_pid_reuse_after_proof_rejected() -> None:
    """A recycled supervisor PID with different start ticks fails the proof."""
    processes = _valid_processes()
    processes[10] = _proc(10, 1, "/usr/local/bin/uv run lubko-supervisor")
    processes[10] = ProcessInfo(
        pid=10,
        ppid=1,
        cmdline="/usr/local/bin/uv run lubko-supervisor",
        start_time_ticks=7777,
        zombie=False,
    )
    proof = sc.evaluate_topology(
        TopologyTargets(
            init_pid=1,
            supervisor_pid=10,
            worker_pid=20,
            supervisor_start_ticks=1010,
            worker_start_ticks=1020,
        ),
        processes,
    )
    assert proof.ok is False
    assert proof.supervisor_identity_matches is False
    assert "PID was reused" in proof.message


def test_worker_pid_reuse_after_proof_rejected() -> None:
    """A recycled worker PID with different start ticks fails the proof."""
    processes = _valid_processes()
    processes[20] = ProcessInfo(
        pid=20,
        ppid=10,
        cmdline="/usr/local/bin/uv run lubko-worker",
        start_time_ticks=8888,
        zombie=False,
    )
    proof = sc.evaluate_topology(
        TopologyTargets(
            init_pid=1,
            supervisor_pid=10,
            worker_pid=20,
            supervisor_start_ticks=1010,
            worker_start_ticks=1020,
        ),
        processes,
    )
    assert proof.ok is False
    assert proof.worker_identity_matches is False
    assert "PID was reused" in proof.message


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
    assert loaded.worker_relationship == "direct-child"
    assert loaded.restart_policy == "always"
    (tmp_path / "startup-contract.json").write_text("{not json", encoding="utf-8")
    assert sc.read_contract() is None


def test_contract_version_mismatch_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An unsupported contract version fails closed on the strict reader."""
    monkeypatch.setattr(sc, "contract_path", lambda: tmp_path / "startup-contract.json")
    (tmp_path / "startup-contract.json").write_text(
        '{"schema_version": 999, "init_markers": ["tini"], "init_command": ["tini-static", "--"], '
        '"supervisor_markers": ["lubko-supervisor"], "supervisor_command": ["lubko-supervisor"], '
        '"worker_relationship": "direct-child", "restart_policy": "always", '
        '"restart_authority": "x", "required_state_dirs": ["supervisor"]}',
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


def test_contract_semantic_mismatch_is_distinct(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A valid but divergent contract is a distinct MISMATCH, not equal to current."""
    monkeypatch.setattr(sc, "contract_path", lambda: tmp_path / "startup-contract.json")
    divergent = StartupContract(
        schema_version=CONTRACT_SCHEMA_VERSION,
        init_markers=CURRENT_CONTRACT.init_markers,
        init_command=CURRENT_CONTRACT.init_command,
        supervisor_markers=CURRENT_CONTRACT.supervisor_markers,
        supervisor_command=CURRENT_CONTRACT.supervisor_command,
        worker_relationship="direct-child",
        restart_policy="never",
        restart_authority="divergent",
        required_state_dirs=CURRENT_CONTRACT.required_state_dirs,
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


def test_prove_restart_authority_contract_of_record() -> None:
    """Without an injected policy the contract is the authority of record."""
    rap = sc.prove_restart_authority(CURRENT_CONTRACT)
    assert rap.ok is True
    assert rap.source == "contract-of-record"
    assert rap.policy == "always"


def test_prove_restart_authority_env_seam(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An injected live policy must equal the contract or fail closed."""
    monkeypatch.setenv(sc.RESTART_POLICY_ENV, "always")
    assert sc.prove_restart_authority(CURRENT_CONTRACT).ok is True
    monkeypatch.setenv(sc.RESTART_POLICY_ENV, "never")
    rap = sc.prove_restart_authority(CURRENT_CONTRACT)
    assert rap.ok is False
    assert "deployment-seam" in rap.source


def test_live_topology_unproven_without_supervisor(monkeypatch: pytest.MonkeyPatch) -> None:
    """No recorded supervisor identity means the topology cannot be proven."""
    monkeypatch.setattr(sc, "read_supervisor_pid", lambda: None)
    proof = sc.verify_live_topology()
    assert proof.ok is False
    assert "no supervisor identity" in proof.message


def test_live_topology_binds_supervisor_start_ticks(monkeypatch: pytest.MonkeyPatch) -> None:
    """The live proof binds the supervisor to recorded start ticks after reads."""
    monkeypatch.setattr(sc, "read_supervisor_pid", lambda: (10, 1010))
    monkeypatch.setattr(sc, "supervisor_running", lambda: True)
    monkeypatch.setattr(sc, "read_status", lambda: None)
    monkeypatch.setattr(sc, "read_process_info", _valid_processes().get)
    proof = sc.verify_live_topology()
    assert proof.ok is True
    assert proof.supervisor_identity_matches is True


def test_live_topology_rejects_supervisor_reuse(monkeypatch: pytest.MonkeyPatch) -> None:
    """A reused supervisor PID after the durable read fails the live proof."""
    monkeypatch.setattr(sc, "read_supervisor_pid", lambda: (10, 1010))
    monkeypatch.setattr(sc, "supervisor_running", lambda: True)
    monkeypatch.setattr(sc, "read_status", lambda: None)

    def _fake(pid: int) -> ProcessInfo | None:
        info = _valid_processes().get(pid)
        if pid == 10 and info is not None:
            return ProcessInfo(
                pid=10, ppid=1, cmdline=info.cmdline, start_time_ticks=5555, zombie=False
            )
        return info

    monkeypatch.setattr(sc, "read_process_info", _fake)
    proof = sc.verify_live_topology()
    assert proof.ok is False
    assert proof.supervisor_identity_matches is False


def test_live_topology_reads_worker_from_status(monkeypatch: pytest.MonkeyPatch) -> None:
    """The live worker is taken from the exact supervisor status identity."""
    processes = _valid_processes()

    class _Child:
        pid = 20
        start_time_ticks = 1020

    class _Status:
        child = _Child()
        holding = False

    monkeypatch.setattr(sc, "read_supervisor_pid", lambda: (10, 1010))
    monkeypatch.setattr(sc, "supervisor_running", lambda: True)
    monkeypatch.setattr(sc, "read_status", _Status)
    monkeypatch.setattr(sc, "read_process_info", processes.get)
    proof = sc.verify_live_topology()
    assert proof.ok is True
    assert proof.worker_pid == 20
    assert proof.worker_identity_matches is True


def test_live_topology_rejects_worker_reuse(monkeypatch: pytest.MonkeyPatch) -> None:
    """A reused worker PID after the durable read fails the live proof."""
    processes = _valid_processes()

    class _Child:
        pid = 20
        start_time_ticks = 1020

    class _Status:
        child = _Child()
        holding = False

    def _fake(pid: int) -> ProcessInfo | None:
        info = processes.get(pid)
        if pid == 20 and info is not None:
            return ProcessInfo(
                pid=20, ppid=10, cmdline=info.cmdline, start_time_ticks=4444, zombie=False
            )
        return info

    monkeypatch.setattr(sc, "read_supervisor_pid", lambda: (10, 1010))
    monkeypatch.setattr(sc, "supervisor_running", lambda: True)
    monkeypatch.setattr(sc, "read_status", _Status)
    monkeypatch.setattr(sc, "read_process_info", _fake)
    proof = sc.verify_live_topology()
    assert proof.ok is False
    assert proof.worker_identity_matches is False


def _patch_status_surface(monkeypatch: pytest.MonkeyPatch, proof: TopologyProof) -> None:
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
    monkeypatch.setattr(sc, "verify_live_topology", lambda: proof)
    monkeypatch.setattr(
        sc,
        "prove_restart_authority",
        lambda _c, **_k: RestartAuthorityProof(
            ok=True, policy="always", source="contract-of-record", message="ok"
        ),
    )


def test_status_command_reports_contract(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """'lubko-deploy status' surfaces the contract, launcher, paths, and proof."""
    proof = TopologyProof(
        ok=True,
        contract_version=CONTRACT_SCHEMA_VERSION,
        init_pid=1,
        init_cmdline="/usr/bin/tini-static -- lubko-supervisor",
        init_is_tini=True,
        supervisor_pid=10,
        supervisor_cmdline="uv run lubko-supervisor",
        supervisor_present=True,
        supervisor_is_contract_binary=True,
        supervisor_under_init=True,
        supervisor_identity_matches=True,
        uses_sleep_placeholder=False,
        worker_pid=20,
        worker_is_direct_child=True,
        worker_identity_matches=True,
        message="startup contract satisfied (tini -> supervisor -> worker direct child)",
    )
    _patch_status_surface(monkeypatch, proof)
    monkeypatch.setattr(supervise, "supervisor_running", lambda: True)
    monkeypatch.setattr(supervise, "read_status", lambda: None)
    monkeypatch.setattr(lifecycle, "read_meta", lambda: None)
    monkeypatch.setattr(lifecycle, "worker_state", lambda _meta: "stopped")
    assert lifecycle.status_cmd() == lifecycle.EXIT_OK
    out = capsys.readouterr().out
    assert "startup contract: current" in out
    assert "startup topology: OK" in out
    assert "restart authority: OK" in out


def test_status_command_surfaces_corruption(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """'lubko-deploy status' distinguishes a missing contract distinctly."""
    proof = TopologyProof(
        ok=False,
        contract_version=CONTRACT_SCHEMA_VERSION,
        init_pid=1,
        init_cmdline="",
        init_is_tini=False,
        supervisor_pid=0,
        supervisor_cmdline="",
        supervisor_present=False,
        supervisor_is_contract_binary=False,
        supervisor_under_init=False,
        supervisor_identity_matches=False,
        uses_sleep_placeholder=False,
        worker_pid=None,
        worker_is_direct_child=False,
        worker_identity_matches=False,
        message="init process (PID 1) is not a supported Tini",
    )
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
    monkeypatch.setattr(sc, "verify_live_topology", lambda: proof)
    monkeypatch.setattr(
        sc,
        "prove_restart_authority",
        lambda _c, **_k: RestartAuthorityProof(
            ok=False, policy="always", source="contract-of-record", message="bad"
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


def test_startup_contract_command_writes_and_proves(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """'lubko-deploy startup-contract --write' publishes and proves the contract."""
    monkeypatch.setattr(sc, "contract_path", lambda: tmp_path / "startup-contract.json")
    monkeypatch.setattr(sc, "write_startup_launcher", lambda _b: None)
    monkeypatch.setattr(sc, "validate_startup_launcher", lambda _b: True)
    proof = TopologyProof(
        ok=True,
        contract_version=CONTRACT_SCHEMA_VERSION,
        init_pid=1,
        init_cmdline="/usr/bin/tini-static -- lubko-supervisor",
        init_is_tini=True,
        supervisor_pid=10,
        supervisor_cmdline="uv run lubko-supervisor",
        supervisor_present=True,
        supervisor_is_contract_binary=True,
        supervisor_under_init=True,
        supervisor_identity_matches=True,
        uses_sleep_placeholder=False,
        worker_pid=None,
        worker_is_direct_child=False,
        worker_identity_matches=True,
        message="startup contract satisfied (tini -> supervisor; no worker claimed yet)",
    )
    monkeypatch.setattr(sc, "verify_live_topology", lambda: proof)
    monkeypatch.setattr(
        sc,
        "prove_restart_authority",
        lambda _c, **_k: RestartAuthorityProof(
            ok=True, policy="always", source="contract-of-record", message="ok"
        ),
    )
    assert lifecycle.startup_contract_cmd(argparse.Namespace(write=True)) == lifecycle.EXIT_OK
    assert (tmp_path / "startup-contract.json").is_file()
    out = capsys.readouterr().out
    assert "startup contract version" in out
    assert "startup topology: OK" in out


def test_startup_contract_command_fails_on_unsupported(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A missing/unmatched contract makes 'lubko-deploy startup-contract' fail."""
    monkeypatch.setattr(sc, "contract_path", lambda: tmp_path / "startup-contract.json")
    proof = TopologyProof(
        ok=False,
        contract_version=CONTRACT_SCHEMA_VERSION,
        init_pid=1,
        init_cmdline="",
        init_is_tini=False,
        supervisor_pid=0,
        supervisor_cmdline="",
        supervisor_present=False,
        supervisor_is_contract_binary=False,
        supervisor_under_init=False,
        supervisor_identity_matches=False,
        uses_sleep_placeholder=False,
        worker_pid=None,
        worker_is_direct_child=False,
        worker_identity_matches=False,
        message="init process (PID 1) is not a supported Tini",
    )
    monkeypatch.setattr(sc, "verify_live_topology", lambda: proof)
    monkeypatch.setattr(
        sc,
        "prove_restart_authority",
        lambda _c, **_k: RestartAuthorityProof(
            ok=False, policy="always", source="contract-of-record", message="bad"
        ),
    )
    assert lifecycle.startup_contract_cmd(argparse.Namespace(write=False)) == lifecycle.EXIT_ERROR


def test_contract_is_frozen_and_current_matches_version() -> None:
    """The shipped contract is frozen and carries the current schema version."""
    assert isinstance(CURRENT_CONTRACT, StartupContract)
    assert CURRENT_CONTRACT.schema_version == CONTRACT_SCHEMA_VERSION
    assert "tini-static" in CURRENT_CONTRACT.init_markers
    assert "lubko-supervisor" in CURRENT_CONTRACT.supervisor_markers
    assert CURRENT_CONTRACT.restart_policy == "always"


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
