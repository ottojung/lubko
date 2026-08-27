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
    ProcessInfo,
    StartupContract,
    StartupContractError,
    TopologyProof,
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


def test_valid_topology_proves_full_chain() -> None:
    """A supported tini -> supervisor -> worker chain proves the contract."""
    proof = sc.evaluate_topology(
        _valid_processes(),
        init_pid=1,
        supervisor_pid=10,
        worker_pid=20,
    )
    assert proof.ok is True
    assert proof.init_is_tini is True
    assert proof.supervisor_under_init is True
    assert proof.supervisor_is_contract_binary is True
    assert proof.uses_sleep_placeholder is False
    assert proof.worker_is_direct_child is True
    assert "tini -> supervisor -> worker" in proof.message


def test_topology_without_worker_is_ok() -> None:
    """No worker is claimed yet still satisfies the contract."""
    processes = _valid_processes()
    del processes[20]
    proof = sc.evaluate_topology(processes, init_pid=1, supervisor_pid=10, worker_pid=None)
    assert proof.ok is True
    assert proof.worker_pid is None
    assert proof.worker_is_direct_child is False
    assert "no worker claimed yet" in proof.message


def test_init_not_tini_rejected() -> None:
    """A non-Tini init leaves the supervisor without restart authority."""
    processes = _valid_processes()
    processes[1] = _proc(1, 0, "/sbin/init")
    proof = sc.evaluate_topology(processes, init_pid=1, supervisor_pid=10, worker_pid=20)
    assert proof.ok is False
    assert proof.init_is_tini is False
    assert "not a supported Tini" in proof.message


def test_sleep_infinity_placeholder_rejected() -> None:
    """The legacy 'sleep infinity' placeholder is never accepted as the supervisor."""
    processes = _valid_processes()
    processes[10] = _proc(10, 1, "/usr/bin/tini-static -- sleep infinity")
    proof = sc.evaluate_topology(processes, init_pid=1, supervisor_pid=10, worker_pid=20)
    assert proof.ok is False
    assert proof.uses_sleep_placeholder is True
    assert proof.supervisor_is_contract_binary is False
    assert "sleep infinity" in proof.message


def test_supervisor_not_under_init_rejected() -> None:
    """The supervisor must be the direct child of the Tini init process."""
    processes = _valid_processes()
    processes[10] = _proc(10, 5, "/usr/local/bin/uv run lubko-supervisor")
    proof = sc.evaluate_topology(processes, init_pid=1, supervisor_pid=10, worker_pid=20)
    assert proof.ok is False
    assert proof.supervisor_under_init is False
    assert "not the direct child of the Tini init" in proof.message


def test_worker_not_direct_child_rejected() -> None:
    """The worker must be the supervisor's direct child, not reparented."""
    processes = _valid_processes()
    processes[20] = _proc(20, 7, "/usr/local/bin/uv run lubko-worker")
    proof = sc.evaluate_topology(processes, init_pid=1, supervisor_pid=10, worker_pid=20)
    assert proof.ok is False
    assert proof.worker_is_direct_child is False
    assert "not the direct child of the supervisor" in proof.message


def test_zombie_supervisor_not_present() -> None:
    """A zombie supervisor cannot satisfy the contract."""
    processes = _valid_processes()
    processes[10] = _proc(10, 1, "/usr/local/bin/uv run lubko-supervisor", zombie=True)
    proof = sc.evaluate_topology(processes, init_pid=1, supervisor_pid=10, worker_pid=20)
    assert proof.ok is False
    assert proof.supervisor_is_contract_binary is False
    assert "absent or a zombie" in proof.message


def test_unknown_supervisor_binary_rejected() -> None:
    """A non-supervisor binary under Tini fails the contract."""
    processes = _valid_processes()
    processes[10] = _proc(10, 1, "/usr/bin/some-other-daemon")
    proof = sc.evaluate_topology(processes, init_pid=1, supervisor_pid=10, worker_pid=20)
    assert proof.ok is False
    assert proof.supervisor_is_contract_binary is False
    assert "not the supported lubko-supervisor binary" in proof.message


def test_contract_round_trip_and_version_check(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The contract artifact round-trips and corruption is hidden as absence."""
    monkeypatch.setattr(sc, "contract_path", lambda: tmp_path / "startup-contract.json")
    sc.write_contract()
    loaded = sc.read_contract()
    assert loaded is not None
    assert loaded.schema_version == CONTRACT_SCHEMA_VERSION
    assert loaded.worker_relationship == "direct-child"
    assert loaded.restart_authority == "init-restarts-supervisor-on-exit"
    (tmp_path / "startup-contract.json").write_text("{not json", encoding="utf-8")
    assert sc.read_contract() is None


def test_contract_version_mismatch_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An unsupported contract version fails closed on the strict reader."""
    monkeypatch.setattr(sc, "contract_path", lambda: tmp_path / "startup-contract.json")
    (tmp_path / "startup-contract.json").write_text(
        '{"schema_version": 999, "init_markers": ["tini"], "supervisor_markers": '
        '["lubko-supervisor"], "supervisor_command": ["lubko-supervisor"], '
        '"worker_relationship": "direct-child", '
        '"restart_authority": "init-restarts-supervisor-on-exit"}',
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


def test_live_topology_unproven_without_supervisor(monkeypatch: pytest.MonkeyPatch) -> None:
    """No recorded supervisor identity means the topology cannot be proven."""
    monkeypatch.setattr(sc, "read_supervisor_pid", lambda: None)
    proof = sc.verify_live_topology()
    assert proof.ok is False
    assert "no supervisor identity" in proof.message


def test_live_topology_uses_exact_process_table(monkeypatch: pytest.MonkeyPatch) -> None:
    """The live proof binds the supervisor to the recorded exact process table."""
    monkeypatch.setattr(sc, "read_supervisor_pid", lambda: (10, 1010))
    monkeypatch.setattr(sc, "supervisor_running", lambda: True)
    monkeypatch.setattr(sc, "read_status", lambda: None)
    monkeypatch.setattr(sc, "read_process_info", lambda pid: _valid_processes().get(pid))
    proof = sc.verify_live_topology()
    assert proof.ok is True
    assert proof.supervisor_pid == 10
    assert proof.worker_pid is None


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
    assert proof.worker_is_direct_child is True


def test_status_command_reports_contract(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """'lubko-deploy status' surfaces the contract version and topology proof."""
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
        uses_sleep_placeholder=False,
        worker_pid=20,
        worker_is_direct_child=True,
        message="startup contract satisfied (tini -> supervisor -> worker direct child)",
    )
    monkeypatch.setattr(sc, "verify_live_topology", lambda: proof)
    monkeypatch.setattr(sc, "read_contract", lambda: CURRENT_CONTRACT)
    monkeypatch.setattr(supervise, "supervisor_running", lambda: True)
    monkeypatch.setattr(supervise, "read_status", lambda: None)
    monkeypatch.setattr(lifecycle, "read_meta", lambda: None)
    monkeypatch.setattr(lifecycle, "worker_state", lambda _meta: "stopped")
    assert lifecycle.status_cmd() == lifecycle.EXIT_OK
    out = capsys.readouterr().out
    assert "startup contract: version" in out
    assert "startup topology: OK" in out


def test_startup_contract_command_writes_and_proves(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """'lubko-deploy startup-contract --write' publishes and proves the contract."""
    monkeypatch.setattr(sc, "contract_path", lambda: tmp_path / "startup-contract.json")
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
        uses_sleep_placeholder=False,
        worker_pid=None,
        worker_is_direct_child=False,
        message="startup contract satisfied (tini -> supervisor; no worker claimed yet)",
    )
    monkeypatch.setattr(sc, "verify_live_topology", lambda: proof)

    assert lifecycle.startup_contract_cmd(argparse.Namespace(write=True)) == lifecycle.EXIT_OK
    assert (tmp_path / "startup-contract.json").is_file()
    out = capsys.readouterr().out
    assert "startup contract version" in out
    assert "startup topology: OK" in out


def test_startup_contract_command_fails_on_unsupported(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unsupported topology makes 'lubko-deploy startup-contract' exit non-zero."""
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
        uses_sleep_placeholder=False,
        worker_pid=None,
        worker_is_direct_child=False,
        message="init process (PID 1) is not a supported Tini",
    )
    monkeypatch.setattr(sc, "verify_live_topology", lambda: proof)

    assert lifecycle.startup_contract_cmd(argparse.Namespace(write=False)) == lifecycle.EXIT_ERROR


def test_contract_is_frozen_and_current_matches_version() -> None:
    """The shipped contract is frozen and carries the current schema version."""
    assert isinstance(CURRENT_CONTRACT, StartupContract)
    assert CURRENT_CONTRACT.schema_version == CONTRACT_SCHEMA_VERSION
    assert "tini-static" in CURRENT_CONTRACT.init_markers
    assert "lubko-supervisor" in CURRENT_CONTRACT.supervisor_markers
