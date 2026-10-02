"""Deterministic invariants for applying an explicit run intent.

A run intent that requires replacing the current worker must never advance
durable authority while the required exact-child retirement has not positively
converged; otherwise a still-live old worker would be reclassified as running
the requested commit merely by rewriting state first.
"""

from __future__ import annotations

import math
from dataclasses import replace
from types import SimpleNamespace
from typing import TYPE_CHECKING, cast

import pytest

from lubko import cli, lifecycle_authority, supervise, supervisor
from lubko.supervisor import Settings, SupervisorDaemon
from tests._fake_authority_db import claim_every_daemon, seed_db_worker

if TYPE_CHECKING:
    import subprocess
    from pathlib import Path

OLD = "1" * 40
NEW = "2" * 40
_DB_CLAIM_SERVER = "srv-apply-desired-test"


@pytest.fixture(autouse=True)
def _db_fencing_claim(monkeypatch: pytest.MonkeyPatch) -> None:
    """Establish a fake-database fencing claim on every daemon under test.

    Steady-state decisions require canonical database authority; local
    caches alone never authorize action.
    """
    claim_every_daemon(monkeypatch, supervisor, _DB_CLAIM_SERVER)


def child(pid: int) -> supervise.WorkerChild:
    """Return an exact child identity recorded for ``pid``."""
    return supervise.WorkerChild(
        pid=pid,
        pgid=pid,
        sid=pid,
        start_time_ticks=pid,
        token=f"token-{pid}",
        worker_id="w",
        spawned_at=0.0,
    )


def desired(
    generation: int, commit: str, *, restart: bool = False
) -> supervise.SupervisorDesired:
    """Return a run intent for the commit with optional forced replacement."""
    return supervise.SupervisorDesired(
        schema_version=supervise.SCHEMA_VERSION,
        generation=generation,
        commit=commit,
        repo="/workspace/repo",
        uv_path="uv",
        worker_id=None,
        restart=restart,
    )


@pytest.fixture
def daemon(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[SupervisorDaemon, list[str]]:
    """Build an isolated daemon recording every replacement spawn attempt.

    Returns:
        The daemon plus the ordered list of commits handed to
        ``_ensure_worker`` in place of real spawning.
    """
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state"))
    spawns: list[str] = []
    daemon = SupervisorDaemon(Settings())
    monkeypatch.setattr(daemon, "_ensure_worker", spawns.append)
    return daemon, spawns


def live_old_worker() -> None:
    """Persist durable authority for a live old-commit worker."""
    supervise.write_state(
        replace(
            supervise.fresh_state(),
            mode=supervise.MODE_RUN,
            applied_generation=1,
            commit=OLD,
            child=child(4242),
        )
    )


def publish_live_old_worker(monkeypatch: pytest.MonkeyPatch, daemon: SupervisorDaemon) -> None:
    """Publish the canonical DB record and live direct-child view for the worker.

    Same-commit settlement keeps the worker only when the canonical row's
    WorkerRecord names the exact live direct child.

    Args:
        monkeypatch: The active monkeypatch fixture.
        daemon: The daemon under test.
    """
    seed_db_worker(
        daemon,
        lifecycle_authority.WorkerRecord(
            token=f"token-{4242}",
            commit=OLD,
            pid=4242,
            pgid=4242,
            sid=4242,
            start_time_ticks=4242,
            worker_id="w",
        ),
    )
    daemon._active_child = supervise.WorkerChild(
        pid=4242,
        pgid=4242,
        sid=4242,
        start_time_ticks=4242,
        token=f"token-{4242}",
        worker_id="w",
        spawned_at=0.0,
    )
    daemon.proc = cast("subprocess.Popen[bytes]", SimpleNamespace(pid=4242, poll=lambda: None))
    monkeypatch.setattr(supervisor, "proc_start_ticks", lambda pid: 4242 if pid == 4242 else None)


@pytest.mark.usefixtures("supervisor_token")
def test_failed_retirement_holds_authority_and_spawns_nothing(
    daemon: tuple[SupervisorDaemon, list[str]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed retirement never advances generation/commit nor spawns."""
    dc, spawns = daemon
    live_old_worker()
    monkeypatch.setattr(dc, "_retire_child", lambda: False)
    monkeypatch.setattr(type(dc), "_child_alive", staticmethod(lambda _state: True))

    dc._apply_desired(desired(2, NEW))

    state = supervise.read_state()
    assert state.applied_generation == 1, "generation did not advance"
    assert state.commit == OLD, "maintained commit kept its authority"
    assert state.child is not None, "old child preserved"
    assert state.child.pid == 4242
    assert spawns == [], "no replacement was authorized"
    assert state.next_attempt_at is not None, "a retry hold was recorded"


@pytest.mark.usefixtures("supervisor_token")
def test_retry_after_transient_retirement_failure_applies_normally(
    daemon: tuple[SupervisorDaemon, list[str]],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Once retirement converges, a later reconciliation applies the intent."""
    dc, spawns = daemon
    build_runtime_with_maintained_entry_points(monkeypatch, tmp_path, NEW)
    live_old_worker()
    outcomes = iter([False, True])
    monkeypatch.setattr(dc, "_retire_child", lambda: next(outcomes))
    monkeypatch.setattr(type(dc), "_child_alive", staticmethod(lambda _state: True))

    dc._apply_desired(desired(2, NEW))
    dc._apply_desired(desired(2, NEW))

    assert spawns == [NEW], "the successful retirement authorized exactly one replacement"
    state = supervise.read_state()
    assert state.applied_generation == 2
    assert state.commit == NEW
    assert state.next_attempt_at is None, "the transient hold cleared"


@pytest.mark.usefixtures("supervisor_token")
def test_same_commit_non_restart_settlement_keeps_live_worker(
    daemon: tuple[SupervisorDaemon, list[str]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A same-commit non-restart intent settles without retiring or spawning."""
    dc, spawns = daemon
    live_old_worker()
    supervise.write_state(replace(supervise.read_state(), ready=True, next_readiness_at=None))
    publish_live_old_worker(monkeypatch, dc)
    retire_calls: list[bool] = []
    monkeypatch.setattr(type(dc), "_child_alive", staticmethod(lambda _state: True))

    def record_retire() -> bool:
        retire_calls.append(True)
        return True

    monkeypatch.setattr(dc, "_retire_child", record_retire)

    dc._apply_desired(desired(3, OLD))

    assert retire_calls == [], "settlement never disturbs the confirmed worker"
    assert spawns == [], "settlement never spawns"
    state = supervise.read_state()
    assert state.applied_generation == 3
    assert state.commit == OLD
    assert state.child is not None, "worker untouched"
    assert state.child.pid == 4242
    assert state.ready is True, "same-worker settlement preserves the proven readiness"
    assert state.next_readiness_at is None


@pytest.mark.usefixtures("supervisor_token")
def test_same_commit_restart_replaces_live_worker(
    daemon: tuple[SupervisorDaemon, list[str]],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An explicit restart replaces even an exact same-commit live worker."""
    dc, spawns = daemon
    build_runtime_with_maintained_entry_points(monkeypatch, tmp_path, OLD)
    live_old_worker()
    supervise.write_state(replace(supervise.read_state(), ready=True))
    publish_live_old_worker(monkeypatch, dc)
    retire_calls: list[bool] = []
    monkeypatch.setattr(type(dc), "_child_alive", staticmethod(lambda _state: True))

    def record_retire() -> bool:
        retire_calls.append(True)
        state = supervise.read_state()
        supervise.write_state(replace(state, child=None, ready=False))
        return True

    monkeypatch.setattr(dc, "_retire_child", record_retire)

    dc._apply_desired(desired(3, OLD, restart=True))

    state = supervise.read_state()
    assert retire_calls == [True]
    assert spawns == [OLD]
    assert state.applied_generation == 3
    assert state.commit == OLD
    assert state.ready is False


@pytest.mark.usefixtures("supervisor_token")
def test_same_commit_settlement_preserves_existing_not_ready_retry(
    daemon: tuple[SupervisorDaemon, list[str]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Settlement never fabricates readiness for an unready same-commit worker."""
    dc, spawns = daemon
    live_old_worker()
    supervise.write_state(replace(supervise.read_state(), ready=False, next_readiness_at=123.0))
    publish_live_old_worker(monkeypatch, dc)
    monkeypatch.setattr(type(dc), "_child_alive", staticmethod(lambda _state: True))

    dc._apply_desired(desired(3, OLD))

    state = supervise.read_state()
    assert spawns == []
    assert state.ready is False
    assert state.next_readiness_at is not None
    assert math.isclose(state.next_readiness_at, 123.0)


def build_runtime_with_maintained_entry_points(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, commit: str
) -> None:
    """Materialize a sealed runtime exposing only the maintained entry points.

    Args:
        monkeypatch: The active monkeypatch fixture.
        tmp_path: Per-test temporary directory.
        commit: Exact commit hash to materialize.
    """

    def fake_sync(_uv_path: str, root: Path, _timeout_seconds: float) -> None:
        """Create only the entry points the target's own tree declares."""
        bin_dir = root / ".venv" / "bin"
        bin_dir.mkdir(parents=True, exist_ok=True)
        for entry in cli.ENTRY_POINTS:
            script = bin_dir / entry
            script.write_text(f"#!/bin/sh\necho {entry}\n", encoding="utf-8")
            script.chmod(0o755)

    monkeypatch.setattr(cli, "_sync_venv", fake_sync)
    monkeypatch.setattr(
        cli,
        "_extract_archive",
        lambda _repo, _commit, _destination, _timeout_seconds: None,
    )
    cli.build_cli_root(tmp_path / "repo", commit, "uv", 60.0)


@pytest.mark.usefixtures("supervisor_token")
def test_uninstantiable_runtime_never_retires_the_live_worker(
    daemon: tuple[SupervisorDaemon, list[str]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A candidate this supervisor cannot launch leaves the worker consuming."""
    dc, spawns = daemon
    live_old_worker()
    retire_calls: list[bool] = []
    monkeypatch.setattr(type(dc), "_child_alive", staticmethod(lambda _state: True))

    def record_retire() -> bool:
        retire_calls.append(True)
        return True

    monkeypatch.setattr(dc, "_retire_child", record_retire)

    dc._apply_desired(desired(2, NEW))

    assert retire_calls == [], "the known-good worker must not be retired first"
    assert spawns == [], "no replacement was authorized"
    state = supervise.read_state()
    assert state.applied_generation == 1, "generation did not advance"
    assert state.commit == OLD, "maintained commit kept its authority"
    assert state.child is not None, "the previous worker keeps consuming"
    assert state.child.pid == 4242
    assert state.next_attempt_at is not None, "a retry hold was recorded"


@pytest.mark.usefixtures("supervisor_token")
def test_predecessor_entry_point_superset_upgrades_without_workerless_interval(
    daemon: tuple[SupervisorDaemon, list[str]],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A predecessor requiring extra entry points upgrades after a pre-handoff check.

    The running supervisor's own requirements are a strict superset of the
    maintained set, so the candidate must be validated against that superset --
    and the known-good worker must still be alive when that answer is sought.
    """
    dc, spawns = daemon
    build_runtime_with_maintained_entry_points(monkeypatch, tmp_path, NEW)
    # The running supervisor is a deployed predecessor: its own requirement set
    # is the maintained set plus the names it was deployed with.
    monkeypatch.setattr(cli, "ENTRY_POINTS", (*cli.ENTRY_POINTS, *cli.RETIRED_ENTRY_POINTS))
    live_old_worker()
    events: list[str] = []
    real_satisfies = cli.runtime_satisfies
    monkeypatch.setattr(type(dc), "_child_alive", staticmethod(lambda _state: True))

    def record_retire() -> bool:
        events.append("retire")
        return True

    def record_compatibility(commit: str, required: frozenset[str]) -> bool:
        events.append("pre-handoff-check")
        return real_satisfies(commit, required)

    monkeypatch.setattr(dc, "_retire_child", record_retire)
    monkeypatch.setattr(cli, "runtime_satisfies", record_compatibility)

    dc._apply_desired(desired(2, NEW))

    assert events == ["pre-handoff-check", "retire"], (
        "compatibility must be established before the known-good worker retires"
    )
    assert spawns == [NEW], "the upgrade proceeds without a workerless interval"
    state = supervise.read_state()
    assert state.applied_generation == 2
    assert state.commit == NEW
    assert state.next_attempt_at is None
