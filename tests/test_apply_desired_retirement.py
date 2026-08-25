"""Deterministic invariants for applying an explicit run intent.

A run intent that requires replacing the current worker must never advance
durable authority while the required exact-child retirement has not positively
converged; otherwise a still-live old worker would be reclassified as running
the requested commit merely by rewriting state first.
"""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING

import pytest

from lubko import supervise
from lubko.supervisor import Settings, SupervisorDaemon

if TYPE_CHECKING:
    from pathlib import Path

OLD = "1" * 40
NEW = "2" * 40


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


def desired(generation: int, commit: str) -> supervise.SupervisorDesired:
    """Return a plain (non-restart) run intent for ``commit``."""
    return supervise.SupervisorDesired(
        schema_version=supervise.SCHEMA_VERSION,
        generation=generation,
        commit=commit,
        repo="/workspace/repo",
        uv_path="uv",
        worker_id=None,
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


def test_retry_after_transient_retirement_failure_applies_normally(
    daemon: tuple[SupervisorDaemon, list[str]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Once retirement converges, a later reconciliation applies the intent."""
    dc, spawns = daemon
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


def test_same_commit_non_restart_settlement_keeps_live_worker(
    daemon: tuple[SupervisorDaemon, list[str]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A same-commit non-restart intent settles without retiring or spawning."""
    dc, spawns = daemon
    live_old_worker()
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
