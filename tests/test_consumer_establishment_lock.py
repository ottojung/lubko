"""Deterministic invariants for the shared consumer-establishment boundary.

Manual ``lubko-deploy recover`` and the long-lived supervisor both decide
whether the sole queue consumer may be established. They share one
cross-process flock around the whole decision — recovery's
preflight-through-publication critical section and the supervisor's
gate-to-spawn critical section — so from one initially consumer-free state
exactly one path can authorize a spawn, no stale preflight observation can
outlive competing supervisor authority, and no supervisor state write can
erase an established manual recovery obligation and spawn beside it.
"""

from __future__ import annotations

import threading
from pathlib import Path

import pytest

from lubko import lifecycle, supervise, supervisor

COMMIT = "a" * 40


class FakeSpawnedProc:
    """Minimal ``Popen`` stand-in for a spawned recovery worker."""

    def __init__(self) -> None:
        """Assign a PID no live process can be proven from."""
        self.pid = 4_999_999


@pytest.fixture(autouse=True)
def state_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Use an isolated durable state root for each test.

    Returns:
        The supervisor state directory.
    """
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state"))
    supervise.state_path().parent.mkdir(parents=True, exist_ok=True)
    return supervise.state_path().parent


@pytest.fixture(autouse=True)
def fast_lock_poll(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make consumer-lock contention polling effectively instantaneous."""
    monkeypatch.setattr(supervise, "CONSUMER_LOCK_POLL_SECONDS", 0.001)


def _options(lock_timeout_seconds: float = 2.0) -> lifecycle.DeployOptions:
    """Build deployment options for a recovery run.

    Args:
        lock_timeout_seconds: How long recovery waits for the shared boundary.

    Returns:
        The deployment inputs.
    """
    return lifecycle.DeployOptions(
        repo=Path.cwd(),
        uv_path="uv",
        bootstrap=False,
        stop_grace_seconds=0.1,
        postgres_timeout_seconds=0.1,
        lock_timeout_seconds=lock_timeout_seconds,
        validation_timeout_seconds=1.0,
        git_timeout_seconds=1.0,
        cli_timeout_seconds=1.0,
    )


def _patch_happy_recovery(
    monkeypatch: pytest.MonkeyPatch,
    events: list[str],
) -> None:
    """Stub every external seam of the recovery path as successful.

    Args:
        monkeypatch: The pytest monkeypatch fixture.
        events: Ordered event log recording the authorized spawn.
    """

    def fake_spawn(_repo: Path, _uv_path: str, _log: Path, _env: dict[str, str]) -> FakeSpawnedProc:
        events.append("recovery_spawn")
        return FakeSpawnedProc()

    monkeypatch.setattr(lifecycle, "_recover_preflight", lambda _options: COMMIT)
    monkeypatch.setattr(lifecycle, "_resolve_stale_recovery_obligation", lambda: True)
    monkeypatch.setattr(lifecycle, "spawn_worker", fake_spawn)

    def fake_settle(
        _proc: FakeSpawnedProc,
        _options: lifecycle.DeployOptions,
        token: str,
        _commit: str,
        _worker_id: str,
    ) -> int:
        events.append(f"recovery_token:{token}")
        return lifecycle.EXIT_OK

    monkeypatch.setattr(lifecycle, "_settle_spawned_recovery_worker", fake_settle)


def test_stale_preflight_never_spawns_beside_a_concurrent_supervisor_decision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A preflight accepted before a concurrent supervisor decision spawns once.

    Recovery pauses immediately after its "no consumer" preflight; the
    supervisor then reaches its own establishment decision. With both paths
    sharing one serialization boundary, the supervisor cannot enter its
    gate-to-spawn critical section while recovery holds it, so exactly one
    spawn is authorized and the surviving durable authority belongs to that
    one consumer.
    """
    events: list[str] = []
    preflight_passed = threading.Event()
    resume_recovery = threading.Event()

    def gated_preflight(_options: lifecycle.DeployOptions) -> str:
        preflight_passed.set()
        assert resume_recovery.wait(timeout=10), "test orchestration stalled"
        return COMMIT

    _patch_happy_recovery(monkeypatch, events)
    monkeypatch.setattr(lifecycle, "_recover_preflight", gated_preflight)

    daemon = supervisor.SupervisorDaemon(supervisor.Settings(lock_timeout_seconds=0.05))
    monkeypatch.setattr(
        daemon,
        "_spawn_worker",
        lambda _commit: pytest.fail("a second queue consumer was authorized"),
    )

    recovery = threading.Thread(target=lifecycle._recover_locked, args=(_options(),))
    recovery.start()
    try:
        assert preflight_passed.wait(timeout=10)

        decision = threading.Thread(target=daemon._ensure_worker, args=(COMMIT,))
        decision.start()
        decision.join(timeout=10)
        assert not decision.is_alive()
        assert daemon._message is not None, "the supervisor held at the boundary"

        resume_recovery.set()
        recovery.join(timeout=10)
        assert not recovery.is_alive()
    finally:
        resume_recovery.set()
        recovery.join(timeout=10)

    assert events.count("recovery_spawn") == 1, "exactly one consumer was authorized"
    held = supervise.read_state().spawning
    assert held is not None, "the authorized consumer keeps its durable authority"
    assert held.token == next(
        event.split(":", 1)[1] for event in events if event.startswith("recovery_token:")
    )


def test_established_recovery_obligation_blocks_a_later_supervisor_spawn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Recovery establishing authority first makes the supervisor hold.

    Recovery is paused immediately after its spawning obligation is durably
    written while it still holds the shared boundary; the supervisor reaching
    ``_ensure_worker`` in that window must fail closed at the boundary without
    any durable state write that could overwrite the fresh obligation.
    """
    events: list[str] = []
    _patch_happy_recovery(monkeypatch, events)
    obligation_written = threading.Event()
    resume_recovery = threading.Event()
    real_write = lifecycle._write_spawning_obligation

    def gated_write(obligation: supervise.SpawningObligation) -> bool:
        written = real_write(obligation)
        if written:
            obligation_written.set()
            assert resume_recovery.wait(timeout=10), "test orchestration stalled"
        return written

    monkeypatch.setattr(lifecycle, "_write_spawning_obligation", gated_write)

    recovery = threading.Thread(target=lifecycle._recover_locked, args=(_options(),))
    recovery.start()
    try:
        assert obligation_written.wait(timeout=10)
        before = supervise.read_state().spawning
        assert before is not None

        daemon = supervisor.SupervisorDaemon(supervisor.Settings(lock_timeout_seconds=0.01))
        monkeypatch.setattr(
            daemon,
            "_spawn_worker",
            lambda _commit: pytest.fail("the supervisor spawned beside recovery authority"),
        )
        daemon._ensure_worker(COMMIT)

        after = supervise.read_state().spawning
        assert after == before, "the fresh recovery obligation was overwritten"
        assert supervise.read_state().child is None, "no second consumer was started"
        assert daemon._message is not None, "the supervisor held at the boundary"

        resume_recovery.set()
        recovery.join(timeout=10)
        assert not recovery.is_alive()
    finally:
        resume_recovery.set()
        recovery.join(timeout=10)

    assert events.count("recovery_spawn") == 1


def test_recovery_fails_closed_while_the_boundary_is_busy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Recovery cannot even start its preflight beside an active decision."""
    events: list[str] = []
    _patch_happy_recovery(monkeypatch, events)

    with supervise.consumer_lock(5.0):
        assert (
            lifecycle._recover_locked(_options(lock_timeout_seconds=0.01)) == lifecycle.EXIT_ERROR
        ), "recovery raced an in-flight establishment decision"

    assert events == [], "an unauthorized recovery worker was started"
    assert supervise.read_state().spawning is None, "no phantom authority was written"
