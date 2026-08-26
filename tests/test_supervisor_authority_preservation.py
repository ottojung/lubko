"""Deterministic invariants for supervisor state writes vs. consumer authority.

A supervisor state transition must never erase or replace a concurrently
established ``spawning``/consumer authority based on an observation made
outside the shared consumer-establishment critical section. Every supervisor
mutation that runs outside ``consumer_lock`` therefore publishes through a
serialized read-merge-write under that lock; these tests force the concurrent
authority write into exactly the window between the supervisor's stale
pre-read (or the publisher's fresh read) and its attempted publication and
prove the established recovery authority always survives.
"""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from lubko import lifecycle, supervise, supervisor
from lubko import worker as worker_mod
from lubko.supervise import (
    INTENT_RUN,
    SpawningObligation,
    WorkerChild,
    fresh_state,
    read_state,
    write_state,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from lubko.supervise import SupervisorState

COMMIT = "a" * 40
TOKEN = "c" * 32


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


def _obligation() -> SpawningObligation:
    """Build an exact recovery-worker obligation as manual recovery writes it.

    Returns:
        A replacement-blocking obligation carrying
        ``parent_death_signal=False`` so only positive convergence may ever
        resolve it.
    """
    return SpawningObligation(
        token=TOKEN,
        commit=COMMIT,
        creator_pid=4_000_000,
        creator_start_time_ticks=0,
        pid=None,
        start_time_ticks=None,
        created_at=0.0,
        boot_id=None,
        parent_death_signal=False,
    )


def _dead_child_state() -> SupervisorState:
    """Seed durable state with a recorded maintained child that has died.

    Returns:
        The seeded state (also written durably).
    """
    child = WorkerChild(
        pid=4_100_000,
        pgid=4_100_000,
        sid=4_100_000,
        start_time_ticks=42,
        token=TOKEN,
        worker_id="host",
        spawned_at=0.0,
    )
    state = replace(
        fresh_state(),
        mode=supervise.MODE_RUN,
        commit=COMMIT,
        child=child,
        intent=INTENT_RUN,
    )
    write_state(state)
    return state


def _racing_read_factory() -> tuple[Callable[[], SupervisorState], Callable[[], SupervisorState]]:
    """Build a ``read_state`` stand-in that establishes authority once.

    The returned reader behaves like the real one until its first
    consumer-free observation; that observation triggers the recovery
    critical section (which durably writes the exact-token obligation, as
    ``lubko-deploy recover`` does under ``consumer_lock``) and then returns
    the post-authority state.

    Returns:
        ``(reader, real_reader)`` where ``reader`` is installed in place of
        both :func:`lubko.supervise.read_state` and the daemon's binding.
    """
    armed = {"on": True}
    real_read = supervise.read_state

    def racing_read_state() -> SupervisorState:
        state = real_read()
        if armed["on"] and state.spawning is None:
            armed["on"] = False
            with_obligation = replace(state, spawning=_obligation())
            write_state(with_obligation)
            return with_obligation
        return state

    return racing_read_state, real_read


def _arm_authority_race(monkeypatch: pytest.MonkeyPatch) -> None:
    """Install the mid-flight authority-establishing reader globally.

    Args:
        monkeypatch: The pytest monkeypatch fixture.
    """
    racing_read_state, _ = _racing_read_factory()
    monkeypatch.setattr(supervise, "read_state", racing_read_state)
    monkeypatch.setattr(supervisor, "read_state", racing_read_state)


def test_crash_handling_never_erases_concurrently_established_recovery_authority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The crash path's late publication keeps a newer recovery obligation.

    ``_handle_crash`` receives a state snapshot read before crash handling
    and spends time recovering owned groups. When manual recovery completes
    its authority-establishing critical section during that window (between
    the stale pre-read and the final publication), the crash path's
    serialized publication must merge onto the newer durable authority
    instead of blindly replacing it with the stale snapshot.
    """
    state = _dead_child_state()
    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    daemon.proc = None

    def fake_recover(_token: str) -> None:
        # Recovery wins the shared boundary exactly here, between the
        # supervisor's initial snapshot and its final publication.
        _arm_authority_race(monkeypatch)

    monkeypatch.setattr(supervisor, "recover_owned_groups", fake_recover)

    daemon._handle_crash(state, now=100.0)

    final = read_state()
    assert final.spawning == _obligation(), (
        "the crash-state rewrite erased the recovery worker's authority"
    )
    assert final.child is None, "the dead child was still cleared"
    assert final.restart_count == 1, "ordinary crash bookkeeping still advanced"
    assert final.next_attempt_at is not None, "crash backoff was still scheduled"


def test_publication_is_serialized_against_a_held_consumer_boundary() -> None:
    """An authority-preserving publication waits for the shared boundary.

    While another process holds ``consumer_lock``, the supervisor's
    protected write must not publish at all: it fails closed with a lock
    timeout instead of slipping its stale snapshot past the concurrent
    authority transition. This proves real serialization, not merely a
    narrower race window.
    """
    state = _dead_child_state()

    with supervise.consumer_lock(5.0), pytest.raises(supervise.ConsumerLockTimeoutError):
        supervise.write_state_preserving_authority(
            replace(state, child=None),
            timeout_seconds=0.02,
        )

    assert read_state() == state, "the contended publication wrote anyway"


def test_no_authority_write_fits_between_fresh_read_and_publication(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The in-lock fresh read merges the newest authority before publishing.

    Recovery's authority write is forced to happen immediately before the
    publisher's fresh read inside the same critical section. Because the
    whole read-merge-write is serialized under ``consumer_lock``, no writer
    can slip between that read and the publication, and the published state
    carries the newest obligation rather than an observation of absence.
    """
    state = _dead_child_state()
    racing_read_state, _real_read = _racing_read_factory()
    observed_in_lock: list[SupervisorState] = []
    real_write = supervise.write_state

    def tracking_read() -> SupervisorState:
        observed = racing_read_state()
        observed_in_lock.append(observed)
        return observed

    def tracking_write(target: SupervisorState) -> None:
        # Simulate the competing writer firing after the publisher's fresh
        # read but "before" publication: only possible without the lock, so
        # under the lock this must already be reflected in what we merge.
        if all(state.spawning is None for state in observed_in_lock):
            pytest.fail("the publisher merged from a pre-authority observation")
        real_write(target)

    monkeypatch.setattr(supervise, "read_state", tracking_read)
    monkeypatch.setattr(supervise, "write_state", tracking_write)

    supervise.write_state_preserving_authority(replace(state, child=None), 5.0)

    assert any(state.spawning is not None for state in observed_in_lock), (
        "the fresh in-lock read did not observe the established authority"
    )
    final = read_state()
    assert final.spawning == _obligation(), (
        "an authority write between the fresh read and publication was lost"
    )
    assert final.child is None


def test_live_recovery_obligation_blocks_a_later_supervisor_spawn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """While a recovery obligation is live, no maintained replacement starts."""
    _dead_child_state()
    obligation = replace(_obligation(), pid=4_200_000, start_time_ticks=7)
    write_state(replace(read_state(), child=None, spawning=obligation))

    daemon = supervisor.SupervisorDaemon(supervisor.Settings(lock_timeout_seconds=0.05))
    monkeypatch.setattr(daemon, "_spawn_worker", lambda _commit: pytest.fail("spawned"))
    # Prove the exact obligation instance is treated as live and
    # unconvergeable, so its authority stays replacement-blocking.
    monkeypatch.setattr(daemon, "_unresolved_alive", lambda _hold: True)
    monkeypatch.setattr(daemon, "_converge_unresolved", lambda _hold: False)

    daemon._ensure_worker(COMMIT)

    final = read_state()
    assert final.spawning == obligation, "the live obligation was erased"
    assert final.child is None, "a second consumer was started"
    assert daemon._message is not None, "the supervisor held instead of spawning"


def test_ordinary_crash_handling_without_concurrency_still_converges(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With no competing authority, crash handling behaves exactly as before."""
    state = _dead_child_state()
    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    daemon.proc = None
    recovered: list[str] = []
    monkeypatch.setattr(supervisor, "recover_owned_groups", recovered.append)

    daemon._handle_crash(state, now=100.0)

    final = read_state()
    assert final.child is None
    assert final.restart_count == 1
    assert final.next_attempt_at is not None
    assert final.last_exit is not None, "the exit was still recorded"
    assert final.spawning is None
    assert recovered == [TOKEN]


def test_not_ready_probe_bookkeeping_preserves_newer_authority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Not-ready probe bookkeeping merges onto a newer recovery obligation.

    The probe result is decided from a snapshot taken before recovery
    established its authority; the serialized publication must keep the
    obligation and still record the readiness retry itself.
    """
    _dead_child_state()
    _arm_authority_race(monkeypatch)
    state = replace(
        fresh_state(),
        mode=supervise.MODE_RUN,
        commit=COMMIT,
        intent=INTENT_RUN,
    )
    write_state(state)
    daemon = supervisor.SupervisorDaemon(supervisor.Settings())

    daemon._record_not_ready(state, now=100.0, child_pid=4_100_000, reason="probe failed")

    final = read_state()
    assert final.spawning == _obligation(), "probe bookkeeping clobbered recovery authority"
    assert final.ready is False
    assert final.next_readiness_at is not None, "the readiness retry was still scheduled"


def test_child_clearing_outside_the_lock_keeps_newer_authority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``_clear_child`` cannot erase an obligation established mid-flight."""
    _dead_child_state()
    _arm_authority_race(monkeypatch)
    daemon = supervisor.SupervisorDaemon(supervisor.Settings())

    daemon._clear_child(0.0)

    final = read_state()
    assert final.child is None
    assert final.spawning == _obligation(), "the child-clearing write erased authority"


def test_normalize_cross_boot_state_defers_instead_of_clobbering(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Startup normalization holds off when the consumer boundary is busy."""
    monkeypatch.setattr(supervisor, "DEFAULT_LOCK_TIMEOUT_SECONDS", 0.02)
    state = _dead_child_state()
    write_state(replace(state, boot_id="previous-boot", next_attempt_at=5.0))
    baseline = read_state()

    with supervise.consumer_lock(5.0):
        supervisor.normalize_cross_boot_state()
        assert read_state() == baseline, "the deferred normalization wrote anyway"

    supervisor.normalize_cross_boot_state()
    final = read_state()
    assert final.boot_id == supervise.current_boot_id(), (
        "normalization converged once the boundary was free"
    )
    assert final.spawning is None


def test_malformed_hold_materialization_keeps_newer_authority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Even the corruption-materializing tick write preserves newer authority."""
    state = _dead_child_state()
    write_state(replace(state, ownership_hold_malformed=True))
    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    _arm_authority_race(monkeypatch)

    daemon.reconcile(now=0.0)

    final = read_state()
    assert final.ownership_hold_malformed, "the hold was dropped"
    assert final.spawning == _obligation(), "the materialization erased authority"


def test_all_out_of_lock_writers_route_through_the_protected_writer() -> None:
    """Structural audit: direct state writes stay confined to locked scopes.

    Every remaining direct ``write_state(...)`` call site in the supervisor
    module must sit inside a function whose execution happens under
    ``consumer_lock`` (or be the protected writer itself). This audit pins
    the allowlist so a future out-of-lock rewrite cannot quietly reintroduce
    the lost-update shape.
    """
    source = Path(supervisor.__file__).read_text(encoding="utf-8")
    locked_methods = {
        "_ensure_consumer_locked",
        "_spawn_worker",
        "_settle_unproven_spawn",
        "_recover_unpublished_spawn",
        "_release_unproven_spawn_authority",
        "_record_proven_private_child",
        "_persist_unobservable_hold",
        "_record_shared_group_hold",
        "_resolve_spawning_obligation",
        "_resolve_pidless_spawn",
        "_resolve_identified_spawn",
        "_resolve_unresolved_child",
        "_shutdown_locked",
        "_retire_child",
        "_preserve_blocking_obligation",
    }
    offenders: list[str] = []
    current = ""
    for raw_line in source.splitlines():
        stripped = raw_line.strip()
        if stripped.startswith("def "):
            current = stripped.split("(")[0].removeprefix("def ")
        if (
            "write_state(" in stripped
            and "preserving" not in stripped
            and current not in locked_methods
        ):
            offenders.append(f"{current}: {stripped}")
    assert not offenders, f"out-of-lock state writes found: {offenders}"


class _FakeExitedProc:
    """Minimal exited-``Popen`` stand-in for crash handling."""

    poll = staticmethod(lambda: 1)


def _exit_handle(daemon: supervisor.SupervisorDaemon) -> object:
    """Read the daemon's exit handle without flow-narrowing its type.

    ``_handle_crash`` mutates ``daemon.proc``, but mypy cannot see that, so
    asserting on the typed attribute directly would make the post-call
    ``is None`` check unreachable. Reading through this helper keeps the
    observation non-narrowing.

    Args:
        daemon: The daemon whose exit handle is observed.

    Returns:
        The current exit handle (or ``None``).
    """
    return daemon.proc


def test_deferred_desired_publication_spawns_and_applies_nothing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A deferred desired-state publication starts no worker and applies nothing."""
    write_state(fresh_state())
    supervise.request_run(COMMIT, repo="repo", uv_path="uv", worker_id=None)
    daemon = supervisor.SupervisorDaemon(supervisor.Settings(lock_timeout_seconds=0.02))
    monkeypatch.setattr(
        daemon,
        "_spawn_worker",
        lambda _commit: pytest.fail("a worker was started from an unpublished intent"),
    )

    with supervise.consumer_lock(5.0):
        daemon.reconcile(now=0.0)
        final = read_state()
        assert final.applied_generation == 0, "an unpublished generation was applied"
        assert final.child is None
        assert final.spawning is None
        assert daemon._message is not None
        assert "deferring" in daemon._message.lower()

    monkeypatch.setattr(daemon, "_spawn_worker", lambda _commit: None)
    daemon.reconcile(now=0.0)
    assert read_state().applied_generation == 1, (
        "the intent never converged once the boundary was free"
    )


def test_deferred_readiness_publication_reports_no_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Readiness success is only reported once ``ready=True`` is durable."""
    _dead_child_state()
    daemon = supervisor.SupervisorDaemon(supervisor.Settings(lock_timeout_seconds=0.02))
    monkeypatch.setattr(daemon, "_check_readiness", lambda _child, _cwd: (True, "ok"))
    monkeypatch.setattr(daemon, "_child_alive", lambda _s: True)
    published: list[str] = []
    pruned: list[str] = []
    deploy_log: list[str] = []
    monkeypatch.setattr(supervisor, "publish_current_surfaces", published.append)
    monkeypatch.setattr(supervisor, "prune_old_incarnation_artifacts", pruned.append)
    monkeypatch.setattr(lifecycle, "append_deploy_log", deploy_log.append)

    with supervise.consumer_lock(5.0):
        daemon._probe_readiness(now=0.0)
        assert read_state().ready is False, "readiness was persisted while deferred"
        assert pruned == [], "incarnation pruning ran on an unpublished readiness"
        assert deploy_log == [], "success was logged without a durable ready record"

    daemon._probe_readiness(now=0.0)
    assert read_state().ready is True
    # The stable surfaces are republished on each probe; success side
    # effects happen exactly once, only after ``ready=True`` is durable.
    assert published[-1] == TOKEN
    assert len(pruned) == 1
    assert len(deploy_log) == 1


def test_deferred_backoff_reset_reports_no_stability(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Backoff stability is only reported once the reset is durable."""
    state = replace(
        _dead_child_state(),
        restart_count=3,
        next_attempt_at=50.0,
        last_spawn_at=0.0,
    )
    write_state(state)
    daemon = supervisor.SupervisorDaemon(supervisor.Settings(lock_timeout_seconds=0.02))
    monkeypatch.setattr(daemon, "_child_alive", lambda _s: True)

    with supervise.consumer_lock(5.0):
        daemon._maybe_reset_backoff(replace(state), now=100.0)
        final = read_state()
        assert final.restart_count == 3, "crash history was reset while deferred"
        assert final.next_attempt_at is not None, "the backoff deadline was cleared while deferred"

    daemon._maybe_reset_backoff(replace(state), now=100.0)
    final = read_state()
    assert final.restart_count == 0
    assert final.next_attempt_at is None


def test_deferred_crash_record_keeps_the_exit_handle_and_silence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A deferred crash publication keeps the handle and reports nothing."""
    state = _dead_child_state()
    daemon = supervisor.SupervisorDaemon(supervisor.Settings(lock_timeout_seconds=0.02))
    monkeypatch.setattr(daemon, "proc", _FakeExitedProc())
    recovered: list[str] = []
    deploy_log: list[str] = []
    monkeypatch.setattr(supervisor, "recover_owned_groups", recovered.append)
    monkeypatch.setattr(lifecycle, "append_deploy_log", deploy_log.append)

    with supervise.consumer_lock(5.0):
        daemon._handle_crash(state, now=100.0)
        assert read_state().child is not None, "the crash was recorded while deferred"
        # Read through the helper so mypy does not narrow the typed
        # attribute across the mutating _handle_crash call.
        assert _exit_handle(daemon) is not None, "the exit handle was dropped unpublished"
        assert deploy_log == [], "the crash was reported without being recorded"

    daemon._handle_crash(state, now=100.0)
    assert read_state().child is None
    assert _exit_handle(daemon) is None
    assert len(deploy_log) == 1


def test_deferred_out_of_lock_retirement_is_not_reported_converged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retirement outside the lock fails closed when its write was deferred."""
    _dead_child_state()
    daemon = supervisor.SupervisorDaemon(supervisor.Settings(lock_timeout_seconds=0.02))
    daemon.proc = None
    monkeypatch.setattr(lifecycle, "stop_worker", lambda *_a, **_k: True)
    monkeypatch.setattr(worker_mod, "drain_sentinel_matches", lambda _token: True)

    with supervise.consumer_lock(5.0):
        assert daemon._retire_child() is False, (
            "retirement claimed convergence on an unpublished write"
        )
        assert read_state().child is not None, "the child identity was cleared unpublished"

    assert daemon._retire_child() is True
    assert read_state().child is None
