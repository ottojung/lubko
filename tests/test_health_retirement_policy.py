"""Supervisor retirement policy: degraded readiness versus authority to signal.

An overdue maintenance scan caused by a slow or stalled database is a
*degraded readiness* condition: it withdraws readiness and never authorizes
signalling a live worker.  Only an immediate safety breach (negative lease
safety) or a sustained, independently verified absence of forward progress may
retire a live incarnation, and the latter is published through the same
bounded crash-style backoff, so a repeatedly failing worker backs off instead
of being replaced in a tight loop.

The forward-progress evidence used here is a real queue roundtrip: a worker
that consumes the queue is making progress no matter how late its scans are,
and a probe the supervisor could not perform at all says nothing about the
worker.  Signalling a live, consuming worker is also destructive of work the
queue has already assigned: the worker's shutdown path fails every in-flight
spawn (``worker shutting down before spawn completed``, asserted in
``tests/test_spawn_async.py``), which is why retirement is the last resort.
"""

from __future__ import annotations

import os
import time
from dataclasses import replace
from typing import TYPE_CHECKING

import pytest

from lubko import lifecycle, supervise, supervisor
from lubko.health import (
    EVIDENCE_RETENTION_SECONDS,
    MAX_RETAINED_EVIDENCE_FILES,
    WORKER_HEALTH_SCHEMA_VERSION,
    WorkerHealth,
    interpret_operational_readiness,
    prune_old_incarnation_artifacts,
)
from lubko.lifecycle import QueueConsumption
from lubko.supervise import proc_start_ticks

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

COMMIT = "a" * 40
CURRENT_TOKEN = "f" * 32


def _child(token: str = CURRENT_TOKEN) -> supervise.WorkerChild:
    """Return the worker child identity the daemon under test maintains.

    Args:
        token: Incarnation token for the child.

    Returns:
        A worker child bound to the live test process.
    """
    ticks = proc_start_ticks(os.getpid())
    assert ticks is not None
    return supervise.WorkerChild(
        pid=os.getpid(),
        pgid=os.getpid(),
        sid=os.getpid(),
        start_time_ticks=ticks,
        token=token,
        worker_id="test-worker",
        spawned_at=time.time(),
    )


def _probe(
    *,
    ready: bool,
    reason: str,
    consumption: QueueConsumption,
) -> supervisor.ReadinessProbe:
    """Build one readiness observation.

    Args:
        ready: Whether the worker is queue-ready and operationally healthy.
        reason: Explanation of the observation.
        consumption: Conclusive classification of the queue roundtrip.

    Returns:
        The readiness observation.
    """
    return supervisor.ReadinessProbe(ready=ready, reason=reason, consumption=consumption)


def _overdue_probe() -> supervisor.ReadinessProbe:
    """Return a probe of a worker that consumes the queue but is scan-overdue.

    Returns:
        The readiness observation of a degrading but progressing worker.
    """
    return _probe(
        ready=False,
        reason="worker operational not ready: overdue scans: cancellation, recovery, gc",
        consumption=QueueConsumption.CONSUMED,
    )


def _unresponsive_probe() -> supervisor.ReadinessProbe:
    """Return a conclusive observation that the worker did not consume the queue.

    Returns:
        The readiness observation of a worker that provably made no progress.
    """
    return _probe(
        ready=False,
        reason="queue consumption not proven",
        consumption=QueueConsumption.NOT_CONSUMED,
    )


def _indeterminate_probe() -> supervisor.ReadinessProbe:
    """Return an observation the supervisor could not complete at all.

    Returns:
        The inconclusive readiness observation.
    """
    return _probe(
        ready=False,
        reason="queue consumption indeterminate: the probe itself could not be performed",
        consumption=QueueConsumption.INDETERMINATE,
    )


def _state(
    child: supervise.WorkerChild,
    *,
    ready: bool,
    next_readiness_at: float | None = None,
) -> supervise.SupervisorState:
    """Build a supervisor state observing one worker child.

    Args:
        child: The maintained worker child.
        ready: Whether readiness was proven for that child.
        next_readiness_at: Monotonic deadline of the next readiness probe.

    Returns:
        The supervisor state.
    """
    return replace(
        supervise.fresh_state(),
        commit=COMMIT,
        child=child,
        ready=ready,
        next_readiness_at=next_readiness_at,
    )


def _wire_daemon(
    monkeypatch: pytest.MonkeyPatch,
    daemon: supervisor.SupervisorDaemon,
    probe: supervisor.ReadinessProbe,
    state_provider: Callable[[], supervise.SupervisorState],
) -> None:
    """Wire a daemon to observe one fixed readiness result.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
        daemon: The daemon under test.
        probe: The observation ``_check_readiness`` returns.
        state_provider: Callable serving the durable state under test.
    """
    monkeypatch.setattr(supervisor, "read_state", state_provider)
    monkeypatch.setattr(daemon, "_child_alive", lambda _state: True)
    monkeypatch.setattr(daemon, "_check_readiness", lambda _child, _cwd: probe)


def _evidence(daemon: supervisor.SupervisorDaemon) -> supervisor._NoProgressEvidence | None:
    """Read the daemon's absent-progress record without narrowing its type.

    ``_probe_readiness`` resets the record, but mypy keeps the narrowing
    introduced by an earlier assertion, so the observation is read through
    this helper instead of the attribute directly.

    Args:
        daemon: The daemon whose evidence is observed.

    Returns:
        The accumulated evidence, or ``None``.
    """
    return daemon._no_progress


def _retirement_recorder() -> tuple[list[bool], Callable[..., bool]]:
    """Build one retirement recorder bound to a private observation list.

    Returns:
        The observation list and a callable standing in for
        ``_retire_child`` that records and reports a successful retirement.
    """
    observed: list[bool] = []

    def retire(**_kwargs: object) -> bool:
        observed.append(True)
        return True

    return observed, retire


# ------------------------------------------------------------------
# Degraded readiness never authorizes signalling a live worker
# ------------------------------------------------------------------


@pytest.mark.usefixtures("supervisor_token")
def test_consuming_worker_is_never_retired_for_overdue_scans(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A consuming worker whose every scan is overdue keeps running.

    The worker demonstrably consumed the queue probe, so its maintenance
    schedule and slow database round trips are degraded readiness.  Evidence
    of absent forward progress never accumulates and the live incarnation is
    never signalled, so any in-flight spawn it owns survives.
    """
    daemon = supervisor.SupervisorDaemon(supervisor.Settings(readiness_interval_seconds=5.0))
    retired, retire = _retirement_recorder()
    monkeypatch.setattr(daemon, "_retire_child", retire)
    state = _state(_child(), ready=True)
    _wire_daemon(monkeypatch, daemon, _overdue_probe(), lambda: state)

    base = time.monotonic()
    for tick in range(24):
        # Once readiness is withdrawn the worker is re-probed on every tick
        # through the ordinary retry schedule.
        state = replace(state, ready=False, next_readiness_at=None)
        daemon._probe_readiness(base + tick * 5.0)

    assert retired == [], "a queue-consuming worker was retired for overdue scans"
    assert _evidence(daemon) is None, "observed progress left evidence behind"


@pytest.mark.usefixtures("supervisor_token")
def test_ready_worker_with_overdue_scans_only_loses_readiness(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Withdrawing readiness is the whole response to a scan-overdue snapshot."""
    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    retired, retire = _retirement_recorder()
    withdrawals: list[str] = []
    monkeypatch.setattr(daemon, "_retire_child", retire)
    monkeypatch.setattr(
        daemon,
        "_record_not_ready",
        lambda _state, _now, _pid, reason: withdrawals.append(reason),
    )
    state = _state(_child(), ready=True)
    _wire_daemon(monkeypatch, daemon, _overdue_probe(), lambda: state)
    monkeypatch.setattr(
        daemon,
        "_check_worker_health",
        lambda _child: (False, "worker operational not ready: overdue scans: gc"),
    )

    daemon._probe_readiness(time.monotonic())

    assert retired == []
    assert withdrawals == ["worker operational not ready: overdue scans: gc"]


@pytest.mark.usefixtures("supervisor_token")
def test_matching_live_overdue_snapshot_is_not_an_immediate_safety_breach(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A current, identity-matching overdue snapshot proves degradation only.

    Unlike a stale or unpinnable snapshot, this one passes every liveness and
    identity cross-check, so it reaches the safety predicate itself: overdue
    scans with non-negative lease safety must withdraw readiness without
    signalling the exact incarnation, however current the snapshot is.
    """
    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    retired, retire = _retirement_recorder()
    withdrawals: list[str] = []
    monkeypatch.setattr(daemon, "_retire_child", retire)
    monkeypatch.setattr(
        daemon,
        "_record_not_ready",
        lambda _state, _now, _pid, reason: withdrawals.append(reason),
    )
    child = _child()
    snapshot = replace(
        _overdue_snapshot(),
        pid=child.pid,
        start_time_ticks=child.start_time_ticks,
        worker_incarnation=child.token,
        published_at=time.time(),
    )
    monkeypatch.setattr(supervisor, "read_worker_health_by_incarnation", lambda _token: snapshot)
    state = _state(child, ready=True)
    _wire_daemon(monkeypatch, daemon, _overdue_probe(), lambda: state)

    daemon._probe_readiness(time.monotonic())

    assert retired == [], "a current overdue snapshot retired a lease-safe worker"
    assert withdrawals == [
        "worker operational not ready: overdue scans: cancellation, recovery, gc"
    ]


@pytest.mark.usefixtures("supervisor_token")
def test_inconclusive_probe_creates_no_retirement_authority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A probe the supervisor could not perform is not evidence about a worker.

    A database outage prevents the roundtrip from being conclusive at all.
    That says nothing about forward progress, so no evidence accumulates and
    no retirement authority is created however long it lasts.
    """
    daemon = supervisor.SupervisorDaemon(supervisor.Settings(readiness_interval_seconds=5.0))
    retired, retire = _retirement_recorder()
    monkeypatch.setattr(daemon, "_retire_child", retire)
    state = _state(_child(), ready=True)
    _wire_daemon(monkeypatch, daemon, _indeterminate_probe(), lambda: state)

    base = time.monotonic()
    for tick in range(24):
        state = replace(state, ready=False, next_readiness_at=None)
        daemon._probe_readiness(base + tick * 60.0)

    assert retired == [], "an inconclusive probe was treated as absent forward progress"
    assert _evidence(daemon) is None


# ------------------------------------------------------------------
# Immediate safety breach
# ------------------------------------------------------------------


@pytest.mark.usefixtures("supervisor_token")
def test_stale_negative_lease_safety_is_not_an_immediate_breach(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A stale snapshot is not a current safety proof, so it cannot retire at once.

    An unpinnable or long-unpublished snapshot can only feed the sustained
    independent observation, which requires corroboration.
    """
    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    monkeypatch.setattr(
        supervisor,
        "read_worker_health_by_incarnation",
        lambda _token: _overdue_snapshot_with(-8.0),
    )
    assert daemon._worker_health_proves_immediate_safety_breach(_child(CURRENT_TOKEN)) is False


def test_negative_lease_safety_retires_immediately(monkeypatch: pytest.MonkeyPatch) -> None:
    """Negative remaining lease safety is retired without any waiting period."""
    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    retired, retire = _retirement_recorder()
    monkeypatch.setattr(daemon, "_retire_child", retire)
    state = _state(_child(), ready=True)
    _wire_daemon(monkeypatch, daemon, _unresponsive_probe(), lambda: state)
    monkeypatch.setattr(
        daemon, "_worker_health_proves_immediate_safety_breach", lambda _child: True
    )

    daemon._probe_readiness(time.monotonic())

    assert retired == [True]
    assert daemon._message is not None
    assert "operationally unsafe" in daemon._message


# ------------------------------------------------------------------
# Sustained, independently verified absence of forward progress
# ------------------------------------------------------------------


@pytest.mark.usefixtures("supervisor_token")
def test_brief_or_single_no_progress_observations_hold_the_worker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Neither one observation nor a short absence signals a live worker."""
    daemon = supervisor.SupervisorDaemon(supervisor.Settings(readiness_interval_seconds=5.0))
    retired, retire = _retirement_recorder()
    monkeypatch.setattr(daemon, "_retire_child", retire)
    state = _state(_child(), ready=True)
    _wire_daemon(monkeypatch, daemon, _unresponsive_probe(), lambda: state)

    base = time.monotonic()
    # Three consecutive conclusive observations spanning only 10 seconds.
    for tick in range(3):
        state = replace(state, ready=False, next_readiness_at=None)
        daemon._probe_readiness(base + tick * 5.0)
    assert retired == []
    assert _evidence(daemon) is not None
    assert _evidence(daemon).consecutive_probes == 3  # type: ignore[union-attr]

    # A long span with too few corroborating observations.
    daemon._reset_forward_progress_evidence()
    for tick in range(2):
        state = replace(state, ready=False, next_readiness_at=None)
        daemon._probe_readiness(base + 600.0 + tick * 5.0)
    assert retired == []
    assert _evidence(daemon) is not None
    assert _evidence(daemon).consecutive_probes == 2  # type: ignore[union-attr]


@pytest.mark.usefixtures("supervisor_token")
def test_long_span_with_too_few_probes_holds_the_worker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A long absence signalled by only two probes is not yet corroborated.

    The grace period alone is not enough: the absence must also be
    corroborated by the required number of consecutive conclusive
    observations, so a worker re-probed on a slow schedule is not retired
    on two data points however far apart they are.
    """
    daemon = supervisor.SupervisorDaemon(supervisor.Settings(readiness_interval_seconds=5.0))
    retired, retire = _retirement_recorder()
    monkeypatch.setattr(daemon, "_retire_child", retire)
    state = _state(_child(), ready=True)
    _wire_daemon(monkeypatch, daemon, _unresponsive_probe(), lambda: state)

    base = time.monotonic()
    # Two consecutive conclusive observations spanning far more than the
    # grace period: the duration holds but the corroboration does not.
    for tick in range(2):
        state = replace(state, ready=False, next_readiness_at=None)
        daemon._probe_readiness(base + tick * 70.0)

    assert retired == [], "two probes retired a worker across a long span"
    assert _evidence(daemon) is not None
    assert _evidence(daemon).consecutive_probes == 2  # type: ignore[union-attr]


@pytest.mark.usefixtures("supervisor_token")
def test_new_incarnation_does_not_inherit_no_progress_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A replacement worker starts with a clean absent-progress record.

    Evidence belongs to one exact incarnation: the first conclusive
    observation about a freshly spawned worker is its own, never the
    previous incarnation's accumulated record.
    """
    daemon = supervisor.SupervisorDaemon(supervisor.Settings(readiness_interval_seconds=5.0))
    retired, retire = _retirement_recorder()
    monkeypatch.setattr(daemon, "_retire_child", retire)
    state = _state(_child("a" * 32), ready=True)
    _wire_daemon(monkeypatch, daemon, _unresponsive_probe(), lambda: state)

    base = time.monotonic()
    for tick in range(2):
        state = replace(state, ready=False, next_readiness_at=None)
        daemon._probe_readiness(base + tick * 10.0)
    assert _evidence(daemon) is not None
    assert _evidence(daemon).consecutive_probes == 2  # type: ignore[union-attr]

    # A replacement incarnation is observed for the first time: its record
    # must start empty, not continue the previous incarnation's count.
    state = _state(_child("b" * 32), ready=False)
    _wire_daemon(monkeypatch, daemon, _unresponsive_probe(), lambda: state)
    daemon._probe_readiness(base + 600.0)

    assert retired == [], "a new incarnation inherited the previous wedge"
    assert _evidence(daemon) is not None
    assert _evidence(daemon).token == "b" * 32  # type: ignore[union-attr]
    assert _evidence(daemon).consecutive_probes == 1  # type: ignore[union-attr]


@pytest.mark.usefixtures("supervisor_token")
def test_observed_health_recovery_discards_no_progress_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A worker that proves its health again clears the absent-progress record.

    Degraded readiness that recovers is the common case: the moment the
    health snapshot is healthy again, any accumulated absence is stale and
    must not survive to a later probe.
    """
    daemon = supervisor.SupervisorDaemon(supervisor.Settings(readiness_interval_seconds=5.0))
    retired, retire = _retirement_recorder()
    monkeypatch.setattr(daemon, "_retire_child", retire)
    state = _state(_child(), ready=True)
    _wire_daemon(monkeypatch, daemon, _unresponsive_probe(), lambda: state)

    base = time.monotonic()
    for tick in range(2):
        state = replace(state, ready=False, next_readiness_at=None)
        daemon._probe_readiness(base + tick * 10.0)
    assert _evidence(daemon) is not None

    # The same incarnation proves a healthy snapshot again.
    state = replace(state, ready=True)
    monkeypatch.setattr(daemon, "_check_worker_health", lambda _child: (True, "ok"))
    daemon._probe_readiness(base + 20.0)

    assert _evidence(daemon) is None, "recovered health left stale evidence behind"
    assert retired == []


@pytest.mark.usefixtures("supervisor_token")
def test_failed_retirement_preserves_no_progress_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A retirement that did not converge keeps its evidence for a retry.

    The accumulated absence stays valid when the retirement itself fails,
    so the next probe can converge instead of starting the observation
    over.
    """
    daemon = supervisor.SupervisorDaemon(supervisor.Settings(readiness_interval_seconds=5.0))
    attempts: list[bool] = []

    def retire(**_kwargs: object) -> bool:
        attempts.append(True)
        return False

    monkeypatch.setattr(daemon, "_retire_child", retire)
    state = _state(_child(), ready=True)
    _wire_daemon(monkeypatch, daemon, _unresponsive_probe(), lambda: state)

    base = time.monotonic()
    for tick in range(4):
        state = replace(state, ready=False, next_readiness_at=None)
        daemon._probe_readiness(base + tick * 20.0)

    assert attempts == [True], "the sustained absence never attempted a retirement"
    assert _evidence(daemon) is not None, "a failed retirement discarded its evidence"
    assert _evidence(daemon).consecutive_probes == 4  # type: ignore[union-attr]


@pytest.mark.usefixtures("supervisor_token")
def test_sustained_no_progress_retires_once_and_shares_crash_backoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Sustained verified absence of progress retires through bounded backoff.

    The retirement is published exactly like an unexpected exit: the durable
    restart counter advances and the replacement is scheduled after the
    exponential backoff instead of being spawned immediately.
    """
    supervise.write_state(_state(_child(), ready=True))
    daemon = supervisor.SupervisorDaemon(
        supervisor.Settings(
            backoff_base_seconds=1.0,
            backoff_max_seconds=4.0,
            readiness_interval_seconds=5.0,
        )
    )
    retired, retire = _retirement_recorder()
    deploy_log: list[str] = []
    monkeypatch.setattr(daemon, "_child_alive", lambda _state: True)
    monkeypatch.setattr(daemon, "_retire_child", retire)
    monkeypatch.setattr(daemon, "_check_readiness", lambda _child, _cwd: _unresponsive_probe())
    monkeypatch.setattr(lifecycle, "append_deploy_log", deploy_log.append)

    base = time.monotonic()
    retired_at: float | None = None
    for tick in range(5):
        supervise.write_state(replace(supervise.read_state(), ready=False, next_readiness_at=None))
        now = base + tick * 20.0
        daemon._probe_readiness(now)
        if retired:
            retired_at = now
            break

    assert len(retired) == 1, "one sustained absence must retire the worker exactly once"
    assert retired_at is not None
    final = supervise.read_state()
    assert final.restart_count == 1, "the health retirement bypassed the crash backoff"
    assert final.next_attempt_at is not None
    assert final.next_attempt_at - retired_at == pytest.approx(1.0), (
        "the replacement was not scheduled behind the backoff"
    )
    assert any("absent forward progress" in line for line in deploy_log)
    assert _evidence(daemon) is None, "evidence survived its own retirement"


@pytest.mark.usefixtures("supervisor_token")
def test_repeated_health_retirements_escalate_and_stay_bounded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Repeated health retirements climb the backoff to its bounded ceiling."""
    daemon = supervisor.SupervisorDaemon(
        supervisor.Settings(backoff_base_seconds=1.0, backoff_max_seconds=4.0)
    )
    retired_per_generation: list[int] = []
    restart_counts: list[int] = []
    retired_at: float | None = None
    monkeypatch.setattr(daemon, "_child_alive", lambda _state: True)
    monkeypatch.setattr(daemon, "_check_readiness", lambda _child, _cwd: _unresponsive_probe())
    monkeypatch.setattr(lifecycle, "append_deploy_log", lambda _line: None)
    supervise.write_state(_state(_child("0" * 32), ready=True))
    base = time.monotonic()
    for generation in range(5):
        observed, retire = _retirement_recorder()
        monkeypatch.setattr(daemon, "_retire_child", retire)
        # Each generation is a fresh incarnation that wedges again long after
        # the previous backoff deadline: the durable counter must climb.
        supervise.write_state(
            replace(
                supervise.read_state(),
                child=_child(f"{generation:032d}"),
                ready=True,
                next_readiness_at=None,
            )
        )
        daemon._no_progress = None
        for tick in range(5):
            supervise.write_state(
                replace(supervise.read_state(), ready=False, next_readiness_at=None)
            )
            now = base + generation * 1000.0 + tick * 20.0
            daemon._probe_readiness(now)
            if observed:
                retired_at = now
                break
        retired_per_generation.append(len(observed))
        restart_counts.append(supervise.read_state().restart_count)
        assert retired_at is not None, "a wedged generation was never retired"
        assert supervisor.SupervisorDaemon._in_backoff(retired_at) is True, (
            "the replacement was authorized before the backoff deadline"
        )

    assert retired_per_generation == [1] * 5, "every wedged generation was retired once"
    assert restart_counts == [1, 2, 3, 4, 5], "health retirements shared no crash counter"
    final = supervise.read_state()
    assert final.next_attempt_at is not None
    assert retired_at is not None
    assert final.next_attempt_at - retired_at <= 4.0, "the backoff exceeded its configured ceiling"


@pytest.mark.usefixtures("supervisor_token")
def test_health_retirement_counter_resets_after_stability(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A stable worker after a health retirement earns the ordinary reset."""
    state = replace(
        supervise.fresh_state(),
        commit=COMMIT,
        child=_child(),
        restart_count=3,
        next_attempt_at=0.0,
        last_spawn_at=0.0,
    )
    supervise.write_state(state)
    daemon = supervisor.SupervisorDaemon(supervisor.Settings(stable_window_seconds=30.0))
    monkeypatch.setattr(daemon, "_child_alive", lambda _state: True)

    daemon._maybe_reset_backoff(state, time.monotonic() + 31.0)

    final = supervise.read_state()
    assert final.restart_count == 0, "a health-driven retirement never recovered"
    assert final.next_attempt_at is None


@pytest.mark.usefixtures("supervisor_token")
def test_observed_progress_discards_sustained_no_progress_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A wedge that recovers clears its evidence and is never retired."""
    daemon = supervisor.SupervisorDaemon(supervisor.Settings(readiness_interval_seconds=5.0))
    retired, retire = _retirement_recorder()
    monkeypatch.setattr(daemon, "_retire_child", retire)
    state = _state(_child(), ready=True)
    _wire_daemon(monkeypatch, daemon, _unresponsive_probe(), lambda: state)

    base = time.monotonic()
    for tick in range(4):
        state = replace(state, ready=False, next_readiness_at=None)
        daemon._probe_readiness(base + tick * 15.0)
    assert _evidence(daemon) is not None
    assert _evidence(daemon).consecutive_probes == 4  # type: ignore[union-attr]

    # The worker resumes consuming the queue: evidence is discarded even
    # though the accumulated absence already spans the grace period.
    _wire_daemon(monkeypatch, daemon, _overdue_probe(), lambda: state)
    daemon._probe_readiness(base + 400.0)

    assert _evidence(daemon) is None
    assert retired == [], "a recovered worker was retired on stale evidence"


@pytest.mark.usefixtures("supervisor_token")
def test_no_progress_evidence_belongs_to_one_incarnation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A freshly spawned worker never inherits a previous wedge's evidence."""
    daemon = supervisor.SupervisorDaemon(supervisor.Settings(readiness_interval_seconds=5.0))
    retired, retire = _retirement_recorder()
    monkeypatch.setattr(daemon, "_retire_child", retire)
    state = _state(_child("a" * 32), ready=True)
    _wire_daemon(monkeypatch, daemon, _unresponsive_probe(), lambda: state)

    base = time.monotonic()
    for tick in range(5):
        state = replace(state, ready=False, next_readiness_at=None)
        daemon._probe_readiness(base + tick * 10.0)
    assert _evidence(daemon) is not None
    assert _evidence(daemon).consecutive_probes == 5  # type: ignore[union-attr]

    state = _state(_child("b" * 32), ready=True)
    _wire_daemon(monkeypatch, daemon, _overdue_probe(), lambda: state)
    daemon._probe_readiness(base + 600.0)

    assert retired == [], "a new incarnation was retired on the previous one's evidence"
    assert _evidence(daemon) is None


# ------------------------------------------------------------------
# Bounded retention of superseded incarnation evidence
# ------------------------------------------------------------------


def _write_artifact(directory: Path, name: str, age_seconds: float) -> None:
    """Write one superseded artifact with a given age.

    Args:
        directory: The directory to write into.
        name: Artifact filename.
        age_seconds: How old the artifact should appear.
    """
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / name
    path.write_text("evidence\n", encoding="utf-8")
    stamp = time.time() - age_seconds
    os.utime(path, (stamp, stamp))


@pytest.fixture
def evidence_dirs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, Path]:
    """Point the worker state directory at an isolated temporary root.

    Args:
        tmp_path: Per-test temporary directory.
        monkeypatch: Pytest monkeypatch fixture.

    Returns:
        The isolated health and logs directories.
    """
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
    return (
        tmp_path / "lubko" / "worker" / "health",
        tmp_path / "lubko" / "worker" / "logs",
    )


def test_superseded_incarnation_evidence_survives_the_retention_window(
    evidence_dirs: tuple[Path, Path],
) -> None:
    """A replacement keeps the retired incarnation's evidence for >=48h."""
    health_dir, logs_dir = evidence_dirs
    previous = "a" * 32
    _write_artifact(health_dir, f"health-{CURRENT_TOKEN}.json", 0.0)
    _write_artifact(logs_dir, f"worker-{CURRENT_TOKEN}.log", 0.0)
    _write_artifact(health_dir, f"health-{previous}.json", 60.0)
    _write_artifact(logs_dir, f"worker-{previous}.log", 60.0)

    prune_old_incarnation_artifacts(CURRENT_TOKEN)

    assert (health_dir / f"health-{previous}.json").exists()
    assert (logs_dir / f"worker-{previous}.log").exists()
    assert EVIDENCE_RETENTION_SECONDS >= 48.0 * 3600.0


def test_incarnation_evidence_retention_is_bounded(evidence_dirs: tuple[Path, Path]) -> None:
    """A retirement storm cannot grow the retained evidence without limit."""
    health_dir, _logs_dir = evidence_dirs
    for index in range(MAX_RETAINED_EVIDENCE_FILES + 3):
        _write_artifact(health_dir, f"health-{index:032d}.json", 3600.0)

    prune_old_incarnation_artifacts(CURRENT_TOKEN)

    retained = sorted(path.name for path in health_dir.glob("health-*.json"))
    assert len(retained) == MAX_RETAINED_EVIDENCE_FILES, f"unbounded evidence: {retained}"
    # The newest incarnations are the ones a post-mortem actually needs.
    assert f"health-{MAX_RETAINED_EVIDENCE_FILES + 2:032d}.json" in retained
    assert "health-00000000000000000000000000000000.json" not in retained


def test_incarnation_evidence_expires_after_the_retention_window(
    evidence_dirs: tuple[Path, Path],
) -> None:
    """Evidence older than the retention window is dropped."""
    health_dir, _logs_dir = evidence_dirs
    _write_artifact(health_dir, f"health-{CURRENT_TOKEN}.json", 0.0)
    _write_artifact(health_dir, f"health-{'a' * 32}.json", EVIDENCE_RETENTION_SECONDS + 60.0)

    prune_old_incarnation_artifacts(CURRENT_TOKEN)

    assert not (health_dir / f"health-{'a' * 32}.json").exists()
    assert (health_dir / f"health-{CURRENT_TOKEN}.json").exists()


# ------------------------------------------------------------------
# Operational interpretation: degradation is not a safety breach
# ------------------------------------------------------------------


def _overdue_snapshot_with(
    min_lease_safety_remaining_seconds: float | None,
) -> WorkerHealth:
    """Return the all-scans-overdue snapshot with one lease-safety value.

    Args:
        min_lease_safety_remaining_seconds: Remaining lease-safety value.

    Returns:
        A worker health snapshot whose every scan is overdue.
    """
    return replace(
        _overdue_snapshot(),
        min_lease_safety_remaining_seconds=min_lease_safety_remaining_seconds,
    )


def _overdue_snapshot() -> WorkerHealth:
    """Return a snapshot whose every maintenance scan is overdue.

    Returns:
        A worker health snapshot with all scans overdue.
    """
    fields: dict[str, object] = {
        "schema_version": WORKER_HEALTH_SCHEMA_VERSION,
        "worker_id": "w",
        "worker_incarnation": "inc",
        "pid": 1,
        "start_time_ticks": 1,
        "started_at": 1.0,
        "published_at": 1.0,
        "alive": True,
        "db_connected": True,
        "db_connected_at": 1.0,
        "db_error_at": None,
        "active_jobs": 0,
        "stopping_jobs": 0,
        "completed_jobs": 0,
        "oldest_active_job_age_seconds": None,
        "lease_safety_margin_seconds": 5.0,
        "min_lease_safety_remaining_seconds": None,
        "db_operation_deadline_seconds": 15.0,
        "db_last_activity_at": 1.0,
        "db_deadline_breached_at": None,
        "db_deadline_breach_count": 0,
        "capture_streams_open": 0,
        "spool_held_bytes": 0,
        "scan_batch_limit": 16,
        "last_scan_batch_size": 0,
        "last_cancellation_scan_at": None,
        "last_recovery_at": None,
        "last_gc_at": None,
        "cancellation_scan_overdue": True,
        "recovery_overdue": True,
        "gc_overdue": True,
        "gc_batch_limit": 32,
        "gc_batch_bound_hit": True,
        "cancellation_batch_limit": 100,
        "cancellation_batch_bound_hit": True,
        "recovery_batch_limit": 100,
        "recovery_batch_bound_hit": True,
        "shutting_down": False,
    }
    return WorkerHealth(**fields)  # type: ignore[arg-type]


def test_overdue_scans_are_degraded_readiness_not_a_safety_breach() -> None:
    """An overdue scan sets no safety flag, so it cannot justify a retirement."""
    op = interpret_operational_readiness(_overdue_snapshot())

    assert op.ready is False
    assert op.any_scan_overdue is True
    assert op.lease_safety_negative is False


def test_negative_lease_safety_is_the_immediate_safety_breach() -> None:
    """Negative remaining lease safety is the safety flag that does justify one."""
    op = interpret_operational_readiness(_overdue_snapshot_with(-12.0))

    assert op.lease_safety_negative is True
    assert op.ready is False
