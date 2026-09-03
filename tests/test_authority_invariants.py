"""Deterministic invariants and crash-boundary tests for the authority model.

The authority is genuinely executable: :class:`lubko.lifecycle_state.AuthorityFacts`
reconciles the real durable + observed sources, and
:func:`lubko.lifecycle_state.assert_authority_invariants` enforces the seven
explicit invariants at every authority boundary. These tests construct crash
before/after authority snapshots and assert all seven invariants hold (or raise
with the correct code), and drive the real failpoint seams to confirm a crash
injects exactly at the durable/side-effect boundary and leaves the authority
single-consumer-safe.

No sleeps, no real subprocesses: the spawn failpoint fires before ``Popen``, so
the supervisor's real ``_spawn_worker`` decision path is exercised without
spawning anything.
"""

from __future__ import annotations

import os
import subprocess
import time
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

import pytest

from lubko import cli, deployctl, lifecycle, lifecycle_state, supervise, supervisor
from lubko.lifecycle_state import (
    INVARIANT_CRASH_CONVERGES_TO_ONE_OR_ZERO,
    INVARIANT_GENERATION_MONOTONIC,
    INVARIANT_MALFORMED_NEVER_ERASED,
    INVARIANT_NO_LIVE_CONSUMER_WITHOUT_AUTHORITY,
    INVARIANT_NO_REPLACEMENT_WHILE_UNRESOLVED,
    INVARIANT_NO_SIGNAL_WITHOUT_PROOF,
    INVARIANT_SINGLE_CONSUMER,
    AuthorityFacts,
    AuthorityInvariantError,
    LifecyclePhase,
    assert_authority_invariants,
    authorize_mission_confirm,
    authorize_mission_publish,
    authorize_mission_rollback,
    authorize_recovery,
    authorize_retirement,
    authorize_spawn,
    check_authority_invariants,
    current_phase,
    phase_from_facts,
    reconcile_authority_facts,
)

COMMIT = "a" * 40


def _fake_entry(_commit: str, _name: str) -> Path:
    """Stand-in CLI entry executable that names a real, harmless binary path.

    Returns:
        A stable, harmless binary path (the spawn never reaches ``Popen`` because
        the armed failpoint fires first).
    """
    return Path("/usr/bin/env")


def _fake_commit_dir(_commit: str) -> Path:
    """Stand-in CLI commit directory (never touched on disk by the spawn path).

    Returns:
        A stable fake runtime root path that is never opened by the spawn path.
    """
    return Path("/opt/lubko-fake-runtime")


def _facts(**overrides: Any) -> AuthorityFacts:  # ruff: ignore[any-type]
    """Build an otherwise-clean authority snapshot with the given overrides.

    Args:
        **overrides: Field values to override on the clean base snapshot.

    Returns:
        The constructed :class:`AuthorityFacts`.
    """
    base = AuthorityFacts(
        desired_generation=0,
        applied_generation=0,
        mission_status=None,
        mission_generation=None,
        mission_commit=None,
        owned_worker_pid=None,
        owned_worker_commit=None,
        owned_worker_identity_proven=False,
        pre_spawn_obligation=False,
        unresolved_child=False,
        candidate_ready=False,
        rollback_pending=False,
        durable_malformed=False,
        supervisor_child_present=False,
        current_child_identity_proven=False,
        ownership_hold_malformed=False,
        unresolved_hold_malformed=False,
        spawning_hold_malformed=False,
    )
    return replace(base, **overrides)


@pytest.fixture(autouse=True)
def state_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Use an isolated durable state root for each test.

    Returns:
        The supervisor state directory.
    """
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state"))
    supervise.state_path().parent.mkdir(parents=True, exist_ok=True)
    return supervise.state_path().parent


# ---------------------------------------------------------------------------
# The seven invariants, asserted on constructed crash before/after snapshots
# ---------------------------------------------------------------------------


def test_invariant_single_consumer_and_crash_convergence() -> None:
    """Two live consumer roles violate SINGLE_CONSUMER and CRASH_CONVERGES."""
    codes = check_authority_invariants(
        _facts(owned_worker_identity_proven=True, unresolved_child=True)
    )
    assert INVARIANT_SINGLE_CONSUMER in codes
    assert INVARIANT_CRASH_CONVERGES_TO_ONE_OR_ZERO in codes
    assert check_authority_invariants(_facts(supervisor_child_present=True)) == []


def test_invariant_generation_monotonic() -> None:
    """Generations never regress; a strictly newer mission is allowed."""
    assert INVARIANT_GENERATION_MONOTONIC in check_authority_invariants(
        _facts(applied_generation=5, desired_generation=3)
    )
    assert INVARIANT_GENERATION_MONOTONIC in check_authority_invariants(
        _facts(mission_status="pending", mission_generation=1, applied_generation=4)
    )
    assert INVARIANT_GENERATION_MONOTONIC not in check_authority_invariants(
        _facts(
            mission_status="pending",
            mission_generation=4,
            applied_generation=3,
            desired_generation=4,
        )
    )


def test_invariant_generation_monotonic_pending_mission_authority() -> None:
    """A pending supervised mission is itself a trusted generation authority.

    applied_generation equal to the pending mission generation is accepted even
    when desired is absent or older, but applied above every trusted source and
    a stale mission below applied stay violations. Only a pending mission grants
    this authority; a terminal mission does not.
    """
    # Pending mission == applied, absent/older desired (no desired.json -> 0).
    assert INVARIANT_GENERATION_MONOTONIC not in check_authority_invariants(
        _facts(
            mission_status="pending",
            mission_generation=5,
            applied_generation=5,
            desired_generation=0,
        )
    )
    # Pending mission == applied, desired strictly older.
    assert INVARIANT_GENERATION_MONOTONIC not in check_authority_invariants(
        _facts(
            mission_status="pending",
            mission_generation=5,
            applied_generation=5,
            desired_generation=3,
        )
    )
    # Applied above every trusted source (above desired AND above pending mission).
    assert INVARIANT_GENERATION_MONOTONIC in check_authority_invariants(
        _facts(
            mission_status="pending",
            mission_generation=5,
            applied_generation=7,
            desired_generation=4,
        )
    )
    # Stale mission below applied remains a violation even with a higher desired.
    assert INVARIANT_GENERATION_MONOTONIC in check_authority_invariants(
        _facts(
            mission_status="pending",
            mission_generation=3,
            applied_generation=5,
            desired_generation=8,
        )
    )
    # A non-pending (terminal) mission does NOT grant authority: applied above
    # desired (absent) is still a violation.
    assert INVARIANT_GENERATION_MONOTONIC in check_authority_invariants(
        _facts(
            mission_status="confirmed",
            mission_generation=5,
            applied_generation=5,
            desired_generation=0,
        )
    )
    # Terminal mission history no longer acts as current generation authority.
    # Settlement intentionally advances desired/applied beyond the mission.
    for terminal_status in ("confirmed", "rolled_back"):
        assert INVARIANT_GENERATION_MONOTONIC not in check_authority_invariants(
            _facts(
                mission_status=terminal_status,
                mission_generation=4,
                applied_generation=5,
                desired_generation=5,
            )
        )

    # Applied below a pending mission (progress not yet recorded) is fine.
    assert INVARIANT_GENERATION_MONOTONIC not in check_authority_invariants(
        _facts(
            mission_status="pending",
            mission_generation=9,
            applied_generation=2,
            desired_generation=0,
        )
    )


def test_invariant_malformed_never_erased() -> None:
    """Corruption without a blocking hold is flagged; with one it is not."""
    assert INVARIANT_MALFORMED_NEVER_ERASED in check_authority_invariants(
        _facts(durable_malformed=True)
    )
    assert INVARIANT_MALFORMED_NEVER_ERASED not in check_authority_invariants(
        _facts(durable_malformed=True, spawning_hold_malformed=True)
    )


def test_invariant_no_replacement_while_unresolved() -> None:
    """A new spawn must not start beside an unresolved earlier child."""
    assert INVARIANT_NO_REPLACEMENT_WHILE_UNRESOLVED in check_authority_invariants(
        _facts(unresolved_child=True, pre_spawn_obligation=True)
    )
    assert INVARIANT_NO_REPLACEMENT_WHILE_UNRESOLVED in check_authority_invariants(
        _facts(unresolved_child=True, supervisor_child_present=True)
    )


def test_invariant_no_signal_without_proof() -> None:
    """A published child whose recorded identity is proven dead must not be signalled."""
    assert INVARIANT_NO_SIGNAL_WITHOUT_PROOF in check_authority_invariants(
        _facts(
            supervisor_child_present=True, owned_worker_pid=123, owned_worker_identity_proven=False
        )
    )
    assert INVARIANT_NO_SIGNAL_WITHOUT_PROOF not in check_authority_invariants(
        _facts(
            supervisor_child_present=True, owned_worker_pid=123, owned_worker_identity_proven=True
        )
    )


def test_invariant_no_live_consumer_without_authority() -> None:
    """A live consumer must not exist under malformed durable authority."""
    codes = check_authority_invariants(
        _facts(owned_worker_identity_proven=True, durable_malformed=True)
    )
    assert INVARIANT_NO_LIVE_CONSUMER_WITHOUT_AUTHORITY in codes


def test_assert_raises_first_violation_code() -> None:
    """assert_authority_invariants raises with the violating code and facts."""
    facts = _facts(owned_worker_identity_proven=True, unresolved_child=True)
    with pytest.raises(AuthorityInvariantError) as exc:
        assert_authority_invariants(facts)
    assert exc.value.code == INVARIANT_SINGLE_CONSUMER
    assert exc.value.facts is facts


def test_clean_state_satisfies_all_invariants() -> None:
    """A reconciled clean running state violates no invariant."""
    monkeypatch_state_running()
    assert check_authority_invariants(reconcile_authority_facts()) == []


# ---------------------------------------------------------------------------
# Transition / authorization decisions
# ---------------------------------------------------------------------------


def test_authorize_decisions() -> None:
    """The transition/authorization decisions follow the reconciled facts."""
    assert authorize_spawn(_facts()) is True
    assert authorize_spawn(_facts(pre_spawn_obligation=True)) is False
    assert authorize_spawn(_facts(unresolved_child=True)) is False
    assert authorize_spawn(_facts(supervisor_child_present=True)) is False
    assert authorize_spawn(_facts(owned_worker_identity_proven=True)) is False
    assert authorize_spawn(_facts(durable_malformed=True)) is False

    assert authorize_recovery(_facts(unresolved_child=True)) is True
    assert (
        authorize_recovery(_facts(unresolved_child=True, owned_worker_identity_proven=True))
        is False
    )
    assert authorize_recovery(_facts()) is False

    assert authorize_retirement(_facts(current_child_identity_proven=True)) is True
    assert authorize_retirement(_facts(durable_malformed=True)) is False
    assert authorize_retirement(_facts()) is False

    assert authorize_mission_publish(_facts()) is True
    assert authorize_mission_publish(_facts(mission_status="pending")) is False
    assert authorize_mission_publish(_facts(durable_malformed=True)) is False
    assert authorize_mission_confirm(_facts(mission_status="pending", candidate_ready=True)) is True
    assert (
        authorize_mission_confirm(_facts(mission_status="pending", candidate_ready=False)) is False
    )
    # Same-mission replay is explicitly safe: confirming an already-confirmed
    # mission is a no-op, not a conflicting transition.
    assert authorize_mission_confirm(_facts(mission_status="confirmed")) is True
    assert authorize_mission_rollback(_facts(mission_status="pending")) is True
    assert authorize_mission_rollback(_facts(mission_status="rolled_back")) is True
    assert (
        authorize_mission_rollback(_facts(durable_malformed=True, mission_status="pending"))
        is False
    )


# ---------------------------------------------------------------------------
# Reconciler fail-closed and desired-generation regression tests
# ---------------------------------------------------------------------------


def test_unreadable_supervisor_state_blocks_spawn(monkeypatch: pytest.MonkeyPatch) -> None:
    """An unreadable supervisor state fails closed: spawn is not authorized.

    The reconciler must treat an exception from ``supervise.read_state`` as
    malformed durable authority, not as absent authority, so ``authorize_spawn``
    cannot fail open and start a second worker.
    """
    monkeypatch_state_running()
    monkeypatch.setattr(
        supervise, "read_state", lambda: (_ for _ in ()).throw(RuntimeError("disk gone"))
    )
    facts = reconcile_authority_facts()
    assert facts.durable_malformed is True
    assert authorize_spawn(facts) is False


def test_unreadable_meta_blocks_spawn(monkeypatch: pytest.MonkeyPatch) -> None:
    """An unreadable worker meta fails closed: spawn is not authorized."""
    monkeypatch.setattr(lifecycle, "read_meta", lambda: (_ for _ in ()).throw(OSError("meta gone")))
    facts = reconcile_authority_facts()
    assert facts.durable_malformed is True
    assert authorize_spawn(facts) is False


def test_unreadable_supervisor_state_holds_spawn_in_runtime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The runtime spawn decision holds (no worker) when supervisor state is unreadable."""
    monkeypatch.setattr(cli, "runtime_is_usable", lambda _c: True)
    monkeypatch.setattr(cli, "cli_entry_executable", _fake_entry)
    monkeypatch.setattr(cli, "cli_commit_dir", _fake_commit_dir)
    monkeypatch.setattr(lifecycle, "worker_env", lambda _t: {})
    monkeypatch.setattr(supervisor, "read_desired", lambda: None)
    monkeypatch.setattr(supervise, "read_desired", lambda: None)
    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    # Unreadable supervisor state fails closed at the spawn decision, after construction.
    monkeypatch.setattr(
        supervise,
        "read_state",
        lambda: (_ for _ in ()).throw(RuntimeError("disk gone")),
    )
    monkeypatch.setattr(
        supervisor,
        "read_state",
        lambda: (_ for _ in ()).throw(RuntimeError("disk gone")),
    )
    assert daemon._spawn_worker(COMMIT) is None


def test_malformed_desired_intent_blocks_authority(monkeypatch: pytest.MonkeyPatch) -> None:
    """Malformed desired authority is not normalized to genuine absence."""
    monkeypatch.setattr(
        supervise,
        "read_desired_strict",
        lambda: (_ for _ in ()).throw(supervise.DesiredIntentError("malformed")),
    )
    monkeypatch.setattr(lifecycle, "read_meta", lambda: None)
    monkeypatch.setattr(deployctl, "read_rollback_state", lambda: None)
    monkeypatch.setattr(supervise, "read_state", supervise.fresh_state)

    facts = reconcile_authority_facts()

    assert facts.desired_generation == 0
    assert facts.durable_malformed is True
    assert phase_from_facts(facts) is LifecyclePhase.OWNERSHIP_PENDING
    assert authorize_spawn(facts) is False


def test_absent_desired_intent_remains_non_malformed(monkeypatch: pytest.MonkeyPatch) -> None:
    """Genuine desired absence retains the mission/bootstrap generation-zero semantics."""
    monkeypatch.setattr(supervise, "read_desired_strict", lambda: None)
    monkeypatch.setattr(lifecycle, "read_meta", lambda: None)
    monkeypatch.setattr(deployctl, "read_rollback_state", lambda: None)
    monkeypatch.setattr(supervise, "read_state", supervise.fresh_state)

    facts = reconcile_authority_facts()

    assert facts.desired_generation == 0
    assert facts.durable_malformed is False


def test_desired_generation_read_from_intent(monkeypatch: pytest.MonkeyPatch) -> None:
    """The actual desired generation participates in the monotonic invariant.

    The reconciler must read the desired run intent (not alias it to the applied
    generation), so a supervisor that has applied a generation above the desired
    intent is flagged by GENERATION_MONOTONIC.
    """
    intent = SimpleNamespace(generation=7)
    monkeypatch.setattr(supervise, "read_desired_strict", lambda: intent)
    monkeypatch.setattr(
        supervise,
        "read_state",
        lambda: SimpleNamespace(
            applied_generation=5,
            child=None,
            spawning=None,
            unresolved_child=None,
            ownership_hold_malformed=False,
            unresolved_hold_malformed=False,
            spawning_hold_malformed=False,
            ready=False,
        ),
    )
    monkeypatch.setattr(lifecycle, "read_meta", lambda: None)
    monkeypatch.setattr(deployctl, "read_rollback_state", lambda: None)

    facts = reconcile_authority_facts()
    assert facts.desired_generation == 7
    assert facts.applied_generation == 5
    assert INVARIANT_GENERATION_MONOTONIC not in check_authority_invariants(facts)

    skewed = replace(facts, applied_generation=9)
    assert INVARIANT_GENERATION_MONOTONIC in check_authority_invariants(skewed)


# ---------------------------------------------------------------------------
# Crash before/after scenarios at the durable boundaries (#282 protocol)
# ---------------------------------------------------------------------------


def test_spawn_publication_protocol_stays_single_consumer() -> None:
    """The child+spawning -> meta -> spawning-clear protocol never yields two.

    Each durable snapshot along the fail-closed publication protocol must satisfy
    SINGLE_CONSUMER and NO_REPLACEMENT_WHILE_UNRESOLVED, even when a crash leaves
    the obligation durable without yet clearing it.
    """
    before = _facts()  # no consumer, no obligation
    obligation_written = _facts(pre_spawn_obligation=True)  # Popen issued, child not proven
    child_published = _facts(pre_spawn_obligation=True, supervisor_child_present=True)
    meta_written = _facts(
        pre_spawn_obligation=True,
        supervisor_child_present=True,
        owned_worker_identity_proven=True,
    )
    cleared = _facts(supervisor_child_present=True, owned_worker_identity_proven=True)
    for snapshot in (before, obligation_written, child_published, meta_written, cleared):
        assert check_authority_invariants(snapshot) == [], snapshot


def test_crash_leaves_replacement_blocking_unresolved_interleaving() -> None:
    """A supervisor crash beside an unresolved manual-recovery hold blocks a second.

    If a crash leaves a pre-spawn obligation durable while a manual recovery has
    already persisted an unresolved child, the authority must flag the violation
    rather than authorizing a second queue consumer.
    """
    interleaved = _facts(
        unresolved_child=True, pre_spawn_obligation=True, supervisor_child_present=True
    )
    codes = check_authority_invariants(interleaved)
    assert INVARIANT_NO_REPLACEMENT_WHILE_UNRESOLVED in codes
    assert INVARIANT_SINGLE_CONSUMER in codes


# ---------------------------------------------------------------------------
# Real failpoint-injection at the durable/side-effect boundaries
# ---------------------------------------------------------------------------


def test_spawn_write_failpoint_blocks_pre_popen(monkeypatch: pytest.MonkeyPatch) -> None:
    """The supervisor.spawning_write failpoint fires before Popen and any write."""
    monkeypatch.setattr(cli, "runtime_is_usable", lambda _c: True)
    monkeypatch.setattr(cli, "cli_entry_executable", _fake_entry)
    monkeypatch.setattr(cli, "cli_commit_dir", _fake_commit_dir)
    monkeypatch.setattr(lifecycle, "worker_env", lambda _t: {})
    monkeypatch.setattr(supervisor, "read_desired", lambda: None)
    monkeypatch.setattr(supervise, "read_desired", lambda: None)
    written: list[object] = []
    monkeypatch.setattr(supervisor, "write_state", written.append)

    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    with (
        pytest.raises(lifecycle_state.FailpointError),
        lifecycle_state.armed_failpoint(
            lifecycle_state.FAILPOINT_SUPERVISOR_SPAWNING_WRITE,
        ),
    ):
        daemon._spawn_worker(COMMIT)
    assert written == [], "the pre-spawn obligation was never written at the crash"


def test_authorize_spawn_blocks_replacement_in_runtime(monkeypatch: pytest.MonkeyPatch) -> None:
    """The runtime spawn decision routes through authorize_spawn and holds on a hold."""
    monkeypatch.setattr(cli, "runtime_is_usable", lambda _c: True)
    monkeypatch.setattr(cli, "cli_entry_executable", _fake_entry)
    monkeypatch.setattr(cli, "cli_commit_dir", _fake_commit_dir)
    monkeypatch.setattr(lifecycle, "worker_env", lambda _t: {})
    monkeypatch.setattr(supervisor, "read_desired", lambda: None)
    monkeypatch.setattr(supervise, "read_desired", lambda: None)
    written: list[object] = []
    monkeypatch.setattr(supervisor, "write_state", written.append)

    state = supervise.read_state()
    supervise.write_state(
        replace(
            state,
            unresolved_child=supervise.UnresolvedChild(
                pid=1,
                start_time_ticks=1,
                token="tok" + "a" * 20,
                spawned_at=0.0,
            ),
        )
    )
    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    assert daemon._spawn_worker(COMMIT) is None
    assert written == [], "no spawning obligation written while an unresolved child blocks"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def monkeypatch_state_running() -> None:
    """Install a clean running durable state for reconciliation.

    This is a module-level helper so the clean-state test reads the genuine
    sources through :func:`reconcile_authority_facts` rather than a mock.
    """
    meta = lifecycle.WorkerMeta(
        schema_version=lifecycle.SCHEMA_VERSION,
        state=lifecycle.STATE_RUNNING,
        pid=1,
        pgid=1,
        sid=1,
        start_time_ticks=1,
        token=os.urandom(8).hex(),
        repo="/r",
        git_commit=COMMIT,
        worker_id="w",
        log_path="/l",
        started_at=None,
        stopped_at=None,
    )
    lifecycle.write_meta(meta)
    supervise.write_state(
        replace(
            supervise.read_state(),
            child=supervise.WorkerChild(
                pid=1,
                pgid=1,
                sid=1,
                start_time_ticks=1,
                token=os.urandom(8).hex(),
                worker_id="w",
                spawned_at=0.0,
            ),
            ready=True,
        )
    )


def test_current_phase_derived_from_reconciled_facts(monkeypatch: pytest.MonkeyPatch) -> None:
    """The fact-derived phase reports RUNNING for the clean running state."""
    monkeypatch_state_running()
    monkeypatch.setattr(lifecycle, "worker_alive", lambda _m: True)
    monkeypatch.setattr(supervise, "child_alive", lambda _c: True)
    assert current_phase() is lifecycle_state.LifecyclePhase.RUNNING


# ---------------------------------------------------------------------------
# Real runtime paths consult the authority model (gates wired, not decorative)
# ---------------------------------------------------------------------------


def _make_meta(commit: str, *, pid: int = 1) -> lifecycle.WorkerMeta:
    """Build a minimal valid worker metadata record for mission fixtures.

    Returns:
        A valid :class:`lubko.lifecycle.WorkerMeta` for the given commit.
    """
    return lifecycle.WorkerMeta(
        schema_version=lifecycle.SCHEMA_VERSION,
        state=lifecycle.STATE_RUNNING,
        pid=pid,
        pgid=pid,
        sid=pid,
        start_time_ticks=pid * 10,
        token=f"tok-{pid}",
        repo="/r",
        git_commit=commit,
        worker_id="w",
        log_path="/l",
        started_at=1.0,
        stopped_at=None,
    )


def _make_mission(
    status: str, *, commit: str = COMMIT, challenge_hash: str | None = None
) -> deployctl.RollbackState:
    """Build a minimal valid rollback mission in ``status``.

    Returns:
        A valid :class:`lubko.deployctl.RollbackState` in the given status.
    """
    return deployctl.RollbackState(
        schema_version=deployctl.ROLLBACK_SCHEMA_VERSION,
        generation=5,
        status=status,
        commit=commit,
        previous_commit="0" * 40,
        challenge_hash=challenge_hash,
        deadline=time.time() + 60.0,
        repo="/r",
        uv_path="uv",
        stop_grace_seconds=1.0,
        git_timeout_seconds=1.0,
        previous_retiring=False,
        previous_meta=_make_meta("0" * 40),
        new_meta=_make_meta(commit),
        supervisor_owned=True,
    )


def _options() -> deployctl.Options:
    """Return runtime options for the deployment handlers."""
    return deployctl.Options(
        repo=Path("/r"),
        uv_path="uv",
        confirm_window_seconds=60.0,
        stop_grace_seconds=1.0,
        postgres_timeout_seconds=1.0,
        lock_timeout_seconds=1.0,
        validation_timeout_seconds=1.0,
        git_timeout_seconds=1.0,
        cli_timeout_seconds=1.0,
    )


def test_post_popen_invariant_refusal_converges_child(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A post-Popen authority-invariant refusal converges the live child.

    The review requires the supervisor to never ``return None`` and forget a
    child it just spawned: an authority-invariant refusal after a successful
    ``Popen`` must route through the existing ``_recover_unpublished_spawn``
    convergence path so the child is reaped, never orphaned.
    """
    script = tmp_path / "sleeper.sh"
    script.write_text("#!/bin/sh\nsleep 30\n")
    script.chmod(0o755)
    monkeypatch.setattr(cli, "runtime_is_usable", lambda _c: True)
    monkeypatch.setattr(cli, "cli_entry_executable", lambda _c, _n: script)
    monkeypatch.setattr(cli, "cli_commit_dir", lambda _c: tmp_path)
    monkeypatch.setattr(lifecycle, "worker_env", lambda _t: {})
    monkeypatch.setattr(supervisor, "read_desired", lambda: None)
    monkeypatch.setattr(supervise, "read_desired", lambda: None)
    monkeypatch.setattr(supervisor, "recover_owned_groups", lambda _t: None)
    captured: list[subprocess.Popen[bytes]] = []
    real_popen: Callable[..., subprocess.Popen[bytes]] = subprocess.Popen

    def spy_popen(*args: object, **kwargs: object) -> subprocess.Popen[bytes]:
        proc = real_popen(*args, **kwargs)
        captured.append(proc)
        return proc

    monkeypatch.setattr(supervisor.subprocess, "Popen", spy_popen)  # type: ignore[attr-defined]
    calls = {"n": 0}
    valid = _facts()
    # A genuine authority-invariant violation: two live consumers are present
    # (a published child AND an unresolved child hold) at the post-Popen
    # publication boundary, which must refuse and converge the live spawn.
    violating = _facts(supervisor_child_present=True, unresolved_child=True)

    def fake_reconcile() -> AuthorityFacts:
        calls["n"] += 1
        return violating if calls["n"] >= 2 else valid

    monkeypatch.setattr(lifecycle_state, "reconcile_authority_facts", fake_reconcile)
    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    assert daemon._spawn_worker(COMMIT) is None
    assert captured, "a real child was spawned before the refusal"
    assert captured[0].poll() is not None, (
        "the live child was converged (reaped), not forgotten after the refusal"
    )


def test_recovery_gate_refuses_conflicting_authority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """_resolve_unresolved_child holds (no convergence) when authority refuses."""
    monkeypatch.setattr(
        lifecycle_state,
        "reconcile_authority_facts",
        lambda: _facts(unresolved_child=True, owned_worker_identity_proven=True),
    )
    state = supervise.read_state()
    supervise.write_state(
        replace(
            state,
            unresolved_child=supervise.UnresolvedChild(
                pid=1, start_time_ticks=1, token="tok" + "a" * 20, spawned_at=0.0
            ),
        )
    )
    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    converged: list[bool] = []

    def spy_converge(_h: object) -> bool:
        converged.append(True)
        return False

    monkeypatch.setattr(daemon, "_converge_unresolved", spy_converge)
    assert daemon._resolve_unresolved_child() is False
    assert converged == [], (
        "convergence must not run when the authority refuses recovery (conflicting consumer)"
    )


def test_recovery_gate_allows_when_authority_permitted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """_resolve_unresolved_child converges when the authority permits recovery."""
    monkeypatch.setattr(
        lifecycle_state,
        "reconcile_authority_facts",
        lambda: _facts(unresolved_child=True),
    )
    state = supervise.read_state()
    supervise.write_state(
        replace(
            state,
            unresolved_child=supervise.UnresolvedChild(
                pid=1, start_time_ticks=1, token="tok" + "a" * 20, spawned_at=0.0
            ),
        )
    )
    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    converged: list[bool] = []

    def _converge(_h: object) -> bool:
        converged.append(True)
        return True

    monkeypatch.setattr(daemon, "_converge_unresolved", _converge)
    monkeypatch.setattr(daemon, "_recover_spawn_owned_groups", lambda _t: True)
    assert daemon._resolve_unresolved_child() is True
    assert converged == [True]


def test_retirement_gate_refuses_unproven_live_child(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """_retire_child refuses (no signal) when authority denies retirement."""
    monkeypatch.setattr(lifecycle, "worker_alive", lambda _m: True)
    monkeypatch.setattr(
        lifecycle_state,
        "reconcile_authority_facts",
        lambda: _facts(current_child_identity_proven=False),
    )
    state = supervise.read_state()
    supervise.write_state(
        replace(
            state,
            child=supervise.WorkerChild(
                pid=1,
                pgid=1,
                sid=1,
                start_time_ticks=1,
                token="tok" + "a" * 20,
                worker_id="w",
                spawned_at=0.0,
            ),
        )
    )
    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    signalled: list[bool] = []

    def _stop(_m: object, _g: object) -> bool:
        signalled.append(True)
        return True

    monkeypatch.setattr(lifecycle, "stop_worker", _stop)
    assert daemon._retire_child() is False
    assert signalled == [], "no signal may be delivered when authority refuses retirement"


def test_retirement_gate_allows_proven_live_child(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """_retire_child signals and clears a provably-our-direct-child worker."""
    monkeypatch.setattr(lifecycle, "worker_alive", lambda _m: True)
    monkeypatch.setattr(
        lifecycle_state,
        "reconcile_authority_facts",
        lambda: _facts(current_child_identity_proven=True),
    )
    state = supervise.read_state()
    supervise.write_state(
        replace(
            state,
            child=supervise.WorkerChild(
                pid=1,
                pgid=1,
                sid=1,
                start_time_ticks=1,
                token="tok" + "a" * 20,
                worker_id="w",
                spawned_at=0.0,
            ),
        )
    )
    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    monkeypatch.setattr(lifecycle, "stop_worker", lambda _m, _g: True)
    monkeypatch.setattr(supervisor, "recover_owned_groups", lambda _t: None)
    assert daemon._retire_child() is True


def test_retirement_gate_clears_dead_recorded_child(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A dead recorded child is cleared (no destructive signal) despite the gate."""
    monkeypatch.setattr(lifecycle, "worker_alive", lambda _m: False)
    monkeypatch.setattr(
        lifecycle_state,
        "reconcile_authority_facts",
        lambda: _facts(current_child_identity_proven=False),
    )
    state = supervise.read_state()
    supervise.write_state(
        replace(
            state,
            child=supervise.WorkerChild(
                pid=1,
                pgid=1,
                sid=1,
                start_time_ticks=1,
                token="tok" + "a" * 20,
                worker_id="w",
                spawned_at=0.0,
            ),
        )
    )
    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    monkeypatch.setattr(lifecycle, "stop_worker", lambda _m, _g: True)
    monkeypatch.setattr(supervisor, "recover_owned_groups", lambda _t: None)
    assert daemon._retire_child() is True


def test_confirm_gate_refuses_malformed_authority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """_confirm_locked rolls back and refuses when durable authority is malformed."""
    mission = _make_mission(deployctl.STATUS_PENDING)
    monkeypatch.setattr(deployctl, "_read_state", lambda: mission)
    monkeypatch.setattr(
        deployctl,
        "read_rollback_state",
        lambda: (_ for _ in ()).throw(deployctl.DeployCtlError("malformed")),
    )
    monkeypatch.setattr(supervise, "supervisor_running", lambda: True)
    challenge = deployctl._generate_challenge()
    monkeypatch.setattr(
        deployctl,
        "_read_state",
        lambda: replace(mission, challenge_hash=deployctl._challenge_digest(challenge)),
    )
    monkeypatch.setattr(deployctl, "_pending_mission_rollback_due", lambda _s: False)
    monkeypatch.setattr(deployctl, "settle_desired", lambda *_, **__: None)
    with pytest.raises(deployctl.DeployCtlError, match="authority refuses confirmation"):
        deployctl._confirm_locked(
            {"type": "confirm", "commit": mission.commit, "challenge": challenge[::-1]},
            _options(),
        )
    # Fail closed: the malformed mission was NOT rolled back or mutated.
    recorded = deployctl._read_state()
    assert recorded is not None
    assert recorded.status == deployctl.STATUS_PENDING


def test_confirm_gate_allows_legitimate(monkeypatch: pytest.MonkeyPatch) -> None:
    """_confirm_locked proceeds when the authority permits the transition."""
    mission = _make_mission(deployctl.STATUS_PENDING)
    challenge = deployctl._generate_challenge()
    monkeypatch.setattr(
        deployctl,
        "_read_state",
        lambda: replace(mission, challenge_hash=deployctl._challenge_digest(challenge)),
    )
    monkeypatch.setattr(deployctl, "read_rollback_state", lambda: mission)
    monkeypatch.setattr(supervise, "supervisor_running", lambda: True)
    monkeypatch.setattr(deployctl, "_pending_mission_rollback_due", lambda _s: False)
    monkeypatch.setattr(deployctl, "settle_desired", lambda *_, **__: None)
    monkeypatch.setattr(cli, "set_current", lambda _c: None)
    monkeypatch.setattr(cli, "gc_cli_roots", lambda _c: None)
    monkeypatch.setattr(deployctl, "append_deploy_log", lambda _l: None)
    written: list[deployctl.RollbackState] = []
    monkeypatch.setattr(deployctl, "_write_state", written.append)
    response = deployctl._confirm_locked(
        {"type": "confirm", "commit": mission.commit, "challenge": challenge[::-1]},
        _options(),
    )
    assert response["confirmed"] is True
    assert written[-1].status == deployctl.STATUS_CONFIRMED


def test_rollback_gate_refuses_malformed_authority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """_rollback_locked holds (no destructive mutation) when authority is malformed."""
    mission = _make_mission(deployctl.STATUS_PENDING)
    monkeypatch.setattr(deployctl, "_read_state", lambda: mission)
    monkeypatch.setattr(
        deployctl,
        "read_rollback_state",
        lambda: (_ for _ in ()).throw(deployctl.DeployCtlError("malformed")),
    )
    monkeypatch.setattr(supervise, "supervisor_running", lambda: True)
    monkeypatch.setattr(deployctl, "settle_desired", lambda *_, **__: None)
    assert deployctl._rollback_locked(mission) is False
    assert deployctl._read_state() is mission


def test_rollback_gate_allows_legitimate(monkeypatch: pytest.MonkeyPatch) -> None:
    """_rollback_locked proceeds when the authority permits the transition."""
    mission = _make_mission(deployctl.STATUS_PENDING)
    monkeypatch.setattr(deployctl, "read_rollback_state", lambda: mission)
    monkeypatch.setattr(supervise, "supervisor_running", lambda: True)
    monkeypatch.setattr(deployctl, "settle_desired", lambda *_, **__: None)
    monkeypatch.setattr(cli, "remove_cli_root", lambda _c: None)
    monkeypatch.setattr(cli, "reconcile_pointer", lambda _c: True)
    monkeypatch.setattr(deployctl, "append_deploy_log", lambda _l: None)
    assert deployctl._rollback_locked(mission) is True
    recorded = deployctl._read_state()
    assert recorded is not None
    assert recorded.status == deployctl.STATUS_ROLLED_BACK


def test_publish_gate_refuses_conflicting_mission(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """_prepare_locked refuses a new pending mission while one is already pending."""
    existing = _make_mission(deployctl.STATUS_PENDING)
    monkeypatch.setattr(deployctl, "_read_state", lambda: existing)
    # Keep the abandoned-pending cleaner from rolling the existing mission back.
    monkeypatch.setattr(deployctl, "_cleanup_pending_locked", lambda: None)
    monkeypatch.setattr(deployctl, "read_meta", lambda: _make_meta("0" * 40))
    monkeypatch.setattr(deployctl, "worker_alive", lambda _m: True)
    monkeypatch.setattr(deployctl, "_require_exact_commit", lambda *_, **__: None)
    monkeypatch.setattr(deployctl, "_require_clean_checkout", lambda *_, **__: None)
    monkeypatch.setattr(deployctl, "_checkout", lambda *_, **__: True)
    monkeypatch.setattr(deployctl, "run_validation", lambda *_, **__: SimpleNamespace(ok=True))
    monkeypatch.setattr(cli, "build_cli_root", lambda *_, **__: None)
    monkeypatch.setattr(
        deployctl, "_candidate_identity", lambda *_, **__: (None, _make_meta(COMMIT))
    )
    monkeypatch.setattr(deployctl, "check_postgres", lambda *_, **__: True)
    monkeypatch.setattr(deployctl, "_restore_previous_prep", lambda *_, **__: None)
    with pytest.raises(deployctl.DeployCtlError, match="authority refuses a new pending mission"):
        deployctl._prepare_locked(_options(), COMMIT, supervised=True)


def test_respawn_gate_accepts_pending_mission_after_progress_no_desired(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """After real mission progress with no desired.json, the respawn gate passes.

    The supervisor advances ``applied_generation`` to the pending mission
    generation once its candidate is the live worker (``_record_mission_progress``),
    and the respawn publication path gates on ``check_authority_invariants`` over
    the reconciled facts. That gate must not flag GENERATION_MONOTONIC for the
    resulting durable state, so a legitimate respawn is published rather than
    converged and reaped.
    """
    mission = _make_mission(deployctl.STATUS_PENDING)  # generation 5, commit COMMIT
    deployctl._write_state(mission)
    # No desired.json is written: desired_generation reconciles to 0.
    state = supervise.read_state()
    supervise.write_state(
        replace(
            state,
            child=supervise.WorkerChild(
                pid=1,
                pgid=1,
                sid=1,
                start_time_ticks=1,
                token="tok" + "a" * 20,
                worker_id="w",
                spawned_at=0.0,
            ),
            commit=COMMIT,
            ready=True,
            applied_generation=0,
        )
    )
    monkeypatch.setattr(supervisor.SupervisorDaemon, "_child_alive", staticmethod(lambda _s: True))
    monkeypatch.setattr(supervisor, "read_desired", lambda: None)
    monkeypatch.setattr(supervise, "read_desired", lambda: None)
    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    # Drive the real supervisor mutation that records mission progress.
    daemon._record_mission_progress(COMMIT)
    assert supervise.read_state().applied_generation == mission.generation
    facts = reconcile_authority_facts()
    assert facts.mission_status == "pending"
    assert facts.applied_generation == facts.mission_generation
    assert facts.desired_generation == 0
    # The exact predicate the respawn publication gate consults: it must accept
    # applied_generation == mission_generation as valid active authority.
    assert INVARIANT_GENERATION_MONOTONIC not in check_authority_invariants(facts)


def test_publish_gate_allows_when_no_pending_mission(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """_prepare_locked creates the pending mission when no conflict exists."""
    monkeypatch.setattr(deployctl, "_read_state", lambda: None)
    monkeypatch.setattr(deployctl, "_cleanup_pending_locked", lambda: None)
    monkeypatch.setattr(deployctl, "read_meta", lambda: _make_meta("0" * 40))
    monkeypatch.setattr(deployctl, "worker_alive", lambda _m: True)
    monkeypatch.setattr(deployctl, "_require_exact_commit", lambda *_, **__: None)
    monkeypatch.setattr(deployctl, "_require_clean_checkout", lambda *_, **__: None)
    monkeypatch.setattr(deployctl, "_checkout", lambda *_, **__: True)
    monkeypatch.setattr(deployctl, "run_validation", lambda *_, **__: SimpleNamespace(ok=True))
    monkeypatch.setattr(cli, "build_cli_root", lambda *_, **__: None)
    monkeypatch.setattr(
        deployctl, "_candidate_identity", lambda *_, **__: (None, _make_meta(COMMIT))
    )
    monkeypatch.setattr(deployctl, "check_postgres", lambda *_, **__: True)
    state, _gated = deployctl._prepare_locked(_options(), COMMIT, supervised=True)
    assert state.status == deployctl.STATUS_PENDING
