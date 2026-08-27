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
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from lubko import cli, lifecycle, lifecycle_state, supervise, supervisor
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
    assert_authority_invariants,
    authorize_mission_confirm,
    authorize_mission_publish,
    authorize_mission_rollback,
    authorize_recovery,
    authorize_retirement,
    authorize_spawn,
    check_authority_invariants,
    current_phase,
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
        recovery_authority=False,
        candidate_ready=False,
        rollback_pending=False,
        durable_malformed=False,
        supervisor_child_present=False,
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

    assert authorize_retirement(_facts(owned_worker_identity_proven=True)) is True
    assert authorize_retirement(_facts()) is False

    assert authorize_mission_publish(_facts()) is True
    assert authorize_mission_publish(_facts(mission_status="pending")) is False
    assert authorize_mission_publish(_facts(durable_malformed=True)) is False
    assert authorize_mission_confirm(_facts(mission_status="pending", candidate_ready=True)) is True
    assert (
        authorize_mission_confirm(_facts(mission_status="pending", candidate_ready=False)) is False
    )
    assert authorize_mission_confirm(_facts(mission_status="confirmed")) is False
    assert authorize_mission_rollback(_facts(mission_status="pending")) is True
    assert (
        authorize_mission_rollback(_facts(durable_malformed=True, mission_status="pending"))
        is False
    )


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
