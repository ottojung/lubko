"""Lifecycle authority state machine for Lubko.

This is the small, typed authority layer described in
``docs/lifecycle_authority_state_machine.md``. It does not introduce a second
persisted model: the derived :class:`LifecyclePhase` and the guard functions read
the genuine durable sources (maintained ``meta.json``, the rollback/deploy
mission state, and the supervisor ``SupervisorState``) and make only decisions
that are provable from those fields.

The runtime call sites in :mod:`lubko.lifecycle`, :mod:`lubko.deployctl`, and
:mod:`lubko.supervisor` route their authority decisions through the guard
functions here, so the fail-closed rules can no longer drift across modules.

The model is genuinely executable:

* :class:`AuthorityFacts` is the reconciled durable + observed snapshot the
  authority decides on, built by :func:`reconcile_authority_facts` from the real
  sources (failing closed on unreadable/corrupt state).
* :func:`assert_authority_invariants` (and its non-raising sibling
  :func:`check_authority_invariants`) enforce the seven explicit invariants from
  the design doc at every authority boundary.
* :func:`authorize_spawn`, :func:`authorize_recovery`, :func:`authorize_retirement`,
  :func:`authorize_mission_publish`, :func:`authorize_mission_confirm`, and
  :func:`authorize_mission_rollback` are the pure, fact-derived transition /
  authorization decisions the runtime routes its substantive lifecycle operations
  through.

A :data:`FAILPOINT_*` seam emits a no-op ``failpoint`` call at each real
durable/side-effect boundary. Deterministic crash tests arm a named failpoint to
inject a failure exactly there and assert the invariants hold; production never
arms them, so behavior is unchanged.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    from collections.abc import Iterator

LOGGER = logging.getLogger(__name__)

# Failpoint boundary identifiers. Each is a no-op unless a deterministic test
# arms it; the constants exist so the boundaries are named in one place.
FAILPOINT_POPEN: Final = "popen"
FAILPOINT_METADATA_PUBLICATION: Final = "metadata_publication"
FAILPOINT_PROCESS_RETIREMENT: Final = "process_retirement"
FAILPOINT_DB_RECOVERY: Final = "db_recovery"
FAILPOINT_MISSION_PUBLISH: Final = "mission_publish"
FAILPOINT_MISSION_CONFIRM: Final = "mission_confirm"
FAILPOINT_MISSION_ROLLBACK: Final = "mission_rollback"
FAILPOINT_MISSION_SETTLEMENT_APPLIED: Final = "mission_settlement_applied"
FAILPOINT_SUPERVISOR_SPAWNING_WRITE: Final = "supervisor.spawning_write"
FAILPOINT_SUPERVISOR_PID_UPGRADE: Final = "supervisor.pid_upgrade_write"
FAILPOINT_SUPERVISOR_SPAWNING_CLEARANCE: Final = "supervisor.spawning_clearance"
FAILPOINT_SUPERVISOR_UNRESOLVED_CHILD: Final = "supervisor.unresolved_child_write"


class FailpointError(RuntimeError):
    """Raised when an armed failpoint fires, simulating a crash at a boundary."""

    def __init__(self, name: str) -> None:
        """Initialize the error with the triggering boundary name.

        Args:
            name: The boundary identifier that triggered the failpoint.
        """
        super().__init__(f"failpoint triggered at boundary: {name}")
        self.name = name


@dataclass(frozen=True, slots=True)
class _FailpointSpec:
    """An armed failpoint specification.

    Attributes:
        exc: The exception to raise when the failpoint fires, or ``None`` to
            raise :class:`FailpointError`.
    """

    exc: BaseException | None


_FAILPOINTS: dict[str, _FailpointSpec] = {}


def arm_failpoint(name: str, *, exc: BaseException | None = None) -> None:
    """Arm a failpoint so ``failpoint(name)`` raises, simulating a crash.

    Args:
        name: The boundary identifier (see the ``FAILPOINT_*`` constants).
        exc: The exception to raise when triggered; defaults to
            :class:`FailpointError`.
    """
    _FAILPOINTS[name] = _FailpointSpec(exc)


def disarm_failpoints() -> None:
    """Clear every armed failpoint, restoring default no-op behavior."""
    _FAILPOINTS.clear()


def failpoint(name: str) -> None:
    """Emit a failpoint at an authority boundary.

    Default behavior is a no-op. When the named failpoint is armed, this raises
    ``exc`` (or :class:`FailpointError`) to simulate a crash exactly at the
    boundary, before any side effect runs.

    Args:
        name: The boundary identifier.

    Raises:
        FailpointError: When the failpoint is armed without a custom exception.
    """
    spec = _FAILPOINTS.get(name)
    if spec is not None:
        if spec.exc is not None:
            raise spec.exc
        raise FailpointError(name)


@contextmanager
def armed_failpoint(name: str, *, exc: BaseException | None = None) -> Iterator[None]:
    """Context manager arming ``name`` for the duration of the block.

    Args:
        name: The boundary identifier to arm.
        exc: The exception to raise when triggered; defaults to
            :class:`FailpointError`.

    Yields:
        Nothing.
    """
    arm_failpoint(name, exc=exc)
    try:
        yield
    finally:
        disarm_failpoints()


class LifecyclePhase(StrEnum):
    """High-level authority phase derived from the reconciled :class:`AuthorityFacts`."""

    UNMANAGED = "unmanaged"
    OWNERSHIP_PENDING = "ownership_pending"
    SPAWN_OBLIGATION = "spawn_obligation"
    SPAWNING = "spawning"
    RUNNING = "running"
    RETIRING = "retiring"
    STOPPED = "stopped"
    RECOVERING = "recovering"
    MISSION_PENDING = "mission_pending"
    CONFIRMED = "confirmed"
    ROLLED_BACK = "rolled_back"


# ---------------------------------------------------------------------------
# Authority facts: the reconciled durable + observed snapshot
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class AuthorityFacts:
    """Reconciled durable + observed snapshot the authority decides on.

    Every field is derived from sources that genuinely persist (maintained
    ``meta.json``, the rollback/deploy mission state, and the supervisor
    ``SupervisorState``) or from observed process identity. A corrupt or
    unreadable durable source forces ``durable_malformed`` so the authority fails
    closed rather than granting or removing authority.

    Attributes:
        desired_generation: Generation the supervisor currently intends to apply.
        applied_generation: Generation the supervisor has durably applied.
        mission_status: Rollback/deploy mission status, or ``None`` when none.
        mission_generation: Generation the open mission was allocated under.
        mission_commit: Commit the open mission targets.
        owned_worker_pid: PID of the maintained worker in ``meta.json``, if any.
        owned_worker_commit: Commit recorded for the maintained worker.
        owned_worker_identity_proven: Whether the maintained worker is proven alive.
        pre_spawn_obligation: A durable pre-``Popen`` spawning obligation exists.
        unresolved_child: An unresolved spawned child hold exists (recovery due).
        candidate_ready: The candidate worker has proven queue readiness.
        rollback_pending: A supervised rollback is pending confirmation.
        durable_malformed: Any durable authority source is unreadable/corrupt.
        supervisor_child_present: The supervisor has published a live child.
        current_child_identity_proven: The recorded child is our exact live
            direct child (the retirement proof).
        ownership_hold_malformed: A malformed ownership hold is blocking.
        unresolved_hold_malformed: A malformed unresolved-child hold is blocking.
        spawning_hold_malformed: A malformed spawning obligation is blocking.
    """

    desired_generation: int
    applied_generation: int
    mission_status: str | None
    mission_generation: int | None
    mission_commit: str | None
    owned_worker_pid: int | None
    owned_worker_commit: str | None
    owned_worker_identity_proven: bool
    pre_spawn_obligation: bool
    unresolved_child: bool
    candidate_ready: bool
    rollback_pending: bool
    durable_malformed: bool
    supervisor_child_present: bool
    current_child_identity_proven: bool
    ownership_hold_malformed: bool
    unresolved_hold_malformed: bool
    spawning_hold_malformed: bool

    def live_consumers(self) -> int:
        """Return the count of live, authorized queue consumers.

        The proven owned worker and an unresolved child (the worker awaiting
        owned-group recovery) are mutually exclusive consumer roles; their sum
        must never exceed one. A ready recovery worker is the *resolution* of an
        unresolved child, not a second consumer, so the two are never counted
        twice.

        Returns:
            The number of currently live consumers (``0`` or ``1`` in any valid
            authority state).
        """
        roles = (
            self.owned_worker_identity_proven or self.supervisor_child_present,
            self.unresolved_child,
        )
        return sum(1 for role in roles if role)


def reconcile_authority_facts() -> AuthorityFacts:
    """Reconcile the durable + observed authority facts from the real sources.

    The reads fail closed: an unreadable/corrupt rollback state or supervisor
    state sets the appropriate malformed flag rather than raising, so the
    authority never implicitly trusts or erases corrupt durable state.

    Returns:
        The reconciled :class:`AuthorityFacts` snapshot.
    """
    from lubko import deployctl, lifecycle, supervise  # ruff: ignore[import-outside-top-level]

    malformed = False
    owned_pid: int | None = None
    owned_commit: str | None = None
    owned_proven = False
    desired_generation = 0

    try:
        meta = lifecycle.read_meta_strict()
    except Exception:  # ruff: ignore[blind-except] - unreadable meta fails closed
        meta = None
        malformed = True
    if meta is not None:
        owned_pid = meta.pid
        owned_commit = meta.git_commit
        owned_proven = lifecycle.worker_alive(meta)

    try:
        mission = deployctl.read_rollback_state()
    except deployctl.DeployCtlError:
        malformed = True
        mission = None

    try:
        desired_intent = supervise.read_desired_strict()
    except Exception:  # ruff: ignore[blind-except] - unreadable desired intent fails closed
        desired_intent = None
        malformed = True
    else:
        desired_generation = desired_intent.generation if desired_intent is not None else 0

    try:
        state = supervise.read_state()
    except Exception:  # ruff: ignore[blind-except] - unreadable supervisor state fails closed
        state = None
        malformed = True

    applied_generation = state.applied_generation if state is not None else 0
    if state is not None:
        malformed = malformed or (
            state.ownership_hold_malformed
            or state.unresolved_hold_malformed
            or state.spawning_hold_malformed
        )
    child = state.child if state is not None else None
    spawning = state.spawning if state is not None else None
    unresolved = state.unresolved_child if state is not None else None
    current_child_identity_proven = child is not None and supervise.child_is_our_direct_child(child)

    return AuthorityFacts(
        desired_generation=desired_generation,
        applied_generation=applied_generation,
        mission_status=mission.status if mission is not None else None,
        mission_generation=(
            getattr(mission, "settlement_generation", None) or mission.generation
            if mission is not None and mission.status == "pending"
            else mission.generation
            if mission is not None
            else None
        ),
        mission_commit=(
            getattr(mission, "settlement_commit", None) or mission.commit
            if mission is not None and mission.status == "pending"
            else mission.commit
            if mission is not None
            else None
        ),
        owned_worker_pid=owned_pid,
        owned_worker_commit=owned_commit,
        owned_worker_identity_proven=owned_proven,
        pre_spawn_obligation=spawning is not None,
        unresolved_child=unresolved is not None,
        candidate_ready=state.ready if state is not None else False,
        rollback_pending=mission.status == "pending" if mission is not None else False,
        durable_malformed=malformed,
        supervisor_child_present=child is not None and supervise.child_alive(child),
        current_child_identity_proven=current_child_identity_proven,
        ownership_hold_malformed=state.ownership_hold_malformed if state is not None else False,
        unresolved_hold_malformed=state.unresolved_hold_malformed if state is not None else False,
        spawning_hold_malformed=state.spawning_hold_malformed if state is not None else False,
    )


def phase_from_facts(facts: AuthorityFacts) -> LifecyclePhase:
    """Derive the authoritative phase from a reconciled :class:`AuthorityFacts`.

    Args:
        facts: The reconciled authority snapshot.

    Returns:
        The phase that best describes the durable facts.
    """
    ordered_phases: tuple[tuple[bool, LifecyclePhase], ...] = (
        (
            facts.durable_malformed
            or facts.ownership_hold_malformed
            or facts.unresolved_hold_malformed
            or facts.spawning_hold_malformed,
            LifecyclePhase.OWNERSHIP_PENDING,
        ),
        (facts.mission_status == "pending", LifecyclePhase.MISSION_PENDING),
        (facts.mission_status == "confirmed", LifecyclePhase.CONFIRMED),
        (facts.mission_status == "rolled_back", LifecyclePhase.ROLLED_BACK),
        (
            facts.supervisor_child_present or facts.owned_worker_identity_proven,
            LifecyclePhase.RUNNING,
        ),
        (facts.pre_spawn_obligation, LifecyclePhase.SPAWN_OBLIGATION),
        (facts.unresolved_child, LifecyclePhase.SPAWNING),
    )
    for condition, phase in ordered_phases:
        if condition:
            return phase
    return LifecyclePhase.UNMANAGED


def current_phase() -> LifecyclePhase:
    """Derive the authoritative lifecycle phase from current durable state.

    The phase is computed from the reconciled :class:`AuthorityFacts` (built by
    :func:`reconcile_authority_facts` from the genuine durable sources). A
    malformed or unreadable durable source forces
    :attr:`LifecyclePhase.OWNERSHIP_PENDING` so the authority fails closed.

    Returns:
        The phase that best describes the current durable facts.
    """
    return phase_from_facts(reconcile_authority_facts())


# ---------------------------------------------------------------------------
# Invariants
# ---------------------------------------------------------------------------

INVARIANT_SINGLE_CONSUMER: Final = "SINGLE_CONSUMER"
INVARIANT_GENERATION_MONOTONIC: Final = "GENERATION_MONOTONIC"
INVARIANT_MALFORMED_NEVER_ERASED: Final = "MALFORMED_NEVER_ERASED"
INVARIANT_NO_REPLACEMENT_WHILE_UNRESOLVED: Final = "NO_REPLACEMENT_WHILE_UNRESOLVED"
INVARIANT_NO_SIGNAL_WITHOUT_PROOF: Final = "NO_SIGNAL_WITHOUT_PROOF"
INVARIANT_CRASH_CONVERGES_TO_ONE_OR_ZERO: Final = "CRASH_CONVERGES_TO_ONE_OR_ZERO"
INVARIANT_NO_LIVE_CONSUMER_WITHOUT_AUTHORITY: Final = "NO_LIVE_CONSUMER_WITHOUT_AUTHORITY"

AUTHORITY_INVARIANT_CODES: Final = (
    INVARIANT_SINGLE_CONSUMER,
    INVARIANT_GENERATION_MONOTONIC,
    INVARIANT_MALFORMED_NEVER_ERASED,
    INVARIANT_NO_REPLACEMENT_WHILE_UNRESOLVED,
    INVARIANT_NO_SIGNAL_WITHOUT_PROOF,
    INVARIANT_CRASH_CONVERGES_TO_ONE_OR_ZERO,
    INVARIANT_NO_LIVE_CONSUMER_WITHOUT_AUTHORITY,
)


class AuthorityInvariantError(RuntimeError):
    """Raised when the authority violates one of its invariants.

    Attributes:
        code: The violated invariant code (see :data:`AUTHORITY_INVARIANT_CODES`).
        facts: The reconciled facts at the moment of the violation.
    """

    def __init__(self, code: str, facts: AuthorityFacts) -> None:
        """Initialize the error with its code and the offending facts.

        Args:
            code: The violated invariant code.
            facts: The reconciled authority facts at the violation.
        """
        super().__init__(f"lifecycle authority invariant violated: {code}")
        self.code = code
        self.facts = facts


def _generation_monotonic_violations(facts: AuthorityFacts) -> list[str]:
    """Return generation-monotonicity violations for the reconciled ``facts``.

    Generations never move backward or silently reuse authority. A pending
    supervised mission is a valid active generation authority: its generation is
    a trusted source alongside ``desired_generation``, so an applied generation
    equal to the pending mission generation (even with an absent or older
    ``desired``) is accepted rather than flagged. Applied must never exceed
    *every* trusted generation source, and a stale mission below the applied
    generation remains a violation.

    Args:
        facts: The reconciled authority snapshot to check.

    Returns:
        A list containing :data:`INVARIANT_GENERATION_MONOTONIC` when violated.
    """
    violations: list[str] = []
    if facts.applied_generation < 0:
        violations.append(INVARIANT_GENERATION_MONOTONIC)
        return violations
    trusted_generation = facts.desired_generation
    if facts.mission_status == "pending" and facts.mission_generation is not None:
        trusted_generation = max(trusted_generation, facts.mission_generation)
    if facts.applied_generation > trusted_generation:
        violations.append(INVARIANT_GENERATION_MONOTONIC)
    if (
        facts.mission_status == "pending"
        and facts.mission_generation is not None
        and facts.mission_generation < facts.applied_generation
    ):
        violations.append(INVARIANT_GENERATION_MONOTONIC)
    return violations


def _authority_violations(facts: AuthorityFacts) -> list[str]:
    """Return the codes of every invariant violated by ``facts``.

    Args:
        facts: The reconciled authority snapshot to check.

    Returns:
        A list of violated invariant codes (empty when the authority is sound).
    """
    live_owned = facts.owned_worker_identity_proven or facts.supervisor_child_present
    violations: list[str] = []

    # 1 + 6: at most one live consumer, at every crash boundary.
    if facts.live_consumers() > 1:
        violations.extend((INVARIANT_SINGLE_CONSUMER, INVARIANT_CRASH_CONVERGES_TO_ONE_OR_ZERO))

    # 2: generations never move backward or silently reuse authority.
    violations.extend(_generation_monotonic_violations(facts))

    # 3: malformed durable authority is never implicitly erased or trusted.
    if facts.durable_malformed:
        blocking = (
            facts.pre_spawn_obligation
            or facts.unresolved_child
            or facts.ownership_hold_malformed
            or facts.unresolved_hold_malformed
            or facts.spawning_hold_malformed
        )
        if not blocking and not live_owned:
            violations.append(INVARIANT_MALFORMED_NEVER_ERASED)

    # 4: no replacement starts while an earlier consumer/process fate is unresolved.
    if facts.unresolved_child and (facts.pre_spawn_obligation or facts.supervisor_child_present):
        violations.append(INVARIANT_NO_REPLACEMENT_WHILE_UNRESOLVED)

    # 5: no process is signalled without an exact identity proof.
    if (
        facts.supervisor_child_present
        and facts.owned_worker_pid is not None
        and not facts.owned_worker_identity_proven
    ):
        violations.append(INVARIANT_NO_SIGNAL_WITHOUT_PROOF)

    # 7: there is never a live consumer without durable replacement-blocking authority.
    if live_owned and facts.durable_malformed:
        violations.append(INVARIANT_NO_LIVE_CONSUMER_WITHOUT_AUTHORITY)

    return violations


def check_authority_invariants(facts: AuthorityFacts) -> list[str]:
    """Return the codes of every invariant violated by ``facts``.

    Args:
        facts: The reconciled authority snapshot to check.

    Returns:
        A list of violated invariant codes (empty when the authority is sound).
    """
    return _authority_violations(facts)


def assert_authority_invariants(facts: AuthorityFacts) -> None:
    """Assert the authority invariants hold for ``facts``.

    Raises:
        AuthorityInvariantError: On the first violated invariant (carrying its
            code and the offending facts).
    """
    violations = _authority_violations(facts)
    if violations:
        raise AuthorityInvariantError(violations[0], facts)


# ---------------------------------------------------------------------------
# Transition / authorization decisions for substantive lifecycle operations
# ---------------------------------------------------------------------------


def authorize_spawn(facts: AuthorityFacts) -> bool:
    """Decide whether a new worker spawn may be authorized.

    A spawn is authorized only when no blocking hold, no unresolved earlier child,
    and no existing live consumer exist, and the durable authority is not
    malformed. This is the centralized re-expression of the supervisor's
    no-replacement-while-unresolved gate.

    Args:
        facts: The reconciled authority snapshot at the spawn boundary.

    Returns:
        ``True`` when a spawn may proceed.
    """
    if facts.durable_malformed:
        return False
    if facts.pre_spawn_obligation or facts.unresolved_child:
        return False
    if facts.owned_worker_identity_proven or facts.supervisor_child_present:
        return False
    return facts.live_consumers() == 0


def authorize_recovery(facts: AuthorityFacts) -> bool:
    """Decide whether owned-command-group recovery may proceed.

    Recovery is appropriate only when an unresolved child hold exists (the
    recovery authority is active), no proven live consumer competes, and the
    durable authority is not malformed.

    Args:
        facts: The reconciled authority snapshot at the recovery boundary.

    Returns:
        ``True`` when recovery may proceed.
    """
    if facts.durable_malformed:
        return False
    if not facts.unresolved_child:
        return False
    return not facts.owned_worker_identity_proven


def authorize_retirement(facts: AuthorityFacts) -> bool:
    """Decide whether a worker retirement may be authorized.

    Retirement may only claim a target whose exact identity is *proven to be our
    own live direct child*; signalling an unproven, reparented, or recycled
    process is forbidden (fail closed). A malformed durable authority also
    refuses, so an unreadable state never authorizes a destructive signal.

    Args:
        facts: The reconciled authority snapshot at the retirement boundary.

    Returns:
        ``True`` when the recorded worker may be retired.
    """
    if facts.durable_malformed:
        return False
    return facts.current_child_identity_proven


def authorize_mission_publish(facts: AuthorityFacts) -> bool:
    """Decide whether a new supervised mission may be published.

    A mission may be published only when none is already pending and the durable
    authority is not malformed.

    Args:
        facts: The reconciled authority snapshot at the mission-publish boundary.

    Returns:
        ``True`` when a mission may be published.
    """
    if facts.durable_malformed:
        return False
    return facts.mission_status != "pending"


def authorize_mission_confirm(facts: AuthorityFacts) -> bool:
    """Decide whether a pending mission may be confirmed.

    Confirmation requires the candidate to be proven queue-ready and the durable
    authority to be sound.  The transition is idempotent: an already-confirmed
    mission replays safely (a same-mission no-op), so it is authorized rather
    than refused.

    Args:
        facts: The reconciled authority snapshot at the mission-confirm boundary.

    Returns:
        ``True`` when the mission may be confirmed (or is already confirmed).
    """
    if facts.durable_malformed:
        return False
    if facts.mission_status == "confirmed":
        # Same-mission replay is explicitly safe.
        return True
    return facts.mission_status == "pending" and facts.candidate_ready


def authorize_mission_rollback(facts: AuthorityFacts) -> bool:
    """Decide whether a pending mission may be rolled back.

    Rollback is appropriate for a pending mission whose durable authority is
    sound; the caller still proves the candidate dead before mutating state.
    The transition is idempotent: an already-rolled-back mission replays safely,
    so it is authorized rather than refused.

    Args:
        facts: The reconciled authority snapshot at the mission-rollback boundary.

    Returns:
        ``True`` when the mission may be rolled back (or is already rolled back).
    """
    if facts.durable_malformed:
        return False
    if facts.mission_status == "rolled_back":
        # Same-mission replay is explicitly safe.
        return True
    return facts.mission_status == "pending"


def mutation_blocker_reason() -> str | None:
    """Return why ordinary lifecycle mutation is currently blocked, if it is.

    This is a behavior-preserving re-expression of the rollback-state guard that
    ``lubko.lifecycle._supervised_mutation_blocker`` consults. It fails closed on
    unreadable/corrupt supervised mission state.

    Returns:
        ``None`` when mutation may proceed, otherwise a human-readable refusal
        reason.
    """
    from lubko import deployctl  # ruff: ignore[import-outside-top-level]

    try:
        mission = deployctl.read_rollback_state()
    except deployctl.DeployCtlError as exc:
        return (
            f"supervised deployment state is unreadable or corrupt ({exc}); "
            "refusing to mutate lifecycle state; inspect with 'lubko-deploy-ctl status'"
        )
    if mission is None:
        return None
    if mission.status == deployctl.STATUS_PENDING:
        return (
            f"a supervised checkout of commit {mission.commit} "
            f"(generation {mission.generation}) is still pending confirmation; "
            "lifecycle mutation is blocked until it is resolved with "
            "'lubko-deploy-ctl confirm' or 'lubko-deploy-ctl rollback'"
        )
    if mission.status not in {deployctl.STATUS_CONFIRMED, deployctl.STATUS_ROLLED_BACK}:
        return (
            f"supervised deployment state has unknown status {mission.status!r}; "
            "refusing to mutate lifecycle state"
        )
    return None


def mutation_blocked() -> bool:
    """Return whether ordinary lifecycle mutation is currently blocked.

    Returns:
        ``True`` when :func:`mutation_blocker_reason` returns a refusal reason.
    """
    return mutation_blocker_reason() is not None


def refuses_version_change(previous: object | None, commit: str, *, git_commit: str | None) -> bool:
    """Decide whether an ordinary deploy would change the recorded version.

    Behavior-preserving re-expression of
    ``lubko.lifecycle._refuse_version_changing_deploy``: once any maintained
    worker is recorded (running, stopped, or otherwise non-live), an ordinary
    deploy must never change its commit. The previous git commit is passed
    explicitly so this module never depends on the concrete metadata type.

    Args:
        previous: Previously recorded worker metadata, or ``None``.
        commit: Exact validated target commit.
        git_commit: The previously recorded commit, or ``None`` when no worker
            was recorded.

    Returns:
        ``True`` when the deploy must be refused.
    """
    return previous is not None and git_commit != commit
