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
    """High-level authority phase derived from the real durable sources."""

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


def current_phase() -> LifecyclePhase:
    """Derive the authoritative lifecycle phase from current durable state.

    The phase is computed only from fields that actually persist: the maintained
    worker metadata, the rollback/deploy mission state, and the supervisor
    daemon state. A malformed or unreadable durable source forces
    :attr:`LifecyclePhase.OWNERSHIP_PENDING` so the authority fails closed.

    Returns:
        The phase that best describes the current durable facts.
    """
    from lubko import deployctl, lifecycle, supervise  # ruff: ignore[import-outside-top-level]

    malformed = False
    mission_status: str | None = None
    owned_proven = False
    supervisor_child_alive = False
    spawning = False
    unresolved_child = False
    blocking_hold = False

    meta = lifecycle.read_meta()
    if meta is not None:
        owned_proven = lifecycle.worker_alive(meta)

    try:
        mission = deployctl.read_rollback_state()
    except deployctl.DeployCtlError:
        malformed = True
        mission = None
    if mission is not None:
        mission_status = mission.status

    try:
        state = supervise.read_state()
    except Exception:  # ruff: ignore[blind-except] - any read failure is treated fail-closed
        state = None
    if state is not None:
        supervisor_child_alive = state.child is not None and supervise.child_alive(state.child)
        spawning = state.spawning is not None
        unresolved_child = state.unresolved_child is not None
        blocking_hold = (
            state.ownership_hold_malformed
            or state.unresolved_hold_malformed
            or state.spawning_hold_malformed
        )

    ordered_phases: tuple[tuple[bool, LifecyclePhase], ...] = (
        (malformed or blocking_hold, LifecyclePhase.OWNERSHIP_PENDING),
        (mission_status == "pending", LifecyclePhase.MISSION_PENDING),
        (mission_status == "confirmed", LifecyclePhase.CONFIRMED),
        (mission_status == "rolled_back", LifecyclePhase.ROLLED_BACK),
        (supervisor_child_alive or owned_proven, LifecyclePhase.RUNNING),
        (spawning, LifecyclePhase.SPAWN_OBLIGATION),
        (unresolved_child, LifecyclePhase.SPAWNING),
    )
    for condition, phase in ordered_phases:
        if condition:
            return phase
    return LifecyclePhase.UNMANAGED


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
