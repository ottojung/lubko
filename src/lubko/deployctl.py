"""Crash-safe supervised self-deployment for Lubko.

``lubko-deploy-ctl`` is the stable control plane used for version-changing
self-deployments. A checkout is provisional until two requests traverse the
replacement worker: first an exact-commit confirmation that returns a fresh
7-character lowercase hexadecimal challenge, then a second exact-commit
confirmation containing that challenge reversed. Until both complete, a forked
watchdog retains the known-good process image and restores the previous commit
automatically on timeout or candidate failure.

The global command line tools are kept coherent with the confirmed commit:
the candidate CLI environment is built during the provisional phase, and the
``current`` pointer is switched only after durable ``confirmed`` state exists,
so rollback can never strand the global CLIs on candidate code. See
:mod:`lubko.cli`.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import secrets
import socket
import subprocess
import sys
import time
from contextlib import suppress
from dataclasses import dataclass, replace
from pathlib import Path
from typing import TYPE_CHECKING, Final
from uuid import UUID

import psycopg
from psycopg.rows import tuple_row

from lubko import cli, lifecycle_state, supervise
from lubko.config import load_database_config
from lubko.durable import write_json_durable
from lubko.lifecycle import (
    SCHEMA_VERSION,
    STATE_RUNNING,
    LockTimeoutError,
    ProcessIdentity,
    WorkerMeta,
    _converge_unproven_spawn,
    append_deploy_log,
    check_postgres,
    deploy_lock,
    detach_standard_streams,
    process_identity,
    read_meta,
    run_validation,
    spawn_worker,
    stop_worker,
    worker_alive,
    worker_env,
    worker_log_path,
    write_meta,
)
from lubko.state import rollback_state_path
from lubko.toolchain import UvResolutionError, resolve_uv
from lubko.worker import JOB_ID_ENV

if TYPE_CHECKING:
    from collections.abc import Sequence

LOGGER: Final = logging.getLogger(__name__)

EXIT_OK: Final = 0
EXIT_ERROR: Final = 1
ROLLBACK_SCHEMA_VERSION: Final = 3
SUPPORTED_ROLLBACK_SCHEMA_VERSIONS: Final = frozenset({2})
STATUS_PENDING: Final = "pending"
STATUS_CONFIRMED: Final = "confirmed"
STATUS_ROLLED_BACK: Final = "rolled_back"
SETTLEMENT_CONFIRM: Final = "confirm"
SETTLEMENT_ROLLBACK: Final = "rollback"
DEFAULT_CONFIRM_WINDOW_SECONDS: Final = 120.0
DEFAULT_STOP_GRACE_SECONDS: Final = 5.0
DEFAULT_POSTGRES_TIMEOUT_SECONDS: Final = 5.0
DEFAULT_LOCK_TIMEOUT_SECONDS: Final = 30.0
DEFAULT_VALIDATION_TIMEOUT_SECONDS: Final = 1200.0
DEFAULT_GIT_TIMEOUT_SECONDS: Final = 10.0
DEFAULT_CLI_TIMEOUT_SECONDS: Final = cli.DEFAULT_BUILD_TIMEOUT_SECONDS
IDENTITY_TIMEOUT_SECONDS: Final = 5.0
IDENTITY_POLL_SECONDS: Final = 0.02
POST_RELEASE_STABILITY_SECONDS: Final = 0.25
WATCHDOG_POLL_SECONDS: Final = 0.5
COMMIT_RE: Final = re.compile(r"[0-9a-f]{40}")
CHALLENGE_RE: Final = re.compile(r"[0-9a-f]{7}")
HANDOFF_POLL_SECONDS: Final = 0.1
HANDOFF_RESPONSE_MAX_BYTES: Final = 1048576
HELPER_ERROR_MAX_CHARS: Final = 8000
HANDOFF_DURABLE_WAIT_SECONDS: Final = 60.0

GATED_SHIM_SOURCE: Final = """
import os
import sys
fd = int(sys.argv[1])
uv = sys.argv[2]
try:
    command = os.read(fd, 1)
finally:
    os.close(fd)
if command != b"G":
    raise SystemExit(2)
os.execvpe(uv, [uv, "run", "lubko-worker"], os.environ)
""".strip()


class DeployCtlError(RuntimeError):
    """Raised when a supervised deployment cannot proceed safely."""


@dataclass(frozen=True, slots=True)
class Options:
    """Runtime inputs shared by supervised-deployment operations."""

    repo: Path
    uv_path: str
    confirm_window_seconds: float
    stop_grace_seconds: float
    postgres_timeout_seconds: float
    lock_timeout_seconds: float
    validation_timeout_seconds: float
    git_timeout_seconds: float
    cli_timeout_seconds: float


@dataclass(frozen=True, slots=True)
class RollbackState:
    """Durable mission retained until a candidate is confirmed or restored."""

    schema_version: int
    generation: int
    status: str
    commit: str
    previous_commit: str
    challenge_hash: str | None
    deadline: float
    repo: str
    uv_path: str
    stop_grace_seconds: float
    git_timeout_seconds: float
    previous_retiring: bool
    previous_meta: WorkerMeta
    new_meta: WorkerMeta
    supervisor_owned: bool | None = None
    settlement_transition: str | None = None
    settlement_generation: int | None = None
    settlement_commit: str | None = None

    def to_dict(self) -> dict[str, object]:
        """Serialize durable rollback state.

        Returns:
            A JSON-compatible mapping.
        """
        result: dict[str, object] = {
            "schema_version": self.schema_version,
            "generation": self.generation,
            "status": self.status,
            "commit": self.commit,
            "previous_commit": self.previous_commit,
            "challenge_hash": self.challenge_hash,
            "deadline": self.deadline,
            "repo": self.repo,
            "uv_path": self.uv_path,
            "stop_grace_seconds": self.stop_grace_seconds,
            "git_timeout_seconds": self.git_timeout_seconds,
            "previous_retiring": self.previous_retiring,
            "previous_meta": self.previous_meta.to_dict(),
            "new_meta": self.new_meta.to_dict(),
            "supervisor_owned": self.supervisor_owned,
        }
        settlement = (
            self.settlement_transition,
            self.settlement_generation,
            self.settlement_commit,
        )
        if any(value is not None for value in settlement):
            result.update(
                settlement_transition=self.settlement_transition,
                settlement_generation=self.settlement_generation,
                settlement_commit=self.settlement_commit,
            )
        return result

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> RollbackState:
        """Parse durable rollback state strictly.

        Args:
            data: Decoded JSON mapping.

        Returns:
            Parsed rollback state.

        Raises:
            DeployCtlError: If the state is malformed.
        """
        try:
            previous = data["previous_meta"]
            replacement = data["new_meta"]
            if not isinstance(previous, dict) or not isinstance(replacement, dict):
                raise TypeError
            # The generation is recovery authority: it must be a genuine positive
            # JSON integer. Booleans, numeric/string/numeric-float values, and
            # zero or negative numbers are corruption; they must never silently
            # degrade to a usable generation that another allocation could reuse.
            raw_generation = data["generation"]
            if (
                not isinstance(raw_generation, int)
                or isinstance(raw_generation, bool)
                or raw_generation < 1
            ):
                raise ValueError
            generation = raw_generation
            settlement_transition, settlement_generation, settlement_commit = _settlement_fields(
                data
            )
            return cls(
                schema_version=int(data["schema_version"]),
                generation=generation,
                status=str(data["status"]),
                commit=str(data["commit"]),
                previous_commit=str(data["previous_commit"]),
                challenge_hash=_optional_string(data.get("challenge_hash")),
                deadline=float(data["deadline"]),
                repo=str(data["repo"]),
                uv_path=str(data["uv_path"]),
                stop_grace_seconds=float(data["stop_grace_seconds"]),
                git_timeout_seconds=float(data["git_timeout_seconds"]),
                previous_retiring=_retiring_flag(data.get("previous_retiring", _ABSENT)),
                previous_meta=WorkerMeta.from_dict(previous),
                new_meta=WorkerMeta.from_dict(replacement),
                supervisor_owned=_optional_bool(data.get("supervisor_owned")),
                settlement_transition=settlement_transition,
                settlement_generation=settlement_generation,
                settlement_commit=settlement_commit,
            )
        except (KeyError, TypeError, ValueError) as exc:
            msg = "supervised deployment state is malformed"
            raise DeployCtlError(msg) from exc


@dataclass(frozen=True, slots=True)
class GatedWorker:
    """Candidate process blocked from consuming the queue until released."""

    proc: subprocess.Popen[bytes]
    gate_writer: int
    meta: WorkerMeta


_ABSENT: Final = object()


def _settlement_fields(
    data: dict[str, object],
) -> tuple[str | None, int | None, str | None]:
    """Parse the optional exact pending-mission settlement handoff.

    Genuine absence means no settlement has begun. Once any handoff field is
    present, all three are mandatory and strict because they are generation
    authority: partial/corrupt state must fail closed rather than degrade to an
    ordinary pending mission.

    Returns:
        The parsed transition, generation, and commit, or three ``None`` values
        when no settlement handoff is present.
    """
    transition = data.get("settlement_transition", _ABSENT)
    generation = data.get("settlement_generation", _ABSENT)
    commit = data.get("settlement_commit", _ABSENT)
    values = (transition, generation, commit)
    if all(value is _ABSENT for value in values):
        return None, None, None
    if any(value is _ABSENT for value in values):
        raise TypeError
    if transition not in {SETTLEMENT_CONFIRM, SETTLEMENT_ROLLBACK}:
        raise ValueError
    if not isinstance(generation, int) or isinstance(generation, bool) or generation < 1:
        raise ValueError
    if not isinstance(commit, str) or COMMIT_RE.fullmatch(commit) is None:
        raise ValueError
    return transition, int(generation), commit


def _retiring_flag(value: object) -> bool:
    """Return the durable ``previous_retiring`` flag.

    Args:
        value: JSON value to inspect; the ``_ABSENT`` sentinel means the key
            is absent.

    Returns:
        The stored boolean, or ``False`` when the key is absent.

    Raises:
        TypeError: If a present value is not a boolean (including null).
    """
    if value is _ABSENT:
        return False
    if not isinstance(value, bool):
        raise TypeError
    return value


def _optional_string(value: object | None) -> str | None:
    """Return a string value or ``None``.

    Args:
        value: JSON value to inspect.

    Returns:
        The string, or ``None``.
    """
    return value if isinstance(value, str) else None


def _optional_bool(value: object | None) -> bool | None:
    """Return a boolean value or ``None`` when absent.

    ``None`` signals unknown lifecycle authority: the caller must fail closed
    rather than granting legacy rollback permission.

    Args:
        value: JSON value to inspect.

    Returns:
        ``True``, ``False``, or ``None`` when the key is absent.
    """
    if isinstance(value, bool):
        return value
    return None


def _write_state(state: RollbackState) -> None:
    """Crash-durably persist rollback authority state.

    ``rollback.json`` is recovery authority: the supervised-deployment mission
    generation is compared against the supervisor desired/applied state after a
    restart, so the write must be confirmed durable before the mission is
    treated as published.

    Args:
        state: State to store.

    Note:
        Fails closed: the write raises :class:`DurabilityError` from
        :func:`lubko.durable.write_json_durable` when it cannot be confirmed
        durable, so callers must not advance a dependent action.
    """
    write_json_durable(rollback_state_path(), state.to_dict())


def _normalize_parsed_state(state: RollbackState) -> RollbackState:
    """Normalize a parsed state onto the current rollback schema.

    Supported older schema versions are parsed explicitly and upgraded in
    memory to the current version so every later rewrite (including mission
    archival) uses the current schema. Missing ownership fields on older
    missions stay ``supervisor_owned=None`` (unknown authority), which every
    consumer must treat fail-closed; they are never implicitly
    legacy-authorized.

    Args:
        state: Parsed state with its on-disk schema version.

    Returns:
        The state pinned to :data:`ROLLBACK_SCHEMA_VERSION` when supported.

    Raises:
        DeployCtlError: If the on-disk version is not supported (including
            unknown future versions).
    """
    if state.schema_version == ROLLBACK_SCHEMA_VERSION:
        return state
    if state.schema_version in SUPPORTED_ROLLBACK_SCHEMA_VERSIONS:
        return replace(state, schema_version=ROLLBACK_SCHEMA_VERSION)
    msg = f"unsupported supervised deployment state version {state.schema_version}"
    raise DeployCtlError(msg)


def _read_state() -> RollbackState | None:
    """Read rollback state, failing closed on corruption.

    Returns:
        Parsed state, or ``None`` when no state file exists.

    Raises:
        DeployCtlError: If an existing state file is invalid.
    """
    path = rollback_state_path()
    try:
        raw = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return None
    except OSError as exc:
        msg = f"cannot read supervised deployment state: {exc}"
        raise DeployCtlError(msg) from exc
    try:
        decoded = json.loads(raw)
    except ValueError as exc:
        msg = "supervised deployment state is not valid JSON"
        raise DeployCtlError(msg) from exc
    if not isinstance(decoded, dict):
        msg = "supervised deployment state must be an object"
        raise DeployCtlError(msg)
    state = RollbackState.from_dict(decoded)
    return _normalize_parsed_state(state)


def next_mission_generation() -> int:
    """Allocate the next monotonic mission generation for a new checkout.

    Must run while the deploy lock is held. The returned generation is
    strictly greater than every durable generation observed so far: the
    existing rollback mission, the supervisor desired intent, and the
    supervisor applied state. A supervisor that compares generations can then
    never mistake a freshly created mission for an older or already-applied
    generation. Allocation is serialized with the desired-intent writes so a
    concurrent restart can never reuse or reorder a generation.

    Returns:
        The next strictly greater positive generation.

    Raises:
        DeployCtlError: If a present supervised-mission authority is unreadable
            or malformed; allocation must fail closed rather than silently
            outrank an untrustworthy open mission.
    """
    with supervise.generation_lock():
        try:
            return supervise.next_generation()
        except supervise.MissionAuthorityError as exc:
            raise DeployCtlError(str(exc)) from exc


def _placeholder_meta(commit: str, repo: str) -> WorkerMeta:
    """Return an identity-less candidate record for a supervisor-owned mission.

    When the external supervisor is live, the candidate process is spawned by
    the daemon, not by deployctl, so deployctl never knows the candidate
    identity at mission-publication time. The placeholder is recorded for
    schema completeness and is deliberately never alive, so nothing in
    deployctl can treat it as a real process; liveness is observed through the
    supervisor's own durable state instead.

    Args:
        commit: Exact candidate commit.
        repo: Maintained checkout the candidate belongs to.

    Returns:
        A non-alive worker metadata record for the mission file.
    """
    return WorkerMeta(
        schema_version=SCHEMA_VERSION,
        state=STATE_RUNNING,
        pid=0,
        pgid=0,
        sid=0,
        start_time_ticks=0,
        token=None,
        repo=repo,
        git_commit=commit,
        worker_id="",
        log_path="",
        started_at=None,
        stopped_at=None,
    )


def _supervised_mission_active(state: RollbackState) -> bool:
    """Return whether the supervisor is currently running the mission candidate.

    In supervised mode the candidate identity lives in the supervisor's durable
    state, so a mission is active exactly when the supervisor tracks that exact
    candidate commit as its live child at or after the mission generation.  The
    recorded child identity is independently verified: a stale ``state.json``
    left by a hard-killed supervisor must not be mistaken for a live candidate.

    Args:
        state: Pending supervised-deployment mission.

    Returns:
        ``True`` when the supervisor owns a proven-live worker for
        ``state.commit`` that it began under this mission generation.
    """
    supervisor_state = supervise.read_state()
    return (
        supervisor_state.commit == state.commit
        and supervisor_state.child is not None
        and supervise.child_alive(supervisor_state.child)
        and supervisor_state.applied_generation >= state.generation
    )


def _mission_candidate_alive(state: RollbackState) -> bool:
    """Return whether the pending-mission candidate is genuinely live.

    With a live external supervisor the candidate liveness is observed through
    the daemon's durable child identity; otherwise the legacy recorded
    candidate metadata is used (one-time bootstrap / emergency path).

    Args:
        state: Pending supervised-deployment mission.

    Returns:
        ``True`` when the candidate consumer is currently live.
    """
    if supervise.supervisor_running():
        return _supervised_mission_active(state)
    return worker_alive(state.new_meta)


def _settlement_spec(
    state: RollbackState,
) -> tuple[str, int, str] | None:
    """Return and validate the exact pending-mission settlement handoff.

    A programmatically constructed partial handoff must fail closed exactly like
    malformed persisted JSON; callers must never silently fall back to ordinary
    pending-mission authority once any settlement field exists.

    Returns:
        The exact transition, generation, and target commit, or ``None`` when no
        settlement handoff has begun.
    """
    values = (
        state.settlement_transition,
        state.settlement_generation,
        state.settlement_commit,
    )
    if all(value is None for value in values):
        return None
    transition, generation, commit = values
    generation_valid = (
        isinstance(generation, int)
        and not isinstance(generation, bool)
        and generation > state.generation
    )
    if (
        transition not in {SETTLEMENT_CONFIRM, SETTLEMENT_ROLLBACK}
        or not generation_valid
        or not isinstance(commit, str)
        or COMMIT_RE.fullmatch(commit) is None
    ):
        raise DeployCtlError("supervised deployment settlement handoff is malformed")
    target = state.commit if transition == SETTLEMENT_CONFIRM else state.previous_commit
    if commit != target:
        raise DeployCtlError("supervised deployment settlement handoff targets the wrong commit")
    return transition, int(generation), commit


def _supervised_mission_authoritative(state: RollbackState) -> bool:
    """Return whether durable supervisor authority still belongs to this mission.

    An ordinary pending mission requires exact applied generation equality. An
    in-progress settlement may additionally own one exact reserved generation,
    durably bound to its transition and target commit before that generation is
    published. No higher desired/applied generation is accepted, even for the
    same commit.

    Returns:
        ``True`` only when observed desired/applied authority is still exactly
        attributable to this mission or its reserved settlement.
    """
    supervisor_state = supervise.read_state()
    try:
        settlement = _settlement_spec(state)
        desired = supervise.read_desired_strict()
    except (DeployCtlError, supervise.DesiredIntentError):
        return False
    if settlement is None:
        return (
            supervisor_state.commit == state.commit
            and supervisor_state.applied_generation == state.generation
        )
    _, generation, target = settlement
    superseded = supervisor_state.applied_generation > generation or (
        desired is not None and desired.generation > generation
    )
    desired_mismatch = (
        desired is not None and desired.generation == generation and desired.commit != target
    )
    if superseded or desired_mismatch:
        return False
    if supervisor_state.applied_generation == generation:
        return supervisor_state.commit == target
    return (
        supervisor_state.applied_generation == state.generation
        and supervisor_state.commit == state.commit
    )


def _pending_mission_rollback_due(state: RollbackState) -> bool:
    """Return whether a pending mission must be rolled back right now.

    Under a live supervisor this mirrors the watchdog policy exactly: a
    transient ``child=None``/restart-backoff observation for the same applied
    target generation is retryable until the confirmation deadline, while
    superseded or contradictory durable supervisor authority fails closed
    immediately.  Without a live supervisor the legacy recorded candidate
    metadata decides.

    Args:
        state: Pending supervised-deployment mission.

    Returns:
        ``True`` when the mission must roll back now.
    """
    if supervise.supervisor_running():
        if not _supervised_mission_authoritative(state):
            return True
        return time.time() >= state.deadline and not _supervised_mission_active(state)
    return time.time() >= state.deadline or not worker_alive(state.new_meta)


def _settlement_exactly_applied(state: RollbackState) -> bool:
    """Return whether the exact reserved settlement is applied and queue-ready."""
    try:
        settlement = _settlement_spec(state)
        desired = supervise.read_desired_strict()
    except (DeployCtlError, supervise.DesiredIntentError):
        return False
    if settlement is None or desired is None:
        return False
    _, generation, target = settlement
    supervisor_state = supervise.read_state()
    return (
        desired.generation == generation
        and desired.commit == target
        and supervisor_state.applied_generation == generation
        and supervisor_state.commit == target
        and supervisor_state.ready
    )


def _read_desired_for_settlement() -> supervise.SupervisorDesired | None:
    """Read strict desired authority while publishing a mission settlement.

    Returns:
        The current desired intent, or ``None`` when no desired intent exists.

    Raises:
        DeployCtlError: If durable desired authority is malformed or unreadable.
    """
    try:
        return supervise.read_desired_strict()
    except supervise.DesiredIntentError as exc:
        raise DeployCtlError(str(exc)) from exc


def _prepare_settlement_reservation(
    current: RollbackState,
    transition: str,
    target: str,
) -> tuple[RollbackState, int]:
    """Resolve or durably reserve the exact settlement generation.

    Args:
        current: Current pending mission under the generation lock.
        transition: Requested terminal settlement transition.
        target: Exact commit the settlement must apply.

    Returns:
        The mission containing the exact reservation and its generation.

    Raises:
        DeployCtlError: If another transition owns the mission, an applied
            confirmation cannot be superseded, or the target is inconsistent.
    """
    settlement = _settlement_spec(current)
    if settlement is not None and settlement[0] != transition:
        can_replace_confirmation = (
            transition == SETTLEMENT_ROLLBACK and settlement[0] == SETTLEMENT_CONFIRM
        )
        if not can_replace_confirmation:
            raise DeployCtlError("a different mission settlement transition is already pending")
        if _settlement_exactly_applied(current):
            raise DeployCtlError("the confirmation settlement is already applied")
        settlement = None
    if settlement is None:
        generation = supervise.next_generation()
        current = replace(
            current,
            settlement_transition=transition,
            settlement_generation=generation,
            settlement_commit=target,
        )
        _write_state(current)
        return current, generation
    _, generation, recorded_target = settlement
    if recorded_target != target:
        raise DeployCtlError("settlement target no longer matches the mission")
    return current, generation


def _validate_settlement_observations(
    supervisor_state: supervise.SupervisorState,
    desired: supervise.SupervisorDesired | None,
    generation: int,
    target: str,
) -> None:
    """Reject authority that contradicts or supersedes an exact settlement.

    Args:
        supervisor_state: Current durable applied supervisor authority.
        desired: Current strict desired authority, if present.
        generation: Reserved settlement generation.
        target: Exact settlement target commit.

    Raises:
        DeployCtlError: If newer or contradictory authority is observed.
    """
    if supervisor_state.applied_generation > generation:
        raise DeployCtlError("supervised deployment settlement was superseded")
    if desired is not None and desired.generation > generation:
        raise DeployCtlError("supervised deployment settlement was superseded")
    if supervisor_state.applied_generation == generation and supervisor_state.commit != target:
        raise DeployCtlError("settlement generation is applied to a different commit")
    if desired is not None and desired.generation == generation and desired.commit != target:
        raise DeployCtlError("settlement generation targets a different commit")


def _write_settlement_desired(
    state: RollbackState,
    generation: int,
    target: str,
) -> None:
    """Publish the exact desired intent for a durably reserved settlement.

    Args:
        state: Mission whose settlement owns the generation.
        generation: Exact reserved generation.
        target: Exact target commit.
    """
    supervise.write_desired(
        supervise.SupervisorDesired(
            schema_version=supervise.SCHEMA_VERSION,
            generation=generation,
            commit=target,
            repo=state.repo,
            uv_path=state.uv_path,
            worker_id=os.getenv("LUBKO_WORKER_ID") or socket.gethostname(),
            requested_at=time.time(),
        )
    )


def _publish_settlement_intent(
    state: RollbackState,
    transition: str,
) -> RollbackState:
    """Durably bind and publish one exact mission settlement generation.

    The deployment lock is held by callers. The shared generation lock makes
    reservation and desired-intent publication one serialized authority
    transition. The mission handoff is written first, so a crash or failed
    desired write leaves replacement-blocking generation authority rather than
    an unrepresented newer intent.

    Returns:
        The pending mission carrying the exact settlement reservation.

    Raises:
        DeployCtlError: If the mission changed, authority was superseded, or the
            requested settlement conflicts with durable authority.
    """
    if transition not in {SETTLEMENT_CONFIRM, SETTLEMENT_ROLLBACK}:
        raise DeployCtlError("unknown supervised deployment settlement transition")
    target = state.commit if transition == SETTLEMENT_CONFIRM else state.previous_commit
    with supervise.generation_lock():
        current = _read_state()
        if (
            current is None
            or current.status != STATUS_PENDING
            or current.generation != state.generation
        ):
            raise DeployCtlError("supervised deployment mission changed during settlement")
        current, generation = _prepare_settlement_reservation(current, transition, target)
        supervisor_state = supervise.read_state()
        desired = _read_desired_for_settlement()
        _validate_settlement_observations(supervisor_state, desired, generation, target)
        if desired is None or desired.generation < generation:
            _write_settlement_desired(state, generation, target)
    return current


def settle_desired(commit: str, repo: str, uv_path: str) -> int:
    """Durably settle a supervised mission target and await queue readiness.

    When ``commit`` is the target of the currently pending supervised mission,
    reserve and persist the exact mission settlement generation before publishing
    that desired intent. Calls outside a pending mission retain the ordinary
    newer-intent behavior used by existing lifecycle operations.

    Args:
        commit: Exact commit the settlement must run.
        repo: Maintained checkout the commit belongs to.
        uv_path: Recorded ``uv`` executable.

    Returns:
        The exact written/resumed settlement generation.

    Raises:
        DeployCtlError: If the exact settlement is superseded, cannot be
            published, or does not become queue-ready.
    """
    mission = _read_state()
    prepared: RollbackState | None = None
    if (
        mission is not None
        and mission.status == STATUS_PENDING
        and commit
        in {
            mission.commit,
            mission.previous_commit,
        }
    ):
        transition = SETTLEMENT_CONFIRM if commit == mission.commit else SETTLEMENT_ROLLBACK
        prepared = _publish_settlement_intent(mission, transition)
        settlement = _settlement_spec(prepared)
        if settlement is None:
            raise DeployCtlError("supervised deployment settlement was not reserved")
        _, generation, _ = settlement
    else:
        generation = supervise.request_run(
            commit,
            repo=repo,
            uv_path=uv_path,
            worker_id=os.getenv("LUBKO_WORKER_ID") or socket.gethostname(),
        )

    lifecycle_state.failpoint(lifecycle_state.FAILPOINT_MISSION_CONFIRM)
    if not supervise.wait_for_generation(generation, supervise.DEFAULT_REQUEST_TIMEOUT_SECONDS):
        raise DeployCtlError("the external supervisor did not apply the settlement intent")
    if not supervise.wait_until_ready(generation, supervise.DEFAULT_REQUEST_TIMEOUT_SECONDS):
        raise DeployCtlError(
            "the external supervisor did not prove the settled worker consumes the queue"
        )
    if prepared is not None:
        if not _settlement_exactly_applied(prepared):
            raise DeployCtlError(
                "supervised deployment settlement was superseded before completion"
            )
        lifecycle_state.failpoint(lifecycle_state.FAILPOINT_MISSION_SETTLEMENT_APPLIED)
    return generation


def publish_mission(state: RollbackState, lock_timeout_seconds: float) -> None:
    """Durably publish a prepared pending mission and arm its watchdog.

    Args:
        state: Prepared pending mission (may already be durable; idempotent).
        lock_timeout_seconds: Deployment-lock timeout for the watchdog.
    """
    _write_state(state)
    lifecycle_state.failpoint("mission_publish")
    try:
        _fork_watchdog(lock_timeout_seconds)
    except DeployCtlError:
        _rollback_locked(state)
        raise


def _wait_for_supervisor_mission(
    state: RollbackState, confirm_window_seconds: float
) -> RollbackState:
    """Wait for the supervisor to run the pending-mission candidate and prove it.

    The supervisor owns the transition: it retires the previous worker and
    starts the candidate from its sealed runtime as a direct child, then proves
    queue readiness. deployctl waits for that convergence and refreshes the
    confirmation deadline only once the candidate is genuinely live.

    Args:
        state: Published pending mission.
        confirm_window_seconds: Confirmation window after candidate readiness.

    Returns:
        The live pending mission with a refreshed deadline.

    Raises:
        DeployCtlError: If the supervisor did not apply the mission or prove
            the candidate.
    """
    if not supervise.wait_for_generation(
        state.generation, supervise.DEFAULT_REQUEST_TIMEOUT_SECONDS
    ):
        msg = "the external supervisor did not apply the pending supervised mission"
        raise DeployCtlError(msg)
    if not supervise.wait_until_ready(state.generation, supervise.DEFAULT_REQUEST_TIMEOUT_SECONDS):
        msg = "the external supervisor did not prove the candidate consumes the queue"
        raise DeployCtlError(msg)
    live = replace(state, deadline=time.time() + confirm_window_seconds)
    _write_state(live)
    return live


def _run_git(repo: Path, args: Sequence[str], timeout: float) -> subprocess.CompletedProcess[str]:
    """Run one bounded Git operation.

    Args:
        repo: Repository checkout.
        args: Git arguments.
        timeout: Timeout in seconds.

    Returns:
        Completed Git process.
    """
    return subprocess.run(
        ["git", *args],
        cwd=repo,
        capture_output=True,
        text=True,
        check=False,
        timeout=timeout,
    )


def _require_exact_commit(repo: Path, commit: str, timeout: float) -> None:
    """Require an exact full commit hash present in the repository.

    Args:
        repo: Repository checkout.
        commit: Requested commit.
        timeout: Git timeout.

    Raises:
        DeployCtlError: If the commit is not an exact present commit.
    """
    if COMMIT_RE.fullmatch(commit) is None:
        msg = "checkout requires an exact 40-character lowercase commit hash"
        raise DeployCtlError(msg)
    try:
        proc = _run_git(repo, ("rev-parse", "--verify", f"{commit}^{{commit}}"), timeout)
    except (OSError, subprocess.TimeoutExpired) as exc:
        msg = f"could not verify commit {commit}: {exc}"
        raise DeployCtlError(msg) from exc
    if proc.returncode != 0 or proc.stdout.strip() != commit:
        msg = f"commit {commit} is not present in the repository"
        raise DeployCtlError(msg)


def _require_clean_checkout(repo: Path, timeout: float) -> None:
    """Refuse to overwrite uncommitted deployment-checkout changes.

    Args:
        repo: Repository checkout.
        timeout: Git timeout.

    Raises:
        DeployCtlError: If the worktree is dirty or unreadable.
    """
    try:
        proc = _run_git(repo, ("status", "--porcelain"), timeout)
    except (OSError, subprocess.TimeoutExpired) as exc:
        msg = f"could not inspect deployment checkout: {exc}"
        raise DeployCtlError(msg) from exc
    if proc.returncode != 0:
        raise DeployCtlError("could not inspect deployment checkout")
    if proc.stdout:
        raise DeployCtlError("deployment checkout is dirty; commit or discard changes first")


def _checkout(repo: Path, commit: str, timeout: float, *, force: bool) -> bool:
    """Check out an exact detached commit.

    Args:
        repo: Repository checkout.
        commit: Exact commit hash.
        timeout: Git timeout.
        force: Whether rollback may discard candidate worktree changes.

    Returns:
        ``True`` on success.
    """
    args = ["checkout", "--detach"]
    if force:
        args.append("--force")
    args.append(commit)
    try:
        proc = _run_git(repo, args, timeout)
    except (OSError, subprocess.TimeoutExpired):
        return False
    return proc.returncode == 0


def _wait_for_identity(proc: subprocess.Popen[bytes]) -> ProcessIdentity | None:
    """Wait for a candidate to establish an independent session.

    On timeout the last observed identity is returned even when it does not
    yet satisfy ``pgid == proc.pid == sid``: that observation is the exact
    pre-transition ownership anchor for converging a live candidate whose
    PGID/SID transitions after the deadline. Callers must explicitly reject a
    non-private identity and hand it to
    :func:`lubko.lifecycle._converge_unproven_spawn`.

    Args:
        proc: Candidate process.

    Returns:
        Exact private-session identity once observed, otherwise the last
        observed identity at the timeout, or ``None`` if it died first.
    """
    deadline = time.monotonic() + IDENTITY_TIMEOUT_SECONDS
    last_observed: ProcessIdentity | None = None
    while True:
        if proc.poll() is not None:
            return None
        identity = process_identity(proc.pid)
        if identity is not None and identity.pgid == proc.pid and identity.sid == proc.pid:
            return identity
        if identity is not None:
            # Keep the newest non-private observation: the final poll before
            # the deadline can transiently return None (for example when the
            # /proc entry is momentarily unreadable) without discarding the
            # exact startup anchor.
            last_observed = identity
        if time.monotonic() >= deadline:
            return last_observed
        time.sleep(IDENTITY_POLL_SECONDS)


def _spawn_gated_candidate(options: Options, commit: str) -> GatedWorker:
    """Spawn a non-consuming candidate behind a stable pipe gate.

    Args:
        options: Deployment options.
        commit: Candidate commit.

    Returns:
        Gated process and exact metadata.

    Raises:
        DeployCtlError: If the candidate cannot establish its identity.
    """
    token = secrets.token_hex(16)
    env = worker_env(token)
    worker_id = env.get("LUBKO_WORKER_ID") or socket.gethostname()
    reader, writer = os.pipe()
    try:
        proc = subprocess.Popen(
            [sys.executable, "-c", GATED_SHIM_SOURCE, str(reader), options.uv_path],
            cwd=options.repo,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
            close_fds=True,
            pass_fds=(reader,),
            env=env,
        )
    finally:
        os.close(reader)
    identity = _wait_for_identity(proc)
    if identity is None or identity.pgid != proc.pid or identity.sid != proc.pid:
        os.close(writer)
        raise DeployCtlError("candidate exited before the rollback mission could be armed")
    meta = WorkerMeta(
        schema_version=SCHEMA_VERSION,
        state=STATE_RUNNING,
        pid=identity.pid,
        pgid=identity.pgid,
        sid=identity.sid,
        start_time_ticks=identity.start_time_ticks,
        token=token,
        repo=str(options.repo),
        git_commit=commit,
        worker_id=worker_id,
        log_path=str(worker_log_path(token)),
        started_at=time.time(),
        stopped_at=None,
    )
    return GatedWorker(proc=proc, gate_writer=writer, meta=meta)


def _close_gate(gate_writer: int) -> None:
    """Close a candidate gate without releasing it.

    Args:
        gate_writer: Write end of the stable startup gate.
    """
    with suppress(OSError):
        os.close(gate_writer)


_GATED_ABORT_GRACE_SECONDS: Final = 2.0


def _abort_gated_candidate(gated: GatedWorker) -> None:
    """Close the gate and synchronously reap the exact gated candidate.

    The candidate is blocked reading from the gate pipe; closing it delivers
    EOF so the shim exits.  A bounded wait follows so the child is reaped
    before the caller proceeds (restoring checkout, rolling back state).
    If the candidate fails to exit within the grace period the exact gated
    metadata identity is escalated through :func:`stop_worker` which
    revalidates PID / start-time-ticks / PGID / SID / token at every signal
    step, so a recycled or reused PID is never signalled.  If the exact
    retirement cannot be proven the caller is failed closed.

    Args:
        gated: The gated candidate to abort.

    Raises:
        DeployCtlError: If the candidate cannot be reaped within the
            escalation bound.
    """
    _close_gate(gated.gate_writer)
    proc = gated.proc
    if proc.poll() is not None:
        return
    try:
        proc.wait(timeout=_GATED_ABORT_GRACE_SECONDS)
        return
    except subprocess.TimeoutExpired:
        pass
    if not stop_worker(gated.meta, _GATED_ABORT_GRACE_SECONDS):
        msg = (
            f"gated candidate pid {gated.meta.pid} could not be reaped after "
            "exact-identity escalation"
        )
        raise DeployCtlError(msg)
    try:
        proc.wait(timeout=_GATED_ABORT_GRACE_SECONDS)
    except subprocess.TimeoutExpired:
        msg = (
            f"gated candidate pid {gated.meta.pid} survived exact-identity "
            "escalation; reaping timed out"
        )
        raise DeployCtlError(msg) from None


def _release_gate(gate_writer: int) -> None:
    """Release a candidate to exec the worker.

    Args:
        gate_writer: Write end of the stable startup gate.

    Raises:
        DeployCtlError: If the release cannot be delivered.
    """
    try:
        os.write(gate_writer, b"G")
    except OSError as exc:
        msg = f"could not release candidate worker: {exc}"
        raise DeployCtlError(msg) from exc
    finally:
        _close_gate(gate_writer)


def _wait_for_released_worker(meta: WorkerMeta) -> bool:
    """Require the released candidate to survive a short stabilization period.

    The actual queue-health proof is the two confirmation requests themselves:
    only the replacement worker is left consuming the queue, so inability to
    consume prevents confirmation and the watchdog rolls back.

    Args:
        meta: Candidate metadata.

    Returns:
        ``True`` when the candidate remains alive.
    """
    deadline = time.monotonic() + POST_RELEASE_STABILITY_SECONDS
    while time.monotonic() < deadline:
        if not worker_alive(meta):
            return False
        time.sleep(IDENTITY_POLL_SECONDS)
    return worker_alive(meta)


def _candidate_response(state: RollbackState) -> dict[str, object]:
    """Build the successful provisional checkout response.

    Args:
        state: Pending deployment state.

    Returns:
        JSON response object.
    """
    return {
        "type": "checkout",
        "ok": True,
        "phase": "pending",
        "commit": state.commit,
        "worker_pid": None if (state.new_meta.pid or 0) <= 0 else state.new_meta.pid,
        "deadline": state.deadline,
    }


def _current_queue_job_id() -> tuple[object | None, bool]:
    """Identify the current queue job from the exact injected root job UUID.

    The owning worker injects the exact root job UUID as ``LUBKO_JOB_ID`` into
    every spawned command environment before the child execs, so queue
    detection never depends on the timing of ``process_pgid`` persistence: the
    command is a queue job if and only if that exact injected value is present.
    The matching row is then validated as needed: a missing row is a deleted
    job and a cancellation marker is reported, so a queue-invoked checkout can
    never silently fall back to the manual destructive path.

    Returns:
        ``(job_id, cancelled)`` when invoked from a queue job, otherwise
        ``(None, False)``.

    Raises:
        DeployCtlError: If the injected job cannot be validated.
    """
    job_text = os.environ.get(JOB_ID_ENV)
    if job_text is None:
        return None, False
    try:
        job_id = UUID(job_text)
    except ValueError:
        msg = f"malformed {JOB_ID_ENV} in the command environment"
        raise DeployCtlError(msg) from None
    try:
        database = load_database_config()
    except (OSError, ValueError) as exc:
        msg = f"cannot validate the injected queue job: {exc}"
        raise DeployCtlError(msg) from exc
    try:
        with psycopg.connect(database.conninfo(), row_factory=tuple_row) as conn:
            with conn.transaction(), conn.cursor() as cursor:
                cursor.execute(
                    "SELECT (payload::jsonb)->'state'->>'status', "
                    "(payload::jsonb)->'state'->>'cancel_requested_at' "
                    "FROM lubko.jobs WHERE id = %s",
                    (job_id,),
                )
                row = cursor.fetchone()
    except psycopg.Error as exc:
        msg = f"could not validate the injected queue job: {exc.__class__.__name__}"
        raise DeployCtlError(msg) from exc
    if row is None:
        raise DeployCtlError("the current queue job row does not exist")
    status = str(row[0])
    if status == "cancelled":
        return job_id, True
    if status != "running":
        msg = f"the current queue job is {status} rather than running"
        raise DeployCtlError(msg)
    return job_id, row[1] is not None


def _send_helper_response(writer: int, response: dict[str, object]) -> None:
    """Send one JSON response line to the controller parent over a pipe.

    Args:
        writer: Write end of the pipe back to the controller parent.
        response: Response object to deliver.
    """
    payload = json.dumps(response, sort_keys=True) + "\n"
    with suppress(OSError):
        os.write(writer, payload.encode())


def _send_helper_error(writer: int, message: str) -> None:
    """Send one protocol-style error response to the controller parent.

    The message is bounded so a pipe delivery can never block or overflow the
    controller parent's bounded response read.

    Args:
        writer: Write end of the pipe back to the controller parent.
        message: Error description.
    """
    bounded = message[:HELPER_ERROR_MAX_CHARS]
    if len(message) > HELPER_ERROR_MAX_CHARS:
        bounded += "..."
    _send_helper_response(writer, {"ok": False, "error": bounded})


def _read_pipe_line(reader: int) -> str:
    """Read one newline-terminated line from a pipe, bounded and deterministic.

    Returns:
        The decoded line without its trailing newline, or ``""`` at EOF.

    Raises:
        DeployCtlError: If the line exceeds the bounded response size.
    """
    chunks: list[bytes] = []
    total = 0
    while total < HANDOFF_RESPONSE_MAX_BYTES:
        chunk = os.read(reader, 65536)
        if not chunk:
            break
        chunks.append(chunk)
        total += len(chunk)
        if chunk.endswith(b"\n"):
            break
    if total >= HANDOFF_RESPONSE_MAX_BYTES:
        raise DeployCtlError("deployment handoff helper response exceeded the bounded size")
    return b"".join(chunks).decode("utf-8", errors="replace").strip()


def _wait_for_durable_success(job_id: object, deadline: float) -> None:
    """Wait until the checkout queue job is durably succeeded and not cancelled.

    The controller parent exits zero so the owning worker finalizes the row.
    The helper may cross the destructive handoff boundary only once the row is
    durably terminal ``succeeded`` in PostgreSQL with no cancellation marker;
    a ``failed``/``cancelled``/deleted row or an expired deadline aborts the
    mission before any destructive step. The row is only ever read, never
    rewritten, so a transient terminal state can never be overwritten.

    Args:
        job_id: Identifier of the checkout queue row.
        deadline: Monotonic-handoff deadline for durable success.

    Raises:
        DeployCtlError: If the row cannot be trusted as durably succeeded.
    """
    try:
        database = load_database_config()
    except (OSError, ValueError) as exc:
        msg = f"handoff helper cannot load database configuration: {exc}"
        raise DeployCtlError(msg) from exc
    try:
        conn = psycopg.connect(database.conninfo(), row_factory=tuple_row)
    except psycopg.Error as exc:
        msg = f"handoff helper cannot reach PostgreSQL: {exc.__class__.__name__}"
        raise DeployCtlError(msg) from exc
    try:
        while time.time() < deadline:
            try:
                with conn.transaction(), conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT (payload::jsonb)->'state'->>'status', "
                        "(payload::jsonb)->'state'->>'cancel_requested_at' "
                        "FROM lubko.jobs "
                        "WHERE id = %s",
                        (job_id,),
                    )
                    row = cursor.fetchone()
            except psycopg.Error:
                time.sleep(HANDOFF_POLL_SECONDS)
                continue
            if row is None:
                raise DeployCtlError("checkout queue job was deleted before durable success")
            status = str(row[0])
            if status == "succeeded" and row[1] is None:
                return
            if status in {"failed", "cancelled"}:
                msg = f"checkout queue job reached {status} before durable success"
                raise DeployCtlError(msg)
            time.sleep(HANDOFF_POLL_SECONDS)
        raise DeployCtlError("checkout queue job did not reach durable success before the deadline")
    finally:
        with suppress(Exception):
            conn.close()


def _abort_mission(gated: GatedWorker | None, state: RollbackState) -> None:
    """Undo a prepared mission without crossing the destructive boundary.

    The initiating checkout row failed, was cancelled, was deleted, or never
    reached durable success. In the legacy path the gated candidate is closed
    (it exits on EOF); in supervised mode no candidate was ever spawned by
    deployctl, so only the reversible preparation is undone. The previous
    exact checkout is restored and the previous worker is left running.
    ``previous_retiring`` is never set, so rollback always reuses or restores
    the previous worker without treating retirement as begun.

    Args:
        gated: The gated candidate process, or ``None`` in supervised mode.
        state: The pending rollback mission.
    """
    if gated is not None:
        _abort_gated_candidate(gated)
    _checkout(
        Path(state.repo),
        state.previous_commit,
        state.git_timeout_seconds,
        force=True,
    )
    cli.remove_cli_root(state.commit)


def _complete_handoff(
    options: Options,
    state: RollbackState,
    gated: GatedWorker | None,
) -> RollbackState:
    """Cross the destructive handoff for a prepared pending mission.

    With a live external supervisor the handoff is purely durable: the pending
    mission is published so the daemon (the single process-lifecycle
    authority) retires the previous worker and starts the candidate from its
    sealed runtime as a direct child, and deployctl waits for the daemon to
    prove candidate readiness. Without a supervisor (one-time bootstrap /
    emergency path only) the legacy gate release runs.

    Args:
        options: Deployment options.
        state: Prepared pending mission.
        gated: The gated candidate (legacy path), or ``None`` when supervised.

    Returns:
        The live pending rollback state.

    Raises:
        DeployCtlError: If the handoff cannot complete; rollback is attempted.
    """
    if supervise.supervisor_running():
        publish_mission(state, options.lock_timeout_seconds)
        return _wait_for_supervisor_mission(state, options.confirm_window_seconds)
    if gated is None:  # pragma: no cover - impossible without a supervisor
        msg = "cannot hand off a supervisor-owned mission without a supervisor"
        raise DeployCtlError(msg)
    retiring = replace(state, previous_retiring=True)
    _write_state(retiring)
    try:
        if not stop_worker(state.previous_meta, options.stop_grace_seconds):
            raise DeployCtlError("could not stop the known-good worker")
        _release_gate(gated.gate_writer)
        if not _wait_for_released_worker(gated.meta):
            raise DeployCtlError("candidate worker exited immediately after release")
        live = replace(retiring, deadline=time.time() + options.confirm_window_seconds)
        _write_state(live)
        return live
    except DeployCtlError:
        _abort_gated_candidate(gated)
        _rollback_locked(retiring)
        raise


def _restart_previous(state: RollbackState) -> WorkerMeta | None:
    """Restore the previous known-good worker process.

    A previous worker that was never told to retire and is still alive is
    reused under its exact recorded identity (old watchdog behavior). Once the
    controller has durably marked ``previous_retiring`` before stopping that
    worker, a momentarily alive process is never trusted: retirement may have
    begun, so the exact old identity is deterministically stopped and awaited
    dead before a fresh previous-commit worker is spawned and verified. That
    guarantees terminal ``rolled_back`` never means a worker that is about to
    exit, so zero queue consumers cannot be the outcome of a completed
    rollback.

    Args:
        state: Rollback mission.

    Returns:
        Restored worker metadata, or ``None`` on failure.
    """
    previous = state.previous_meta
    if not state.previous_retiring and worker_alive(previous):
        return previous
    if worker_alive(previous) and not stop_worker(previous, state.stop_grace_seconds):
        return None
    token = secrets.token_hex(16)
    env = worker_env(token)
    worker_id = env.get("LUBKO_WORKER_ID") or previous.worker_id or socket.gethostname()
    try:
        proc = spawn_worker(
            Path(state.repo),
            state.uv_path,
            worker_log_path(token),
            env,
        )
    except OSError:
        return None
    identity = _wait_for_identity(proc)
    if identity is None or identity.pgid != proc.pid or identity.sid != proc.pid:
        if identity is not None:
            LOGGER.error(
                "worker pid %d is live without an acceptable identity; converging it",
                proc.pid,
            )
        _converge_unproven_spawn(proc, state.stop_grace_seconds, identity)
        return None
    meta = WorkerMeta(
        schema_version=SCHEMA_VERSION,
        state=STATE_RUNNING,
        pid=identity.pid,
        pgid=identity.pgid,
        sid=identity.sid,
        start_time_ticks=identity.start_time_ticks,
        token=token,
        repo=state.repo,
        git_commit=state.previous_commit,
        worker_id=worker_id,
        log_path=str(worker_log_path(token)),
        started_at=time.time(),
        stopped_at=None,
    )
    if not worker_alive(meta) or not check_postgres(DEFAULT_POSTGRES_TIMEOUT_SECONDS):
        stop_worker(meta, state.stop_grace_seconds)
        return None
    return meta


def _retire_candidate_locked(state: RollbackState) -> bool:
    """Stop the pending-mission candidate and prove its exact death.

    Rollback must never mutate the checkout, restart the previous worker, or
    record terminal ``rolled_back`` state while the candidate worker might
    still be alive: ``stop_worker`` reporting success is never trusted alone.
    The exact candidate identity is independently rechecked after the stop, so
    a stop that fails or a misleading success keeps the rollback nonterminal.

    Args:
        state: Pending rollback mission.

    Returns:
        ``True`` only when the candidate worker is proven dead.
    """
    if not stop_worker(state.new_meta, state.stop_grace_seconds):
        append_deploy_log("supervised rollback could not stop the candidate worker")
        return False
    if worker_alive(state.new_meta):
        append_deploy_log("supervised rollback requires the candidate worker to be proven dead")
        return False
    return True


def _restore_previous_locked(state: RollbackState) -> bool:
    """Restore the previous exact checkout and worker after candidate death.

    Assumes the candidate worker has been proven dead, so the previous
    known-good checkout may be force-restored, the previous worker restarted,
    its metadata written, and the terminal ``rolled_back`` state recorded.

    Args:
        state: Pending rollback mission.

    Returns:
        ``True`` only when checkout, worker, metadata, and state are restored.
    """
    repo = Path(state.repo)
    if not _checkout(repo, state.previous_commit, state.git_timeout_seconds, force=True):
        append_deploy_log("supervised rollback could not restore previous checkout")
        return False
    restored = _restart_previous(state)
    if restored is None:
        append_deploy_log("supervised rollback could not restart previous worker")
        return False
    write_meta(restored)
    _write_state(replace(state, status=STATUS_ROLLED_BACK, challenge_hash=None))
    cli.remove_cli_root(state.commit)
    if cli.reconcile_pointer(state.previous_commit):
        append_deploy_log(f"supervised rollback restored commit {state.previous_commit}")
    else:
        append_deploy_log(
            f"supervised rollback restored commit {state.previous_commit} "
            "but could not restore the maintained CLI pointer"
        )
    return True


def _clear_settlement(state: RollbackState, status: str) -> RollbackState:
    """Return terminal mission history with no active settlement authority."""
    return replace(
        state,
        status=status,
        settlement_transition=None,
        settlement_generation=None,
        settlement_commit=None,
    )


def _finalize_supervised_rollback(state: RollbackState) -> RollbackState:
    """Durably archive an exactly applied rollback settlement.

    Returns:
        The terminal rolled-back mission state.
    """
    terminal = _clear_settlement(replace(state, challenge_hash=None), STATUS_ROLLED_BACK)
    _write_state(terminal)
    cli.remove_cli_root(state.commit)
    if cli.reconcile_pointer(state.previous_commit):
        append_deploy_log(f"supervised rollback restored commit {state.previous_commit}")
    else:
        append_deploy_log(
            f"supervised rollback restored commit {state.previous_commit} "
            "but could not restore the maintained CLI pointer"
        )
    return terminal


def _finalize_supervised_confirmation(state: RollbackState) -> RollbackState:
    """Durably archive an exactly applied confirmation settlement.

    Returns:
        The terminal confirmed mission state.
    """
    terminal = _clear_settlement(state, STATUS_CONFIRMED)
    _write_state(terminal)
    try:
        cli.set_current(state.commit)
    except cli.CliError as exc:
        append_deploy_log(f"supervised deployment confirmed but CLI activation failed: {exc}")
    cli.gc_cli_roots((state.commit, state.previous_commit))
    append_deploy_log(f"supervised deployment confirmed commit {state.commit}")
    return terminal


def _recover_applied_settlement(state: RollbackState) -> RollbackState:
    """Finish an exact queue-ready settlement left pending by a prior crash.

    Returns:
        A terminal mission when exact settlement authority is already applied,
        otherwise the input pending mission unchanged.
    """
    if state.status != STATUS_PENDING or not supervise.supervisor_running():
        return state
    settlement = _settlement_spec(state)
    if settlement is None or not _settlement_exactly_applied(state):
        return state
    transition, _, _ = settlement
    if transition == SETTLEMENT_CONFIRM:
        return _finalize_supervised_confirmation(state)
    return _finalize_supervised_rollback(state)


def _rollback_locked(state: RollbackState) -> bool:
    """Restore the exact previous known-good commit and worker.

    With a live external supervisor the rollback is a durable settlement: a
    newer desired generation selects the previous exact commit and the daemon
    starts a fresh previous-commit worker from its sealed runtime; deployctl
    then records the terminal ``rolled_back`` history. Without a supervisor
    (one-time bootstrap / emergency path only) the legacy direct restore runs,
    and it fails closed: the candidate worker must be genuinely dead before any
    checkout mutation, previous-worker restart, metadata write, or terminal
    ``rolled_back`` state.

    Args:
        state: Pending rollback mission.

    Returns:
        ``True`` only when checkout, worker, metadata, and state are restored and
        the candidate worker is proven dead.
    """
    if state.status == STATUS_PENDING:
        state = _recover_applied_settlement(state)
    if state.status != STATUS_PENDING:
        return True
    lifecycle_state.failpoint("mission_rollback")
    # Exact-authority gate: the pending->rolled_back transition may only run when
    # the durable authority is sound (an unreadable/corrupt mission refuses by
    # failing closed); the caller still proves the candidate dead before mutating
    # state, so this is purely the authority decision boundary.
    if not lifecycle_state.authorize_mission_rollback(_mission_authority_facts(state.status)):
        append_deploy_log("lifecycle authority refuses rollback of the pending mission; holding")
        return False
    if supervise.supervisor_running():
        try:
            settle_desired(state.previous_commit, state.repo, state.uv_path)
        except DeployCtlError:
            append_deploy_log("supervised rollback could not settle the previous commit")
            return False
        _finalize_supervised_rollback(state)
        return True
    if not _retire_candidate_locked(state):
        return False
    return _restore_previous_locked(state)


def _watchdog_main(lock_timeout_seconds: float) -> None:
    """Retain rollback authority until the mission reaches a terminal state.

    With a live external supervisor the watchdog only rolls back after the
    confirmation deadline has passed without the daemon keeping the candidate
    consumer live, so it never fights the supervisor's own bounded restart of a
    transiently crashed candidate.

    Args:
        lock_timeout_seconds: Deployment-lock timeout for rollback attempts.
    """
    while True:
        try:
            state = _read_state()
        except DeployCtlError:
            time.sleep(WATCHDOG_POLL_SECONDS)
            continue
        if state is None or state.status in {STATUS_CONFIRMED, STATUS_ROLLED_BACK}:
            return
        if supervise.supervisor_running():
            # While the supervisor is present, candidate liveness is a separate
            # observation: only roll back after the deadline AND the exact child
            # is no longer proven live.  A stale state.json child must not count
            # as live.
            should_rollback = time.time() >= state.deadline and not _supervised_mission_active(
                state
            )
        elif state.supervisor_owned is not False:
            # Pending supervised missions and missions with unknown lifecycle
            # authority must never enter the legacy direct rollback/worker
            # lifecycle.  If the supervisor is temporarily absent, fail closed
            # and leave the durable mission and desired generation untouched for
            # the next supervisor incarnation.  ``None`` (unknown authority)
            # fails closed identically to ``True``.
            should_rollback = False
        else:
            should_rollback = time.time() >= state.deadline or not worker_alive(state.new_meta)
        if should_rollback:
            try:
                with deploy_lock(lock_timeout_seconds):
                    current = _read_state()
                    if current is not None and current.status == STATUS_PENDING:
                        if _rollback_locked(current):
                            return
            except (DeployCtlError, LockTimeoutError):
                pass
        time.sleep(WATCHDOG_POLL_SECONDS)


def _fork_watchdog(lock_timeout_seconds: float) -> None:
    """Fork the already-loaded stable wrapper before destructive handoff.

    The child closes its copy of the gate writer immediately. Therefore, if the
    parent dies before release, EOF reaches the gated candidate and the
    candidate exits rather than becoming an orphan queue consumer.

    Args:
        lock_timeout_seconds: Lock timeout used by the watchdog.

    Raises:
        DeployCtlError: If the stable rollback authority cannot be forked.
    """
    try:
        pid = os.fork()
    except OSError as exc:
        msg = f"could not arm rollback watchdog: {exc}"
        raise DeployCtlError(msg) from exc
    if pid != 0:
        return
    # The fork happens while the parent holds deploy_lock(). Close every
    # inherited non-stdio descriptor before entering the watchdog loop so a
    # parent crash cannot leave the child owning the inherited flock or gate.
    os.closerange(3, int(os.sysconf("SC_OPEN_MAX")))
    with suppress(OSError):
        os.setsid()
    # The watchdog outlives the job process that forked it: sever the
    # inherited standard streams (capture-pipe write ends under worker-owned
    # capture) exactly like every other detached deployment child.
    detach_standard_streams(keep=set())
    try:
        _watchdog_main(lock_timeout_seconds)
    finally:
        os._exit(0)


def archive_mission(state: RollbackState, status: str) -> None:
    """Archivially record a mission as terminal history.

    This is the public writer used by lifecycle-state migration: a stale
    pending mission that has already been superseded by a newer desired
    generation is archived terminal (``confirmed`` or ``rolled_back``) so it
    becomes inert history that can never influence the worker.

    Args:
        state: The mission to archive.
        status: Terminal status (``confirmed`` or ``rolled_back``).

    Raises:
        DeployCtlError: If ``status`` is not a terminal mission status.
    """
    if status not in {STATUS_CONFIRMED, STATUS_ROLLED_BACK}:
        msg = f"cannot archive a mission as {status!r}"
        raise DeployCtlError(msg)
    _write_state(replace(state, status=status, challenge_hash=None))


def read_rollback_state() -> RollbackState | None:
    """Read the durable supervised-deployment state for external observers.

    This is the public read used by the external supervisor.  A missing state
    file is reported as ``None`` (no mission).  Corrupt or contradictory state
    raises :class:`DeployCtlError` so the supervisor can fail closed into a
    hold: it must never treat untrustworthy mission metadata as "no mission"
    and run a worker during an unknown handoff.

    Returns:
        The parsed state, or ``None`` when no state file exists.

    Raises:
        DeployCtlError: If an existing state file is malformed, unsupported, or
            unreadable.
    """
    path = rollback_state_path()
    try:
        raw = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return None
    except OSError as exc:
        msg = f"cannot read supervised deployment state: {exc}"
        raise DeployCtlError(msg) from exc
    try:
        decoded = json.loads(raw)
    except ValueError as exc:
        msg = "supervised deployment state is not valid JSON"
        raise DeployCtlError(msg) from exc
    if not isinstance(decoded, dict):
        msg = "supervised deployment state must be an object"
        raise DeployCtlError(msg)
    try:
        state = RollbackState.from_dict(decoded)
    except (KeyError, TypeError, ValueError) as exc:
        msg = "supervised deployment state is malformed"
        raise DeployCtlError(msg) from exc
    return _normalize_parsed_state(state)


def _cleanup_pending_locked() -> None:
    """Resolve an abandoned pending mission before accepting another checkout.

    Raises:
        DeployCtlError: If a live mission is still pending or rollback cannot
            complete.
    """
    state = _read_state()
    if state is None or state.status in {STATUS_CONFIRMED, STATUS_ROLLED_BACK}:
        return
    if state.status != STATUS_PENDING:
        raise DeployCtlError(f"unknown supervised deployment status {state.status!r}")
    if _mission_candidate_alive(state) and time.time() < state.deadline:
        raise DeployCtlError("another supervised checkout is still pending confirmation")
    if not _rollback_locked(state):
        raise DeployCtlError("an unresolved rollback is still pending")


def _mission_authority_facts(
    status: str | None, *, candidate_ready: bool = False
) -> lifecycle_state.AuthorityFacts:
    """Reconcile the mission-authority facts for a transition gate.

    The mission transitions consult the authority model with the exact mission
    status already loaded by the runtime (the genuine source of the handoff),
    plus an independent malformed-authority probe of the durable rollback state.
    Unrelated durable sources (owned worker meta, desired intent) are not part of
    the mission authority and stay at their safe defaults.

    Args:
        status: The current mission status the runtime is acting on, or ``None``
            when no mission is durable.
        candidate_ready: Whether the candidate has already proven queue readiness
            (the exact readiness proof obtained by the calling branch).

    Returns:
        The mission-authority snapshot for :mod:`lubko.lifecycle_state` gates.
    """
    malformed = False
    try:
        read_rollback_state()
    except DeployCtlError:
        malformed = True
    return lifecycle_state.AuthorityFacts(
        desired_generation=0,
        applied_generation=0,
        mission_status=status,
        mission_generation=None,
        mission_commit=None,
        owned_worker_pid=None,
        owned_worker_commit=None,
        owned_worker_identity_proven=False,
        pre_spawn_obligation=False,
        unresolved_child=False,
        candidate_ready=candidate_ready,
        rollback_pending=status == STATUS_PENDING,
        durable_malformed=malformed,
        supervisor_child_present=False,
        current_child_identity_proven=False,
        ownership_hold_malformed=False,
        unresolved_hold_malformed=False,
        spawning_hold_malformed=False,
    )


def _prepare_locked(
    options: Options,
    commit: str,
    *,
    supervised: bool,
) -> tuple[RollbackState, GatedWorker | None]:
    """Prepare an exact candidate without crossing the destructive boundary.

    Performs only reversible preparation while holding the deploy lock: resolve
    any abandoned mission, validate the exact clean commits, check out the
    candidate, validate it, build its sealed provisional runtime, verify
    PostgreSQL, and (in the legacy no-supervisor bootstrap path) spawn the
    gated candidate and arm the watchdog. When the external supervisor is live
    the mission is only *prepared* here and returned unwritten: publication
    happens at handoff time so the pending mission can never retire the old
    worker before the initiating checkout row is durably succeeded. The
    previous worker is never stopped here.

    Args:
        options: Deployment options.
        commit: Exact candidate commit.
        supervised: Whether the external supervisor owns worker processes.

    Returns:
        The pending rollback state and (legacy path only) its gated candidate.

    Raises:
        DeployCtlError: On any unsafe or failed preparation; the previous
            checkout is restored and the previous worker is left untouched.
    """
    _cleanup_pending_locked()
    previous = read_meta()
    if previous is None or not worker_alive(previous) or previous.git_commit is None:
        raise DeployCtlError("a live maintained known-good worker is required for safe checkout")
    previous_commit = previous.git_commit
    _require_exact_commit(options.repo, previous_commit, options.git_timeout_seconds)
    _require_exact_commit(options.repo, commit, options.git_timeout_seconds)
    if commit == previous_commit:
        raise DeployCtlError("candidate commit is already the maintained worker commit")
    _require_clean_checkout(options.repo, options.git_timeout_seconds)
    if not _checkout(options.repo, commit, options.git_timeout_seconds, force=False):
        raise DeployCtlError(f"could not check out candidate commit {commit}")
    report = run_validation(options.repo, options.uv_path, options.validation_timeout_seconds)
    if not report.ok:
        _restore_previous_prep(options, previous_commit, commit)
        raise DeployCtlError(f"candidate validation failed: {report.detail}")
    try:
        cli.build_cli_root(options.repo, commit, options.uv_path, options.cli_timeout_seconds)
    except cli.CliError as exc:
        _restore_previous_prep(options, previous_commit, commit)
        msg = f"candidate CLI environment could not be built: {exc}"
        raise DeployCtlError(msg) from exc
    gated, new_meta = _candidate_identity(options, commit, supervised=supervised)
    if not check_postgres(options.postgres_timeout_seconds):
        if gated is not None:
            _abort_gated_candidate(gated)
        _restore_previous_prep(options, previous_commit, commit)
        raise DeployCtlError("stable wrapper cannot reach PostgreSQL before handoff")
    # Exact-authority gate: a NEW pending mission may only be created when no
    # mission is already pending and the durable authority is not malformed. An
    # abandoned pending mission was already resolved by _cleanup_pending_locked,
    # so a refusal here is a genuine conflict or fail-closed authority.
    existing = _read_state()
    if not lifecycle_state.authorize_mission_publish(
        _mission_authority_facts(existing.status if existing is not None else None)
    ):
        _restore_previous_prep(options, previous_commit, commit)
        raise DeployCtlError(
            "lifecycle authority refuses a new pending mission: a supervised checkout "
            "is already pending or durable authority is malformed"
        )
    state = RollbackState(
        schema_version=ROLLBACK_SCHEMA_VERSION,
        generation=next_mission_generation(),
        status=STATUS_PENDING,
        commit=commit,
        previous_commit=previous_commit,
        challenge_hash=None,
        deadline=time.time() + options.confirm_window_seconds,
        repo=str(options.repo),
        uv_path=options.uv_path,
        stop_grace_seconds=options.stop_grace_seconds,
        git_timeout_seconds=options.git_timeout_seconds,
        previous_retiring=False,
        previous_meta=previous,
        new_meta=new_meta,
        supervisor_owned=supervised,
    )
    if not supervised:
        _publish_legacy_mission(state, gated, options.lock_timeout_seconds)
    return state, gated


def _candidate_identity(
    options: Options,
    commit: str,
    *,
    supervised: bool,
) -> tuple[GatedWorker | None, WorkerMeta]:
    """Produce the candidate identity record for a prepared mission.

    With a live external supervisor the candidate is owned by the daemon, so a
    never-alive placeholder is recorded; otherwise deployctl spawns the gated
    candidate (one-time bootstrap / emergency path).

    Args:
        options: Deployment options.
        commit: Exact candidate commit.
        supervised: Whether the external supervisor owns worker processes.

    Returns:
        The ``(gated, new_meta)`` pair.
    """
    if supervised:
        return None, _placeholder_meta(commit, str(options.repo))
    gated = _spawn_gated_candidate(options, commit)
    return gated, gated.meta


def _publish_legacy_mission(
    state: RollbackState,
    gated: GatedWorker | None,
    lock_timeout_seconds: float,
) -> None:
    """Persist a legacy pending mission and arm its watchdog.

    Args:
        state: Legacy pending mission.
        gated: The gated candidate, or ``None``.
        lock_timeout_seconds: Deployment-lock timeout for the watchdog.

    Raises:
        DeployCtlError: If the watchdog cannot be armed; rollback is attempted.
    """
    _write_state(state)
    try:
        _fork_watchdog(lock_timeout_seconds)
    except DeployCtlError:
        if gated is not None:
            _abort_gated_candidate(gated)
        _rollback_locked(state)
        raise


def _restore_previous_prep(options: Options, previous_commit: str, candidate_commit: str) -> None:
    """Restore the reversible preparation state before a checkout failure.

    The candidate checkout is force-restored to the exact previous commit and
    the provisional candidate CLI environment is removed, leaving the previous
    worker untouched.

    Args:
        options: Deployment options.
        previous_commit: Exact previously maintained commit.
        candidate_commit: Exact candidate commit whose environment to remove.
    """
    _checkout(options.repo, previous_commit, options.git_timeout_seconds, force=True)
    cli.remove_cli_root(candidate_commit)


def _deploy_locked(options: Options, commit: str) -> RollbackState:
    """Prepare and hand off one exact candidate while holding the deploy lock.

    The synchronous path used by manual (non-queue) invocations: preparation is
    immediately followed by the handoff. With a live external supervisor the
    handoff publishes the pending mission and waits for the daemon to own the
    candidate transition; otherwise the legacy gated handoff runs (one-time
    bootstrap/emergency path only).

    Args:
        options: Deployment options.
        commit: Exact candidate commit.

    Returns:
        Live pending rollback state.
    """
    supervised = supervise.supervisor_running()
    state, gated = _prepare_locked(options, commit, supervised=supervised)
    return _complete_handoff(options, state, gated)


def _run_helper(options: Options, commit: str, job_id: object, writer: int) -> None:
    """Run the detached queue-handoff helper to completion in the child.

    The child detaches into its own session immediately so the retiring
    worker's group shutdown can never reach it, acquires the deployment lock
    itself, performs the reversible preparation, delivers the candidate or
    error response to the parent, waits for the initiating row to be durably
    succeeded, and only then crosses the destructive boundary. This function
    never returns: it exits the child process.

    Args:
        options: Deployment options.
        commit: Exact candidate commit.
        job_id: Captured checkout queue row identifier.
        writer: Write end of the response pipe to the parent.
    """
    with suppress(OSError):
        os.setsid()
    detach_standard_streams(keep={writer})
    try:
        try:
            with deploy_lock(options.lock_timeout_seconds):
                _helper_locked(options, commit, job_id, writer)
        except LockTimeoutError as exc:
            _send_helper_error(writer, f"timed out waiting for the deployment lock: {exc}")
        except DeployCtlError as exc:
            _send_helper_error(writer, str(exc))
        except OSError as exc:
            _send_helper_error(writer, f"operating-system error: {exc}")
    finally:
        with suppress(OSError):
            os.close(writer)
    os._exit(0)


def _helper_locked(options: Options, commit: str, job_id: object, writer: int) -> None:
    """Run one lock-held queue handoff mission in the detached helper.

    The candidate or error response is delivered to the parent before any
    destructive step. The parent exits zero only for a genuine candidate
    response so the owning worker finalizes the checkout row as durably
    ``succeeded``; a helper error or helper death makes it exit non-zero so the
    row is durably ``failed``. The helper then waits for that exact row to be
    durably succeeded before crossing the handoff. With a live external
    supervisor the handoff publishes the pending mission so the daemon owns the
    worker transition; otherwise the legacy gated handoff runs (one-time
    bootstrap/emergency path only). The durable-success wait deadline is
    computed only after preparation returns (it is not the confirmation
    deadline captured during preparation), so a long validation phase can never
    expire the handoff wait before it starts. Any failure before durable
    success aborts the mission with the previous worker left running.

    Args:
        options: Deployment options.
        commit: Exact candidate commit.
        job_id: Captured checkout queue row identifier.
        writer: Write end of the response pipe to the parent.
    """
    supervised = supervise.supervisor_running()
    state, gated = _prepare_locked(options, commit, supervised=supervised)
    durable_deadline = time.time() + HANDOFF_DURABLE_WAIT_SECONDS
    _send_helper_response(writer, _candidate_response(state))
    try:
        _wait_for_durable_success(job_id, durable_deadline)
    except DeployCtlError:
        _abort_mission(gated, state)
        append_deploy_log("queue checkout aborted before the destructive handoff")
        return
    try:
        _complete_handoff(options, state, gated)
    except DeployCtlError as exc:
        append_deploy_log(f"queue handoff failed after durable success: {exc}")


def _queue_checkout(options: Options, commit: str, job_id: object) -> dict[str, object]:
    """Handle a queue-invoked checkout through a detached helper process.

    The controller forks a helper into a separate session; the helper performs
    all reversible preparation and the destructive handoff, while this parent
    delivers the response and exits zero so the owning worker finalizes the
    checkout row as durably succeeded. The parent never waits for the helper
    and never touches the terminal row itself.

    Args:
        options: Deployment options.
        commit: Exact candidate commit.
        job_id: Captured checkout queue row identifier.

    Returns:
        The protocol response delivered by the helper.

    Raises:
        DeployCtlError: If the helper cannot be forked or never reports.
    """
    reader, writer = os.pipe()
    try:
        try:
            pid = os.fork()
        except OSError as exc:
            msg = f"could not fork the deployment handoff helper: {exc}"
            raise DeployCtlError(msg) from exc
        if pid == 0:
            os.close(reader)
            _run_helper(options, commit, job_id, writer)
        os.close(writer)
        try:
            raw = _read_pipe_line(reader)
        finally:
            os.close(reader)
    finally:
        with suppress(OSError):
            os.close(reader)
        with suppress(OSError):
            os.close(writer)
    if not raw:
        raise DeployCtlError("deployment handoff helper exited before reporting an outcome")
    try:
        response = json.loads(raw)
    except ValueError as exc:
        msg = "deployment handoff helper reported an invalid response"
        raise DeployCtlError(msg) from exc
    if not isinstance(response, dict):
        raise DeployCtlError("deployment handoff helper reported a non-object response")
    return response


def _handle_checkout(options: Options, request: dict[str, object]) -> dict[str, object]:
    """Handle an exact-commit checkout request.

    A queue-invoked checkout runs through a detached helper that waits for the
    initiating row to be durably succeeded before the destructive handoff; a
    manual invocation retains the synchronous safe path.

    Args:
        options: Deployment options.
        request: Decoded request object.

    Returns:
        Protocol response.

    Raises:
        DeployCtlError: If the checkout is unsafe or cannot be reported.
    """
    commit = request.get("commit")
    if not isinstance(commit, str):
        raise DeployCtlError("checkout request requires string field 'commit'")
    job_id, cancelled = _current_queue_job_id()
    if job_id is not None:
        if cancelled:
            raise DeployCtlError("checkout job was cancelled during deployment")
        return _queue_checkout(options, commit, job_id)
    try:
        with deploy_lock(options.lock_timeout_seconds):
            _reconcile_cli(_read_state())
            state = _deploy_locked(options, commit)
            return _candidate_response(state)
    except LockTimeoutError as exc:
        raise DeployCtlError("timed out waiting for the deployment lock") from exc


def _generate_challenge() -> str:
    """Return a fresh 7-character lowercase hexadecimal confirmation challenge.

    Returns:
        A challenge matching ``[0-9a-f]{7}``.
    """
    return secrets.token_hex(4)[:7]


def _challenge_digest(challenge: str) -> str:
    """Return the durable digest of one confirmation challenge.

    Args:
        challenge: Challenge string.

    Returns:
        Hex SHA-256 digest.
    """
    return hashlib.sha256(challenge.encode()).hexdigest()


def _cli_target_commit(state: RollbackState | None) -> str | None:
    """Return the commit the global CLIs must resolve to right now.

    A pending mission is deliberately ignored: while a candidate is
    provisional the pointer must stay on the previous confirmed commit, so a
    repair never activates candidate code before confirmation.

    An in-flight cold migration is also a hold: while the durable desired
    intent still carries its ``migration`` flag, the supervisor has not yet
    proven the migrated target queue-ready and converged authority, so the
    only safe target is the previous confirmed authority. Reconciliation must
    neither activate the unproven target nor keep restoring the superseded
    terminal record. A strictly newer deployment mission supersedes the
    migration and resumes normal reconciliation.

    Args:
        state: Current supervised-deployment state, or ``None``.

    Returns:
        The exact commit the CLI pointer should select, or ``None``.
    """
    try:
        desired = supervise.read_desired_strict()
    except supervise.DesiredIntentError:
        # A present-but-malformed authoritative intent is observable
        # corruption: fail closed instead of falling back to other authority
        # surfaces (which may already name the unproven migrated commit).
        return None
    if desired is not None and desired.migration:
        if state is None or state.generation <= desired.generation:
            return None
    if state is None:
        meta = read_meta()
        return None if meta is None else meta.git_commit
    if state.status == STATUS_PENDING:
        return state.previous_commit
    if state.status == STATUS_CONFIRMED:
        return state.commit
    return state.previous_commit


def _reconcile_cli(state: RollbackState | None) -> None:
    """Idempotently repair a stale maintained CLI pointer.

    A crash after durable ``confirmed`` state is written but before the CLI
    pointer is switched leaves a permanently confirmed worker with stale CLIs.
    Every controller invocation that observes deployment state runs this
    repair: the pointer is moved to the confirmed commit only when that
    commit's environment is already usable, and never towards a provisional
    candidate.

    Args:
        state: Current supervised-deployment state, or ``None``.
    """
    target = _cli_target_commit(state)
    if target is None:
        return
    if cli.reconcile_pointer(target):
        append_deploy_log(f"reconciled maintained CLI pointer to commit {target}")


def _verify_challenge(state: RollbackState, answer: object) -> None:
    """Validate a second-confirmation challenge answer or roll back.

    Args:
        state: Live pending deployment state.
        answer: Decoded challenge response.

    Raises:
        DeployCtlError: If the challenge response is unexpected, malformed, or
            does not match the stored challenge.
    """
    if not isinstance(answer, str) or state.challenge_hash is None:
        _rollback_locked(state)
        raise DeployCtlError("unexpected challenge response; deployment was rolled back")
    if CHALLENGE_RE.fullmatch(answer) is None:
        _rollback_locked(state)
        raise DeployCtlError("malformed challenge response; deployment was rolled back")
    if not secrets.compare_digest(_challenge_digest(answer[::-1]), state.challenge_hash):
        _rollback_locked(state)
        raise DeployCtlError("challenge response is incorrect; deployment was rolled back")


def _confirmation_response(state: RollbackState) -> dict[str, object]:
    """Build the terminal successful confirmation response.

    Args:
        state: Confirmed mission state.

    Returns:
        Protocol response for a confirmed deployment.
    """
    return {"type": "confirm", "ok": True, "commit": state.commit, "confirmed": True}


def _confirmation_state(request: dict[str, object]) -> RollbackState:
    """Load, recover, and validate the mission targeted by confirmation.

    Args:
        request: Decoded confirmation request.

    Returns:
        A confirmed idempotent mission or a valid pending confirmation mission.

    Raises:
        DeployCtlError: If no confirmation is pending, rollback is in progress,
            the window elapsed, or the requested commit does not match.
    """
    state = _read_state()
    if state is None:
        raise DeployCtlError("no checkout is pending confirmation")
    if state.status == STATUS_PENDING:
        state = _recover_applied_settlement(state)
    if state.status == STATUS_CONFIRMED:
        if request.get("commit") != state.commit:
            raise DeployCtlError("confirmation commit does not match the confirmed commit")
        return state
    if state.status != STATUS_PENDING:
        raise DeployCtlError("no checkout is pending confirmation")
    if state.settlement_transition == SETTLEMENT_ROLLBACK:
        _rollback_locked(state)
        raise DeployCtlError("deployment rollback is already in progress")
    if _pending_mission_rollback_due(state):
        _rollback_locked(state)
        raise DeployCtlError("confirmation window lapsed; deployment was rolled back")
    if request.get("commit") != state.commit:
        _rollback_locked(state)
        raise DeployCtlError("confirmation commit does not match the proposed commit; rolled back")
    return state


def _issue_confirmation_challenge(state: RollbackState) -> dict[str, object]:
    """Persist and return a fresh confirmation challenge.

    Args:
        state: Pending mission to challenge.

    Returns:
        Protocol response containing the plaintext challenge.
    """
    challenge = _generate_challenge()
    _write_state(replace(state, challenge_hash=_challenge_digest(challenge)))
    return {
        "type": "confirm",
        "ok": True,
        "commit": state.commit,
        "challenge": challenge,
    }


def _authorize_confirmation(state: RollbackState, answer: object) -> None:
    """Verify challenge and lifecycle authority immediately before confirmation.

    Args:
        state: Pending mission being confirmed.
        answer: Supplied challenge answer.

    Raises:
        DeployCtlError: If challenge, liveness, or lifecycle authority fails.
    """
    _verify_challenge(state, answer)
    if _pending_mission_rollback_due(state):
        _rollback_locked(state)
        raise DeployCtlError("candidate failed before confirmation; deployment was rolled back")
    if not lifecycle_state.authorize_mission_confirm(
        _mission_authority_facts(state.status, candidate_ready=True)
    ):
        _rollback_locked(state)
        raise DeployCtlError(
            "lifecycle authority refuses confirmation (candidate not proven ready or "
            "durable authority malformed); deployment was rolled back"
        )


def _prepare_confirmation_candidate(state: RollbackState, options: Options) -> None:
    """Prepare the exact candidate for terminal confirmation.

    Args:
        state: Pending mission being confirmed.
        options: Runtime options for legacy CLI preparation.

    Raises:
        DeployCtlError: If the legacy CLI environment cannot be prepared.
    """
    if supervise.supervisor_running():
        settle_desired(state.commit, state.repo, state.uv_path)
        return
    try:
        cli.build_cli_root(
            Path(state.repo), state.commit, state.uv_path, options.cli_timeout_seconds
        )
    except cli.CliError as exc:
        _rollback_locked(state)
        msg = f"confirmed CLI environment could not be prepared; deployment was rolled back: {exc}"
        raise DeployCtlError(msg) from exc
    write_meta(state.new_meta)


def _finalize_confirmation(state: RollbackState) -> RollbackState:
    """Persist terminal confirmation and maintain the CLI pointer.

    Args:
        state: Pending mission whose candidate is prepared.

    Returns:
        Terminal confirmed mission state.
    """
    if supervise.supervisor_running():
        return _finalize_supervised_confirmation(state)
    terminal = _clear_settlement(state, STATUS_CONFIRMED)
    _write_state(terminal)
    try:
        cli.set_current(terminal.commit)
    except cli.CliError as exc:
        append_deploy_log(f"supervised deployment confirmed but CLI activation failed: {exc}")
    cli.gc_cli_roots((terminal.commit, terminal.previous_commit))
    append_deploy_log(f"supervised deployment confirmed commit {terminal.commit}")
    return terminal


def _confirm_locked(request: dict[str, object], options: Options) -> dict[str, object]:
    """Advance one pending deployment through the two-phase handshake.

    Args:
        request: Decoded confirm request.
        options: Runtime options.

    Returns:
        Protocol response.

    """
    state = _confirmation_state(request)
    if state.status == STATUS_CONFIRMED:
        return _confirmation_response(state)
    answer = request.get("challenge")
    if answer is None:
        return _issue_confirmation_challenge(state)
    _authorize_confirmation(state, answer)
    _prepare_confirmation_candidate(state, options)
    state = _finalize_confirmation(state)
    return _confirmation_response(state)


def _handle_confirm(options: Options, request: dict[str, object]) -> dict[str, object]:
    """Serialize confirmation with rollback/deadline enforcement.

    Args:
        options: Deployment options.
        request: Decoded request object.

    Returns:
        Protocol response.
    """
    try:
        with deploy_lock(options.lock_timeout_seconds):
            return _confirm_locked(request, options)
    except LockTimeoutError as exc:
        raise DeployCtlError("timed out waiting for the deployment lock") from exc


def _handle_status(options: Options) -> dict[str, object]:
    """Report and lazily enforce supervised-deployment state.

    Args:
        options: Deployment options.

    Returns:
        Protocol status response.
    """
    try:
        with deploy_lock(options.lock_timeout_seconds):
            state = _read_state()
            if state is not None and state.status == STATUS_PENDING:
                state = _recover_applied_settlement(state)
                if state.status == STATUS_PENDING and _pending_mission_rollback_due(state):
                    _rollback_locked(state)
                    state = _read_state()
            meta = read_meta()
            _reconcile_cli(state)
    except LockTimeoutError as exc:
        raise DeployCtlError("timed out waiting for the deployment lock") from exc
    if state is None:
        return {
            "type": "status",
            "ok": True,
            "phase": "idle",
            "known_commit": None if meta is None else meta.git_commit,
        }
    if state.status == STATUS_PENDING:
        return {
            "type": "status",
            "ok": True,
            "phase": "await-reversal" if state.challenge_hash is not None else "await-confirmation",
            "proposed_commit": state.commit,
            "previous_commit": state.previous_commit,
            "deadline": state.deadline,
        }
    known = state.commit if state.status == STATUS_CONFIRMED else state.previous_commit
    return {
        "type": "status",
        "ok": True,
        "phase": "idle",
        "last_outcome": state.status,
        "known_commit": known,
    }


def _dispatch(options: Options, request: dict[str, object]) -> dict[str, object]:
    """Dispatch one protocol request.

    Args:
        options: Runtime options.
        request: Decoded request.

    Returns:
        Protocol response.

    Raises:
        DeployCtlError: For unknown request types.
    """
    request_type = request.get("type")
    if request_type == "checkout":
        return _handle_checkout(options, request)
    if request_type == "confirm":
        return _handle_confirm(options, request)
    if request_type == "status":
        return _handle_status(options)
    raise DeployCtlError("request type must be checkout, confirm, or status")


def _parse_request(text: str) -> dict[str, object]:
    """Decode one JSON protocol request.

    Args:
        text: JSON object text.

    Returns:
        Decoded object.

    Raises:
        DeployCtlError: If the request is not a JSON object.
    """
    try:
        request = json.loads(text)
    except ValueError as exc:
        raise DeployCtlError("request is not valid JSON") from exc
    if not isinstance(request, dict):
        raise DeployCtlError("request must be a JSON object")
    return request


def _emit(response: dict[str, object]) -> None:
    """Write exactly one JSON response line.

    Args:
        response: Response object.
    """
    sys.stdout.write(json.dumps(response, sort_keys=True) + "\n")
    sys.stdout.flush()


def build_parser() -> argparse.ArgumentParser:
    """Build the stable wrapper CLI parser.

    Returns:
        Configured parser.
    """
    parser = argparse.ArgumentParser(
        prog="lubko-deploy-ctl",
        description="Supervise exact-commit Lubko self-deployments with automatic rollback.",
    )
    parser.add_argument("request", help="one JSON request object")
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument("--uv", default=None)
    parser.add_argument(
        "--confirm-window-seconds",
        type=float,
        default=float(
            os.getenv("LUBKO_ROLLBACK_WINDOW_SECONDS", str(DEFAULT_CONFIRM_WINDOW_SECONDS))
        ),
    )
    parser.add_argument("--grace-seconds", type=float, default=DEFAULT_STOP_GRACE_SECONDS)
    parser.add_argument("--db-timeout", type=float, default=DEFAULT_POSTGRES_TIMEOUT_SECONDS)
    parser.add_argument("--lock-timeout", type=float, default=DEFAULT_LOCK_TIMEOUT_SECONDS)
    parser.add_argument(
        "--validation-timeout", type=float, default=DEFAULT_VALIDATION_TIMEOUT_SECONDS
    )
    parser.add_argument("--git-timeout", type=float, default=DEFAULT_GIT_TIMEOUT_SECONDS)
    parser.add_argument("--cli-timeout", type=float, default=DEFAULT_CLI_TIMEOUT_SECONDS)
    return parser


def _request_type(request: dict[str, object]) -> str:
    """Return the protocol request type, or an empty string.

    Args:
        request: Decoded request object.

    Returns:
        The ``type`` field when it is a string, otherwise ``""``.
    """
    value = request.get("type")
    return value if isinstance(value, str) else ""


def _checkout_failure_exit_code(request_type: str, response: dict[str, object]) -> int:
    """Return the process exit code for one controller response.

    A failed ``checkout`` must exit non-zero: the owning queue worker then
    records the row as ``failed``, so a helper death or a reported error can
    never leave a falsely-successful checkout row. Every other protocol
    rejection keeps returning zero so its structured JSON error reaches the
    orchestrator.

    Args:
        request_type: Protocol request type.
        response: The emitted response object.

    Returns:
        ``EXIT_ERROR`` for a failed checkout, ``EXIT_OK`` otherwise.
    """
    if request_type == "checkout" and response.get("ok") is not True:
        return EXIT_ERROR
    return EXIT_OK


# Public queue-handoff transport primitives shared with the ``lubko-deploy``
# CLI (issue #68). A queue-invoked ``lubko-deploy deploy`` forks the same
# detached-handoff pattern as a supervised checkout and must obey the same
# ordering: the initiating queue row reaches durable ``succeeded`` before any
# destructive old-worker retirement. These public names are the tested private
# helpers above, exposed so the deploy path reuses exactly one implementation.
current_queue_job_id = _current_queue_job_id
read_pipe_line = _read_pipe_line
send_helper_response = _send_helper_response
send_helper_error = _send_helper_error
wait_for_durable_success = _wait_for_durable_success
handoff_durable_wait_seconds = HANDOFF_DURABLE_WAIT_SECONDS

# Public protocol-parsing helpers: the JSON request/response contract of the
# controller protocol is a stable interface exercised directly by the tests.
parse_request = _parse_request
request_type = _request_type
checkout_failure_exit_code = _checkout_failure_exit_code

# Public rollback-spawn convergence helper: previous-worker replacement is a
# stable rollback contract exercised directly by the tests.
restart_previous = _restart_previous


def main(argv: list[str] | None = None) -> int:
    """Run one stable-wrapper protocol request.

    Args:
        argv: CLI arguments, or ``None`` for ``sys.argv``.

    Returns:
        Process exit code. A failed ``checkout`` exits non-zero so the owning
        queue job is durably recorded as ``failed``; other protocol rejections
        still return zero so queue jobs can deliver their structured JSON error
        to the orchestrator.
    """
    parser = build_parser()
    args = parser.parse_args(argv)
    request_type = ""
    try:
        request = _parse_request(args.request)
        request_type = _request_type(request)
        uv_path = resolve_uv(args.uv)
        options = Options(
            repo=args.repo.resolve(),
            uv_path=uv_path,
            confirm_window_seconds=args.confirm_window_seconds,
            stop_grace_seconds=args.grace_seconds,
            postgres_timeout_seconds=args.db_timeout,
            lock_timeout_seconds=args.lock_timeout,
            validation_timeout_seconds=args.validation_timeout,
            git_timeout_seconds=args.git_timeout,
            cli_timeout_seconds=args.cli_timeout,
        )
        if options.confirm_window_seconds <= 0:
            raise DeployCtlError("confirmation window must be positive")
        response = _dispatch(options, request)
    except (DeployCtlError, UvResolutionError) as exc:
        response = {"ok": False, "error": str(exc)}
        _emit(response)
        return _checkout_failure_exit_code(request_type, response)
    except OSError as exc:
        response = {"ok": False, "error": f"operating-system error: {exc}"}
        _emit(response)
        return EXIT_ERROR
    _emit(response)
    return _checkout_failure_exit_code(request_type, response)


if __name__ == "__main__":
    raise SystemExit(main())
