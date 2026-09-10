"""Crash-safe supervised self-deployment primitives for Lubko.

The host side exposes a small set of exact, idempotent deployment primitives.
The external orchestrator owns transaction sequencing; host state retains only
what is required to preserve exact-commit authority, readiness, rollback, and
fail-closed recovery across crashes. Confirmation is a single exact-commit
operation rather than a multi-step challenge handshake.
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import re
import secrets
import socket
import subprocess  # ruff: ignore[suspicious-subprocess-import] — required for process management; no shell injection
import sys
import time
from contextlib import suppress
from dataclasses import dataclass, replace
from pathlib import Path
from typing import TYPE_CHECKING, Final
from uuid import UUID

import psycopg
from psycopg.rows import tuple_row

from lubko import cli, lifecycle, lifecycle_state, startup_contract, supervise
from lubko.config import load_database_config
from lubko.durable import DurabilityError, remove_durable, write_json_durable
from lubko.lifecycle import (
    SCHEMA_VERSION,
    STATE_RUNNING,
    LockTimeoutError,
    WorkerMeta,
    WorkerMetadataError,
    _converge_unproven_spawn,
    append_deploy_log,
    check_postgres,
    deploy_lock,
    detach_standard_streams,
    parse_supervisor_candidate_meta,
    process_identity,
    read_meta,
    read_meta_strict,
    run_validation,
    stop_worker,
    supervisor_candidate_wire_descriptor,
    worker_alive,
    worker_env,
    worker_log_path,
    write_meta,
)
from lubko.state import rollback_state_path, state_root
from lubko.toolchain import UvResolutionError, resolve_uv
from lubko.worker import JOB_ID_ENV

if TYPE_CHECKING:
    from collections.abc import Sequence

    from lubko.lifecycle import ProcessIdentity

LOGGER: Final = logging.getLogger(__name__)

EXIT_OK: Final = 0
EXIT_ERROR: Final = 1
ROLLBACK_SCHEMA_VERSION: Final = 4
#: Stable on-disk compatibility envelope for supervisor-owned missions.
#: Schema-3 predecessor supervisors understand this envelope and ignore additive fields.
SUPERVISOR_ROLLBACK_WIRE_SCHEMA_VERSION: Final = 3
SUPPORTED_ROLLBACK_SCHEMA_VERSIONS: Final = frozenset({2, 3})
STATUS_PENDING: Final = "pending"
STATUS_CONFIRMED: Final = "confirmed"
STATUS_ROLLED_BACK: Final = "rolled_back"
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
HANDOFF_POLL_SECONDS: Final = 0.1
HANDOFF_RESPONSE_MAX_BYTES: Final = 1048576
HELPER_ERROR_MAX_CHARS: Final = 8000
HANDOFF_DURABLE_WAIT_SECONDS: Final = 60.0

#: Durable file that preserves the pre-confirmation startup artifacts so
#: rollback can restore them exactly.  Written before confirmation mutates
#: the artifacts and removed after confirmation succeeds.
_PRE_CONFIRMATION_ARTIFACTS_NAME: Final = "pre-confirmation-startup-artifacts.json"
_CONFIRMATION_RECEIPT_NAME: Final = "startup-confirmation-receipt.json"
_MAX_BYTE_VALUE: Final = 255

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
    deadline: float
    repo: str
    uv_path: str
    stop_grace_seconds: float
    git_timeout_seconds: float
    previous_retiring: bool
    previous_meta: WorkerMeta
    new_meta: WorkerMeta | None
    supervisor_owned: bool | None = None
    previous_restart_meta: WorkerMeta | None = None
    previous_restart_released: bool = False

    def to_dict(self) -> dict[str, object]:
        """Serialize durable rollback state.

        Returns:
            A JSON-compatible mapping.
        """
        supervisor_wire = self.supervisor_owned is True
        result: dict[str, object] = {
            "schema_version": (
                SUPERVISOR_ROLLBACK_WIRE_SCHEMA_VERSION if supervisor_wire else self.schema_version
            ),
            "generation": self.generation,
            "status": self.status,
            "commit": self.commit,
            "previous_commit": self.previous_commit,
            "deadline": self.deadline,
            "repo": self.repo,
            "uv_path": self.uv_path,
            "stop_grace_seconds": self.stop_grace_seconds,
            "git_timeout_seconds": self.git_timeout_seconds,
            "previous_retiring": self.previous_retiring,
            "previous_meta": self.previous_meta.to_dict(),
            "new_meta": (
                supervisor_candidate_wire_descriptor(self.commit, self.repo)
                if supervisor_wire
                else None
                if self.new_meta is None
                else self.new_meta.to_dict()
            ),
            "supervisor_owned": self.supervisor_owned,
            "previous_restart_meta": (
                None if self.previous_restart_meta is None else self.previous_restart_meta.to_dict()
            ),
            "previous_restart_released": self.previous_restart_released,
        }
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
            state = cls._build_from_dict(data)
        except (KeyError, TypeError, ValueError) as exc:
            msg = "supervised deployment state is malformed"
            raise DeployCtlError(msg) from exc
        return state

    @classmethod
    def _build_from_dict(cls, data: dict[str, object]) -> RollbackState:
        """Build rollback state from a decoded mapping.

        Returns:
            Parsed rollback state.

        Raises:
            TypeError: If a required field has the wrong type.
            ValueError: If a required field has an invalid value.
        """
        previous = data["previous_meta"]
        replacement = data["new_meta"]
        if not isinstance(previous, dict):
            msg = "previous_meta must be a dict"
            raise TypeError(msg)
        supervisor_owned = _optional_json_bool(data.get("supervisor_owned"))
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
            msg = "generation must be a positive integer"
            raise ValueError(msg)
        generation = raw_generation
        schema_version = _required_json_int(data["schema_version"])
        status = _required_json_string(data["status"])
        if status not in {STATUS_PENDING, STATUS_CONFIRMED, STATUS_ROLLED_BACK}:
            msg = f"unexpected status {status!r}"
            raise ValueError(msg)
        commit = _required_commit(data["commit"])
        previous_commit = _required_commit(data["previous_commit"])
        repo = _required_json_string(data["repo"])
        replacement_meta = parse_supervisor_candidate_meta(
            replacement, supervisor_owned=supervisor_owned, commit=commit, repo=repo
        )
        restart_meta, restart_released = _parse_previous_restart(
            data,
            supervisor_owned=supervisor_owned,
            status=status,
            repo=repo,
            previous_commit=previous_commit,
        )
        return cls(
            schema_version=schema_version,
            generation=generation,
            status=status,
            commit=commit,
            previous_commit=previous_commit,
            deadline=_required_nonnegative_finite_json_number(data["deadline"]),
            repo=repo,
            uv_path=_required_json_string(data["uv_path"]),
            stop_grace_seconds=_required_positive_finite_json_number(data["stop_grace_seconds"]),
            git_timeout_seconds=_required_positive_finite_json_number(data["git_timeout_seconds"]),
            previous_retiring=_retiring_flag(data.get("previous_retiring", _ABSENT)),
            previous_meta=WorkerMeta.from_dict(previous),
            new_meta=replacement_meta,
            supervisor_owned=supervisor_owned,
            previous_restart_meta=restart_meta,
            previous_restart_released=restart_released,
        )


@dataclass(frozen=True, slots=True)
class GatedWorker:
    """Candidate process blocked from consuming the queue until released."""

    proc: subprocess.Popen[bytes]
    gate_writer: int
    meta: WorkerMeta


_ABSENT: Final = object()


def _parse_previous_restart(
    data: dict[str, object],
    *,
    supervisor_owned: bool | None,
    status: str,
    repo: str,
    previous_commit: str,
) -> tuple[WorkerMeta | None, bool]:
    """Parse and validate durable legacy rollback restart authority.

    Args:
        data: Decoded rollback state.
        supervisor_owned: Durable mission ownership classification.
        status: Parsed rollback mission status.
        repo: Expected repository identity.
        previous_commit: Commit the restart worker must run.

    Returns:
        The optional restart worker metadata and its release flag.

    Raises:
        TypeError: If restart authority is malformed or inconsistent.
    """
    restart_raw = data.get("previous_restart_meta")
    if restart_raw is None:
        restart_meta = None
    elif isinstance(restart_raw, dict):
        restart_meta = WorkerMeta.from_dict(restart_raw)
    else:
        raise TypeError
    restart_released = _retiring_flag(data.get("previous_restart_released", _ABSENT))
    if restart_meta is None:
        if restart_released:
            raise TypeError
    elif (
        supervisor_owned is not False
        or status != STATUS_PENDING
        or restart_meta.state != STATE_RUNNING
        or restart_meta.repo != repo
        or restart_meta.git_commit != previous_commit
    ):
        raise TypeError
    return restart_meta, restart_released


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


def _required_json_int(value: object) -> int:
    """Return an exact JSON integer, rejecting booleans and coercion.

    Raises:
        TypeError: If the value is not an integer or is a boolean.
    """
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError
    return value


def _required_json_string(value: object) -> str:
    """Return an exact JSON string without normalizing other values.

    Raises:
        TypeError: If the value is not a string.
    """
    if not isinstance(value, str):
        raise TypeError
    return value


def _required_commit(value: object) -> str:
    """Return an exact full commit id.

    Raises:
        ValueError: If the value does not match the commit pattern.
    """
    commit = _required_json_string(value)
    if COMMIT_RE.fullmatch(commit) is None:
        raise ValueError
    return commit


def _required_finite_json_number(value: object) -> float:
    """Return a finite JSON number, rejecting booleans and numeric strings.

    Raises:
        TypeError: If the value is not a number or is a boolean.
        ValueError: If the value is not finite.
    """
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise TypeError
    result = float(value)
    if not math.isfinite(result):
        raise ValueError
    return result


def _required_nonnegative_finite_json_number(value: object) -> float:
    """Return a finite JSON number in the non-negative domain.

    Raises:
        ValueError: If the value is negative.
    """
    result = _required_finite_json_number(value)
    if result < 0:
        raise ValueError
    return result


def _required_positive_finite_json_number(value: object) -> float:
    """Return a finite JSON number in the positive domain.

    Raises:
        ValueError: If the value is not positive.
    """
    result = _required_finite_json_number(value)
    if result <= 0:
        raise ValueError
    return result


def _optional_json_string(value: object | None) -> str | None:
    """Return a nullable exact JSON string."""
    if value is None:
        return None
    return _required_json_string(value)


def _optional_json_bool(value: object | None) -> bool | None:
    """Return a nullable exact JSON boolean.

    Raises:
        TypeError: If the value is not a boolean.
    """
    if value is None:
        return None
    if not isinstance(value, bool):
        raise TypeError
    return value


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
    if state.supervisor_owned is False:
        return state.new_meta is not None and worker_alive(state.new_meta)
    if not supervise.supervisor_running():
        return False
    return _supervised_mission_active(state)


def _supervised_confirmation_authority_matches(
    state: RollbackState,
    desired: supervise.SupervisorDesired | None,
    status: supervise.SupervisorStatus | None,
) -> bool:
    """Return whether desired and applied authority remain compatible with a mission.

    The mission itself may be the newest durable supervisor intent. In that case
    ``desired`` legitimately still names an older generation (and may still name
    the previous commit) while the supervisor has applied the mission generation
    directly. That older desired intent is superseded by the mission, so exact
    application of the mission remains compatible.

    A desired generation at or after the mission is different: it is a newer
    lifecycle obligation and must name the same commit, with the applied generation
    bounded between the mission and desired generations. Missing, contradictory,
    or otherwise out-of-range authority still fails closed.
    """
    if desired is None or status is None or status.commit != state.commit:
        return False
    if desired.generation < state.generation:
        return status.applied_generation == state.generation
    return (
        desired.commit == state.commit
        and state.generation <= status.applied_generation <= desired.generation
    )


def _supervised_terminalization_authority_matches(
    expected_commit: str,
    expected_generation: int,
    desired: supervise.SupervisorDesired | None,
    status: supervise.SupervisorStatus | None,
) -> bool:
    """Return whether terminalization still owns one live queue-ready worker.

    The status snapshot alone is not a synchronous liveness proof: the child can
    exit after readiness was published but before deployctl acquires the
    generation lock. Bind the exact settled desired/status generation to the
    supervisor's durable child identity and re-prove that child alive before a
    pending mission may become terminal.
    """
    if desired is None or status is None:
        return False
    if (
        desired.commit != expected_commit
        or desired.generation != expected_generation
        or status.commit != expected_commit
        or status.applied_generation != expected_generation
    ):
        return False
    if status.ready is not True or status.holding or status.child is None:
        return False
    supervisor_state = supervise.read_state()
    return (
        supervisor_state.commit == expected_commit
        and supervisor_state.applied_generation == expected_generation
        and supervisor_state.ready
        and not supervise.is_holding(supervisor_state)
        and supervisor_state.child == status.child
        and supervise.child_alive(status.child)
    )


def _supervised_mission_authoritative(state: RollbackState) -> bool:
    """Return whether durable supervisor authority remains compatible with this mission.

    Pending confirmation may legitimately coexist with a newer same-commit
    restart or migration generation. Treat that lifecycle obligation as
    compatible while the mission is pending, but fail closed on unreadable,
    missing, contradictory, or different-commit authority. Terminalization
    still binds to the exact settled generation separately.
    """
    supervisor_state = supervise.read_state()
    try:
        desired = supervise.read_desired_strict()
    except supervise.DesiredIntentError:
        return False
    status = supervise.read_status()
    return (
        status is not None
        and supervisor_state.commit == status.commit
        and supervisor_state.applied_generation == status.applied_generation
        and _supervised_confirmation_authority_matches(state, desired, status)
    )


def _require_known_confirmation_ownership(state: RollbackState) -> None:
    """Require an explicit durable owner before confirmation can advance.

    Raises:
        DeployCtlError: If the confirmation authority is unknown.
    """
    if state.supervisor_owned is None:
        msg = "confirmation authority is unknown; deployment remains pending"
        raise DeployCtlError(msg)


def _require_confirmation_authority(state: RollbackState) -> None:
    """Fail closed when supervised confirmation no longer owns durable authority.

    Raises:
        DeployCtlError: If the confirmation authority has been superseded.
    """
    _require_known_confirmation_ownership(state)
    if state.supervisor_owned is False or not supervise.supervisor_running():
        return
    try:
        desired = supervise.read_desired_strict()
    except supervise.DesiredIntentError as exc:
        msg = "supervisor authority was superseded before confirmation; deployment remains pending"
        raise DeployCtlError(msg) from exc
    if not _supervised_confirmation_authority_matches(state, desired, supervise.read_status()):
        msg = "supervisor authority was superseded before confirmation; deployment remains pending"
        raise DeployCtlError(msg)


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
    if state.supervisor_owned is False:
        return (
            state.new_meta is None
            or time.time() >= state.deadline
            or not worker_alive(state.new_meta)
        )
    if not supervise.supervisor_running():
        return True
    if not _supervised_mission_authoritative(state):
        return True
    if time.time() < state.deadline:
        return False
    status = supervise.read_status()
    return (
        status is None
        or status.commit != state.commit
        or status.applied_generation < state.generation
        or status.ready is not True
        or status.holding
        or not _supervised_mission_active(state)
    )


def settle_desired(commit: str, repo: str, uv_path: str) -> int:
    """Request one exact supervisor target and await queue readiness.

    The external orchestrator owns transaction sequencing. This host-side
    primitive is intentionally idempotent in effect. If the current desired
    intent already names the exact commit, settlement preserves that generation
    (including any restart or migration obligation) and waits for it to become
    queue-ready. Otherwise it publishes a fresh generation for the exact commit.

    Returns:
        The applied supervisor generation.

    Raises:
        DeployCtlError: If the supervisor cannot apply or prove the target.
    """
    worker_id = os.getenv("LUBKO_WORKER_ID") or socket.gethostname()
    try:
        desired = supervise.read_desired_strict()
    except supervise.DesiredIntentError as exc:
        msg = "the supervisor desired intent is not trustworthy"
        raise DeployCtlError(msg) from exc
    if desired is not None and desired.commit == commit:
        # Preserve an already-published same-commit lifecycle obligation. In
        # particular, confirmation must not erase a concurrent restart or
        # migration by publishing a newer ordinary settlement generation.
        generation = desired.generation
    else:
        generation = supervise.request_run(
            commit,
            repo=repo,
            uv_path=uv_path,
            worker_id=worker_id,
        )
    lifecycle_state.failpoint(lifecycle_state.FAILPOINT_MISSION_CONFIRM)
    if not supervise.wait_for_generation(generation, supervise.DEFAULT_REQUEST_TIMEOUT_SECONDS):
        msg = "the external supervisor did not apply the requested target"
        raise DeployCtlError(msg)
    if not supervise.wait_until_ready(
        generation, supervise.DEFAULT_REQUEST_TIMEOUT_SECONDS, commit=commit
    ):
        msg = "the external supervisor did not prove the requested worker queue-ready"
        raise DeployCtlError(msg)
    return generation


def publish_mission(state: RollbackState, lock_timeout_seconds: float) -> None:
    """Durably publish a prepared pending mission and arm its watchdog.

    Args:
        state: Prepared pending mission (may already be durable; idempotent).
        lock_timeout_seconds: Deployment-lock timeout for the watchdog.

    Raises:
        DeployCtlError: If publishing or arming the watchdog fails.
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
    if not supervise.wait_until_ready(
        state.generation, supervise.DEFAULT_REQUEST_TIMEOUT_SECONDS, commit=state.commit
    ):
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
    return subprocess.run(  # ruff: ignore[subprocess-without-shell-equals-true] — callers pass only hardcoded git args
        ["git", *args],  # ruff: ignore[start-process-with-partial-path] — git is a system tool on PATH
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
        msg = "could not inspect deployment checkout"
        raise DeployCtlError(msg)
    if proc.stdout:
        msg = "deployment checkout is dirty; commit or discard changes first"
        raise DeployCtlError(msg)


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
        proc = subprocess.Popen(  # ruff: ignore[subprocess-without-shell-equals-true] — sys.executable with known GATED_SHIM_SOURCE script
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
        msg = "candidate exited before the rollback mission could be armed"
        raise DeployCtlError(msg)
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
    except subprocess.TimeoutExpired:
        pass
    else:
        return
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
        "worker_pid": (
            None if state.new_meta is None or (state.new_meta.pid or 0) <= 0 else state.new_meta.pid
        ),
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
        with (
            psycopg.connect(database.conninfo(), row_factory=tuple_row) as conn,
            conn.transaction(),
            conn.cursor() as cursor,
        ):
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
        msg = "the current queue job row does not exist"
        raise DeployCtlError(msg)
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
        msg = "deployment handoff helper response exceeded the bounded size"
        raise DeployCtlError(msg)
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
                msg = "checkout queue job was deleted before durable success"
                raise DeployCtlError(msg)
            status = str(row[0])
            if status == "succeeded" and row[1] is None:
                return
            if status in {"failed", "cancelled"}:
                msg = f"checkout queue job reached {status} before durable success"
                raise DeployCtlError(msg)
            time.sleep(HANDOFF_POLL_SECONDS)
        msg = "checkout queue job did not reach durable success before the deadline"
        raise DeployCtlError(msg)
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


def _complete_supervisor_owned_handoff(
    options: Options,
    state: RollbackState,
    gated: GatedWorker | None,
) -> RollbackState:
    """Publish and await a handoff prepared for supervisor ownership.

    Returns:
        The live pending rollback state after supervisor convergence.

    Raises:
        DeployCtlError: If a legacy gated candidate is carried or convergence fails.
    """
    if gated is not None:
        _abort_gated_candidate(gated)
        msg = "supervisor-owned handoff cannot carry a legacy gated candidate"
        raise DeployCtlError(msg)
    publish_mission(state, options.lock_timeout_seconds)
    return _wait_for_supervisor_mission(state, options.confirm_window_seconds)


def _release_legacy_gated_candidate(
    options: Options,
    retiring: RollbackState,
    gated: GatedWorker,
) -> RollbackState:
    """Stop the previous worker, release the gated candidate, and verify liveness.

    Returns:
        The live pending rollback state after the gated candidate is released.

    Raises:
        DeployCtlError: If the previous worker cannot be stopped or the
            candidate exits immediately.
    """
    if not stop_worker(retiring.previous_meta, options.stop_grace_seconds):
        msg = "could not stop the known-good worker"
        raise DeployCtlError(msg)
    _release_gate(gated.gate_writer)
    if not _wait_for_released_worker(gated.meta):
        msg = "candidate worker exited immediately after release"
        raise DeployCtlError(msg)
    live = replace(retiring, deadline=time.time() + options.confirm_window_seconds)
    _write_state(live)
    return live


def _complete_legacy_handoff(
    options: Options,
    state: RollbackState,
    gated: GatedWorker | None,
) -> RollbackState:
    """Complete an explicitly legacy gated handoff without mixing authorities.

    Returns:
        The live pending rollback state after the gated candidate is released.

    Raises:
        DeployCtlError: If the gated candidate is missing or authority is lost.
    """
    if gated is None:
        msg = "legacy handoff requires its prepared gated candidate"
        raise DeployCtlError(msg)
    if supervise.supervisor_running():
        _abort_gated_candidate(gated)
        if not _rollback_locked(state):
            msg = "legacy handoff lost authority to a live supervisor and rollback remains pending"
            raise DeployCtlError(msg)
        msg = "legacy handoff aborted because a supervisor became authoritative"
        raise DeployCtlError(msg)
    retiring = replace(state, previous_retiring=True)
    _write_state(retiring)
    try:
        return _release_legacy_gated_candidate(options, retiring, gated)
    except DeployCtlError:
        _abort_gated_candidate(gated)
        _rollback_locked(retiring)
        raise


def _complete_handoff(
    options: Options,
    state: RollbackState,
    gated: GatedWorker | None,
) -> RollbackState:
    """Cross the destructive handoff under the preparation's durable authority.

    ``state.supervisor_owned`` freezes the ownership mode chosen during
    preparation. A later supervisor liveness observation may affect whether a
    legacy handoff is still safe, but it must never silently reclassify the
    prepared mission or mix supervisor-owned and gated legacy artifacts.

    Args:
        options: Deployment options.
        state: Prepared pending mission with explicit durable ownership.
        gated: The gated candidate (legacy path), or ``None`` when supervised.

    Returns:
        The live pending rollback state.

    Raises:
        DeployCtlError: If the handoff cannot complete safely.
    """
    if state.supervisor_owned is True:
        return _complete_supervisor_owned_handoff(options, state, gated)
    if state.supervisor_owned is False:
        return _complete_legacy_handoff(options, state, gated)
    if gated is not None:
        _abort_gated_candidate(gated)
    msg = "cannot hand off a deployment with unknown supervisor ownership"
    raise DeployCtlError(msg)


def _durable_previous_worker(
    state: RollbackState,
) -> tuple[bool, WorkerMeta | None]:
    """Resolve a live durable worker before legacy rollback spawns another.

    Args:
        state: Rollback mission.

    Returns:
        A pair of ``(resolved, worker)``. ``resolved`` means durable metadata
        decides the retry: ``worker`` is adopted when exact previous-commit
        authority is proven, while ``None`` means fail closed. An unresolved
        result permits the normal previous-worker restart path.
    """
    try:
        current = read_meta_strict()
    except WorkerMetadataError as exc:
        append_deploy_log(f"legacy rollback cannot trust maintained worker metadata: {exc}")
        return True, None
    if current is None or not worker_alive(current):
        return False, None
    exact_previous = (
        current.state == STATE_RUNNING
        and current.repo == state.repo
        and current.git_commit == state.previous_commit
    )
    if not exact_previous:
        append_deploy_log(
            "legacy rollback found a live maintained worker outside previous-commit authority"
        )
        return True, None
    previous = state.previous_meta
    same_original_identity = (
        current.pid == previous.pid
        and current.pgid == previous.pgid
        and current.sid == previous.sid
        and current.start_time_ticks == previous.start_time_ticks
        and current.token == previous.token
    )
    if state.previous_retiring and same_original_identity:
        return False, None
    return True, current


def _spawn_gated_previous_worker(state: RollbackState, previous: WorkerMeta) -> GatedWorker | None:
    """Spawn one previous-commit worker behind a non-consuming pipe gate.

    Returns:
        The gated worker, or ``None`` if spawning or identity proof fails.
    """
    token = secrets.token_hex(16)
    env = worker_env(token)
    worker_id = env.get("LUBKO_WORKER_ID") or previous.worker_id or socket.gethostname()
    reader, writer = os.pipe()
    try:
        proc = subprocess.Popen(  # ruff: ignore[subprocess-without-shell-equals-true] — sys.executable with known GATED_SHIM_SOURCE script
            [sys.executable, "-c", GATED_SHIM_SOURCE, str(reader), state.uv_path],
            cwd=Path(state.repo),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
            close_fds=True,
            pass_fds=(reader,),
            env=env,
        )
    except OSError:
        os.close(writer)
        return None
    finally:
        os.close(reader)
    identity = _wait_for_identity(proc)
    if identity is None or identity.pgid != proc.pid or identity.sid != proc.pid:
        _close_gate(writer)
        if identity is not None:
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
    return GatedWorker(proc=proc, gate_writer=writer, meta=meta)


def _clear_previous_restart_obligation(state: RollbackState) -> None:
    """Durably clear a proven-dead legacy rollback restart obligation."""
    _write_state(
        replace(
            state,
            previous_restart_meta=None,
            previous_restart_released=False,
        )
    )


def _recover_previous_restart(state: RollbackState) -> tuple[bool, WorkerMeta | None]:
    """Resolve durable rollback restart authority before another spawn.

    Returns:
        Whether durable restart authority resolved the attempt and the worker to adopt, if any.
    """
    meta = state.previous_restart_meta
    if meta is None:
        return False, None
    if (
        not state.previous_restart_released
        and worker_alive(meta)
        and (not stop_worker(meta, state.stop_grace_seconds) or worker_alive(meta))
    ):
        append_deploy_log("legacy rollback could not converge an unreleased previous-worker spawn")
        return True, None
    if not state.previous_restart_released:
        _clear_previous_restart_obligation(state)
        return False, None
    if worker_alive(meta):
        if _wait_for_released_worker(meta) and check_postgres(DEFAULT_POSTGRES_TIMEOUT_SECONDS):
            return True, meta
        if not stop_worker(meta, state.stop_grace_seconds) or worker_alive(meta):
            append_deploy_log(
                "legacy rollback could not converge an unhealthy released previous-worker spawn"
            )
            return True, None
    _clear_previous_restart_obligation(state)
    return False, None


def _spawn_previous_worker(state: RollbackState, previous: WorkerMeta) -> WorkerMeta | None:
    """Spawn, durably anchor, release, and verify one previous-commit worker.

    Returns:
        Verified previous-worker metadata, or ``None`` when restart cannot safely complete.

    Raises:
        DurabilityError: If durable state cannot be persisted.
    """
    gated = _spawn_gated_previous_worker(state, previous)
    if gated is None:
        return None
    prepared = replace(state, previous_restart_meta=gated.meta, previous_restart_released=False)
    try:
        _write_state(prepared)
    except DurabilityError:
        _abort_gated_candidate(gated)
        raise
    try:
        _release_gate(gated.gate_writer)
    except DeployCtlError:
        _abort_gated_candidate(gated)
        _clear_previous_restart_obligation(prepared)
        return None
    released = replace(prepared, previous_restart_released=True)
    try:
        _write_state(released)
    except DurabilityError:
        _abort_gated_candidate(gated)
        raise
    if not _wait_for_released_worker(gated.meta) or not check_postgres(
        DEFAULT_POSTGRES_TIMEOUT_SECONDS
    ):
        _abort_gated_candidate(gated)
        _clear_previous_restart_obligation(released)
        return None
    return gated.meta


def _restart_previous(state: RollbackState) -> WorkerMeta | None:
    """Restore the previous known-good worker process.

    A previous worker that was never told to retire and is still alive is
    reused under its exact recorded identity (old watchdog behavior). Once the
    controller has durably marked ``previous_retiring`` before stopping that
    worker, a momentarily alive process is never trusted: retirement may have
    begun, so the exact old identity is deterministically stopped and awaited
    dead before a fresh previous-commit worker is spawned and verified. A live
    durable worker published by an interrupted prior rollback is adopted only
    when it proves exact previous-commit authority and is not that retiring
    original identity.

    Args:
        state: Rollback mission.

    Returns:
        Restored worker metadata, or ``None`` on failure.
    """
    resolved, current = _durable_previous_worker(state)
    if resolved:
        return current
    restart_resolved, restarted = _recover_previous_restart(state)
    if restart_resolved:
        return restarted
    previous = state.previous_meta
    if not state.previous_retiring and worker_alive(previous):
        return previous
    if worker_alive(previous) and not stop_worker(previous, state.stop_grace_seconds):
        return None
    return _spawn_previous_worker(state, previous)


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
    if state.new_meta is None:
        append_deploy_log("legacy rollback is missing candidate identity metadata")
        return False
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
        return False
    write_meta(restored)
    _write_state(
        replace(
            state,
            status=STATUS_ROLLED_BACK,
            previous_restart_meta=None,
            previous_restart_released=False,
        )
    )
    cli.remove_cli_root(state.commit)
    if cli.reconcile_pointer(state.previous_commit):
        append_deploy_log(f"supervised rollback restored commit {state.previous_commit}")
    else:
        append_deploy_log(
            f"supervised rollback restored commit {state.previous_commit} "
            "but could not restore the maintained CLI pointer"
        )
    try:
        bin_home = lifecycle._resolve_bin_home()  # ruff: ignore[private-member-access]
    except (OSError, ValueError):
        bin_home = None
    if bin_home is not None:
        snapshot_restored = _restore_pre_confirmation_artifacts(bin_home)
        startup_contract.cleanup_staging(bin_home)
        if snapshot_restored:
            _remove_pre_confirmation_artifacts()
    _remove_confirmation_receipt()
    return True


def _finalize_supervised_rollback(state: RollbackState, expected_generation: int) -> RollbackState:
    """Durably archive only a still-current queue-ready rollback target.

    Terminalization is serialized with desired-generation writers. A restart,
    migration, or other newer desired generation that wins before this lock is
    acquired invalidates the older readiness proof even when it names the same
    previous commit. The mission remains pending so the newer obligation can
    converge.

    Returns:
        The terminal rolled-back state.

    Raises:
        DeployCtlError: If the queue-readiness proof was superseded or cannot
            be bound to the current durable supervisor generation.
    """
    with supervise.generation_lock():
        try:
            desired = supervise.read_desired_strict()
        except supervise.DesiredIntentError as exc:
            msg = "cannot roll back while supervisor desired authority is unreadable"
            raise DeployCtlError(msg) from exc
        status = supervise.read_status()
        if not _supervised_terminalization_authority_matches(
            state.previous_commit, expected_generation, desired, status
        ):
            msg = (
                "the supervisor readiness proof was superseded before rollback; "
                "deployment remains pending"
            )
            raise DeployCtlError(msg)
        terminal = replace(state, status=STATUS_ROLLED_BACK)
        _write_state(terminal)
    cli.remove_cli_root(state.commit)
    if cli.reconcile_pointer(state.previous_commit):
        append_deploy_log(f"supervised rollback restored commit {state.previous_commit}")
    else:
        append_deploy_log(
            f"supervised rollback restored commit {state.previous_commit} "
            "but could not restore the maintained CLI pointer"
        )
    try:
        bin_home = lifecycle._resolve_bin_home()  # ruff: ignore[private-member-access]
    except (OSError, ValueError):
        bin_home = None
    if bin_home is not None:
        snapshot_restored = _restore_pre_confirmation_artifacts(bin_home)
        startup_contract.cleanup_staging(bin_home)
        if snapshot_restored:
            _remove_pre_confirmation_artifacts()
    _remove_confirmation_receipt()
    return terminal


def _pre_confirmation_artifacts_path() -> Path:
    """Return the durable path for the pre-confirmation startup-artifact snapshot.

    Returns:
        The snapshot path under the deploy state directory.
    """
    return state_root() / "deploy" / _PRE_CONFIRMATION_ARTIFACTS_NAME


def _snapshot_pre_confirmation_artifacts(bin_home: Path) -> None:
    """Durably preserve the current startup artifacts before confirmation mutates them.

    The snapshot always records all three artifact keys: ``None`` for absent
    artifacts, or a list of integers (0..255) for present artifacts.

    Args:
        bin_home: Directory containing the launcher scripts.
    """
    raw = startup_contract.snapshot_startup_artifacts(bin_home)
    snapshot = dict[str, object](raw.items())
    write_json_durable(_pre_confirmation_artifacts_path(), snapshot)


def _restore_pre_confirmation_artifacts(bin_home: Path) -> bool:
    """Restore startup artifacts from the pre-confirmation snapshot during rollback.

    Returns ``True`` only when every recorded artifact (including launcher
    executable mode) was restored successfully.  If no snapshot exists or
    any restore step fails, returns ``False`` so the caller retains the
    snapshot for later recovery.

    Args:
        bin_home: Directory containing the launcher scripts.

    Returns:
        ``True`` when all artifacts were restored; ``False`` otherwise.
    """
    snapshot = _read_pre_confirmation_snapshot()
    if snapshot is None:
        return False
    try:
        startup_contract.restore_startup_artifacts(snapshot, bin_home)
        append_deploy_log("startup artifacts restored from pre-confirmation snapshot")
    except (DurabilityError, OSError) as exc:
        append_deploy_log(f"warning: could not restore pre-confirmation startup artifacts: {exc}")
        return False
    return _verify_restored_launcher_mode(
        bin_home, had_launcher=snapshot.get("launcher") is not None
    )


_SNAPSHOT_KNOWN_KEYS: Final = frozenset({"contract", "definition", "launcher"})


def _read_pre_confirmation_snapshot() -> dict[str, list[int] | None] | None:
    """Read and decode the pre-confirmation snapshot.

    Requires exactly the three known artifact keys (contract, definition,
    launcher) with no missing or unknown keys.  Missing or extra keys are
    treated as malformed so rollback never interprets them as absence.

    Returns:
        The decoded snapshot, or ``None`` on any failure.
    """
    path = _pre_confirmation_artifacts_path()
    try:
        raw = path.read_text(encoding="utf-8")
    except (FileNotFoundError, OSError):
        return None
    try:
        decoded = json.loads(raw)
    except ValueError:
        return None
    if not isinstance(decoded, dict):
        return None
    if set(decoded.keys()) != _SNAPSHOT_KNOWN_KEYS:
        return None
    snapshot: dict[str, list[int] | None] = {}
    for key in _SNAPSHOT_KNOWN_KEYS:
        value = decoded[key]
        if value is None:
            snapshot[key] = None
        elif isinstance(value, list) and all(
            isinstance(v, int) and not isinstance(v, bool) and 0 <= v <= _MAX_BYTE_VALUE
            for v in value
        ):
            snapshot[key] = value
        else:
            return None
    return snapshot


def _verify_restored_launcher_mode(bin_home: Path, *, had_launcher: bool) -> bool:
    """Check the restored launcher has the required executable mode.

    Args:
        bin_home: Directory containing the launcher scripts.
        had_launcher: Whether the snapshot contained launcher bytes.

    Returns:
        ``True`` when mode is correct or launcher was not in snapshot.
    """
    if not had_launcher:
        return True
    launcher = bin_home / startup_contract.STARTUP_LAUNCHER_NAME
    try:
        mode = launcher.stat().st_mode & 0o777
    except OSError:
        return False
    if mode != startup_contract.STARTUP_LAUNCHER_MODE:
        append_deploy_log(
            f"warning: restored launcher has mode {oct(mode)}, "
            f"expected {oct(startup_contract.STARTUP_LAUNCHER_MODE)}"
        )
        return False
    return True


def _remove_pre_confirmation_artifacts() -> None:
    """Remove the pre-confirmation startup-artifact snapshot after successful confirmation."""
    path = _pre_confirmation_artifacts_path()
    with suppress(DurabilityError, FileNotFoundError, OSError):
        remove_durable(path)


def _confirmation_receipt_path() -> Path:
    """Return the durable path for the startup confirmation receipt."""
    return state_root() / "deploy" / _CONFIRMATION_RECEIPT_NAME


def _write_confirmation_receipt(commit: str, manifest: dict[str, object]) -> None:
    """Durably record the content authority for promoted startup artifacts.

    Stores the commit plus all hash/size fields from the successful staging
    manifest so that repeat confirm can verify active artifacts against this
    durable authority without re-reading the (now-deleted) staging manifest.

    Args:
        commit: The exact commit whose startup artifacts were promoted.
        manifest: The staging manifest that was used for promotion.
    """
    receipt: dict[str, object] = {"commit": commit}
    for key in (
        "contract_hash",
        "contract_size",
        "definition_hash",
        "definition_size",
        "launcher_hash",
        "launcher_size",
    ):
        receipt[key] = manifest.get(key)
    write_json_durable(_confirmation_receipt_path(), receipt)


def _read_confirmation_receipt() -> dict[str, object] | None:
    """Read the durable confirmation receipt, treating corruption as absent.

    Returns:
        The receipt dict, or ``None`` if absent/corrupt.
    """
    path = _confirmation_receipt_path()
    try:
        raw = path.read_text(encoding="utf-8")
    except (FileNotFoundError, OSError):
        return None
    try:
        decoded = json.loads(raw)
    except ValueError:
        return None
    if not isinstance(decoded, dict):
        return None
    commit = decoded.get("commit")
    if not isinstance(commit, str):
        return None
    return decoded


def _remove_confirmation_receipt() -> None:
    """Remove the confirmation receipt during rollback."""
    with suppress(DurabilityError, FileNotFoundError, OSError):
        remove_durable(_confirmation_receipt_path())


def _finalize_post_promotion(commit: str, manifest: dict[str, object], bin_home: Path) -> None:
    """Write the durable content-authority receipt and clean up staging.

    This must be called AFTER promotion succeeds and BEFORE returning
    success to the caller.  The receipt write is durable so a crash after
    this point leaves a recoverable state.  Staging cleanup happens only
    after the receipt is durably written.

    Args:
        commit: The confirmed commit whose artifacts were promoted.
        manifest: The staging manifest used for promotion.
        bin_home: Directory containing the launcher scripts.

    Raises:
        DeployCtlError: If the receipt cannot be durably written.
    """
    try:
        _write_confirmation_receipt(commit, manifest)
    except (DurabilityError, OSError) as exc:
        msg = "could not write startup confirmation receipt"
        raise DeployCtlError(msg) from exc
    startup_contract.cleanup_staging(bin_home)


def _stage_candidate_startup_artifacts(commit: str) -> None:
    """Run the candidate code's startup-contract staging to produce B's artifacts.

    The candidate commit B's own ``lubko-deploy`` entry point is invoked
    directly from B's sealed CLI environment so the staged bytes are generated
    by B's loaded module, not by the current (A) runtime.  This preserves the
    invariant that unconfirmed candidate artifacts never become startup
    authority and that only B's code defines B's startup contract.

    Args:
        commit: Candidate commit hash whose CLI environment to use.

    Raises:
        DeployCtlError: If B's staging command fails.
    """
    cli_root = cli.cli_commit_dir(commit)
    b_deploy_ctl = cli_root / ".venv" / "bin" / "lubko-deploy"
    if not b_deploy_ctl.is_file():
        msg = f"candidate CLI environment for {commit} is incomplete (lubko-deploy missing)"
        raise DeployCtlError(msg)
    try:
        result = subprocess.run(  # ruff: ignore[subprocess-without-shell-equals-true]
            [str(b_deploy_ctl), "startup-contract", "--write-staged"],
            capture_output=True,
            text=True,
            timeout=30.0,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        msg = f"candidate startup-artifact staging timed out for {commit}"
        raise DeployCtlError(msg) from exc
    except OSError as exc:
        msg = f"could not execute candidate startup-artifact staging: {exc}"
        raise DeployCtlError(msg) from exc
    if result.returncode != 0:
        stderr = result.stderr.strip()[-HELPER_ERROR_MAX_CHARS:]
        msg = f"candidate startup-artifact staging failed for {commit}: {stderr}"
        raise DeployCtlError(msg)


def _finalize_supervised_confirmation(
    state: RollbackState, expected_generation: int
) -> RollbackState:
    """Durably archive only a still-current queue-ready supervisor target.

    Terminalization is serialized with desired-generation writers. A restart,
    migration, or other newer desired generation that wins before this lock is
    acquired invalidates the older readiness proof even when it names the same
    commit. The mission remains pending so the newer obligation can converge.

    Returns:
        The terminal confirmed state.

    Raises:
        DeployCtlError: If the queue-readiness proof was superseded or cannot
            be bound to the current durable supervisor generation.
    """
    with supervise.generation_lock():
        try:
            desired = supervise.read_desired_strict()
        except supervise.DesiredIntentError as exc:
            msg = "cannot confirm while supervisor desired authority is unreadable"
            raise DeployCtlError(msg) from exc
        status = supervise.read_status()
        if not _supervised_terminalization_authority_matches(
            state.commit, expected_generation, desired, status
        ):
            msg = (
                "the supervisor readiness proof was superseded before confirmation; "
                "deployment remains pending"
            )
            raise DeployCtlError(msg)
        terminal = replace(state, status=STATUS_CONFIRMED)
        _write_state(terminal)
    try:
        cli.set_current(state.commit)
    except cli.CliError as exc:
        append_deploy_log(f"supervised deployment confirmed but CLI activation failed: {exc}")
    cli.gc_cli_roots((state.commit, state.previous_commit))
    append_deploy_log(f"supervised deployment confirmed commit {state.commit}")
    return terminal


def _rollback_legacy_locked(state: RollbackState) -> bool:
    """Roll back one explicitly legacy-owned pending mission.

    Returns:
        ``True`` only when the legacy candidate is retired and the previous
        checkout/worker are restored.
    """
    if not _retire_candidate_locked(state):
        return False
    return _restore_previous_locked(state)


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
    if state.supervisor_owned is None:
        append_deploy_log(
            "rollback authority is unknown; holding pending mission without inferring ownership"
        )
        return False
    if state.supervisor_owned is False:
        return _rollback_legacy_locked(state)
    if not supervise.supervisor_running():
        append_deploy_log(
            "supervised rollback lost supervisor authority before settlement; "
            "holding pending mission"
        )
        return False
    try:
        rollback_generation = settle_desired(state.previous_commit, state.repo, state.uv_path)
    except DeployCtlError:
        append_deploy_log("supervised rollback could not settle the previous commit")
        finalized = False
    else:
        try:
            _finalize_supervised_rollback(state, rollback_generation)
        except DeployCtlError:
            append_deploy_log("supervised rollback readiness was superseded before terminalization")
            finalized = False
        else:
            finalized = True
    return finalized


def _watchdog_rollback_attempt(lock_timeout_seconds: float) -> bool:
    """Attempt one watchdog rollback under the deploy lock.

    Returns:
        ``True`` when the rollback reached a terminal state.
    """
    try:
        with deploy_lock(lock_timeout_seconds):
            current = _read_state()
            if (
                current is not None
                and current.status == STATUS_PENDING
                and _rollback_locked(current)
            ):
                return True
    except (DeployCtlError, LockTimeoutError):
        pass
    return False


def _watchdog_main(lock_timeout_seconds: float) -> None:
    """Retain rollback authority until the mission reaches a terminal state.

    With a live external supervisor the watchdog delegates rollback policy to
    the canonical pending-mission predicate, so durable authority, deadline,
    and readiness decisions cannot drift from status/confirmation handling.
    Supervisor absence still fails closed for supervisor-owned/unknown missions.

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
        if state.supervisor_owned is False:
            should_rollback = (
                state.new_meta is None
                or time.time() >= state.deadline
                or not worker_alive(state.new_meta)
            )
        elif supervise.supervisor_running():
            # Reuse the canonical mission predicate used by status and
            # confirmation so durable authority and deadline policy cannot
            # drift between observers.
            should_rollback = _pending_mission_rollback_due(state)
        else:
            # Pending supervised missions and missions with unknown lifecycle
            # authority must never enter the legacy direct rollback/worker
            # lifecycle.  If the supervisor is temporarily absent, fail closed
            # and leave the durable mission and desired generation untouched for
            # the next supervisor incarnation.  ``None`` (unknown authority)
            # fails closed identically to ``True``.
            should_rollback = False
        if should_rollback and _watchdog_rollback_attempt(lock_timeout_seconds):
            return
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
    _write_state(replace(state, status=status))


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
        msg = f"unknown supervised deployment status {state.status!r}"
        raise DeployCtlError(msg)
    if state.supervisor_owned is not False:
        if not _pending_mission_rollback_due(state):
            msg = "another supervised checkout is still pending confirmation"
            raise DeployCtlError(msg)
    elif _mission_candidate_alive(state) and time.time() < state.deadline:
        msg = "another supervised checkout is still pending confirmation"
        raise DeployCtlError(msg)
    if not _rollback_locked(state):
        msg = "an unresolved rollback is still pending"
        raise DeployCtlError(msg)


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
        msg = "a live maintained known-good worker is required for safe checkout"
        raise DeployCtlError(msg)
    previous_commit = previous.git_commit
    _require_exact_commit(options.repo, previous_commit, options.git_timeout_seconds)
    _require_exact_commit(options.repo, commit, options.git_timeout_seconds)
    if commit == previous_commit:
        msg = "candidate commit is already the maintained worker commit"
        raise DeployCtlError(msg)
    _require_clean_checkout(options.repo, options.git_timeout_seconds)
    if not _checkout(options.repo, commit, options.git_timeout_seconds, force=False):
        msg = f"could not check out candidate commit {commit}"
        raise DeployCtlError(msg)
    report = run_validation(options.repo, options.uv_path, options.validation_timeout_seconds)
    if not report.ok:
        _restore_previous_prep(options, previous_commit, commit)
        msg = f"candidate validation failed: {report.detail}"
        raise DeployCtlError(msg)
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
        msg = "stable wrapper cannot reach PostgreSQL before handoff"
        raise DeployCtlError(msg)
    # Exact-authority gate: a NEW pending mission may only be created when no
    # mission is already pending and the durable authority is not malformed. An
    # abandoned pending mission was already resolved by _cleanup_pending_locked,
    # so a refusal here is a genuine conflict or fail-closed authority.
    existing = _read_state()
    if not lifecycle_state.authorize_mission_publish(
        _mission_authority_facts(existing.status if existing is not None else None)
    ):
        _restore_previous_prep(options, previous_commit, commit)
        msg = (
            "lifecycle authority refuses a new pending mission: a supervised checkout "
            "is already pending or durable authority is malformed"
        )
        raise DeployCtlError(msg)
    state = RollbackState(
        schema_version=ROLLBACK_SCHEMA_VERSION,
        generation=next_mission_generation(),
        status=STATUS_PENDING,
        commit=commit,
        previous_commit=previous_commit,
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
) -> tuple[GatedWorker | None, WorkerMeta | None]:
    """Produce the candidate identity record for a prepared mission.

    With a live external supervisor the candidate identity is owned by the daemon
    and is not duplicated in rollback state; otherwise deployctl spawns the gated
    candidate (one-time bootstrap / emergency path).

    Args:
        options: Deployment options.
        commit: Exact candidate commit.
        supervised: Whether the external supervisor owns worker processes.

    Returns:
        The ``(gated, new_meta)`` pair.
    """
    if supervised:
        return None, None
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
        msg = "deployment handoff helper exited before reporting an outcome"
        raise DeployCtlError(msg)
    try:
        response = json.loads(raw)
    except ValueError as exc:
        msg = "deployment handoff helper reported an invalid response"
        raise DeployCtlError(msg) from exc
    if not isinstance(response, dict):
        msg = "deployment handoff helper reported a non-object response"
        raise DeployCtlError(msg)
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
        msg = "checkout request requires string field 'commit'"
        raise DeployCtlError(msg)
    job_id, cancelled = _current_queue_job_id()
    if job_id is not None:
        if cancelled:
            msg = "checkout job was cancelled during deployment"
            raise DeployCtlError(msg)
        return _queue_checkout(options, commit, job_id)
    try:
        with deploy_lock(options.lock_timeout_seconds):
            _reconcile_cli(_read_state())
            state = _deploy_locked(options, commit)
            return _candidate_response(state)
    except LockTimeoutError as exc:
        msg = "timed out waiting for the deployment lock"
        raise DeployCtlError(msg) from exc


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
    if (
        desired is not None
        and desired.migration
        and (state is None or state.generation <= desired.generation)
    ):
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


def _confirmation_response(state: RollbackState) -> dict[str, object]:
    """Build the terminal successful confirmation response.

    Args:
        state: Confirmed mission state.

    Returns:
        Protocol response for a confirmed deployment.
    """
    return {"type": "confirm", "ok": True, "commit": state.commit, "confirmed": True}


def _confirmation_rollback_error(state: RollbackState, failure: str) -> str:
    """Return a truthful confirmation failure after requesting rollback.

    Args:
        state: Pending mission whose rollback was requested.
        failure: Human-readable reason confirmation cannot proceed.

    Returns:
        A terminal rollback diagnostic only when rollback actually completed;
        otherwise an explicit fail-closed pending diagnostic.
    """
    if _rollback_locked(state):
        return f"{failure}; deployment was rolled back"
    return f"{failure}; deployment remains pending because rollback could not yet be completed"


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
        msg = "no checkout is pending confirmation"
        raise DeployCtlError(msg)
    if state.status == STATUS_CONFIRMED:
        if request.get("commit") != state.commit:
            msg = "confirmation commit does not match the confirmed commit"
            raise DeployCtlError(msg)
        return state
    if state.status != STATUS_PENDING:
        msg = "no checkout is pending confirmation"
        raise DeployCtlError(msg)
    _require_known_confirmation_ownership(state)
    if _pending_mission_rollback_due(state):
        raise DeployCtlError(_confirmation_rollback_error(state, "confirmation window lapsed"))
    if request.get("commit") != state.commit:
        raise DeployCtlError(
            _confirmation_rollback_error(
                state, "confirmation commit does not match the proposed commit"
            )
        )
    _require_confirmation_authority(state)
    return state


def _authorize_confirmation(state: RollbackState) -> None:
    """Recheck candidate liveness and lifecycle authority before confirmation.

    Raises:
        DeployCtlError: If the candidate has failed or lifecycle authority refuses confirmation.
    """
    if _pending_mission_rollback_due(state):
        raise DeployCtlError(
            _confirmation_rollback_error(state, "candidate failed before confirmation")
        )
    if not lifecycle_state.authorize_mission_confirm(
        _mission_authority_facts(state.status, candidate_ready=True)
    ):
        raise DeployCtlError(
            _confirmation_rollback_error(
                state,
                "lifecycle authority refuses confirmation (candidate not proven ready or "
                "durable authority malformed)",
            )
        )
    _require_confirmation_authority(state)


def _prepare_confirmation_candidate(state: RollbackState, options: Options) -> int | None:
    """Prepare the exact candidate for terminal confirmation.

    Args:
        state: Pending mission being confirmed.
        options: Runtime options for legacy CLI preparation.

    Returns:
        The exact settled supervisor generation, or ``None`` for legacy ownership.

    Raises:
        DeployCtlError: If the legacy CLI environment cannot be prepared.
    """
    _require_known_confirmation_ownership(state)
    if state.supervisor_owned is False:
        try:
            cli.build_cli_root(
                Path(state.repo), state.commit, state.uv_path, options.cli_timeout_seconds
            )
        except cli.CliError as exc:
            msg = _confirmation_rollback_error(
                state, f"confirmed CLI environment could not be prepared: {exc}"
            )
            raise DeployCtlError(msg) from exc
        if state.new_meta is None:
            msg = _confirmation_rollback_error(
                state, "legacy deployment is missing candidate identity metadata"
            )
            raise DeployCtlError(msg)
        write_meta(state.new_meta)
        return None
    if not supervise.supervisor_running():
        msg = _confirmation_rollback_error(
            state, "cannot confirm a supervisor-owned deployment without a live supervisor"
        )
        raise DeployCtlError(msg)
    return settle_desired(state.commit, state.repo, state.uv_path)


def _finalize_confirmation(
    state: RollbackState, expected_generation: int | None = None
) -> RollbackState:
    """Persist terminal confirmation and maintain the CLI pointer.

    Args:
        state: Pending mission whose candidate is prepared.
        expected_generation: Exact supervisor generation established during
            preparation, or ``None`` for explicitly legacy-owned confirmation.

    Returns:
        Terminal confirmed mission state.

    Raises:
        DeployCtlError: If confirmation ownership is lost or authority is superseded.
    """
    _require_known_confirmation_ownership(state)
    if state.supervisor_owned is True:
        if not supervise.supervisor_running():
            msg = "cannot confirm a supervisor-owned deployment without a live supervisor"
            raise DeployCtlError(msg)
        if expected_generation is None:
            msg = "supervised confirmation is missing its settled generation"
            raise DeployCtlError(msg)
        return _finalize_supervised_confirmation(state, expected_generation)
    terminal = replace(state, status=STATUS_CONFIRMED)
    _write_state(terminal)
    try:
        cli.set_current(terminal.commit)
    except cli.CliError as exc:
        append_deploy_log(f"supervised deployment confirmed but CLI activation failed: {exc}")
    cli.gc_cli_roots((terminal.commit, terminal.previous_commit))
    append_deploy_log(f"supervised deployment confirmed commit {terminal.commit}")
    return terminal


def _pre_terminalize(state: RollbackState, options: Options) -> int | None:
    """Execute pre-terminalization steps and return the settled generation.

    Authorizes the confirmation, prepares the candidate, stages B's startup
    artifacts, and writes the staging manifest.

    Args:
        state: Pending mission.
        options: Deployment options.

    Returns:
        The settled supervisor generation for ``_finalize_confirmation``,
        or ``None`` for legacy ownership.

    Raises:
        DeployCtlError: If any pre-terminalization step fails.
    """
    _authorize_confirmation(state)
    expected_generation = _prepare_confirmation_candidate(state, options)
    _stage_candidate_startup_artifacts(state.commit)
    try:
        bin_home = lifecycle._resolve_bin_home()  # ruff: ignore[private-member-access]
    except (OSError, ValueError) as exc:
        msg = f"cannot confirm: startup artifact manifest write failed: {exc}"
        raise DeployCtlError(msg) from exc
    startup_contract.write_staging_manifest(state.commit, bin_home)
    return expected_generation


def _resolve_bin_home_or_fail() -> Path:
    """Resolve the bin home path.

    Returns:
        The resolved bin home path.

    Raises:
        DeployCtlError: If the path cannot be resolved.
    """
    try:
        return lifecycle._resolve_bin_home()  # ruff: ignore[private-member-access]
    except (OSError, ValueError) as exc:
        msg = f"cannot confirm: startup artifact snapshot failed: {exc}"
        raise DeployCtlError(msg) from exc


def _write_snapshot_or_fail(bin_home: Path) -> None:
    """Write the pre-confirmation snapshot.

    Converts both :class:`DurabilityError` (from durable write) and
    :class:`OSError` (from reading existing artifacts) into a clear
    :class:`DeployCtlError` so confirmation fails before terminalization.

    Args:
        bin_home: Directory containing the launcher scripts.

    Raises:
        DeployCtlError: If the snapshot cannot be recorded.
    """
    try:
        _snapshot_pre_confirmation_artifacts(bin_home)
    except (DurabilityError, OSError) as exc:
        msg = "cannot confirm: pre-confirmation startup artifact snapshot could not be recorded"
        raise DeployCtlError(msg) from exc


def _durable_mission_matches(state: RollbackState) -> bool:
    """Check if the durable mission state confirms the same commit.

    Used after a post-terminalization exception to decide whether the durable
    terminal write happened despite the exception.  Only STATUS_CONFIRMED
    with the same commit and compatible ownership is checked — the generation
    is deliberately ignored because ``_finalize_supervised_confirmation``
    may write a *different* generation when a newer desired generation
    supersedes the mission before the terminal lock is acquired.  In that
    case the confirmed deployment is still valid and recovery data must be
    retained.

    Returns:
        ``True`` when the durable state is STATUS_CONFIRMED with the same
        commit and compatible ownership as ``state``.
    """
    try:
        durable = read_rollback_state()
    except DeployCtlError:
        return False
    return (
        durable is not None
        and durable.status == STATUS_CONFIRMED
        and durable.commit == state.commit
        and durable.supervisor_owned == state.supervisor_owned
    )


def _confirm_locked(request: dict[str, object], options: Options) -> dict[str, object]:
    """Confirm one exact pending deployment as a single idempotent primitive.

    Candidate startup artifacts are staged *before* the terminal state write
    by invoking the candidate code's own staging command.  This ensures B's
    startup artifacts are generated by B's loaded module, never by the
    old (A) runtime.  A durable manifest binds the staged bytes to the exact
    commit with content hashes.  Activation happens *after* the terminal
    state write so that unconfirmed candidates never become startup authority.

    A synchronous promotion failure after terminalization surfaces as
    non-success to the caller: the durable confirmed state and staged
    recovery data are preserved, but the response is ``ok: false`` until
    promotion succeeds.  The ``STATUS_CONFIRMED`` fast path retries
    idempotent promotion and fails closed if artifacts still don't match.

    Exception discipline: after any exception, the durable mission state is
    re-read to decide whether terminalization may have happened.  If the
    durable state is still pending, staging + snapshot are cleaned up (B was
    never confirmed).  If the durable state is terminal (STATUS_CONFIRMED),
    staging + snapshot are retained for supervisor or retry promotion — the
    durable terminal write precedes all potentially-raising post-write work
    (CLI activation, GC, logging) so a crash/exception after that write must
    not discard recovery data.

    Returns:
        Protocol response for the confirmed deployment.
    """
    state = _confirmation_state(request)
    if state.status == STATUS_CONFIRMED:
        return _confirmed_idempotent_response(state)
    bin_home = _resolve_bin_home_or_fail()
    _write_snapshot_or_fail(bin_home)
    terminalized = False
    try:
        expected_generation = _pre_terminalize(state, options)
        state = _finalize_confirmation(state, expected_generation)
        terminalized = True
    except BaseException:
        if not terminalized and _durable_mission_matches(state):
            terminalized = True
        if not terminalized:
            startup_contract.cleanup_staging(bin_home)
            _remove_pre_confirmation_artifacts()
        raise
    # Read manifest before promotion (promotion retains it for crash safety).
    manifest = startup_contract.read_staging_manifest()
    promotion_error = startup_contract.promote_staged_artifacts(
        state.commit, state.commit, bin_home
    )
    if promotion_error is not None:
        msg = f"startup artifact promotion failed: {promotion_error}"
        append_deploy_log(msg)
        return {"type": "confirm", "ok": False, "commit": state.commit, "error": msg}
    # Finalize: write receipt then clean staging.
    if manifest is not None:
        try:
            _finalize_post_promotion(state.commit, manifest, bin_home)
        except DeployCtlError as exc:
            msg = f"startup artifact confirmation receipt could not be written: {exc}"
            append_deploy_log(msg)
            return {"type": "confirm", "ok": False, "commit": state.commit, "error": msg}
    _remove_pre_confirmation_artifacts()
    return _confirmation_response(state)


def _confirmed_idempotent_response(state: RollbackState) -> dict[str, object]:
    """Handle the STATUS_CONFIRMED fast path with idempotent promotion.

    When confirmation is retried for an already-terminal mission, the
    startup artifacts may still be stale from a prior crash between
    terminalization and promotion.  This function retries idempotent
    promotion using the retained manifest and staged bytes.

    A durable confirmation receipt records the content authority (commit
    plus hashes/sizes) after successful promotion.  On repeat confirm,
    the receipt is checked first and active artifacts are verified against
    it.  If active bytes have drifted or the receipt is corrupt/wrong-commit,
    a retained valid staging manifest can repair them.

    Fails closed if bin_home cannot be resolved: returning success without
    verifying artifacts would be a false positive.

    Args:
        state: Terminal confirmed mission state.

    Returns:
        Protocol response — success only when artifacts match.
    """
    try:
        bin_home = lifecycle._resolve_bin_home()  # ruff: ignore[private-member-access]
    except (OSError, ValueError) as exc:
        return {
            "type": "confirm",
            "ok": False,
            "commit": state.commit,
            "error": f"startup artifact promotion incomplete: cannot resolve bin home: {exc}",
        }
    # Check durable receipt: verify active artifacts against content authority.
    receipt = _read_confirmation_receipt()
    if receipt is not None and receipt.get("commit") == state.commit:
        error = startup_contract.verify_active_artifacts_match(receipt, bin_home)
        if error is None:
            return _confirmation_response(state)
        # Active artifacts drifted despite receipt — try repair via manifest.
    # No receipt, wrong-commit receipt, or drift — attempt promotion.
    # Read manifest before promotion (promotion retains it for crash safety).
    manifest = startup_contract.read_staging_manifest()
    promotion_error = startup_contract.promote_staged_artifacts(
        state.commit, state.commit, bin_home
    )
    if promotion_error is not None:
        return {
            "type": "confirm",
            "ok": False,
            "commit": state.commit,
            "error": f"startup artifact promotion incomplete: {promotion_error}",
        }
    # Promotion succeeded — finalize: write receipt then clean staging.
    if manifest is not None:
        verify_error = startup_contract.verify_active_artifacts_match(manifest, bin_home)
        if verify_error is not None:
            return {
                "type": "confirm",
                "ok": False,
                "commit": state.commit,
                "error": f"startup artifact verification failed after promotion: {verify_error}",
            }
        try:
            _finalize_post_promotion(state.commit, manifest, bin_home)
        except DeployCtlError as exc:
            return {
                "type": "confirm",
                "ok": False,
                "commit": state.commit,
                "error": f"startup artifact confirmation receipt could not be written: {exc}",
            }
    _remove_pre_confirmation_artifacts()
    return _confirmation_response(state)


def _handle_confirm(options: Options, request: dict[str, object]) -> dict[str, object]:
    """Serialize confirmation with rollback/deadline enforcement.

    Args:
        options: Deployment options.
        request: Decoded request object.

    Returns:
        Protocol response.

    Raises:
        DeployCtlError: If confirmation fails or the lock cannot be acquired.
    """
    try:
        with deploy_lock(options.lock_timeout_seconds):
            return _confirm_locked(request, options)
    except LockTimeoutError as exc:
        msg = "timed out waiting for the deployment lock"
        raise DeployCtlError(msg) from exc


def _enforce_pending_rollback() -> tuple[RollbackState | None, WorkerMeta | None]:
    """Enforce pending rollback under the deploy lock.

    Returns:
        The current deployment state and worker metadata after enforcement.
    """
    state = _read_state()
    if (
        state is not None
        and state.status == STATUS_PENDING
        and _pending_mission_rollback_due(state)
    ):
        _rollback_locked(state)
        state = _read_state()
    meta = read_meta()
    _reconcile_cli(state)
    return state, meta


def _handle_status(options: Options) -> dict[str, object]:
    """Report and lazily enforce supervised-deployment state.

    Args:
        options: Deployment options.

    Returns:
        Protocol status response.

    Raises:
        DeployCtlError: If the lock cannot be acquired.
    """
    try:
        with deploy_lock(options.lock_timeout_seconds):
            state, meta = _enforce_pending_rollback()
    except LockTimeoutError as exc:
        msg = "timed out waiting for the deployment lock"
        raise DeployCtlError(msg) from exc
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
            "phase": "await-confirmation",
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
    msg = "request type must be checkout, confirm, or status"
    raise DeployCtlError(msg)


def _parse_request(text: str) -> dict[str, object]:
    """Decode one JSON protocol request.

    Args:
        text: JSON object text.

    Returns:
        Decoded object.

    Raises:
        DeployCtlError: If the request is not a valid JSON object.
    """
    try:
        request = json.loads(text)
    except ValueError as exc:
        msg = "request is not valid JSON"
        raise DeployCtlError(msg) from exc
    if not isinstance(request, dict):
        msg = "request must be a JSON object"
        raise DeployCtlError(msg)
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


def _build_options(args: argparse.Namespace) -> Options:
    """Build deployment options from parsed CLI arguments.

    Returns:
        Configured deployment options.

    Raises:
        DeployCtlError: If the confirmation window is non-positive.
    """
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
        msg = "confirmation window must be positive"
        raise DeployCtlError(msg)
    return options


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
        options = _build_options(args)
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
