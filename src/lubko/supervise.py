"""Durable supervisor control protocol shared by the daemon and the CLIs.

The external supervisor (``lubko-supervisor``, see :mod:`lubko.supervisor`)
owns the maintained Lubko worker process.  The deployment and lifecycle
commands must be able to hand the worker to that daemon without racing it, so
all coordination travels through small atomic JSON files under
``$XDG_STATE_HOME/lubko/supervisor/``:

- ``desired.json`` — the explicit run intent written by ``lubko-deploy`` when
  it wants the daemon to run the exact confirmed commit. A restart is the same
  run intent issued again with a newer ``generation``. A monotonically
  increasing ``generation`` makes concurrent writers last-writer-wins and lets
  the daemon recognise every new intent exactly once; a newer intent for the
  same commit is a process replacement, never a no-op;
- ``state.json`` — the daemon's own durable record of the generation it has
  applied, the worker child it owns, and its crash-loop backoff state, so a
  supervisor restart reconstructs deterministically without duplicating the
  worker;
- ``status.json`` — a machine-readable observation surface the CLIs poll and
  ``lubko-deploy status`` reports;
- ``supervisor.pid`` — the daemon's own exact identity (PID and start time in
  clock ticks), used by the CLIs only to detect that a daemon is running;
  nothing ever signals it through this file;
- ``.supervisor.lock`` — the process-level ownership lock: an advisory
  ``flock`` held by the daemon for its whole lifetime, so a second concurrent
  ``lubko-supervisor`` fails closed before it can mutate durable state or
  touch worker lifecycle, while a later start always takes ownership after the
  current owner (even a crash-killed one) exits, because the kernel releases
  the lock at process death.

The supervisor never infers intentional shutdown from process names, argv,
timing guesses, or transient queue state: every intentional transition either
flows through ``desired.json`` or through the durable supervised-deployment
state (``worker/rollback.json``) that already records the exact retiring
worker identity.
"""

from __future__ import annotations

import fcntl
import json
import math
import os
import re
import time
from contextlib import contextmanager, suppress
from dataclasses import dataclass, replace
from pathlib import Path
from typing import TYPE_CHECKING, Final

from lubko._exact_signal import open_pidfd as _open_supervisor_pidfd
from lubko._exact_signal import pidfd_send_signal as _pidfd_send_signal
from lubko.durable import remove_durable, write_bytes_durable, write_json_durable
from lubko.state import rollback_state_path, state_root

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

SCHEMA_VERSION: Final = 1

BOOT_ID_PATH: Final = Path("/proc/sys/kernel/random/boot_id")

SUPERVISOR_RUNTIME_COMMIT_RE: Final = re.compile(r"^[0-9a-f]{40}\n\Z")

STAT_MIN_FIELDS: Final = 20
STAT_STARTTIME_FIELD_INDEX: Final = 19
STAT_STATE_FIELD_INDEX: Final = 0

MODE_RUN: Final = "run"
MODE_IDLE: Final = "idle"

INTENT_RUN: Final = "run"
INTENT_RETIRING: Final = "retiring"

#: How long a live worker child must survive before its restart count resets.
DEFAULT_STABLE_WINDOW_SECONDS: Final = 30.0

#: How long the CLIs wait for the daemon to apply a requested generation.
DEFAULT_REQUEST_TIMEOUT_SECONDS: Final = 60.0
REQUEST_POLL_SECONDS: Final = 0.1


@dataclass(frozen=True, slots=True)
class WorkerChild:
    """Exact identity of the worker process the daemon currently owns."""

    pid: int
    pgid: int
    sid: int
    start_time_ticks: int
    token: str
    worker_id: str
    spawned_at: float


@dataclass(frozen=True, slots=True)
class SupervisorDesired:
    """Explicit run intent the CLIs hand to the supervisor daemon.

    The intent always names the exact commit the daemon must run: a
    deployment requests a new commit, a restart requests the already
    confirmed commit again at a newer generation. There is no durable
    stopped intent.

    ``restart`` distinguishes an explicit process replacement from a
    same-commit settlement (confirmation/rollback): only a restart force-
    replaces the running child; durable mission settlement that already has
    the exact commit running only records the newer generation.

    ``migration`` marks a cold lifecycle migration intent (``lubko-deploy
    migrate``). Because the flag travels inside this same atomically written
    intent, publishing the target commit and recording the migration are one
    indivisible durable transition: there is no crash window in which the
    supervisor could run the migrated commit without the completion obligation.
    While the flag is set and no strictly newer mission supersedes it,
    deployment/CLI reconciliation must hold fail-closed on the previous
    authority until the daemon proves the migrated worker queue-ready and
    converges the maintained CLI pointer and deployctl authority onto the
    exact target commit, then clears the flag again.
    """

    schema_version: int
    generation: int
    commit: str
    repo: str
    uv_path: str
    worker_id: str | None
    restart: bool = False
    requested_at: float = 0.0
    migration: bool = False

    def to_dict(self) -> dict[str, object]:
        """Serialize the desired intent for storage.

        Returns:
            A JSON-serializable mapping.
        """
        return {
            "schema_version": self.schema_version,
            "generation": self.generation,
            "commit": self.commit,
            "repo": self.repo,
            "uv_path": self.uv_path,
            "worker_id": self.worker_id,
            "restart": self.restart,
            "requested_at": self.requested_at,
            "migration": self.migration,
        }

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> SupervisorDesired:
        """Parse a stored desired intent strictly.

        Args:
            data: Mapping produced by :meth:`to_dict`.

        Returns:
            The parsed intent.

        Raises:
            ValueError: If required fields are missing or malformed.
            TypeError: If a present ``restart`` or ``migration`` value is not
                a JSON boolean.
        """
        schema_version = _optional_int(data.get("schema_version"))
        generation = _optional_int(data.get("generation"))
        commit = _optional_string(data.get("commit"))
        if schema_version is None or generation is None or commit is None:
            msg = "supervisor desired state is malformed"
            raise ValueError(msg)
        migration = data.get("migration", False)
        # Absence is backward-compatible false, but a present value must be a
        # real JSON boolean: anything else means the authoritative intent file
        # is not trustworthy and parsing must fail closed.
        if not isinstance(migration, bool):
            msg = "supervisor desired state is malformed"
            raise TypeError(msg)
        restart = data.get("restart", False)
        # Same fail-closed contract as ``migration``: absence means false, but
        # a present value that is not a real JSON boolean means the durable
        # intent is untrustworthy and must enter malformed-desired handling.
        if not isinstance(restart, bool):
            msg = "supervisor desired state is malformed"
            raise TypeError(msg)
        try:
            return cls(
                schema_version=schema_version,
                generation=generation,
                commit=commit,
                repo=str(data.get("repo") or ""),
                uv_path=str(data.get("uv_path") or ""),
                worker_id=_optional_string(data.get("worker_id")),
                restart=restart,
                requested_at=_optional_float(data.get("requested_at")) or 0.0,
                migration=migration,
            )
        except (KeyError, TypeError, ValueError) as exc:
            msg = "supervisor desired state is malformed"
            raise ValueError(msg) from exc


@dataclass(frozen=True, slots=True)
class UnresolvedChild:
    """A spawned direct child whose exact session/group identity was never proven.

    This is a fail-closed hold: it records ownership of a live spawned child
    that could not be converged after its identity establishment timed out.
    It deliberately carries **no** process group or session authority: the
    recorded identity never satisfies (and is never trusted for) private
    session/group semantics, so nothing downstream may ever signal a group on
    its behalf. Convergence uses exact single-PID signals guarded by start
    time ticks only.
    """

    pid: int
    start_time_ticks: int | None
    token: str
    spawned_at: float

    def to_dict(self) -> dict[str, object]:
        """Serialize the unresolved child for storage.

        Returns:
            A JSON-serializable mapping.
        """
        return {
            "pid": self.pid,
            "start_time_ticks": self.start_time_ticks,
            "token": self.token,
            "spawned_at": self.spawned_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> UnresolvedChild:
        """Parse one unresolved child strictly.

        Args:
            data: Mapping produced by :meth:`to_dict`.

        Returns:
            The parsed hold.

        Raises:
            ValueError: If a required field is missing or malformed.
        """
        pid = _optional_int(data.get("pid"))
        token = _optional_string(data.get("token"))
        if pid is None or token is None:
            msg = "unresolved child hold is malformed"
            raise ValueError(msg)
        return cls(
            pid=pid,
            start_time_ticks=_optional_int(data.get("start_time_ticks")),
            token=token,
            spawned_at=_optional_float(data.get("spawned_at")) or 0.0,
        )


@dataclass(frozen=True, slots=True)
class SpawningObligation:
    """Durable pre-spawn recovery obligation for a possibly-live worker spawn.

    This is written crash-durably **before** ``Popen`` and upgraded with the
    exact child identity immediately after a successful spawn, so a supervisor
    death at any point after the successful ``Popen`` always leaves behind a
    replacement-blocking record that names the first spawn's fate as
    unresolved. A successor supervisor must positively resolve this
    obligation before any new worker can be authorized; it never resolves by
    process-name matching or broad numeric signalling:

    - ``pid``/``start_time_ticks`` present: resolution checks exact identity
      (live non-zombie process whose start ticks match) and converges by
      pinned single-PID signals only;
    - ``pid`` absent (crash between ``Popen`` and the upgrade write): the
      spawned child carries ``PR_SET_PDEATHSIG=SIGKILL``, so the kernel has
      already killed it when the spawning supervisor died — unless the child
      was spawned in an earlier boot, which is trivially gone;
    - a present-but-malformed obligation survives corruption as a durable
      blocking flag that only operator repair clears.

    ``creator_pid``/``creator_start_ticks`` identify the supervisor process
    that performed the spawn so an obligation can never be misread as
    resolved-or-live by the wrong incarnation.
    """

    token: str
    commit: str
    creator_pid: int
    creator_start_time_ticks: int
    pid: int | None
    start_time_ticks: int | None
    created_at: float
    boot_id: str | None

    def to_dict(self) -> dict[str, object]:
        """Serialize the obligation for storage.

        Returns:
            A JSON-serializable mapping.
        """
        return {
            "token": self.token,
            "commit": self.commit,
            "creator_pid": self.creator_pid,
            "creator_start_time_ticks": self.creator_start_time_ticks,
            "pid": self.pid,
            "start_time_ticks": self.start_time_ticks,
            "created_at": self.created_at,
            "boot_id": self.boot_id,
        }

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> SpawningObligation:
        """Parse one pre-spawn obligation strictly.

        Args:
            data: Mapping produced by :meth:`to_dict`.

        Returns:
            The parsed obligation.

        Raises:
            ValueError: If a required field is missing or malformed.
        """
        token = _optional_string(data.get("token"))
        creator_pid = _optional_int(data.get("creator_pid"))
        creator_ticks = _optional_int(data.get("creator_start_time_ticks"))
        if token is None or creator_pid is None or creator_ticks is None:
            msg = "spawning obligation is malformed"
            raise ValueError(msg)
        return cls(
            token=token,
            commit=_optional_string(data.get("commit")) or "",
            creator_pid=creator_pid,
            creator_start_time_ticks=creator_ticks,
            pid=_optional_int(data.get("pid")),
            start_time_ticks=_optional_int(data.get("start_time_ticks")),
            created_at=_optional_float(data.get("created_at")) or 0.0,
            boot_id=_optional_string(data.get("boot_id")),
        )


@dataclass(frozen=True, slots=True)
class LastExit:
    """Durable record of the most recent worker child exit."""

    returncode: int | None
    at: float


@dataclass(frozen=True, slots=True)
class SupervisorState:
    """Durable state the daemon keeps about itself and its worker child."""

    schema_version: int
    applied_generation: int
    mode: str
    commit: str | None
    child: WorkerChild | None
    unresolved_child: UnresolvedChild | None
    #: A present worker-ownership record or the whole state authority was
    #: malformed/unreadable. This is durable replacement-blocking state.
    ownership_hold_malformed: bool
    #: A present-but-malformed ``unresolved_child`` record was found on disk.
    #: The durable fail-closed hold cannot be forgotten just because its shape
    # is corrupt: this flag keeps the blocking obligation alive so no
    # replacement worker can ever be authorized beside a possibly-live
    # unresolved spawned child. It only ever clears through explicit operator
    # repair of the state file.
    unresolved_hold_malformed: bool
    #: A present pre-spawn recovery obligation: a spawn may have happened and
    #: its fate is not yet positively resolved. Replacement-blocking.
    spawning: SpawningObligation | None
    #: A present-but-malformed ``spawning`` obligation was found on disk. The
    #: blocking obligation survives its own shape corruption and only clears
    #: through explicit operator repair of the state file.
    spawning_hold_malformed: bool
    intent: str
    restart_count: int
    next_attempt_at: float | None
    last_exit: LastExit | None
    last_spawn_at: float | None
    ready: bool
    next_readiness_at: float | None
    boot_id: str | None

    def to_dict(self) -> dict[str, object]:
        """Serialize the daemon state for storage.

        Returns:
            A JSON-serializable mapping.
        """
        return {
            "schema_version": self.schema_version,
            "applied_generation": self.applied_generation,
            "mode": self.mode,
            "commit": self.commit,
            "child": None if self.child is None else _child_to_dict(self.child),
            "unresolved_child": (
                None if self.unresolved_child is None else self.unresolved_child.to_dict()
            ),
            "ownership_hold_malformed": self.ownership_hold_malformed,
            "unresolved_hold_malformed": self.unresolved_hold_malformed,
            "spawning": (None if self.spawning is None else self.spawning.to_dict()),
            "spawning_hold_malformed": self.spawning_hold_malformed,
            "intent": self.intent,
            "restart_count": self.restart_count,
            "next_attempt_at": self.next_attempt_at,
            "last_exit": None
            if self.last_exit is None
            else {
                "returncode": self.last_exit.returncode,
                "at": self.last_exit.at,
            },
            "last_spawn_at": self.last_spawn_at,
            "ready": self.ready,
            "next_readiness_at": self.next_readiness_at,
            "boot_id": self.boot_id,
        }

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> SupervisorState:
        """Parse stored daemon state, tolerating absence but failing closed on shape corruption.

        Args:
            data: Mapping produced by :meth:`to_dict`.

        Returns:
            The parsed state, or a fresh idle state for an empty mapping.
        """
        child, ownership_hold_malformed = _parse_optional_child(data)
        unresolved, unresolved_hold_malformed = _parse_optional_unresolved_child(data)
        spawning, spawning_hold_malformed = _parse_optional_spawning(data)
        generation, generation_malformed = _parse_present_strict(
            data, "applied_generation", _strict_non_negative_int
        )
        if generation_malformed:
            # A present-but-malformed applied generation must never silently
            # degrade to 0: that would let an already-applied restart intent
            # replay and retire a healthy worker. Enter the durable hold.
            ownership_hold_malformed = True
        restart_count, restart_malformed = _parse_present_strict(
            data, "restart_count", _strict_non_negative_int
        )
        if restart_malformed:
            # A present-but-malformed restart count must never silently degrade
            # to 0: that would erase crash history and let reconciliation treat
            # a repeatedly crashing worker as fresh, spawning and restarting
            # without backoff. Enter the durable hold.
            ownership_hold_malformed = True
        next_attempt_at, deadline_malformed = _parse_present_nullable_float(data, "next_attempt_at")
        if deadline_malformed:
            # A present-but-malformed backoff deadline must never silently
            # degrade to absence: that would clear an active crash-loop
            # backoff and let reconciliation restart immediately from a
            # corrupted schedule. Enter the durable hold.
            ownership_hold_malformed = True
        boot_id, boot_malformed = _parse_present_optional_string(data, "boot_id")
        if boot_malformed:
            # A present-but-malformed boot identifier must never silently
            # degrade to absence: cross-boot normalization would then treat
            # the state as written by an unknown prior boot and erase an
            # active crash-loop backoff deadline. Enter the durable hold.
            ownership_hold_malformed = True
        return cls(
            schema_version=_optional_int(data.get("schema_version")) or SCHEMA_VERSION,
            applied_generation=generation or 0,
            mode=_optional_string(data.get("mode")) or MODE_IDLE,
            commit=_optional_string(data.get("commit")),
            child=child,
            unresolved_child=unresolved,
            ownership_hold_malformed=ownership_hold_malformed
            or _strict_safety_hold(data, "ownership_hold_malformed"),
            unresolved_hold_malformed=unresolved_hold_malformed
            or _strict_safety_hold(data, "unresolved_hold_malformed"),
            spawning=spawning,
            spawning_hold_malformed=spawning_hold_malformed
            or _strict_safety_hold(data, "spawning_hold_malformed"),
            intent=_optional_string(data.get("intent")) or INTENT_RUN,
            restart_count=restart_count or 0,
            next_attempt_at=next_attempt_at,
            last_exit=_parse_last_exit(data),
            last_spawn_at=_optional_float(data.get("last_spawn_at")),
            ready=data.get("ready", False) is True,
            next_readiness_at=_optional_float(data.get("next_readiness_at")),
            boot_id=boot_id,
        )


@dataclass(frozen=True, slots=True)
class SupervisorStatus:
    """Machine-readable observation of the running supervisor.

    ``supervisor_start_time_ticks`` is the exact start time of the supervisor
    process in clock ticks, persisted alongside ``supervisor_pid`` so that
    ``read_status()`` can bind the snapshot to the exact current incarnation
    and reject stale, dead, replaced, or PID-reused snapshots.
    """

    schema_version: int
    supervisor_pid: int
    supervisor_start_time_ticks: int
    started_at: float
    applied_generation: int
    mode: str
    commit: str | None
    child: WorkerChild | None
    intent: str
    restart_count: int
    next_attempt_at: float | None
    last_exit: LastExit | None
    mission: str | None
    db_ready: bool | None
    ready: bool | None
    message: str | None
    worker_health: dict[str, object] | None

    def to_dict(self) -> dict[str, object]:
        """Serialize the status for the CLIs and operators.

        Returns:
            A JSON-serializable mapping.
        """
        return {
            "schema_version": self.schema_version,
            "supervisor_pid": self.supervisor_pid,
            "supervisor_start_time_ticks": self.supervisor_start_time_ticks,
            "started_at": self.started_at,
            "applied_generation": self.applied_generation,
            "mode": self.mode,
            "commit": self.commit,
            "child": None if self.child is None else _child_to_dict(self.child),
            "intent": self.intent,
            "restart_count": self.restart_count,
            "next_attempt_at": self.next_attempt_at,
            "last_exit": None
            if self.last_exit is None
            else {
                "returncode": self.last_exit.returncode,
                "at": self.last_exit.at,
            },
            "mission": self.mission,
            "db_ready": self.db_ready,
            "ready": self.ready,
            "message": self.message,
            "worker_health": self.worker_health,
        }

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> SupervisorStatus:
        """Parse a stored status mapping.

        Args:
            data: Mapping produced by :meth:`to_dict`.

        Returns:
            The parsed status.
        """
        child_data = data.get("child")
        child: WorkerChild | None = None
        if isinstance(child_data, dict):
            try:
                child = _child_from_dict(child_data)
            except (TypeError, ValueError, KeyError):
                child = None
        exit_data = data.get("last_exit")
        last_exit: LastExit | None = None
        if isinstance(exit_data, dict):
            try:
                last_exit = LastExit(
                    returncode=_optional_int(exit_data.get("returncode")),
                    at=float(exit_data.get("at") or 0.0),
                )
            except (TypeError, ValueError):
                last_exit = None
        return cls(
            schema_version=_optional_int(data.get("schema_version")) or SCHEMA_VERSION,
            supervisor_pid=_optional_int(data.get("supervisor_pid")) or 0,
            supervisor_start_time_ticks=_optional_int(data.get("supervisor_start_time_ticks")) or 0,
            started_at=_optional_float(data.get("started_at")) or 0.0,
            applied_generation=_optional_int(data.get("applied_generation")) or 0,
            mode=_optional_string(data.get("mode")) or MODE_IDLE,
            commit=_optional_string(data.get("commit")),
            child=child,
            intent=_optional_string(data.get("intent")) or INTENT_RUN,
            restart_count=_optional_int(data.get("restart_count")) or 0,
            next_attempt_at=_optional_float(data.get("next_attempt_at")),
            last_exit=last_exit,
            mission=_optional_string(data.get("mission")),
            db_ready=_optional_bool(data.get("db_ready")),
            ready=_optional_bool(data.get("ready")),
            message=_optional_string(data.get("message")),
            worker_health=_optional_dict(data.get("worker_health")),
        )


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------


def supervisor_dir() -> Path:
    """Return the directory holding external-supervisor state.

    Returns:
        The per-user supervisor state directory.
    """
    return state_root() / "supervisor"


def desired_path() -> Path:
    """Return the path of the durable desired-intent file.

    Returns:
        The ``desired.json`` path.
    """
    return supervisor_dir() / "desired.json"


def state_path() -> Path:
    """Return the path of the daemon's durable state file.

    Returns:
        The ``state.json`` path.
    """
    return supervisor_dir() / "state.json"


def status_path() -> Path:
    """Return the path of the machine-readable status file.

    Returns:
        The ``status.json`` path.
    """
    return supervisor_dir() / "status.json"


def supervisor_pid_path() -> Path:
    """Return the path of the daemon identity file.

    Returns:
        The ``supervisor.pid`` path.
    """
    return supervisor_dir() / "supervisor.pid"


def supervisor_runtime_override_path() -> Path:
    """Return the path of the temporary supervisor-runtime override pointer.

    This is a plain text file containing exactly one 40-hex commit followed
    by a newline.  The stable ``lubko-supervisor`` shell launcher reads it
    to choose which runtime the *supervisor daemon itself* runs from, while
    ``cli/current``, ``desired.json``, and the confirmed worker commit
    remain untouched.

    Returns:
        The ``supervisor-runtime`` path (no extension, plain text).
    """
    return supervisor_dir() / "supervisor-runtime"


def supervisor_log_path() -> Path:
    """Return the stable path of the supervisor's own log.

    Returns:
        The supervisor log path.
    """
    return supervisor_dir() / "supervisor.log"


def clear_migration_flag(generation: int) -> bool:
    """Clear the migration flag of exactly the named generation, idempotently.

    The rewrite happens under the generation lock so it can never clobber or
    be clobbered by a newer concurrent intent: if the current durable intent
    no longer is that exact migration generation (superseded by a newer
    deploy/restart), the flag write is skipped.

    Args:
        generation: The migration generation whose flag should clear.

    Returns:
        ``True`` when this call cleared the flag, ``False`` when the durable
        intent had already moved on.
    """
    with generation_lock():
        current = read_desired()
        if current is None or not current.migration or current.generation != generation:
            return False
        write_desired(replace(current, migration=False))
    return True


@contextmanager
def generation_lock() -> Iterator[None]:
    """Serialize generation allocation and desired-intent writes.

    The generation space is shared by the supervisor applied state, the
    desired run intent, and the durable supervised mission. Allocating and
    writing a generation must be atomic so concurrent restarts/deploys can
    never observe or reuse an equal, reordered, or already-applied generation.
    Lock ordering is always deployment lock first, then this generation lock.

    Yields:
        Nothing while the lock is held.
    """
    path = supervisor_dir() / ".generation.lock"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def supervisor_lock_path() -> Path:
    """Return the path of the process-level supervisor ownership lock.

    Returns:
        The lock file path inside the supervisor state directory.
    """
    return supervisor_dir() / ".supervisor.lock"


def acquire_supervisor_lock() -> int:
    """Acquire the exclusive supervisor ownership lock non-blockingly.

    The advisory flock is the singleton mechanism: at most one process can
    hold the lock at a time and the kernel revokes it automatically whenever
    the owning process exits, whether by graceful stop, crash, or SIGKILL,
    so a later daemon can always take ownership after the current owner dies.
    Because the caller keeps the returned descriptor open for the whole daemon
    lifetime, the lock governs the entire run, not merely a pidfile write: a
    second concurrent start fails closed before it can mutate supervisor state
    or restart worker lifecycles.

    Args:
        None.

    Returns:
        The open file descriptor holding the lock.

    Raises:
        OSError: If another process already holds the lock and
            ``EWOULDBLOCK`` is raised, meaning a live daemon is already
            running.
    """
    path = supervisor_lock_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        os.close(fd)
        raise
    return fd


# ---------------------------------------------------------------------------
# Boot identity
# ---------------------------------------------------------------------------


def current_boot_id() -> str | None:
    """Return an identifier for the current boot, or ``None`` when unknown.

    The kernel exposes a stable per-boot UUID in
    ``/proc/sys/kernel/random/boot_id``; it changes across host/container
    reboot and stays constant across daemon restarts within one boot. When the
    identifier cannot be read the caller must assume the clock domain is
    unverifiable and treat durable monotonic values as stale.

    Returns:
        The boot identifier string, or ``None``.
    """
    try:
        return BOOT_ID_PATH.read_text(encoding="utf-8").strip() or None
    except OSError:
        return None


# ---------------------------------------------------------------------------
# Atomic storage
# ---------------------------------------------------------------------------


def _write_json(path: Path, mapping: dict[str, object]) -> None:
    """Atomically persist one JSON object.

    Args:
        path: Destination path.
        mapping: Object to store.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.tmp")
    temporary.write_text(json.dumps(mapping, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def _read_json(path: Path) -> dict[str, object] | None:
    """Read one JSON object, returning ``None`` for absence and corruption.

    Args:
        path: Source path.

    Returns:
        The decoded object, or ``None``.
    """
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError:
        return None
    try:
        decoded = json.loads(raw)
    except ValueError:
        return None
    return decoded if isinstance(decoded, dict) else None


def write_desired(desired: SupervisorDesired) -> None:
    """Crash-durably persist a desired intent.

    Args:
        desired: Intent to store.

    Note:
        Fails closed: the write raises :class:`DurabilityError` from
        :func:`lubko.durable.write_json_durable` when it cannot be confirmed
        durable, so callers must not advance a dependent action.
    """
    write_json_durable(desired_path(), desired.to_dict())


class DesiredIntentError(RuntimeError):
    """Raised when a present desired intent exists but cannot be trusted."""


def read_desired_strict() -> SupervisorDesired | None:
    """Load the desired intent in one authoritative snapshot read.

    Returns:
        The parsed intent, or ``None`` only for genuine absence.

    Raises:
        DesiredIntentError: If a present intent file is unreadable,
            invalid JSON, not an object, malformed (including a present
            non-boolean ``migration`` value), or of an unsupported schema
            version. Callers must fail closed rather than treat this like
            absence.
    """
    path = desired_path()
    try:
        raw = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return None
    except OSError as exc:
        msg = f"cannot read the supervisor desired intent: {exc}"
        raise DesiredIntentError(msg) from exc
    try:
        decoded = json.loads(raw)
    except ValueError as exc:
        msg = "the supervisor desired intent is not valid JSON"
        raise DesiredIntentError(msg) from exc
    if not isinstance(decoded, dict):
        msg = "the supervisor desired intent must be an object"
        raise DesiredIntentError(msg)
    try:
        desired = SupervisorDesired.from_dict(decoded)
    except (TypeError, ValueError) as exc:
        msg = "the supervisor desired intent is malformed"
        raise DesiredIntentError(msg) from exc
    if desired.schema_version != SCHEMA_VERSION:
        msg = f"unsupported supervisor desired intent version {desired.schema_version}"
        raise DesiredIntentError(msg)
    return desired


def read_desired() -> SupervisorDesired | None:
    """Load the desired intent, treating corruption as absence.

    Intentionally lenient wrapper for callers whose contract is "no usable
    intent means do nothing": corruption never reaches them as data. Callers
    that must distinguish observable corruption from genuine absence should
    use :func:`read_desired_strict`.

    Returns:
        The parsed intent, or ``None`` when absent or malformed.
    """
    try:
        return read_desired_strict()
    except DesiredIntentError:
        return None


def write_state(state: SupervisorState) -> None:
    """Crash-durably persist the daemon's durable state.

    This is recovery authority: the applied generation and mode decide which
    worker the daemon owns after a restart, so the write must be confirmed
    durable before any dependent lifecycle action proceeds.

    Args:
        state: State to store.

    Note:
        Fails closed: the write raises :class:`DurabilityError` from
        :func:`lubko.durable.write_json_durable` when it cannot be confirmed
        durable, so callers must not advance a dependent action.
    """
    write_json_durable(state_path(), state.to_dict())


def read_state() -> SupervisorState:
    """Load the daemon's durable state, preserving holds on authority failure.

    Genuine absence is a fresh idle state. A present state file that cannot be
    read, decoded, shaped, or versioned is instead represented by a durable
    replacement-blocking hold. Rewrites preserve that hold until an operator
    explicitly repairs the state.

    Returns:
        The parsed state.
    """
    path = state_path()
    try:
        raw = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return fresh_state()
    except OSError:
        return replace(fresh_state(), ownership_hold_malformed=True)
    try:
        decoded = json.loads(raw)
    except ValueError:
        return replace(fresh_state(), ownership_hold_malformed=True)
    if not isinstance(decoded, dict):
        return replace(fresh_state(), ownership_hold_malformed=True)
    schema_version = _optional_int(decoded.get("schema_version"))
    if schema_version != SCHEMA_VERSION:
        return replace(fresh_state(), ownership_hold_malformed=True)
    return SupervisorState.from_dict(decoded)


def fresh_state() -> SupervisorState:
    """Return the empty daemon state used before any intent was applied.

    Returns:
        An idle state with no worker child.
    """
    return SupervisorState(
        schema_version=SCHEMA_VERSION,
        applied_generation=0,
        mode=MODE_IDLE,
        commit=None,
        child=None,
        unresolved_child=None,
        ownership_hold_malformed=False,
        unresolved_hold_malformed=False,
        spawning=None,
        spawning_hold_malformed=False,
        intent=INTENT_RUN,
        restart_count=0,
        next_attempt_at=None,
        last_exit=None,
        last_spawn_at=None,
        ready=False,
        next_readiness_at=None,
        boot_id=None,
    )


def write_status(status: SupervisorStatus) -> None:
    """Persist the machine-readable status as a lightweight atomic write.

    ``status.json`` is observation-only health/status: it is never used as
    recovery authority (the live identity binding in ``supervisor.pid`` plus
    ``state.json`` already decide liveness), so it stays a low-overhead atomic
    write and is intentionally *not* routed through the crash-durable primitive.

    Args:
        status: Status to store.
    """
    _write_json(status_path(), status.to_dict())


def read_status() -> SupervisorStatus | None:
    """Load the machine-readable status.

    The status is only returned when its persisted identity matches the current
    supervisor incarnation: the recorded ``supervisor_pid`` and ``started_at``
    must agree with the daemon identity file (``supervisor.pid``), and that
    exact process must be a live, non-zombie ``lubko-supervisor``.  This
    prevents stale, dead, replaced, or PID-reused snapshots from ever
    appearing ready.

    Returns:
        The parsed status, or ``None`` when absent, malformed, or stale.
    """
    data = _read_json(status_path())
    if data is None:
        return None
    status = SupervisorStatus.from_dict(data)
    if status.schema_version != SCHEMA_VERSION:
        return None
    if status.supervisor_pid == 0:
        return None
    if not _status_identity_matches(status):
        return None
    return status


def _status_identity_matches(status: SupervisorStatus) -> bool:
    """Return whether the status identity matches the live supervisor process.

    Three-way binding is required: the status snapshot's own ``supervisor_pid``
    and ``supervisor_start_time_ticks`` must agree with the daemon identity
    file (``supervisor.pid``), and that exact process must be a live, non-zombie
    ``lubko-supervisor``.  When the identity file is missing, stale, the process
    is dead/replaced, or the PID was reused with a different start time, the
    status snapshot is treated as stale.

    A holding pidfd does not keep a numeric PID reserved, so numeric ``/proc``
    observations alone cannot prove a stable identity. The recorded PID is
    pinned with a pidfd and, after numeric start-time/liveness/cmdline checks,
    the pinned original process is re-proven alive via ``pidfd_send_signal(0)``
    immediately before acceptance. If the original exited while the numeric PID
    was reused, the pinned liveness proof fails and status is rejected.

    Args:
        status: Parsed status to validate.

    Returns:
        ``True`` when the identity is the current live supervisor incarnation.
    """
    recorded = read_supervisor_pid()
    if recorded is None:
        return False
    pid, ticks = recorded
    if pid != status.supervisor_pid or status.supervisor_start_time_ticks != ticks or ticks == 0:
        return False
    try:
        pidfd = _open_supervisor_pidfd(pid)
    except (OSError, AttributeError):
        return False
    try:
        cmdline = _read_cmdline(pid)
        if (
            _process_is_zombie(pid)
            or proc_start_ticks(pid) != ticks
            or ("lubko-supervisor" not in cmdline and "lubko.supervisor" not in cmdline)
        ):
            return False
        try:
            _pidfd_send_signal(pidfd, 0)
        except (OSError, AttributeError):
            return False
        return True
    finally:
        with suppress(OSError):
            os.close(pidfd)


def write_supervisor_pid(pid: int, start_time_ticks: int) -> None:
    """Crash-durably persist the daemon's exact identity for detection by the CLIs.

    The identity file is recovery authority: it is the exact live supervisor
    incarnation that every status/health reader binds against, so the write
    must be confirmed durable.

    Args:
        pid: The daemon's process ID.
        start_time_ticks: The daemon's start time in clock ticks.

    Note:
        Fails closed: the write raises :class:`DurabilityError` from
        :func:`lubko.durable.write_json_durable` when it cannot be confirmed
        durable.
    """
    write_json_durable(
        supervisor_pid_path(),
        {"schema_version": SCHEMA_VERSION, "pid": pid, "start_time_ticks": start_time_ticks},
    )


def read_supervisor_pid() -> tuple[int, int] | None:
    """Load the recorded daemon identity.

    Returns:
        The ``(pid, start_time_ticks)`` pair, or ``None`` when absent.
    """
    data = _read_json(supervisor_pid_path())
    if data is None:
        return None
    pid = _optional_int(data.get("pid"))
    ticks = _optional_int(data.get("start_time_ticks"))
    if pid is None or ticks is None:
        return None
    return pid, ticks


# ---------------------------------------------------------------------------
# Desired-intent helpers for the deployment CLIs
# ---------------------------------------------------------------------------


def _mission_generation() -> int:
    """Return the durable supervised-mission generation, or 0 when absent.

    The mission is deployctl-owned and lives under the worker state; only the
    ``generation`` field is observed here so generation allocation can never
    be outranked by an open mission without forming an import cycle with
    :mod:`lubko.deployctl`. A corrupt, absent, or legacy mission contributes
    0.

    Returns:
        The mission's generation, or ``0`` when none is usable.
    """
    try:
        data = json.loads(rollback_state_path().read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return 0
    if isinstance(data, dict):
        value = data.get("generation")
        if isinstance(value, int) and not isinstance(value, bool):
            return value
        if isinstance(value, str):
            try:
                return int(value)
            except ValueError:
                pass
    return 0


def next_generation() -> int:
    """Return the next generation for a new desired intent.

    The generation is one greater than every generation seen so far: the
    supervisor applied generation, the desired intent, and the durable
    supervised-mission generation. A writer that lost a read-modify-write race
    never reuses a generation the daemon has already applied, and a restart or
    deploy issued against an open mission can never be outranked by that older
    mission.

    Returns:
        The next monotonic generation.
    """
    applied = read_state().applied_generation
    desired = read_desired()
    desired_generation = desired.generation if desired is not None else 0
    return max(applied, desired_generation, _mission_generation()) + 1


def request_run(
    commit: str,
    *,
    repo: str,
    uv_path: str,
    worker_id: str | None,
    restart: bool = False,
) -> int:
    """Request the daemon to run the exact confirmed worker commit.

    Args:
        commit: Exact confirmed commit to run.
        repo: Maintained checkout the commit belongs to.
        uv_path: Resolved ``uv`` executable (recorded for coherence).
        worker_id: Worker identifier to hand to the worker.
        restart: Whether this intent force-replaces a process already running
            ``commit`` (restart) or may merely record the settlement if the
            exact commit is already the live worker (confirmation/rollback).

    Returns:
        The generation of the written intent.
    """
    with generation_lock():
        generation = next_generation()
        write_desired(
            SupervisorDesired(
                schema_version=SCHEMA_VERSION,
                generation=generation,
                commit=commit,
                repo=repo,
                uv_path=uv_path,
                worker_id=worker_id,
                restart=restart,
                requested_at=time.time(),
            )
        )
    return generation


def request_restart(
    commit: str,
    *,
    repo: str,
    uv_path: str,
    worker_id: str | None,
) -> int:
    """Request the daemon to replace the process running ``commit`` with a fresh one.

    A restart is a new run intent for the already confirmed commit at a newer
    generation. The daemon treats a ``restart`` intent for the same commit as
    a process replacement (retire the current child, start a fresh worker from
    the same commit-addressed runtime) rather than a no-op.

    Args:
        commit: Exact commit currently confirmed; the worker must run it again.
        repo: Maintained checkout the commit belongs to.
        uv_path: Resolved ``uv`` executable (recorded for coherence).
        worker_id: Worker identifier to hand to the worker.

    Returns:
        The generation of the written intent.
    """
    return request_run(
        commit,
        repo=repo,
        uv_path=uv_path,
        worker_id=worker_id,
        restart=True,
    )


def wait_for_generation(generation: int, timeout_seconds: float) -> bool:
    """Wait until the daemon reports it applied a generation.

    Args:
        generation: Requested generation.
        timeout_seconds: Maximum seconds to wait.

    Returns:
        ``True`` when the daemon applied the generation (or a later one).
    """
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        status = read_status()
        if status is not None and status.applied_generation >= generation:
            return True
        time.sleep(REQUEST_POLL_SECONDS)
    return False


def wait_until_ready(generation: int, timeout_seconds: float) -> bool:
    """Wait until the daemon proves its worker consumes the queue.

    Readiness is the daemon's own proof that the exact worker child it spawned
    reached the queue-consumer boundary, so waiting here means the replacement
    is genuinely consuming ``lubko.jobs``, not merely alive or PostgreSQL
    reachable.

    Args:
        generation: Requested generation.
        timeout_seconds: Maximum seconds to wait.

    Returns:
        ``True`` when the daemon applied the generation and its worker is
        queue-ready.
    """
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        status = read_status()
        # A ``child=None`` observation is the daemon's ordinary bounded
        # restart-backoff state for the same applied generation, not a
        # terminal failure: keep polling until readiness or this timeout.
        if status is not None and status.applied_generation >= generation and status.ready:
            return True
        time.sleep(REQUEST_POLL_SECONDS)
    return False


# ---------------------------------------------------------------------------
# Supervisor detection
# ---------------------------------------------------------------------------


def proc_start_ticks(pid: int) -> int | None:
    """Return a process start time in clock ticks, or ``None`` if unknown.

    Args:
        pid: Process ID to inspect.

    Returns:
        The start time in clock ticks, or ``None`` when unreadable.
    """
    try:
        stat = (Path("/proc") / str(pid) / "stat").read_bytes()
    except OSError:
        return None
    close_paren = stat.rfind(b")")
    if close_paren == -1:
        return None
    fields = stat[close_paren + 2 :].split()
    if len(fields) < STAT_MIN_FIELDS:
        return None
    try:
        return int(fields[STAT_STARTTIME_FIELD_INDEX])
    except ValueError:
        return None


def _process_is_zombie(pid: int) -> bool:
    """Return whether a process is a zombie or dead.

    Args:
        pid: Process ID to inspect.

    Returns:
        ``True`` when the process is zombie, dead, or unreadable.
    """
    try:
        stat = (Path("/proc") / str(pid) / "stat").read_bytes()
    except OSError:
        return True
    close_paren = stat.rfind(b")")
    if close_paren == -1:
        return True
    fields = stat[close_paren + 2 :].split()
    if not fields:
        return True
    return fields[STAT_STATE_FIELD_INDEX] in {b"Z", b"X"}


def child_alive(child: WorkerChild) -> bool:
    """Return whether the recorded child identity is genuinely alive.

    The exact PID and start time must match a live non-zombie process.  This
    is a lightweight liveness check used by external observers (e.g. the
    deployctl watchdog) that cannot verify the parent relationship.

    Args:
        child: Exact child identity recorded by the supervisor.

    Returns:
        ``True`` when the process is live and its start time matches.
    """
    if _process_is_zombie(child.pid):
        return False
    return proc_start_ticks(child.pid) == child.start_time_ticks


def _read_cmdline(pid: int) -> str:
    """Read the joined command line of a live process.

    Args:
        pid: Process whose command line to inspect.

    Returns:
        The joined command line, or ``""`` when unreadable.
    """
    try:
        raw = (Path("/proc") / str(pid) / "cmdline").read_bytes()
    except OSError:
        return ""
    return " ".join(part.decode("utf-8", "replace") for part in raw.split(b"\0") if part)


def supervisor_running() -> bool:
    """Return whether a live external supervisor daemon is running.

    This is a read-only detection used by the deployment CLIs to decide whether
    to route worker transitions through the daemon.  Nothing ever signals a
    process through this identity.

    Returns:
        ``True`` only when the recorded daemon identity matches a live process
        whose command line names the supervisor.
    """
    recorded = read_supervisor_pid()
    if recorded is None:
        return False
    pid, ticks = recorded
    if ticks == 0:
        return False
    if _process_is_zombie(pid):
        return False
    if proc_start_ticks(pid) != ticks:
        return False
    return "lubko-supervisor" in _read_cmdline(pid) or "lubko.supervisor" in _read_cmdline(pid)


# ---------------------------------------------------------------------------
# Optional value coercion
# ---------------------------------------------------------------------------


def _optional_string(value: object | None) -> str | None:
    """Return a string value or ``None``.

    Args:
        value: JSON value to inspect.

    Returns:
        The string, or ``None``.
    """
    return value if isinstance(value, str) else None


def _optional_int(value: object | None) -> int | None:
    """Return an integer value or ``None``.

    Args:
        value: JSON value to inspect.

    Returns:
        The integer, or ``None``.
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None


def _strict_non_negative_int(value: object) -> int | None:
    """Return a non-negative integer generation, or ``None`` when malformed.

    Args:
        value: JSON value to inspect.

    Returns:
        The non-negative integer, or ``None`` for booleans, non-integers,
        strings, and negative values.
    """
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value


def _strict_finite_float(value: object) -> float | None:
    """Return a finite number, or ``None`` when malformed.

    Args:
        value: JSON value to inspect.

    Returns:
        The finite float, or ``None`` for booleans, non-numeric values,
        strings, NaN, infinities, and integers too large to represent.
    """
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    try:
        result = float(value)
    except OverflowError:
        return None
    if not math.isfinite(result):
        return None
    return result


def _optional_float(value: object | None) -> float | None:
    """Return a float value or ``None``.

    Args:
        value: JSON value to inspect.

    Returns:
        The float, or ``None``.
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def _optional_bool(value: object | None) -> bool | None:
    """Return a boolean value or ``None``.

    Args:
        value: JSON value to inspect.

    Returns:
        The boolean, or ``None``.
    """
    return value if isinstance(value, bool) else None


def _strict_safety_hold(data: dict[str, object], key: str) -> bool:
    """Decode a durable safety bit without allowing malformed data to clear it.

    Args:
        data: Decoded durable state mapping.
        key: Safety-bit field name.

    Returns:
        ``False`` when the field is absent, its boolean value when valid, or
        ``True`` when a present value is not a JSON boolean.
    """
    if key not in data:
        return False
    value = _optional_bool(data[key])
    return value if value is not None else True


def _parse_present_strict(
    data: dict[str, object],
    key: str,
    parser: Callable[[object], int | None],
) -> tuple[int, bool]:
    """Parse a strictly typed field that defaults to zero on genuine absence.

    Args:
        data: Decoded durable state mapping.
        key: Field name.
        parser: Strict parser returning the valid value or ``None``.

    Returns:
        A ``(value, malformed)`` pair: the parsed value (zero for genuine
        absence) and whether a present value was malformed.
    """
    if key not in data:
        return 0, False
    value = parser(data[key])
    return (value or 0), value is None


def _parse_present_optional_string(
    data: dict[str, object],
    key: str,
) -> tuple[str | None, bool]:
    """Parse an optional string identity field.

    Args:
        data: Decoded durable state mapping.
        key: Field name.

    Returns:
        A ``(value, malformed)`` pair: the parsed string (``None`` for
        genuine absence or explicit null) and whether a present non-null
        value was malformed.
    """
    raw = data.get(key)
    if raw is None:
        return None, False
    return (raw, False) if isinstance(raw, str) else (None, True)


def _parse_present_nullable_float(
    data: dict[str, object],
    key: str,
) -> tuple[float | None, bool]:
    """Parse an optional finite-float deadline field.

    Args:
        data: Decoded durable state mapping.
        key: Field name.

    Returns:
        A ``(value, malformed)`` pair: the parsed deadline (``None`` for
        genuine absence or explicit null) and whether a present non-null
        value was malformed.
    """
    raw = data.get(key)
    if raw is None:
        return None, False
    value = _strict_finite_float(raw)
    return value, value is None


def _parse_optional_child(
    data: dict[str, object],
) -> tuple[WorkerChild | None, bool]:
    """Parse the optional worker-ownership record.

    Args:
        data: Decoded durable state mapping.

    Returns:
        The child, or ``None``, plus whether a present record was malformed.
    """
    child_data = data.get("child")
    if child_data is None:
        return None, False
    if isinstance(child_data, dict):
        try:
            return _child_from_dict(child_data), False
        except (TypeError, ValueError, KeyError):
            return None, True
    return None, True


def _parse_optional_unresolved_child(
    data: dict[str, object],
) -> tuple[UnresolvedChild | None, bool]:
    """Parse the optional unresolved-child hold.

    Args:
        data: Decoded durable state mapping.

    Returns:
        The hold, or ``None``, plus whether a present hold was malformed.
    """
    unresolved_data = data.get("unresolved_child")
    if unresolved_data is None:
        return None, False
    if isinstance(unresolved_data, dict):
        try:
            return UnresolvedChild.from_dict(unresolved_data), False
        except (TypeError, ValueError):
            # A present-but-malformed hold must never degrade to absence:
            # the durable blocking obligation survives as a flag that the
            # daemon treats exactly like an unresolvable hold.
            return None, True
    return None, True


def _parse_optional_spawning(
    data: dict[str, object],
) -> tuple[SpawningObligation | None, bool]:
    """Parse the optional pre-spawn recovery obligation.

    Args:
        data: Decoded durable state mapping.

    Returns:
        The obligation, or ``None``, plus whether a present obligation was
        malformed.
    """
    spawning_data = data.get("spawning")
    if spawning_data is None:
        return None, False
    if isinstance(spawning_data, dict):
        try:
            return SpawningObligation.from_dict(spawning_data), False
        except (TypeError, ValueError):
            # A present-but-malformed obligation must never degrade to
            # absence: a possibly live spawned child may exist, so the
            # blocking obligation survives as a flag treated exactly like an
            # unresolvable hold.
            return None, True
    return None, True


def _parse_last_exit(data: dict[str, object]) -> LastExit | None:
    """Parse the optional last-exit record, tolerating shape corruption.

    Args:
        data: Decoded durable state mapping.

    Returns:
        The last exit, or ``None``.
    """
    exit_data = data.get("last_exit")
    if not isinstance(exit_data, dict):
        return None
    try:
        return LastExit(
            returncode=_optional_int(exit_data.get("returncode")),
            at=float(exit_data.get("at") or 0.0),
        )
    except (TypeError, ValueError):
        return None


def _optional_dict(value: object | None) -> dict[str, object] | None:
    """Return a dictionary value or ``None``.

    Args:
        value: JSON value to inspect.

    Returns:
        The dictionary, or ``None``.
    """
    return value if isinstance(value, dict) else None


def _child_to_dict(child: WorkerChild) -> dict[str, object]:
    """Serialize one worker child identity.

    Args:
        child: Identity to serialize.

    Returns:
        A JSON-serializable mapping.
    """
    return {
        "pid": child.pid,
        "pgid": child.pgid,
        "sid": child.sid,
        "start_time_ticks": child.start_time_ticks,
        "token": child.token,
        "worker_id": child.worker_id,
        "spawned_at": child.spawned_at,
    }


def _child_from_dict(data: dict[str, object]) -> WorkerChild:
    """Parse one worker child identity strictly.

    Args:
        data: Mapping produced by :func:`_child_to_dict`.

    Returns:
        The parsed identity.

    Raises:
        ValueError: If a required field is missing or malformed.
    """
    pid = _optional_int(data.get("pid"))
    pgid = _optional_int(data.get("pgid"))
    sid = _optional_int(data.get("sid"))
    ticks = _optional_int(data.get("start_time_ticks"))
    token = _optional_string(data.get("token"))
    worker_id = _optional_string(data.get("worker_id"))
    if pid is None or pgid is None or sid is None or ticks is None:
        msg = "supervisor worker child identity is malformed"
        raise ValueError(msg)
    if token is None or worker_id is None:
        msg = "supervisor worker child identity is malformed"
        raise ValueError(msg)
    return WorkerChild(
        pid=pid,
        pgid=pgid,
        sid=sid,
        start_time_ticks=ticks,
        token=token,
        worker_id=worker_id,
        spawned_at=_optional_float(data.get("spawned_at")) or 0.0,
    )


# ---------------------------------------------------------------------------
# Supervisor-runtime override (plain-text 40-hex commit pointer)
# ---------------------------------------------------------------------------


def read_supervisor_runtime_override() -> str | None:
    """Read the supervisor-runtime override commit, if present and valid.

    The override is a plain text file containing exactly one 40-hex commit
    followed by a newline.  The stable ``lubko-supervisor`` shell launcher
    reads it to choose which runtime the daemon runs from; ``cli/current``
    and ``desired.json`` remain untouched.

    Returns:
        The 40-hex commit, or ``None`` when absent or malformed.
    """
    path = supervisor_runtime_override_path()
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError:
        return None
    if not SUPERVISOR_RUNTIME_COMMIT_RE.fullmatch(raw):
        return None
    return raw[:40]


def write_supervisor_runtime_override(commit: str) -> None:
    """Crash-durably publish the supervisor-runtime override pointer.

    The file contains exactly one 40-hex commit followed by a newline.  It is
    recovery authority: the ``lubko-supervisor`` launcher reads it to choose
    which runtime the daemon starts from, so the write must be confirmed
    durable before the staged runtime is treated as active.

    Args:
        commit: Exact 40-hex commit the supervisor launcher should run.

    Note:
        Fails closed: the write raises :class:`DurabilityError` from
        :func:`lubko.durable.write_bytes_durable` when it cannot be confirmed
        durable.
    """
    write_bytes_durable(supervisor_runtime_override_path(), f"{commit}\n".encode())


def clear_supervisor_runtime_override() -> bool:
    """Crash-durably remove the supervisor-runtime override pointer if present.

    Only regular files are removed: symlinks, directories, and other special
    entries are never silently deleted. The removal is authoritative state
    cleanup, so it is routed through :func:`lubko.durable.remove_durable` to
    fsync the parent directory and fail closed when the removal cannot be
    confirmed.

    Returns:
        ``True`` when the override was present and removed, ``False``
        when it was already absent.

    Note:
        Fails closed: the underlying :func:`lubko.durable.remove_durable`
        raises :class:`DurabilityError` when the removal cannot be confirmed
        durable.
    """
    path = supervisor_runtime_override_path()
    if not path.is_file() or path.is_symlink():
        return False
    remove_durable(path)
    return True
