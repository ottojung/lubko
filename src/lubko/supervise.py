"""Durable supervisor control protocol shared by the daemon and the CLIs.

The external supervisor (``lubko-supervisor``, see :mod:`lubko.supervisor`)
owns the maintained Lubko worker process.  The deployment and lifecycle
commands must be able to hand the worker to that daemon without racing it, so
all coordination travels through small atomic JSON files under
``$XDG_STATE_HOME/lubko/supervisor/``:

- ``desired.json`` — the explicit intent written by ``lubko-deploy`` /
  ``lubko-deploy`` ``stop`` when they want the daemon to run or stop a worker.
  A monotonically increasing ``generation`` makes concurrent writers
  last-writer-wins and lets the daemon recognise every new intent exactly once;
- ``state.json`` — the daemon's own durable record of the generation it has
  applied, the worker child it owns, and its crash-loop backoff state, so a
  supervisor restart reconstructs deterministically without duplicating the
  worker;
- ``status.json`` — a machine-readable observation surface the CLIs poll and
  ``lubko-deploy status`` reports;
- ``supervisor.pid`` — the daemon's own exact identity (PID and start time in
  clock ticks), used by the CLIs only to detect that a daemon is running;
  nothing ever signals it through this file.

The supervisor never infers intentional shutdown from process names, argv,
timing guesses, or transient queue state: every intentional transition either
flows through ``desired.json`` or through the durable supervised-deployment
state (``worker/rollback.json``) that already records the exact retiring
worker identity.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Final

from lubko.state import state_root

SCHEMA_VERSION: Final = 1

STAT_MIN_FIELDS: Final = 20
STAT_STARTTIME_FIELD_INDEX: Final = 19
STAT_STATE_FIELD_INDEX: Final = 0

MODE_RUN: Final = "run"
MODE_STOPPED: Final = "stopped"
MODE_IDLE: Final = "idle"

INTENT_RUN: Final = "run"
INTENT_RETIRING: Final = "retiring"
INTENT_STOPPED: Final = "stopped"

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
    """Explicit intent the CLIs hand to the supervisor daemon."""

    schema_version: int
    generation: int
    mode: str
    commit: str | None
    repo: str
    uv_path: str
    worker_id: str | None
    requested_at: float

    def to_dict(self) -> dict[str, object]:
        """Serialize the desired intent for storage.

        Returns:
            A JSON-serializable mapping.
        """
        return {
            "schema_version": self.schema_version,
            "generation": self.generation,
            "mode": self.mode,
            "commit": self.commit,
            "repo": self.repo,
            "uv_path": self.uv_path,
            "worker_id": self.worker_id,
            "requested_at": self.requested_at,
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
        """
        schema_version = _optional_int(data.get("schema_version"))
        generation = _optional_int(data.get("generation"))
        if schema_version is None or generation is None:
            msg = "supervisor desired state is malformed"
            raise ValueError(msg)
        try:
            return cls(
                schema_version=schema_version,
                generation=generation,
                mode=str(data.get("mode") or ""),
                commit=_optional_string(data.get("commit")),
                repo=str(data.get("repo") or ""),
                uv_path=str(data.get("uv_path") or ""),
                worker_id=_optional_string(data.get("worker_id")),
                requested_at=_optional_float(data.get("requested_at")) or 0.0,
            )
        except (KeyError, TypeError, ValueError) as exc:
            msg = "supervisor desired state is malformed"
            raise ValueError(msg) from exc


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
    intent: str
    restart_count: int
    next_attempt_at: float | None
    last_exit: LastExit | None
    last_spawn_at: float | None
    ready: bool
    next_readiness_at: float | None

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
        }

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> SupervisorState:
        """Parse stored daemon state, tolerating absence but failing closed on shape corruption.

        Args:
            data: Mapping produced by :meth:`to_dict`.

        Returns:
            The parsed state, or a fresh idle state for an empty mapping.
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
            applied_generation=_optional_int(data.get("applied_generation")) or 0,
            mode=_optional_string(data.get("mode")) or MODE_IDLE,
            commit=_optional_string(data.get("commit")),
            child=child,
            intent=_optional_string(data.get("intent")) or INTENT_RUN,
            restart_count=_optional_int(data.get("restart_count")) or 0,
            next_attempt_at=_optional_float(data.get("next_attempt_at")),
            last_exit=last_exit,
            last_spawn_at=_optional_float(data.get("last_spawn_at")),
            ready=data.get("ready", False) is True,
            next_readiness_at=_optional_float(data.get("next_readiness_at")),
        )


@dataclass(frozen=True, slots=True)
class SupervisorStatus:
    """Machine-readable observation of the running supervisor."""

    schema_version: int
    supervisor_pid: int
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

    def to_dict(self) -> dict[str, object]:
        """Serialize the status for the CLIs and operators.

        Returns:
            A JSON-serializable mapping.
        """
        return {
            "schema_version": self.schema_version,
            "supervisor_pid": self.supervisor_pid,
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


def supervisor_log_path() -> Path:
    """Return the stable path of the supervisor's own log.

    Returns:
        The supervisor log path.
    """
    return supervisor_dir() / "supervisor.log"


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
    """Atomically persist a desired intent.

    Args:
        desired: Intent to store.
    """
    _write_json(desired_path(), desired.to_dict())


def read_desired() -> SupervisorDesired | None:
    """Load the desired intent, failing closed on shape corruption.

    Returns:
        The parsed intent, or ``None`` when absent or malformed.
    """
    data = _read_json(desired_path())
    if data is None:
        return None
    try:
        desired = SupervisorDesired.from_dict(data)
    except ValueError:
        return None
    if desired.schema_version != SCHEMA_VERSION:
        return None
    return desired


def write_state(state: SupervisorState) -> None:
    """Atomically persist the daemon's durable state.

    Args:
        state: State to store.
    """
    _write_json(state_path(), state.to_dict())


def read_state() -> SupervisorState:
    """Load the daemon's durable state, defaulting to a fresh idle state.

    A malformed state file fails closed to a fresh state rather than ever
    launching an arbitrary commit from ambiguous metadata.

    Returns:
        The parsed state.
    """
    data = _read_json(state_path())
    if data is None:
        return fresh_state()
    state = SupervisorState.from_dict(data)
    if state.schema_version != SCHEMA_VERSION:
        return fresh_state()
    return state


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
        intent=INTENT_RUN,
        restart_count=0,
        next_attempt_at=None,
        last_exit=None,
        last_spawn_at=None,
        ready=False,
        next_readiness_at=None,
    )


def write_status(status: SupervisorStatus) -> None:
    """Atomically persist the machine-readable status.

    Args:
        status: Status to store.
    """
    _write_json(status_path(), status.to_dict())


def read_status() -> SupervisorStatus | None:
    """Load the machine-readable status.

    Returns:
        The parsed status, or ``None`` when absent or malformed.
    """
    data = _read_json(status_path())
    if data is None:
        return None
    status = SupervisorStatus.from_dict(data)
    if status.schema_version != SCHEMA_VERSION:
        return None
    return status


def write_supervisor_pid(pid: int, start_time_ticks: int) -> None:
    """Persist the daemon's exact identity for detection by the CLIs.

    Args:
        pid: The daemon's process ID.
        start_time_ticks: The daemon's start time in clock ticks.
    """
    _write_json(
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


def next_generation() -> int:
    """Return the next generation for a new desired intent.

    The generation is one greater than every generation seen so far, so a
    writer that lost a read-modify-write race never reuses a generation the
    daemon has already applied.

    Returns:
        The next monotonic generation.
    """
    applied = read_state().applied_generation
    desired = read_desired()
    desired_generation = desired.generation if desired is not None else 0
    return max(applied, desired_generation) + 1


def request_run(
    commit: str,
    *,
    repo: str,
    uv_path: str,
    worker_id: str | None,
) -> int:
    """Request the daemon to run the exact confirmed worker commit.

    Args:
        commit: Exact confirmed commit to run.
        repo: Maintained checkout the commit belongs to.
        uv_path: Resolved ``uv`` executable (recorded for coherence).
        worker_id: Worker identifier to hand to the worker.

    Returns:
        The generation of the written intent.
    """
    generation = next_generation()
    write_desired(
        SupervisorDesired(
            schema_version=SCHEMA_VERSION,
            generation=generation,
            mode=MODE_RUN,
            commit=commit,
            repo=repo,
            uv_path=uv_path,
            worker_id=worker_id,
            requested_at=time.time(),
        )
    )
    return generation


def request_stop() -> int:
    """Request the daemon to stop and hold the maintained worker.

    Returns:
        The generation of the written intent.
    """
    generation = next_generation()
    write_desired(
        SupervisorDesired(
            schema_version=SCHEMA_VERSION,
            generation=generation,
            mode=MODE_STOPPED,
            commit=None,
            repo="",
            uv_path="",
            worker_id=None,
            requested_at=time.time(),
        )
    )
    return generation


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
        if status is not None and status.applied_generation >= generation:
            if status.ready:
                return True
            if status.child is None:
                return False
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
