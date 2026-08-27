"""Versioned, repository-owned supervisor startup contract and live topology proof.

The production reliability guarantee depends on ``lubko-supervisor`` being the
container's long-lived process owner, restarted by the init process (Tini)
after every container/host restart. That guarantee is only end-to-end when the
deployment actually starts the supervisor that way; this module makes the
contract an authoritative, versioned artifact owned by the repository and gives
the maintained status surface a deterministic proof of the live process
topology, not merely an inference from worker liveness.

The supported startup contract is::

    tini-static -- lubko-supervisor

Tini (PID 1) launches the supervisor as its direct child; the supervisor in
turn owns the maintained worker as a direct child. The contract is recorded
under ``$XDG_STATE_HOME/lubko/deploy/startup-contract.json`` so an installation
proves which contract version it was built against, and the same version travels
with the code that depends on it.

The live proof walks the exact process tree and rejects unsupported
topologies such as ``tini-static -- sleep infinity``: the supervisor must be a
live ``lubko-supervisor`` directly parented to the Tini init, and the running
worker must be the supervisor's direct child. Nothing here signals by process
name; the topology is proven by exact parent/child identity read from
``/proc``.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Final

from lubko.durable import write_json_durable
from lubko.state import state_root
from lubko.supervise import (
    read_status,
    read_supervisor_pid,
    supervisor_running,
)

CONTRACT_SCHEMA_VERSION: Final = 1

#: Process table entry for the init process (PID 1).
INIT_PID: Final = 1

#: Command-line markers that identify a supported Tini init process.
DEFAULT_INIT_MARKERS: Final = ("tini-static", "tini")

#: Command-line markers that identify the supported supervisor binary.
DEFAULT_SUPERVISOR_MARKERS: Final = ("lubko-supervisor", "lubko.supervisor")

#: Command-line fragments that mark an unsupported placeholder topology the
#: contract must never accept as a supervisor (for example the legacy
#: ``sleep infinity`` child of Tini).
UNSUPPORTED_SUPERVISOR_MARKERS: Final = ("sleep infinity",)

#: Minimum field count of a ``/proc/<pid>/stat`` line we can interpret.
STAT_MIN_FIELDS: Final = 20
#: Index of the process state field in ``/proc/<pid>/stat``.
STAT_STATE_FIELD_INDEX: Final = 0
#: Index of the parent-PID field in ``/proc/<pid>/stat``.
STAT_PPID_FIELD_INDEX: Final = 1
#: Index of the start-time (clock ticks) field in ``/proc/<pid>/stat``.
STAT_STARTTIME_FIELD_INDEX: Final = 19


class StartupContractError(RuntimeError):
    """Raised when a present startup-contract artifact cannot be trusted."""


@dataclass(frozen=True, slots=True)
class StartupContract:
    """Authoritative, versioned supervisor startup contract.

    The contract names what the init process (Tini) must launch, how the
    supervisor is restarted after a container/host restart (the init process
    restarts it on exit), and that the maintained worker becomes a direct child
    of the supervisor. A version change bumps ``schema_version`` so an
    installation that recorded an older contract fails closed instead of
    silently trusting an obsolete startup definition.
    """

    schema_version: int
    init_markers: tuple[str, ...]
    supervisor_markers: tuple[str, ...]
    supervisor_command: tuple[str, ...]
    worker_relationship: str
    restart_authority: str

    def to_dict(self) -> dict[str, object]:
        """Serialize the contract for storage.

        Returns:
            A JSON-serializable mapping.
        """
        return {
            "schema_version": self.schema_version,
            "init_markers": list(self.init_markers),
            "supervisor_markers": list(self.supervisor_markers),
            "supervisor_command": list(self.supervisor_command),
            "worker_relationship": self.worker_relationship,
            "restart_authority": self.restart_authority,
        }

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> StartupContract:
        """Parse a stored contract strictly.

        Args:
            data: Mapping produced by :meth:`to_dict`.

        Returns:
            The parsed contract.

        Raises:
            TypeError: If a required field is missing or malformed.
        """
        schema_version = data.get("schema_version")
        if not isinstance(schema_version, int) or isinstance(schema_version, bool):
            msg = "startup contract is malformed"
            raise TypeError(msg)
        init_markers = _require_str_tuple(data.get("init_markers"), "init_markers")
        supervisor_markers = _require_str_tuple(
            data.get("supervisor_markers"), "supervisor_markers"
        )
        supervisor_command = _require_str_tuple(
            data.get("supervisor_command"), "supervisor_command"
        )
        worker_relationship = data.get("worker_relationship")
        restart_authority = data.get("restart_authority")
        if not isinstance(worker_relationship, str) or not isinstance(restart_authority, str):
            msg = "startup contract is malformed"
            raise TypeError(msg)
        return cls(
            schema_version=schema_version,
            init_markers=init_markers,
            supervisor_markers=supervisor_markers,
            supervisor_command=supervisor_command,
            worker_relationship=worker_relationship,
            restart_authority=restart_authority,
        )


#: The canonical supported startup contract shipped with the code.
CURRENT_CONTRACT: Final = StartupContract(
    schema_version=CONTRACT_SCHEMA_VERSION,
    init_markers=DEFAULT_INIT_MARKERS,
    supervisor_markers=DEFAULT_SUPERVISOR_MARKERS,
    supervisor_command=("lubko-supervisor",),
    worker_relationship="direct-child",
    restart_authority="init-restarts-supervisor-on-exit",
)


@dataclass(frozen=True, slots=True)
class ProcessInfo:
    """Exact, minimal identity of one live process read from ``/proc``."""

    pid: int
    ppid: int
    cmdline: str
    start_time_ticks: int
    zombie: bool


@dataclass(frozen=True, slots=True)
class TopologyProof:
    """Deterministic proof of the live supervisor startup topology.

    ``ok`` is ``True`` only when the init process is a supported Tini, the
    supervisor is that init's live direct child running the supported binary
    (specifically not the ``sleep infinity`` placeholder), and — when a worker
    is running — it is the supervisor's direct child. ``message`` explains the
    first failing link so an unsupported topology is rejected with a clear,
    operator-actionable reason.
    """

    ok: bool
    contract_version: int
    init_pid: int
    init_cmdline: str
    init_is_tini: bool
    supervisor_pid: int
    supervisor_cmdline: str
    supervisor_present: bool
    supervisor_is_contract_binary: bool
    supervisor_under_init: bool
    uses_sleep_placeholder: bool
    worker_pid: int | None
    worker_is_direct_child: bool
    message: str


def contract_path() -> Path:
    """Return the path of the versioned startup-contract artifact.

    Returns:
        The ``startup-contract.json`` path under the deploy state directory.
    """
    return state_root() / "deploy" / "startup-contract.json"


def write_contract(contract: StartupContract = CURRENT_CONTRACT) -> None:
    """Crash-durably publish the current startup contract artifact.

    The artifact is recovery/authority evidence: an installation proves the
    contract version it was built against, so the write must be confirmed
    durable before the version it asserts is treated as active.

    Args:
        contract: Contract to record (defaults to the code's current contract).

    Note:
        Fails closed: the write raises :class:`DurabilityError` from
        :func:`lubko.durable.write_json_durable` when it cannot be confirmed
        durable.
    """
    write_json_durable(contract_path(), contract.to_dict())


def read_contract() -> StartupContract | None:
    """Load the startup-contract artifact, treating corruption as absence.

    Returns:
        The parsed contract, or ``None`` when absent or malformed.
    """
    try:
        return read_contract_strict()
    except StartupContractError:
        return None


def read_contract_strict() -> StartupContract | None:
    """Load the startup-contract artifact, failing closed on untrusted data.

    Returns:
        The parsed contract, or ``None`` only for genuine absence.

    Raises:
        StartupContractError: If a present artifact is unreadable, invalid
            JSON, not an object, malformed, or of an unsupported schema
            version. Callers must fail closed rather than treat this like
            absence.
    """
    path = contract_path()
    try:
        raw = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return None
    except OSError as exc:
        msg = f"cannot read the startup contract: {exc}"
        raise StartupContractError(msg) from exc
    try:
        decoded = json.loads(raw)
    except ValueError as exc:
        msg = "the startup contract is not valid JSON"
        raise StartupContractError(msg) from exc
    if not isinstance(decoded, dict):
        msg = "the startup contract must be an object"
        raise StartupContractError(msg)
    try:
        contract = StartupContract.from_dict(decoded)
    except (ValueError, TypeError) as exc:
        msg = "the startup contract is malformed"
        raise StartupContractError(msg) from exc
    if contract.schema_version != CONTRACT_SCHEMA_VERSION:
        msg = f"unsupported startup contract version {contract.schema_version}"
        raise StartupContractError(msg)
    return contract


def read_process_info(pid: int) -> ProcessInfo | None:
    """Return the exact identity of a live process, or ``None`` if unknown.

    Args:
        pid: Process ID to inspect.

    Returns:
        The process identity, or ``None`` when the process is gone or its
        ``/proc`` entries are unreadable.
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
        ppid = int(fields[STAT_PPID_FIELD_INDEX])
        start_time_ticks = int(fields[STAT_STARTTIME_FIELD_INDEX])
        state = fields[STAT_STATE_FIELD_INDEX].decode("ascii", "replace")
    except (ValueError, UnicodeDecodeError):
        return None
    cmdline = _read_cmdline(pid)
    return ProcessInfo(
        pid=pid,
        ppid=ppid,
        cmdline=cmdline,
        start_time_ticks=start_time_ticks,
        zombie=state in {"Z", "X"},
    )


def evaluate_topology(
    processes: dict[int, ProcessInfo | None],
    *,
    init_pid: int,
    supervisor_pid: int,
    worker_pid: int | None,
    contract: StartupContract = CURRENT_CONTRACT,
) -> TopologyProof:
    """Prove the startup topology from an exact process-table snapshot.

    The snapshot is the single source of truth: this function signals nothing
    and infers nothing from queue state. It proves the init process is a
    supported Tini, the supervisor is that init's live direct child running the
    supported binary (and explicitly not the ``sleep infinity`` placeholder),
    and — when a worker is present — the worker is the supervisor's direct
    child.

    Args:
        processes: Mapping of PID to its exact :class:`ProcessInfo` (or
            ``None`` when the process is gone); must include at least the init
            and supervisor PIDs and any running worker PID.
        init_pid: The init process ID (conventionally 1).
        supervisor_pid: The exact supervisor process ID.
        worker_pid: The running worker process ID, or ``None`` when no worker
            identity is being claimed.
        contract: The contract whose markers define a supported topology.

    Returns:
        The structured topology proof.
    """
    init = processes.get(init_pid)
    supervisor = processes.get(supervisor_pid)
    worker = processes.get(worker_pid) if worker_pid is not None else None

    init_is_tini = (
        init is not None
        and not init.zombie
        and _cmdline_has_any(init.cmdline, contract.init_markers)
    )

    supervisor_present = supervisor is not None and not supervisor.zombie
    supervisor_is_contract_binary = (
        supervisor is not None
        and not supervisor.zombie
        and _cmdline_has_any(supervisor.cmdline, contract.supervisor_markers)
    )
    uses_sleep_placeholder = (
        supervisor is not None
        and not supervisor.zombie
        and _cmdline_has_any(supervisor.cmdline, UNSUPPORTED_SUPERVISOR_MARKERS)
    )
    supervisor_under_init = (
        supervisor is not None
        and not supervisor.zombie
        and supervisor.ppid == init_pid
        and (init is not None and not init.zombie)
    )

    worker_is_direct_child = False
    if worker_pid is not None:
        worker_is_direct_child = (
            worker is not None
            and not worker.zombie
            and worker.ppid == supervisor_pid
            and supervisor_present
        )

    ok = (
        init_is_tini
        and supervisor_under_init
        and supervisor_is_contract_binary
        and not uses_sleep_placeholder
        and (worker_pid is None or worker_is_direct_child)
    )

    message = _topology_message(
        TopologyProof(
            ok=ok,
            contract_version=contract.schema_version,
            init_pid=init_pid,
            init_cmdline="" if init is None else init.cmdline,
            init_is_tini=init_is_tini,
            supervisor_pid=supervisor_pid,
            supervisor_cmdline="" if supervisor is None else supervisor.cmdline,
            supervisor_present=supervisor_present,
            supervisor_is_contract_binary=supervisor_is_contract_binary,
            supervisor_under_init=supervisor_under_init,
            uses_sleep_placeholder=uses_sleep_placeholder,
            worker_pid=worker_pid,
            worker_is_direct_child=worker_is_direct_child,
            message="",
        )
    )

    return TopologyProof(
        ok=ok,
        contract_version=contract.schema_version,
        init_pid=init_pid,
        init_cmdline="" if init is None else init.cmdline,
        init_is_tini=init_is_tini,
        supervisor_pid=supervisor_pid,
        supervisor_cmdline="" if supervisor is None else supervisor.cmdline,
        supervisor_present=supervisor_present,
        supervisor_is_contract_binary=supervisor_is_contract_binary,
        supervisor_under_init=supervisor_under_init,
        uses_sleep_placeholder=uses_sleep_placeholder,
        worker_pid=worker_pid,
        worker_is_direct_child=worker_is_direct_child,
        message=message,
    )


def verify_live_topology(
    contract: StartupContract = CURRENT_CONTRACT,
) -> TopologyProof:
    """Prove the live startup topology from the real process table.

    The supervisor identity is bound to its recorded durable identity (PID and
    start time) so a recycled or replaced process can never satisfy the proof;
    the worker PID comes from the live supervisor status, which is itself bound
    to the same exact supervisor incarnation.

    Args:
        contract: The contract whose markers define a supported topology.

    Returns:
        The structured live topology proof. ``ok`` is ``False`` with a clear
        message when no supervisor is recorded or it is not live.
    """
    recorded = read_supervisor_pid()
    if recorded is None:
        return _unproven(contract, "no supervisor identity is recorded")
    supervisor_pid, _ticks = recorded
    if not supervisor_running():
        return _unproven(contract, f"supervisor pid {supervisor_pid} is not a live supervisor")
    worker_pid: int | None = None
    status = read_status()
    if status is not None and status.child is not None and not status.holding:
        worker_pid = status.child.pid
    processes: dict[int, ProcessInfo | None] = {
        INIT_PID: read_process_info(INIT_PID),
        supervisor_pid: read_process_info(supervisor_pid),
    }
    if worker_pid is not None:
        processes[worker_pid] = read_process_info(worker_pid)
    return evaluate_topology(
        processes,
        init_pid=INIT_PID,
        supervisor_pid=supervisor_pid,
        worker_pid=worker_pid,
        contract=contract,
    )


def _unproven(contract: StartupContract, message: str) -> TopologyProof:
    """Build a failed topology proof with no process evidence.

    Args:
        contract: The contract whose version to record.
        message: The reason the proof could not be established.

    Returns:
        A proof with ``ok=False``.
    """
    return TopologyProof(
        ok=False,
        contract_version=contract.schema_version,
        init_pid=INIT_PID,
        init_cmdline="",
        init_is_tini=False,
        supervisor_pid=0,
        supervisor_cmdline="",
        supervisor_present=False,
        supervisor_is_contract_binary=False,
        supervisor_under_init=False,
        uses_sleep_placeholder=False,
        worker_pid=None,
        worker_is_direct_child=False,
        message=message,
    )


def _cmdline_has_any(cmdline: str, markers: tuple[str, ...]) -> bool:
    """Return whether a command line contains any of the markers.

    Args:
        cmdline: Joined process command line.
        markers: Substrings that identify a process family.

    Returns:
        ``True`` when at least one marker is present.
    """
    return any(marker in cmdline for marker in markers)


def _topology_message(proof: TopologyProof) -> str:
    """Build a human-readable explanation of the topology proof outcome.

    Args:
        proof: The structured topology proof whose fields decide the message.

    Returns:
        A single-line, operator-actionable message.
    """
    if proof.ok:
        if proof.worker_pid is None:
            return "startup contract satisfied (tini -> supervisor; no worker claimed yet)"
        return "startup contract satisfied (tini -> supervisor -> worker direct child)"
    if not proof.init_is_tini:
        reason = (
            "init process (PID 1) is not a supported Tini; the supervisor is not restart-guarded"
        )
    elif not proof.supervisor_present:
        reason = "supervisor process is absent or a zombie; the contract cannot be proven"
    elif proof.uses_sleep_placeholder:
        reason = (
            "unsupported placeholder topology: Tini launched 'sleep infinity' instead of "
            "lubko-supervisor; the worker has no automatic restart authority"
        )
    elif not proof.supervisor_is_contract_binary:
        reason = "process under Tini is not the supported lubko-supervisor binary"
    elif not proof.supervisor_under_init:
        reason = "supervisor is not the direct child of the Tini init process"
    elif proof.worker_pid is not None and not proof.worker_is_direct_child:
        reason = "running worker is not the direct child of the supervisor"
    else:
        reason = "startup contract not satisfied"
    return reason


def _require_str_tuple(value: object, field: str) -> tuple[str, ...]:
    """Coerce a stored contract field into a tuple of strings.

    Args:
        value: Decoded JSON value.
        field: Field name, used only for error context.

    Returns:
        The tuple of strings.

    Raises:
        TypeError: If the value is missing, not a list, or contains a
            non-string element.
    """
    if not isinstance(value, list):
        msg = f"startup contract field {field} is malformed"
        raise TypeError(msg)
    if not all(isinstance(item, str) for item in value):
        msg = f"startup contract field {field} is malformed"
        raise TypeError(msg)
    return tuple(value)


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
