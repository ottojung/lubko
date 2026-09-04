"""Versioned, repository-owned supervisor startup contract and live topology proof.

The production reliability guarantee depends on ``lubko-supervisor`` being the
container's long-lived process owner, restored after a container or host
restart. That guarantee is only end-to-end when the deployment actually starts
the supervisor that way; this module makes the contract an authoritative,
versioned, repository-owned definition (including a generated, installable
startup launcher) and gives the maintained status surface a deterministic proof
of the live process topology, not merely an inference from worker liveness.

The supported startup definition is::

    tini-static -- lubko-supervisor

Tini is the container init: it launches the supervisor as its direct child and
reaps zombies / forwards signals. The outer host/container environment is trusted
to restart Lubko appropriately; that external setup is intentionally outside this
contract and is neither declared nor inspected by Lubko.

The live proof walks the exact process tree and rejects unsupported topologies
such as ``tini-static -- sleep infinity``: the supervisor must be a live
``lubko-supervisor`` directly parented to the Tini init, and the running worker
must be the supervisor's direct child. The supervisor and worker are each bound
to their exact recorded start-time ticks after every ``/proc`` read, so a PID
reuse after the proof is detected rather than trusted. Nothing here signals by
process name; the topology is proven by exact parent/child identity read from
``/proc``.
"""

from __future__ import annotations

import json
import os
import shlex
from dataclasses import dataclass
from pathlib import Path
from typing import Final

from lubko import config as _config
from lubko.durable import (
    DurabilityError,
    fsync_directory,
    write_bytes_durable,
    write_json_durable,
)
from lubko.state import state_root
from lubko.supervise import (
    MalformedSupervisorIdentityError,
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

#: Name of the generated, versioned startup launcher the container should run.
STARTUP_LAUNCHER_NAME: Final = "lubko-startup"

#: Name of the generated, versioned container/service startup definition.
STARTUP_DEFINITION_NAME: Final = "lubko-startup-definition.json"

#: Schema version of the startup definition artifact.
STARTUP_DEFINITION_SCHEMA_VERSION: Final = 1


#: Required permission mode for the contract's state directories: private to
#: the owner, no group or world access.
DEFAULT_STATE_DIR_MODE: Final = 0o700

#: Mask of permission bits that must never be set on a private state or config
#: path: any group or world access fails closed.
PRIVATE_MODE_MASK: Final = 0o077

#: The private config files the current config subsystem requires. These are
#: derived from :mod:`lubko.config` at module load so the versioned contract
#: records the exact private worker/database config path expectations, and are
#: re-resolved by :func:`private_config_paths` (for validation) so env overrides
#: and monkeypatching behave consistently.
DEFAULT_CONFIG_FILES: Final = (
    str(_config.database_config_path()),
    str(_config.worker_config_path()),
)

#: Required executable mode for the installed startup launcher.
STARTUP_LAUNCHER_MODE: Final = 0o755


def private_config_paths() -> tuple[Path, ...]:
    """Return the private config paths the contract requires to be private.

    The paths are the exact worker/database config locations used by the current
    config subsystem (see :mod:`lubko.config`); the contract validates their
    existence and permission mode without ever reading their contents.

    Returns:
        The resolved private config paths.
    """
    return (_config.database_config_path(), _config.worker_config_path())


class StartupContractError(RuntimeError):
    """Raised when a present startup-contract artifact cannot be trusted."""


@dataclass(frozen=True, slots=True)
class StartupContract:
    """Authoritative, versioned supervisor startup contract.

    The contract is the repository-owned definition of how the container must
    start the supervisor.  It names the exact ``tini-static -- lubko-supervisor``
    command and the state directories
    the deployment must mount with the required permissions.  A version change
    or any semantic difference from :data:`CURRENT_CONTRACT` fails closed: an
    installation that recorded an obsolete or divergent contract is never
    trusted.
    """

    schema_version: int
    init_markers: tuple[str, ...]
    init_command: tuple[str, ...]
    supervisor_markers: tuple[str, ...]
    supervisor_command: tuple[str, ...]
    worker_relationship: str
    required_state_dirs: tuple[str, ...]
    required_config_files: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        """Serialize the contract for storage.

        Returns:
            A JSON-serializable mapping.
        """
        return {
            "schema_version": self.schema_version,
            "init_markers": list(self.init_markers),
            "init_command": list(self.init_command),
            "supervisor_markers": list(self.supervisor_markers),
            "supervisor_command": list(self.supervisor_command),
            "worker_relationship": self.worker_relationship,
            "required_state_dirs": list(self.required_state_dirs),
            "required_config_files": list(self.required_config_files),
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
        init_command = _require_str_tuple(data.get("init_command"), "init_command")
        supervisor_markers = _require_str_tuple(
            data.get("supervisor_markers"), "supervisor_markers"
        )
        supervisor_command = _require_str_tuple(
            data.get("supervisor_command"), "supervisor_command"
        )
        required_state_dirs = _require_str_tuple(
            data.get("required_state_dirs"), "required_state_dirs"
        )
        required_config_files = _require_str_tuple(
            data.get("required_config_files"), "required_config_files"
        )
        worker_relationship = data.get("worker_relationship")
        if not isinstance(worker_relationship, str):
            msg = "startup contract is malformed"
            raise TypeError(msg)
        return cls(
            schema_version=schema_version,
            init_markers=init_markers,
            init_command=init_command,
            supervisor_markers=supervisor_markers,
            supervisor_command=supervisor_command,
            worker_relationship=worker_relationship,
            required_state_dirs=required_state_dirs,
            required_config_files=required_config_files,
        )


#: The canonical supported startup contract shipped with the code.
CURRENT_CONTRACT: Final = StartupContract(
    schema_version=CONTRACT_SCHEMA_VERSION,
    init_markers=DEFAULT_INIT_MARKERS,
    init_command=("tini-static", "--"),
    supervisor_markers=DEFAULT_SUPERVISOR_MARKERS,
    supervisor_command=("lubko-supervisor",),
    worker_relationship="direct-child",
    required_state_dirs=("supervisor", "worker", "deploy"),
    required_config_files=DEFAULT_CONFIG_FILES,
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
class TopologyTargets:
    """The exact processes and recorded identities a topology proof must bind."""

    init_pid: int
    supervisor_pid: int
    worker_pid: int | None
    supervisor_start_ticks: int | None
    worker_start_ticks: int | None


@dataclass(frozen=True, slots=True)
class TopologyProof:
    """Deterministic proof of the live supervisor startup topology.

    ``ok`` is ``True`` only when the init process is a supported Tini, the
    supervisor is that init's live direct child running the supported binary
    (specifically not the ``sleep infinity`` placeholder), the recorded
    supervisor instance is still the exact live process (start ticks match
    after every ``/proc`` read), and — when a worker is running — it is the
    supervisor's direct child whose recorded instance is also still exact.
    ``message`` explains the first failing link so an unsupported topology is
    rejected with a clear, operator-actionable reason.
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
    supervisor_identity_matches: bool
    uses_sleep_placeholder: bool
    worker_pid: int | None
    worker_is_direct_child: bool
    worker_identity_matches: bool
    message: str


@dataclass(frozen=True, slots=True)
class ContractAssessment:
    """Distinct classification of the recorded startup contract."""

    state: str
    contract: StartupContract | None
    message: str


@dataclass(frozen=True, slots=True)
class ContractPathValidation:
    """Validation of the contract's required state directories."""

    ok: bool
    missing: tuple[str, ...]
    mode_mismatched: tuple[str, ...]
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
    contract version and exact definition it was built against, so the write
    must be confirmed durable before the definition it asserts is treated as
    active.

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
    raw_version = decoded.get("schema_version")
    if not isinstance(raw_version, int) or isinstance(raw_version, bool):
        msg = "the startup contract is malformed"
        raise StartupContractError(msg)
    if raw_version != CONTRACT_SCHEMA_VERSION:
        msg = f"unsupported startup contract version {raw_version}"
        raise StartupContractError(msg)
    try:
        contract = StartupContract.from_dict(decoded)
    except (ValueError, TypeError) as exc:
        msg = "the startup contract is malformed"
        raise StartupContractError(msg) from exc
    return contract


def assess_recorded_contract() -> ContractAssessment:
    """Classify the recorded startup contract for status and verification.

    The states are mutually exclusive and surfaced distinctly: ``missing`` (no
    artifact at all), ``corrupt`` (unreadable/malformed/unsupported-version),
    ``mismatch`` (present and parseable but not exactly equal to
    :data:`CURRENT_CONTRACT`), and ``current`` (exactly equal).

    Returns:
        The contract assessment.
    """
    try:
        contract = read_contract_strict()
    except StartupContractError as exc:
        return ContractAssessment("corrupt", None, str(exc))
    if contract is None:
        return ContractAssessment("missing", None, "no startup contract is recorded")
    if contract == CURRENT_CONTRACT:
        return ContractAssessment(
            "current", contract, "recorded startup contract matches the code version exactly"
        )
    return ContractAssessment(
        "mismatch",
        contract,
        "recorded startup contract differs from the code version (semantic mismatch)",
    )


def contract_matches_current(contract: StartupContract) -> bool:
    """Return whether a contract is exactly equal to the code's current contract.

    Args:
        contract: Parsed contract to compare.

    Returns:
        ``True`` only when every field equals :data:`CURRENT_CONTRACT`.
    """
    return contract == CURRENT_CONTRACT


def canonical_startup_command() -> list[str]:
    """Return the exact, versioned container startup command.

    Returns:
        The ``tini-static -- lubko-supervisor`` argv.
    """
    return [*CURRENT_CONTRACT.init_command, *CURRENT_CONTRACT.supervisor_command]


def generate_startup_launcher_content() -> str:
    """Return the versioned startup launcher script source.

    The launcher execs the canonical ``tini-static -- lubko-supervisor`` command
    (resolving ``lubko-supervisor`` via the installed bin launcher), so the
    container entrypoint can be pointed at this single repository-owned file
    instead of ``sleep infinity``.

    Returns:
        The launcher script text.
    """
    command = " ".join(shlex.quote(token) for token in canonical_startup_command())
    return (
        "#!/bin/sh\n"
        "# Generated by lubko; repository-owned versioned startup contract.\n"
        "# Container entrypoint: exec tini-static -- lubko-supervisor.\n"
        f"exec {command}\n"
    )


def write_startup_launcher(bin_home: Path) -> None:
    """Install the versioned startup launcher, verifying the write exactly.

    Args:
        bin_home: Directory containing the launcher scripts.

    Raises:
        OSError: If the directory is missing, the write fails, or the
            installed content does not match the generated source.
    """
    if not bin_home.is_dir():
        msg = f"bin directory {bin_home} does not exist"
        raise OSError(msg)
    target = bin_home / STARTUP_LAUNCHER_NAME
    expected = generate_startup_launcher_content().encode("utf-8")
    # Crash-durable, atomic install: write the bytes (temp + fsync + rename +
    # directory fsync) via the repository durable machinery, then durably
    # establish the executable mode so the installed launcher is confirmed active
    # before the deployment records success.
    write_bytes_durable(target, expected)
    Path(target).chmod(STARTUP_LAUNCHER_MODE)
    _fsync_file(target)
    fsync_directory(bin_home)
    if target.read_bytes() != expected:
        msg = f"startup launcher content mismatch after installation: {target}"
        raise OSError(msg)


def _fsync_file(path: Path) -> None:
    """Fsync a file's metadata and data so a mode change is durable.

    Args:
        path: File to fsync.
    """
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def validate_startup_launcher(bin_home: Path) -> bool:
    """Return whether the installed startup launcher matches the versioned source.

    Args:
        bin_home: Directory containing the launcher scripts.

    Returns:
        ``True`` when the launcher exists, is executable, and its content
        equals the generated source for :data:`CURRENT_CONTRACT`.
    """
    target = bin_home / STARTUP_LAUNCHER_NAME
    if not target.is_file():
        return False
    if not os.access(target, os.X_OK):
        return False
    return target.read_bytes() == generate_startup_launcher_content().encode("utf-8")


def validate_contract_paths(contract: StartupContract = CURRENT_CONTRACT) -> ContractPathValidation:
    """Validate the contract's required state directories exist with right mode.

    Args:
        contract: Contract whose required directories to validate.

    Returns:
        The path validation result.
    """
    root = state_root()
    missing: list[str] = []
    mode_mismatched: list[str] = []
    for relative in contract.required_state_dirs:
        directory = root / relative
        if not directory.is_dir():
            missing.append(relative)
            continue
        try:
            mode = directory.stat().st_mode & 0o777
        except OSError:
            missing.append(relative)
            continue
        if mode != DEFAULT_STATE_DIR_MODE:
            mode_mismatched.append(relative)
    if missing or mode_mismatched:
        detail = ""
        if missing:
            detail += f"missing state directories: {', '.join(missing)}; "
        if mode_mismatched:
            detail += f"wrong permission state directories: {', '.join(mode_mismatched)}"
        return ContractPathValidation(
            ok=False,
            missing=tuple(missing),
            mode_mismatched=tuple(mode_mismatched),
            message=detail.strip(),
        )
    return ContractPathValidation(
        ok=True,
        missing=(),
        mode_mismatched=(),
        message="all required state directories are present with the required permissions",
    )


def create_contract_state_dirs(contract: StartupContract = CURRENT_CONTRACT) -> None:
    """Create the contract's required state directories with secure permissions.

    A fresh install or bootstrap has not yet created the private state mounts
    the contract requires, so they must exist (with the contract-required mode)
    before the deployment seams are validated; otherwise a supported first
    install would fail closed on directories it has not had a chance to create.

    Args:
        contract: Contract whose required directories to create.
    """
    root = state_root()
    for relative in contract.required_state_dirs:
        directory = root / relative
        directory.mkdir(mode=DEFAULT_STATE_DIR_MODE, parents=True, exist_ok=True)
        # The deploy state directory may already exist (created under the umask
        # by the durable writes of the contract/definition artifacts), so enforce
        # the exact required mode explicitly rather than relying on mkdir.
        directory.chmod(DEFAULT_STATE_DIR_MODE)


def validate_contract_config() -> ContractPathValidation:
    """Validate the contract's private config file expectations.

    The contract records the private worker/database config paths used by the
    current config subsystem (see :mod:`lubko.config`). Each must exist as a
    regular file and carry no group or world access bits; the check reads only
    ``stat`` metadata and never reveals the file contents.

    Returns:
        The config path validation result.
    """
    missing: list[str] = []
    mode_mismatched: list[str] = []
    for path in private_config_paths():
        if not path.is_file():
            missing.append(str(path))
            continue
        try:
            mode = path.stat().st_mode & 0o777
        except OSError:
            missing.append(str(path))
            continue
        if mode & PRIVATE_MODE_MASK != 0:
            mode_mismatched.append(str(path))
    if missing or mode_mismatched:
        detail = ""
        if missing:
            detail += f"missing private config files: {', '.join(missing)}; "
        if mode_mismatched:
            detail += f"world/group-readable config files: {', '.join(mode_mismatched)}"
        return ContractPathValidation(
            ok=False,
            missing=tuple(missing),
            mode_mismatched=tuple(mode_mismatched),
            message=detail.strip(),
        )
    return ContractPathValidation(
        ok=True,
        missing=(),
        mode_mismatched=(),
        message="all private config files are present with the required permissions",
    )


def startup_definition_path() -> Path:
    """Return the path of the versioned startup definition artifact.

    Returns:
        The ``lubko-startup-definition.json`` path under the deploy state dir.
    """
    return state_root() / "deploy" / STARTUP_DEFINITION_NAME


def generate_startup_definition() -> dict[str, object]:
    """Return the concrete, repository-owned container/service startup definition.

    The definition is the authoritative, versioned description of how the
    supported deployment must start the supervisor: the exact
    ``tini-static -- lubko-supervisor`` command, required state mounts, and the
    private config path expectations. It is
    consumed by the supported install/bootstrap/deploy path and validated exactly
    by the maintained verifier — unlike a prose instruction, it controls startup.

    Returns:
        A JSON-serializable mapping of the startup definition.
    """
    return {
        "schema_version": STARTUP_DEFINITION_SCHEMA_VERSION,
        "command": list(canonical_startup_command()),
        "required_state_dirs": list(CURRENT_CONTRACT.required_state_dirs),
        "required_config_files": list(CURRENT_CONTRACT.required_config_files),
    }


def write_startup_definition() -> None:
    """Crash-durably publish the current startup definition artifact.

    The definition is deployment authority: the supported install/bootstrap path
    installs it and the verifier requires it to match exactly, so the write must
    be confirmed durable before the definition it asserts is treated as active.
    """
    write_json_durable(startup_definition_path(), generate_startup_definition())


def read_startup_definition() -> dict[str, object] | None:
    """Load the startup definition artifact, treating unreadable data as absence.

    Returns:
        The parsed definition, or ``None`` when absent, unreadable, or invalid.
    """
    try:
        raw = startup_definition_path().read_text(encoding="utf-8")
    except FileNotFoundError:
        return None
    except OSError:
        return None
    try:
        decoded = json.loads(raw)
    except ValueError:
        return None
    if not isinstance(decoded, dict):
        return None
    return decoded


def validate_startup_definition() -> ContractPathValidation:
    """Validate the installed startup definition matches the current contract.

    A missing or divergent definition means the supported deployment path did
    not install the authoritative startup definition (or it drifted), so the
    restart/topology proof must not report the supported topology as active.

    Returns:
        The startup definition validation result.
    """
    recorded = read_startup_definition()
    if recorded is None:
        return ContractPathValidation(
            ok=False,
            missing=(),
            mode_mismatched=(),
            message="startup definition is not installed by the supported deployment path",
        )
    if recorded != generate_startup_definition():
        return ContractPathValidation(
            ok=False,
            missing=(),
            mode_mismatched=(),
            message="installed startup definition does not match the current contract",
        )
    return ContractPathValidation(
        ok=True,
        missing=(),
        mode_mismatched=(),
        message="installed startup definition matches the current contract",
    )


def install_and_validate_startup_definition(bin_home: Path) -> str | None:
    """Install the repository-owned startup launcher and definition; validate seams.

    Combines the launcher install/validation, the concrete startup-definition
    install/validation, and the required state-directory validation into one
    fail-closed step so callers (install/bootstrap) cannot record a successful
    deployment while the repository-owned startup definition or its state mounts
    are missing or have drifted. The deployment remains container-agnostic: it
    installs the authoritative definition this repo owns, but cannot mutate the
    outer container manager. Outer host/service-manager behavior is trusted and
    intentionally outside this verifier.

    Args:
        bin_home: Directory containing the launcher scripts.

    Returns:
        ``None`` on success, or a user-facing error message when the launcher,
        definition, or required state directories are missing or have drifted.
    """
    try:
        write_startup_launcher(bin_home)
    except OSError as exc:
        return f"could not install the startup launcher: {exc}"
    if not validate_startup_launcher(bin_home):
        return "startup launcher is missing or has drifted after install"
    try:
        write_startup_definition()
    except (DurabilityError, OSError) as exc:
        return f"could not install the startup definition: {exc}"
    definition = validate_startup_definition()
    if not definition.ok:
        return f"startup definition is not satisfied: {definition.message}"
    create_contract_state_dirs()
    paths = validate_contract_paths()
    if not paths.ok:
        return f"required startup state directories are not satisfied: {paths.message}"
    return None


def _read_proc_stat(pid: int) -> bytes | None:
    """Read the raw ``/proc/<pid>/stat`` bytes, or ``None`` if unreadable.

    Args:
        pid: Process ID to inspect.

    Returns:
        The raw stat bytes, or ``None`` when the process is gone.
    """
    try:
        return (Path("/proc") / str(pid) / "stat").read_bytes()
    except OSError:
        return None


def _parse_stat_fields(stat: bytes | None) -> tuple[int, int, str] | None:
    """Extract the minimal identity fields from ``/proc/<pid>/stat`` bytes.

    Args:
        stat: Raw ``/proc/<pid>/stat`` bytes, or ``None`` when unreadable.

    Returns:
        ``(ppid, start_time_ticks, state)`` when the line is parseable, else
        ``None``.
    """
    if stat is None:
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
    return ppid, start_time_ticks, state


def read_process_info(pid: int) -> ProcessInfo | None:
    """Return the exact identity of a live process, or ``None`` if unknown.

    The process identity is captured atomically with respect to the command-line
    read: the stat (PID/PPID/start-ticks/state) is read first, the command line
    is read second, and then the stat is re-read. If the second stat diverges
    from the first, the PID was recycled between the two reads, so the identity
    is rejected rather than returned as a spliced (and therefore forgeable)
    combination of an old start tick with a new occupant's command line. This
    closes the splice a recycled PID would otherwise open against the later
    start-tick comparison in :func:`evaluate_topology`.

    Args:
        pid: Process ID to inspect.

    Returns:
        The process identity, or ``None`` when the process is gone, unreadable,
        or its identity changed across the observation.
    """
    stat_first = _read_proc_stat(pid)
    first = _parse_stat_fields(stat_first)
    if first is None:
        return None
    cmdline = _read_cmdline(pid)
    stat_second = _read_proc_stat(pid)
    second = _parse_stat_fields(stat_second)
    if second is None or second != first:
        return None
    ppid, start_time_ticks, state = first
    return ProcessInfo(
        pid=pid,
        ppid=ppid,
        cmdline=cmdline,
        start_time_ticks=start_time_ticks,
        zombie=state in {"Z", "X"},
    )


def evaluate_topology(
    targets: TopologyTargets,
    processes: dict[int, ProcessInfo | None],
    *,
    contract: StartupContract = CURRENT_CONTRACT,
) -> TopologyProof:
    """Prove the startup topology from an exact process-table snapshot.

    The snapshot is the single source of truth: this function signals nothing
    and infers nothing from queue state. It proves the init process is a
    supported Tini, the supervisor is that init's live direct child running the
    supported binary (and explicitly not the ``sleep infinity`` placeholder),
    the recorded supervisor instance is still the exact live process (its start
    ticks match those captured before the ``/proc`` reads), and — when a worker
    is present — the worker is the supervisor's direct child whose recorded
    instance is also still exact.

    Args:
        targets: The exact process IDs and recorded start ticks to bind.
        processes: Mapping of PID to its exact :class:`ProcessInfo` (or
            ``None`` when the process is gone); must include at least the init
            and supervisor PIDs and any running worker PID.
        contract: The contract whose markers define a supported topology.

    Returns:
        The structured topology proof.
    """
    init = processes.get(targets.init_pid)
    supervisor = processes.get(targets.supervisor_pid)
    worker = processes.get(targets.worker_pid) if targets.worker_pid is not None else None

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
        and supervisor.ppid == targets.init_pid
        and (init is not None and not init.zombie)
    )
    supervisor_identity_matches = targets.supervisor_start_ticks is None or (
        supervisor is not None and supervisor.start_time_ticks == targets.supervisor_start_ticks
    )

    worker_is_direct_child = False
    worker_identity_matches = True
    if targets.worker_pid is not None:
        worker_is_direct_child = (
            worker is not None
            and not worker.zombie
            and worker.ppid == targets.supervisor_pid
            and supervisor_present
        )
        worker_identity_matches = targets.worker_start_ticks is None or (
            worker is not None and worker.start_time_ticks == targets.worker_start_ticks
        )

    ok = (
        init_is_tini
        and supervisor_under_init
        and supervisor_is_contract_binary
        and not uses_sleep_placeholder
        and supervisor_identity_matches
        and (targets.worker_pid is None or (worker_is_direct_child and worker_identity_matches))
    )

    message = _topology_message(
        TopologyProof(
            ok=ok,
            contract_version=contract.schema_version,
            init_pid=targets.init_pid,
            init_cmdline="" if init is None else init.cmdline,
            init_is_tini=init_is_tini,
            supervisor_pid=targets.supervisor_pid,
            supervisor_cmdline="" if supervisor is None else supervisor.cmdline,
            supervisor_present=supervisor_present,
            supervisor_is_contract_binary=supervisor_is_contract_binary,
            supervisor_under_init=supervisor_under_init,
            supervisor_identity_matches=supervisor_identity_matches,
            uses_sleep_placeholder=uses_sleep_placeholder,
            worker_pid=targets.worker_pid,
            worker_is_direct_child=worker_is_direct_child,
            worker_identity_matches=worker_identity_matches,
            message="",
        )
    )

    return TopologyProof(
        ok=ok,
        contract_version=contract.schema_version,
        init_pid=targets.init_pid,
        init_cmdline="" if init is None else init.cmdline,
        init_is_tini=init_is_tini,
        supervisor_pid=targets.supervisor_pid,
        supervisor_cmdline="" if supervisor is None else supervisor.cmdline,
        supervisor_present=supervisor_present,
        supervisor_is_contract_binary=supervisor_is_contract_binary,
        supervisor_under_init=supervisor_under_init,
        supervisor_identity_matches=supervisor_identity_matches,
        uses_sleep_placeholder=uses_sleep_placeholder,
        worker_pid=targets.worker_pid,
        worker_is_direct_child=worker_is_direct_child,
        worker_identity_matches=worker_identity_matches,
        message=message,
    )


def verify_live_topology(
    contract: StartupContract = CURRENT_CONTRACT,
) -> TopologyProof:
    """Prove the live startup topology from the real process table.

    The supervisor identity is bound to its recorded durable identity (PID and
    start time) so a recycled or replaced process can never satisfy the proof;
    the worker PID and start ticks come from the live supervisor status, which
    is itself bound to the same exact supervisor incarnation. Every ``/proc``
    read happens before the recorded start ticks are re-compared, so a PID
    reuse after the reads is detected.

    Args:
        contract: The contract whose markers define a supported topology.

    Returns:
        The structured live topology proof. ``ok`` is ``False`` with a clear
        message when no supervisor is recorded or it is not live.
    """
    try:
        recorded = read_supervisor_pid()
    except MalformedSupervisorIdentityError:
        return _unproven(contract, "supervisor identity record is malformed")
    if recorded is None:
        return _unproven(contract, "no supervisor identity is recorded")
    supervisor_pid, supervisor_start_ticks = recorded
    if not supervisor_running():
        return _unproven(contract, f"supervisor pid {supervisor_pid} is not a live supervisor")
    worker_pid: int | None = None
    worker_start_ticks: int | None = None
    status = read_status()
    if status is not None and status.child is not None and not status.holding:
        worker_pid = status.child.pid
        worker_start_ticks = status.child.start_time_ticks
    processes: dict[int, ProcessInfo | None] = {
        INIT_PID: read_process_info(INIT_PID),
        supervisor_pid: read_process_info(supervisor_pid),
    }
    if worker_pid is not None:
        processes[worker_pid] = read_process_info(worker_pid)
    return evaluate_topology(
        TopologyTargets(
            init_pid=INIT_PID,
            supervisor_pid=supervisor_pid,
            worker_pid=worker_pid,
            supervisor_start_ticks=supervisor_start_ticks,
            worker_start_ticks=worker_start_ticks,
        ),
        processes,
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
        supervisor_identity_matches=False,
        uses_sleep_placeholder=False,
        worker_pid=None,
        worker_is_direct_child=False,
        worker_identity_matches=False,
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
    reasons: list[tuple[bool, str]] = [
        (
            not proof.init_is_tini,
            (
                "init process (PID 1) is not a supported Tini; the supervisor has no "
                "reaper/signal-forwarding init"
            ),
        ),
        (
            not proof.supervisor_present,
            "supervisor process is absent or a zombie; the contract cannot be proven",
        ),
        (
            proof.uses_sleep_placeholder,
            (
                "unsupported placeholder topology: Tini launched 'sleep infinity' instead of "
                "lubko-supervisor; the worker has no supported supervisor"
            ),
        ),
        (
            not proof.supervisor_is_contract_binary,
            "process under Tini is not the supported lubko-supervisor binary",
        ),
        (
            not proof.supervisor_under_init,
            "supervisor is not the direct child of the Tini init process",
        ),
        (
            not proof.supervisor_identity_matches,
            "recorded supervisor identity is gone or its PID was reused after the proof",
        ),
        (
            proof.worker_pid is not None and not proof.worker_is_direct_child,
            "running worker is not the direct child of the supervisor",
        ),
        (
            proof.worker_pid is not None and not proof.worker_identity_matches,
            "recorded worker identity is gone or its PID was reused after the proof",
        ),
    ]
    for condition, reason in reasons:
        if condition:
            return reason
    return "startup contract not satisfied"


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


#: Minimum field count of a ``/proc/<pid>/stat`` line we can interpret.
STAT_MIN_FIELDS: Final = 20
#: Index of the process state field in ``/proc/<pid>/stat``.
STAT_STATE_FIELD_INDEX: Final = 0
#: Index of the parent-PID field in ``/proc/<pid>/stat``.
STAT_PPID_FIELD_INDEX: Final = 1
#: Index of the start-time (clock ticks) field in ``/proc/<pid>/stat``.
STAT_STARTTIME_FIELD_INDEX: Final = 19
