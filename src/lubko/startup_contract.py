"""Versioned, repository-owned supervisor startup contract.

The production reliability guarantee depends on ``lubko-supervisor`` being the
container's long-lived process owner, restored after a container or host
restart. That guarantee is only end-to-end when the deployment actually starts
the supervisor that way; this module makes the contract an authoritative,
versioned, repository-owned definition (including a generated, installable
startup launcher).

The supported startup definition is::

    tini-static -- lubko-supervisor

The outer host/container environment is trusted to restart Lubko appropriately;
that external setup is intentionally outside this contract and is neither
declared nor inspected by Lubko.
"""

from __future__ import annotations

import json
import os
import shlex
import shutil
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

CONTRACT_SCHEMA_VERSION: Final = 1

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
    init_command: tuple[str, ...]
    supervisor_command: tuple[str, ...]
    required_state_dirs: tuple[str, ...]
    required_config_files: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        """Serialize the contract for storage.

        Returns:
            A JSON-serializable mapping.
        """
        return {
            "schema_version": self.schema_version,
            "init_command": list(self.init_command),
            "supervisor_command": list(self.supervisor_command),
            "required_state_dirs": list(self.required_state_dirs),
            "required_config_files": list(self.required_config_files),
        }

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> StartupContract:
        """Parse a stored contract strictly.

        Args:
            data: Mapping produced by :meth:`to_dict`. Legacy schema-v1 keys
                (``init_markers``, ``supervisor_markers``,
                ``worker_relationship``) are silently ignored.

        Returns:
            The parsed contract.

        Raises:
            TypeError: If a required field is missing or malformed.
        """
        schema_version = data.get("schema_version")
        if not isinstance(schema_version, int) or isinstance(schema_version, bool):
            msg = "startup contract is malformed"
            raise TypeError(msg)
        init_command = _require_str_tuple(data.get("init_command"), "init_command")
        supervisor_command = _require_str_tuple(
            data.get("supervisor_command"), "supervisor_command"
        )
        required_state_dirs = _require_str_tuple(
            data.get("required_state_dirs"), "required_state_dirs"
        )
        required_config_files = _require_str_tuple(
            data.get("required_config_files"), "required_config_files"
        )
        return cls(
            schema_version=schema_version,
            init_command=init_command,
            supervisor_command=supervisor_command,
            required_state_dirs=required_state_dirs,
            required_config_files=required_config_files,
        )


#: The canonical supported startup contract shipped with the code.
CURRENT_CONTRACT: Final = StartupContract(
    schema_version=CONTRACT_SCHEMA_VERSION,
    init_command=("tini-static", "--"),
    supervisor_command=("lubko-supervisor",),
    required_state_dirs=("supervisor", "worker", "deploy"),
    required_config_files=DEFAULT_CONFIG_FILES,
)


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

    The shebang resolves ``sh`` at generation time for portability across
    GNU Linux and Termux.

    Returns:
        The launcher script text.

    Raises:
        StartupContractError: If ``sh`` is not found on PATH.
    """
    sh = shutil.which("sh")
    if sh is None:
        msg = "sh not found on PATH; a POSIX shell is required to generate the startup launcher"
        raise StartupContractError(msg)
    command = " ".join(shlex.quote(token) for token in canonical_startup_command())
    return (
        f"#!{sh}\n"
        "# Generated by lubko; repository-owned versioned startup contract.\n"
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
    not install the authoritative startup definition (or it drifted).

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
    are missing or have drifted.

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
