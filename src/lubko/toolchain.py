"""Resolution and persistence of the maintained ``uv`` executable.

Runtime resolution deliberately does not make one external ``uv`` patch release
part of Lubko's production protocol. CI pins its validation toolchain exactly and
``uv.lock`` is consumed frozen; runtime deployment only requires an executable
``uv`` path. The last successfully resolved path is persisted as a fallback when
``uv`` is not on ``PATH``.
"""

from __future__ import annotations

import json
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Final

from lubko.durable import write_json_durable
from lubko.state import state_root

TOOLCHAIN_SCHEMA_VERSION: Final = 1
TOOLCHAIN_FILE_NAME: Final = "toolchain.json"


class UvResolutionError(RuntimeError):
    """Raised when no usable ``uv`` executable can be resolved."""


@dataclass(frozen=True, slots=True)
class ToolchainMeta:
    """Recorded identity of the maintained ``uv`` executable."""

    schema_version: int
    uv_path: str

    def to_dict(self) -> dict[str, object]:
        """Serialize the metadata for storage.

        Returns:
            A JSON-serializable mapping.
        """
        return {"schema_version": self.schema_version, "uv_path": self.uv_path}


def toolchain_path() -> Path:
    """Return the path of the versioned toolchain metadata file.

    Returns:
        The toolchain metadata path.
    """
    return state_root() / TOOLCHAIN_FILE_NAME


def write_toolchain(uv_path: str) -> None:
    """Crash-durably persist one usable ``uv`` executable path."""
    validate_uv_candidate(uv_path)
    meta = ToolchainMeta(schema_version=TOOLCHAIN_SCHEMA_VERSION, uv_path=uv_path)
    write_json_durable(toolchain_path(), meta.to_dict())


def read_toolchain() -> ToolchainMeta | None:
    """Load the recorded toolchain metadata, tolerating absence and corruption.

    A missing file, malformed JSON, an unsupported schema version, or a
    non-string ``uv_path`` all yield ``None``, so a stale or broken record is
    never mistaken for a usable executable.

    Returns:
        The recorded metadata, or ``None`` when no usable record exists.
    """
    try:
        data = json.loads(toolchain_path().read_text())
    except (OSError, ValueError):
        return None
    if not isinstance(data, dict):
        return None
    schema_version = data.get("schema_version")
    if (
        not isinstance(schema_version, int)
        or isinstance(schema_version, bool)
        or schema_version != TOOLCHAIN_SCHEMA_VERSION
    ):
        return None
    uv_path = data.get("uv_path")
    if not isinstance(uv_path, str) or not uv_path:
        return None
    return ToolchainMeta(schema_version=TOOLCHAIN_SCHEMA_VERSION, uv_path=uv_path)


def is_executable(path: str) -> bool:
    """Return whether a path is an existing regular executable file.

    Args:
        path: Path to inspect.

    Returns:
        ``True`` when the path names an existing, executable regular file.
    """
    return Path(path).is_file() and os.access(path, os.X_OK)


def validate_uv_candidate(uv_path: str) -> None:
    """Require ``uv_path`` to name an existing executable regular file.

    Runtime compatibility is exercised by the actual frozen ``uv`` commands;
    Lubko intentionally does not reject an executable because its patch version
    differs from CI's build-tool pin.

    Raises:
        UvResolutionError: If the candidate is not an executable regular file.
    """
    if not is_executable(uv_path):
        msg = f"uv executable not found or not executable: {uv_path!r}"
        raise UvResolutionError(msg)


def resolve_uv(explicit: str | None) -> str:
    """Resolve the ``uv`` executable following the maintained precedence.

    Each candidate must be an executable regular file. An explicit unusable
    value is never silently replaced.

    Args:
        explicit: Explicit ``uv`` value from ``--uv``, or ``None``.

    Returns:
        The resolved absolute path of the ``uv`` executable.

    Raises:
        UvResolutionError: If no usable ``uv`` executable can be resolved.
    """
    if explicit is not None:
        candidate = shutil.which(explicit) or explicit
    else:
        on_path = shutil.which("uv")
        if on_path is not None:
            candidate = on_path
        else:
            recorded = read_toolchain()
            if recorded is not None and is_executable(recorded.uv_path):
                candidate = recorded.uv_path
            else:
                raise UvResolutionError(_unresolvable_message(recorded))
    validate_uv_candidate(candidate)
    return candidate


def _unresolvable_message(recorded: ToolchainMeta | None) -> str:
    """Build an actionable message for an unresolvable ``uv``.

    Args:
        recorded: Recorded metadata, or ``None`` when nothing is recorded.

    Returns:
        A user-facing explanation of how to proceed.
    """
    if recorded is not None:
        return (
            "no uv on PATH and the recorded uv executable is unusable "
            f"({recorded.uv_path!r} does not exist or is not executable); "
            "reinstall once with uv available via lubko-install --uv PATH, "
            "or pass --uv PATH to lubko-deploy"
        )
    return (
        "uv not found on PATH and no usable uv executable is recorded in Lubko state; "
        "run lubko-install once with uv available (for example lubko-install --uv /path/to/uv), "
        "or pass --uv PATH to lubko-deploy"
    )
