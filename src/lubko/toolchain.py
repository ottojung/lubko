"""Resolution and persistence of the maintained ``uv`` executable.

``lubko-install`` records the exact ``uv`` executable it successfully used to
install the maintained commands into a small versioned JSON metadata file under
the per-user Lubko state tree (``$XDG_STATE_HOME/lubko/toolchain.json``, default
``~/.local/state/lubko/toolchain.json``). Later, ``lubko-deploy`` keeps working
even when ``uv`` is no longer on PATH by falling back to that recorded
executable.

Resolution follows a strict, deterministic precedence:

1. an explicit ``--uv`` argument, validated and never silently replaced;
2. ``uv`` found on the current PATH;
3. the ``uv`` executable recorded in Lubko state, validated to still exist and
   be executable.

When nothing is usable, resolution fails with a clear, actionable error.
"""

from __future__ import annotations

import json
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Final

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
    """Atomically persist the resolved ``uv`` executable.

    Args:
        uv_path: Absolute path of the ``uv`` executable used.
    """
    directory = toolchain_path().parent
    directory.mkdir(parents=True, exist_ok=True)
    meta = ToolchainMeta(schema_version=TOOLCHAIN_SCHEMA_VERSION, uv_path=uv_path)
    tmp_path = directory / "toolchain.json.tmp"
    tmp_path.write_text(json.dumps(meta.to_dict(), indent=2, sort_keys=True) + "\n")
    tmp_path.replace(toolchain_path())


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
    if data.get("schema_version") != TOOLCHAIN_SCHEMA_VERSION:
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


def resolve_uv(explicit: str | None) -> str:
    """Resolve the ``uv`` executable following the maintained precedence.

    The strict precedence is: explicit ``--uv``, then ``uv`` on PATH, then the
    recorded Lubko toolchain executable. An explicit value that is not usable
    is never silently replaced.

    Args:
        explicit: Explicit ``uv`` value from ``--uv``, or ``None``.

    Returns:
        The resolved absolute path of the ``uv`` executable.

    Raises:
        UvResolutionError: If no usable ``uv`` executable can be resolved.
    """
    if explicit is not None:
        return _resolve_explicit(explicit)
    on_path = shutil.which("uv")
    if on_path is not None:
        return on_path
    recorded = read_toolchain()
    if recorded is not None and is_executable(recorded.uv_path):
        return recorded.uv_path
    raise UvResolutionError(_unresolvable_message(recorded))


def _resolve_explicit(explicit: str) -> str:
    """Resolve and validate an explicitly requested ``uv`` executable.

    Args:
        explicit: The ``--uv`` value.

    Returns:
        The resolved absolute path.

    Raises:
        UvResolutionError: If the explicit value is not usable.
    """
    path = shutil.which(explicit) or explicit
    if is_executable(path):
        return path
    msg = f"explicit uv executable not found or not executable: {explicit!r}"
    raise UvResolutionError(msg)


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
