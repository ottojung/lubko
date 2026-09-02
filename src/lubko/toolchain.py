"""Resolution and persistence of the maintained ``uv`` executable.

Lubko pins a single ``uv`` version (see :data:`SUPPORTED_UV_VERSION`). Every
``uv`` candidate that :func:`resolve_uv` returns is verified by running
``uv --version`` and checking the reported version against the pin, so an
upstream ``uv`` release (or a binary swapped in place at the recorded path)
cannot change the toolchain for an unchanged Lubko commit. Resolution fails
closed on a missing/unreadable executable, a non-zero ``uv --version``,
malformed output, a version mismatch, or a timeout.

``lubko-install`` records the exact ``uv`` executable and its validated version
in a small versioned JSON metadata file under the per-user Lubko state tree
(``$XDG_STATE_HOME/lubko/toolchain.json``, default
``~/.local/state/lubko/toolchain.json``). Later, ``lubko-deploy`` keeps working
even when ``uv`` is no longer on PATH by falling back to that recorded
executable — and re-validates it at use time against the pin.

Resolution follows a strict, deterministic precedence:
 1. an explicit ``--uv`` argument, validated and never silently replaced;
 2. ``uv`` found on the current PATH;
 3. the ``uv`` executable recorded in Lubko state, validated to still exist,
    be executable, and report the pinned version.

When nothing is usable, resolution fails with a clear, actionable error.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Final

from lubko.durable import write_json_durable
from lubko.state import state_root

TOOLCHAIN_SCHEMA_VERSION: Final = 1
TOOLCHAIN_FILE_NAME: Final = "toolchain.json"

# The single ``uv`` version the maintained toolchain supports. CI, the
# documented install procedure, and this runtime check all enforce this exact
# pin; changing it is a deliberate, reviewed toolchain change (see
# docs/TOOLCHAIN.md).
SUPPORTED_UV_VERSION: Final = "0.10.12"

# Bounded time allowed for `uv --version` during candidate validation. A slow
# or hung candidate fails closed rather than hanging the deploy/install.
UV_VERSION_CHECK_TIMEOUT_SECONDS: Final = 2.0

_UV_VERSION_RE: Final = re.compile(r"\buv (\d+\.\d+\.\d+)\b")


class UvResolutionError(RuntimeError):
    """Raised when no usable ``uv`` executable can be resolved."""


@dataclass(frozen=True, slots=True)
class ToolchainMeta:
    """Recorded identity of the maintained ``uv`` executable."""

    schema_version: int
    uv_path: str
    uv_version: str | None = None

    def to_dict(self) -> dict[str, object]:
        """Serialize the metadata for storage.

        Returns:
            A JSON-serializable mapping. ``uv_version`` is omitted when unknown
            so old records without it remain loadable.
        """
        data: dict[str, object] = {
            "schema_version": self.schema_version,
            "uv_path": self.uv_path,
        }
        if self.uv_version is not None:
            data["uv_version"] = self.uv_version
        return data


def toolchain_path() -> Path:
    """Return the path of the versioned toolchain metadata file.

    Returns:
        The toolchain metadata path.
    """
    return state_root() / TOOLCHAIN_FILE_NAME


def write_toolchain(uv_path: str) -> None:
    """Crash-durably persist the resolved ``uv`` executable.

    ``toolchain.json`` is recovery authority for later installs/deploys: it is
    the fallback ``uv`` used when none is on PATH, so the write must be
    confirmed durable. The candidate is re-validated against the supported pin
    and its version recorded, so a value is never persisted without having
    passed the same check a later resolution would apply.

    Args:
        uv_path: Absolute path of the ``uv`` executable used.
    """
    version = validate_uv_candidate(uv_path)
    meta = ToolchainMeta(
        schema_version=TOOLCHAIN_SCHEMA_VERSION,
        uv_path=uv_path,
        uv_version=version,
    )
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
    uv_version = data.get("uv_version")
    if uv_version is not None and not isinstance(uv_version, str):
        uv_version = None
    return ToolchainMeta(
        schema_version=TOOLCHAIN_SCHEMA_VERSION,
        uv_path=uv_path,
        uv_version=uv_version,
    )


def is_executable(path: str) -> bool:
    """Return whether a path is an existing regular executable file.

    Args:
        path: Path to inspect.

    Returns:
        ``True`` when the path names an existing, executable regular file.
    """
    return Path(path).is_file() and os.access(path, os.X_OK)


def validate_uv_candidate(uv_path: str) -> str:
    """Verify a ``uv`` executable exists, runs, and reports the pinned version.

    Runs ``uv --version`` under a bounded timeout and parses the reported
    version, failing closed on any problem.

    Args:
        uv_path: Candidate ``uv`` executable path.

    Returns:
        The validated ``uv`` version string.

    Raises:
        UvResolutionError: If the candidate is not executable, ``uv --version``
            fails or times out, its output is unparseable, or the reported
            version does not match :data:`SUPPORTED_UV_VERSION`.
    """
    if not is_executable(uv_path):
        msg = f"uv executable not found or not executable: {uv_path!r}"
        raise UvResolutionError(msg)
    try:
        proc = subprocess.run(
            [uv_path, "--version"],
            capture_output=True,
            text=True,
            timeout=UV_VERSION_CHECK_TIMEOUT_SECONDS,
            check=False,
        )
    except subprocess.TimeoutExpired:
        msg = f"uv version check timed out after {UV_VERSION_CHECK_TIMEOUT_SECONDS}s: {uv_path!r}"
        raise UvResolutionError(msg) from None
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout).strip()
        msg = f"uv --version failed (exit {proc.returncode}) for {uv_path!r}: {detail}"
        raise UvResolutionError(msg)
    match = _UV_VERSION_RE.search(proc.stdout + proc.stderr)
    if match is None:
        msg = f"uv --version produced no parseable version for {uv_path!r}: {proc.stdout.strip()!r}"
        raise UvResolutionError(msg)
    version = match.group(1)
    if version != SUPPORTED_UV_VERSION:
        msg = (
            f"uv version {version} does not match the supported pin "
            f"{SUPPORTED_UV_VERSION} for {uv_path!r}"
        )
        raise UvResolutionError(msg)
    return version


def resolve_uv(explicit: str | None) -> str:
    """Resolve the ``uv`` executable following the maintained precedence.

    Each candidate is verified by :func:`validate_uv_candidate` before being
    returned, including the recorded fallback, which is re-validated at use
    time so a binary swapped in place at the recorded path cannot bypass the
    pin. An explicit value that is not usable is never silently replaced.

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
            f"({recorded.uv_path!r} does not exist, is not executable, or "
            f"does not report the supported uv {SUPPORTED_UV_VERSION}); "
            "reinstall once with uv available via lubko-install --uv PATH, "
            "or pass --uv PATH to lubko-deploy"
        )
    return (
        "uv not found on PATH and no usable uv executable is recorded in Lubko state; "
        "run lubko-install once with uv available (for example lubko-install --uv /path/to/uv), "
        "or pass --uv PATH to lubko-deploy"
    )
