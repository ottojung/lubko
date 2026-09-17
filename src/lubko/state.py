"""Shared per-user XDG state paths for the Lubko tools."""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Final

STATE_ROOT_ENV: Final = "XDG_STATE_HOME"
STATE_ROOT_FALLBACK: Final = ".local/state"

#: Environment variable carrying the required high-entropy opaque supervisor
#: state namespace token.  The token isolates authoritative mutable supervisor
#: state under a tokenized subdirectory so ordinary/default state resolution
#: cannot accidentally reach live supervisor authority.
SUPERVISOR_STATE_TOKEN_ENV: Final = "LUBKO_SUPERVISOR_STATE_TOKEN"  # ruff: ignore[hardcoded-password-string]

#: Path-safe 256-bit hex token: exactly 64 lowercase hexadecimal characters.
_TOKEN_PATTERN: Final = re.compile(r"^[0-9a-f]{64}$")


class SupervisorStateTokenError(Exception):
    """The supervisor state token is absent, empty, or invalid."""


def validate_supervisor_state_token(token: str) -> str:
    """Validate that *token* is a path-safe 256-bit hex string.

    A valid token is exactly 64 lowercase hexadecimal characters (256 bits of
    entropy when generated via ``secrets.token_hex(32)``).

    Args:
        token: Candidate token string.

    Returns:
        The validated token (unchanged).

    Raises:
        SupervisorStateTokenError: If *token* is empty or does not match the
            expected 64-character lowercase hex format.
    """
    if not token:
        msg = (
            "LUBKO_SUPERVISOR_STATE_TOKEN is required but empty; "
            "supervisor startup is refused without a valid 256-bit hex token"
        )
        raise SupervisorStateTokenError(msg)
    if not _TOKEN_PATTERN.match(token):
        msg = (
            "LUBKO_SUPERVISOR_STATE_TOKEN is invalid; expected exactly 64 "
            "lowercase hex characters (256-bit path-safe opaque token)"
        )
        raise SupervisorStateTokenError(msg)
    return token


def supervisor_state_token() -> str | None:
    """Return the validated supervisor state token from the environment.

    Returns:
        The validated 64-character hex token, or ``None`` when the
        environment variable is absent or empty.
    """
    raw = os.environ.get(SUPERVISOR_STATE_TOKEN_ENV, "")
    if not raw:
        return None
    return validate_supervisor_state_token(raw)


def state_root() -> Path:
    """Return the per-user Lubko state root following XDG conventions.

    Returns:
        ``$XDG_STATE_HOME/lubko``, falling back to ``~/.local/state/lubko``.
    """
    base = os.environ.get(STATE_ROOT_ENV) or str(Path.home() / STATE_ROOT_FALLBACK)
    return Path(base) / "lubko"


def worker_state_dir() -> Path:
    """Return the stable directory containing maintained-worker state.

    Returns:
        The worker state directory.
    """
    return state_root() / "worker"


def rollback_state_path() -> Path:
    """Return the stable supervised-deployment state path.

    Returns:
        The rollback state file path.
    """
    return worker_state_dir() / "rollback.json"


def cli_root_dir() -> Path:
    """Return the stable directory holding per-commit CLI environments.

    Each confirmed commit owns an immutable CLI environment under this root;
    ``cli/current`` is a symlink selecting the active commit.

    Returns:
        The per-commit CLI root directory.
    """
    return state_root() / "cli"
