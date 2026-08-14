"""Shared per-user XDG state paths for the Lubko tools."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Final

STATE_ROOT_ENV: Final = "XDG_STATE_HOME"
STATE_ROOT_FALLBACK: Final = ".local/state"


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
