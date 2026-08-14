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
