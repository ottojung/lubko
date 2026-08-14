"""Shared per-user XDG state paths and supervised-deployment markers."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Final

STATE_ROOT_ENV: Final = "XDG_STATE_HOME"
STATE_ROOT_FALLBACK: Final = ".local/state"
ROLLBACK_FILE: Final = "rollback.json"
READINESS_PREFIX: Final = "ready-"
TERMINAL_SUPERVISION_STATES: Final = frozenset({"confirmed", "rolled_back"})


class SupervisionStateError(RuntimeError):
    """Raised when supervised-deployment state exists but is unreadable."""


def state_root() -> Path:
    """Return the per-user Lubko state root following XDG conventions.

    Returns:
        ``$XDG_STATE_HOME/lubko``, falling back to ``~/.local/state/lubko``.
    """
    base = os.environ.get(STATE_ROOT_ENV) or str(Path.home() / STATE_ROOT_FALLBACK)
    return Path(base) / "lubko"


def worker_state_dir() -> Path:
    """Return the directory containing maintained-worker state.

    Returns:
        The stable worker state directory.
    """
    return state_root() / "worker"


def rollback_state_path() -> Path:
    """Return the stable supervised-deployment state path.

    Returns:
        The rollback state path.
    """
    return worker_state_dir() / ROLLBACK_FILE


def readiness_path(token: str) -> Path:
    """Return the readiness marker path for one lifecycle token.

    Args:
        token: Exact lifecycle token of the candidate worker.

    Returns:
        The token-scoped readiness marker path.
    """
    return worker_state_dir() / f"{READINESS_PREFIX}{token}"


def announce_readiness(token: str) -> None:
    """Atomically announce that a worker reached the queue-processing boundary.

    The worker calls this only after PostgreSQL connection and jobs-table
    invariant verification succeed. The marker is keyed by the lifecycle token
    so a stale worker cannot satisfy a later deployment.

    Args:
        token: Exact lifecycle token inherited by the worker.
    """
    directory = worker_state_dir()
    directory.mkdir(parents=True, exist_ok=True)
    target = readiness_path(token)
    temporary = target.with_suffix(".tmp")
    temporary.write_text(token + "\n", encoding="utf-8")
    temporary.chmod(0o600)
    temporary.replace(target)


def readiness_matches(token: str) -> bool:
    """Return whether the exact candidate token has announced readiness.

    Args:
        token: Expected lifecycle token.

    Returns:
        ``True`` only for an exact token-scoped marker.
    """
    try:
        return readiness_path(token).read_text(encoding="utf-8").strip() == token
    except OSError:
        return False


def clear_readiness(token: str | None) -> None:
    """Remove a candidate readiness marker if one exists.

    Args:
        token: Lifecycle token to clear, or ``None``.
    """
    if token is not None:
        readiness_path(token).unlink(missing_ok=True)


def supervision_status() -> str | None:
    """Return the persisted supervised-deployment status.

    Absence means no supervised deployment has ever been recorded. Corrupt or
    malformed state fails closed because lifecycle mutation is unsafe while the
    rollback authority is uncertain.

    Returns:
        The stored status, or ``None`` when the state file is absent.

    Raises:
        SupervisionStateError: If a state file exists but is invalid.
    """
    path = rollback_state_path()
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return None
    except OSError as exc:
        msg = f"cannot read supervised deployment state: {exc}"
        raise SupervisionStateError(msg) from exc
    try:
        payload = json.loads(text)
    except ValueError as exc:
        msg = "supervised deployment state is not valid JSON"
        raise SupervisionStateError(msg) from exc
    if not isinstance(payload, dict) or not isinstance(payload.get("status"), str):
        msg = "supervised deployment state has no valid status"
        raise SupervisionStateError(msg)
    return str(payload["status"])


def supervision_blocks_lifecycle() -> bool:
    """Return whether ordinary lifecycle mutation must fail closed.

    Returns:
        ``True`` for every nonterminal supervised state.

    Raises:
        SupervisionStateError: If the state exists but is unreadable.
    """
    status = supervision_status()
    return status is not None and status not in TERMINAL_SUPERVISION_STATES
