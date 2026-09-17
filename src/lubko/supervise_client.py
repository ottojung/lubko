"""Clean client API for tokenless CLI access to supervisor authority.

Tokenless CLI tools must not directly resolve private authority paths.
This module provides functions that:

- **With token** (supervisor daemon context): delegate to direct file
  access in ``supervise`` (fast, no IPC).
- **Without token** (CLI context): all private authority access goes
  through the control socket to the running supervisor.

``supervise.read_state()`` etc. continue to require the token and raise
``SupervisorStateTokenError`` when absent -- they are NOT dual-mode.
"""

from __future__ import annotations

from dataclasses import replace

from lubko import supervise
from lubko.control_socket import (
    _CLIENT_TIMEOUT_SECONDS,
    _recv_message,
    _send_message,
    connect_abstract_socket,
)
from lubko.state import SupervisorStateTokenError, supervisor_state_token


def _has_token() -> bool:
    """Return whether the supervisor state token is available."""
    return supervisor_state_token() is not None


def _socket_request(request: dict[str, object]) -> dict[str, object]:
    """Send a request to the supervisor via the control socket.

    Returns:
        The decoded response dict.

    Raises:
        ConnectionError: If the socket closes before responding.
    """
    conn = connect_abstract_socket()
    try:
        conn.settimeout(_CLIENT_TIMEOUT_SECONDS)
        _send_message(conn, request)
        response = _recv_message(conn)
    finally:
        conn.close()
    if response is None:
        msg = "control socket closed before response"
        raise ConnectionError(msg)
    return response


def _snapshot_request() -> dict[str, object]:
    """Request an authority_snapshot from the supervisor.

    Returns:
        The decoded response dict.

    Raises:
        RuntimeError: If the supervisor returns an error.
    """
    response = _socket_request({"type": "authority_snapshot"})
    if not response.get("ok"):
        msg = response.get("error", "unknown error from supervisor")
        raise RuntimeError(msg)
    return response


def _parse_desired(data: dict[str, object] | None) -> supervise.SupervisorDesired | None:
    """Parse a SupervisorDesired from a dict, returning None for null.

    Returns:
        The parsed desired intent, or ``None`` for null input.
    """
    if data is None:
        return None
    return supervise.SupervisorDesired.from_dict(data)


def _parse_state(data: dict[str, object]) -> supervise.SupervisorState:
    """Parse a SupervisorState from a dict.

    Returns:
        The parsed supervisor state.
    """
    return supervise.SupervisorState.from_dict(data)


# ---------------------------------------------------------------------------
# Read functions
# ---------------------------------------------------------------------------


def read_state_client() -> supervise.SupervisorState:
    """Read the supervisor durable state.

    With token: direct file access.  Without token: authority_snapshot IPC.

    Returns:
        The parsed supervisor state.

    Raises:
        ConnectionRefusedError: If supervisor is not running (no token).
        TypeError: If the response is malformed.
    """
    if _has_token():
        return supervise.read_state()
    try:
        response = _snapshot_request()
    except (ConnectionRefusedError, OSError) as exc:
        msg = "supervisor is not running; cannot read state without a token"
        raise ConnectionRefusedError(msg) from exc
    state_data = response.get("state")
    if not isinstance(state_data, dict):
        msg = "supervisor response missing state in authority_snapshot"
        raise TypeError(msg)
    return _parse_state(state_data)


def read_desired_client() -> supervise.SupervisorDesired | None:
    """Read the desired intent.

    With token: direct file access.  Without token: authority_snapshot IPC.
    Returns ``None`` when the desired intent is genuinely absent.

    Returns:
        The parsed desired intent, or ``None`` when absent.

    Raises:
        DesiredIntentError: If a present intent cannot be trusted.
        ConnectionRefusedError: If supervisor is not running (no token).
    """
    if _has_token():
        return supervise.read_desired_strict()
    try:
        response = _snapshot_request()
    except (ConnectionRefusedError, OSError) as exc:
        msg = "supervisor is not running; cannot read desired without a token"
        raise ConnectionRefusedError(msg) from exc
    desired_data = response.get("desired")
    if desired_data is None:
        return None
    if not isinstance(desired_data, dict):
        msg = "supervisor response has malformed desired in authority_snapshot"
        raise supervise.DesiredIntentError(msg)
    try:
        return _parse_desired(desired_data)
    except (TypeError, ValueError) as exc:
        msg = "supervisor desired intent from authority_snapshot is malformed"
        raise supervise.DesiredIntentError(msg) from exc


def read_status_cli() -> dict[str, object] | None:
    """Read the supervisor status observation surface.

    Always reads from the untokenized ``status.json`` -- no token needed.

    Returns:
        The status dict, or ``None`` when absent.
    """
    status = supervise.read_status()
    if status is None:
        return None
    return status.to_dict()


# ---------------------------------------------------------------------------
# Write functions
# ---------------------------------------------------------------------------


def write_desired_client(desired: supervise.SupervisorDesired) -> int:
    """Write a desired intent via the control socket when no token.

    With token: direct file access.  Without token: control socket IPC.

    Returns:
        The generation of the written intent.

    Raises:
        RuntimeError: If the supervisor rejects the request.
        TypeError: If the response is malformed.
    """
    if _has_token():
        supervise.write_desired(desired)
        return desired.generation
    response = _socket_request({
        "type": "deploy",
        "commit": desired.commit,
        "repo": desired.repo,
        "uv_path": desired.uv_path,
        "worker_id": desired.worker_id,
        "restart": desired.restart,
        "migration": desired.migration,
    })
    if not response.get("ok"):
        msg = response.get("error", "unknown error from supervisor")
        raise RuntimeError(msg)
    generation = response.get("generation")
    if not isinstance(generation, int):
        msg = "supervisor response missing 'generation'"
        raise TypeError(msg)
    return generation


def write_state_client(state: supervise.SupervisorState) -> None:
    """Write the supervisor durable state.

    With token: direct file access.  Without token: raises -- only the
    supervisor daemon may write state directly.

    Raises:
        SupervisorStateTokenError: If no token is available.
    """
    if _has_token():
        supervise.write_state(state)
        return
    msg = "supervisor state writes require the token; use the control socket"
    raise SupervisorStateTokenError(msg)


def supervisor_alive() -> bool:
    """Return whether the supervisor is reachable via the control socket."""
    try:
        response = _socket_request({"type": "ping"})
        return response.get("ok") is True
    except (ConnectionRefusedError, OSError):
        return False


def request_run_client(  # ruff: ignore[too-many-arguments]
    commit: str,
    *,
    repo: str,
    uv_path: str,
    worker_id: str | None,
    restart: bool = False,
    migration: bool = False,
) -> int:
    """Request the daemon to run a commit, via control socket when no token.

    With token: direct ``supervise.request_run()``.
    Without token: control socket IPC.

    Returns:
        The generation of the written intent.

    Raises:
        RuntimeError: If the supervisor rejects the request.
        TypeError: If the response is malformed.
    """
    if _has_token():
        return supervise.request_run(
            commit,
            repo=repo,
            uv_path=uv_path,
            worker_id=worker_id,
            restart=restart,
            migration=migration,
        )
    response = _socket_request({
        "type": "deploy",
        "commit": commit,
        "repo": repo,
        "uv_path": uv_path,
        "worker_id": worker_id,
        "restart": restart,
        "migration": migration,
    })
    if not response.get("ok"):
        msg = response.get("error", "unknown error from supervisor")
        raise RuntimeError(msg)
    generation = response.get("generation")
    if not isinstance(generation, int):
        msg = "supervisor response missing 'generation'"
        raise TypeError(msg)
    return generation


# ---------------------------------------------------------------------------
# Narrow recovery obligation operations
# ---------------------------------------------------------------------------


def set_spawning_obligation_client(obligation: supervise.SpawningObligation) -> None:
    """Set the spawning obligation via the control socket.

    With token: direct state read/replace/write.
    Without token: narrow semantic control socket request.

    Args:
        obligation: The obligation to record.

    Raises:
        RuntimeError: If the supervisor rejects the request.
    """
    if _has_token():
        current = supervise.read_state()
        supervise.write_state(replace(current, spawning=obligation))
        return
    response = _socket_request({
        "type": "set_spawning_obligation",
        "obligation": obligation.to_dict(),
    })
    if not response.get("ok"):
        msg = response.get("error", "unknown error from supervisor")
        raise RuntimeError(msg)


def clear_spawning_obligation_client() -> None:
    """Clear the spawning obligation via the control socket.

    With token: direct state read/replace/write.
    Without token: narrow semantic control socket request.

    Raises:
        RuntimeError: If the supervisor rejects the request.
    """
    if _has_token():
        current = supervise.read_state()
        supervise.write_state(replace(current, spawning=None))
        return
    response = _socket_request({"type": "clear_spawning_obligation"})
    if not response.get("ok"):
        msg = response.get("error", "unknown error from supervisor")
        raise RuntimeError(msg)


# ---------------------------------------------------------------------------
# Generation allocation
# ---------------------------------------------------------------------------


def allocate_generation_client() -> int:
    """Allocate the next generation via the supervisor.

    With token: direct ``supervise.next_generation()`` under the
    generation lock.
    Without token: control socket IPC to the supervisor, which
    computes ``max(applied, desired, mission) + 1`` atomically.

    Returns:
        The allocated generation.

    Raises:
        RuntimeError: If the supervisor rejects the request.
        TypeError: If the response is malformed.
    """
    if _has_token():
        with supervise.generation_lock():
            return supervise.next_generation()
    response = _socket_request({"type": "allocate_generation"})
    if not response.get("ok"):
        msg = response.get("error", "unknown error from supervisor")
        raise RuntimeError(msg)
    generation = response.get("generation")
    if not isinstance(generation, int):
        msg = "supervisor response missing 'generation'"
        raise TypeError(msg)
    return generation
