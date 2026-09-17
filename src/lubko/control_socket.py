"""Stable tokenless control transport via abstract Unix-domain socket.

The control socket is the **exclusive write boundary** between tokenless CLI
tools (``lubko-deploy``, ``lubko-deploy-ctl``) and the token-holding
supervisor daemon.  CLIs send JSON requests over the socket; the supervisor
validates, allocates generations, and writes to its private tokenized
authority.  Status observation uses the non-authoritative ``status.json``
at the stable untokenized path.

Uses Linux abstract Unix-domain sockets (null-byte prefix) so the
transport is independent of XDG path length, creates no stale socket
files, and is auto-cleaned by the kernel on process death.  The socket
name is deterministically derived from non-secret public inputs (uid +
sha256 of state_root) with bounded length.  Both server and client
validate ``SO_PEERCRED`` (same-UID check) before exchanging any
lifecycle payload.  All socket I/O uses explicit timeouts so no CLI may
hang indefinitely on a wedged endpoint.

The socket is non-authoritative: it is a transport mechanism, not durable
authority.  It does not reveal or select the private supervisor state
token.  Error messages never contain token values.

Protocol
--------
All messages are length-prefixed (4-byte big-endian uint32) JSON.  A
request is one length-prefixed JSON object.  The server responds with one
length-prefixed JSON object, then closes the connection.

Request types:

``deploy``
    Write a run intent.  Fields: ``commit``, ``repo``, ``uv_path``,
    ``worker_id`` (optional), ``restart`` (bool, default false),
    ``migration`` (bool, default false).
    Response: ``{"ok": true, "generation": <int>}`` or error.

``status``
    Return the current supervisor status.
    Response: ``{"ok": true, "status": <status dict>}`` or error.

``authority_snapshot``
    Return the private desired+state authority snapshots (no token leak).
    Response: ``{"ok": true, "desired": <dict|null>, "state": <dict>}``
    where ``desired`` is the serialized ``SupervisorDesired`` (or null)
    and ``state`` is the serialized ``SupervisorState``.

``set_spawning_obligation``
    Narrow semantic write: set the spawning obligation on the state.
    Payload must contain ``obligation`` (a JSON-serialized
    ``SpawningObligation``).  The supervisor applies validation and
    serialization under its own locks.  Response: ``{"ok": true}``.

``clear_spawning_obligation``
    Narrow semantic write: clear the spawning obligation on the state.
    Response: ``{"ok": true}`` or error.

``allocate_generation``
    Compute and return the next generation under the generation lock.
    The supervisor reads state, desired, and mission authority, then
    returns ``max(applied, desired, mission) + 1``.  This is the only
    correct way to allocate a generation without the token.
    Response: ``{"ok": true, "generation": <int>}`` or error.

``ping``
    Liveness check.  Response: ``{"ok": true}``.
"""

from __future__ import annotations

import hashlib
import json
import os
import socket
import struct
from typing import Any, Final

from lubko.state import state_root

#: Maximum bytes for a single control message (requests are small).
_MAX_MESSAGE_BYTES: Final = 64 * 1024

#: Linux SO_PEERCRED socket option.
_SO_PEERCRED: Final = 17

#: ucred struct size: pid_t(4) + uid_t(4) + gid_t(4) = 12 bytes.
_UCRED_SIZE: Final = 12

#: Default timeout for client connect/send/recv operations (seconds).
_CLIENT_TIMEOUT_SECONDS: Final = 5.0

#: Default timeout for server accept/recv operations (seconds).
_SERVER_TIMEOUT_SECONDS: Final = 5.0


def _abstract_socket_name() -> bytes:
    """Derive a bounded abstract socket name from uid and state_root.

    The name is deterministic from non-secret public inputs so the
    supervisor and CLI tools always agree on the same socket without
    any configuration.  The sha256 digest of the state root path is
    truncated to 16 hex characters for bounded length.

    Returns:
        Abstract socket name bytes (no null prefix -- the caller adds it).
    """
    uid = os.getuid()
    state_root_bytes = str(state_root()).encode("utf-8")
    digest = hashlib.sha256(state_root_bytes).hexdigest()[:16]
    return f"lubko-supervisor-{uid}-{digest}".encode("ascii")


def bind_abstract_socket() -> socket.socket:
    """Create and bind an abstract Unix-domain socket for the supervisor.

    The socket is bound to the abstract namespace (null-byte prefix) so
    it is independent of filesystem path length and creates no stale
    files.  ``SO_REUSEADDR`` is set so a restart after crash rebinds
    immediately.  The listening socket is set nonblocking so
    ``accept()`` in the supervisor tick loop returns immediately when no
    clients are pending rather than blocking the reconciliation cycle.

    Returns:
        A bound, listening, nonblocking ``AF_UNIX`` socket.
    """
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    name = b"\0" + _abstract_socket_name()
    sock.bind(name)
    sock.listen(8)
    sock.setblocking(False)  # ruff: ignore[boolean-positional-value-in-call]
    return sock


def connect_abstract_socket(
    timeout: float = _CLIENT_TIMEOUT_SECONDS,
) -> socket.socket:
    """Connect to the supervisor's abstract Unix-domain control socket.

    The connection uses an explicit timeout so no CLI may hang
    indefinitely on a wedged endpoint.  After connecting, the client
    validates the server's UID via ``SO_PEERCRED`` before sending any
    lifecycle payload.

    Args:
        timeout: Maximum seconds to wait for connect and initial
            peercred validation.

    Returns:
        A connected, peer-validated ``AF_UNIX`` socket.

    Raises:
        OSError: If the abstract socket cannot be reached or the peer
            UID does not match.
    """
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    name = b"\0" + _abstract_socket_name()
    try:
        sock.connect(name)
    except OSError:
        sock.close()
        raise
    validate_peercred(sock)
    return sock


def validate_peercred(conn: socket.socket) -> int:
    """Validate that the peer has the same UID via ``SO_PEERCRED``.

    Used by both server (validating clients) and client (validating the
    server) so a different-UID process cannot pre-bind the deterministic
    abstract name and intercept lifecycle requests.

    Args:
        conn: Connected Unix-domain socket.

    Returns:
        The peer's PID.

    Raises:
        PermissionError: If the peer UID does not match our UID.
    """
    creds = conn.getsockopt(socket.SOL_SOCKET, _SO_PEERCRED, _UCRED_SIZE)
    pid: int
    uid: int
    pid, uid, _gid = struct.unpack("iii", creds)
    if uid != os.getuid():
        msg = f"control socket peer UID {uid} != our UID {os.getuid()}"
        raise PermissionError(msg)
    return pid


def _send_message(conn: socket.socket, data: dict[str, Any]) -> None:
    """Send a length-prefixed JSON message over *conn*.

    Args:
        conn: Connected socket.
        data: JSON-serializable message.
    """
    payload = json.dumps(data, separators=(",", ":")).encode("utf-8")
    conn.sendall(struct.pack("!I", len(payload)) + payload)


def _recv_message(conn: socket.socket) -> dict[str, Any] | None:
    """Receive a length-prefixed JSON message from *conn*.

    The decoded JSON must be a top-level object (dict).  Non-object JSON
    (arrays, strings, numbers, null) is rejected as malformed.

    Args:
        conn: Connected socket.

    Returns:
        The decoded message dict, or ``None`` on connection close.

    Raises:
        TypeError: If the message is too large, not valid JSON, or not
            a JSON object.
        ValueError: If the message header is malformed.
    """
    header = _recv_exact(conn, 4)
    if header is None:
        return None
    length = struct.unpack("!I", header)[0]
    if length > _MAX_MESSAGE_BYTES:
        msg = f"control message too large: {length} bytes"
        raise ValueError(msg)
    payload = _recv_exact(conn, length)
    if payload is None:
        return None
    decoded = json.loads(payload)
    if not isinstance(decoded, dict):
        msg = "control message is not a JSON object"
        raise TypeError(msg)
    return decoded


def _recv_exact(conn: socket.socket, n: int) -> bytes | None:
    """Read exactly *n* bytes from *conn*, returning ``None`` on EOF.

    Args:
        conn: Connected socket.
        n: Number of bytes to read.

    Returns:
        The bytes, or ``None`` if the connection closed before *n* bytes.
    """
    buf = bytearray()
    while len(buf) < n:
        chunk = conn.recv(n - len(buf))
        if not chunk:
            return None
        buf.extend(chunk)
    return bytes(buf)


class ControlRequest:
    """Parsed control socket request."""

    __slots__ = ("payload", "request_type")

    def __init__(self, request_type: str, payload: dict[str, object]) -> None:
        """Store the request type and payload.

        Args:
            request_type: Discriminator string (e.g. ``"deploy"``).
            payload: Request-specific fields.
        """
        self.request_type = request_type
        self.payload = payload

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> ControlRequest:
        """Parse a raw message dict into a typed request.

        Args:
            data: Decoded JSON message (already validated as a dict).

        Returns:
            The parsed request.

        Raises:
            ValueError: If the ``type`` field is missing or not a string.
        """
        request_type = data.get("type")
        if not isinstance(request_type, str) or not request_type:
            msg = "control request missing 'type'"
            raise ValueError(msg)
        return cls(request_type=request_type, payload=data)


class ControlResponse:
    """Control socket response builder."""

    @staticmethod
    def ok(**extra: object) -> dict[str, object]:
        """Build a success response.

        Returns:
            ``{"ok": true, ...}`` with any extra fields.
        """
        return {"ok": True, **extra}

    @staticmethod
    def error(message: str) -> dict[str, object]:
        """Build an error response.

        Args:
            message: Human-readable error description (must not contain
                token values).

        Returns:
            ``{"ok": false, "error": "<message>"}``.
        """
        return {"ok": False, "error": message}
