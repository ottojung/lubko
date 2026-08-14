"""Versioned JSON binding for the two-column Lubko transport table.

The transport table ``lubko.jobs`` keeps exactly two columns forever:

- ``id`` — a unique random identifier (``uuid``);
- ``payload`` — one string containing a JSON object (``text``), opaque at rest.

Every evolving job/request/result/state/cancellation/process-identity datum
lives inside ``payload`` as a versioned JSON object. The schema never gains a
third column; protocol evolution happens by adding fields within the current
version or by bumping ``v``. See ``docs/protocol.md`` for the authoritative
human-readable specification.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Final

PROTOCOL_VERSION: Final = 1

JOB_TYPE_COMMAND: Final = "command"
KNOWN_JOB_TYPES: Final = frozenset({JOB_TYPE_COMMAND})

STATUS_PENDING: Final = "pending"
STATUS_RUNNING: Final = "running"
STATUS_SUCCEEDED: Final = "succeeded"
STATUS_FAILED: Final = "failed"
STATUS_CANCELLED: Final = "cancelled"
KNOWN_STATUSES: Final = frozenset({
    STATUS_PENDING,
    STATUS_RUNNING,
    STATUS_SUCCEEDED,
    STATUS_FAILED,
    STATUS_CANCELLED,
})

TWO_COLUMN_INVARIANT: Final = (
    "The Lubko transport table lubko.jobs has exactly two columns forever: "
    "id (unique random identifier) and payload (one string containing a JSON "
    "object). All evolving job/request/result/state/cancellation/"
    "process-identity data lives inside that JSON payload. Never add a third "
    "column."
)


class ProtocolError(ValueError):
    """Raised when a job payload violates the versioned binding."""


@dataclass(frozen=True, slots=True)
class JobRequest:
    """The immutable submission carried by a job payload."""

    cwd: str
    command: str | None
    args: tuple[str, ...] | None


@dataclass(frozen=True, slots=True)
class JobPayload:
    """A parsed and validated protocol v1 job payload."""

    version: int
    type: str
    request: JobRequest
    status: str


def build_payload(
    *, cwd: str, command: str | None = None, args: list[str] | None = None
) -> dict[str, Any]:
    """Build a protocol v1 ``command`` job payload ready for submission.

    Exactly one of ``command`` (a shell command string) or ``args`` (an
    argv-style list) must be provided.

    Args:
        cwd: Absolute working directory for the job.
        command: Shell command to run through ``bash -lc``, or ``None``.
        args: argv-style command to run directly, or ``None``.

    Returns:
        The versioned payload dict.

    Raises:
        ProtocolError: If the request violates the binding.
    """
    request: dict[str, object] = {"cwd": cwd}
    if command is not None and args is not None:
        msg = "request may provide command or args, not both"
        raise ProtocolError(msg)
    if command is not None:
        if not command:
            msg = "request.command must be a non-empty string"
            raise ProtocolError(msg)
        request["command"] = command
    elif args:
        if not args or not all(args):
            msg = "request.args must be a non-empty array of strings"
            raise ProtocolError(msg)
        request["args"] = list(args)
    else:
        msg = "request must provide command or args"
        raise ProtocolError(msg)
    if not cwd:
        msg = "request.cwd must be a non-empty string"
        raise ProtocolError(msg)
    return {
        "v": PROTOCOL_VERSION,
        "type": JOB_TYPE_COMMAND,
        "request": request,
        "state": {"status": STATUS_PENDING},
    }


def _parse_request(raw_request: object) -> JobRequest:
    """Validate the ``request`` section of a payload.

    Args:
        raw_request: The raw ``request`` value.

    Returns:
        The validated job request.

    Raises:
        ProtocolError: If the request violates the binding.
    """
    if not isinstance(raw_request, dict):
        msg = "payload must contain a request object"
        raise ProtocolError(msg)
    cwd = raw_request.get("cwd")
    if not isinstance(cwd, str) or not cwd:
        msg = "request.cwd must be a non-empty string"
        raise ProtocolError(msg)
    command = raw_request.get("command")
    args = raw_request.get("args")
    if command is not None and args is not None:
        msg = "request may provide command or args, not both"
        raise ProtocolError(msg)
    if command is not None and (not isinstance(command, str) or not command):
        msg = "request.command must be a non-empty string"
        raise ProtocolError(msg)
    if args is not None and (
        not isinstance(args, list) or not args or not all(isinstance(a, str) and a for a in args)
    ):
        msg = "request.args must be a non-empty array of strings"
        raise ProtocolError(msg)
    return JobRequest(cwd=cwd, command=command, args=tuple(args) if args is not None else None)


def _parse_status(raw_state: object) -> str:
    """Validate the ``state`` section of a payload.

    Args:
        raw_state: The raw ``state`` value.

    Returns:
        The validated job status.

    Raises:
        ProtocolError: If the state violates the binding.
    """
    if not isinstance(raw_state, dict):
        msg = "payload must contain a state object"
        raise ProtocolError(msg)
    status = raw_state.get("status")
    if not isinstance(status, str) or status not in KNOWN_STATUSES:
        msg = f"unknown job status: {status!r}"
        raise ProtocolError(msg)
    return status


def parse_payload(data: object) -> JobPayload:
    """Parse and validate a job payload against the versioned binding.

    The stored ``payload`` column is opaque text; a raw JSON string is decoded
    before validation.

    Args:
        data: The JSON object stored in the ``payload`` column, either as a
            raw JSON string or as an already-decoded mapping.

    Returns:
        The parsed and validated payload.

    Raises:
        ProtocolError: If the payload violates the binding.
    """
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except json.JSONDecodeError as exc:
            msg = f"payload is not valid JSON: {exc}"
            raise ProtocolError(msg) from exc
    if not isinstance(data, dict):
        msg = "payload must be a JSON object"
        raise ProtocolError(msg)
    version = data.get("v")
    if version != PROTOCOL_VERSION:
        msg = f"unsupported protocol version: {version!r} (expected {PROTOCOL_VERSION})"
        raise ProtocolError(msg)
    job_type = data.get("type")
    if job_type not in KNOWN_JOB_TYPES:
        msg = f"unknown job type: {job_type!r}"
        raise ProtocolError(msg)
    return JobPayload(
        version=version,
        type=job_type,
        request=_parse_request(data.get("request")),
        status=_parse_status(data.get("state")),
    )
