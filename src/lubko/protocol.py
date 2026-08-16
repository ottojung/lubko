"""Versioned JSON binding for the two-column Lubko transport table.

The transport table ``lubko.jobs`` keeps exactly two columns forever:

- ``id`` — a unique random identifier (``uuid``);
- ``payload`` — one string containing a JSON object (``text``), opaque at rest.

Every evolving job/request/result/state/cancellation/process-identity datum
lives inside ``payload`` as a versioned JSON object. The schema never gains a
third column; protocol evolution happens by adding fields within the current
version or by bumping ``v``. See ``docs/protocol.md`` for the authoritative
human-readable specification.

Protocol v3 distinguishes two payload kinds:

- ``command`` — a runnable root job with ``request``, ``state``, optional
  terminal ``result``, and optional bounded live output tails (``output``);
- ``output_chunk`` — an immutable, explicitly owned historical output chunk
  belonging to exactly one root ``command`` job (via ``thread``).

A v3 ``command`` request is process-only: ``request.process`` is the sole
executable field, a non-empty array of non-empty strings exec'd directly as
argv with no shell. The former v2 ``request.command`` / ``request.args``
fields are gone; v2 payloads are rejected.

Every payload Lubko writes is strictly bounded: root live tails are the
newest at most ``OUTPUT_TAIL_MAX_BYTES`` raw bytes per stream and output
chunks are at most ``OUTPUT_CHUNK_MAX_BYTES`` raw bytes, so normal polling of a
root job can never return unbounded stdout/stderr.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Final
from uuid import UUID

PROTOCOL_VERSION: Final = 3

#: The last protocol generation before the process-only binding. It is
#: referenced only to produce a clear rejection diagnostic; v2 is never
#: accepted and no v2 data is preserved.
LEGACY_PROTOCOL_V2: Final = 2

JOB_TYPE_COMMAND: Final = "command"
JOB_TYPE_OUTPUT_CHUNK: Final = "output_chunk"
KNOWN_JOB_TYPES: Final = frozenset({JOB_TYPE_COMMAND, JOB_TYPE_OUTPUT_CHUNK})

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

CHUNK_STREAMS: Final = ("stdout", "stderr")

#: Strict maximum size (in raw bytes) of a root live output tail window.
#: UTF-8 decoding of at most this many bytes yields at most this many
#: characters, so the decoded ``tail`` text is bounded by the same number.
OUTPUT_TAIL_MAX_BYTES: Final = 4000

#: Strict maximum size (in raw bytes) of one immutable output chunk value.
#: UTF-8 decoding of at most this many bytes yields at most this many
#: characters, so the decoded ``value`` text is bounded by the same number.
OUTPUT_CHUNK_MAX_BYTES: Final = 2000

TWO_COLUMN_INVARIANT: Final = (
    "The Lubko transport table lubko.jobs has exactly two columns forever: "
    "id (unique random identifier) and payload (one string containing a JSON "
    "object). All evolving job/request/result/state/cancellation/"
    "process-identity/output data lives inside that JSON payload. Never add a "
    "third column."
)


class ProtocolError(ValueError):
    """Raised when a job payload violates the versioned binding."""


@dataclass(frozen=True, slots=True)
class JobRequest:
    """The immutable submission carried by a job payload."""

    cwd: str
    process: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class OutputWindow:
    """A bounded rolling live tail of one root-job output stream."""

    tail: str
    start: int
    end: int
    previous: UUID | None


@dataclass(frozen=True, slots=True)
class OutputSection:
    """The bounded live output tails of a root ``command`` job."""

    stdout: OutputWindow | None
    stderr: OutputWindow | None


@dataclass(frozen=True, slots=True)
class ResultView:
    """The validated, bounded terminal result of a root job."""

    stdout: str
    stderr: str
    exit_code: int | None
    cancellation_note: str | None
    recovery_note: str | None


@dataclass(frozen=True, slots=True)
class JobPayload:
    """A parsed and validated protocol v3 ``command`` job payload."""

    version: int
    type: str
    request: JobRequest
    status: str
    output: OutputSection | None
    result: ResultView | None


@dataclass(frozen=True, slots=True)
class OutputChunk:
    """A parsed and validated protocol v3 ``output_chunk`` payload."""

    thread: UUID
    stream: str
    sequence: int
    start: int
    end: int
    value: str
    previous: UUID | None


def _parse_uuid(value: object) -> UUID | None:
    """Validate an optional JSON string as a UUID.

    Args:
        value: The raw JSON value, normally a string or ``None``.

    Returns:
        The parsed UUID, or ``None``.

    Raises:
        ProtocolError: If the value is not a UUID string.
    """
    if value is None:
        return None
    if not isinstance(value, str):
        msg = "payload UUID field must be a string"
        raise ProtocolError(msg)
    try:
        return UUID(value)
    except ValueError:
        msg = f"payload UUID field is not a valid UUID: {value!r}"
        raise ProtocolError(msg) from None


def _process_tuple(process: object) -> tuple[str, ...]:
    """Validate and return a ``request.process`` argv as a string tuple.

    ``request.process`` must be a non-empty array of non-empty strings. The
    check is performed at runtime even when a caller's static type already
    promises strings, so a genuine process-only binding can never tolerate a
    malformed argv.

    Args:
        process: The raw ``request.process`` value.

    Returns:
        The validated argv tuple.

    Raises:
        ProtocolError: If the value violates the binding.
    """
    if (
        not isinstance(process, list)
        or not process
        or not all(isinstance(arg, str) and arg for arg in process)
    ):
        msg = "request.process must be a non-empty array of non-empty strings"
        raise ProtocolError(msg)
    return tuple(process)


def build_payload(*, cwd: str, process: list[str]) -> dict[str, Any]:
    """Build a protocol v3 ``command`` job payload ready for submission.

    ``process`` is always required: a non-empty argv-style array of non-empty
    strings that the worker exec's directly with no shell. The former v2
    ``request.command`` / ``request.args`` fields do not exist in v3.

    Args:
        cwd: Absolute working directory for the job.
        process: Non-empty argv-style array of non-empty strings to exec
            directly with no shell. Every element is runtime-checked to be a
            non-empty string.

    Returns:
        The versioned payload dict.

    Raises:
        ProtocolError: If the request violates the binding.
    """
    request: dict[str, object] = {"cwd": cwd}
    validated = _process_tuple(list(process))
    request["process"] = list(validated)
    if not cwd:
        msg = "request.cwd must be a non-empty string"
        raise ProtocolError(msg)
    return {
        "v": PROTOCOL_VERSION,
        "type": JOB_TYPE_COMMAND,
        "request": request,
        "state": {"status": STATUS_PENDING},
    }


def build_output_window_payload(
    *, tail: str, start: int, end: int, previous: UUID | None
) -> dict[str, Any]:
    """Build the JSON mapping of one bounded live output tail window.

    Args:
        tail: Decoded text of the newest output window.
        start: Byte offset where the window begins.
        end: Byte offset where the window ends.
        previous: UUID of the newest immutable chunk for the stream, or
            ``None`` when no chunk exists yet.

    Returns:
        The window mapping suitable for the root ``output`` section.

    Raises:
        ProtocolError: If the window violates the size bound.
    """
    if len(tail) > OUTPUT_TAIL_MAX_BYTES:
        msg = f"output tail must be at most {OUTPUT_TAIL_MAX_BYTES} characters"
        raise ProtocolError(msg)
    _validate_offsets(start, end)
    window: dict[str, Any] = {
        "tail": tail,
        "start": start,
        "end": end,
    }
    if previous is not None:
        window["previous"] = str(previous)
    else:
        window["previous"] = None
    return window


def build_output_chunk_payload(  # ruff: ignore[too-many-arguments]
    *,
    thread: UUID,
    stream: str,
    sequence: int,
    start: int,
    end: int,
    value: str,
    previous: UUID | None,
) -> dict[str, Any]:
    """Build an immutable ``output_chunk`` payload mapping.

    Args:
        thread: UUID of the owning root ``command`` job.
        stream: Which stream the chunk belongs to (``stdout`` or ``stderr``).
        sequence: Monotonic chunk sequence number for the stream.
        start: Byte offset where the chunk begins.
        end: Byte offset where the chunk ends.
        value: Immutable decoded output text of the chunk.
        previous: UUID of the previous chunk in the chain, or ``None``.

    Returns:
        The chunk payload mapping.

    Raises:
        ProtocolError: If the chunk violates the binding or size bound.
    """
    _validate_stream(stream)
    if sequence < 0:
        msg = "output_chunk.sequence must be a non-negative integer"
        raise ProtocolError(msg)
    if len(value) > OUTPUT_CHUNK_MAX_BYTES:
        msg = f"output_chunk.value must be at most {OUTPUT_CHUNK_MAX_BYTES} characters"
        raise ProtocolError(msg)
    _validate_offsets(start, end)
    chunk: dict[str, Any] = {
        "v": PROTOCOL_VERSION,
        "type": JOB_TYPE_OUTPUT_CHUNK,
        "thread": str(thread),
        "stream": stream,
        "sequence": sequence,
        "start": start,
        "end": end,
        "value": value,
    }
    if previous is not None:
        chunk["previous"] = str(previous)
    else:
        chunk["previous"] = None
    return chunk


def _validate_offsets(start: int, end: int) -> None:
    """Validate a ``[start, end)`` byte-offset window.

    Args:
        start: Window start offset.
        end: Window end offset.

    Raises:
        ProtocolError: If the offsets are malformed.
    """
    if start < 0 or end < 0:
        msg = "output offsets must be non-negative integers"
        raise ProtocolError(msg)
    if end < start:
        msg = f"output window end {end} precedes start {start}"
        raise ProtocolError(msg)


def _validate_stream(stream: object) -> None:
    """Validate an output stream name.

    Args:
        stream: The raw ``stream`` value.

    Raises:
        ProtocolError: If the stream is unknown.
    """
    if stream not in CHUNK_STREAMS:
        msg = f"unknown output stream: {stream!r}"
        raise ProtocolError(msg)


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
    legacy_keys = ["command", "args"]
    if any(key in raw_request for key in legacy_keys):
        msg = (
            "request must provide only process; the v2 request.command/request.args "
            "fields are not accepted in protocol v3"
        )
        raise ProtocolError(msg)
    cwd = raw_request.get("cwd")
    if not isinstance(cwd, str) or not cwd:
        msg = "request.cwd must be a non-empty string"
        raise ProtocolError(msg)
    process = raw_request.get("process")
    if process is None:
        msg = "request.process is required; the v2 request.command/request.args fields are gone"
        raise ProtocolError(msg)
    return JobRequest(cwd=cwd, process=_process_tuple(process))


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


def _parse_window(raw: object) -> OutputWindow:
    """Validate one bounded live output window.

    Args:
        raw: The raw ``output.<stream>`` value.

    Returns:
        The validated window.

    Raises:
        ProtocolError: If the window violates the binding or size bound.
    """
    if not isinstance(raw, dict):
        msg = "output window must be an object"
        raise ProtocolError(msg)
    tail = raw.get("tail")
    if not isinstance(tail, str) or len(tail) > OUTPUT_TAIL_MAX_BYTES:
        msg = f"output window tail must be a string of at most {OUTPUT_TAIL_MAX_BYTES} characters"
        raise ProtocolError(msg)
    start = raw.get("start")
    end = raw.get("end")
    if not isinstance(start, int) or isinstance(start, bool):
        msg = "output window start must be an integer"
        raise ProtocolError(msg)
    if not isinstance(end, int) or isinstance(end, bool):
        msg = "output window end must be an integer"
        raise ProtocolError(msg)
    _validate_offsets(start, end)
    previous = _parse_uuid(raw.get("previous"))
    return OutputWindow(tail=tail, start=start, end=end, previous=previous)


def _parse_output_section(raw: object) -> OutputSection | None:
    """Validate the optional ``output`` section of a root job.

    Args:
        raw: The raw ``output`` value.

    Returns:
        The validated output section, or ``None`` when absent.

    Raises:
        ProtocolError: If the section violates the binding.
    """
    if raw is None:
        return None
    if not isinstance(raw, dict):
        msg = "payload output must be an object"
        raise ProtocolError(msg)
    stdout = _parse_window(raw["stdout"]) if raw.get("stdout") is not None else None
    stderr = _parse_window(raw["stderr"]) if raw.get("stderr") is not None else None
    return OutputSection(stdout=stdout, stderr=stderr)


def _parse_result(raw: object) -> ResultView | None:
    """Validate the optional terminal ``result`` section of a root job.

    Args:
        raw: The raw ``result`` value.

    Returns:
        The validated result, or ``None`` when absent.

    Raises:
        ProtocolError: If the result violates the binding.
    """
    if raw is None:
        return None
    if not isinstance(raw, dict):
        msg = "payload result must be an object"
        raise ProtocolError(msg)
    stdout = raw.get("stdout")
    stderr = raw.get("stderr")
    if not isinstance(stdout, str) or len(stdout) > OUTPUT_TAIL_MAX_BYTES:
        msg = f"result.stdout must be a string of at most {OUTPUT_TAIL_MAX_BYTES} characters"
        raise ProtocolError(msg)
    if not isinstance(stderr, str) or len(stderr) > OUTPUT_TAIL_MAX_BYTES:
        msg = f"result.stderr must be a string of at most {OUTPUT_TAIL_MAX_BYTES} characters"
        raise ProtocolError(msg)
    exit_code = raw.get("exit_code")
    if exit_code is not None and (not isinstance(exit_code, int) or isinstance(exit_code, bool)):
        msg = "result.exit_code must be an integer or null"
        raise ProtocolError(msg)
    cancellation_note = raw.get("cancellation_note")
    if cancellation_note is not None and not isinstance(cancellation_note, str):
        msg = "result.cancellation_note must be a string or null"
        raise ProtocolError(msg)
    recovery_note = raw.get("recovery_note")
    if recovery_note is not None and not isinstance(recovery_note, str):
        msg = "result.recovery_note must be a string or null"
        raise ProtocolError(msg)
    return ResultView(
        stdout=stdout,
        stderr=stderr,
        exit_code=exit_code,
        cancellation_note=cancellation_note,
        recovery_note=recovery_note,
    )


def _decode_payload(data: object) -> dict[str, Any]:
    """Decode a stored payload from JSON text or an already-decoded mapping.

    Args:
        data: The JSON object stored in the ``payload`` column, either as a
            raw JSON string or as an already-decoded mapping.

    Returns:
        The decoded mapping.

    Raises:
        ProtocolError: If the payload is not a valid JSON object.
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
    return data


def _parse_version_and_type(data: dict[str, Any]) -> tuple[int, str]:
    """Validate the version and type fields of a payload.

    Args:
        data: The decoded payload mapping.

    Returns:
        The ``(version, type)`` pair.

    Raises:
        ProtocolError: If the version or type is invalid.
    """
    version = data.get("v")
    if version != PROTOCOL_VERSION:
        if version == LEGACY_PROTOCOL_V2:
            msg = (
                "unsupported protocol version: v2 payloads are no longer accepted; "
                "submit protocol v3 process payloads instead"
            )
            raise ProtocolError(msg)
        msg = f"unsupported protocol version: {version!r} (expected {PROTOCOL_VERSION})"
        raise ProtocolError(msg)
    job_type = data.get("type")
    if job_type not in KNOWN_JOB_TYPES:
        msg = f"unknown job type: {job_type!r}"
        raise ProtocolError(msg)
    return int(version), str(job_type)


def parse_payload(data: object) -> JobPayload:
    """Parse and validate a ``command`` job payload against the binding.

    The stored ``payload`` column is opaque text; a raw JSON string is decoded
    before validation. ``output_chunk`` rows are rejected here; use
    :func:`parse_chunk_payload` for those.

    Args:
        data: The JSON object stored in the ``payload`` column, either as a
            raw JSON string or as an already-decoded mapping.

    Returns:
        The parsed and validated command payload.

    Raises:
        ProtocolError: If the payload violates the binding.
    """
    decoded = _decode_payload(data)
    version, job_type = _parse_version_and_type(decoded)
    if job_type != JOB_TYPE_COMMAND:
        msg = f"payload is not a command job: {job_type!r}"
        raise ProtocolError(msg)
    return JobPayload(
        version=version,
        type=job_type,
        request=_parse_request(decoded.get("request")),
        status=_parse_status(decoded.get("state")),
        output=_parse_output_section(decoded.get("output")),
        result=_parse_result(decoded.get("result")),
    )


def parse_chunk_payload(data: object) -> OutputChunk:
    """Parse and validate an ``output_chunk`` payload against the binding.

    Args:
        data: The JSON object stored in the ``payload`` column, either as a
            raw JSON string or as an already-decoded mapping.

    Returns:
        The parsed and validated chunk.

    Raises:
        ProtocolError: If the payload violates the binding.
    """
    decoded = _decode_payload(data)
    _version, job_type = _parse_version_and_type(decoded)
    if job_type != JOB_TYPE_OUTPUT_CHUNK:
        msg = f"payload is not an output_chunk: {job_type!r}"
        raise ProtocolError(msg)
    thread = _parse_uuid(decoded.get("thread"))
    if thread is None:
        msg = "output_chunk must carry an owning thread UUID"
        raise ProtocolError(msg)
    _validate_stream(decoded.get("stream"))
    stream = str(decoded.get("stream"))
    sequence = decoded.get("sequence")
    if not isinstance(sequence, int) or isinstance(sequence, bool) or sequence < 0:
        msg = "output_chunk.sequence must be a non-negative integer"
        raise ProtocolError(msg)
    start = decoded.get("start")
    end = decoded.get("end")
    if not isinstance(start, int) or isinstance(start, bool):
        msg = "output_chunk.start must be an integer"
        raise ProtocolError(msg)
    if not isinstance(end, int) or isinstance(end, bool):
        msg = "output_chunk.end must be an integer"
        raise ProtocolError(msg)
    _validate_offsets(start, end)
    value = decoded.get("value")
    if not isinstance(value, str) or len(value) > OUTPUT_CHUNK_MAX_BYTES:
        msg = f"output_chunk.value must be a string of at most {OUTPUT_CHUNK_MAX_BYTES} characters"
        raise ProtocolError(msg)
    previous = _parse_uuid(decoded.get("previous"))
    return OutputChunk(
        thread=thread,
        stream=stream,
        sequence=int(sequence),
        start=int(start),
        end=int(end),
        value=value,
        previous=previous,
    )
