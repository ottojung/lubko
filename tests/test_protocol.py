"""Tests for the versioned Lubko transport JSON binding."""

import json
from typing import Any
from uuid import UUID, uuid4

import pytest

from lubko.protocol import (
    CHUNK_STREAMS,
    JOB_TYPE_COMMAND,
    JOB_TYPE_OUTPUT_CHUNK,
    KNOWN_JOB_TYPES,
    KNOWN_STATUSES,
    OUTPUT_CHUNK_MAX_BYTES,
    OUTPUT_TAIL_MAX_BYTES,
    PROTOCOL_VERSION,
    STATUS_PENDING,
    TWO_COLUMN_INVARIANT,
    JobRequest,
    OutputWindow,
    ProtocolError,
    build_output_chunk_payload,
    build_output_window_payload,
    build_payload,
    parse_chunk_payload,
    parse_payload,
)


def test_invariant_matches_issue_contract() -> None:
    """The invariant names exactly two columns and the JSON payload."""
    assert "exactly two columns forever" in TWO_COLUMN_INVARIANT
    assert "id" in TWO_COLUMN_INVARIANT
    assert "payload" in TWO_COLUMN_INVARIANT


def test_protocol_version_is_three() -> None:
    """Protocol v3 is the current binding."""
    assert PROTOCOL_VERSION == 3
    assert JOB_TYPE_COMMAND in KNOWN_JOB_TYPES
    assert JOB_TYPE_OUTPUT_CHUNK in KNOWN_JOB_TYPES
    assert STATUS_PENDING in KNOWN_STATUSES
    assert CHUNK_STREAMS == ("stdout", "stderr")


def test_build_payload_process() -> None:
    """build_payload emits a claimable protocol v3 command payload."""
    payload = build_payload(cwd="/workspace/project", process=["git", "status", "--short"])

    assert payload["v"] == PROTOCOL_VERSION
    assert payload["type"] == JOB_TYPE_COMMAND
    assert payload["request"] == {
        "cwd": "/workspace/project",
        "process": ["git", "status", "--short"],
    }
    assert payload["state"] == {"status": STATUS_PENDING}


def test_build_payload_process_element_is_empty() -> None:
    """An empty string inside the argv array is a binding violation."""
    with pytest.raises(ProtocolError, match="non-empty"):
        build_payload(cwd="/x", process=["git", ""])


def test_build_payload_rejects_non_string_process() -> None:
    """build_payload runtime-validates process is a list of strings, not truthy items."""
    with pytest.raises(ProtocolError, match=r"request\.process"):
        build_payload(cwd="/x", process=[1, 2])  # type: ignore[list-item]
    with pytest.raises(ProtocolError, match=r"request\.process"):
        build_payload(cwd="/x", process=(1, "git"))  # type: ignore[arg-type]


def test_build_payload_rejects_empty_process() -> None:
    """A missing or empty request.process is a binding violation."""
    with pytest.raises(ProtocolError, match=r"request\.process"):
        build_payload(cwd="/x", process=[])


def test_build_output_window_payload() -> None:
    """A live tail window carries bounded tail text and byte offsets."""
    previous = uuid4()
    window = build_output_window_payload(tail="hello", start=100, end=105, previous=previous)

    assert window == {
        "tail": "hello",
        "start": 100,
        "end": 105,
        "previous": str(previous),
    }


def test_build_output_window_payload_null_previous() -> None:
    """A stream without chunks records a null previous pointer."""
    window = build_output_window_payload(tail="hi", start=0, end=2, previous=None)
    assert window["previous"] is None


def test_build_output_window_payload_rejects_oversized_tail() -> None:
    """A tail larger than the strict bound is rejected."""
    with pytest.raises(ProtocolError, match="at most"):
        build_output_window_payload(
            tail="x" * (OUTPUT_TAIL_MAX_BYTES + 1),
            start=0,
            end=OUTPUT_TAIL_MAX_BYTES + 1,
            previous=None,
        )


def test_build_output_window_payload_rejects_bad_offsets() -> None:
    """Negative or inverted offsets are rejected."""
    with pytest.raises(ProtocolError, match="non-negative"):
        build_output_window_payload(tail="hi", start=-1, end=1, previous=None)
    with pytest.raises(ProtocolError, match="precedes"):
        build_output_window_payload(tail="hi", start=5, end=1, previous=None)


def test_build_output_chunk_payload() -> None:
    """A chunk payload carries explicit ownership and immutable offsets."""
    thread = uuid4()
    previous = uuid4()
    chunk = build_output_chunk_payload(
        thread=thread,
        stream="stdout",
        sequence=17,
        start=15342,
        end=19342,
        value="historical output",
        previous=previous,
    )

    assert chunk["v"] == PROTOCOL_VERSION
    assert chunk["type"] == JOB_TYPE_OUTPUT_CHUNK
    assert chunk["thread"] == str(thread)
    assert chunk["stream"] == "stdout"
    assert chunk["sequence"] == 17
    assert chunk["start"] == 15342
    assert chunk["end"] == 19342
    assert chunk["value"] == "historical output"
    assert chunk["previous"] == str(previous)


def test_build_output_chunk_payload_rejects_unknown_stream() -> None:
    """Only stdout and stderr are valid chunk streams."""
    with pytest.raises(ProtocolError, match="output stream"):
        build_output_chunk_payload(
            thread=uuid4(),
            stream="logs",
            sequence=0,
            start=0,
            end=1,
            value="x",
            previous=None,
        )


def test_build_output_chunk_payload_rejects_oversized_value() -> None:
    """A chunk larger than the strict bound is rejected."""
    with pytest.raises(ProtocolError, match="at most"):
        build_output_chunk_payload(
            thread=uuid4(),
            stream="stderr",
            sequence=0,
            start=0,
            end=OUTPUT_CHUNK_MAX_BYTES + 1,
            value="x" * (OUTPUT_CHUNK_MAX_BYTES + 1),
            previous=None,
        )


def test_build_output_chunk_payload_rejects_bad_offsets() -> None:
    """A chunk with an end before its start is rejected."""
    with pytest.raises(ProtocolError, match="precedes"):
        build_output_chunk_payload(
            thread=uuid4(),
            stream="stdout",
            sequence=0,
            start=100,
            end=10,
            value="x",
            previous=None,
        )


def test_parse_payload_accepts_json_text() -> None:
    """The stored text payload decodes and validates."""
    text = json.dumps({
        "v": PROTOCOL_VERSION,
        "type": JOB_TYPE_COMMAND,
        "request": {"cwd": "/workspace/project", "process": ["echo", "hi"]},
        "state": {"status": STATUS_PENDING},
    })

    parsed = parse_payload(text)

    assert parsed.version == PROTOCOL_VERSION
    assert parsed.type == JOB_TYPE_COMMAND
    assert parsed.request == JobRequest(cwd="/workspace/project", process=("echo", "hi"))
    assert parsed.status == STATUS_PENDING
    assert parsed.output is None
    assert parsed.result is None


def test_parse_payload_accepts_decoded_mapping() -> None:
    """An already-decoded mapping parses identically to JSON text."""
    data: dict[str, Any] = {
        "v": PROTOCOL_VERSION,
        "type": JOB_TYPE_COMMAND,
        "request": {"cwd": "/x", "process": ["true"]},
        "state": {"status": "running"},
    }

    parsed = parse_payload(data)

    assert parsed.request == JobRequest(cwd="/x", process=("true",))
    assert parsed.status == "running"


def test_parse_payload_accepts_bounded_output_and_result() -> None:
    """A root job with bounded live tails and a terminal result parses."""
    tail = "x" * OUTPUT_TAIL_MAX_BYTES
    data: dict[str, Any] = {
        "v": PROTOCOL_VERSION,
        "type": JOB_TYPE_COMMAND,
        "request": {"cwd": "/x", "process": ["true"]},
        "state": {"status": "succeeded"},
        "output": {
            "stdout": {"tail": tail, "start": 0, "end": OUTPUT_TAIL_MAX_BYTES, "previous": None},
            "stderr": {"tail": "", "start": 0, "end": 0, "previous": None},
        },
        "result": {
            "stdout": tail,
            "stderr": "",
            "exit_code": 0,
            "cancellation_note": None,
            "recovery_note": None,
        },
    }

    parsed = parse_payload(data)

    assert parsed.output is not None
    assert parsed.output.stdout == OutputWindow(
        tail=tail, start=0, end=OUTPUT_TAIL_MAX_BYTES, previous=None
    )
    assert parsed.output.stderr == OutputWindow(tail="", start=0, end=0, previous=None)
    assert parsed.result is not None
    assert parsed.result.exit_code == 0


def test_parse_payload_rejects_oversized_tail() -> None:
    """An oversized live tail violates the strict size bound."""
    with pytest.raises(ProtocolError, match="at most"):
        parse_payload({
            "v": PROTOCOL_VERSION,
            "type": JOB_TYPE_COMMAND,
            "request": {"cwd": "/x", "process": ["ls"]},
            "state": {"status": "running"},
            "output": {
                "stdout": {
                    "tail": "x" * (OUTPUT_TAIL_MAX_BYTES + 1),
                    "start": 0,
                    "end": OUTPUT_TAIL_MAX_BYTES + 1,
                    "previous": None,
                }
            },
        })


def test_parse_payload_rejects_non_json_text() -> None:
    """Malformed JSON text is a binding violation."""
    with pytest.raises(ProtocolError, match="not valid JSON"):
        parse_payload("{not json")


def test_parse_payload_rejects_unsupported_version() -> None:
    """Legacy and unknown protocol versions (v1, v2, and beyond) are rejected."""
    for version in (1, 2, 4):
        with pytest.raises(ProtocolError, match="unsupported protocol version"):
            parse_payload({
                "v": version,
                "type": JOB_TYPE_COMMAND,
                "request": {"cwd": "/x", "process": ["ls"]},
                "state": {"status": STATUS_PENDING},
            })


def test_parse_payload_rejects_legacy_v2_command_form() -> None:
    """A payload carrying only the legacy request.command field is v2-era and rejected."""
    with pytest.raises(ProtocolError, match="unsupported protocol version"):
        parse_payload({
            "v": 2,
            "type": JOB_TYPE_COMMAND,
            "request": {"cwd": "/x", "command": "ls"},
            "state": {"status": STATUS_PENDING},
        })


def test_parse_payload_rejects_unknown_type() -> None:
    """Unknown job kinds are rejected."""
    with pytest.raises(ProtocolError, match="unknown job type"):
        parse_payload({"v": 3, "type": "runaway", "request": {}, "state": {}})


def test_parse_payload_rejects_chunk_rows() -> None:
    """output_chunk rows are parsed by the chunk parser, never as command rows."""
    with pytest.raises(ProtocolError, match="not a command job"):
        parse_payload(
            build_output_chunk_payload(
                thread=uuid4(),
                stream="stdout",
                sequence=0,
                start=0,
                end=1,
                value="x",
                previous=None,
            )
        )


def test_parse_payload_rejects_missing_request() -> None:
    """A payload without a request object is a binding violation."""
    with pytest.raises(ProtocolError, match="request object"):
        parse_payload({"v": 3, "type": JOB_TYPE_COMMAND, "state": {}})


def test_parse_payload_rejects_missing_cwd() -> None:
    """A request without a cwd is a binding violation."""
    with pytest.raises(ProtocolError, match=r"request\.cwd"):
        parse_payload({"v": 3, "type": JOB_TYPE_COMMAND, "request": {"process": ["ls"]}})


def test_parse_payload_rejects_missing_process() -> None:
    """A request without request.process is a binding violation."""
    with pytest.raises(ProtocolError, match=r"request\.process"):
        parse_payload({"v": 3, "type": JOB_TYPE_COMMAND, "request": {"cwd": "/x"}})


def test_parse_payload_rejects_legacy_command_key() -> None:
    """The legacy request.command key is rejected even alongside a valid process."""
    with pytest.raises(ProtocolError, match="legacy protocol v2 fields"):
        parse_payload({
            "v": 3,
            "type": JOB_TYPE_COMMAND,
            "request": {"cwd": "/x", "process": ["ls"], "command": "ls"},
        })


def test_parse_payload_rejects_legacy_args_key() -> None:
    """The legacy request.args key is rejected even alongside a valid process."""
    with pytest.raises(ProtocolError, match="legacy protocol v2 fields"):
        parse_payload({
            "v": 3,
            "type": JOB_TYPE_COMMAND,
            "request": {"cwd": "/x", "process": ["ls"], "args": ["ls"]},
        })


def test_parse_payload_rejects_missing_state() -> None:
    """A payload without a state object is a binding violation."""
    with pytest.raises(ProtocolError, match="state object"):
        parse_payload({
            "v": 3,
            "type": JOB_TYPE_COMMAND,
            "request": {"cwd": "/x", "process": ["ls"]},
        })


def test_parse_payload_rejects_unknown_status() -> None:
    """An unknown job status is a binding violation."""
    with pytest.raises(ProtocolError, match="unknown job status"):
        parse_payload({
            "v": 3,
            "type": JOB_TYPE_COMMAND,
            "request": {"cwd": "/x", "process": ["ls"]},
            "state": {"status": "exploded"},
        })


def test_parse_payload_rejects_absent_status() -> None:
    """A state without a status is a binding violation."""
    with pytest.raises(ProtocolError, match="unknown job status"):
        parse_payload({
            "v": 3,
            "type": JOB_TYPE_COMMAND,
            "request": {"cwd": "/x", "process": ["ls"]},
            "state": {},
        })


def test_parse_chunk_payload_roundtrip() -> None:
    """A built chunk payload parses back to the identical chunk."""
    thread = uuid4()
    previous = uuid4()
    payload = build_output_chunk_payload(
        thread=thread,
        stream="stderr",
        sequence=3,
        start=4000,
        end=8000,
        value="old",
        previous=previous,
    )

    chunk = parse_chunk_payload(payload)

    assert chunk.thread == thread
    assert chunk.stream == "stderr"
    assert chunk.sequence == 3
    assert chunk.start == 4000
    assert chunk.end == 8000
    assert chunk.value == "old"
    assert chunk.previous == previous


def test_parse_chunk_payload_accepts_json_text() -> None:
    """A stored chunk payload decodes and validates from JSON text."""
    payload = build_output_chunk_payload(
        thread=uuid4(),
        stream="stdout",
        sequence=0,
        start=0,
        end=5,
        value="hello",
        previous=None,
    )
    chunk = parse_chunk_payload(json.dumps(payload))
    assert chunk.value == "hello"


def test_parse_chunk_payload_rejects_missing_thread() -> None:
    """A chunk without explicit thread ownership is rejected."""
    with pytest.raises(ProtocolError, match="thread"):
        parse_chunk_payload({
            "v": PROTOCOL_VERSION,
            "type": JOB_TYPE_OUTPUT_CHUNK,
            "stream": "stdout",
            "sequence": 0,
            "start": 0,
            "end": 1,
            "value": "x",
        })


def test_parse_chunk_payload_rejects_bad_previous_uuid() -> None:
    """A chunk whose previous pointer is not a UUID is rejected."""
    with pytest.raises(ProtocolError, match="UUID"):
        parse_chunk_payload({
            "v": PROTOCOL_VERSION,
            "type": JOB_TYPE_OUTPUT_CHUNK,
            "thread": str(uuid4()),
            "stream": "stdout",
            "sequence": 0,
            "start": 0,
            "end": 1,
            "value": "x",
            "previous": "not-a-uuid",
        })


def test_parse_chunk_payload_rejects_command_rows() -> None:
    """A command payload is rejected by the chunk parser."""
    with pytest.raises(ProtocolError, match="not an output_chunk"):
        parse_chunk_payload(build_payload(cwd="/x", process=["ls"]))


def test_parse_chunk_payload_rejects_oversized_value() -> None:
    """An oversized chunk value is rejected."""
    with pytest.raises(ProtocolError, match="at most"):
        parse_chunk_payload({
            "v": PROTOCOL_VERSION,
            "type": JOB_TYPE_OUTPUT_CHUNK,
            "thread": str(uuid4()),
            "stream": "stdout",
            "sequence": 0,
            "start": 0,
            "end": OUTPUT_CHUNK_MAX_BYTES + 1,
            "value": "x" * (OUTPUT_CHUNK_MAX_BYTES + 1),
        })


def test_output_offsets_use_uuid_previous() -> None:
    """The window previous pointer parses into the returned OutputWindow."""
    previous = uuid4()
    parsed = parse_payload({
        "v": PROTOCOL_VERSION,
        "type": JOB_TYPE_COMMAND,
        "request": {"cwd": "/x", "process": ["ls"]},
        "state": {"status": "running"},
        "output": {"stdout": {"tail": "hi", "start": 0, "end": 2, "previous": str(previous)}},
    })
    assert parsed.output is not None
    assert parsed.output.stdout is not None
    assert parsed.output.stdout.previous == UUID(str(previous))
