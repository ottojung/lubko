"""Protocol v4 payload construction and parsing invariants."""

import json
from typing import Any, cast
from uuid import uuid4

import pytest

from lubko.protocol import (
    OUTPUT_CHUNK_MAX_BYTES,
    OUTPUT_TAIL_MAX_BYTES,
    PROTOCOL_VERSION,
    STATUS_PENDING,
    ProtocolError,
    build_output_chunk_payload,
    build_output_window_payload,
    build_payload,
    parse_chunk_payload,
    parse_payload,
)

SERVER = "srv"


def command_payload() -> dict[str, object]:
    """Return one valid minimal ``command`` payload."""
    return build_payload(server=SERVER, cwd="/srv/jobs", process=["echo", "hi"])


def test_build_payload_shape() -> None:
    """Built command payloads carry the versioned binding fields exactly."""
    payload = command_payload()
    assert payload["v"] == PROTOCOL_VERSION
    assert payload["type"] == "command"
    assert payload["server"] == SERVER
    assert payload["state"] == {"status": STATUS_PENDING}
    assert payload["request"] == {"cwd": "/srv/jobs", "process": ["echo", "hi"]}


def test_round_trip_command() -> None:
    """Built payloads parse back into equivalent validated views."""
    parsed = parse_payload(json.dumps(command_payload()))
    assert parsed.server == SERVER
    assert parsed.request.cwd == "/srv/jobs"
    assert parsed.request.process == ("echo", "hi")
    assert parsed.status == STATUS_PENDING
    assert parsed.output is None
    assert parsed.result is None


def test_rejects_bad_server() -> None:
    """Server identities must be present non-empty strings everywhere."""
    with pytest.raises(ProtocolError):
        build_payload(server="", cwd="/srv/jobs", process=["ls"])
    raw = command_payload()
    raw["server"] = ""
    with pytest.raises(ProtocolError):
        parse_payload(raw)
    del raw["server"]
    with pytest.raises(ProtocolError):
        parse_payload(raw)


def test_accepts_absolute_cwd() -> None:
    """Absolute cwd round-trips through build and parse."""
    payload = build_payload(server=SERVER, cwd="/srv/jobs", process=["echo", "hi"])
    parsed = parse_payload(json.dumps(payload))
    assert parsed.request.cwd == "/srv/jobs"


def test_rejects_relative_cwd_on_build() -> None:
    """Build rejects relative working directories without resolving them."""
    with pytest.raises(ProtocolError, match="absolute"):
        build_payload(server=SERVER, cwd="srv/jobs", process=["echo"])
    with pytest.raises(ProtocolError, match="absolute"):
        build_payload(server=SERVER, cwd="./srv", process=["echo"])
    with pytest.raises(ProtocolError, match="absolute"):
        build_payload(server=SERVER, cwd="../srv", process=["echo"])
    with pytest.raises(ProtocolError, match="non-empty"):
        build_payload(server=SERVER, cwd="", process=["echo"])


def test_rejects_non_string_cwd_on_build() -> None:
    """Build rejects a truthy non-string cwd instead of emitting it."""
    with pytest.raises(ProtocolError):
        build_payload(server=SERVER, cwd=cast("str", 5), process=["echo"])


def test_rejects_relative_cwd_on_parse() -> None:
    """Parse rejects relative working directories without resolving them."""
    for bad in ("srv/jobs", "./srv", "../srv", "", 5, None):
        raw = command_payload()
        raw["request"] = {"cwd": bad, "process": ["echo"]}
        with pytest.raises(ProtocolError):
            parse_payload(raw)


@pytest.mark.parametrize(
    "process",
    [[], ["echo", ""], "echo", [1, 2], None],
)
def test_rejects_invalid_process(process: object) -> None:
    """The process field must be a non-empty array of non-empty strings."""
    raw = command_payload()
    raw["request"] = {"cwd": "/srv/jobs", "process": process}
    with pytest.raises(ProtocolError):
        parse_payload(raw)


def test_rejects_legacy_command_fields() -> None:
    """Legacy v2 ``command``/``args`` request fields are rejected."""
    raw = command_payload()
    raw["request"] = {"cwd": "/srv/jobs", "command": "echo", "args": [], "process": ["x"]}
    with pytest.raises(ProtocolError, match="legacy"):
        parse_payload(raw)


def test_rejects_unknown_version_type_and_status() -> None:
    """Unknown versions, job types, and statuses fail validation."""
    raw = command_payload()
    raw["v"] = PROTOCOL_VERSION + 1
    with pytest.raises(ProtocolError, match="version"):
        parse_payload(raw)
    raw = command_payload()
    raw["type"] = "mystery"
    with pytest.raises(ProtocolError, match="type"):
        parse_payload(raw)
    raw = command_payload()
    raw["state"] = {"status": "quantum"}
    with pytest.raises(ProtocolError, match="status"):
        parse_payload(raw)


def test_rejects_non_object_and_bad_json() -> None:
    """Payloads must decode to JSON objects from raw text or mappings."""
    with pytest.raises(ProtocolError):
        parse_payload("[]")
    with pytest.raises(ProtocolError):
        parse_payload("{not json")


def test_output_window_round_trip_and_bounds() -> None:
    """Output windows carry offsets and honor the raw-span size bound."""
    window = build_output_window_payload(tail="abc", start=0, end=3, previous=None)
    assert window == {"tail": "abc", "start": 0, "end": 3, "previous": None}
    previous = uuid4()
    window = build_output_window_payload(tail="abc", start=1, end=3, previous=previous)
    assert window["previous"] == str(previous)
    with pytest.raises(ProtocolError):
        build_output_window_payload(tail="a", start=5, end=4, previous=None)
    with pytest.raises(ProtocolError):
        build_output_window_payload(
            tail="a" * (OUTPUT_TAIL_MAX_BYTES + 1), start=0, end=0, previous=None
        )
    with pytest.raises(ProtocolError):
        parse_payload(
            json.dumps(
                command_payload()
                | {
                    "output": {
                        "stdout": {
                            "tail": "a" * (OUTPUT_TAIL_MAX_BYTES + 1),
                            "start": 0,
                            "end": 0,
                            "previous": None,
                        }
                    }
                }
            )
        )


def test_output_window_parsed_from_payload() -> None:
    """Embedded output sections parse into per-stream windows."""
    raw = command_payload()
    raw["output"] = {
        "stdout": {"tail": "out", "start": 0, "end": 3, "previous": None},
    }
    parsed = parse_payload(json.dumps(raw))
    assert parsed.output is not None
    assert parsed.output.stdout is not None
    assert parsed.output.stdout.tail == "out"
    assert parsed.output.stderr is None


def test_result_section_parsing() -> None:
    """Terminal results validate their bounded fields and integer exit codes."""
    raw = command_payload()
    raw["result"] = {
        "stdout": "",
        "stderr": "",
        "exit_code": 0,
        "cancellation_note": None,
        "recovery_note": None,
    }
    parsed = parse_payload(json.dumps(raw))
    assert parsed.result is not None
    assert parsed.result.exit_code == 0
    result = raw["result"]
    assert isinstance(result, dict)
    result["exit_code"] = True
    with pytest.raises(ProtocolError, match="exit_code"):
        parse_payload(json.dumps(raw))


def test_chunk_round_trip() -> None:
    """Output chunks serialize and parse losslessly."""
    payload = build_output_chunk_payload(
        server=SERVER,
        thread=uuid4(),
        stream="stdout",
        sequence=0,
        start=0,
        end=2,
        value="ok",
        previous=None,
    )
    chunk = parse_chunk_payload(payload)
    assert chunk.value == "ok"
    assert chunk.stream == "stdout"
    assert chunk.previous is None


def test_chunk_validation_errors() -> None:
    """Chunk streams, sequences, sizes, and offsets are strictly validated."""
    thread = uuid4()
    base: dict[str, Any] = {
        "server": SERVER,
        "thread": thread,
        "stream": "stdout",
        "sequence": 0,
        "start": 0,
        "end": 2,
        "value": "ok",
        "previous": None,
    }
    with pytest.raises(ProtocolError, match="stream"):
        build_output_chunk_payload(**base | {"stream": "stdin"})
    with pytest.raises(ProtocolError, match="sequence"):
        build_output_chunk_payload(**base | {"sequence": -1})
    with pytest.raises(ProtocolError, match="precedes"):
        build_output_chunk_payload(**base | {"start": 9, "end": 1})
    with pytest.raises(ProtocolError, match="value"):
        build_output_chunk_payload(**base | {"value": "x" * (OUTPUT_CHUNK_MAX_BYTES + 1)})
    overlong = build_output_chunk_payload(**base)
    overlong["value"] = "x" * (OUTPUT_CHUNK_MAX_BYTES + 1)
    with pytest.raises(ProtocolError, match="value"):
        parse_chunk_payload(overlong)


def test_chunk_requires_thread_and_kind_separation() -> None:
    """Chunks need an owning thread and never parse as commands."""
    raw = build_output_chunk_payload(
        server=SERVER,
        thread=uuid4(),
        stream="stdout",
        sequence=0,
        start=0,
        end=2,
        value="ok",
        previous=None,
    )
    del raw["thread"]
    with pytest.raises(ProtocolError, match="thread"):
        parse_chunk_payload(raw)
    with pytest.raises(ProtocolError, match="output_chunk"):
        parse_payload(raw)


def test_output_window_span_exact_limit_accepted() -> None:
    """A live window whose raw span exactly equals the bound is accepted."""
    assert (
        build_output_window_payload(
            tail="x" * OUTPUT_TAIL_MAX_BYTES, start=0, end=OUTPUT_TAIL_MAX_BYTES, previous=None
        )["end"]
        == OUTPUT_TAIL_MAX_BYTES
    )
    raw = {
        "tail": "x" * OUTPUT_TAIL_MAX_BYTES,
        "start": 0,
        "end": OUTPUT_TAIL_MAX_BYTES,
        "previous": None,
    }
    parsed_win = parse_payload(json.dumps(command_payload() | {"output": {"stdout": raw}}))
    assert parsed_win.output is not None
    assert parsed_win.output.stdout is not None
    assert parsed_win.output.stdout.end == OUTPUT_TAIL_MAX_BYTES


def test_output_window_span_over_limit_rejected() -> None:
    """A live window whose raw span exceeds the bound is rejected by builder and parser.

    The tail text itself stays within the character bound so the failure is
    specifically the raw-span check, independent of the text-length check.
    """
    with pytest.raises(ProtocolError):
        build_output_window_payload(
            tail="x",
            start=0,
            end=OUTPUT_TAIL_MAX_BYTES + 1,
            previous=None,
        )
    with pytest.raises(ProtocolError):
        parse_payload(
            json.dumps(
                command_payload()
                | {
                    "output": {
                        "stdout": {
                            "tail": "x",
                            "start": 0,
                            "end": OUTPUT_TAIL_MAX_BYTES + 1,
                            "previous": None,
                        }
                    }
                }
            )
        )


def test_output_chunk_span_exact_limit_accepted() -> None:
    """A chunk whose raw span exactly equals the bound is accepted."""
    payload = build_output_chunk_payload(
        server=SERVER,
        thread=uuid4(),
        stream="stdout",
        sequence=0,
        start=0,
        end=OUTPUT_CHUNK_MAX_BYTES,
        value="x" * OUTPUT_CHUNK_MAX_BYTES,
        previous=None,
    )
    assert parse_chunk_payload(payload).end == OUTPUT_CHUNK_MAX_BYTES


def test_output_chunk_span_over_limit_rejected() -> None:
    """A chunk whose raw span exceeds the bound is rejected by builder and parser."""
    with pytest.raises(ProtocolError):
        build_output_chunk_payload(
            server=SERVER,
            thread=uuid4(),
            stream="stdout",
            sequence=0,
            start=0,
            end=OUTPUT_CHUNK_MAX_BYTES + 1,
            value="x",
            previous=None,
        )
    with pytest.raises(ProtocolError):
        parse_chunk_payload({
            "v": PROTOCOL_VERSION,
            "type": "output_chunk",
            "server": SERVER,
            "thread": str(uuid4()),
            "stream": "stdout",
            "sequence": 0,
            "start": 0,
            "end": OUTPUT_CHUNK_MAX_BYTES + 1,
            "value": "x",
            "previous": None,
        })


def test_output_span_replacement_semantics_preserved() -> None:
    """Windows bound the raw span, not the re-encoded text length.

    Invalid UTF-8 is replaced on decode, so the re-encoded text length need
    not equal ``end - start``. The bound still applies only to the raw span.
    """
    invalid = b"\xff" * 10
    text = invalid.decode("utf-8", errors="replace")
    assert len(text.encode("utf-8")) != len(invalid)
    window = build_output_window_payload(tail=text, start=0, end=len(invalid), previous=None)
    assert window["end"] - window["start"] == len(invalid)
    parsed = parse_payload(json.dumps(command_payload() | {"output": {"stdout": window}}))
    assert parsed.output is not None
    assert parsed.output.stdout is not None
    assert parsed.output.stdout.end - parsed.output.stdout.start == len(invalid)

    with pytest.raises(ProtocolError):
        build_output_window_payload(
            tail=text, start=0, end=OUTPUT_TAIL_MAX_BYTES + 1, previous=None
        )
