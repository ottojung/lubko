"""Protocol v4 payload construction and parsing invariants."""

import json
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
    """Check that build payload shape holds."""
    payload = command_payload()
    assert payload["v"] == PROTOCOL_VERSION
    assert payload["type"] == "command"
    assert payload["server"] == SERVER
    assert payload["state"] == {"status": STATUS_PENDING}
    assert payload["request"] == {"cwd": "/srv/jobs", "process": ["echo", "hi"]}


def test_round_trip_command() -> None:
    """Check that round trip command holds."""
    parsed = parse_payload(json.dumps(command_payload()))
    assert parsed.server == SERVER
    assert parsed.request.cwd == "/srv/jobs"
    assert parsed.request.process == ("echo", "hi")
    assert parsed.status == STATUS_PENDING
    assert parsed.output is None
    assert parsed.result is None


def test_rejects_bad_server() -> None:
    """Check that rejects bad server holds."""
    with pytest.raises(ProtocolError):
        build_payload(server="", cwd="/srv/jobs", process=["ls"])
    raw = command_payload()
    raw["server"] = ""
    with pytest.raises(ProtocolError):
        parse_payload(raw)
    del raw["server"]
    with pytest.raises(ProtocolError):
        parse_payload(raw)


@pytest.mark.parametrize(
    "process",
    [[], ["echo", ""], "echo", [1, 2], None],
)
def test_rejects_invalid_process(process: object) -> None:
    """Check that rejects invalid process holds."""
    with pytest.raises(ProtocolError):
        build_payload(server=SERVER, cwd="/srv/jobs", process=process)  # type: ignore[arg-type]


def test_rejects_legacy_command_fields() -> None:
    """Check that rejects legacy command fields holds."""
    raw = command_payload()
    raw["request"] = {"cwd": "/srv/jobs", "command": "echo", "args": [], "process": ["x"]}
    with pytest.raises(ProtocolError, match="legacy"):
        parse_payload(raw)


def test_rejects_unknown_version_type_and_status() -> None:
    """Check that rejects unknown version type and status holds."""
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
    """Check that rejects non object and bad json holds."""
    with pytest.raises(ProtocolError):
        parse_payload("[]")
    with pytest.raises(ProtocolError):
        parse_payload("{not json")


def test_output_window_round_trip_and_bounds() -> None:
    """Check that output window round trip and bounds holds."""
    window = build_output_window_payload(tail="abc", start=0, end=3, previous=None)
    assert window == {"tail": "abc", "start": 0, "end": 3, "previous": None}
    previous = uuid4()
    window = build_output_window_payload(tail="abc", start=1, end=3, previous=previous)
    assert window["previous"] == str(previous)
    with pytest.raises(ProtocolError):
        build_output_window_payload(
            tail="a" * (OUTPUT_TAIL_MAX_BYTES + 1), start=0, end=0, previous=None
        )
    with pytest.raises(ProtocolError):
        build_output_window_payload(tail="a", start=5, end=4, previous=None)


def test_output_window_parsed_from_payload() -> None:
    """Check that output window parsed from payload holds."""
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
    """Check that result section parsing holds."""
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


def chunk_kwargs() -> dict[str, object]:
    """Return valid keyword arguments for one minimal output chunk."""
    thread = uuid4()
    return {
        "server": SERVER,
        "thread": thread,
        "stream": "stdout",
        "sequence": 0,
        "start": 0,
        "end": 2,
        "value": "ok",
        "previous": None,
    }


def test_chunk_round_trip() -> None:
    """Check that chunk round trip holds."""
    kwargs = chunk_kwargs()
    chunk = parse_chunk_payload(build_output_chunk_payload(**kwargs))  # type: ignore[arg-type]
    assert chunk.value == "ok"
    assert chunk.stream == "stdout"
    assert chunk.previous is None


def test_chunk_validation_errors() -> None:
    """Check that chunk validation errors holds."""
    bad_stream = dict(chunk_kwargs(), stream="stdin")
    with pytest.raises(ProtocolError, match="stream"):
        build_output_chunk_payload(**bad_stream)  # type: ignore[arg-type]
    negative = dict(chunk_kwargs(), sequence=-1)
    with pytest.raises(ProtocolError, match="sequence"):
        build_output_chunk_payload(**negative)  # type: ignore[arg-type]
    oversized = dict(chunk_kwargs(), value="x" * (OUTPUT_CHUNK_MAX_BYTES + 1))
    with pytest.raises(ProtocolError, match="value"):
        build_output_chunk_payload(**oversized)  # type: ignore[arg-type]
    reversed_offsets = dict(chunk_kwargs(), start=9, end=1)
    with pytest.raises(ProtocolError, match="precedes"):
        build_output_chunk_payload(**reversed_offsets)  # type: ignore[arg-type]


def test_chunk_requires_thread_and_kind_separation() -> None:
    """Check that chunk requires thread and kind separation holds."""
    raw = build_output_chunk_payload(**chunk_kwargs())  # type: ignore[arg-type]
    del raw["thread"]
    with pytest.raises(ProtocolError, match="thread"):
        parse_chunk_payload(raw)
    with pytest.raises(ProtocolError, match="output_chunk"):
        parse_payload(raw)
