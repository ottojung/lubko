"""Tests for the versioned Lubko transport JSON binding."""

import json
from typing import Any

import pytest

from lubko.protocol import (
    JOB_TYPE_COMMAND,
    KNOWN_JOB_TYPES,
    KNOWN_STATUSES,
    PROTOCOL_VERSION,
    STATUS_PENDING,
    TWO_COLUMN_INVARIANT,
    JobRequest,
    ProtocolError,
    build_payload,
    parse_payload,
)


def test_invariant_matches_issue_contract() -> None:
    """The invariant names exactly two columns and the JSON payload."""
    assert "exactly two columns forever" in TWO_COLUMN_INVARIANT
    assert "id" in TWO_COLUMN_INVARIANT
    assert "payload" in TWO_COLUMN_INVARIANT


def test_protocol_version_is_one() -> None:
    """Protocol v1 is the current binding."""
    assert PROTOCOL_VERSION == 1
    assert JOB_TYPE_COMMAND in KNOWN_JOB_TYPES
    assert STATUS_PENDING in KNOWN_STATUSES


def test_build_payload_command() -> None:
    """build_payload emits a claimable protocol v1 command payload."""
    payload = build_payload(cwd="/workspace/project", command="git status --short")

    assert payload["v"] == PROTOCOL_VERSION
    assert payload["type"] == JOB_TYPE_COMMAND
    assert payload["request"] == {"cwd": "/workspace/project", "command": "git status --short"}
    assert payload["state"] == {"status": STATUS_PENDING}


def test_build_payload_args() -> None:
    """build_payload emits an argv-style request when args is provided."""
    payload = build_payload(cwd="/workspace/project", args=["git", "status", "--short"])

    assert payload["request"] == {"cwd": "/workspace/project", "args": ["git", "status", "--short"]}


def test_build_payload_rejects_command_and_args() -> None:
    """Providing both command and args is a binding violation."""
    with pytest.raises(ProtocolError, match="not both"):
        build_payload(cwd="/x", command="echo hi", args=["echo", "hi"])


def test_build_payload_rejects_neither() -> None:
    """Providing neither command nor args is a binding violation."""
    with pytest.raises(ProtocolError, match="command or args"):
        build_payload(cwd="/x")


def test_parse_payload_accepts_json_text() -> None:
    """The stored text payload decodes and validates."""
    text = json.dumps({
        "v": PROTOCOL_VERSION,
        "type": JOB_TYPE_COMMAND,
        "request": {"cwd": "/workspace/project", "command": "echo hi"},
        "state": {"status": STATUS_PENDING},
    })

    parsed = parse_payload(text)

    assert parsed.version == PROTOCOL_VERSION
    assert parsed.type == JOB_TYPE_COMMAND
    assert parsed.request == JobRequest(cwd="/workspace/project", command="echo hi", args=None)
    assert parsed.status == STATUS_PENDING


def test_parse_payload_accepts_decoded_mapping() -> None:
    """An already-decoded mapping parses identically to JSON text."""
    data: dict[str, Any] = {
        "v": PROTOCOL_VERSION,
        "type": JOB_TYPE_COMMAND,
        "request": {"cwd": "/x", "args": ["true"]},
        "state": {"status": "running"},
    }

    parsed = parse_payload(data)

    assert parsed.request == JobRequest(cwd="/x", command=None, args=("true",))
    assert parsed.status == "running"


def test_parse_payload_rejects_non_json_text() -> None:
    """Malformed JSON text is a binding violation."""
    with pytest.raises(ProtocolError, match="not valid JSON"):
        parse_payload("{not json")


def test_parse_payload_rejects_unsupported_version() -> None:
    """Unknown protocol versions are rejected."""
    with pytest.raises(ProtocolError, match="unsupported protocol version"):
        parse_payload({"v": 2, "type": JOB_TYPE_COMMAND, "request": {"cwd": "/x"}, "state": {}})


def test_parse_payload_rejects_unknown_type() -> None:
    """Unknown job kinds are rejected."""
    with pytest.raises(ProtocolError, match="unknown job type"):
        parse_payload({"v": 1, "type": "runaway", "request": {}, "state": {}})


def test_parse_payload_rejects_missing_request() -> None:
    """A payload without a request object is a binding violation."""
    with pytest.raises(ProtocolError, match="request object"):
        parse_payload({"v": 1, "type": JOB_TYPE_COMMAND, "state": {}})


def test_parse_payload_rejects_missing_cwd() -> None:
    """A command request without a cwd is a binding violation."""
    with pytest.raises(ProtocolError, match=r"request\.cwd"):
        parse_payload({"v": 1, "type": JOB_TYPE_COMMAND, "request": {"command": "ls"}})


def test_parse_payload_rejects_command_and_args() -> None:
    """A request with both command and args is a binding violation."""
    with pytest.raises(ProtocolError, match="not both"):
        parse_payload({
            "v": 1,
            "type": JOB_TYPE_COMMAND,
            "request": {"cwd": "/x", "command": "ls", "args": ["ls"]},
        })


def test_parse_payload_rejects_missing_state() -> None:
    """A payload without a state object is a binding violation."""
    with pytest.raises(ProtocolError, match="state object"):
        parse_payload({"v": 1, "type": JOB_TYPE_COMMAND, "request": {"cwd": "/x", "command": "ls"}})


def test_parse_payload_rejects_unknown_status() -> None:
    """An unknown job status is a binding violation."""
    with pytest.raises(ProtocolError, match="unknown job status"):
        parse_payload({
            "v": 1,
            "type": JOB_TYPE_COMMAND,
            "request": {"cwd": "/x", "command": "ls"},
            "state": {"status": "exploded"},
        })


def test_parse_payload_rejects_absent_status() -> None:
    """A state without a status is a binding violation."""
    with pytest.raises(ProtocolError, match="unknown job status"):
        parse_payload({
            "v": 1,
            "type": JOB_TYPE_COMMAND,
            "request": {"cwd": "/x", "command": "ls"},
            "state": {},
        })
