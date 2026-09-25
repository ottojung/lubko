"""Borys board protocol, concurrency, and CLI invariants."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, cast

import pytest

import lubko.board as board_module
from lubko.board import (
    MAX_SAFE_INTEGER,
    Board,
    BoardClient,
    BoardError,
    BoardIssue,
    BoardResource,
    HttpResponse,
    StandardHttpClient,
    parse_board,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from lubko.board import BoardMessage, IssueState


@dataclass(slots=True)
class RecordedRequest:
    """One request captured by the fake transport."""

    method: str
    url: str
    headers: dict[str, str]
    body: bytes | None


class FakeHttp:
    """Deterministic queued HTTP transport."""

    def __init__(self, responses: list[HttpResponse]) -> None:
        """Initialize with responses returned in order."""
        self.responses = list(responses)
        self.requests: list[RecordedRequest] = []

    def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str],
        body: bytes | None,
    ) -> HttpResponse:
        """Record a request and return the next queued response.

        Returns:
            The next queued response.

        Raises:
            AssertionError: If the response queue is exhausted.
        """
        self.requests.append(
            RecordedRequest(
                method=method,
                url=url,
                headers=dict(headers),
                body=body,
            )
        )
        if not self.responses:
            msg = "fake HTTP response queue exhausted"
            raise AssertionError(msg)
        return self.responses.pop(0)


def issue(
    number: int,
    *,
    title: str | None = None,
    state: IssueState = "open",
    updated_at: str = "2026-09-24T10:00:00.000Z",
    messages: list[BoardMessage] | None = None,
) -> BoardIssue:
    """Build one valid issue for tests.

    Returns:
        A valid issue.
    """
    return BoardIssue(
        number=number,
        title=title or f"Issue {number}",
        body="",
        state=state,
        createdAt="2026-09-24T10:00:00.000Z",
        updatedAt=updated_at,
        messages=list(messages or []),
    )


def board(
    *,
    next_issue: int = 2,
    issues: list[BoardIssue] | None = None,
    resources: list[BoardResource] | None = None,
) -> Board:
    """Build one valid board for tests.

    Returns:
        A valid board.
    """
    return Board(
        schemaVersion=2,
        nextIssueNumber=next_issue,
        issues=list(issues) if issues is not None else [issue(1)],
        resources=list(resources or []),
    )


def response(status: int, value: object | None = None, *, etag: str | None = None) -> HttpResponse:
    """Build a fake JSON HTTP response.

    Returns:
        The fake response.
    """
    headers = {} if etag is None else {"ETag": etag}
    body_bytes = b"" if value is None else json.dumps(value).encode()
    return HttpResponse(status=status, headers=headers, body=body_bytes)


def decode_request_body(request: RecordedRequest) -> Board:
    """Decode one captured PUT body as a board.

    Returns:
        The decoded board.

    Raises:
        AssertionError: If the request has no body.
    """
    if request.body is None:
        msg = "expected request body"
        raise AssertionError(msg)
    return parse_board(cast("object", json.loads(request.body)))


def fixed_now() -> datetime:
    """Return a deterministic clock value before the test messages.

    Returns:
        The fixed UTC datetime.
    """
    return datetime(2026, 9, 24, 9, 0, tzinfo=UTC)


def test_schema_is_strict_and_rejects_assignment() -> None:
    """Unknown issue fields, including assignment, are incompatible with Borys v1."""
    raw = cast("dict[str, object]", json.loads(json.dumps(board())))
    raw_issue = cast("dict[str, object]", cast("list[object]", raw["issues"])[0])
    raw_issue["assignee"] = "agent-a"
    with pytest.raises(BoardError, match="malformed issue"):
        parse_board(raw)


def test_schema_matches_javascript_scalar_semantics() -> None:
    """Boolean, unsafe-integer, and non-string fields are rejected like the UI."""
    boolean_version = cast("dict[str, object]", json.loads(json.dumps(board())))
    boolean_version["schemaVersion"] = True
    with pytest.raises(BoardError, match="malformed board"):
        parse_board(boolean_version)

    unsafe_counter = cast("dict[str, object]", json.loads(json.dumps(board())))
    unsafe_counter["nextIssueNumber"] = MAX_SAFE_INTEGER + 1
    with pytest.raises(BoardError, match="malformed board"):
        parse_board(unsafe_counter)

    invalid_state = cast("dict[str, object]", json.loads(json.dumps(board())))
    raw_issue = cast("dict[str, object]", cast("list[object]", invalid_state["issues"])[0])
    raw_issue["state"] = []
    with pytest.raises(BoardError, match="malformed issue"):
        parse_board(invalid_state)


def test_schema_missing_required_issue_field_is_controlled_error() -> None:
    """Missing issue keys fail as schema errors rather than raw mapping errors."""
    raw = cast("dict[str, object]", json.loads(json.dumps(board())))
    raw_issue = cast("dict[str, object]", cast("list[object]", raw["issues"])[0])
    del raw_issue["updatedAt"]
    with pytest.raises(BoardError, match="malformed issue"):
        parse_board(raw)


def test_invalid_capability_fails_before_network() -> None:
    """Malformed bearer capabilities never reach Skrynia."""
    fake = FakeHttp([])
    client = BoardClient(http=fake, capability="not-a-capability")

    with pytest.raises(BoardError, match="64 hexadecimal"):
        client.close(1)
    assert fake.requests == []


def test_invalid_board_url_port_is_controlled_error() -> None:
    """Malformed URL ports fail as board errors before opening a connection."""
    client = StandardHttpClient()

    with pytest.raises(BoardError, match="invalid port"):
        client.request(
            "GET",
            "https://example.invalid:not-a-port/store/borys/board-v1",
            headers={},
            body=None,
        )


def test_read_only_load_requires_no_capability() -> None:
    """Public reads work without any write capability."""
    fake = FakeHttp([response(200, board(), etag='"v1"')])
    client = BoardClient(http=fake)

    assert client.list_issues() == [issue(1)]
    assert fake.requests[0].method == "GET"
    assert "X-Skrynia-Capability" not in fake.requests[0].headers


def test_missing_etag_is_rejected() -> None:
    """A read without an ETag is never accepted as a safe mutation base."""
    fake = FakeHttp([response(200, board())])
    client = BoardClient(http=fake)

    with pytest.raises(BoardError, match="no ETag"):
        client.load_board()


def test_mutation_without_capability_does_not_touch_network() -> None:
    """Mutations fail closed before any request when the capability is absent."""
    fake = FakeHttp([])
    client = BoardClient(http=fake)

    with pytest.raises(BoardError, match="capability"):
        client.create_issue("No write access")
    assert fake.requests == []


def test_create_uses_cas_headers_and_confirms_committed_state() -> None:
    """Create sends both authorization and compare-and-swap headers."""
    initial = board()
    committed = board(
        next_issue=3,
        issues=[issue(1), issue(2, title="Created")],
    )
    fake = FakeHttp([
        response(200, initial, etag='"v1"'),
        response(200),
        response(200, committed, etag='"v2"'),
    ])
    client = BoardClient(
        capability="a" * 64,
        http=fake,
        now=lambda: datetime(2026, 9, 24, 10, 5, tzinfo=UTC),
    )

    created = client.create_issue("Created")

    assert created["number"] == 2
    put = fake.requests[1]
    assert put.method == "PUT"
    assert put.headers["If-Match"] == '"v1"'
    assert put.headers["X-Skrynia-Capability"] == "a" * 64


def test_create_replays_against_latest_counter_after_conflict() -> None:
    """A conflicting creator forces issue-number allocation from the latest board."""
    initial = board()
    winner = board(
        next_issue=3,
        issues=[issue(1), issue(2, title="Winner")],
    )
    committed = board(
        next_issue=4,
        issues=[issue(1), issue(2, title="Winner"), issue(3, title="Mine")],
    )
    fake = FakeHttp([
        response(200, initial, etag='"v1"'),
        response(412),
        response(200, winner, etag='"v2"'),
        response(200),
        response(200, committed, etag='"v3"'),
    ])
    client = BoardClient(
        capability="b" * 64,
        http=fake,
        now=lambda: datetime(2026, 9, 24, 10, 5, tzinfo=UTC),
    )

    created = client.create_issue("Mine")

    assert created["number"] == 3
    first_candidate = decode_request_body(fake.requests[1])
    second_candidate = decode_request_body(fake.requests[3])
    assert first_candidate["issues"][-1]["number"] == 2
    assert [item["number"] for item in second_candidate["issues"]] == [1, 2, 3]
    assert second_candidate["issues"][1]["title"] == "Winner"


def test_comment_replay_preserves_winner_and_clock_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Comment replay keeps the concurrent message and clamps its timestamp."""
    original = issue(
        1,
        messages=[
            {
                "id": "old",
                "author": "human",
                "body": "old",
                "createdAt": "2026-09-24T10:00:00.000Z",
            }
        ],
    )
    winner = issue(
        1,
        updated_at="2026-09-24T11:00:00.000Z",
        messages=[
            {
                "id": "old",
                "author": "human",
                "body": "old",
                "createdAt": "2026-09-24T10:00:00.000Z",
            },
            {
                "id": "winner",
                "author": "other",
                "body": "won",
                "createdAt": "2026-09-24T11:00:00.000Z",
            },
        ],
    )
    committed = issue(
        1,
        updated_at="2026-09-24T11:00:00.000Z",
        messages=[
            *winner["messages"],
            {
                "id": "stable-id",
                "author": "agent",
                "body": "mine",
                "createdAt": "2026-09-24T11:00:00.000Z",
            },
        ],
    )
    fake = FakeHttp([
        response(200, board(issues=[original]), etag='"v1"'),
        response(412),
        response(200, board(issues=[winner]), etag='"v2"'),
        response(200),
        response(200, board(issues=[committed]), etag='"v3"'),
    ])
    client = BoardClient(
        capability="c" * 64,
        http=fake,
        now=fixed_now,
    )
    monkeypatch.setattr(board_module, "_new_message_id", lambda: "stable-id")

    result = client.comment(1, "agent", "mine")

    assert [message["id"] for message in result["messages"]] == ["old", "winner", "stable-id"]
    first_candidate = decode_request_body(fake.requests[1])["issues"][0]
    second_candidate = decode_request_body(fake.requests[3])["issues"][0]
    assert first_candidate["messages"][-1]["id"] == "stable-id"
    assert second_candidate["messages"][-1]["id"] == "stable-id"
    assert second_candidate["messages"][-1]["createdAt"] == "2026-09-24T11:00:00.000Z"


def test_close_and_reopen_preserve_unrelated_issue() -> None:
    """State changes touch only the requested issue and preserve other threads."""
    unrelated = issue(2, title="Untouched")
    initial = board(next_issue=3, issues=[issue(1), unrelated])
    closed = board(next_issue=3, issues=[issue(1, state="closed"), unrelated])
    reopened = board(next_issue=3, issues=[issue(1), unrelated])
    fake = FakeHttp([
        response(200, initial, etag='"v1"'),
        response(200),
        response(200, closed, etag='"v2"'),
        response(200, closed, etag='"v2"'),
        response(200),
        response(200, reopened, etag='"v3"'),
    ])
    client = BoardClient(
        capability="d" * 64,
        http=fake,
        now=lambda: datetime(2026, 9, 24, 10, 5, tzinfo=UTC),
    )

    assert client.close(1)["state"] == "closed"
    assert client.reopen(1)["state"] == "open"

    close_candidate = decode_request_body(fake.requests[1])
    reopen_candidate = decode_request_body(fake.requests[4])
    assert close_candidate["issues"][1] == unrelated
    assert reopen_candidate["issues"][1] == unrelated


def test_retry_exhaustion_fails_clearly() -> None:
    """Persistent contention ends with a bounded, explicit failure."""
    current = board()
    fake = FakeHttp([
        response(200, current, etag='"v1"'),
        response(412),
        response(200, current, etag='"v2"'),
        response(412),
        response(200, current, etag='"v3"'),
    ])
    client = BoardClient(capability="e" * 64, http=fake, max_attempts=2)

    with pytest.raises(BoardError, match="changed too often"):
        client.close(1)


def test_capability_is_never_reflected_in_http_errors() -> None:
    """Normal failures never expose the bearer capability."""
    capability = "f" * 64
    fake = FakeHttp([
        response(200, board(), etag='"v1"'),
        response(403, {"error": capability}),
    ])
    client = BoardClient(capability=capability, http=fake)

    with pytest.raises(BoardError) as raised:
        client.close(1)
    assert capability not in str(raised.value)


def test_v1_board_is_normalized_to_v2_in_memory() -> None:
    """A deployed v1 board is normalized in memory to the v2 schema."""
    raw = cast("dict[str, object]", json.loads(json.dumps(board())))
    raw["schemaVersion"] = 1
    raw.pop("resources")
    for raw_issue in cast("list[dict[str, object]]", raw["issues"]):
        raw_issue.pop("body")

    parsed = parse_board(raw)

    assert parsed["schemaVersion"] == 2
    assert parsed["resources"] == []
    assert not parsed["issues"][0]["body"]


def test_resource_schema_rejects_noncanonical_or_invalid_dependencies() -> None:
    """Resources require canonical identities and existing unique dependencies."""
    valid = BoardResource(
        host="lubko://server",
        path="/workspace/project",
        issueNumbers=[1],
        createdAt="2026-09-24T10:00:00.000Z",
        updatedAt="2026-09-24T10:00:00.000Z",
    )
    assert parse_board(board(resources=[valid]))["resources"] == [valid]
    mutations: tuple[dict[str, object], ...] = (
        {"host": "lubko://server/"},
        {"path": "/workspace//project"},
        {"path": "/workspace/../project"},
        {"path": "/workspace/./project"},
        {"path": "/workspace/"},
        {"issueNumbers": []},
        {"issueNumbers": [1, 1]},
        {"issueNumbers": [2]},
    )
    for mutation in mutations:
        raw = cast("dict[str, object]", json.loads(json.dumps(board(resources=[valid]))))
        raw_resource = cast("dict[str, object]", cast("list[object]", raw["resources"])[0])
        raw_resource.update(mutation)
        with pytest.raises(BoardError, match="malformed resource"):
            parse_board(raw)


def test_resource_add_is_idempotent_and_remove_deletes_last_dependency() -> None:
    """Duplicate dependencies are stable and an empty resource is deleted."""
    initial = board()
    first = board(
        resources=[
            BoardResource(
                host="lubko://server",
                path="/workspace/project",
                issueNumbers=[1],
                createdAt="2026-09-24T10:05:00.000Z",
                updatedAt="2026-09-24T10:05:00.000Z",
            )
        ]
    )
    duplicate = board(
        resources=[
            BoardResource(
                host="lubko://server",
                path="/workspace/project",
                issueNumbers=[1],
                createdAt="2026-09-24T10:05:00.000Z",
                updatedAt="2026-09-24T10:05:00.000Z",
            )
        ]
    )
    fake = FakeHttp([
        response(200, initial, etag='"v1"'),
        response(200),
        response(200, first, etag='"v2"'),
        response(200, first, etag='"v2"'),
        response(200),
        response(200, duplicate, etag='"v3"'),
        response(200, duplicate, etag='"v3"'),
        response(200),
        response(200, board(), etag='"v4"'),
    ])
    client = BoardClient(
        capability="1" * 64,
        http=fake,
        now=lambda: datetime(2026, 9, 24, 10, 5, tzinfo=UTC),
    )

    assert client.add_resource(1, "lubko://server", "/workspace/project") == first["resources"][0]
    assert (
        client.add_resource(1, "lubko://server", "/workspace/project") == duplicate["resources"][0]
    )
    assert client.remove_resource(1, "lubko://server", "/workspace/project") == []


def test_closed_issue_cannot_gain_resource_and_state_preserves_dependency() -> None:
    """Open issues alone may gain dependencies, and state changes retain them."""
    resource = BoardResource(
        host="lubko://server",
        path="/workspace/project",
        issueNumbers=[1],
        createdAt="2026-09-24T10:00:00.000Z",
        updatedAt="2026-09-24T10:00:00.000Z",
    )
    current = board(resources=[resource])
    closed = board(issues=[issue(1, state="closed")], resources=[resource])
    fake = FakeHttp([
        response(200, closed, etag='"v1"'),
        response(200, current, etag='"v1"'),
        response(200),
        response(200, closed, etag='"v2"'),
    ])
    client = BoardClient(capability="2" * 64, http=fake)

    with pytest.raises(BoardError, match="closed"):
        client.add_resource(1, "lubko://server", "/workspace/project")
    assert client.close(1)["state"] == "closed"
    assert decode_request_body(fake.requests[2])["resources"][0] == resource


def test_resource_list_reports_dependent_states_and_filters() -> None:
    """Resource views expose dependent states, status, and deterministic filters."""
    resource = BoardResource(
        host="lubko://server",
        path="/workspace/project",
        issueNumbers=[1, 2],
        createdAt="2026-09-24T10:00:00.000Z",
        updatedAt="2026-09-24T10:00:00.000Z",
    )
    current = board(
        next_issue=3,
        issues=[issue(1), issue(2, state="closed")],
        resources=[resource],
    )
    fake = FakeHttp([
        response(200, current, etag='"v1"'),
        response(200, current, etag='"v1"'),
        response(200, current, etag='"v1"'),
        response(200, current, etag='"v1"'),
        response(200, current, etag='"v1"'),
    ])
    client = BoardClient(http=fake)

    expected = client.list_resources()
    assert expected == [
        {
            "host": "lubko://server",
            "path": "/workspace/project",
            "issues": [{"number": 1, "state": "open"}, {"number": 2, "state": "closed"}],
            "protected": True,
            "collectible": False,
        }
    ]
    assert client.list_resources(issue=2) == expected
    assert client.list_resources(issue=3) == []
    assert client.list_resources(host="lubko://other") == []


def test_edit_rejects_closed_issue_before_writing() -> None:
    """Closed issue bodies are immutable."""
    fake = FakeHttp([response(200, board(issues=[issue(1, state="closed")]), etag='"v1"')])
    client = BoardClient(capability="3" * 64, http=fake)

    with pytest.raises(BoardError, match="closed"):
        client.edit_issue(1, "replacement")

    assert len(fake.requests) == 1
    assert fake.requests[0].method == "GET"


def test_cli_json_output_is_stable(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The JSON mode emits parseable deterministic machine output."""
    fake = FakeHttp([response(200, board(), etag='"v1"')])
    client = BoardClient(http=fake)
    monkeypatch.setattr(board_module, "_client_from_environment", lambda: client)

    assert board_module.main(["list", "--json"]) == 0

    captured = capsys.readouterr()
    assert json.loads(captured.out) == [issue(1)]
    assert not captured.err
