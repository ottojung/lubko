"""Borys board protocol, concurrency, and CLI invariants."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import cast

import pytest

import lubko.board as board_module
from lubko.board import (
    Board,
    BoardClient,
    BoardError,
    BoardIssue,
    HttpResponse,
    IssueState,
    parse_board,
)


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
        """Record a request and return the next queued response."""
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
    messages: list[dict[str, str]] | None = None,
) -> BoardIssue:
    """Build one valid issue for tests."""
    return BoardIssue(
        number=number,
        title=title or f"Issue {number}",
        state=state,
        createdAt="2026-09-24T10:00:00.000Z",
        updatedAt=updated_at,
        messages=cast(list[board_module.BoardMessage], messages or []),
    )


def board(
    *,
    next_issue: int = 2,
    issues: list[BoardIssue] | None = None,
) -> Board:
    """Build one valid board for tests."""
    return Board(
        schemaVersion=1,
        nextIssueNumber=next_issue,
        issues=list(issues) if issues is not None else [issue(1)],
    )


def response(status: int, value: object | None = None, *, etag: str | None = None) -> HttpResponse:
    """Build a fake JSON HTTP response."""
    headers = {} if etag is None else {"ETag": etag}
    body_bytes = b"" if value is None else json.dumps(value).encode()
    return HttpResponse(status=status, headers=headers, body=body_bytes)


def decode_request_body(request: RecordedRequest) -> Board:
    """Decode one captured PUT body as a board."""
    if request.body is None:
        msg = "expected request body"
        raise AssertionError(msg)
    return parse_board(cast(object, json.loads(request.body)))


def fixed_now() -> datetime:
    """Return a deterministic clock value before the test messages."""
    return datetime(2026, 9, 24, 9, 0, tzinfo=UTC)


def test_schema_is_strict_and_rejects_assignment() -> None:
    """Unknown issue fields, including assignment, are incompatible with Borys v1."""
    raw = cast(dict[str, object], json.loads(json.dumps(board())))
    raw_issue = cast(dict[str, object], cast(list[object], raw["issues"])[0])
    raw_issue["assignee"] = "agent-a"
    with pytest.raises(BoardError, match="malformed issue"):
        parse_board(raw)


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


def test_comment_replay_preserves_winner_and_clock_order() -> None:
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
        new_id=lambda: "stable-id",
    )

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


def test_cli_json_output_is_stable(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The JSON mode emits parseable deterministic machine output."""
    fake = FakeHttp([response(200, board(), etag='"v1"')])
    client = BoardClient(http=fake)
    monkeypatch.setattr(board_module, "_client_from_environment", lambda: client)

    assert board_module.main(["--json", "list"]) == 0

    captured = capsys.readouterr()
    assert json.loads(captured.out) == [issue(1)]
    assert captured.err == ""
