"""Borys shared-board client and command-line interface."""

from __future__ import annotations

import argparse
import copy
import http.client
import json
import os
import re
import sys
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from operator import itemgetter
from typing import TYPE_CHECKING, Final, Literal, Protocol, TypedDict, cast
from urllib.parse import urlsplit

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence
    from urllib.parse import SplitResult

BOARD_BASE_URL_ENV: Final = "LUBKO_BOARD_URL"
BOARD_CAPABILITY_ENV: Final = "LUBKO_BOARD_CAPABILITY"
BOARD_AUTHOR_ENV: Final = "LUBKO_BOARD_AUTHOR"
DEFAULT_BOARD_BASE_URL: Final = "https://vau.place/_skrynia"
BORYS_NAMESPACE: Final = "borys"
BOARD_KEY: Final = "board-v1"
BOARD_SCHEMA_VERSION: Final = 1
DEFAULT_MAX_ATTEMPTS: Final = 6
HTTP_TIMEOUT_SECONDS: Final = 30.0
CAPABILITY_RE: Final = re.compile(r"[0-9a-fA-F]{64}")

IssueState = Literal["open", "closed"]


class BoardMessage(TypedDict):
    """One chronological Borys issue message."""

    id: str
    author: str
    body: str
    createdAt: str


class BoardIssue(TypedDict):
    """One Borys issue thread."""

    number: int
    title: str
    state: IssueState
    createdAt: str
    updatedAt: str
    messages: list[BoardMessage]


class Board(TypedDict):
    """The complete Borys board document."""

    schemaVersion: Literal[1]
    nextIssueNumber: int
    issues: list[BoardIssue]


class BoardError(RuntimeError):
    """Raised when the Borys board cannot be read or safely mutated."""


@dataclass(frozen=True, slots=True)
class HttpResponse:
    """Small transport-neutral HTTP response."""

    status: int
    headers: Mapping[str, str]
    body: bytes


class HttpClient(Protocol):
    """Minimal HTTP transport needed by BoardClient."""

    def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str],
        body: bytes | None,
    ) -> HttpResponse:
        """Perform one HTTP request.

        Args:
            method: HTTP method.
            url: Absolute HTTP or HTTPS URL.
            headers: Request headers.
            body: Optional request body.

        Returns:
            The response status, headers, and bytes.
        """


class StandardHttpClient:
    """Standard-library HTTP implementation."""

    def __init__(self, timeout: float = HTTP_TIMEOUT_SECONDS) -> None:
        """Initialize the transport.

        Args:
            timeout: Per-request network timeout in seconds.
        """
        self._timeout = timeout

    def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str],
        body: bytes | None,
    ) -> HttpResponse:
        """Perform one HTTP request.

        Args:
            method: HTTP method.
            url: Absolute HTTP or HTTPS URL.
            headers: Request headers.
            body: Optional request body.

        Returns:
            The response status, headers, and bytes.

        Raises:
            BoardError: If the URL is invalid or the connection fails.
        """
        parsed = urlsplit(url)
        connection = _connection(parsed, self._timeout)
        target = parsed.path or "/"
        if parsed.query:
            target = f"{target}?{parsed.query}"
        try:
            connection.request(method, target, body=body, headers=dict(headers))
            response = connection.getresponse()
            response_body = response.read()
            response_headers = dict(response.getheaders())
            return HttpResponse(
                status=response.status,
                headers=response_headers,
                body=response_body,
            )
        except (OSError, http.client.HTTPException) as exc:
            msg = f"Skrynia request failed: {exc}"
            raise BoardError(msg) from exc
        finally:
            connection.close()


def _connection(
    parsed: SplitResult,
    timeout: float,
) -> http.client.HTTPConnection | http.client.HTTPSConnection:
    """Create a validated HTTP(S) connection.

    Args:
        parsed: Parsed absolute URL.
        timeout: Network timeout in seconds.

    Returns:
        An HTTP or HTTPS connection.

    Raises:
        BoardError: If the URL is not a supported absolute HTTP(S) URL.
    """
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        msg = "LUBKO_BOARD_URL must be an absolute http:// or https:// URL"
        raise BoardError(msg)
    try:
        port = parsed.port
    except ValueError as exc:
        msg = "LUBKO_BOARD_URL contains an invalid port"
        raise BoardError(msg) from exc
    if parsed.scheme == "https":
        return http.client.HTTPSConnection(parsed.hostname, port=port, timeout=timeout)
    return http.client.HTTPConnection(parsed.hostname, port=port, timeout=timeout)


@dataclass(frozen=True, slots=True)
class StoredBoard:
    """A validated board together with the ETag of that exact version."""

    board: Board
    etag: str


def _exact_keys(value: Mapping[str, object], expected: frozenset[str]) -> bool:
    """Return whether a JSON object has exactly the expected keys."""
    return frozenset(value) == expected


def _is_non_empty_text(value: object) -> bool:
    """Return whether a JSON value is a non-empty string."""
    return isinstance(value, str) and bool(value)


def _timestamp_seconds(value: object) -> float | None:
    """Parse a Borys timestamp to seconds since the epoch.

    Returns:
        Parsed epoch seconds, or None for an invalid timestamp.
    """
    if not isinstance(value, str) or not value:
        return None
    normalized = f"{value[:-1]}+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.timestamp()


def _is_positive_int(value: object) -> bool:
    """Return whether a JSON value is a positive non-boolean integer."""
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def _parse_message(value: object) -> BoardMessage:
    """Validate and return one Borys message.

    Returns:
        The validated message.

    Raises:
        BoardError: If the value is not a Borys v1 message.
    """
    if not isinstance(value, dict):
        msg = "Borys board contains an incompatible or malformed message"
        raise BoardError(msg)
    message = cast("dict[str, object]", value)
    if (
        not _exact_keys(message, frozenset({"id", "author", "body", "createdAt"}))
        or not _is_non_empty_text(message["id"])
        or not _is_non_empty_text(message["author"])
        or not _is_non_empty_text(message["body"])
        or _timestamp_seconds(message["createdAt"]) is None
    ):
        msg = "Borys board contains an incompatible or malformed message"
        raise BoardError(msg)
    return cast("BoardMessage", message)


def _parse_issue(value: object) -> BoardIssue:
    """Validate and return one Borys issue.

    Returns:
        The validated issue.

    Raises:
        BoardError: If the value is not a Borys v1 issue.
    """
    if not isinstance(value, dict):
        msg = "Borys board contains an incompatible or malformed issue"
        raise BoardError(msg)
    issue = cast("dict[str, object]", value)
    expected = frozenset({"number", "title", "state", "createdAt", "updatedAt", "messages"})
    if not _exact_keys(issue, expected):
        msg = "Borys board contains an incompatible or malformed issue"
        raise BoardError(msg)
    valid_identity = (
        _is_positive_int(issue["number"])
        and _is_non_empty_text(issue["title"])
        and issue["state"] in {"open", "closed"}
    )
    valid_times = (
        _timestamp_seconds(issue["createdAt"]) is not None
        and _timestamp_seconds(issue["updatedAt"]) is not None
    )
    if not valid_identity or not valid_times or not isinstance(issue["messages"], list):
        msg = "Borys board contains an incompatible or malformed issue"
        raise BoardError(msg)
    parsed = cast("BoardIssue", issue)
    messages = [_parse_message(message) for message in cast("list[object]", issue["messages"])]
    previous: float | None = None
    for message in messages:
        created = _timestamp_seconds(message["createdAt"])
        if created is None:
            msg = "Borys board contains an incompatible message timestamp"
            raise BoardError(msg)
        if previous is not None and created < previous:
            msg = f"Borys issue {parsed['number']} has messages out of chronological order"
            raise BoardError(msg)
        previous = created
    parsed["messages"] = messages
    return parsed


def parse_board(value: object) -> Board:
    """Validate a Borys v1 board document.

    Args:
        value: Decoded JSON value.

    Returns:
        The validated board.

    Raises:
        BoardError: If the document is incompatible or malformed.
    """
    if not isinstance(value, dict):
        msg = "Skrynia object borys/board-v1 contains an incompatible or malformed board"
        raise BoardError(msg)
    raw = cast("dict[str, object]", value)
    expected = frozenset({"schemaVersion", "nextIssueNumber", "issues"})
    if (
        not _exact_keys(raw, expected)
        or raw["schemaVersion"] != BOARD_SCHEMA_VERSION
        or not _is_positive_int(raw["nextIssueNumber"])
        or not isinstance(raw["issues"], list)
    ):
        msg = "Skrynia object borys/board-v1 contains an incompatible or malformed board"
        raise BoardError(msg)
    issues = [_parse_issue(issue) for issue in cast("list[object]", raw["issues"])]
    numbers = [issue["number"] for issue in issues]
    if len(numbers) != len(set(numbers)):
        msg = "Borys board contains duplicate issue numbers"
        raise BoardError(msg)
    next_issue_number = cast("int", raw["nextIssueNumber"])
    if numbers and next_issue_number <= max(numbers):
        msg = "Borys board issue number counter is inconsistent with its issues"
        raise BoardError(msg)
    return Board(
        schemaVersion=1,
        nextIssueNumber=next_issue_number,
        issues=issues,
    )


def _header(headers: Mapping[str, str], name: str) -> str | None:
    """Return one HTTP header case-insensitively."""
    wanted = name.casefold()
    for key, value in headers.items():
        if key.casefold() == wanted:
            return value
    return None


def _json_bytes(value: object) -> bytes:
    """Serialize compact UTF-8 JSON bytes.

    Returns:
        Compact encoded JSON.
    """
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode()


def _decode_json(body: bytes, context: str) -> object:
    """Decode response JSON without exposing response contents in errors.

    Returns:
        The decoded JSON value.

    Raises:
        BoardError: If the bytes are not valid JSON.
    """
    try:
        return cast("object", json.loads(body))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        msg = f"{context} returned invalid JSON"
        raise BoardError(msg) from exc


def _iso_timestamp(value: datetime) -> str:
    """Format one timestamp like JavaScript Date.toISOString().

    Returns:
        A UTC ISO timestamp with millisecond precision.
    """
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _latest_timestamp(now: datetime, *floors: str | None) -> str:
    """Return an ISO timestamp no earlier than any supplied floor."""
    if now.tzinfo is None:
        now = now.replace(tzinfo=UTC)
    candidates = [now.timestamp()]
    for floor in floors:
        if floor is not None:
            parsed = _timestamp_seconds(floor)
            if parsed is not None:
                candidates.append(parsed)
    return _iso_timestamp(datetime.fromtimestamp(max(candidates), tz=UTC))


def _new_message_id() -> str:
    """Return a new opaque message identifier."""
    return str(uuid.uuid4())


class BoardClient:
    """Thin concurrency-safe client for the shared Borys board."""

    def __init__(
        self,
        *,
        base_url: str = DEFAULT_BOARD_BASE_URL,
        capability: str | None = None,
        http: HttpClient | None = None,
        now: Callable[[], datetime] | None = None,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    ) -> None:
        """Initialize the client.

        Args:
            base_url: Skrynia base URL, normally ending in /_skrynia.
            capability: Optional Borys write capability.
            http: Optional injectable HTTP transport.
            now: Optional clock used by mutations.
            max_attempts: Maximum compare-and-swap attempts.

        Raises:
            ValueError: If max_attempts is not positive.
        """
        if max_attempts < 1:
            msg = "max_attempts must be positive"
            raise ValueError(msg)
        self._url = f"{base_url.rstrip('/')}/store/{BORYS_NAMESPACE}/{BOARD_KEY}"
        self._capability = capability.strip() if capability else None
        self._http = http if http is not None else StandardHttpClient()
        self._now = now if now is not None else lambda: datetime.now(tz=UTC)
        self._max_attempts = max_attempts

    def load_board(self) -> Board:
        """Read and validate the current board.

        Returns:
            The current board.

        Raises:
            BoardError: If the board is missing, malformed, or lacks an ETag.
        """
        stored = self._read()
        if stored is None:
            msg = "Borys board does not exist"
            raise BoardError(msg)
        return stored.board

    def list_issues(self, state: IssueState | None = None) -> list[BoardIssue]:
        """List issues deterministically by issue number.

        Args:
            state: Optional state filter.

        Returns:
            Matching issues sorted by issue number.
        """
        issues = self.load_board()["issues"]
        filtered = (
            issues if state is None else [issue for issue in issues if issue["state"] == state]
        )
        return sorted(filtered, key=itemgetter("number"))

    def get_issue(self, number: int) -> BoardIssue:
        """Return one issue by number.

        Args:
            number: Positive issue number.

        Returns:
            The requested issue.

        """
        return self._require_issue(self.load_board(), number)

    def create_issue(self, title: str) -> BoardIssue:
        """Create an open issue using the current board counter.

        Args:
            title: Human-readable issue title.

        Returns:
            The committed issue.

        Raises:
            BoardError: If the title is empty or the mutation fails.
        """
        clean_title = title.strip()
        if not clean_title:
            msg = "Issue title is required"
            raise BoardError(msg)
        created_number = 0

        def mutate(board: Board) -> Board:
            nonlocal created_number
            created_number = board["nextIssueNumber"]
            timestamp = _iso_timestamp(self._now())
            issue = BoardIssue(
                number=created_number,
                title=clean_title,
                state="open",
                createdAt=timestamp,
                updatedAt=timestamp,
                messages=[],
            )
            candidate = copy.deepcopy(board)
            candidate["nextIssueNumber"] += 1
            candidate["issues"].append(issue)
            return candidate

        committed = self._mutate(mutate)
        return self._require_issue(committed, created_number)

    def comment(self, number: int, author: str, body: str) -> BoardIssue:
        """Append one message to an issue.

        Args:
            number: Issue number.
            author: Explicit message author.
            body: Message body.

        Returns:
            The committed issue.

        Raises:
            BoardError: If the author/body is empty or the mutation fails.
        """
        clean_author = author.strip()
        clean_body = body.strip()
        if not clean_author:
            msg = "Message author is required"
            raise BoardError(msg)
        if not clean_body:
            msg = "Message body is required"
            raise BoardError(msg)
        message_id = _new_message_id()

        def update(issue: BoardIssue) -> BoardIssue:
            updated = copy.deepcopy(issue)
            last_message = updated["messages"][-1]["createdAt"] if updated["messages"] else None
            created_at = _latest_timestamp(self._now(), last_message)
            updated["messages"].append(
                BoardMessage(
                    id=message_id,
                    author=clean_author,
                    body=clean_body,
                    createdAt=created_at,
                )
            )
            return updated

        return self._update_issue(number, update)

    def close(self, number: int) -> BoardIssue:
        """Close one issue.

        Args:
            number: Issue number.

        Returns:
            The committed issue.
        """
        return self._set_state(number, "closed")

    def reopen(self, number: int) -> BoardIssue:
        """Reopen one issue.

        Args:
            number: Issue number.

        Returns:
            The committed issue.
        """
        return self._set_state(number, "open")

    def _set_state(self, number: int, state: IssueState) -> BoardIssue:
        def update(issue: BoardIssue) -> BoardIssue:
            changed = copy.deepcopy(issue)
            changed["state"] = state
            return changed

        return self._update_issue(number, update)

    @staticmethod
    def _require_issue(board: Board, number: int) -> BoardIssue:
        for issue in board["issues"]:
            if issue["number"] == number:
                return issue
        msg = f"Borys issue {number} does not exist"
        raise BoardError(msg)

    def _update_issue(
        self,
        number: int,
        update: Callable[[BoardIssue], BoardIssue],
    ) -> BoardIssue:
        def mutate(board: Board) -> Board:
            current = self._require_issue(board, number)
            changed = update(copy.deepcopy(current))
            last_message = changed["messages"][-1]["createdAt"] if changed["messages"] else None
            changed["updatedAt"] = _latest_timestamp(
                self._now(),
                current["updatedAt"],
                last_message,
            )
            candidate = copy.deepcopy(board)
            candidate["issues"] = [
                changed if issue["number"] == number else issue for issue in candidate["issues"]
            ]
            return candidate

        committed = self._mutate(mutate)
        return self._require_issue(committed, number)

    def _require_capability(self) -> str:
        if not self._capability:
            msg = "A Borys write capability is required"
            raise BoardError(msg)
        if CAPABILITY_RE.fullmatch(self._capability) is None:
            msg = "The Borys write capability must be 64 hexadecimal characters"
            raise BoardError(msg)
        return self._capability

    def _mutate(self, mutate: Callable[[Board], Board]) -> Board:
        capability = self._require_capability()
        stored = self._require_stored()
        for _attempt in range(self._max_attempts):
            candidate = mutate(stored.board)
            response = self._http.request(
                "PUT",
                self._url,
                headers={
                    "Content-Type": "application/json",
                    "If-Match": stored.etag,
                    "X-Skrynia-Capability": capability,
                },
                body=_json_bytes(candidate),
            )
            if response.status == http.client.PRECONDITION_FAILED:
                stored = self._require_stored()
                continue
            if response.status != http.client.OK:
                self._raise_http_error("PUT", response)
            return self._require_stored().board
        msg = "Borys board changed too often; the conditional write was not committed"
        raise BoardError(msg)

    def _require_stored(self) -> StoredBoard:
        stored = self._read()
        if stored is None:
            msg = "Borys board does not exist"
            raise BoardError(msg)
        return stored

    def _read(self) -> StoredBoard | None:
        response = self._http.request(
            "GET",
            self._url,
            headers={"Cache-Control": "no-cache"},
            body=None,
        )
        if response.status == http.client.NOT_FOUND:
            return None
        if response.status != http.client.OK:
            self._raise_http_error("GET", response)
        etag = _header(response.headers, "ETag")
        if not etag:
            msg = "Skrynia GET board-v1 returned no ETag; refusing an unsafe board write"
            raise BoardError(msg)
        value = _decode_json(response.body, "Skrynia GET borys/board-v1")
        return StoredBoard(board=parse_board(value), etag=etag)

    @staticmethod
    def _raise_http_error(method: str, response: HttpResponse) -> None:
        msg = f"Skrynia {method} {BORYS_NAMESPACE}/{BOARD_KEY} failed ({response.status})"
        raise BoardError(msg)


def _parser() -> argparse.ArgumentParser:
    """Build the command-line parser.

    Returns:
        The configured argument parser.
    """
    parser = argparse.ArgumentParser(prog="lubko-board")
    parser.add_argument("--json", action="store_true", dest="json_output")
    subparsers = parser.add_subparsers(dest="command", required=True)

    list_parser = subparsers.add_parser("list")
    list_parser.add_argument("--state", choices=("open", "closed", "all"), default="all")

    show_parser = subparsers.add_parser("show")
    show_parser.add_argument("number", type=int)

    create_parser = subparsers.add_parser("create")
    create_parser.add_argument("title")

    comment_parser = subparsers.add_parser("comment")
    comment_parser.add_argument("number", type=int)
    comment_parser.add_argument("body")
    comment_parser.add_argument("--author")

    close_parser = subparsers.add_parser("close")
    close_parser.add_argument("number", type=int)

    reopen_parser = subparsers.add_parser("reopen")
    reopen_parser.add_argument("number", type=int)

    for command_parser in (
        list_parser,
        show_parser,
        create_parser,
        comment_parser,
        close_parser,
        reopen_parser,
    ):
        command_parser.add_argument(
            "--json",
            action="store_true",
            dest="json_output",
            default=argparse.SUPPRESS,
            help=argparse.SUPPRESS,
        )
    return parser


def _issue_json(issue: BoardIssue) -> str:
    """Return stable compact JSON for one issue.

    Returns:
        Serialized issue JSON.
    """
    return json.dumps(issue, separators=(",", ":"), ensure_ascii=False, sort_keys=True)


def _issues_json(issues: list[BoardIssue]) -> str:
    """Return stable compact JSON for an issue list.

    Returns:
        Serialized issue-list JSON.
    """
    return json.dumps(issues, separators=(",", ":"), ensure_ascii=False, sort_keys=True)


def _human_issue(issue: BoardIssue) -> str:
    """Render one complete issue for a terminal.

    Returns:
        Human-readable issue text.
    """
    lines = [f"#{issue['number']} [{issue['state']}] {issue['title']}"]
    for message in issue["messages"]:
        lines.extend((f"{message['author']} @ {message['createdAt']}", message["body"]))
    return "\n".join(lines)


def _write_stdout(text: str) -> None:
    """Write one line to standard output."""
    sys.stdout.write(f"{text}\n")


def _write_stderr(text: str) -> None:
    """Write one line to standard error."""
    sys.stderr.write(f"{text}\n")


def _client_from_environment() -> BoardClient:
    """Construct a client from environment settings.

    Returns:
        A configured board client.
    """
    return BoardClient(
        base_url=os.environ.get(BOARD_BASE_URL_ENV, DEFAULT_BOARD_BASE_URL),
        capability=os.environ.get(BOARD_CAPABILITY_ENV),
    )


def _run_command(args: argparse.Namespace, client: BoardClient) -> BoardIssue | list[BoardIssue]:
    """Execute one parsed CLI command.

    Returns:
        The selected or mutated issue, or an issue list.

    Raises:
        BoardError: If the command is invalid or cannot be completed.
    """
    command = cast("str", args.command)
    if command == "list":
        raw_state = cast("str", args.state)
        state = None if raw_state == "all" else cast("IssueState", raw_state)
        return client.list_issues(state)
    if command == "show":
        return client.get_issue(cast("int", args.number))
    if command == "create":
        return client.create_issue(cast("str", args.title))
    if command == "comment":
        author = cast("str | None", args.author) or os.environ.get(BOARD_AUTHOR_ENV)
        if not author:
            msg = f"Message author is required; use --author or {BOARD_AUTHOR_ENV}"
            raise BoardError(msg)
        return client.comment(cast("int", args.number), author, cast("str", args.body))
    if command == "close":
        return client.close(cast("int", args.number))
    if command == "reopen":
        return client.reopen(cast("int", args.number))
    msg = f"unsupported lubko-board command: {command}"
    raise BoardError(msg)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the lubko-board command-line interface.

    Args:
        argv: Optional command arguments excluding the executable name.

    Returns:
        Process exit code.
    """
    args = _parser().parse_args(argv)
    try:
        result = _run_command(args, _client_from_environment())
    except BoardError as exc:
        _write_stderr(f"lubko-board: {exc}")
        return 1

    if cast("bool", args.json_output):
        if isinstance(result, list):
            _write_stdout(_issues_json(result))
        else:
            _write_stdout(_issue_json(result))
        return 0

    if isinstance(result, list):
        for issue in result:
            _write_stdout(f"#{issue['number']} [{issue['state']}] {issue['title']}")
        return 0

    _write_stdout(_human_issue(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
