"""Per-server PostgreSQL authorization boundary (issue #285).

The transport queue must be isolated at the database authorization boundary, not
only by the worker's application-layer server predicates. These tests assert the
two enforcements the worker performs against a live PostgreSQL session:

  * the boundary itself is present (RLS enabled, the trusted session-server
    identity function exists, and the per-server policies are installed), and
  * the connected principal resolves to exactly the daemon's configured server.

They also confirm the application keeps its exact server-match predicate on every
query (defense in depth) and that the migration declares the intended boundary.
No live database is required: the connection double is scripted.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Self, cast

import pytest

from lubko import worker
from lubko.worker import (
    SchemaInvariantError,
    claim_jobs,
    recover_stale_jobs,
    verify_server_identity,
    verify_server_isolation,
)

if TYPE_CHECKING:
    from lubko.worker import JobsConnection

MIGRATION_PATH = (
    Path(__file__).resolve().parent.parent / "migrations" / "0004_server_isolation_boundary.sql"
)


class _NoopCtx:
    """No-op context manager used by the connection doubles."""

    def __enter__(self) -> None:
        return None

    def __exit__(self, *_args: object) -> None:
        return None


class _ScriptedCursor:
    """Cursor that returns a preloaded (kind, value) per ``execute`` call."""

    def __init__(self, responses: list[tuple[str, object]]) -> None:
        self._responses = list(responses)
        self.row_factory = tuple

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, _query: str, _params: object = None) -> None:
        self._kind, self._value = self._responses.pop(0)

    def fetchone(self) -> object:
        if self._kind == "one":
            return self._value
        return None

    def fetchall(self) -> object:
        if self._kind == "all":
            return self._value
        return []


class _ScriptedConn:
    """Connection double returning scripted per-statement results."""

    def __init__(self, responses: list[tuple[str, object]]) -> None:
        self._responses = responses
        self.transactions = 0

    def transaction(self) -> _NoopCtx:
        self.transactions += 1
        return _NoopCtx()

    def cursor(self, **_kwargs: object) -> _ScriptedCursor:
        return _ScriptedCursor(self._responses)


class _RecordingCursor:
    """Cursor that records every executed query string."""

    def __init__(self, log: list[str]) -> None:
        self._log = log
        self.row_factory = tuple

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, query: str, _params: object = None) -> None:
        self._log.append(query)

    @staticmethod
    def fetchone() -> None:
        return None

    @staticmethod
    def fetchall() -> list[object]:
        return []


class _RecordingConn:
    """Connection double that records executed queries for assertion."""

    def __init__(self) -> None:
        self.queries: list[str] = []
        self.transactions = 0

    def transaction(self) -> _NoopCtx:
        self.transactions += 1
        return _NoopCtx()

    def cursor(self, **_kwargs: object) -> _RecordingCursor:
        return _RecordingCursor(self.queries)


def _as_conn(obj: object) -> JobsConnection:
    return cast("JobsConnection", obj)


def test_verify_server_isolation_passes_when_boundary_present() -> None:
    """A fully provisioned boundary is accepted without raising."""
    conn = _as_conn(
        _ScriptedConn([
            ("one", (True,)),
            ("one", (1,)),
            ("all", [("jobs_isolation_select",)]),
        ])
    )
    verify_server_isolation(conn)


@pytest.mark.parametrize(
    ("responses", "missing"),
    [
        (
            [("one", (False,)), ("one", (1,)), ("all", [("jobs_isolation_select",)])],
            "row-level security",
        ),
        (
            [("one", (True,)), ("one", None), ("all", [("jobs_isolation_select",)])],
            "session_server",
        ),
        (
            [("one", (True,)), ("one", (1,)), ("all", [])],
            "isolation polic",
        ),
        (
            [("one", (True,)), ("one", (1,)), ("all", [("unrelated_policy",)])],
            "isolation polic",
        ),
    ],
)
def test_verify_server_isolation_fails_when_boundary_incomplete(
    responses: list[tuple[str, object]],
    missing: str,
) -> None:
    """Any missing boundary element fails closed with a diagnostic."""
    conn = _as_conn(_ScriptedConn(responses))
    with pytest.raises(SchemaInvariantError, match=missing):
        verify_server_isolation(conn)


def test_verify_server_identity_passes_on_exact_match() -> None:
    """The session's bound server must equal the configured server."""
    conn = _as_conn(_ScriptedConn([("one", ("srv",))]))
    verify_server_identity(conn, "srv")


@pytest.mark.parametrize(
    "response",
    [
        ("one", ("other",)),
        ("one", None),
    ],
)
def test_verify_server_identity_fails_when_unbound(response: tuple[str, object]) -> None:
    """A mismatched or unmapped principal cannot run as the configured server."""
    conn = _as_conn(_ScriptedConn([response]))
    with pytest.raises(SchemaInvariantError, match="not bound to server"):
        verify_server_identity(conn, "srv")


def test_claim_jobs_sql_scopes_by_server() -> None:
    """Claiming still carries the exact server-match predicate (defense in depth)."""
    raw = _RecordingConn()
    claim_jobs(_as_conn(raw), worker.Settings.from_environment(server="srv"), 1)
    assert any(worker.SERVER_MATCH_SQL in query for query in raw.queries)


def test_recover_stale_jobs_sql_scopes_by_server() -> None:
    """Lease recovery still carries the exact server-match predicate."""
    raw = _RecordingConn()
    recover_stale_jobs(_as_conn(raw), "srv")
    assert any(worker.SERVER_MATCH_SQL in query for query in raw.queries)


def test_migration_declares_rls_boundary() -> None:
    """The migration installs RLS, the identity function, and the policies."""
    text = MIGRATION_PATH.read_text(encoding="utf-8").lower()
    assert "enable row level security" in text
    assert "create or replace function lubko.session_server" in text
    assert "jobs_isolation_select" in text
    assert "jobs_isolation_insert" in text
    assert "jobs_isolation_update" in text
    assert "jobs_isolation_delete" in text
    # Insert spoofing of another server's rows is blocked by WITH CHECK.
    assert "with check" in text
    # The orchestrator/admin principal keeps broader privileges.
    assert "bypassrls" in text
    # The trusted mapping root of trust is created.
    assert "server_principals" in text


def test_migration_session_server_uses_session_user_not_current_user() -> None:
    """The identity function must key off session_user, never current_user.

    Inside SECURITY DEFINER, current_user is the function *definer*, so using it
    would collapse every caller to the same identity and silently destroy the
    per-server boundary. This guards against a future refactor reverting it.
    """
    text = MIGRATION_PATH.read_text(encoding="utf-8")
    # The function body must reference session_user as the principal source.
    assert "principal = session_user" in text
    # A bare current_user reference inside the function body is forbidden.
    body = text[text.index("create or replace function lubko.session_server") :]
    body = body[: body.index("$$;")] if "$$;" in body else body
    assert "current_user" not in body
