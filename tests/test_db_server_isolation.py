"""Per-server PostgreSQL authorization boundary for the transport queue.

The transport queue must be isolated at the database authorization boundary, not
only by the worker's application-layer server predicates. These tests assert the
enforcements the worker performs against a live PostgreSQL session:

  * the boundary itself is present (RLS enabled, the trusted session-server
    identity function, the same-server chunk-ownership enforcement, and the
    per-server policies are installed), and
  * the connected principal resolves to exactly the daemon's configured server.

They also confirm the application keeps its exact server-match predicate on every
query (defense in depth) and that the migration declares the intended boundary.
No live database is required: the connection double is scripted, and the
migration is asserted at the text level. See the module note below on why a true
embedded PostgreSQL run is intentionally not part of this suite.
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

# NOTE ON LIVE-DB AUTHORIZATION TESTING
# A genuine end-to-end test of PostgreSQL RLS/trigger semantics would require a
# running server. This repository's test contract forbids slow/heavyweight
# process startup and any optional/skip/integration test tier, and no PostgreSQL
# binary is available in the canonical environment. The strongest exact invariant
# tests possible without a live server are therefore: (1) the worker's own
# authorization-decision code (verify_server_isolation / verify_server_identity)
# exercised with scripted catalog results, so a missing boundary element fails
# closed; and (2) the migration text asserted to declare the RLS, the trusted
# session_server() identity function keyed on session_user, the same-server
# chunk-ownership trigger, and the external-only lubko_admin broad credential.
# Operators must additionally validate 0004 against staging PostgreSQL.


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


# A fully provisioned boundary: rls, session function, chunk function, chunk
# trigger, then the isolation policies.
_COMPLETE_BOUNDARY: list[tuple[str, object]] = [
    ("one", (True,)),
    ("one", (1,)),
    ("one", (1,)),
    ("one", (1,)),
    ("all", [("jobs_isolation_select",)]),
]


def test_verify_server_isolation_passes_when_boundary_present() -> None:
    """A fully provisioned boundary is accepted without raising."""
    conn = _as_conn(_ScriptedConn(list(_COMPLETE_BOUNDARY)))
    verify_server_isolation(conn)


@pytest.mark.parametrize(
    ("broken_index", "broken_value", "missing"),
    [
        (0, ("one", (False,)), "row-level security"),
        (1, ("one", None), "session_server"),
        (2, ("one", None), "enforce_chunk_root_server"),
        (3, ("one", None), "jobs_chunk_root_server"),
        (4, ("all", []), "isolation polic"),
        (4, ("all", [("unrelated_policy",)]), "isolation polic"),
    ],
)
def test_verify_server_isolation_fails_when_boundary_incomplete(
    broken_index: int,
    broken_value: tuple[str, object],
    missing: str,
) -> None:
    """Any missing boundary element fails closed with a diagnostic."""
    responses = list(_COMPLETE_BOUNDARY)
    responses[broken_index] = broken_value
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
    # The trusted mapping root of trust is created.
    assert "server_principals" in text


def test_migration_enforces_same_server_chunk_ownership() -> None:
    """A chunk must reference a same-server command root, enforced by a trigger.

    The root lookup is inlined in a single SECURITY DEFINER trigger function and is
    NOT a standalone, externally callable helper: a worker cannot call a privileged
    cross-server lookup directly. EXECUTE on the trigger function is revoked from
    PUBLIC and granted only to the worker/admin roles, while the trigger itself
    still fires for them.
    """
    text = MIGRATION_PATH.read_text(encoding="utf-8").lower()
    # No standalone, data-returning cross-server lookup helper exists.
    assert "create or replace function lubko.chunk_root_server" not in text
    # The enforcement lives only inside the trigger function.
    assert "create or replace function lubko.enforce_chunk_root_server" in text
    assert "security definer" in text
    assert "set search_path = lubko" in text
    assert "create trigger jobs_chunk_root_server" in text
    # The diagnostic must not leak the foreign root's server value: a worker could
    # otherwise probe arbitrary root UUIDs and read their server from the error
    # text, turning the trigger into a cross-server metadata oracle.
    assert "root server" not in text
    assert "cross-server chunk ownership" in text
    # Direct privileged invocation is closed: PUBLIC EXECUTE is revoked.
    assert "revoke execute on function lubko.enforce_chunk_root_server" in text
    assert "grant execute on function lubko.enforce_chunk_root_server" in text


def test_migration_session_server_uses_session_user_not_current_user() -> None:
    """The identity function must key off session_user, never current_user.

    Inside SECURITY DEFINER, current_user is the function *definer*, so using it
    would collapse every caller to the same identity and silently destroy the
    per-server boundary. This guards against a future refactor reverting it.
    """
    text = MIGRATION_PATH.read_text(encoding="utf-8")
    assert "principal = session_user" in text
    body = text[text.index("create or replace function lubko.session_server") :]
    body = body[: body.index("$$;")] if "$$;" in body else body
    assert "current_user" not in body


def test_migration_admin_credential_is_external_not_local() -> None:
    """The broad credential is external-only; the local role has no BYPASSRLS.

    A local worker-host process must never hold a broad credential, so
    lubko_worker is left RLS-scoped. The broad lubko_admin credential (BYPASSRLS)
    exists only for the off-host orchestrator, where the worker cannot reach it.
    """
    text = MIGRATION_PATH.read_text(encoding="utf-8").lower()
    assert "lubko_admin" in text
    assert "bypassrls" in text
    # The local legacy role must NOT be granted BYPASSRLS on its own.
    assert "alter role lubko_worker bypassrls" not in text
