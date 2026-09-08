"""Malformed opaque transport rows never poison worker operations.

Every worker function must guard its SQL against rows whose ``payload`` is not
valid JSON, carries an unsupported protocol version, belongs to a different
server, or has an unrelated payload shape.  The guards are structural: the SQL
must contain ``CASE WHEN payload IS JSON`` safe-casting predicates, exact
server-match clauses, version predicates, and status predicates so that
malformed, future-version, and wrong-server rows are provably inert.

These invariants are exercised through mock connections that record the emitted
SQL, following the same pattern used by :mod:`test_server_routing` and
:mod:`test_transport_gc_timestamp`.
"""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING, Self, cast
from uuid import uuid4

from lubko.worker import (
    JobResult,
    Settings,
    bulk_refresh_leases,
    claim_jobs,
    collect_transport,
    discover_cancellations,
    finish_job,
    reap_unsupported_jobs,
    recover_stale_jobs,
    request_cancel,
)

if TYPE_CHECKING:
    from lubko.worker import JobsConnection


# -- mock connection doubles --------------------------------------------------


class _Ctx:
    def __enter__(self) -> None:
        return None

    def __exit__(self, *_args: object) -> None:
        return None


class _Cursor:
    def __init__(self, queries: list[tuple[str, object]], rows: list[tuple[object, ...]]) -> None:
        self.queries = queries
        self.rows = rows
        self.rowcount = 0

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, query: str, params: object = None) -> None:
        self.queries.append((query, params))
        self.rowcount = 1 if self.rows else 0

    def fetchall(self) -> list[tuple[object, ...]]:
        return self.rows

    def fetchone(self) -> tuple[object, ...] | None:
        return self.rows[0] if self.rows else None


class _Conn:
    def __init__(self, rows: list[tuple[object, ...]] | None = None) -> None:
        self.queries: list[tuple[str, object]] = []
        self.rows = rows or []

    @staticmethod
    def transaction() -> _Ctx:
        return _Ctx()

    def cursor(self, **_kwargs: object) -> _Cursor:
        return _Cursor(self.queries, self.rows)


def _as_conn(obj: object) -> JobsConnection:
    return cast("JobsConnection", obj)


# -- shared assertion helpers -------------------------------------------------


def _assert_json_guard_present(query: str) -> None:
    """Every worker SQL must safely cast payloads that might not be JSON."""
    assert "IS JSON THEN" in query, "missing CASE WHEN ... IS JSON THEN payload END guard"


def _assert_server_guard_present(query: str) -> None:
    """Every worker SQL must match exactly one server identity."""
    assert "IS JSON THEN" in query, "missing CASE WHEN ... IS JSON guard"
    assert "->>'server'" in query, "missing server identity guard"


# -- tests --------------------------------------------------------------------


def test_claim_jobs_excludes_malformed_and_wrong_server_rows() -> None:
    """claim_jobs SQL contains JSON, version, server, and status guards."""
    conn = _Conn()
    claim_jobs(_as_conn(conn), Settings.from_environment(server="test-srv"), 10)
    for query, _params in conn.queries:
        _assert_json_guard_present(query)
        _assert_server_guard_present(query)
        assert "->>'type' = 'command'" in query
        assert "'v' = %(protocol_version)s::text" in query
        assert "->>'status' = 'pending'" in query


def test_bulk_refresh_leases_excludes_malformed_and_wrong_server_rows() -> None:
    """bulk_refresh_leases SQL contains JSON, server, status, and identity guards."""
    conn = _Conn()
    settings = Settings.from_environment(server="test-srv")
    bulk_refresh_leases(_as_conn(conn), settings, [uuid4()])
    for query, _params in conn.queries:
        _assert_json_guard_present(query)
        _assert_server_guard_present(query)
        assert "->>'status' = 'running'" in query
        assert "->>'worker_id'" in query
        assert "->>'worker_incarnation'" in query


def test_discover_cancellations_excludes_malformed_and_wrong_server_rows() -> None:
    """discover_cancellations SQL contains JSON, server, status, and cancel guards."""
    conn = _Conn()
    settings = Settings.from_environment(server="test-srv")
    discover_cancellations(_as_conn(conn), settings)
    for query, _params in conn.queries:
        _assert_json_guard_present(query)
        _assert_server_guard_present(query)
        assert "->>'status' = 'running'" in query
        assert "~ %(cancel_requested_at_pattern)s" in query


def test_finish_job_excludes_malformed_and_wrong_server_rows() -> None:
    """finish_job SQL contains JSON, server, status, and cancel-request guards."""
    conn = _Conn(rows=[("running",)])
    result = JobResult(
        status="succeeded",
        exit_code=0,
        stdout="",
        stderr="",
        cancellation_note=None,
    )
    finish_job(_as_conn(conn), uuid4(), result, server="test-srv")
    for query, _params in conn.queries:
        _assert_json_guard_present(query)
        _assert_server_guard_present(query)
        assert "->>'status' = 'running'" in query
        assert "~ %(cancel_requested_at_pattern)s" in query


def test_request_cancel_pending_path_excludes_malformed_rows() -> None:
    """request_cancel pending-path SQL contains JSON, server, and status guards."""
    conn = _Conn(rows=[("pending",)])
    request_cancel(_as_conn(conn), uuid4(), server="test-srv")
    for query, _params in conn.queries:
        _assert_json_guard_present(query)
        _assert_server_guard_present(query)
        assert "->>'status'" in query


def test_recover_stale_jobs_excludes_malformed_and_wrong_server_rows() -> None:
    """recover_stale_jobs SQL contains JSON, server, status, and lease guards."""
    conn = _Conn()
    recover_stale_jobs(_as_conn(conn), "test-srv")
    for query, _params in conn.queries:
        _assert_json_guard_present(query)
        _assert_server_guard_present(query)
        assert "->>'status' = 'running'" in query
        assert "~ %(lease_expires_at_pattern)s" in query


def test_reap_unsupported_jobs_excludes_malformed_and_wrong_server_rows() -> None:
    """reap_unsupported_jobs SQL contains JSON, server, and version-type guards."""
    conn = _Conn()
    reap_unsupported_jobs(_as_conn(conn), Settings.from_environment(server="test-srv"), 100)
    for query, _params in conn.queries:
        _assert_json_guard_present(query)
        _assert_server_guard_present(query)
        assert "->>'type' = 'command'" in query
        assert "->>'status' = 'pending'" in query


def test_collect_transport_excludes_malformed_and_wrong_server_rows() -> None:
    """collect_transport SQL contains JSON, server, status, and timestamp guards."""
    conn = _Conn()
    settings = replace(Settings.from_environment(server="test-srv"), gc_retention_seconds=0.0)
    collect_transport(_as_conn(conn), settings)
    for query, _params in conn.queries:
        _assert_json_guard_present(query)
        _assert_server_guard_present(query)
