"""Mechanical regression check for the canonical safe-payload boundary.

The ``_safe_payload_sql()`` helper is the single application-side boundary
converting opaque ``payload text`` into safe nullable JSONB.  Every
ambient-row SQL expression must be derived from this helper or an explicitly
reviewed equivalent.

This module reads ``worker.py`` source and verifies that:

1. ``_safe_payload_sql`` is defined and exported.
2. No SQL string in the file contains a raw unsafe ``payload::jsonb`` cast
   (which would bypass the totalizing guard).
3. No SQL string in the file contains a raw ``CASE WHEN ... IS JSON``
   outside the helper definition itself (confirming centralisation).
4. Every emitted SQL query produced by the worker's public DB functions
   includes the ``IS JSON THEN`` safe-guard (mock-cursor coverage).

Raises:
    AssertionError: When a structural invariant is violated.
"""

from __future__ import annotations

import re
from dataclasses import replace as _replace
from pathlib import Path
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
from lubko.worker import (
    _safe_payload_sql as _safe_payload_sql,  # ruff: ignore[useless-import-alias, import-private-name]
)

if TYPE_CHECKING:
    from lubko.worker import JobsConnection

_WORKER_SRC = Path(__file__).resolve().parent.parent / "src" / "lubko" / "worker.py"


# ---------------------------------------------------------------------------
# mock connection doubles (identical to test_malformed_queue_postgres)
# ---------------------------------------------------------------------------


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


def _replace_settings(settings: Settings) -> Settings:
    """Return a Settings copy with gc_retention_seconds=0 for collect_transport."""
    return _replace(settings, gc_retention_seconds=0.0)


# ---------------------------------------------------------------------------
# source-level structural checks
# ---------------------------------------------------------------------------


def test_safe_payload_sql_helper_is_defined() -> None:
    """The canonical boundary function exists and returns the expected fragment."""
    result = _safe_payload_sql()
    assert result == "((CASE WHEN payload IS JSON THEN payload END)::jsonb)"
    assert "IS JSON THEN" in result


def test_safe_payload_sql_aliased_column() -> None:
    """The helper works with table-qualified aliases."""
    assert _safe_payload_sql("job.payload") == (
        "((CASE WHEN job.payload IS JSON THEN job.payload END)::jsonb)"
    )
    assert _safe_payload_sql("chunk.payload") == (
        "((CASE WHEN chunk.payload IS JSON THEN chunk.payload END)::jsonb)"
    )


def test_worker_source_contains_no_raw_payload_jsonb_cast() -> None:
    """No SQL in worker.py may use an unguarded ``payload::jsonb`` cast.

    The safe pattern is always wrapped in ``CASE WHEN ... IS JSON THEN``.
    A raw ``payload::jsonb`` would bypass the totalising guard.

    Raises:
        AssertionError: If a raw ``payload::jsonb`` cast is found.
    """
    src = _WORKER_SRC.read_text(encoding="utf-8")
    lines = src.splitlines()
    in_helper = False
    in_docstring = False
    for lineno_0, line in enumerate(lines, start=1):
        stripped = line.strip()
        # Track docstring state
        if stripped.startswith(('"""', "'''")):
            count = stripped.count(stripped[:3])
            if count >= 2:
                # Single-line docstring, skip
                pass
            else:
                in_docstring = not in_docstring
            continue
        if in_docstring:
            continue
        if stripped.startswith("def _safe_payload_sql"):
            in_helper = True
            continue
        if in_helper:
            if stripped and not stripped.startswith(("#", "def ", "    ", '"""')):
                in_helper = False
            else:
                continue
        if re.search(r"\bpayload::jsonb\b", line):
            msg = (
                f"line {lineno_0}: raw 'payload::jsonb' cast found outside "
                f"_safe_payload_sql helper; use _safe_payload_sql() instead"
            )
            raise AssertionError(msg)


def test_worker_source_contains_no_inline_case_when_payload_is_json() -> None:
    """No SQL string in worker.py may hand-write ``CASE WHEN ... IS JSON``.

    All such expressions must be derived from ``_safe_payload_sql()``.

    Raises:
        AssertionError: If an inline CASE WHEN pattern is found.
    """
    src = _WORKER_SRC.read_text(encoding="utf-8")
    lines = src.splitlines()
    helper_start = None
    helper_end = None
    for lineno_0, line in enumerate(lines, start=1):
        if line.strip().startswith("def _safe_payload_sql("):
            helper_start = lineno_0
        elif (
            helper_start is not None
            and helper_end is None
            and line.strip()
            and not line.startswith(" ")
        ):
            helper_end = lineno_0
    if helper_end is None:
        helper_end = len(lines) + 1

    for lineno_0, line in enumerate(lines, start=1):
        if helper_start is not None and helper_start <= lineno_0 < helper_end:
            continue
        if "CASE WHEN" in line and "IS JSON THEN" in line and "END)::jsonb" in line:
            if "_SAFE_PAYLOAD_SQL" in line or "_safe_payload_sql" in line:
                continue
            msg = (
                f"line {lineno_0}: inline CASE WHEN ... IS JSON THEN ... END "
                f"found outside _safe_payload_sql; use _safe_payload_sql() "
                f"or _SAFE_PAYLOAD_SQL instead"
            )
            raise AssertionError(msg)


def test_worker_source_uses_safe_payload_sql_constant() -> None:
    """The ``_SAFE_PAYLOAD_SQL`` constant is defined for default-column use."""
    src = _WORKER_SRC.read_text(encoding="utf-8")
    assert "_SAFE_PAYLOAD_SQL" in src
    assert re.search(
        r"_SAFE_PAYLOAD_SQL\s*:\s*Final\s*=\s*_safe_payload_sql\(\)",
        src,
    )


# ---------------------------------------------------------------------------
# emitted-SQL coverage: every DB function's SQL contains the safe-guard
# ---------------------------------------------------------------------------


def test_all_emitted_sql_contains_json_guard() -> None:
    """Every SQL string emitted by worker DB functions contains the JSON guard.

    This is the runtime regression complement to the source-level checks:
    even if the source looks correct, the generated SQL must still include
    the ``IS JSON THEN`` safe-guard on every query.
    """
    settings = Settings.from_environment(server="test-srv")
    result = JobResult(
        status="succeeded",
        exit_code=0,
        stdout="",
        stderr="",
        cancellation_note=None,
    )

    conn = _Conn(rows=[(uuid4(), '{"v":4}')])
    claim_jobs(_as_conn(conn), settings, 10)
    _assert_all_queries_have_guard(conn, "claim_jobs")

    conn = _Conn(rows=[("running",)])
    bulk_refresh_leases(_as_conn(conn), settings, [uuid4()])
    _assert_all_queries_have_guard(conn, "bulk_refresh_leases")

    conn = _Conn(rows=[("running",)])
    discover_cancellations(_as_conn(conn), settings)
    _assert_all_queries_have_guard(conn, "discover_cancellations")

    conn = _Conn(rows=[("running",)])
    finish_job(_as_conn(conn), uuid4(), result, server="test-srv")
    _assert_all_queries_have_guard(conn, "finish_job")

    conn = _Conn(rows=[("pending",)])
    request_cancel(_as_conn(conn), uuid4(), server="test-srv")
    _assert_all_queries_have_guard(conn, "request_cancel")

    conn = _Conn(rows=[(uuid4(), '{"v":4}')])
    recover_stale_jobs(_as_conn(conn), "test-srv")
    _assert_all_queries_have_guard(conn, "recover_stale_jobs")

    conn = _Conn(rows=[(uuid4(), 4, "number")])
    reap_unsupported_jobs(_as_conn(conn), settings, 100)
    _assert_all_queries_have_guard(conn, "reap_unsupported_jobs")

    conn = _Conn()
    collect_transport(_as_conn(conn), _replace_settings(settings))
    _assert_all_queries_have_guard(conn, "collect_transport")


def _assert_all_queries_have_guard(conn: _Conn, func_name: str) -> None:
    """Assert every query emitted by a DB function contains the JSON guard."""
    for query, _params in conn.queries:
        assert "IS JSON THEN" in query, (
            f"{func_name} emitted SQL missing IS JSON THEN guard: {query[:120]}..."
        )
