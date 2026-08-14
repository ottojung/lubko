"""Tests enforcing the two-column transport invariant and the baseline migration."""

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Final, Self, cast

import psycopg
import pytest

from lubko.worker import JOBS_COLUMN_TYPES, SchemaInvariantError, verify_jobs_table_invariant

REPO_ROOT: Final = Path(__file__).resolve().parent.parent
MIGRATIONS_DIR: Final = REPO_ROOT / "migrations"
BASELINE_MIGRATION: Final = MIGRATIONS_DIR / "0001_two_column_protocol.sql"
PROTOCOL_INVARIANT_PHRASE: Final = "exactly two columns forever"
TWO_COLUMN_COUNT: Final = 2
FORBIDDEN_LEGACY_PHRASES: Final = (
    "jobs_v2",
    "jobs_legacy",
    "legacy",
    "cutover",
    "backfill",
)


class _FakeCursor:
    """A cursor returning a fixed set of rows from ``fetchall``."""

    def __init__(self, rows: list[tuple[str, str]]) -> None:
        self._rows = rows

    @staticmethod
    def execute(sql: str, params: object | None = None) -> None:
        del sql, params

    def fetchall(self) -> list[tuple[str, str]]:
        return self._rows

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_exc: object) -> None:
        return None


class _FakeConnection:
    """A connection test double returning queued information-schema rows."""

    def __init__(self, rows: list[tuple[str, str]]) -> None:
        self._rows = rows
        self.transaction_count = 0

    def cursor(self, **_kwargs: object) -> "_FakeCursor":
        return _FakeCursor(self._rows)

    @contextmanager
    def transaction(self) -> Iterator[None]:
        self.transaction_count += 1
        yield


def as_connection(conn: _FakeConnection) -> psycopg.Connection[tuple[object, ...]]:
    """Adapt the fake connection to the worker's connection type.

    Args:
        conn: Fake connection test double.

    Returns:
        The same object typed as a psycopg connection.
    """
    return cast("psycopg.Connection[tuple[object, ...]]", conn)


def test_verify_accepts_exactly_two_columns() -> None:
    """The canonical two-column text schema passes the invariant check."""
    conn = as_connection(_FakeConnection([("id", "uuid"), ("payload", "text")]))

    verify_jobs_table_invariant(conn)


def test_verify_wraps_invariant_read_in_own_transaction() -> None:
    """The invariant read commits in its own top-level transaction.

    A bare SELECT on a default psycopg connection would leave an implicit
    transaction open, turning every later ``conn.transaction()`` block into a
    savepoint so claimed job updates never commit.
    """
    fake = _FakeConnection([("id", "uuid"), ("payload", "text")])

    verify_jobs_table_invariant(as_connection(fake))

    assert fake.transaction_count == 1


def test_verify_rejects_missing_payload_column() -> None:
    """A table without the payload column is rejected."""
    conn = as_connection(_FakeConnection([("id", "uuid")]))

    with pytest.raises(SchemaInvariantError, match=PROTOCOL_INVARIANT_PHRASE):
        verify_jobs_table_invariant(conn)


def test_verify_rejects_extra_third_column() -> None:
    """Any third column is rejected, even a jsonb one."""
    conn = as_connection(_FakeConnection([("id", "uuid"), ("payload", "text"), ("status", "text")]))

    with pytest.raises(SchemaInvariantError, match=PROTOCOL_INVARIANT_PHRASE):
        verify_jobs_table_invariant(conn)


def test_verify_rejects_many_extra_columns() -> None:
    """Any multi-column table is rejected, never just an exact third column."""
    conn = as_connection(
        _FakeConnection([
            ("cancel_requested_at", "timestamp with time zone"),
            ("cancellation_note", "text"),
            ("command", "text"),
            ("created_at", "timestamp with time zone"),
            ("cwd", "text"),
            ("exit_code", "integer"),
            ("finished_at", "timestamp with time zone"),
            ("id", "uuid"),
            ("process_pgid", "integer"),
            ("process_pid", "integer"),
            ("started_at", "timestamp with time zone"),
            ("status", "text"),
            ("stderr", "text"),
            ("stdout", "text"),
            ("updated_at", "timestamp with time zone"),
            ("worker_id", "text"),
        ])
    )

    with pytest.raises(SchemaInvariantError, match=PROTOCOL_INVARIANT_PHRASE):
        verify_jobs_table_invariant(conn)


def test_verify_rejects_payload_not_text() -> None:
    """The payload column must be text, not jsonb."""
    conn = as_connection(_FakeConnection([("id", "uuid"), ("payload", "jsonb")]))

    with pytest.raises(SchemaInvariantError, match="payload"):
        verify_jobs_table_invariant(conn)


def test_expected_column_types_are_id_and_text_payload() -> None:
    """The code-level contract is exactly id uuid plus payload text."""
    assert list(JOBS_COLUMN_TYPES) == [("id", "uuid"), ("payload", "text")]


def _split_top_level(text: str) -> list[str]:
    """Split on commas that are not nested inside parentheses.

    Args:
        text: Text whose top-level commas delimit the parts.

    Returns:
        The parts split at depth-zero commas.
    """
    parts: list[str] = []
    depth = 0
    current: list[str] = []
    for char in text:
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        if char == "," and depth == 0:
            parts.append("".join(current))
            current = []
        else:
            current.append(char)
    parts.append("".join(current))
    return parts


def _create_table_columns(sql: str, table: str) -> list[str]:
    """Extract the top-level column definitions of a CREATE TABLE statement.

    Args:
        sql: The migration SQL text.
        table: The fully qualified table name to extract.

    Returns:
        The top-level column and constraint definitions.
    """
    marker = f"create table if not exists {table} ("
    start = sql.index(marker) + len(marker)
    depth = 0
    end = len(sql)
    for index in range(start, len(sql)):
        char = sql[index]
        if char == "(":
            depth += 1
        elif char == ")":
            if depth == 0:
                end = index
                break
            depth -= 1
    return [part.strip() for part in _split_top_level(sql[start:end])]


def _read_baseline_migration() -> str:
    """Read the baseline migration SQL text.

    Returns:
        The baseline migration file contents.
    """
    return BASELINE_MIGRATION.read_text(encoding="utf-8")


def test_baseline_migration_creates_exactly_two_columns() -> None:
    """The baseline migration creates exactly id uuid plus payload text."""
    columns = _create_table_columns(_read_baseline_migration(), "lubko.jobs")

    assert len(columns) == TWO_COLUMN_COUNT
    assert columns[0].startswith("id uuid")
    assert columns[1].startswith("payload text")


def test_baseline_migration_declares_payload_as_text_with_checks() -> None:
    """The baseline declares payload as text with the JSON-object checks."""
    sql = _read_baseline_migration()

    assert "payload text not null" in sql
    assert "jsonb_typeof(payload::jsonb) = 'object'" in sql
    assert "(payload::jsonb) ? 'v'" in sql
    assert "((payload::jsonb)->'state'->>'status') is not null" in sql


def test_baseline_migration_grants_worker_access() -> None:
    """The baseline grants the worker role SELECT and UPDATE on lubko.jobs."""
    sql = _read_baseline_migration()

    assert "grant select, update on table lubko.jobs to lubko_worker" in sql
    assert "to_regrole('lubko_worker')" in sql


def test_baseline_migration_creates_queue_index() -> None:
    """The baseline creates the queue expression index on the two columns."""
    sql = _read_baseline_migration()

    assert "create index if not exists jobs_queue_idx" in sql
    assert "((payload::jsonb)->'state'->>'status')" in sql
    assert "((payload::jsonb)->'state'->>'created_at')" in sql


def test_baseline_migration_is_idempotent() -> None:
    """Every baseline statement is safe to apply more than once."""
    sql = _read_baseline_migration()

    assert "create table if not exists" in sql
    assert "create index if not exists" in sql
    assert "to_regrole('lubko_worker')" in sql


def test_baseline_migration_documents_the_invariant() -> None:
    """The baseline carries the invariant comment on the transport table."""
    sql = _read_baseline_migration()

    assert "comment on table lubko.jobs is" in sql
    assert "Never add a third column" in sql


def test_migrations_contain_no_legacy_references() -> None:
    """No migration may reintroduce legacy compatibility paths."""
    for migration in MIGRATIONS_DIR.glob("*.sql"):
        sql = migration.read_text(encoding="utf-8")
        for phrase in FORBIDDEN_LEGACY_PHRASES:
            assert phrase not in sql, migration


def test_worker_role_access_is_part_of_the_binding() -> None:
    """The binding spec documents the worker role grant, and README names it."""
    protocol_doc = (REPO_ROOT / "docs" / "protocol.md").read_text(encoding="utf-8")
    readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")

    assert "grant select, update on table lubko.jobs to lubko_worker" in protocol_doc
    assert "lubko_worker" in readme


def test_invariant_phrase_appears_in_code_docs_and_migrations() -> None:
    """The invariant is documented prominently everywhere it can drift."""
    targets = [
        REPO_ROOT / "README.md",
        REPO_ROOT / "docs" / "SKILL.md",
        REPO_ROOT / "docs" / "protocol.md",
        BASELINE_MIGRATION,
        REPO_ROOT / "src" / "lubko" / "protocol.py",
        REPO_ROOT / "src" / "lubko" / "worker.py",
    ]
    for target in targets:
        assert PROTOCOL_INVARIANT_PHRASE in target.read_text(encoding="utf-8"), target


def test_payload_is_string_text_in_docs() -> None:
    """Documentation states the payload is one string containing JSON, not jsonb."""
    protocol_doc = (REPO_ROOT / "docs" / "protocol.md").read_text(encoding="utf-8")

    assert "string containing a JSON object" in protocol_doc
    assert "`text`" in protocol_doc
    assert "stores `::text` back" in protocol_doc
