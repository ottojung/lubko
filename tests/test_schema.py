"""Tests enforcing the two-column transport invariant and the migration files."""

from pathlib import Path
from typing import Final, Self, cast

import psycopg
import pytest

from lubko.worker import JOBS_COLUMN_TYPES, SchemaInvariantError, verify_jobs_table_invariant

REPO_ROOT: Final = Path(__file__).resolve().parent.parent
MIGRATIONS_DIR: Final = REPO_ROOT / "migrations"
PROTOCOL_INVARIANT_PHRASE: Final = "exactly two columns forever"
TWO_COLUMN_COUNT: Final = 2


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

    def cursor(self, **_kwargs: object) -> "_FakeCursor":
        return _FakeCursor(self._rows)


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


def test_verify_rejects_legacy_multi_column_schema() -> None:
    """The legacy multi-column schema is rejected."""
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


def _read_migration(name: str) -> str:
    return (MIGRATIONS_DIR / name).read_text(encoding="utf-8")


def test_prep_migration_creates_exactly_two_columns() -> None:
    """Migration 0002 creates a table with exactly id uuid + payload text."""
    sql = _read_migration("0002_two_column_protocol.sql")

    columns = _create_table_columns(sql, "lubko.jobs_v2")

    assert len(columns) == TWO_COLUMN_COUNT
    assert columns[0].startswith("id uuid")
    assert columns[1].startswith("payload text")


def test_prep_migration_never_touches_legacy_table() -> None:
    """Migration 0002 must stay additive so the legacy worker keeps running."""
    sql = _read_migration("0002_two_column_protocol.sql")

    assert "alter table lubko.jobs " not in sql
    assert "drop table" not in sql
    assert "rename to" not in sql


def test_cutover_migration_promotes_two_column_table() -> None:
    """Migration 0003 retires the legacy table and promotes jobs_v2."""
    sql = _read_migration("0003_cutover_two_column_protocol.sql")

    assert "rename to lubko.jobs_legacy" in sql
    assert "rename to lubko.jobs" in sql
    assert "lubko.jobs_v2" in sql


def test_prep_migration_is_idempotent_by_guards() -> None:
    """Migration 0002 uses create-if-not-exists and guarded backfill."""
    sql = _read_migration("0002_two_column_protocol.sql")

    assert "create table if not exists" in sql
    assert "create index if not exists" in sql
    assert "on conflict (id) do update" in sql
    assert "to_regclass" in sql


def test_cutover_migration_is_idempotent_by_guards() -> None:
    """Migration 0003 guards every step with schema checks."""
    sql = _read_migration("0003_cutover_two_column_protocol.sql")

    assert "to_regclass" in sql
    assert "information_schema.columns" in sql


def test_prep_migration_grants_worker_access() -> None:
    """Migration 0002 grants the worker role the same SELECT/UPDATE it needs."""
    sql = _read_migration("0002_two_column_protocol.sql")

    assert "grant select, update on table lubko.jobs_v2 to lubko_worker" in sql
    assert "to_regrole('lubko_worker')" in sql


def test_prep_migration_copies_legacy_grants() -> None:
    """Migration 0002 mirrors every legacy lubko.jobs grant onto jobs_v2."""
    sql = _read_migration("0002_two_column_protocol.sql")

    assert "information_schema.role_table_grants" in sql
    assert "table_name = 'jobs'" in sql
    assert "quote_ident" in sql
    assert "'PUBLIC'" in sql
    assert "with grant option" in sql


def test_prep_migration_repairs_grants_on_rerun() -> None:
    """Migration 0002 uses idempotent GRANT so re-applying repairs privileges."""
    sql = _read_migration("0002_two_column_protocol.sql")

    assert "create table if not exists" in sql
    assert "grant select, update on table lubko.jobs_v2 to lubko_worker" in sql
    assert "is_grantable" in sql


def test_cutover_migration_reasserts_worker_grant() -> None:
    """Migration 0003 re-asserts the worker grant on the promoted table."""
    sql = _read_migration("0003_cutover_two_column_protocol.sql")

    assert "grant select, update on table lubko.jobs to lubko_worker" in sql
    assert "to_regrole('lubko_worker')" in sql


def test_worker_role_access_is_part_of_the_binding() -> None:
    """The binding spec documents the worker role grant, and README names it."""
    protocol_doc = (REPO_ROOT / "docs" / "protocol.md").read_text(encoding="utf-8")
    readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")

    assert "grant select, update on table lubko.jobs to lubko_worker" in protocol_doc
    assert "lubko_worker" in readme


def test_payload_column_is_text_in_migrations() -> None:
    """Migrations declare payload as text and store JSON back as text."""
    for name in ("0002_two_column_protocol.sql", "0003_cutover_two_column_protocol.sql"):
        sql = _read_migration(name)
        assert "payload text not null" in sql
        assert "::text" in sql
    prep = _read_migration("0002_two_column_protocol.sql")
    assert "payload::jsonb" in prep
    assert "jsonb_typeof(payload::jsonb) = 'object'" in prep


def test_invariant_phrase_appears_in_code_docs_and_migrations() -> None:
    """The invariant is documented prominently everywhere it can drift."""
    targets = [
        REPO_ROOT / "README.md",
        REPO_ROOT / "docs" / "SKILL.md",
        REPO_ROOT / "docs" / "protocol.md",
        MIGRATIONS_DIR / "0002_two_column_protocol.sql",
        MIGRATIONS_DIR / "0003_cutover_two_column_protocol.sql",
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
