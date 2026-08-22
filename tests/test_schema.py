"""Tests enforcing the two-column transport invariant and the protocol v3 migrations."""

import contextlib
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Final, Self, cast

import psycopg
import pytest

from lubko.worker import (
    CHUNK_ORDER_INDEX_NAME,
    CHUNK_OWNER_INDEX_NAME,
    JOBS_COLUMN_TYPES,
    TYPE_AWARE_CONSTRAINT_NAME,
    WAKEUP_FUNCTION_NAME,
    WAKEUP_TRIGGER_NAME,
    SchemaInvariantError,
    verify_jobs_table_invariant,
    verify_protocol_schema,
)
from tests import _pg

REPO_ROOT: Final = Path(__file__).resolve().parent.parent
MIGRATIONS_DIR: Final = REPO_ROOT / "migrations"
BASELINE_MIGRATION: Final = MIGRATIONS_DIR / "0001_two_column_protocol.sql"
PROTOCOL_INVARIANT_PHRASE: Final = "exactly two columns forever"
TWO_COLUMN_COUNT: Final = 2
TYPE_AWARE_CONSTRAINT: Final = "jobs_payload_type_shape"
FORBIDDEN_LEGACY_PHRASES: Final = (
    "jobs_v2",
    "jobs_legacy",
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


class _QueuedCursor:
    """A cursor returning one queued batch of rows per ``execute`` call."""

    def __init__(self, batches: list[list[tuple[str, ...]]]) -> None:
        self._batches = list(batches)

    @staticmethod
    def execute(sql: str, params: object | None = None) -> None:
        del sql, params

    def fetchall(self) -> list[tuple[str, ...]]:
        if self._batches:
            return self._batches.pop(0)
        return []

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_exc: object) -> None:
        return None


class _QueuedConnection:
    """A connection test double returning one batch of rows per query."""

    def __init__(self, batches: list[list[tuple[str, ...]]]) -> None:
        self._batches = batches

    def cursor(self, **_kwargs: object) -> "_QueuedCursor":
        return _QueuedCursor(self._batches)

    @staticmethod
    def transaction() -> contextlib.nullcontext[None]:
        return contextlib.nullcontext()


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


def test_baseline_migration_declares_payload_as_text_with_type_aware_checks() -> None:
    """The baseline declares payload as text with the JSON-object checks."""
    sql = _read_baseline_migration()

    assert "payload text not null" in sql
    assert "jsonb_typeof(payload::jsonb) = 'object'" in sql
    assert "(payload::jsonb) ? 'v'" in sql
    assert TYPE_AWARE_CONSTRAINT in sql
    assert "'command'" in sql
    assert "'output_chunk'" in sql
    assert "'stdout'" in sql
    assert "'stderr'" in sql
    assert "(((payload::jsonb)->'state'->>'status') is not null)" in sql


def test_baseline_migration_grants_worker_access() -> None:
    """The baseline grants schema usage and SELECT/INSERT/UPDATE on lubko.jobs.

    Protocol v3 requires the worker role to insert immutable output_chunk rows
    in addition to reading and claiming jobs.
    """
    sql = _read_baseline_migration()

    assert "grant usage on schema lubko to lubko_worker" in sql
    assert "grant select, insert, update, delete on table lubko.jobs to lubko_worker" in sql
    assert "to_regrole('lubko_worker')" in sql


def test_baseline_migration_does_not_encode_process_shape() -> None:
    """The v2 -> v3 breaking change is content-only and needs no DDL upgrade.

    The type-aware constraint stays generic (request object plus state.status);
    the required request.process validation and the legacy command/args
    prohibition live in the payload parser, never in SQL, so the physical
    two-column table is identical between protocol versions.
    """
    sql = _read_baseline_migration()

    assert "request->'process'" not in sql
    assert "jobs_payload_type_shape" in sql
    assert "jsonb_typeof((payload::jsonb)->'request') = 'object'" in sql


def test_baseline_migration_creates_command_queue_index() -> None:
    """The baseline queue index covers only command rows."""
    sql = _read_baseline_migration()

    assert "create index if not exists jobs_queue_idx" in sql
    assert "((payload::jsonb)->'state'->>'status')" in sql
    assert "((payload::jsonb)->'state'->>'created_at')" in sql
    assert "((payload::jsonb)->>'type') = 'command'" in sql


def test_baseline_migration_creates_chunk_indexes() -> None:
    """The baseline indexes chunk ownership and deterministic ordering."""
    sql = _read_baseline_migration()

    assert "create index if not exists jobs_chunk_owner_idx" in sql
    assert "create index if not exists jobs_chunk_order_idx" in sql
    assert "((payload::jsonb)->>'thread')" in sql
    assert "((payload::jsonb)->'sequence')::bigint" in sql
    assert "((payload::jsonb)->>'type') = 'output_chunk'" in sql


def test_baseline_migration_declares_event_driven_wakeup_objects() -> None:
    """The baseline installs the wakeup NOTIFY function and trigger idempotently."""
    sql = _read_baseline_migration()

    assert "create or replace function lubko.notify_jobs_changed()" in sql
    assert "pg_notify('lubko_jobs_changed'" in sql
    assert "drop trigger if exists lubko_jobs_notify_wakeups" in sql
    assert "create trigger lubko_jobs_notify_wakeups" in sql
    assert "after insert or update on lubko.jobs" in sql


def test_baseline_migration_wakeup_trigger_only_notifies_actionable_changes() -> None:
    """Only pending entries and fresh cancellation markers notify, never worker writes.

    The trigger must wake an idle worker for the durable changes that need a
    supervisor turn (a submitted/requeued job, a cancellled running job) and
    remain silent for the worker's own writes (claims, heartbeats, output
    publication, finalization) and for immutable output_chunk rows, so a busy
    worker is never woken in a loop by its own activity.
    """
    sql = _read_baseline_migration()

    assert "(new.payload::jsonb)->'state'->>'status' = 'pending'" in sql
    assert "(old.payload::jsonb)->'state'->>'status' <> 'pending'" in sql
    assert "'cancel_requested_at' is null" in sql
    assert "'cancel_requested_at' is not null" in sql
    assert "(new.payload::jsonb)->>'type' <> 'command'" in sql


def test_baseline_migration_is_idempotent() -> None:
    """Every baseline statement is safe to apply more than once."""
    sql = _read_baseline_migration()

    assert "create table if not exists" in sql
    assert "create index if not exists" in sql
    assert "to_regrole('lubko_worker')" in sql
    assert "create or replace function lubko.notify_jobs_changed()" in sql
    assert "drop trigger if exists lubko_jobs_notify_wakeups on lubko.jobs" in sql


def test_baseline_migration_documents_the_invariant() -> None:
    """The baseline carries the invariant comment on the transport table."""
    sql = _read_baseline_migration()

    assert "comment on table lubko.jobs is" in sql
    assert "Never add a third column" in sql


def test_migrations_contain_no_legacy_references() -> None:
    """No migration may reintroduce legacy schema objects or a backfill path.

    The migration documents the destructive v2 -> v3 cutover but must never
    create legacy transport tables or any backfill/staging mechanics.
    """
    for migration in MIGRATIONS_DIR.glob("*.sql"):
        sql = migration.read_text(encoding="utf-8")
        for phrase in FORBIDDEN_LEGACY_PHRASES:
            assert phrase not in sql, migration


def test_worker_role_access_is_part_of_the_binding() -> None:
    """The binding spec documents the worker role grant, and README names it."""
    protocol_doc = (REPO_ROOT / "docs" / "protocol.md").read_text(encoding="utf-8")
    readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")

    assert "grant usage on schema lubko to lubko_worker" in protocol_doc
    assert (
        "grant select, insert, update, delete on table lubko.jobs to lubko_worker" in protocol_doc
    )
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


def as_protocol_connection(conn: _QueuedConnection) -> psycopg.Connection[tuple[object, ...]]:
    """Adapt a queued test double to the worker's connection type.

    Args:
        conn: Queued connection test double.

    Returns:
        The same object typed as a psycopg connection.
    """
    return cast("psycopg.Connection[tuple[object, ...]]", conn)


def _v3_catalog_batches() -> list[list[tuple[str, ...]]]:
    """Return the catalog batches of a canonical migrated v3 schema.

    ``verify_protocol_schema`` runs four catalog reads in a fixed order:
    ``pg_constraint``, ``pg_indexes``, ``pg_proc`` (wakeup function), and
    ``pg_trigger`` (wakeup trigger).

    Returns:
        One queued row batch per catalog query, each modeling the canonical
        baseline applied by ``migrations/0001_two_column_protocol.sql``.
    """
    return [
        [(TYPE_AWARE_CONSTRAINT_NAME,), ("jobs_payload_is_json_object",)],
        [(CHUNK_OWNER_INDEX_NAME,), (CHUNK_ORDER_INDEX_NAME,), ("jobs_queue_idx",)],
        [(WAKEUP_FUNCTION_NAME,)],
        [(WAKEUP_TRIGGER_NAME,)],
    ]


def test_verify_protocol_schema_accepts_migrated_shape() -> None:
    """A migrated table with the canonical v3 baseline shape passes verification."""
    conn = as_protocol_connection(_QueuedConnection(_v3_catalog_batches()))

    verify_protocol_schema(conn)


def test_verify_protocol_schema_rejects_missing_type_aware_constraint() -> None:
    """A table without the type-aware constraint is refused."""
    batches = _v3_catalog_batches()
    batches[0] = [("jobs_payload_is_json_object",), ("jobs_payload_has_status",)]
    conn = as_protocol_connection(_QueuedConnection(batches))

    with pytest.raises(SchemaInvariantError, match=TYPE_AWARE_CONSTRAINT_NAME):
        verify_protocol_schema(conn)


def test_verify_protocol_schema_rejects_missing_chunk_indexes() -> None:
    """A table without the chunk ownership/ordering indexes is refused."""
    batches = _v3_catalog_batches()
    batches[1] = [("jobs_queue_idx",)]
    conn = as_protocol_connection(_QueuedConnection(batches))

    with pytest.raises(SchemaInvariantError, match="index jobs_chunk"):
        verify_protocol_schema(conn)


def test_verify_protocol_schema_rejects_missing_wakeup_function() -> None:
    """A table without the wakeup NOTIFY function is refused."""
    batches = _v3_catalog_batches()
    batches[2] = []
    conn = as_protocol_connection(_QueuedConnection(batches))

    with pytest.raises(SchemaInvariantError, match="wakeup function"):
        verify_protocol_schema(conn)


def test_verify_protocol_schema_rejects_missing_wakeup_trigger() -> None:
    """A table without the wakeup NOTIFY trigger is refused."""
    batches = _v3_catalog_batches()
    batches[3] = []
    conn = as_protocol_connection(_QueuedConnection(batches))

    with pytest.raises(SchemaInvariantError, match="wakeup trigger"):
        verify_protocol_schema(conn)


def test_verify_protocol_schema_rejects_non_canonical_shape() -> None:
    """A two-column table lacking the canonical v3 output-chunk shape is refused."""
    conn = as_protocol_connection(
        _QueuedConnection([
            [("jobs_payload_has_status",), ("jobs_payload_is_json_object",)],
            [("jobs_queue_idx",)],
            [(WAKEUP_FUNCTION_NAME,)],
            [(WAKEUP_TRIGGER_NAME,)],
        ])
    )

    with pytest.raises(SchemaInvariantError, match=r"0001_two_column_protocol\.sql"):
        verify_protocol_schema(conn)


# ---------------------------------------------------------------------------
# Incremental migration 0002: DELETE grant for GC
# ---------------------------------------------------------------------------

GC_MIGRATION: Final = MIGRATIONS_DIR / "0002_grant_transport_gc_delete.sql"


def _read_gc_migration() -> str:
    """Read the incremental GC migration SQL text.

    Returns:
        The migration file contents.
    """
    return GC_MIGRATION.read_text(encoding="utf-8")


def test_gc_migration_grants_delete_only() -> None:
    """Migration 0002 grants exactly DELETE, not broader privileges."""
    sql = _read_gc_migration()

    assert "grant delete on table lubko.jobs to lubko_worker" in sql
    # Must not grant SELECT/INSERT/UPDATE (those belong in 0001).
    assert "grant select" not in sql
    assert "grant insert" not in sql
    assert "grant update" not in sql


def test_gc_migration_is_idempotent() -> None:
    """Migration 0002 uses guarded GRANT safe to re-apply."""
    sql = _read_gc_migration()

    assert "to_regrole('lubko_worker')" in sql
    assert "grant delete" in sql


def test_gc_migration_does_not_alter_table() -> None:
    """Migration 0002 contains no DDL that alters the transport table."""
    sql = _read_gc_migration()

    assert "alter table" not in sql.lower()
    assert "create table" not in sql.lower()
    assert "create index" not in sql.lower()


def test_baseline_and_gc_migration_combined_grant() -> None:
    """Fresh install applying both 0001 and 0002 gets SELECT/INSERT/UPDATE/DELETE.

    The two migrations are composable: 0001 provides the full baseline and
    0002 adds only the missing DELETE for existing installations.
    """
    baseline = _read_baseline_migration()
    gc = _read_gc_migration()

    # 0001 has SELECT/INSERT/UPDATE/DELETE.
    assert "grant select, insert, update, delete on table lubko.jobs to lubko_worker" in baseline
    # 0002 has DELETE only.
    assert "grant delete on table lubko.jobs to lubko_worker" in gc
    # Together they are idempotent and do not conflict.
    combined = baseline + "\n" + gc
    assert combined.count("grant delete") >= 1


# ---------------------------------------------------------------------------
# Real PostgreSQL upgrade regression
# ---------------------------------------------------------------------------


def test_gc_migration_upgrades_existing_install(
    pg_cluster: _pg.PgCluster,
) -> None:
    """Apply 0002 on an existing-install state and verify DELETE is granted.

    Simulates an existing installation that applied 0001 before the GC
    feature: the role exists with SELECT/INSERT/UPDATE but not DELETE.
    After applying 0002, ``has_table_privilege`` confirms DELETE is granted.
    Re-applying 0002 proves idempotency.  Existing privileges are unchanged.
    """
    conninfo = pg_cluster.conninfo()
    with psycopg.connect(conninfo) as conn:
        conn.execute("DROP TABLE IF EXISTS lubko.jobs CASCADE")
        conn.execute("DROP SCHEMA IF EXISTS lubko CASCADE")
        conn.execute("CREATE SCHEMA lubko")
        conn.execute(
            "CREATE TABLE lubko.jobs ("
            "id uuid primary key default gen_random_uuid(),"
            "payload text not null"
            ")"
        )
        conn.execute("DROP ROLE IF EXISTS lubko_worker")
        conn.execute("CREATE ROLE lubko_worker LOGIN")
        # Apply baseline without DELETE (simulates pre-GC install).
        conn.execute("grant usage on schema lubko to lubko_worker")
        conn.execute("grant select, insert, update on table lubko.jobs to lubko_worker")

    # Verify DELETE is not yet granted.
    with psycopg.connect(conninfo) as conn:
        row = conn.execute(
            "SELECT has_table_privilege('lubko_worker', 'lubko.jobs', 'DELETE')"
        ).fetchone()
    assert row is not None
    assert row[0] is False

    # Apply migration 0002.
    with psycopg.connect(conninfo) as conn:
        conn.execute(_read_gc_migration())

    # Verify DELETE is now granted.
    with psycopg.connect(conninfo) as conn:
        row = conn.execute(
            "SELECT has_table_privilege('lubko_worker', 'lubko.jobs', 'DELETE')"
        ).fetchone()
    assert row is not None
    assert row[0] is True

    # Verify existing privileges are unchanged.
    with psycopg.connect(conninfo) as conn:
        for priv in ("SELECT", "INSERT", "UPDATE"):
            row = conn.execute(
                "SELECT has_table_privilege('lubko_worker', 'lubko.jobs', %s)",
                (priv,),
            ).fetchone()
            assert row is not None
            assert row[0] is True, f"{priv} should still be granted"

    # Re-apply 0002 to prove idempotency.
    with psycopg.connect(conninfo) as conn:
        conn.execute(_read_gc_migration())

    with psycopg.connect(conninfo) as conn:
        row = conn.execute(
            "SELECT has_table_privilege('lubko_worker', 'lubko.jobs', 'DELETE')"
        ).fetchone()
    assert row is not None
    assert row[0] is True

    # Cleanup.
    with psycopg.connect(conninfo) as conn:
        conn.execute("REVOKE DELETE ON lubko.jobs FROM lubko_worker")
        conn.execute("REVOKE SELECT, INSERT, UPDATE ON lubko.jobs FROM lubko_worker")
        conn.execute("REVOKE USAGE ON SCHEMA lubko FROM lubko_worker")
        conn.execute("DROP ROLE IF EXISTS lubko_worker")
