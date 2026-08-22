"""Tests enforcing the two-column transport invariant and the protocol v3 migrations."""

import contextlib
import json
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


def _v4_shape_constraint_def() -> str:
    """Return a realistic ``pg_get_constraintdef`` of the v4 shape constraint.

    The text mirrors how PostgreSQL normalizes the canonical baseline
    definition (spacing and cast suffixes included), so verification must be
    robust against normalization rather than matching source formatting.
    """
    return (
        "CHECK ((CASE WHEN ((payload)::jsonb ->> 'type'::text) = 'command' THEN "
        "(((((payload)::jsonb -> 'v'::text)) = '4'::jsonb) AND "
        "jsonb_typeof((payload)::jsonb -> 'request'::text) = 'object' AND "
        "(((payload)::jsonb -> 'state'::text) ->> 'status'::text) IS NOT NULL AND "
        "jsonb_typeof((payload)::jsonb -> 'server'::text) = 'string'::text AND "
        "((payload)::jsonb ->> 'server'::text) <> ''::text) "
        "WHEN ((payload)::jsonb ->> 'type'::text) = 'output_chunk' THEN true ELSE true END))"
    )


def _v3_catalog_batches() -> list[list[tuple[str, ...]]]:
    """Return the catalog batches of a pre-cutover v3 schema.

    Same catalog query order as the canonical batches, but the type-aware
    constraint definition predates server-routing enforcement.
    """
    return [
        [
            (TYPE_AWARE_CONSTRAINT_NAME, "CHECK (((payload)::jsonb ->> 'type'::text) = 'x')"),
            ("jobs_payload_is_json_object", "CHECK (true)"),
        ],
        [(CHUNK_OWNER_INDEX_NAME,), (CHUNK_ORDER_INDEX_NAME,), ("jobs_queue_idx",)],
    ]


def _v4_catalog_batches() -> list[list[tuple[str, ...]]]:
    """Return the catalog batches of a canonical migrated v4 schema.

    ``verify_protocol_schema`` runs two catalog reads in a fixed order:
    ``pg_constraint`` (with definitions) and ``pg_indexes``.

    Returns:
        One queued row batch per catalog query, each modeling the canonical
        baseline applied by ``migrations/0001_two_column_protocol.sql``.
    """
    return [
        [
            (TYPE_AWARE_CONSTRAINT_NAME, _v4_shape_constraint_def()),
            ("jobs_payload_is_json_object", "CHECK (true)"),
        ],
        [(CHUNK_OWNER_INDEX_NAME,), (CHUNK_ORDER_INDEX_NAME,), ("jobs_queue_idx",)],
    ]


def test_verify_protocol_schema_accepts_migrated_shape() -> None:
    """A migrated table with the canonical v4 baseline shape passes verification."""
    conn = as_protocol_connection(_QueuedConnection(_v4_catalog_batches()))

    verify_protocol_schema(conn)


def test_verify_protocol_schema_rejects_pre_v4_serverless_constraint() -> None:
    """A table still carrying the pre-v4 (serverless) shape constraint is refused.

    The cutover gate must detect a same-named constraint whose definition does
    not enforce the required top-level server field, even though PostgreSQL
    normalizes spacing and casts in ``pg_get_constraintdef`` output.
    """
    conn = as_protocol_connection(_QueuedConnection(_v3_catalog_batches()))

    with pytest.raises(SchemaInvariantError, match="0003_protocol_v4_server_routing"):
        verify_protocol_schema(conn)


def test_verify_protocol_schema_rejects_missing_type_aware_constraint() -> None:
    """A table without the type-aware constraint is refused."""
    batches = _v4_catalog_batches()
    batches[0] = [
        ("jobs_payload_is_json_object", "CHECK (true)"),
        ("jobs_payload_has_status", "CHECK (true)"),
    ]
    conn = as_protocol_connection(_QueuedConnection(batches))

    with pytest.raises(SchemaInvariantError, match=TYPE_AWARE_CONSTRAINT_NAME):
        verify_protocol_schema(conn)


def test_verify_protocol_schema_rejects_missing_chunk_indexes() -> None:
    """A table without the chunk ownership/ordering indexes is refused."""
    batches = _v4_catalog_batches()
    batches[1] = [("jobs_queue_idx",)]
    conn = as_protocol_connection(_QueuedConnection(batches))

    with pytest.raises(SchemaInvariantError, match="index jobs_chunk"):
        verify_protocol_schema(conn)


def test_verify_protocol_schema_rejects_non_canonical_shape() -> None:
    """A two-column table lacking the canonical v4 output-chunk shape is refused."""
    conn = as_protocol_connection(
        _QueuedConnection([
            [
                ("jobs_payload_has_status", "CHECK (true)"),
                ("jobs_payload_is_json_object", "CHECK (true)"),
            ],
            [("jobs_queue_idx",)],
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


# ---------------------------------------------------------------------------
# Migration 0003: protocol v4 server-routing cutover
# ---------------------------------------------------------------------------

CUTOVER_MIGRATION: Final = MIGRATIONS_DIR / "0003_protocol_v4_server_routing.sql"

V3_SHAPE_CONSTRAINT_SQL: Final = """
    constraint jobs_payload_type_shape check (
        case
            when (payload::jsonb)->>'type' = 'command' then
                jsonb_typeof((payload::jsonb)->'request') = 'object'
                and (((payload::jsonb)->'state'->>'status') is not null)
            when (payload::jsonb)->>'type' = 'output_chunk' then
                jsonb_typeof((payload::jsonb)->'value') = 'string'
                and (((payload::jsonb)->>'thread') is not null)
                and (((payload::jsonb)->>'stream') in ('stdout', 'stderr'))
                and (((payload::jsonb)->>'sequence') ~ '^[0-9]+$')
                and (((payload::jsonb)->>'start') ~ '^[0-9]+$')
                and (((payload::jsonb)->>'end') ~ '^[0-9]+$')
            else true
        end
    )
"""

V3_COMMAND_PAYLOAD: Final = (
    '{"v":3,"type":"command",'
    '"request":{"cwd":"/workspace","process":["echo","hi"]},'
    '"state":{"status":"pending"}}'
)

V4_COMMAND_PAYLOAD: Final = (
    '{"v":4,"type":"command","server":"alpha-server",'
    '"request":{"cwd":"/workspace","process":["echo","hi"]},'
    '"state":{"status":"pending"}}'
)


def _create_v3_table(conn: psycopg.Connection[tuple[object, ...]]) -> None:
    """Create a table carrying the pre-cutover v3 shape constraint."""
    conn.execute("DROP TABLE IF EXISTS lubko.jobs CASCADE")
    conn.execute(
        "CREATE TABLE lubko.jobs ("
        "id uuid primary key default gen_random_uuid(),"
        "payload text not null" + V3_SHAPE_CONSTRAINT_SQL + ")"
    )


def test_cutover_migration_requires_truncate_first(pg_cluster: _pg.PgCluster) -> None:
    """Applying 0003 against a table still holding v3 rows fails fast.

    The server-required CHECK validation must refuse rows without a routing
    identity so the transport can never be half-upgraded; the supported order
    is quiesce, truncate, then apply.
    """
    conninfo = pg_cluster.conninfo()
    sql = CUTOVER_MIGRATION.read_text(encoding="utf-8")
    with psycopg.connect(conninfo) as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS lubko")
        _create_v3_table(conn)
        conn.execute("INSERT INTO lubko.jobs (payload) VALUES (%s)", (V3_COMMAND_PAYLOAD,))

    with (
        psycopg.connect(conninfo, autocommit=True) as conn,
        pytest.raises(psycopg.Error, match="server"),
    ):
        conn.execute(sql)


def test_cutover_migration_upgrades_existing_v3_table(pg_cluster: _pg.PgCluster) -> None:
    """An existing v3 table is cut over by truncating and then applying 0003.

    The documented sequence is exercised end to end against real PostgreSQL:
    quiesce/drain, TRUNCATE the old rows away, apply 0003, and prove the
    upgraded table both verifies as canonical protocol v4 shape and enforces
    the required non-empty server field at the database level.
    """
    conninfo = pg_cluster.conninfo()
    sql = CUTOVER_MIGRATION.read_text(encoding="utf-8")
    with psycopg.connect(conninfo) as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS lubko")
        _create_v3_table(conn)
        conn.execute("INSERT INTO lubko.jobs (payload) VALUES (%s)", (V3_COMMAND_PAYLOAD,))
        # The destructive row cutover: no legacy row is converted or preserved.
        conn.execute("TRUNCATE lubko.jobs")

    with psycopg.connect(conninfo, autocommit=True) as conn:
        conn.execute(sql)
    with psycopg.connect(conninfo) as conn:
        # A real pre-v4 installation already carried the chunk indexes from
        # its original baseline; re-applying the (now v4, idempotent) baseline
        # restores exactly those objects on the upgraded table. The
        # create-table statement is a no-op because the table exists with its
        # new constraint.
        conn.execute(BASELINE_MIGRATION.read_text(encoding="utf-8"))

    # The verifier accepts the upgraded table.
    with psycopg.connect(conninfo) as conn:
        verify_jobs_table_invariant(conn)
        verify_protocol_schema(conn)
        # A fresh v4 round trip works...
        row = conn.execute(
            "INSERT INTO lubko.jobs (payload) VALUES (%s) RETURNING id",
            (V4_COMMAND_PAYLOAD,),
        ).fetchone()
        assert row is not None
        # ...and unaddressed payloads are rejected by the database itself.
        for bad in (
            V3_COMMAND_PAYLOAD,
            (
                '{"v":4,"type":"command","server":"",'
                '"request":{"cwd":"/x","process":["ls"]},"state":{"status":"pending"}}'
            ),
        ):
            with pytest.raises(psycopg.errors.CheckViolation), conn.transaction():
                conn.execute("INSERT INTO lubko.jobs (payload) VALUES (%s)", (bad,))

    # Re-applying 0003 proves idempotency.
    with psycopg.connect(conninfo) as conn:
        conn.execute(sql)
    with psycopg.connect(conninfo) as conn:
        verify_protocol_schema(conn)


def test_fresh_baseline_enforces_server_field(jobs_db: str) -> None:
    """A fresh-install baseline table rejects payloads without a server field."""
    for bad in (
        V3_COMMAND_PAYLOAD,
        '{"v":4,"type":"output_chunk","thread":"'
        + "0" * 8
        + "-0000-4000-8000-000000000000"
        + '","stream":"stdout","sequence":0,"start":0,"end":1,"value":"x","previous":null}',
    ):
        with (
            psycopg.connect(jobs_db) as conn,
            pytest.raises(psycopg.errors.CheckViolation),
            conn.transaction(),
        ):
            conn.execute("INSERT INTO lubko.jobs (payload) VALUES (%s)", (bad,))


def test_fresh_baseline_rejects_legacy_rows_even_with_server(jobs_db: str) -> None:
    """Legacy v2/v3-shaped rows are refused even when they carry a server.

    The canonical v4 binding enforces the actual protocol version as a JSON
    number, so a v2/v3 command or output_chunk payload cannot enter the table
    by merely adding a ``server`` field, and a string ``"4"`` version is not
    aliased onto the numeric protocol version.
    """
    for bad in (
        # v2 command with a legacy request.command field plus a valid server.
        (
            '{"v":2,"type":"command","server":"alpha-server",'
            '"request":{"cwd":"/x","command":"echo hi"},"state":{"status":"pending"}}'
        ),
        # v3 command with full v4 shape except the numeric version.
        (
            '{"v":3,"type":"command","server":"alpha-server",'
            '"request":{"cwd":"/x","process":["ls"]},"state":{"status":"pending"}}'
        ),
        # String-aliased version is not a protocol version.
        (
            '{"v":"4","type":"command","server":"alpha-server",'
            '"request":{"cwd":"/x","process":["ls"]},"state":{"status":"pending"}}'
        ),
        # Legacy output_chunk with a server but no v4 version.
        (
            '{"v":3,"type":"output_chunk","server":"alpha-server","thread":"'
            + "0" * 8
            + "-0000-4000-8000-000000000000"
            + '","stream":"stdout","sequence":0,"start":0,"end":1,'
            '"value":"x","previous":null}'
        ),
    ):
        with (
            psycopg.connect(jobs_db) as conn,
            pytest.raises(psycopg.errors.CheckViolation),
            conn.transaction(),
        ):
            conn.execute("INSERT INTO lubko.jobs (payload) VALUES (%s)", (bad,))


def test_cutover_refuses_non_v4_rows_even_with_server(pg_cluster: _pg.PgCluster) -> None:
    """Applying 0003 fails fast while non-v4 rows remain, server included.

    The pre-validation diagnostic counts command/output_chunk payloads that
    lack a non-empty server string OR the numeric protocol version 4, so an
    old v2/v3 row that happens to carry a server still blocks the cutover
    until the destructive truncate runs.
    """
    conninfo = pg_cluster.conninfo()
    sql = CUTOVER_MIGRATION.read_text(encoding="utf-8")
    with psycopg.connect(conninfo) as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS lubko")
        _create_v3_table(conn)
        conn.execute(
            "INSERT INTO lubko.jobs (payload) VALUES (%s)",
            (
                (
                    '{"v":3,"type":"command","server":"alpha-server",'
                    '"request":{"cwd":"/x","process":["ls"]},"state":{"status":"pending"}}'
                ),
            ),
        )

    with (
        psycopg.connect(conninfo, autocommit=True) as conn,
        pytest.raises(psycopg.Error, match="version 4"),
    ):
        conn.execute(sql)


def test_refused_cutover_leaves_v3_schema_state_intact(pg_cluster: _pg.PgCluster) -> None:
    """A refused pre-cutover apply leaves the original v3 schema untouched.

    The migration's nonconforming-row preflight runs before any destructive or
    schema-changing DDL inside one explicit transaction, so an incorrect apply
    against a table that still holds v3 rows raises without dropping the v3
    constraint, without adding a NOT VALID v4 constraint, and without
    rebuilding the queue index — no half-upgraded state is possible even under
    ordinary autocommit execution.
    """
    conninfo = pg_cluster.conninfo()
    sql = CUTOVER_MIGRATION.read_text(encoding="utf-8")
    with psycopg.connect(conninfo) as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS lubko")
        _create_v3_table(conn)
        conn.execute("INSERT INTO lubko.jobs (payload) VALUES (%s)", (V3_COMMAND_PAYLOAD,))

    with psycopg.connect(conninfo) as conn:
        before_constraint = conn.execute(
            "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
            "WHERE conrelid = 'lubko.jobs'::regclass AND conname = %s",
            ("jobs_payload_type_shape",),
        ).fetchone()
        before_indexes = conn.execute(
            "SELECT indexname FROM pg_indexes "
            "WHERE schemaname = 'lubko' AND tablename = 'jobs' ORDER BY indexname"
        ).fetchall()
    assert before_constraint is not None
    assert "server" not in before_constraint[0]

    with (
        psycopg.connect(conninfo, autocommit=True) as conn,
        pytest.raises(psycopg.Error, match="server"),
    ):
        conn.execute(sql)

    with psycopg.connect(conninfo) as conn:
        after_constraint = conn.execute(
            "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
            "WHERE conrelid = 'lubko.jobs'::regclass AND conname = %s",
            ("jobs_payload_type_shape",),
        ).fetchone()
        after_indexes = conn.execute(
            "SELECT indexname FROM pg_indexes "
            "WHERE schemaname = 'lubko' AND tablename = 'jobs' ORDER BY indexname"
        ).fetchall()
    assert after_constraint == before_constraint
    assert after_indexes == before_indexes
    # The original row is still there and still accepted by the intact v3 shape.
    with psycopg.connect(conninfo) as conn:
        row = conn.execute("SELECT count(*) FROM lubko.jobs").fetchone()
    assert row is not None
    assert row[0] == 1


@pytest.mark.parametrize(
    "server_value",
    [123, True, None, {"name": "x"}, ["alpha"], 1.5],
)
def test_fresh_baseline_rejects_non_string_server(server_value: object, jobs_db: str) -> None:
    """The DB enforces server as a JSON string, not merely text-coercible.

    A JSON number, boolean, null, object, or array in the server field violates
    ``jobs_payload_type_shape`` even when its ``->>`` text rendering would be
    non-empty, so a daemon's ``->>``-based claim predicate can never alias a
    malformed value onto its configured identity.
    """
    payload = json.dumps({
        "v": 4,
        "type": "command",
        "server": server_value,
        "request": {"cwd": "/x", "process": ["ls"]},
        "state": {"status": "pending"},
    })
    with psycopg.connect(jobs_db) as conn, pytest.raises(psycopg.errors.CheckViolation):
        conn.execute("INSERT INTO lubko.jobs (payload) VALUES (%s)", (payload,))
