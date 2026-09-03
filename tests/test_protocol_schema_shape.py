"""Startup verification for the canonical jobs payload-shape constraint."""

from __future__ import annotations

from typing import TYPE_CHECKING, Self, cast

import pytest

from lubko import worker

if TYPE_CHECKING:
    from lubko.worker import JobsConnection


STRICT_TYPE_SHAPE = """
CHECK (
CASE
    WHEN jsonb_typeof(((payload)::jsonb -> 'type'::text))
         IS NOT DISTINCT FROM 'string'::text
         AND (((payload)::jsonb ->> 'type'::text) = 'command'::text)
    THEN
        (CASE
            WHEN jsonb_typeof(((payload)::jsonb -> 'v'::text))
                 IS NOT DISTINCT FROM 'number'::text
            THEN (((payload)::jsonb -> 'v'::text))::numeric
                 = floor((((payload)::jsonb -> 'v'::text))::numeric)
            ELSE false
         END)
        AND jsonb_typeof((((payload)::jsonb -> 'state'::text) -> 'status'::text))
            IS NOT DISTINCT FROM 'string'::text
        AND ((((payload)::jsonb -> 'state'::text) ->> 'status'::text)
            = ANY (ARRAY['pending'::text, 'running'::text, 'succeeded'::text,
                         'failed'::text, 'cancelled'::text]))
        AND coalesce(jsonb_typeof(((payload)::jsonb -> 'server'::text)), ''::text)
            = 'string'::text
        AND coalesce(((payload)::jsonb ->> 'server'::text), ''::text) <> ''::text
    WHEN jsonb_typeof(((payload)::jsonb -> 'type'::text))
         IS NOT DISTINCT FROM 'string'::text
         AND (((payload)::jsonb ->> 'type'::text) = 'output_chunk'::text)
    THEN
        coalesce(jsonb_typeof(((payload)::jsonb -> 'thread'::text)), ''::text)
            = 'string'::text
        AND coalesce(jsonb_typeof(((payload)::jsonb -> 'server'::text)), ''::text)
            = 'string'::text
        AND coalesce(((payload)::jsonb ->> 'server'::text), ''::text) <> ''::text
        AND coalesce(jsonb_typeof(((payload)::jsonb -> 'stream'::text)), ''::text)
            = 'string'::text
        AND (((payload)::jsonb ->> 'stream'::text)
            = ANY (ARRAY['stdout'::text, 'stderr'::text]))
        AND (CASE WHEN jsonb_typeof(((payload)::jsonb -> 'sequence'::text))
                 IS NOT DISTINCT FROM 'number'::text
             THEN (((payload)::jsonb -> 'sequence'::text))::numeric
                    = floor((((payload)::jsonb -> 'sequence'::text))::numeric)
                  AND (((payload)::jsonb -> 'sequence'::text))::numeric >= 0
             ELSE false END)
        AND (CASE WHEN jsonb_typeof(((payload)::jsonb -> 'start'::text))
                 IS NOT DISTINCT FROM 'number'::text
             THEN (((payload)::jsonb -> 'start'::text))::numeric
                    = floor((((payload)::jsonb -> 'start'::text))::numeric)
                  AND (((payload)::jsonb -> 'start'::text))::numeric >= 0
             ELSE false END)
        AND (CASE WHEN jsonb_typeof(((payload)::jsonb -> 'end'::text))
                 IS NOT DISTINCT FROM 'number'::text
             THEN (((payload)::jsonb -> 'end'::text))::numeric
                    = floor((((payload)::jsonb -> 'end'::text))::numeric)
                  AND (((payload)::jsonb -> 'end'::text))::numeric >= 0
             ELSE false END)
    ELSE false
END)
"""

STALE_ROUTING_SHAPE = """
CHECK (
CASE
    WHEN (((payload)::jsonb ->> 'type'::text) = 'command'::text) THEN
        (((payload)::jsonb -> 'v'::text) = '4'::jsonb)
        AND jsonb_typeof(((payload)::jsonb -> 'request'::text)) = 'object'::text
        AND ((((payload)::jsonb -> 'state'::text) ->> 'status'::text) IS NOT NULL)
        AND coalesce(jsonb_typeof(((payload)::jsonb -> 'server'::text)), ''::text)
            = 'string'::text
        AND coalesce(((payload)::jsonb ->> 'server'::text), ''::text) <> ''::text
    WHEN (((payload)::jsonb ->> 'type'::text) = 'output_chunk'::text) THEN
        (((payload)::jsonb -> 'v'::text) = '4'::jsonb)
        AND (((payload)::jsonb ->> 'thread'::text) IS NOT NULL)
        AND (((payload)::jsonb ->> 'stream'::text)
            = ANY (ARRAY['stdout'::text, 'stderr'::text]))
        AND (((payload)::jsonb ->> 'sequence'::text) ~ '^[0-9]+$'::text)
        AND (((payload)::jsonb ->> 'start'::text) ~ '^[0-9]+$'::text)
        AND (((payload)::jsonb ->> 'end'::text) ~ '^[0-9]+$'::text)
    ELSE true
END)
"""


class _NoopCtx:
    def __enter__(self) -> None:
        return None

    def __exit__(self, *_args: object) -> None:
        return None


class _SchemaCursor:
    def __init__(self, definition: str) -> None:
        self.definition = definition
        self.query_number = 0

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, _query: str, _params: object = None) -> None:
        self.query_number += 1

    def fetchall(self) -> list[tuple[object, ...]]:
        if self.query_number == 1:
            return [(worker.TYPE_AWARE_CONSTRAINT_NAME, self.definition)]
        return [(worker.CHUNK_OWNER_INDEX_NAME,), (worker.CHUNK_ORDER_INDEX_NAME,)]


class _SchemaConn:
    def __init__(self, definition: str) -> None:
        self.definition = definition

    @staticmethod
    def transaction() -> _NoopCtx:
        return _NoopCtx()

    def cursor(self, **_kwargs: object) -> _SchemaCursor:
        return _SchemaCursor(self.definition)


def _as_conn(conn: object) -> JobsConnection:
    return cast("JobsConnection", conn)


def test_current_payload_shape_is_accepted() -> None:
    """The current strict shape passes startup verification."""
    assert worker._has_current_type_shape_constraint(STRICT_TYPE_SHAPE)
    worker.verify_protocol_schema(_as_conn(_SchemaConn(STRICT_TYPE_SHAPE)))


def test_stale_routing_shape_is_rejected() -> None:
    """A routing-aware but permissive historical shape fails closed."""
    assert not worker._has_current_type_shape_constraint(STALE_ROUTING_SHAPE)
    with pytest.raises(worker.SchemaInvariantError, match="stale or incomplete"):
        worker.verify_protocol_schema(_as_conn(_SchemaConn(STALE_ROUTING_SHAPE)))


@pytest.mark.parametrize(
    "strict_fragment",
    [
        "jsonb_typeof(((payload)::jsonb -> 'type'::text))",
        "jsonb_typeof((((payload)::jsonb -> 'state'::text) -> 'status'::text))",
        "coalesce(jsonb_typeof(((payload)::jsonb -> 'thread'::text)), ''::text)",
        "coalesce(jsonb_typeof(((payload)::jsonb -> 'stream'::text)), ''::text)",
        "jsonb_typeof(((payload)::jsonb -> 'sequence'::text))",
        "jsonb_typeof(((payload)::jsonb -> 'start'::text))",
        "jsonb_typeof(((payload)::jsonb -> 'end'::text))",
        "floor((((payload)::jsonb -> 'sequence'::text))::numeric)",
        "floor((((payload)::jsonb -> 'start'::text))::numeric)",
        "floor((((payload)::jsonb -> 'end'::text))::numeric)",
    ],
)
def test_payload_shape_rejects_missing_strict_semantics(strict_fragment: str) -> None:
    """Removing any required strict semantic family makes the shape stale."""
    weakened = STRICT_TYPE_SHAPE.replace(strict_fragment, "true /* weakened */")
    assert not worker._has_current_type_shape_constraint(weakened)


@pytest.mark.parametrize(
    "weakened",
    [
        f"({STRICT_TYPE_SHAPE}) OR true",
        f"({STRICT_TYPE_SHAPE}) OR (1 = 1)",
        f"CASE WHEN true THEN true ELSE ({STRICT_TYPE_SHAPE}) END",
        f"CASE WHEN 1 = 1 THEN (1 = 1) ELSE ({STRICT_TYPE_SHAPE}) END",
    ],
)
def test_payload_shape_rejects_permissive_branches_with_all_markers(
    weakened: str,
) -> None:
    """Extra permissive branches cannot hide behind the canonical markers."""
    assert not worker._has_current_type_shape_constraint(weakened)
    with pytest.raises(worker.SchemaInvariantError, match="stale or incomplete"):
        worker.verify_protocol_schema(_as_conn(_SchemaConn(weakened)))


def test_payload_shape_matching_tolerates_postgres_rendering_noise() -> None:
    """PostgreSQL formatting, casing, casts, and grouping do not affect matching."""
    noisy = STRICT_TYPE_SHAPE.upper().replace(" ", "\n\t")
    assert worker._has_current_type_shape_constraint(noisy)
