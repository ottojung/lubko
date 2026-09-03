"""Retention authority for persisted worker completion timestamps."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Self, cast

from lubko.worker import GC_FINISHED_AT_PATTERN, Settings, collect_transport

if TYPE_CHECKING:
    from lubko.worker import JobsConnection


class _Ctx:
    def __enter__(self) -> None:
        return None

    def __exit__(self, *_args: object) -> None:
        return None


class _Cursor:
    def __init__(self, queries: list[tuple[str, object]]) -> None:
        self.queries = queries
        self.rowcount = 0

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, query: str, params: object = None) -> None:
        self.queries.append((query, params))
        self.rowcount = 0

    @staticmethod
    def fetchall() -> list[object]:
        return []


class _Conn:
    def __init__(self) -> None:
        self.queries: list[tuple[str, object]] = []

    @staticmethod
    def transaction() -> _Ctx:
        return _Ctx()

    def cursor(self, **_kwargs: object) -> _Cursor:
        return _Cursor(self.queries)


def _canonical(value: object) -> bool:
    return (
        isinstance(value, str)
        and not value.startswith("0000")
        and re.fullmatch(GC_FINISHED_AT_PATTERN, value) is not None
    )


def test_gc_timestamp_authority_accepts_only_canonical_worker_shape() -> None:
    """Only worker-shaped string timestamps can become retention authority."""
    assert _canonical("2026-09-01T20:30:40.123456Z")
    assert _canonical("2099-12-31T23:59:59.000000Z")

    assert _canonical("2024-02-29T00:00:00.000000Z")

    malformed: tuple[object, ...] = (
        "2026-02-29T20:30:40.123456Z",
        "2025-02-29T20:30:40.123456Z",
        "2026-02-30T20:30:40.123456Z",
        "2026-02-31T20:30:40.123456Z",
        "2026-04-31T20:30:40.123456Z",
        0,
        False,
        None,
        {},
        [],
        "",
        "0",
        "2026-9-01T20:30:40.123456Z",
        "2026-13-01T20:30:40.123456Z",
        "2026-12-01T24:30:40.123456Z",
        "2026-12-01T20:60:40.123456Z",
        "2026-12-01T20:30:60.123456Z",
        "0000-01-01T00:00:00.000000Z",
    )
    assert not any(_canonical(value) for value in malformed)


def test_gc_revalidates_timestamp_authority_before_phase_two_deletion() -> None:
    """A stale GC mark cannot bypass timestamp validation during deletion."""
    conn = _Conn()
    settings = Settings.from_environment(server="test-server")

    collect_transport(cast("JobsConnection", conn), settings)

    mark_query, mark_params = conn.queries[0]
    drain_query, drain_params = conn.queries[1]
    for query, params in (
        (mark_query, mark_params),
        (drain_query, drain_params),
    ):
        assert "jsonb_typeof((payload::jsonb)->'state'->'finished_at') = 'string'" in query
        assert "~ %(gc_finished_at_pattern)s" in query
        assert "left((payload::jsonb)->'state'->>'finished_at', 4) <> '0000'" in query
        assert "finished_at') < gc_params.cutoff" in query
        assert isinstance(params, dict)
        assert params["gc_finished_at_pattern"] == GC_FINISHED_AT_PATTERN

    assert "IN ('succeeded', 'failed', 'cancelled')" in drain_query
    assert "(payload::jsonb)->'state'->'gc' = 'true'::jsonb" in drain_query
    assert "(payload::jsonb)->'state'->'gc' IS NULL" in mark_query
    assert "(payload::jsonb)->'state'->'gc' = 'null'::jsonb" in mark_query
    assert "(payload::jsonb)->'state'->'gc' = 'false'::jsonb" in mark_query
    assert "->>'gc'" not in mark_query
    assert "->>'gc'" not in drain_query
