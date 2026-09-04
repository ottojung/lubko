"""Cancellation authority requires the worker's canonical UTC timestamp shape."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Self, cast
from uuid import uuid4

from lubko.worker import (
    CANCEL_REQUESTED_AT_PATTERN,
    JobResult,
    Settings,
    discover_cancellations,
    finish_job,
)

if TYPE_CHECKING:
    from lubko.worker import JobsConnection


class _Ctx:
    def __enter__(self) -> None:
        return None

    def __exit__(self, *_args: object) -> None:
        return None


class _Cursor:
    def __init__(self, queries: list[tuple[str, object]], rows: list[tuple[object, ...]]) -> None:
        self.queries = queries
        self.rows = rows

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, query: str, params: object = None) -> None:
        self.queries.append((query, params))

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


def _canonical(value: object) -> bool:
    return (
        isinstance(value, str)
        and not value.startswith("0000")
        and re.fullmatch(CANCEL_REQUESTED_AT_PATTERN, value) is not None
    )


def test_cancellation_timestamp_authority_accepts_only_canonical_worker_shape() -> None:
    """Only worker-shaped string timestamps can become cancellation authority."""
    assert _canonical("2026-09-02T23:10:11.123456Z")
    assert _canonical("2099-12-31T23:59:59.000000Z")

    assert _canonical("2024-02-29T00:00:00.000000Z")

    malformed: tuple[object, ...] = (
        "2026-02-29T20:30:40.123456Z",
        "2025-02-29T20:30:40.123456Z",
        "2026-02-30T20:30:40.123456Z",
        "2026-02-31T20:30:40.123456Z",
        "2026-04-31T20:30:40.123456Z",
        None,
        True,
        1,
        1.5,
        [],
        {},
        "",
        "bogus",
        "2026-9-02T23:10:11.123456Z",
        "2026-13-02T23:10:11.123456Z",
        "2026-12-02T24:10:11.123456Z",
        "2026-12-02T23:60:11.123456Z",
        "2026-12-02T23:10:60.123456Z",
        "2026-12-02T23:10:11Z",
        "0000-01-01T00:00:00.000000Z",
    )
    assert not any(_canonical(value) for value in malformed)


def _assert_strict_cancel_predicate(query: str, params: object) -> None:
    assert "jsonb_typeof((payload::jsonb)->'state'->'cancel_requested_at') = 'string'" in query
    assert "~ %(cancel_requested_at_pattern)s" in query
    assert "left((payload::jsonb)->'state'->>'cancel_requested_at', 4) <> '0000'" in query
    assert "cancel_requested_at' IS NOT NULL" not in query
    assert isinstance(params, dict)
    assert params["cancel_requested_at_pattern"] == CANCEL_REQUESTED_AT_PATTERN


def test_cancellation_discovery_and_finalization_share_strict_timestamp_authority() -> None:
    """Discovery and finalization use the same strict durable cancellation boundary."""
    discover_conn = _Conn()
    settings = Settings.from_environment(server="test-server")
    discover_cancellations(cast("JobsConnection", discover_conn), settings)
    discover_query, discover_params = discover_conn.queries[0]
    _assert_strict_cancel_predicate(discover_query, discover_params)

    finish_conn = _Conn(rows=[("succeeded",)])
    result = JobResult(
        status="succeeded",
        exit_code=0,
        stdout="",
        stderr="",
        cancellation_note=None,
    )
    assert (
        finish_job(cast("JobsConnection", finish_conn), uuid4(), result, server="test-server")
        == "succeeded"
    )
    finish_query, finish_params = finish_conn.queries[0]
    _assert_strict_cancel_predicate(finish_query, finish_params)
    assert finish_query.count("~ %(cancel_requested_at_pattern)s") == 2
