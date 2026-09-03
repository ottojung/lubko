"""Lease recovery authority requires the worker canonical UTC timestamp shape."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Self, cast

from lubko.worker import LEASE_EXPIRES_AT_PATTERN, recover_stale_jobs

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

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, query: str, params: object = None) -> None:
        self.queries.append((query, params))

    @staticmethod
    def fetchall() -> list[tuple[object, ...]]:
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
        and re.fullmatch(LEASE_EXPIRES_AT_PATTERN, value) is not None
    )


def test_lease_timestamp_authority_accepts_only_canonical_worker_shape() -> None:
    """Only canonical worker-shaped lease strings can authorize recovery."""
    assert _canonical("2026-09-02T23:10:11.123456Z")
    assert _canonical("2099-12-31T23:59:59.000000Z")
    malformed: tuple[object, ...] = (
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


def test_recovery_uses_strict_lease_timestamp_authority() -> None:
    """Recovery SQL must fail closed before comparing persisted lease text."""
    conn = _Conn()
    recover_stale_jobs(cast("JobsConnection", conn), "test-server")
    query, params = conn.queries[0]
    assert "jsonb_typeof((payload::jsonb)->'state'->'lease_expires_at') = 'string'" in query
    assert "~ %(lease_expires_at_pattern)s" in query
    assert "left((payload::jsonb)->'state'->>'lease_expires_at', 4) <> '0000'" in query
    assert "lease_expires_at' IS NOT NULL" not in query
    assert "FOR UPDATE SKIP LOCKED" in query
    assert isinstance(params, dict)
    assert params["lease_expires_at_pattern"] == LEASE_EXPIRES_AT_PATTERN
