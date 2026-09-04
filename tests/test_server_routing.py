"""Application-level exact server routing for the shared transport queue."""

from __future__ import annotations

from typing import TYPE_CHECKING, Self, cast

from lubko import worker
from lubko.worker import claim_jobs, recover_stale_jobs

if TYPE_CHECKING:
    from lubko.worker import JobsConnection


class _NoopCtx:
    """No-op transaction context used by the recording connection."""

    def __enter__(self) -> None:
        return None

    def __exit__(self, *_args: object) -> None:
        return None


class _RecordingCursor:
    """Record every executed query string while returning no rows."""

    def __init__(self, log: list[str]) -> None:
        self._log = log
        self.row_factory = tuple

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, query: str, _params: object = None) -> None:
        self._log.append(query)

    @staticmethod
    def fetchone() -> None:
        return None

    @staticmethod
    def fetchall() -> list[object]:
        return []


class _RecordingConn:
    """Minimal connection double recording SQL emitted by worker operations."""

    def __init__(self) -> None:
        self.queries: list[str] = []

    @staticmethod
    def transaction() -> _NoopCtx:
        return _NoopCtx()

    def cursor(self, **_kwargs: object) -> _RecordingCursor:
        return _RecordingCursor(self.queries)


def _as_conn(obj: object) -> JobsConnection:
    """Cast a connection double to the worker connection protocol.

    Returns:
        The connection double viewed as the worker connection type.
    """
    return cast("JobsConnection", obj)


def test_claim_jobs_sql_scopes_by_server() -> None:
    """Claiming requires the exact configured server predicate."""
    raw = _RecordingConn()
    claim_jobs(_as_conn(raw), worker.Settings.from_environment(server="srv"), 1)
    assert any(worker.SERVER_MATCH_SQL in query for query in raw.queries)


def test_recover_stale_jobs_sql_scopes_by_server() -> None:
    """Lease recovery requires the exact configured server predicate."""
    raw = _RecordingConn()
    recover_stale_jobs(_as_conn(raw), "srv")
    assert any(worker.SERVER_MATCH_SQL in query for query in raw.queries)
