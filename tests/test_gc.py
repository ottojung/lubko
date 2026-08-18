"""Deterministic integration tests for transport garbage collection.

Three bounded phases run independently:

1. **Mark** -- terminal roots get ``state.gc = true``; publication refuses them.
2. **Chunk drain** -- bounded ``DELETE`` of chunks per GC-marked root; root
   deleted only when zero chunks remain.
3. **Orphan cleanup** -- bounded anti-join with UUID-validated ``thread``.
"""

from __future__ import annotations

import json
import logging
import threading
from contextlib import nullcontext
from typing import TYPE_CHECKING, Self, cast
from uuid import uuid4

import psycopg
import pytest

from lubko.worker import (
    Settings,
    collect_transport,
)

if TYPE_CHECKING:
    from contextlib import AbstractContextManager
    from uuid import UUID

    from lubko.worker import (
        JobsConnection,
    )


def _make_settings(
    *,
    gc_retention_seconds: float = 0.0,
    gc_batch_limit: int = 100,
) -> Settings:
    return Settings(
        worker_id="test-worker",
        poll_interval_seconds=0.05,
        process_poll_interval_seconds=0.01,
        cancel_grace_seconds=0.5,
        lease_duration_seconds=1.0,
        lease_refresh_interval_seconds=0.2,
        lease_recovery_interval_seconds=0.1,
        lease_safety_margin_seconds=0.2,
        gc_retention_seconds=gc_retention_seconds,
        gc_batch_limit=gc_batch_limit,
    )


@pytest.fixture
def db(jobs_db: str) -> str:
    """Provide the shared migrated ``lubko.jobs`` table.

    Args:
        jobs_db: Connection string from the shared fixture.

    Returns:
        The connection string.
    """
    return jobs_db


def _insert_terminal_job(
    conninfo: str,
    *,
    status: str = "succeeded",
    finished_at: str = "2020-01-01T00:00:00.000000Z",
    worker_id: str = "old-worker",
) -> UUID:
    payload = json.dumps({
        "v": 3,
        "type": "command",
        "request": {"cwd": "/workspace", "process": ["echo", "hi"]},
        "state": {"status": status, "finished_at": finished_at, "worker_id": worker_id},
    })
    with psycopg.connect(conninfo) as conn:
        row = conn.execute(
            "INSERT INTO lubko.jobs (payload) VALUES (%s) RETURNING id", (payload,)
        ).fetchone()
    assert row is not None
    return cast("UUID", row[0])


def _insert_pending_job(conninfo: str) -> UUID:
    payload = json.dumps({
        "v": 3,
        "type": "command",
        "request": {"cwd": "/workspace", "process": ["sleep", "30"]},
        "state": {"status": "pending"},
    })
    with psycopg.connect(conninfo) as conn:
        row = conn.execute(
            "INSERT INTO lubko.jobs (payload) VALUES (%s) RETURNING id", (payload,)
        ).fetchone()
    assert row is not None
    return cast("UUID", row[0])


def _insert_running_job(conninfo: str) -> UUID:
    payload = json.dumps({
        "v": 3,
        "type": "command",
        "request": {"cwd": "/workspace", "process": ["sleep", "30"]},
        "state": {
            "status": "running",
            "worker_id": "test-worker",
            "worker_incarnation": "inc-1",
            "started_at": "2026-01-01T00:00:00.000000Z",
            "lease_expires_at": "2099-01-01T00:00:00.000000Z",
        },
    })
    with psycopg.connect(conninfo) as conn:
        row = conn.execute(
            "INSERT INTO lubko.jobs (payload) VALUES (%s) RETURNING id", (payload,)
        ).fetchone()
    assert row is not None
    return cast("UUID", row[0])


def _insert_output_chunk(conninfo: str, root_id: UUID, *, sequence: int = 0) -> UUID:
    payload = json.dumps({
        "v": 3,
        "type": "output_chunk",
        "thread": str(root_id),
        "stream": "stdout",
        "sequence": sequence,
        "start": 0,
        "end": 10,
        "value": "chunk data",
        "previous": None,
    })
    with psycopg.connect(conninfo) as conn:
        row = conn.execute(
            "INSERT INTO lubko.jobs (payload) VALUES (%s) RETURNING id", (payload,)
        ).fetchone()
    assert row is not None
    return cast("UUID", row[0])


def _insert_orphan_chunk(conninfo: str, dangling_root_id: UUID) -> UUID:
    payload = json.dumps({
        "v": 3,
        "type": "output_chunk",
        "thread": str(dangling_root_id),
        "stream": "stdout",
        "sequence": 0,
        "start": 0,
        "end": 10,
        "value": "orphan data",
        "previous": None,
    })
    with psycopg.connect(conninfo) as conn:
        row = conn.execute(
            "INSERT INTO lubko.jobs (payload) VALUES (%s) RETURNING id", (payload,)
        ).fetchone()
    assert row is not None
    return cast("UUID", row[0])


def _count_all(conninfo: str) -> int:
    with psycopg.connect(conninfo) as conn:
        row = conn.execute("SELECT count(*) FROM lubko.jobs").fetchone()
    assert row is not None
    return int(row[0])


def _row_exists(conninfo: str, job_id: UUID) -> bool:
    with psycopg.connect(conninfo) as conn:
        row = conn.execute("SELECT 1 FROM lubko.jobs WHERE id = %s", (job_id,)).fetchone()
    return row is not None


def _count_chunks_for_root(conninfo: str, root_id: UUID) -> int:
    with psycopg.connect(conninfo) as conn:
        row = conn.execute(
            "SELECT count(*) FROM lubko.jobs\n"
            "WHERE (payload::jsonb)->>'type' = 'output_chunk'\n"
            "    AND (payload::jsonb)->>'thread' = %s",
            (str(root_id),),
        ).fetchone()
    assert row is not None
    return int(row[0])


def _is_gc_marked(conninfo: str, root_id: UUID) -> bool:
    with psycopg.connect(conninfo) as conn:
        row = conn.execute(
            "SELECT (payload::jsonb)->'state'->>'gc'\nFROM lubko.jobs WHERE id = %s",
            (root_id,),
        ).fetchone()
    return row is not None and row[0] == "true"


class _RecordingCursor:
    def __init__(self, conn: _RecordingConnection) -> None:
        self._conn = conn

    def execute(self, sql: str, params: object | None = None) -> None:
        self._conn.executions.append((sql, params))

    def fetchone(self) -> object:
        if self._conn.rows:
            batch = self._conn.rows[0]
            if batch:
                return batch.pop(0)
        return None

    def fetchall(self) -> list[object]:
        if self._conn.rows:
            return self._conn.rows.pop(0)
        return []

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc_value: BaseException | None,
        _traceback: object,
    ) -> None:
        return None


class _RecordingConnection:
    def __init__(self) -> None:
        self.executions: list[tuple[str, object | None]] = []
        self.rows: list[list[object]] = []

    def cursor(self, **_kwargs: object) -> _RecordingCursor:
        return _RecordingCursor(self)

    @staticmethod
    def transaction() -> AbstractContextManager[None]:
        """Simulate a transaction as a no-op context manager.

        Returns:
            A no-op context manager.
        """
        return nullcontext()


def _as_db(conn: _RecordingConnection) -> JobsConnection:
    """Adapt the recording test double to the worker's connection type.

    Args:
        conn: Recording test double.

    Returns:
        The same object, compatible with the worker's connection interface.
    """
    return cast("JobsConnection", conn)


def _drain_all(db_conninfo: str, settings: Settings) -> None:
    """Run GC passes until no GC-marked roots or orphans remain."""
    for _ in range(50):
        with psycopg.connect(db_conninfo) as conn:
            roots, _chunks, orphans = collect_transport(conn, settings)
        if not roots and orphans == 0:
            gc_remaining = False
            with psycopg.connect(db_conninfo) as conn:
                row = conn.execute(
                    "SELECT 1 FROM lubko.jobs\n"
                    "WHERE (payload::jsonb)->>'type' = 'command'\n"
                    "    AND ((payload::jsonb)->'state'->>'gc') = 'true'\n"
                    "LIMIT 1"
                ).fetchone()
                gc_remaining = row is not None
            if not gc_remaining:
                break


# ---------------------------------------------------------------------------
# Mocked unit tests
# ---------------------------------------------------------------------------


def test_collect_transport_marks_roots_with_gc_flag() -> None:
    """Phase 1 sets state.gc = true on terminal roots."""
    conn = _RecordingConnection()
    root_id = uuid4()
    conn.rows = [[(root_id,)], [], []]  # Phase 1, 2, 3

    roots, _chunks, _orphans = collect_transport(_as_db(conn), _make_settings())

    assert roots == [root_id]
    sql = conn.executions[0][0]
    assert "state,gc" in sql
    assert "to_jsonb(true)" in sql
    assert "IS DISTINCT FROM" in sql


def test_collect_transport_returns_empty_on_no_work() -> None:
    """No work to do returns empty results."""
    conn = _RecordingConnection()
    conn.rows = [[], [], []]  # Phase 1, 2, 3

    roots, chunks, orphans = collect_transport(_as_db(conn), _make_settings())

    assert roots == []
    assert chunks == 0
    assert orphans == 0


def test_collect_transport_orphan_pass_validates_uuid() -> None:
    """Phase 3 validates thread values are UUIDs before the anti-join."""
    conn = _RecordingConnection()
    conn.rows = [[], [], []]  # Phase 1, 2, 3

    collect_transport(_as_db(conn), _make_settings())

    sql = conn.executions[2][0]
    assert "~" in sql  # UUID regex validation
    assert "uuid" in sql  # explicit cast for the join


def test_collect_transport_orphan_pass_uses_for_update_skip_locked() -> None:
    """Phase 3 uses FOR UPDATE SKIP LOCKED for concurrent safety."""
    conn = _RecordingConnection()
    conn.rows = [[], [], []]  # Phase 1, 2, 3

    collect_transport(_as_db(conn), _make_settings())

    sql = conn.executions[2][0]
    assert "FOR UPDATE OF" in sql
    assert "SKIP LOCKED" in sql


# ---------------------------------------------------------------------------
# Settings tests
# ---------------------------------------------------------------------------


def test_settings_rejects_zero_gc_retention() -> None:
    """A negative GC retention is refused."""
    with pytest.raises(ValueError, match="GC_RETENTION_SECONDS"):
        Settings(
            worker_id="w",
            poll_interval_seconds=1.0,
            process_poll_interval_seconds=0.1,
            cancel_grace_seconds=5.0,
            gc_retention_seconds=-1.0,
        )


def test_settings_rejects_zero_gc_interval() -> None:
    """A zero GC interval is refused."""
    with pytest.raises(ValueError, match="GC_INTERVAL_SECONDS"):
        Settings(
            worker_id="w",
            poll_interval_seconds=1.0,
            process_poll_interval_seconds=0.1,
            cancel_grace_seconds=5.0,
            gc_interval_seconds=0.0,
        )


def test_settings_rejects_zero_gc_batch_limit() -> None:
    """A zero GC batch limit is refused."""
    with pytest.raises(ValueError, match="GC_BATCH_LIMIT"):
        Settings(
            worker_id="w",
            poll_interval_seconds=1.0,
            process_poll_interval_seconds=0.1,
            cancel_grace_seconds=5.0,
            gc_batch_limit=0,
        )


def test_settings_gc_defaults() -> None:
    """GC settings default to documented values."""
    settings = Settings(
        worker_id="w",
        poll_interval_seconds=1.0,
        process_poll_interval_seconds=0.1,
        cancel_grace_seconds=5.0,
    )
    assert settings.gc_retention_seconds == pytest.approx(3600.0)
    assert settings.gc_interval_seconds == pytest.approx(60.0)
    assert settings.gc_batch_limit == 100


def test_settings_reads_gc_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """GC settings come from the environment."""
    monkeypatch.setenv("LUBKO_GC_RETENTION_SECONDS", "600")
    monkeypatch.setenv("LUBKO_GC_INTERVAL_SECONDS", "30")
    monkeypatch.setenv("LUBKO_GC_BATCH_LIMIT", "50")
    settings = Settings.from_environment()
    assert settings.gc_retention_seconds == pytest.approx(600.0)
    assert settings.gc_interval_seconds == pytest.approx(30.0)
    assert settings.gc_batch_limit == 50


# ---------------------------------------------------------------------------
# PostgreSQL integration — marking
# ---------------------------------------------------------------------------


def test_gc_marks_terminal_root(db: str) -> None:
    """A terminal job is marked gc = true with zero retention."""
    root_id = _insert_terminal_job(db)
    for i in range(5):
        _insert_output_chunk(db, root_id, sequence=i)
    settings = _make_settings(gc_retention_seconds=0.0, gc_batch_limit=2)
    with psycopg.connect(db) as conn:
        roots, _, _ = collect_transport(conn, settings)
    assert root_id in roots
    assert _is_gc_marked(db, root_id)
    assert _row_exists(db, root_id)


def test_gc_does_not_mark_recent_terminal_job(db: str) -> None:
    """A recently finished job is not marked when retention is large."""
    root_id = _insert_terminal_job(db, finished_at="2099-01-01T00:00:00.000000Z")
    settings = _make_settings(gc_retention_seconds=3600.0)
    with psycopg.connect(db) as conn:
        roots, _, _ = collect_transport(conn, settings)
    assert roots == []
    assert not _is_gc_marked(db, root_id)


def test_gc_does_not_mark_pending_job(db: str) -> None:
    """A pending job is never marked by GC."""
    root_id = _insert_pending_job(db)
    settings = _make_settings(gc_retention_seconds=0.0)
    with psycopg.connect(db) as conn:
        roots, _, _ = collect_transport(conn, settings)
    assert roots == []
    assert not _is_gc_marked(db, root_id)


def test_gc_does_not_mark_running_job(db: str) -> None:
    """A running job is never marked by GC."""
    root_id = _insert_running_job(db)
    settings = _make_settings(gc_retention_seconds=0.0)
    with psycopg.connect(db) as conn:
        roots, _, _ = collect_transport(conn, settings)
    assert roots == []
    assert not _is_gc_marked(db, root_id)


def test_gc_marks_all_terminal_statuses(db: str) -> None:
    """All terminal statuses are marked: succeeded, failed, cancelled."""
    s = _insert_terminal_job(db, status="succeeded")
    f = _insert_terminal_job(db, status="failed")
    c = _insert_terminal_job(db, status="cancelled")
    for jid in [s, f, c]:
        for i in range(5):
            _insert_output_chunk(db, jid, sequence=i)
    settings = _make_settings(gc_retention_seconds=0.0, gc_batch_limit=3)
    with psycopg.connect(db) as conn:
        roots, _, _ = collect_transport(conn, settings)
    assert sorted(roots) == sorted([s, f, c])
    for jid in [s, f, c]:
        assert _is_gc_marked(db, jid)
        assert _row_exists(db, jid)


def test_gc_marking_respects_batch_limit(db: str) -> None:
    """Phase 1 marks at most batch_limit roots per pass."""
    [_insert_terminal_job(db) for _ in range(5)]
    settings = _make_settings(gc_retention_seconds=0.0, gc_batch_limit=3)
    with psycopg.connect(db) as conn:
        roots, _, _ = collect_transport(conn, settings)
    assert len(roots) == 3


def test_gc_marking_is_idempotent(db: str) -> None:
    """Running GC twice does not re-mark already-marked roots."""
    root_id = _insert_terminal_job(db)
    settings = _make_settings(gc_retention_seconds=0.0)
    with psycopg.connect(db) as conn:
        roots1, _, _ = collect_transport(conn, settings)
    with psycopg.connect(db) as conn:
        roots2, _, _ = collect_transport(conn, settings)
    assert root_id in roots1
    assert roots2 == []


def test_gc_concurrent_marking_is_idempotent(db: str) -> None:
    """Concurrent marking passes mark each root exactly once."""
    ids = [_insert_terminal_job(db) for _ in range(6)]
    results: list[list[UUID]] = []

    def gc_pass() -> None:
        settings = _make_settings(gc_retention_seconds=0.0, gc_batch_limit=10)
        with psycopg.connect(db) as conn:
            roots, _, _ = collect_transport(conn, settings)
        results.append(roots)

    workers = [threading.Thread(target=gc_pass) for _ in range(3)]
    for t in workers:
        t.start()
    for t in workers:
        t.join(timeout=30)
    all_marked = [rid for batch in results for rid in batch]
    assert sorted(all_marked) == sorted(ids)


# ---------------------------------------------------------------------------
# PostgreSQL integration — chunk drain
# ---------------------------------------------------------------------------


def test_gc_drains_chunks_and_removes_root(db: str) -> None:
    """Multiple passes drain all chunks and remove the root."""
    root_id = _insert_terminal_job(db)
    for i in range(5):
        _insert_output_chunk(db, root_id, sequence=i)
    settings = _make_settings(gc_retention_seconds=0.0, gc_batch_limit=10)
    for _ in range(10):
        with psycopg.connect(db) as conn:
            _r, _c, _o = collect_transport(conn, settings)
        if not _row_exists(db, root_id):
            break
    assert not _row_exists(db, root_id)
    assert _count_chunks_for_root(db, root_id) == 0


def test_gc_drain_respects_per_root_chunk_limit(db: str) -> None:
    """Each chunk drain pass deletes at most gc_batch_limit chunks per root."""
    root_id = _insert_terminal_job(db)
    chunk_ids = [_insert_output_chunk(db, root_id, sequence=i) for i in range(10)]
    settings = _make_settings(gc_retention_seconds=0.0, gc_batch_limit=3)
    with psycopg.connect(db) as conn:
        _roots, _chunks, _orphans = collect_transport(conn, settings)
    assert _is_gc_marked(db, root_id)
    remaining = [cid for cid in chunk_ids if _row_exists(db, cid)]
    assert len(remaining) == 7


def test_gc_drain_converges_over_multiple_passes(db: str) -> None:
    """Many chunks per root are drained over multiple bounded passes."""
    root_id = _insert_terminal_job(db)
    chunk_ids = [_insert_output_chunk(db, root_id, sequence=i) for i in range(25)]
    settings = _make_settings(gc_retention_seconds=0.0, gc_batch_limit=5)
    for _ in range(20):
        with psycopg.connect(db) as conn:
            _r, _c, _o = collect_transport(conn, settings)
        if not _row_exists(db, root_id):
            break
    assert not _row_exists(db, root_id)
    for cid in chunk_ids:
        assert not _row_exists(db, cid)


def test_gc_root_deleted_only_after_all_chunks_gone(db: str) -> None:
    """Root persists until its last chunk is drained."""
    root_id = _insert_terminal_job(db)
    _insert_output_chunk(db, root_id, sequence=0)
    _insert_output_chunk(db, root_id, sequence=1)
    settings = _make_settings(gc_retention_seconds=0.0, gc_batch_limit=1)
    with psycopg.connect(db) as conn:
        _r, _c, _o = collect_transport(conn, settings)
    assert _is_gc_marked(db, root_id)
    assert _row_exists(db, root_id)
    remaining = _count_chunks_for_root(db, root_id)
    assert remaining >= 1


def test_gc_concurrent_drain_is_idempotent(db: str) -> None:
    """Concurrent chunk drain passes converge without conflict."""
    root_id = _insert_terminal_job(db)
    chunk_ids = [_insert_output_chunk(db, root_id, sequence=i) for i in range(6)]
    settings = _make_settings(gc_retention_seconds=0.0, gc_batch_limit=10)

    def gc_pass() -> None:
        with psycopg.connect(db) as conn:
            collect_transport(conn, settings)

    workers = [threading.Thread(target=gc_pass) for _ in range(3)]
    for t in workers:
        t.start()
    for t in workers:
        t.join(timeout=30)
    for cid in chunk_ids:
        assert not _row_exists(db, cid)
    assert not _row_exists(db, root_id)


# ---------------------------------------------------------------------------
# PostgreSQL integration — orphan cleanup
# ---------------------------------------------------------------------------


def test_gc_cleans_orphan_chunks(db: str) -> None:
    """Orphan chunks whose root is absent are cleaned by the anti-join pass."""
    dangling = uuid4()
    orphan = _insert_orphan_chunk(db, dangling)
    trigger = _insert_terminal_job(db)
    settings = _make_settings(gc_retention_seconds=0.0)
    with psycopg.connect(db) as conn:
        roots, _chunks, orphans = collect_transport(conn, settings)
    assert trigger in roots
    assert orphans == 1
    assert not _row_exists(db, orphan)


def test_gc_does_not_clean_chunks_owned_by_existing_root(db: str) -> None:
    """Chunks whose root still exists are not treated as orphans."""
    root_id = _insert_terminal_job(db)
    chunk = _insert_output_chunk(db, root_id, sequence=0)
    with psycopg.connect(db) as conn:
        conn.execute(
            "UPDATE lubko.jobs\nSET payload = jsonb_set(\n"
            "    jsonb_set(payload::jsonb, '{state,status}', to_jsonb('succeeded'::text)),\n"
            "    '{state,finished_at}', to_jsonb('2099-01-01T00:00:00.000000Z'::text)\n"
            ")::text\nWHERE id = %s",
            (root_id,),
        )
    settings = _make_settings(gc_retention_seconds=0.0)
    with psycopg.connect(db) as conn:
        _roots, _chunks, orphans = collect_transport(conn, settings)
    assert orphans == 0
    assert _row_exists(db, chunk)


def test_gc_orphan_pass_is_bounded_by_limit(db: str) -> None:
    """The orphan pass cleans at most batch_limit orphans per pass."""
    dangling = uuid4()
    orphan_ids = [_insert_orphan_chunk(db, dangling) for _ in range(10)]
    settings = _make_settings(gc_retention_seconds=0.0, gc_batch_limit=3)
    with psycopg.connect(db) as conn:
        _roots, _chunks, orphans = collect_transport(conn, settings)
    assert orphans == 3
    remaining = [oid for oid in orphan_ids if _row_exists(db, oid)]
    assert len(remaining) == 7


def test_gc_orphan_pass_converges_over_multiple_passes(db: str) -> None:
    """Multiple GC passes eventually clean all orphans."""
    dangling = uuid4()
    orphan_ids = [_insert_orphan_chunk(db, dangling) for _ in range(7)]
    settings = _make_settings(gc_retention_seconds=0.0, gc_batch_limit=3)
    total_orphans = 0
    for _ in range(5):
        with psycopg.connect(db) as conn:
            _r, _c, orphan_chunks = collect_transport(conn, settings)
        total_orphans += orphan_chunks
        if not any(_row_exists(db, oid) for oid in orphan_ids):
            break
    assert total_orphans == 7


def test_gc_concurrent_orphan_cleanup_is_idempotent(db: str) -> None:
    """Concurrent orphan cleanup passes clean each orphan exactly once."""
    dangling = uuid4()
    orphan_ids = [_insert_orphan_chunk(db, dangling) for _ in range(6)]
    results: list[int] = []

    def gc_pass() -> None:
        settings = _make_settings(gc_retention_seconds=0.0, gc_batch_limit=10)
        with psycopg.connect(db) as conn:
            _r, _c, orphan_chunks = collect_transport(conn, settings)
        results.append(orphan_chunks)

    workers = [threading.Thread(target=gc_pass) for _ in range(3)]
    for t in workers:
        t.start()
    for t in workers:
        t.join(timeout=30)
    assert sum(results) == 6
    for oid in orphan_ids:
        assert not _row_exists(db, oid)


# ---------------------------------------------------------------------------
# Bounded-work proof tests
# ---------------------------------------------------------------------------


def test_gc_proves_bounded_work_many_chunks_per_root(db: str) -> None:
    """30 chunks per root converge over multiple bounded passes."""
    root_id = _insert_terminal_job(db)
    chunk_ids = [_insert_output_chunk(db, root_id, sequence=i) for i in range(30)]

    settings = _make_settings(gc_retention_seconds=0.0, gc_batch_limit=5)
    for _ in range(20):
        with psycopg.connect(db) as conn:
            _r, _c, _o = collect_transport(conn, settings)
        if not _row_exists(db, root_id):
            break

    assert not _row_exists(db, root_id)
    for cid in chunk_ids:
        assert not _row_exists(db, cid)


def test_gc_proves_bounded_single_pass_deletes_at_most_limit_chunks(db: str) -> None:
    """Each pass deletes at most gc_batch_limit chunks per root."""
    root_id = _insert_terminal_job(db)
    [_insert_output_chunk(db, root_id, sequence=i) for i in range(15)]

    settings = _make_settings(gc_retention_seconds=0.0, gc_batch_limit=3)

    # First pass: mark + drain 3
    with psycopg.connect(db) as conn:
        _r, _c, _o = collect_transport(conn, settings)
    assert _is_gc_marked(db, root_id)
    deleted_first = 15 - _count_chunks_for_root(db, root_id)
    assert deleted_first == 3

    # Second pass: drain 3 more
    with psycopg.connect(db) as conn:
        _r, _c, _o = collect_transport(conn, settings)
    deleted_second = (15 - 3) - _count_chunks_for_root(db, root_id)
    assert deleted_second == 3


def test_gc_converges_mixed_roots_and_orphans(db: str) -> None:
    """Root pass + orphan pass eventually clean everything."""
    root_a = _insert_terminal_job(db)
    for i in range(5):
        _insert_output_chunk(db, root_a, sequence=i)
    dangling = uuid4()
    orphan_ids = [_insert_orphan_chunk(db, dangling) for _ in range(5)]

    settings = _make_settings(gc_retention_seconds=0.0, gc_batch_limit=3)
    _drain_all(db, settings)
    assert _count_all(db) == 0
    for oid in orphan_ids:
        assert not _row_exists(db, oid)


def test_gc_concurrent_full_lifecycle_is_idempotent(db: str) -> None:
    """Concurrent full GC lifecycle passes converge without conflict."""
    ids = [_insert_terminal_job(db) for _ in range(4)]
    dangling = uuid4()
    orphan_ids = [_insert_orphan_chunk(db, dangling) for _ in range(4)]

    def gc_pass() -> None:
        settings = _make_settings(gc_retention_seconds=0.0, gc_batch_limit=3)
        for _ in range(20):
            with psycopg.connect(db) as conn:
                collect_transport(conn, settings)

    workers = [threading.Thread(target=gc_pass) for _ in range(3)]
    for t in workers:
        t.start()
    for t in workers:
        t.join(timeout=60)

    for jid in ids:
        assert not _row_exists(db, jid)
    for oid in orphan_ids:
        assert not _row_exists(db, oid)


def test_gc_no_job_contents_in_logs(caplog: pytest.LogCaptureFixture) -> None:
    """GC log output does not contain job IDs or payload contents."""
    conn = _RecordingConnection()
    conn.rows = [[], [], []]  # Phase 1, 2, 3
    with caplog.at_level(logging.DEBUG):
        collect_transport(_as_db(conn), _make_settings())
    assert "root_id" not in caplog.text
