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
import subprocess
import threading
from contextlib import nullcontext
from typing import TYPE_CHECKING, Self, cast
from uuid import uuid4

import psycopg
import pytest

from lubko.health import proc_start_ticks
from lubko.worker import (
    OUTPUT_STREAMS,
    ActiveJob,
    OutputStream,
    Settings,
    collect_transport,
    publish_output,
    recover_stale_jobs,
)

if TYPE_CHECKING:
    from contextlib import AbstractContextManager
    from pathlib import Path
    from uuid import UUID

    from lubko.worker import JobsConnection


def _make_settings(
    *,
    server: str = "alpha-server",
    gc_retention_seconds: float = 0.0,
    gc_batch_limit: int = 100,
) -> Settings:
    return Settings(
        server=server,
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
    server: str = "alpha-server",
    finished_at: str = "2020-01-01T00:00:00.000000Z",
    worker_id: str = "old-worker",
) -> UUID:
    payload = json.dumps({
        "v": 4,
        "type": "command",
        "server": server,
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
        "v": 4,
        "type": "command",
        "server": "alpha-server",
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
        "v": 4,
        "type": "command",
        "server": "alpha-server",
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


def _insert_output_chunk(
    conninfo: str, root_id: UUID, *, sequence: int = 0, server: str = "alpha-server"
) -> UUID:
    payload = json.dumps({
        "v": 4,
        "type": "output_chunk",
        "server": server,
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
        "v": 4,
        "type": "output_chunk",
        "server": "alpha-server",
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


def test_collect_transport_orphan_pass_uses_cast_free_comparison() -> None:
    """Phase 3 uses cast-free, case-normalized comparison."""
    conn = _RecordingConnection()
    conn.rows = [[], [], []]  # Phase 1, 2, 3

    collect_transport(_as_db(conn), _make_settings())

    sql = conn.executions[2][0]
    assert "lower(root.id::text)" in sql  # cast-free, case-normalized
    assert "lower(chunk.payload::jsonb->>'thread')" in sql
    # No ::uuid cast on the thread value
    assert "::uuid" not in sql


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
            server="alpha-server",
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
            server="alpha-server",
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
            server="alpha-server",
            worker_id="w",
            poll_interval_seconds=1.0,
            process_poll_interval_seconds=0.1,
            cancel_grace_seconds=5.0,
            gc_batch_limit=0,
        )


def test_settings_gc_defaults() -> None:
    """GC settings default to documented values."""
    settings = Settings(
        server="alpha-server",
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
    settings = Settings.from_environment(server="env-server")
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


# ---------------------------------------------------------------------------
# Chunk counting
# ---------------------------------------------------------------------------


def test_gc_chunk_count_is_accurate(db: str) -> None:
    """collect_transport returns the actual number of owned chunks deleted."""
    root_id = _insert_terminal_job(db)
    chunk_ids = [_insert_output_chunk(db, root_id, sequence=i) for i in range(7)]

    settings = _make_settings(gc_retention_seconds=0.0, gc_batch_limit=10)
    with psycopg.connect(db) as conn:
        _roots, chunks, _orphans = collect_transport(conn, settings)

    assert chunks == 7
    for cid in chunk_ids:
        assert not _row_exists(db, cid)


def test_gc_chunk_count_partial_drain(db: str) -> None:
    """Chunk count reflects only the bounded batch actually deleted."""
    root_id = _insert_terminal_job(db)
    for i in range(10):
        _insert_output_chunk(db, root_id, sequence=i)

    settings = _make_settings(gc_retention_seconds=0.0, gc_batch_limit=3)
    with psycopg.connect(db) as conn:
        _roots, chunks, _orphans = collect_transport(conn, settings)

    assert chunks == 3


def test_gc_chunk_count_zero_when_no_chunks(db: str) -> None:
    """Chunk count is zero when roots have no owned chunks."""
    _insert_terminal_job(db)

    settings = _make_settings(gc_retention_seconds=0.0)
    with psycopg.connect(db) as conn:
        _roots, chunks, _orphans = collect_transport(conn, settings)

    assert chunks == 0


# ---------------------------------------------------------------------------
# Lease-recovery interaction
# ---------------------------------------------------------------------------


def test_gc_collects_after_lease_recovery_makes_terminal(db: str) -> None:
    """An expired running job is recovered first, then GC collects it later.

    GC itself never collects live running rows. The expired lease is
    recovered by recover_stale_jobs into a terminal failed status, and only
    once the finished_at timestamp is old enough does GC mark and drain it.
    """
    # Insert a running job with an expired lease.
    root_id = _insert_running_job(db)
    # Force the lease into the past.
    with psycopg.connect(db) as conn:
        conn.execute(
            "UPDATE lubko.jobs\n"
            "SET payload = jsonb_set(\n"
            "    payload::jsonb,\n"
            "    '{state,lease_expires_at}',\n"
            "    to_jsonb('2020-01-01T00:00:00.000000Z'::text)\n"
            ")::text\n"
            "WHERE id = %s",
            (root_id,),
        )

    # GC must NOT collect it (still running, even though lease expired).
    settings = _make_settings(gc_retention_seconds=0.0)
    with psycopg.connect(db) as conn:
        roots, _c, _o = collect_transport(conn, settings)
    assert root_id not in roots
    assert _row_exists(db, root_id)

    # Lease recovery marks it failed.
    with psycopg.connect(db) as conn:
        recovered = recover_stale_jobs(conn, _make_settings().server)
    assert len(recovered) == 1
    assert recovered[0][0] == root_id
    assert _read_status(db, root_id) == "failed"

    # GC with large retention must NOT collect it (finished_at is recent).
    settings_big = _make_settings(gc_retention_seconds=86400.0)
    with psycopg.connect(db) as conn:
        roots, _c, _o = collect_transport(conn, settings_big)
    assert root_id not in roots

    # Force finished_at into the deep past so the retention window covers it.
    with psycopg.connect(db) as conn:
        conn.execute(
            "UPDATE lubko.jobs\n"
            "SET payload = jsonb_set(\n"
            "    payload::jsonb,\n"
            "    '{state,finished_at}',\n"
            "    to_jsonb('2020-01-01T00:00:00.000000Z'::text)\n"
            ")::text\n"
            "WHERE id = %s",
            (root_id,),
        )

    # Add chunks so Phase 2 does not delete the root immediately.
    for i in range(5):
        _insert_output_chunk(db, root_id, sequence=i)

    # Now GC marks it and root survives Phase 2 (chunks > batch_limit).
    small_batch = _make_settings(gc_retention_seconds=0.0, gc_batch_limit=2)
    with psycopg.connect(db) as conn:
        roots, _c, _o = collect_transport(conn, small_batch)
    assert root_id in roots
    assert _is_gc_marked(db, root_id)
    assert _row_exists(db, root_id)


def _read_status(conninfo: str, job_id: UUID) -> str:
    """Read the current status of a job.

    Args:
        conninfo: PostgreSQL connection string.
        job_id: Job identifier.

    Returns:
        The current job status.
    """
    with psycopg.connect(conninfo) as conn:
        row = conn.execute(
            "SELECT (payload::jsonb)->'state'->>'status'\nFROM lubko.jobs WHERE id = %s",
            (job_id,),
        ).fetchone()
    assert row is not None
    return str(row[0])


# ---------------------------------------------------------------------------
# Phase-1 eligibility: unknown/future status is retained
# ---------------------------------------------------------------------------


def test_gc_does_not_mark_unknown_status(db: str) -> None:
    """A command with an unknown/future status is never marked by GC.

    Phase-1 explicitly selects status IN succeeded/failed/cancelled.
    Any unknown or future status value is retained, not collected.
    """
    payload = json.dumps({
        "v": 4,
        "type": "command",
        "server": "alpha-server",
        "request": {"cwd": "/workspace", "process": ["echo", "hi"]},
        "state": {
            "status": "mystery",
            "finished_at": "2020-01-01T00:00:00.000000Z",
        },
    })
    with psycopg.connect(db) as conn:
        row = conn.execute(
            "INSERT INTO lubko.jobs (payload) VALUES (%s) RETURNING id", (payload,)
        ).fetchone()
    assert row is not None
    unknown_id = cast("UUID", row[0])

    settings = _make_settings(gc_retention_seconds=0.0)
    with psycopg.connect(db) as conn:
        roots, _c, _o = collect_transport(conn, settings)
    assert unknown_id not in roots
    assert _row_exists(db, unknown_id)
    assert not _is_gc_marked(db, unknown_id)


def test_gc_does_not_mark_pending_status(db: str) -> None:
    """A pending command is never marked by GC."""
    root_id = _insert_pending_job(db)
    settings = _make_settings(gc_retention_seconds=0.0)
    with psycopg.connect(db) as conn:
        roots, _c, _o = collect_transport(conn, settings)
    assert root_id not in roots


def test_gc_does_not_mark_running_status(db: str) -> None:
    """A running command is never marked by GC."""
    root_id = _insert_running_job(db)
    settings = _make_settings(gc_retention_seconds=0.0)
    with psycopg.connect(db) as conn:
        roots, _c, _o = collect_transport(conn, settings)
    assert root_id not in roots


# ---------------------------------------------------------------------------
# Orphan matching: malformed/non-UUID thread text
# ---------------------------------------------------------------------------


def test_gc_orphan_pass_cleans_malformed_thread(db: str) -> None:
    """Malformed/empty/non-UUID thread chunks are orphans and get cleaned.

    The cast-free, case-normalized comparison (lower(root.id::text) =
    lower(thread)) means non-UUID thread text simply never matches any
    root, so the chunk is correctly treated as an orphan and deleted.
    No cast error is ever raised regardless of planner predicate order.
    """
    # Insert chunks with various malformed thread values.
    cases = [
        ("not-a-uuid-at-all", "malformed"),
        ("", "empty"),
        ("ABCDEF01-2345-4321-ABCD-EF0123456789", "uppercase"),
        ("12345", "short"),
        ("zzzzzzzz-zzzz-zzzz-zzzz-zzzzzzzzzzzz", "non-hex"),
    ]
    chunk_ids: list[UUID] = []
    for thread_val, _label in cases:
        payload = json.dumps({
            "v": 4,
            "type": "output_chunk",
            "server": "alpha-server",
            "thread": thread_val,
            "stream": "stdout",
            "sequence": 0,
            "start": 0,
            "end": 5,
            "value": "hello",
            "previous": None,
        })
        with psycopg.connect(db) as conn:
            row = conn.execute(
                "INSERT INTO lubko.jobs (payload) VALUES (%s) RETURNING id", (payload,)
            ).fetchone()
        assert row is not None
        chunk_ids.append(cast("UUID", row[0]))

    # GC orphan pass must not crash and must clean all five.
    settings = _make_settings(gc_retention_seconds=0.0, gc_batch_limit=100)
    with psycopg.connect(db) as conn:
        _roots, _chunks, orphans = collect_transport(conn, settings)

    assert orphans == 5
    for cid in chunk_ids:
        assert not _row_exists(db, cid), f"chunk {cid} should be cleaned"


def test_gc_orphan_pass_retains_valid_owned_chunks(db: str) -> None:
    """Chunks with valid UUID thread and an existing root are NOT orphans."""
    root_id = _insert_terminal_job(db, finished_at="2099-01-01T00:00:00.000000Z")
    chunk = _insert_output_chunk(db, root_id, sequence=0)

    settings = _make_settings(gc_retention_seconds=0.0, gc_batch_limit=100)
    with psycopg.connect(db) as conn:
        _roots, _chunks, orphans = collect_transport(conn, settings)

    assert orphans == 0
    assert _row_exists(db, chunk)


# ---------------------------------------------------------------------------
# Concurrent publication-vs-GC race (genuinely concurrent)
# ---------------------------------------------------------------------------


def test_gc_skips_publisher_locked_root_proving_skip_locked(db: str) -> None:
    """Publisher-first: SKIP LOCKED skips root locked by concurrent publisher.

    Proof of the two-sided invariant with real PostgreSQL:

    (A) Publisher-first ordering:
      1. Publisher holds root ``FOR UPDATE`` in an open transaction.
      2. Real ``collect_transport`` runs concurrently and returns without
         marking or deleting that locked root (``SKIP LOCKED``).
      3. Publisher commits; a later GC pass marks, drains, and deletes
         everything with no orphans.
    """
    root_id = _insert_terminal_job(db)
    for i in range(5):
        _insert_output_chunk(db, root_id, sequence=i)
    settings = _make_settings(gc_retention_seconds=0.0, gc_batch_limit=10)

    # Publisher opens a transaction and visibly locks the root row.
    pub_conn = psycopg.connect(db)
    pub_conn.autocommit = False
    with pub_conn.transaction(), pub_conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM lubko.jobs\n"
            "WHERE id = %s AND (payload::jsonb)->'state'->>'gc' IS DISTINCT FROM 'true'\n"
            "FOR UPDATE",
            (root_id,),
        )

        # Start GC thread while publisher lock is visibly held.
        gc_results: list[tuple[list[UUID], int, int]] = []

        def gc_fn() -> None:
            with psycopg.connect(db) as c:
                gc_results.append(collect_transport(c, settings))

        gc_thread = threading.Thread(target=gc_fn)
        gc_thread.start()

        # Bounded join: GC must terminate while publisher lock is still held.
        gc_thread.join(timeout=30)
        assert not gc_thread.is_alive(), "GC thread did not terminate"
        assert len(gc_results) == 1, "GC thread did not return a result"
        roots_marked, _chunks, _orphans = gc_results[0]

        # SKIP LOCKED: root absent from Phase-1 output and unmodified.
        assert root_id not in roots_marked
        with psycopg.connect(db) as chk:
            row = chk.execute(
                "SELECT (payload::jsonb)->'state'->>'gc'\nFROM lubko.jobs WHERE id = %s",
                (root_id,),
            ).fetchone()
        assert row is not None
        assert row[0] != "true"
        assert _row_exists(db, root_id)

        # Release publisher lock.
    pub_conn.close()

    # Later GC pass: marks, drains all chunks, deletes root, no orphans.
    with psycopg.connect(db) as conn:
        roots, chunks, orphans = collect_transport(conn, settings)
    assert root_id in roots
    assert chunks == 5
    assert orphans == 0
    assert not _row_exists(db, root_id)
    assert _count_chunks_for_root(db, root_id) == 0


def test_gc_mark_prevents_publication_no_orphan_chunks(db: str, tmp_path: Path) -> None:
    """GC-wins: after gc mark commits, publish_output refuses the root.

    Proof of the two-sided invariant with real PostgreSQL:

    (B) GC-wins ordering:
      1. Real ``collect_transport`` marks the root gc=true (Phase 1) and
         partially drains chunks (Phase 2) in one bounded pass.
      2. ``publish_output`` on the same root refuses (gc flag visible).
      3. No new chunk is created; root survives (>batch chunks).
      4. Subsequent GC passes drain remaining chunks and delete root.
    """
    root_id = _insert_terminal_job(db)
    # More chunks than gc_batch_limit so root survives Phase 2.
    for i in range(5):
        _insert_output_chunk(db, root_id, sequence=i)
    settings = _make_settings(gc_retention_seconds=0.0, gc_batch_limit=3)

    # GC marks the root (Phase 1) and partially drains (Phase 2).
    with psycopg.connect(db) as conn:
        roots, _chunks, _orphans = collect_transport(conn, settings)
    assert root_id in roots
    assert _is_gc_marked(db, root_id)
    assert _row_exists(db, root_id)
    # Phase 2 deleted gc_batch_limit=3 chunks, 2 remain, root alive.
    assert _count_chunks_for_root(db, root_id) == 2

    # Real subprocess writes capture output into pytest-owned tmp_path.
    stdout_path = tmp_path / "stdout"
    stderr_path = tmp_path / "stderr"
    proc = subprocess.Popen(
        ["/bin/echo", "hello"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        stdout_data, stderr_data = proc.communicate(timeout=10)
        stdout_path.write_bytes(stdout_data)
        stderr_path.write_bytes(stderr_data)

        job = ActiveJob(
            id=root_id,
            cwd=str(tmp_path),
            process=("/bin/echo", "hello"),
            proc=proc,
            pid=proc.pid,
            pgid=proc.pid,
            start_ticks=proc_start_ticks(proc.pid) or 0,
            started_mono=0.0,
            claimed_at=0.0,
        )
        job.stdout = OutputStream(path=stdout_path)
        job.stderr = OutputStream(path=stderr_path)

        # publish_output refuses the gc-marked root.
        with psycopg.connect(db) as conn:
            published = publish_output(
                conn, job, list(OUTPUT_STREAMS), 0.0, server="alpha-server", force=True
            )
        assert published is False
        # No new chunk created by the refused publication.
        assert _count_chunks_for_root(db, root_id) == 2
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5)
        stdout_path.unlink(missing_ok=True)
        stderr_path.unlink(missing_ok=True)

    # Subsequent GC passes drain remaining chunks and delete root.
    for _ in range(10):
        with psycopg.connect(db) as conn:
            collect_transport(conn, settings)
        if not _row_exists(db, root_id):
            break
    assert not _row_exists(db, root_id)
    assert _count_chunks_for_root(db, root_id) == 0


# ---------------------------------------------------------------------------
# Uppercase canonical UUID thread: must be recognised and retained
# ---------------------------------------------------------------------------


def test_gc_orphan_retains_uppercase_thread_with_existing_root(db: str) -> None:
    """A chunk whose thread is an uppercase UUID and whose root exists is retained.

    The cast-free, case-normalized comparison (lower(root.id::text) =
    lower(thread)) recognises uppercase canonical UUIDs, so the chunk
    matches its owning root and is not classified as an orphan.
    """
    root_id = _insert_terminal_job(db, finished_at="2099-01-01T00:00:00.000000Z")
    payload = json.dumps({
        "v": 4,
        "type": "output_chunk",
        "server": "alpha-server",
        "thread": str(root_id).upper(),
        "stream": "stdout",
        "sequence": 0,
        "start": 0,
        "end": 5,
        "value": "hello",
        "previous": None,
    })
    with psycopg.connect(db) as conn:
        row = conn.execute(
            "INSERT INTO lubko.jobs (payload) VALUES (%s) RETURNING id", (payload,)
        ).fetchone()
    assert row is not None
    chunk_id = cast("UUID", row[0])

    settings = _make_settings(gc_retention_seconds=0.0, gc_batch_limit=100)
    with psycopg.connect(db) as conn:
        _roots, _chunks, orphans = collect_transport(conn, settings)

    assert orphans == 0
    assert _row_exists(db, chunk_id)


def test_gc_drains_uppercase_thread_chunks_from_marked_root(db: str) -> None:
    """Phase-2 chunk drain matches uppercase-UUID thread chunks via lower().

    A GC-marked root with uppercase-thread chunks is drained correctly
    because Phase-2 uses lower() normalization for the ownership comparison.
    """
    root_id = _insert_terminal_job(db)
    # Insert chunks with uppercase thread.
    for i in range(5):
        payload = json.dumps({
            "v": 4,
            "type": "output_chunk",
            "server": "alpha-server",
            "thread": str(root_id).upper(),
            "stream": "stdout",
            "sequence": i,
            "start": 0,
            "end": 5,
            "value": f"chunk{i}",
            "previous": None,
        })
        with psycopg.connect(db) as conn:
            conn.execute("INSERT INTO lubko.jobs (payload) VALUES (%s)", (payload,))

    settings = _make_settings(gc_retention_seconds=0.0, gc_batch_limit=10)
    with psycopg.connect(db) as conn:
        _roots, chunks, _orphans = collect_transport(conn, settings)

    assert chunks == 5
    assert not _row_exists(db, root_id)


def test_gc_orphan_deletes_malformed_with_no_owner(db: str) -> None:
    """Malformed/empty thread chunks are deleted even when other roots exist.

    A chunk with thread 'not-a-uuid' has no possible owner.  The cast-free
    comparison lower(root.id::text) = lower('not-a-uuid') never matches any
    root, so NOT EXISTS is true and the chunk is correctly cleaned.
    Other valid owned chunks in the table are not affected.
    """
    # A valid root with a valid chunk — must be retained.
    valid_root = _insert_terminal_job(db, finished_at="2099-01-01T00:00:00.000000Z")
    valid_chunk = _insert_output_chunk(db, valid_root, sequence=0)

    # A malformed chunk — must be deleted.
    bad_payload = json.dumps({
        "v": 4,
        "type": "output_chunk",
        "server": "alpha-server",
        "thread": "not-a-uuid",
        "stream": "stdout",
        "sequence": 0,
        "start": 0,
        "end": 5,
        "value": "garbage",
        "previous": None,
    })
    with psycopg.connect(db) as conn:
        row = conn.execute(
            "INSERT INTO lubko.jobs (payload) VALUES (%s) RETURNING id", (bad_payload,)
        ).fetchone()
    assert row is not None
    bad_id = cast("UUID", row[0])

    settings = _make_settings(gc_retention_seconds=0.0, gc_batch_limit=100)
    with psycopg.connect(db) as conn:
        _roots, _chunks, orphans = collect_transport(conn, settings)

    assert orphans == 1
    assert not _row_exists(db, bad_id)
    assert _row_exists(db, valid_chunk)


def test_gc_is_isolated_per_server(db: str) -> None:
    """A GC pass collects only terminal roots and chunks of its own server.

    A beta-addressed terminal root with owned chunks is invisible to an alpha
    daemon's collection pass (mark, drain, and orphan phases all skip it); the
    same pass run for the beta identity drains exactly that transport.
    """
    beta_id = _insert_terminal_job(db, server="beta-server")
    _insert_output_chunk(db, beta_id, sequence=0, server="beta-server")
    _insert_output_chunk(db, beta_id, sequence=1, server="beta-server")

    with psycopg.connect(db) as conn:
        assert collect_transport(conn, _make_settings()) == ([], 0, 0)
    assert _row_exists(db, beta_id)
    assert _count_chunks_for_root(db, beta_id) == 2

    with psycopg.connect(db) as conn:
        roots, chunks, orphans = collect_transport(conn, _make_settings(server="beta-server"))
    assert roots == [beta_id]
    assert chunks == 2
    assert orphans == 0
    assert not _row_exists(db, beta_id)
    assert _count_chunks_for_root(db, beta_id) == 0


def test_gc_orphan_pass_is_isolated_per_server(db: str) -> None:
    """Orphan chunks of another server are never collected by this daemon."""
    dangling_beta = uuid4()
    _insert_orphan_chunk_for_server(db, dangling_beta, "beta-server")

    with psycopg.connect(db) as conn:
        _roots, _chunks, orphans = collect_transport(conn, _make_settings())
    assert orphans == 0
    assert _row_exists(db, _chunk_id(db, dangling_beta))

    with psycopg.connect(db) as conn:
        _roots, _chunks, orphans = collect_transport(conn, _make_settings(server="beta-server"))
    assert orphans == 1


def _insert_orphan_chunk_for_server(conninfo: str, dangling_root_id: UUID, server: str) -> UUID:
    payload = json.dumps({
        "v": 4,
        "type": "output_chunk",
        "server": server,
        "thread": str(dangling_root_id),
        "stream": "stdout",
        "sequence": 0,
        "start": 0,
        "end": 10,
        "value": "orphan",
        "previous": None,
    })
    with psycopg.connect(conninfo) as conn:
        row = conn.execute(
            "INSERT INTO lubko.jobs (payload) VALUES (%s) RETURNING id", (payload,)
        ).fetchone()
    assert row is not None
    return cast("UUID", row[0])


def _chunk_id(conninfo: str, thread: UUID) -> UUID:
    with psycopg.connect(conninfo) as conn:
        row = conn.execute(
            "SELECT id FROM lubko.jobs WHERE (payload::jsonb)->>'thread' = %s",
            (str(thread),),
        ).fetchone()
    assert row is not None
    return cast("UUID", row[0])
