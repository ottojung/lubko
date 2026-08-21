"""Integration tests for the nonblocking Lubko supervisor against real PostgreSQL.

These tests run the actual :class:`lubko.worker.Supervisor` in a thread against
an isolated PostgreSQL cluster and prove the concurrency, output, lease,
outage, shutdown, and multi-worker safety acceptance criteria with real process
groups and real row-locking.
"""

from __future__ import annotations

import itertools
import json
import logging
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
from contextlib import contextmanager, suppress
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final, cast
from unittest.mock import PropertyMock, patch
from uuid import UUID, uuid4

import psycopg
import pytest

from lubko import health
from lubko import worker as worker_module
from lubko.config import DatabaseConfig
from lubko.protocol import OUTPUT_CHUNK_MAX_BYTES
from lubko.worker import (
    CHUNK_ORDER_INDEX_NAME,
    CHUNK_OWNER_INDEX_NAME,
    OUTPUT_STREAM_STDOUT,
    STOP_REASON_LEASE,
    TYPE_AWARE_CONSTRAINT_NAME,
    ActiveJob,
    OutputStream,
    SchemaInvariantError,
    Settings,
    Supervisor,
    collect_transport,
    delete_job_and_chunks,
    drain_sentinel_path,
    group_has_members,
    pg_safe_decode,
    publish_output,
    request_cancel,
    spawn_job,
    verify_jobs_table_invariant,
    verify_protocol_schema,
)
from tests import _process_guard as guard

if TYPE_CHECKING:
    from collections.abc import Iterator

    from lubko.worker import Job, JobResult, JobsConnection
    from tests import _pg

OUTPUT_TAIL_MAX_BYTES: Final = 4000
CHUNK_MAX_BYTES: Final = 2000

REPO_ROOT: Final = Path(__file__).resolve().parent.parent
BASELINE_MIGRATION: Final = REPO_ROOT / "migrations" / "0001_two_column_protocol.sql"

#: A two-column table without the canonical output-chunk shape (no type-aware
#: constraint, no chunk indexes): the startup guard must refuse it.
PRE_CANONICAL_SCHEMA_DDL: Final = """
create table lubko.jobs (
    id uuid primary key default gen_random_uuid(),
    payload text not null
        constraint jobs_payload_is_json_object check (
            jsonb_typeof(payload::jsonb) = 'object'
        )
        constraint jobs_payload_has_version check ((payload::jsonb) ? 'v')
        constraint jobs_payload_has_status check (
            ((payload::jsonb)->'state'->>'status') is not null
        )
);
create index if not exists jobs_queue_idx
    on lubko.jobs (
        ((payload::jsonb)->'state'->>'status'),
        ((payload::jsonb)->'state'->>'created_at')
    );
"""


def supervisor_settings(worker_id: str = "test-supervisor") -> Settings:
    """Build fast-timing supervisor settings for integration tests.

    Args:
        worker_id: Worker identifier to record.

    Returns:
        Settings with fast lease and publication timing.
    """
    return Settings(
        worker_id=worker_id,
        poll_interval_seconds=0.05,
        process_poll_interval_seconds=0.02,
        cancel_grace_seconds=0.3,
        lease_duration_seconds=1.5,
        lease_refresh_interval_seconds=0.15,
        lease_recovery_interval_seconds=0.2,
        output_publication_interval_seconds=0.1,
        claim_batch_limit=16,
        lease_safety_margin_seconds=0.3,
        db_operation_timeout_seconds=3.0,
    )


def make_database_config(cluster: _pg.PgCluster) -> DatabaseConfig:
    """Build a database config pointing at the isolated cluster.

    Args:
        cluster: The running cluster.

    Returns:
        A database configuration usable by the supervisor.
    """
    return DatabaseConfig(
        host=str(cluster.socket_dir),
        port=cluster.port,
        dbname="postgres",
        user="postgres",
        password="",
    )


@contextmanager
def supervisor_running(
    settings: Settings, database: DatabaseConfig, db: str
) -> Iterator[Supervisor]:
    """Run a supervisor in a thread and guarantee deterministic teardown.

    Args:
        settings: Supervisor settings.
        database: Database configuration.
        db: Connection string for leftover cleanup.

    Yields:
        The running supervisor.

    Raises:
        AssertionError: If the supervisor thread does not stop within the
            timeout.
    """
    supervisor = Supervisor(settings, database)
    thread = threading.Thread(target=supervisor.run, name="supervisor", daemon=True)
    thread.start()
    try:
        yield supervisor
    finally:
        supervisor.request_shutdown()
        thread.join(timeout=30)
        if thread.is_alive():
            msg = "supervisor thread did not stop within the timeout"
            raise AssertionError(msg)
        _kill_leftover_groups(db)


def _kill_leftover_groups(db: str) -> None:
    """Force-kill any process group still recorded as running after shutdown.

    Args:
        db: PostgreSQL connection string.
    """
    with psycopg.connect(db) as conn:
        rows = conn.execute(
            "SELECT (payload::jsonb)->'state'->>'process_pgid' AS pgid\n"
            "FROM lubko.jobs\n"
            "WHERE (payload::jsonb)->>'type' = 'command'\n"
            "    AND (payload::jsonb)->'state'->>'status' = 'running'\n"
        ).fetchall()
    for row in rows:
        pgid = row[0]
        if pgid is not None:
            with suppress(ProcessLookupError):
                os.killpg(int(pgid), signal.SIGKILL)


def shell_command_argv(command: str) -> list[str]:
    """Wrap a shell snippet as an explicit process argv that execs ``sh``.

    The v3 protocol executes ``request.process`` directly and never runs a
    shell implicitly. Tests that need shell semantics therefore select the
    shell interpreter themselves, exactly as a v3 orchestrator would.

    Args:
        command: Shell snippet to run through ``sh -c``.

    Returns:
        An argv array that execs the snippet through ``/bin/sh``.
    """
    return [shutil.which("sh") or "/bin/sh", "-c", command]


def insert_job(conninfo: str, cwd: str, command: str) -> UUID:
    """Insert a protocol v3 pending command job running a shell snippet.

    Args:
        conninfo: PostgreSQL connection string.
        cwd: Working directory for the job.
        command: Shell snippet, executed by an explicit ``/bin/sh -c`` argv.

    Returns:
        The job identifier.
    """
    return insert_process_job(conninfo, cwd, shell_command_argv(command))


def insert_process_job(conninfo: str, cwd: str, process: list[str]) -> UUID:
    """Insert a protocol v3 pending command job executing argv directly.

    Args:
        conninfo: PostgreSQL connection string.
        cwd: Working directory for the job.
        process: Non-empty argv array to execute directly.

    Returns:
        The job identifier.
    """
    payload = json.dumps({
        "v": 3,
        "type": "command",
        "request": {"cwd": cwd, "process": process},
        "state": {"status": "pending"},
    })
    with psycopg.connect(conninfo) as conn:
        row = conn.execute(
            "INSERT INTO lubko.jobs (payload) VALUES (%s) RETURNING id",
            (payload,),
        ).fetchone()
    assert row is not None
    return cast("UUID", row[0])


def read_root(conninfo: str, job_id: UUID) -> dict[str, Any]:
    """Read and decode a root job's payload.

    Args:
        conninfo: PostgreSQL connection string.
        job_id: Job to read.

    Returns:
        The decoded payload mapping.
    """
    with psycopg.connect(conninfo) as conn:
        row = conn.execute(
            "SELECT payload FROM lubko.jobs WHERE id = %s",
            (job_id,),
        ).fetchone()
    assert row is not None
    data = json.loads(row[0])
    assert isinstance(data, dict)
    return data


def read_status(conninfo: str, job_id: UUID) -> str:
    """Return a job's current status.

    Args:
        conninfo: PostgreSQL connection string.
        job_id: Job to inspect.

    Returns:
        The job status.
    """
    return cast("str", read_root(conninfo, job_id)["state"]["status"])


def read_chunks(conninfo: str, thread_id: UUID) -> list[tuple[UUID, dict[str, Any]]]:
    """Read the immutable chunks of a root job with structured ordering.

    Args:
        conninfo: PostgreSQL connection string.
        thread_id: Owning root job.

    Returns:
        The ``(id, payload)`` chunk pairs ordered newest first.
    """
    with psycopg.connect(conninfo) as conn:
        rows = conn.execute(
            "SELECT id, payload FROM lubko.jobs\n"
            "WHERE (payload::jsonb)->>'type' = 'output_chunk'\n"
            "    AND (payload::jsonb)->>'thread' = %s\n"
            "ORDER BY ((payload::jsonb)->'sequence')::bigint DESC",
            (str(thread_id),),
        ).fetchall()
    result: list[tuple[UUID, dict[str, Any]]] = []
    for row in rows:
        data = json.loads(row[1])
        assert isinstance(data, dict)
        result.append((cast("UUID", row[0]), data))
    return result


def count_rows(conninfo: str, thread_id: UUID) -> int:
    """Count all rows (root plus chunks) owned by a thread.

    Args:
        conninfo: PostgreSQL connection string.
        thread_id: Owning root job.

    Returns:
        The number of rows.
    """
    with psycopg.connect(conninfo) as conn:
        row = conn.execute(
            "SELECT count(*)::int FROM lubko.jobs\n"
            "WHERE id = %s\n"
            "    OR ((payload::jsonb)->>'type' = 'output_chunk' AND "
            "(payload::jsonb)->>'thread' = %s)",
            (thread_id, str(thread_id)),
        ).fetchone()
    assert row is not None
    return cast("int", row[0])


def wait_until(predicate: object, timeout: float = 30.0) -> None:
    """Poll until a predicate holds, raising if the deadline expires.

    Args:
        predicate: Callable condition to satisfy.
        timeout: Maximum seconds to wait.

    Raises:
        AssertionError: If the condition is not met within the timeout.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():  # type: ignore[operator]
            return
        time.sleep(0.05)
    msg = "condition not met within timeout"
    raise AssertionError(msg)


def test_v3_worker_refuses_non_canonical_schema(jobs_db: str, pg_cluster: _pg.PgCluster) -> None:
    """A two-column table lacking the canonical output-chunk shape is refused."""
    with psycopg.connect(jobs_db) as conn:
        conn.execute("DROP TABLE IF EXISTS lubko.jobs CASCADE")
        conn.execute(PRE_CANONICAL_SCHEMA_DDL)
    with psycopg.connect(jobs_db) as conn:
        verify_jobs_table_invariant(conn)
        with pytest.raises(SchemaInvariantError, match=r"0001_two_column_protocol\.sql"):
            verify_protocol_schema(conn)

    with pytest.raises(SchemaInvariantError):
        Supervisor(supervisor_settings(), make_database_config(pg_cluster)).run()


def test_v3_worker_accepts_fresh_baseline_schema(jobs_db: str) -> None:
    """A fresh install applying the canonical baseline alone is fully usable."""
    with psycopg.connect(jobs_db) as conn:
        conn.execute("DROP TABLE IF EXISTS lubko.jobs CASCADE")
        conn.execute(BASELINE_MIGRATION.read_text(encoding="utf-8"))
    with psycopg.connect(jobs_db) as conn:
        verify_jobs_table_invariant(conn)
        verify_protocol_schema(conn)
        row = conn.execute(
            "SELECT conname FROM pg_constraint "
            "WHERE conrelid = 'lubko.jobs'::regclass AND contype = 'c'"
        ).fetchall()
        names = {item[0] for item in row}
        assert TYPE_AWARE_CONSTRAINT_NAME in names
        indexes = {
            item[0]
            for item in conn.execute(
                "SELECT indexname FROM pg_indexes WHERE schemaname = 'lubko' AND tablename = 'jobs'"
            ).fetchall()
        }
        assert CHUNK_OWNER_INDEX_NAME in indexes
        assert CHUNK_ORDER_INDEX_NAME in indexes


def _worker_conninfo(cluster: _pg.PgCluster) -> str:
    """Return a connection string for the ``lubko_worker`` role.

    Args:
        cluster: The running cluster.

    Returns:
        A libpq connection string using trust authentication.
    """
    return f"host={cluster.socket_dir} port={cluster.port} dbname=postgres user=lubko_worker"


RESET_WORKER_ROLE_SQL: Final = """
do $$
begin
    if to_regrole('lubko_worker') is not null then
        execute 'drop owned by lubko_worker';
        execute 'drop role lubko_worker';
    end if;
end
$$;
"""


def test_worker_role_can_operate_on_a_fresh_install(
    jobs_db: str, pg_cluster: _pg.PgCluster, tmp_path: Path
) -> None:
    """A non-superuser ``lubko_worker`` role can run a full v3 worker.

    On a purged database the canonical baseline grants the worker role schema
    usage plus SELECT/INSERT/UPDATE on ``lubko.jobs``. This test provisions a
    real non-superuser ``lubko_worker`` role, applies the baseline while it
    exists, and proves the role can verify the schema, insert an immutable
    ``output_chunk`` row, and run a full supervisor that publishes chunks
    end to end.
    """
    with psycopg.connect(jobs_db) as conn:
        conn.execute(RESET_WORKER_ROLE_SQL)
        conn.execute("CREATE ROLE lubko_worker LOGIN")
        conn.execute("DROP TABLE IF EXISTS lubko.jobs CASCADE")
        conn.execute(BASELINE_MIGRATION.read_text(encoding="utf-8"))

    # Direct privilege checks: schema verification reads catalogs, and the
    # worker role can insert an immutable output_chunk row.
    with psycopg.connect(_worker_conninfo(pg_cluster)) as conn:
        verify_jobs_table_invariant(conn)
        verify_protocol_schema(conn)
        chunk_payload = json.dumps({
            "v": 3,
            "type": "output_chunk",
            "thread": str(uuid4()),
            "stream": "stdout",
            "sequence": 0,
            "start": 0,
            "end": 5,
            "value": "hello",
            "previous": None,
        })
        conn.execute(
            "INSERT INTO lubko.jobs (id, payload) VALUES (%s, %s)",
            (uuid4(), chunk_payload),
        )
        row = conn.execute(
            "SELECT count(*)::int FROM lubko.jobs WHERE (payload::jsonb)->>'type' = 'output_chunk'"
        ).fetchone()
    assert row is not None
    assert row[0] == 1

    # End to end: run a supervisor connected AS lubko_worker on a command that
    # produces enough output to create immutable chunks.
    command = "i=0; while [ $i -lt 8000 ]; do echo line-$i; i=$((i+1)); done"
    job_id = insert_job(jobs_db, str(tmp_path), command)
    database = DatabaseConfig(
        host=str(pg_cluster.socket_dir),
        port=pg_cluster.port,
        dbname="postgres",
        user="lubko_worker",
        password="",
    )
    with supervisor_running(supervisor_settings(), database, jobs_db):
        wait_until(lambda: read_status(jobs_db, job_id) == "succeeded")
        wait_until(lambda: _has_chunks(jobs_db, job_id))

    assert not zombie_children()
    assert read_status(jobs_db, job_id) == "succeeded"
    with psycopg.connect(_worker_conninfo(pg_cluster)) as conn:
        row = conn.execute(
            "SELECT count(*)::int FROM lubko.jobs "
            "WHERE (payload::jsonb)->>'type' = 'output_chunk' "
            "AND (payload::jsonb)->>'thread' = %s",
            (str(job_id),),
        ).fetchone()
    assert row is not None
    assert row[0] > 0
    with psycopg.connect(jobs_db) as conn:
        conn.execute(RESET_WORKER_ROLE_SQL)


def test_worker_role_can_run_gc_cleanup(
    jobs_db: str, pg_cluster: _pg.PgCluster, tmp_path: Path
) -> None:
    """The lubko_worker role can execute the GC collect_transport path.

    Automatic transport garbage collection uses DELETE to remove terminal
    roots and their owned chunks. The baseline migration must grant DELETE
    so the worker role can exercise this authority on a fresh install.
    """
    with psycopg.connect(jobs_db) as conn:
        conn.execute(RESET_WORKER_ROLE_SQL)
        conn.execute("CREATE ROLE lubko_worker LOGIN")
        conn.execute("DROP TABLE IF EXISTS lubko.jobs CASCADE")
        conn.execute(BASELINE_MIGRATION.read_text(encoding="utf-8"))

    # Insert a terminal root with chunks as the worker role.
    terminal_payload = json.dumps({
        "v": 3,
        "type": "command",
        "request": {"cwd": str(tmp_path), "process": ["echo", "done"]},
        "state": {
            "status": "succeeded",
            "finished_at": "2020-01-01T00:00:00.000000Z",
            "worker_id": "old-worker",
        },
    })
    with psycopg.connect(_worker_conninfo(pg_cluster)) as conn:
        row = conn.execute(
            "INSERT INTO lubko.jobs (payload) VALUES (%s) RETURNING id",
            (terminal_payload,),
        ).fetchone()
    assert row is not None
    terminal_id = cast("UUID", row[0])

    chunk_payload = json.dumps({
        "v": 3,
        "type": "output_chunk",
        "thread": str(terminal_id),
        "stream": "stdout",
        "sequence": 0,
        "start": 0,
        "end": 5,
        "value": "hello",
        "previous": None,
    })
    with psycopg.connect(_worker_conninfo(pg_cluster)) as conn:
        conn.execute("INSERT INTO lubko.jobs (payload) VALUES (%s)", (chunk_payload,))

    # collect_transport must succeed as the worker role (needs DELETE).
    settings = Settings(
        worker_id="test-worker",
        poll_interval_seconds=0.05,
        process_poll_interval_seconds=0.01,
        cancel_grace_seconds=0.5,
        lease_duration_seconds=1.0,
        lease_refresh_interval_seconds=0.2,
        lease_recovery_interval_seconds=0.1,
        lease_safety_margin_seconds=0.2,
        gc_retention_seconds=0.0,
        gc_batch_limit=10,
    )
    with psycopg.connect(_worker_conninfo(pg_cluster)) as conn:
        roots, _chunks, _orphans = collect_transport(conn, settings)

    assert terminal_id in roots

    # The terminal root and its chunk should be gone after drain.
    for _ in range(10):
        with psycopg.connect(_worker_conninfo(pg_cluster)) as conn:
            collect_transport(conn, settings)
        with psycopg.connect(_worker_conninfo(pg_cluster)) as conn:
            row = conn.execute("SELECT 1 FROM lubko.jobs WHERE id = %s", (terminal_id,)).fetchone()
        if row is None:
            break
    with psycopg.connect(_worker_conninfo(pg_cluster)) as conn:
        remaining = conn.execute(
            "SELECT count(*)::int FROM lubko.jobs "
            "WHERE (payload::jsonb)->>'type' = 'output_chunk' "
            "AND (payload::jsonb)->>'thread' = %s",
            (str(terminal_id),),
        ).fetchone()
    assert remaining is not None
    assert remaining[0] == 0

    with psycopg.connect(jobs_db) as conn:
        conn.execute(RESET_WORKER_ROLE_SQL)


def test_background_process_group_is_reaped_after_terminal_status(
    jobs_db: str, pg_cluster: _pg.PgCluster, tmp_path: Path
) -> None:
    """A leftover exact PGID after the root exits is terminated before finalize.

    ``sleep 30 & echo done``: the root shell exits successfully while a
    background member of the same exact process group keeps running. The job
    must not be finalized (or untracked) until that exact group is gone, and
    its natural exit status must be preserved.
    """
    job_id = insert_job(jobs_db, str(tmp_path), "sleep 30 & echo done")

    with supervisor_running(supervisor_settings(), make_database_config(pg_cluster), jobs_db):
        wait_until(lambda: read_status(jobs_db, job_id) == "succeeded")

    payload = read_root(jobs_db, job_id)
    pgid = int(payload["state"]["process_pgid"])
    assert payload["state"]["status"] == "succeeded"
    assert payload["result"]["exit_code"] == 0
    assert payload["result"]["cancellation_note"] is None
    assert not group_has_members(pgid)
    assert not zombie_children()


def zombie_children() -> list[int]:
    """Return the zombie children of the current process.

    Returns:
        The zombie child PIDs.
    """
    me = os.getpid()
    zombies: list[int] = []
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            stat = (entry / "stat").read_bytes()
        except OSError:
            continue
        close = stat.rfind(b")")
        if close == -1:
            continue
        fields = stat[close + 2 :].split()
        if len(fields) < 4:
            continue
        if fields[0] in {b"Z", b"X"} and int(fields[3]) == me:
            zombies.append(int(entry.name))
    return zombies


def test_multiple_jobs_run_concurrently_with_temporal_overlap(
    jobs_db: str, pg_cluster: _pg.PgCluster, tmp_path: Path
) -> None:
    """Several jobs genuinely overlap in time while all are running."""
    markers = tmp_path / "markers"
    markers.mkdir()
    commands = [
        (
            f"echo $(date +%s%N) > {markers}/j{i}.start\n"
            f"sleep 1.5\n"
            f"echo $(date +%s%N) > {markers}/j{i}.end"
        )
        for i in range(4)
    ]
    job_ids = [insert_job(jobs_db, str(tmp_path), command) for command in commands]

    with supervisor_running(supervisor_settings(), make_database_config(pg_cluster), jobs_db):
        wait_until(lambda: all((markers / f"j{i}.start").exists() for i in range(4)))
        assert not any((markers / f"j{i}.end").exists() for i in range(4))
        for job_id in job_ids:
            assert read_status(jobs_db, job_id) == "running"
        wait_until(lambda: all(read_status(jobs_db, j) == "succeeded" for j in job_ids))

    starts = [int((markers / f"j{i}.start").read_text().strip()) for i in range(4)]
    ends = [int((markers / f"j{i}.end").read_text().strip()) for i in range(4)]
    assert max(starts) < min(ends)
    for job_id in job_ids:
        assert read_status(jobs_db, job_id) == "succeeded"


def test_short_job_finishes_while_long_job_remains_running(
    jobs_db: str, pg_cluster: _pg.PgCluster, tmp_path: Path
) -> None:
    """A short job completes while another job keeps running."""
    short_id = insert_job(jobs_db, str(tmp_path), "sleep 0.3")
    long_id = insert_job(jobs_db, str(tmp_path), "sleep 30")

    with supervisor_running(supervisor_settings(), make_database_config(pg_cluster), jobs_db):
        wait_until(lambda: read_status(jobs_db, short_id) == "succeeded")
        assert read_status(jobs_db, long_id) == "running"

    assert read_status(jobs_db, short_id) == "succeeded"
    assert read_status(jobs_db, long_id) in {"cancelled", "failed"}


def test_independent_cancellation(jobs_db: str, pg_cluster: _pg.PgCluster, tmp_path: Path) -> None:
    """Cancelling one job never affects an unrelated running job."""
    first = insert_job(jobs_db, str(tmp_path), "sleep 30")
    second = insert_job(jobs_db, str(tmp_path), "sleep 30")

    with supervisor_running(supervisor_settings(), make_database_config(pg_cluster), jobs_db):
        wait_until(lambda: read_status(jobs_db, first) == "running")
        wait_until(lambda: read_status(jobs_db, second) == "running")
        with psycopg.connect(jobs_db) as conn:
            status = request_cancel(conn, first)
        assert status == "running"
        wait_until(lambda: read_status(jobs_db, first) == "cancelled")
        assert read_status(jobs_db, second) == "running"

    assert read_status(jobs_db, first) == "cancelled"
    assert read_status(jobs_db, second) in {"cancelled", "failed"}


def test_independent_stdout_and_stderr(
    jobs_db: str, pg_cluster: _pg.PgCluster, tmp_path: Path
) -> None:
    """Each running job's bounded output tail carries only its own stream."""
    first = insert_job(
        jobs_db,
        str(tmp_path),
        "for i in $(seq 1 5000); do echo AAA-$i; done; sleep 30",
    )
    second = insert_job(
        jobs_db,
        str(tmp_path),
        "for i in $(seq 1 5000); do echo BBB-$i; done; sleep 30",
    )

    with supervisor_running(supervisor_settings(), make_database_config(pg_cluster), jobs_db):
        wait_until(lambda: read_status(jobs_db, first) == "running")
        wait_until(lambda: read_status(jobs_db, second) == "running")
        wait_until(lambda: _root_stdout_tail(jobs_db, first).count("AAA") > 10)
        wait_until(lambda: _root_stdout_tail(jobs_db, second).count("BBB") > 10)

        first_tail = _root_stdout_tail(jobs_db, first)
        second_tail = _root_stdout_tail(jobs_db, second)
        assert "BBB" not in first_tail
        assert "AAA" not in second_tail


def _root_stdout_tail(conninfo: str, job_id: UUID) -> str:
    """Return a root job's published stdout tail text.

    Args:
        conninfo: PostgreSQL connection string.
        job_id: Job to inspect.

    Returns:
        The stdout tail text.
    """
    payload = read_root(conninfo, job_id)
    output = payload.get("output") or {}
    stdout = output.get("stdout") or {}
    return cast("str", stdout.get("tail") or "")


def test_one_job_failure_isolation(jobs_db: str, pg_cluster: _pg.PgCluster, tmp_path: Path) -> None:
    """A failing job is finalized independently of a concurrently running job."""
    failing = insert_job(jobs_db, str(tmp_path), "echo boom >&2; exit 7")
    healthy = insert_job(jobs_db, str(tmp_path), "sleep 30")

    with supervisor_running(supervisor_settings(), make_database_config(pg_cluster), jobs_db):
        wait_until(lambda: read_status(jobs_db, failing) == "failed")
        assert read_status(jobs_db, healthy) == "running"
        failed_payload = read_root(jobs_db, failing)
        assert failed_payload["result"]["exit_code"] == 7
        assert "boom" in failed_payload["result"]["stderr"]

    assert read_status(jobs_db, healthy) in {"cancelled", "failed"}


def test_job_runs_in_declared_working_directory(
    jobs_db: str, pg_cluster: _pg.PgCluster, tmp_path: Path
) -> None:
    """v3 process argv (direct and via an explicit sh) executes in its declared cwd."""
    work_dir = tmp_path / "runner"
    work_dir.mkdir()
    sh_id = insert_job(jobs_db, str(work_dir), "pwd")
    process_id = insert_process_job(
        jobs_db,
        str(work_dir),
        [sys.executable, "-c", "import os; print(os.getcwd())"],
    )

    with supervisor_running(supervisor_settings(), make_database_config(pg_cluster), jobs_db):
        wait_until(lambda: read_status(jobs_db, sh_id) == "succeeded")
        wait_until(lambda: read_status(jobs_db, process_id) == "succeeded")

    for job_id in (sh_id, process_id):
        payload = read_root(jobs_db, job_id)
        assert payload["state"]["status"] == "succeeded"
        assert payload["result"]["exit_code"] == 0
        assert payload["result"]["stdout"].strip() == str(work_dir)


def test_preflight_rejects_nonexistent_working_directory(
    jobs_db: str, pg_cluster: _pg.PgCluster, tmp_path: Path
) -> None:
    """A command whose cwd does not exist fails cleanly without harming others."""
    missing = tmp_path / "does-not-exist"
    bad_id = insert_job(jobs_db, str(missing), "echo never")
    healthy = insert_job(jobs_db, str(tmp_path), "echo healthy; sleep 0.3")

    with supervisor_running(supervisor_settings(), make_database_config(pg_cluster), jobs_db):
        wait_until(lambda: read_status(jobs_db, healthy) == "succeeded")
        wait_until(lambda: read_status(jobs_db, bad_id) == "failed")

    payload = read_root(jobs_db, bad_id)
    assert payload["state"]["status"] == "failed"
    assert payload["result"]["exit_code"] == 127
    assert "unable to enter working directory" in payload["result"]["stderr"]
    assert read_status(jobs_db, healthy) == "succeeded"


def test_claim_rejects_request_without_process_without_harming_others(
    jobs_db: str, pg_cluster: _pg.PgCluster, tmp_path: Path
) -> None:
    """A v3 request missing request.process fails cleanly without harming others."""
    bad_payload = json.dumps({
        "v": 3,
        "type": "command",
        "request": {"cwd": str(tmp_path)},
        "state": {"status": "pending"},
    })
    with psycopg.connect(jobs_db) as conn:
        row = conn.execute(
            "INSERT INTO lubko.jobs (payload) VALUES (%s) RETURNING id",
            (bad_payload,),
        ).fetchone()
    assert row is not None
    bad_id = cast("UUID", row[0])
    healthy = insert_job(jobs_db, str(tmp_path), "echo healthy; sleep 0.3")

    with supervisor_running(supervisor_settings(), make_database_config(pg_cluster), jobs_db):
        wait_until(lambda: read_status(jobs_db, healthy) == "succeeded")
        wait_until(lambda: read_status(jobs_db, bad_id) == "failed")
        subsequent = insert_job(jobs_db, str(tmp_path), "echo after")
        wait_until(lambda: read_status(jobs_db, subsequent) == "succeeded")

    payload = read_root(jobs_db, bad_id)
    assert payload["state"]["status"] == "failed"
    assert payload["result"]["exit_code"] == 2
    assert "request.process" in payload["result"]["stderr"]
    assert "invalid job payload" in payload["result"]["stderr"]
    assert read_status(jobs_db, healthy) == "succeeded"


def test_process_argv_passes_shell_metacharacters_literally(
    jobs_db: str, pg_cluster: _pg.PgCluster, tmp_path: Path
) -> None:
    """The v3 worker passes shell metacharacters literally, with no shell evaluation."""
    literal = "a;b $HOME *.txt $(id)"
    job_id = insert_process_job(
        jobs_db,
        str(tmp_path),
        [sys.executable, "-c", "import sys; print(sys.argv[1])", literal],
    )

    with supervisor_running(supervisor_settings(), make_database_config(pg_cluster), jobs_db):
        wait_until(lambda: read_status(jobs_db, job_id) == "succeeded")

    payload = read_root(jobs_db, job_id)
    assert payload["state"]["status"] == "succeeded"
    assert payload["result"]["exit_code"] == 0
    assert payload["result"]["stdout"].strip() == literal
    assert not payload["result"]["stderr"]


def test_legacy_v2_payloads_are_rejected_as_unsupported_version(
    jobs_db: str, pg_cluster: _pg.PgCluster, tmp_path: Path
) -> None:
    """A v2 payload carrying request.command is rejected, not executed."""
    v2_payload = json.dumps({
        "v": 2,
        "type": "command",
        "request": {"cwd": str(tmp_path), "command": "echo v2-only"},
        "state": {"status": "pending"},
    })
    with psycopg.connect(jobs_db) as conn:
        row = conn.execute(
            "INSERT INTO lubko.jobs (payload) VALUES (%s) RETURNING id",
            (v2_payload,),
        ).fetchone()
    assert row is not None
    job_id = cast("UUID", row[0])
    healthy = insert_job(jobs_db, str(tmp_path), "echo healthy")

    with supervisor_running(supervisor_settings(), make_database_config(pg_cluster), jobs_db):
        wait_until(lambda: read_status(jobs_db, healthy) == "succeeded")
        wait_until(lambda: read_status(jobs_db, job_id) == "failed")

    payload = read_root(jobs_db, job_id)
    assert payload["state"]["status"] == "failed"
    assert payload["result"]["exit_code"] == 2
    assert "unsupported protocol version" in payload["result"]["stderr"]
    assert "invalid job payload" in payload["result"]["stderr"]
    assert read_status(jobs_db, healthy) == "succeeded"


def test_lease_heartbeats_across_multiple_active_jobs(
    jobs_db: str, pg_cluster: _pg.PgCluster, tmp_path: Path
) -> None:
    """One bulk heartbeat keeps every concurrent job's lease fresh."""
    job_ids = [insert_job(jobs_db, str(tmp_path), "sleep 30") for _ in range(3)]

    with supervisor_running(supervisor_settings(), make_database_config(pg_cluster), jobs_db):
        wait_until(lambda: all(read_status(jobs_db, j) == "running" for j in job_ids))
        first = {j: read_root(jobs_db, j)["state"]["lease_expires_at"] for j in job_ids}
        wait_until(lambda: _all_leases_advance(job_ids, first, jobs_db))

    for job_id in job_ids:
        assert read_status(jobs_db, job_id) in {"cancelled", "failed"}


def _all_leases_advance(job_ids: list[UUID], first: dict[UUID, str], conninfo: str) -> bool:
    """Return whether every job's lease has advanced past its first sample.

    Args:
        job_ids: Jobs to inspect.
        first: First lease sample per job.
        conninfo: PostgreSQL connection string.

    Returns:
        ``True`` when every lease advanced.
    """
    return all(read_root(conninfo, j)["state"]["lease_expires_at"] > first[j] for j in job_ids)


def test_lease_heartbeats_two_jobs_but_not_an_orphaned_owned_row(
    jobs_db: str, pg_cluster: _pg.PgCluster, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Scoped heartbeat keeps two active jobs fresh but not an orphan owned row.

    Issue #74: a claimed job whose immediate finalization write failed (or any
    running row the worker does not locally track) must never be heartbeated
    merely because two other jobs are active.  The bulk heartbeat refreshes
    exactly the two locally-owned active jobs; the orphan's lease is left
    untouched and is allowed to expire for safe recovery.
    """
    incarnation = "issue-74-incarnation"
    monkeypatch.setenv("LUBKO_LIFECYCLE_TOKEN", incarnation)
    settings = supervisor_settings(worker_id="issue-74-worker")
    db = make_database_config(pg_cluster)

    active_ids = [insert_job(jobs_db, str(tmp_path), "sleep 30") for _ in range(2)]

    with supervisor_running(settings, db, jobs_db):
        wait_until(lambda: all(read_status(jobs_db, j) == "running" for j in active_ids))
        # An owned running row the worker never locally tracked (simulating a
        # claimed job whose immediate finalization write failed before the
        # worker recorded it).
        orphan = _insert_owned_running_row(
            jobs_db,
            settings.worker_id,
            settings.worker_incarnation,
            settings.lease_duration_seconds,
        )
        first_active = {j: read_root(jobs_db, j)["state"]["lease_expires_at"] for j in active_ids}
        orphan_first = read_root(jobs_db, orphan)["state"]["lease_expires_at"]
        wait_until(
            lambda: all(
                read_root(jobs_db, j)["state"]["lease_expires_at"] > first_active[j]
                for j in active_ids
            )
        )
        # The two active jobs keep their leases fresh (multi-job efficiency).
        for j in active_ids:
            assert read_root(jobs_db, j)["state"]["lease_expires_at"] > first_active[j]
        # The orphan is NOT heartbeated merely because the others are active.
        assert read_root(jobs_db, orphan)["state"]["lease_expires_at"] == orphan_first


def test_processless_claimed_job_terminal_write_fails_is_not_reexecuted(
    jobs_db: str, pg_cluster: _pg.PgCluster, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A processless claimed job whose immediate terminal write fails is terminalized.

    Issue #74: a job that cannot spawn (processless) and whose immediate
    finalization write fails must be handled locally — terminalized via the
    quarantine path, never re-executed, and never heartbeated merely because
    another job is active.  A genuinely active job keeps heartbeating alongside.
    """

    def _fail_finish(_conn: object, _job_id: object, _result: object) -> str:
        msg = "simulated deterministic finalization failure"
        raise psycopg.DataError(msg)

    monkeypatch.setattr(worker_module, "finish_job", _fail_finish)

    active_id = insert_job(jobs_db, str(tmp_path), "sleep 30")
    # A processless claimed job: the executable does not exist, so spawn fails
    # and the supervisor reaches the immediate-finalization path.
    processless_id = insert_process_job(jobs_db, str(tmp_path), ["/nonexistent/lubko-no-such-bin"])

    settings = _issue74_settings()
    db = make_database_config(pg_cluster)
    with supervisor_running(settings, db, jobs_db):
        wait_until(lambda: read_status(jobs_db, active_id) == "running")
        # The processless job must become terminal without ever running.
        wait_until(lambda: read_status(jobs_db, processless_id) in {"failed", "cancelled"})
        # The genuinely active job keeps its lease fresh (multi-job efficiency).
        first = read_root(jobs_db, active_id)["state"]["lease_expires_at"]
        wait_until(lambda: read_root(jobs_db, active_id)["state"]["lease_expires_at"] > first)
        assert read_status(jobs_db, active_id) == "running"
        assert read_status(jobs_db, processless_id) == "failed"


def _select_owned_running_ids(conn: JobsConnection, settings: Settings) -> list[UUID]:
    """Return every running command row currently owned by this worker.

    Args:
        conn: Open PostgreSQL connection.
        settings: Worker runtime settings.

    Returns:
        The owned running root IDs.
    """
    rows = []
    with conn.transaction():
        cursor = conn.execute(
            "SELECT id FROM lubko.jobs\n"
            "WHERE (payload::jsonb)->>'type' = 'command'\n"
            "  AND (payload::jsonb)->'state'->>'status' = 'running'\n"
            "  AND (payload::jsonb)->'state'->>'worker_id' = %(worker_id)s\n"
            "  AND (payload::jsonb)->'state'->>'worker_incarnation' = %(worker_incarnation)s\n",
            {
                "worker_id": settings.worker_id,
                "worker_incarnation": settings.worker_incarnation,
            },
        )
        rows = cursor.fetchall()
    return [cast("UUID", row[0]) for row in rows]


REAL_BULK_REFRESH = worker_module.bulk_refresh_leases
REAL_FINISH_JOB = worker_module.finish_job


def _old_unscoped_bulk_refresh(
    conn: JobsConnection, settings: Settings, _root_ids: object
) -> list[UUID]:
    """Pre-fix heartbeat: refresh every owned running row regardless of tracking.

    Used only as a mutation-control stand-in for the original unscoped
    ``bulk_refresh_leases`` so the regression tests can prove the fix would fail
    under the old behaviour.  Calls the real (saved) ``bulk_refresh_leases`` with
    the full set of owned running IDs to reproduce the pre-fix behaviour.

    Returns:
        The refreshed root IDs (all owned running rows).
    """
    owned = _select_owned_running_ids(conn, settings)
    return REAL_BULK_REFRESH(conn, settings, owned)


def _issue74_settings() -> Settings:
    """Supervisor settings whose lease outlives a brief transient outage.

    A transient connectivity/DB outage pauses heartbeats; the lease must be long
    enough that the active job keeps heartbeating once connectivity returns.

    Returns:
        Fast-timing supervisor settings with a longer lease.
    """
    return replace(
        supervisor_settings(),
        lease_duration_seconds=8.0,
        lease_safety_margin_seconds=0.5,
    )


def test_original_bug_processless_job_not_heartbeated_after_outage(
    jobs_db: str, pg_cluster: _pg.PgCluster, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Original #74 bug: a processless claimed job is not heartbeated after an outage.

    A is an active job that heartbeats. B is claimed, fails before spawn, and
    its immediate finalization write fails as a transient connectivity/DB
    outage. Connectivity is restored while A keeps heartbeating. The fix must
    prove B is NOT refreshed and is eventually stale-recovered as failed, with
    no uncertain re-execution.
    """
    # A is started first and is genuinely active/heartbeating before B exists,
    # so any claim-batch ordering cannot abort A's startup when B fails.
    active_id = insert_job(jobs_db, str(tmp_path), "sleep 30")

    def _finish_connectivity_outage(conn: JobsConnection, job_id: UUID, result: JobResult) -> str:
        # Only B's immediate finalization fails as a transient outage; every
        # other write (including A's and any shutdown finalization) is real so
        # the supervisor behaves normally around the failure.
        if job_id == processless_id:
            exc = psycopg.OperationalError("simulated transient outage")
            exc.sqlstate = "08006"
            raise exc
        return REAL_FINISH_JOB(conn, job_id, result)

    monkeypatch.setattr("lubko.worker.finish_job", _finish_connectivity_outage)

    settings = _issue74_settings()
    db = make_database_config(pg_cluster)
    with supervisor_running(settings, db, jobs_db):
        wait_until(lambda: read_status(jobs_db, active_id) == "running")
        # Now introduce B: it cannot spawn (processless) and its immediate
        # finalization hits a transient connectivity outage.  A keeps running
        # and heartbeating independently of B's failure.
        processless_id = insert_process_job(
            jobs_db, str(tmp_path), ["/nonexistent/lubko-no-such-bin"]
        )
        wait_until(lambda: read_status(jobs_db, processless_id) == "running")
        # B's immediate finalization failed on a connectivity error and B is not
        # locally tracked, so it is never re-finalized; it depends entirely on
        # the scoped heartbeat staying away so its lease can expire.
        a_first = read_root(jobs_db, active_id)["state"]["lease_expires_at"]
        wait_until(lambda: read_root(jobs_db, active_id)["state"]["lease_expires_at"] > a_first)
        # B must never be heartbeated merely because A is active.
        b_lease = read_root(jobs_db, processless_id)["state"]["lease_expires_at"]
        wait_until(
            lambda: (
                read_root(jobs_db, active_id)["state"]["lease_expires_at"] > a_first
                and read_root(jobs_db, processless_id)["state"]["lease_expires_at"] == b_lease
            )
        )
        # B is eventually stale-recovered as failed, with no re-execution.
        wait_until(lambda: read_status(jobs_db, processless_id) == "failed")
        assert read_status(jobs_db, active_id) == "running"
        # B never ran a process (it failed before spawn).
        assert "process_pgid" not in read_root(jobs_db, processless_id).get("state", {})


def test_original_bug_reproduced_under_old_unscoped_heartbeat(
    jobs_db: str, pg_cluster: _pg.PgCluster, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Mutation check: the pre-fix unscoped heartbeat keeps the orphan alive (the bug).

    With ``bulk_refresh_leases`` refreshing every owned running row, an orphaned
    owned running row (a claimed job whose immediate terminalization write
    failed and which the worker does not locally track) is heartbeated merely
    because other jobs are active, so it is never stale-recovered.  The fixed
    regression ``test_original_bug_processless_job_not_heartbeated_after_outage``
    proves the inverse: under the scoped heartbeat the same row is left
    untouched and expires into safe recovery.  This is a controlled stand-in
    for the issue's stated connectivity-outage sequence so the fix's effect is
    observable without the claim-rollback that a simulated mid-tick outage would
    trigger.
    """
    monkeypatch.setattr("lubko.worker.bulk_refresh_leases", _old_unscoped_bulk_refresh)

    settings = _issue74_settings()
    db = make_database_config(pg_cluster)

    active_ids = [insert_job(jobs_db, str(tmp_path), "sleep 30") for _ in range(2)]
    with supervisor_running(settings, db, jobs_db):
        wait_until(lambda: all(read_status(jobs_db, j) == "running" for j in active_ids))
        # An owned running row the worker never locally tracked (simulating a
        # claimed job whose immediate finalization write failed before the
        # worker recorded it).
        orphan = _insert_owned_running_row(
            jobs_db,
            settings.worker_id,
            settings.worker_incarnation,
            settings.lease_duration_seconds,
        )
        first_active = {j: read_root(jobs_db, j)["state"]["lease_expires_at"] for j in active_ids}
        orphan_first = read_root(jobs_db, orphan)["state"]["lease_expires_at"]
        wait_until(
            lambda: all(
                read_root(jobs_db, j)["state"]["lease_expires_at"] > first_active[j]
                for j in active_ids
            )
        )
        # Under the old unscoped heartbeat the orphan IS refreshed merely
        # because the active jobs are heartbeated in the same bulk statement.
        assert read_root(jobs_db, orphan)["state"]["lease_expires_at"] > orphan_first
        # Therefore the orphan is never stale-recovered: it stays running.
        assert read_status(jobs_db, orphan) == "running"


def test_retry_terminations_handles_double_terminalization_failure(
    jobs_db: str, pg_cluster: _pg.PgCluster, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Both immediate finalization and quarantine fail -> local retry, no heartbeat.

    Forces a processless claimed job whose immediate finalization write fails
    AND whose quarantine terminalization also fails once, exercising the real
    ``_retry_terminations`` path: the job is kept locally owned for retry (so it
    is represented), its lease is never heartbeated, and the bounded retry
    converges to a terminal failed state with no re-execution.
    """
    # A is started first and is genuinely active/heartbeating before B exists.
    active_id = insert_job(jobs_db, str(tmp_path), "sleep 30")

    quarantine_calls: list[UUID] = []

    def _quarantine_fail_once_then_succeed(_conn: object, job_id: UUID, _reason: str) -> bool:
        quarantine_calls.append(job_id)
        # The first (immediate-finalization) call fails so the row is kept
        # locally for retry; the retry then converges.
        return len(quarantine_calls) != 1

    monkeypatch.setattr("lubko.worker._quarantine_job", _quarantine_fail_once_then_succeed)

    def _finish_raise(conn: JobsConnection, job_id: UUID, result: JobResult) -> str:
        if job_id == processless_id:
            msg = "simulated deterministic finalization failure"
            raise psycopg.DataError(msg)
        return REAL_FINISH_JOB(conn, job_id, result)

    monkeypatch.setattr("lubko.worker.finish_job", _finish_raise)

    settings = _issue74_settings()
    db = make_database_config(pg_cluster)
    with supervisor_running(settings, db, jobs_db):
        wait_until(lambda: read_status(jobs_db, active_id) == "running")
        # Introduce B after A is active; B cannot spawn and its immediate
        # finalization write fails, so it enters the retry-owned path.
        processless_id = insert_process_job(
            jobs_db, str(tmp_path), ["/nonexistent/lubko-no-such-bin"]
        )
        wait_until(lambda: read_status(jobs_db, processless_id) == "running")
        # The failed job's lease must never be heartbeated merely because A is
        # active.
        b_lease = read_root(jobs_db, processless_id)["state"]["lease_expires_at"]
        a_first = read_root(jobs_db, active_id)["state"]["lease_expires_at"]
        # The double-failure path was entered (immediate write + first retry).
        wait_until(lambda: len(quarantine_calls) >= 2)
        # A keeps heartbeating; B stays frozen (not refreshed).
        wait_until(
            lambda: (
                read_root(jobs_db, active_id)["state"]["lease_expires_at"] > a_first
                and read_root(jobs_db, processless_id)["state"]["lease_expires_at"] == b_lease
            )
        )
        # B converges to terminal failed via the retry; never re-executed.
        wait_until(lambda: read_status(jobs_db, processless_id) == "failed")
        assert read_status(jobs_db, active_id) == "running"
        assert "process_pgid" not in read_root(jobs_db, processless_id).get("state", {})


def _insert_owned_running_row(
    conninfo: str, worker_id: str, incarnation: str, lease_seconds: float
) -> UUID:
    """Insert a running command row owned by the given worker, with a future lease.

    Args:
        conninfo: PostgreSQL connection string.
        worker_id: Owning worker identifier.
        incarnation: Owning worker incarnation.
        lease_seconds: Seconds until the lease expires (kept in the future so
            the recovery pass does not reclaim it during the test window).

    Returns:
        The inserted job identifier.
    """
    job_id = uuid4()
    lease = (datetime.now(UTC) + timedelta(seconds=lease_seconds)).isoformat()
    payload = json.dumps({
        "v": 3,
        "type": "command",
        "request": {"cwd": "/workspace", "process": ["sleep", "30"]},
        "state": {
            "status": "running",
            "worker_id": worker_id,
            "worker_incarnation": incarnation,
            "lease_expires_at": lease,
        },
    })
    with psycopg.connect(conninfo) as conn:
        conn.execute(
            "INSERT INTO lubko.jobs (id, payload) VALUES (%s, %s)",
            (job_id, payload),
        )
    return job_id


def test_output_tail_bounded_with_immutable_chunks(
    jobs_db: str, pg_cluster: _pg.PgCluster, tmp_path: Path
) -> None:
    """Large output yields a bounded rolling tail and immutable chunks."""
    command = "i=0; while [ $i -lt 8000 ]; do echo line-$i; i=$((i+1)); done"
    job_id = insert_job(jobs_db, str(tmp_path), command)
    seen_lengths: list[int] = []

    def sample_tail() -> None:
        payload = read_root(jobs_db, job_id)
        output = payload.get("output") or {}
        stdout = output.get("stdout") or {}
        tail = stdout.get("tail") or ""
        seen_lengths.append(len(tail))

    with supervisor_running(supervisor_settings(), make_database_config(pg_cluster), jobs_db):
        wait_until(lambda: read_status(jobs_db, job_id) == "succeeded")
        wait_until(lambda: _has_chunks(jobs_db, job_id))
        for _ in range(3):
            sample_tail()
            time.sleep(0.2)

    payload = read_root(jobs_db, job_id)
    output = payload["output"]
    stdout = output["stdout"]
    tail = stdout["tail"]
    assert stdout["end"] == stdout["start"] + len(tail)
    assert len(tail) == OUTPUT_TAIL_MAX_BYTES
    assert tail.rstrip().endswith("line-7999")
    assert len(json.dumps(payload)) < 20_000

    assert len(seen_lengths) == 3
    assert all(length <= OUTPUT_TAIL_MAX_BYTES for length in seen_lengths)
    for earlier, later in itertools.pairwise(seen_lengths):
        assert later >= earlier

    chunks = read_chunks(jobs_db, job_id)
    assert chunks
    ordered = sorted(chunks, key=lambda item: item[1]["sequence"])
    expected_start = 0
    previous_id: UUID | None = None
    for chunk_id, chunk in ordered:
        assert chunk["thread"] == str(job_id)
        assert chunk["stream"] == "stdout"
        assert chunk["start"] == expected_start
        assert chunk["end"] - chunk["start"] == CHUNK_MAX_BYTES
        if previous_id is None:
            assert chunk["previous"] is None
        else:
            assert chunk["previous"] == str(previous_id)
        expected_start = chunk["end"]
        previous_id = chunk_id
    assert stdout["previous"] == str(previous_id)
    newest = ordered[-1][1]
    assert newest["end"] > stdout["start"] >= newest["start"]


def _has_chunks(conninfo: str, job_id: UUID) -> bool:
    """Return whether the job has at least one immutable chunk.

    Args:
        conninfo: PostgreSQL connection string.
        job_id: Owning root job.

    Returns:
        ``True`` when at least one chunk exists.
    """
    return bool(read_chunks(conninfo, job_id))


def test_cleanup_deletes_root_and_all_owned_chunks(
    jobs_db: str, pg_cluster: _pg.PgCluster, tmp_path: Path
) -> None:
    """Root deletion cleans every explicitly owned chunk, including orphans."""
    command = "i=0; while [ $i -lt 8000 ]; do echo line-$i; i=$((i+1)); done"
    job_id = insert_job(jobs_db, str(tmp_path), command)
    with supervisor_running(supervisor_settings(), make_database_config(pg_cluster), jobs_db):
        wait_until(lambda: read_status(jobs_db, job_id) == "succeeded")
        wait_until(lambda: _has_chunks(jobs_db, job_id))
    assert count_rows(jobs_db, job_id) > 1

    orphan_id = UUID("11111111-1111-1111-1111-111111111111")
    orphan_payload = json.dumps({
        "v": 3,
        "type": "output_chunk",
        "thread": str(job_id),
        "stream": "stderr",
        "sequence": 999,
        "start": 0,
        "end": 5,
        "value": "orphan",
        "previous": None,
    })
    with psycopg.connect(jobs_db) as conn:
        conn.execute(
            "INSERT INTO lubko.jobs (id, payload) VALUES (%s, %s)",
            (orphan_id, orphan_payload),
        )

    with psycopg.connect(jobs_db) as conn:
        delete_job_and_chunks(conn, job_id)

    assert count_rows(jobs_db, job_id) == 0


def _publish_job_for(job_id: UUID, cwd: str) -> ActiveJob:
    """Build an active job publishing one 9000-byte stdout capture.

    Args:
        job_id: Identifier matching an existing root ``command`` row.
        cwd: Working directory used as the temporary capture-file base.

    Returns:
        An active job whose stdout capture would archive three chunks.
    """
    proc = subprocess.Popen(
        ["/bin/true"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    guard.register(proc)
    job = ActiveJob(
        id=job_id,
        cwd=cwd,
        process=(sys.executable, "-c", "import time; time.sleep(30)"),
        proc=proc,
        pid=proc.pid,
        pgid=proc.pid,
        started_mono=time.monotonic(),
        claimed_at=time.time(),
    )
    job.stdout = OutputStream(path=Path(cwd) / "stdout.cap")
    job.stderr = OutputStream(path=Path(cwd) / "stderr.cap")
    return job


def _publish_on_connection(conninfo: str, job: ActiveJob, results: list[bool]) -> None:
    """Publish one job's output on a dedicated connection, recording the result.

    Args:
        conninfo: PostgreSQL connection string.
        job: The active job to publish.
        results: List receiving the publication result (root retained?).
    """
    conn = psycopg.connect(conninfo)
    try:
        results.append(
            publish_output(conn, job, [OUTPUT_STREAM_STDOUT], time.monotonic(), force=True)
        )
    finally:
        conn.close()


def _delete_job_and_chunks_on_connection(conninfo: str, job_id: UUID) -> None:
    """Delete one job and its chunks on a dedicated connection.

    Args:
        conninfo: PostgreSQL connection string.
        job_id: Identifier of the root job to delete.
    """
    conn = psycopg.connect(conninfo)
    try:
        delete_job_and_chunks(conn, job_id)
    finally:
        conn.close()


def _wait_for_blocked_jobs_locks(conninfo: str, count: int, timeout: float = 15.0) -> None:
    """Wait until ``count`` other backends are queued on a locked ``lubko.jobs`` row.

    Args:
        conninfo: PostgreSQL connection string.
        count: Number of blocked backends to wait for.
        timeout: Maximum seconds to wait.

    Raises:
        AssertionError: If the expected blocked backends do not queue in time.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with psycopg.connect(conninfo) as conn:
            row = conn.execute(
                "SELECT count(DISTINCT pid)::int\n"
                "FROM pg_locks\n"
                "WHERE NOT granted AND pid <> pg_backend_pid()\n",
            ).fetchone()
        if row is not None and row[0] >= count:
            return
        time.sleep(0.02)
    msg = f"timed out waiting for {count} backend(s) queued on a lubko.jobs row lock"
    raise AssertionError(msg)


def test_concurrent_delete_job_and_chunks_vs_publication_leaves_no_orphan_chunks(
    jobs_db: str,
    tmp_path: Path,
) -> None:
    """A root deleted while output publishes leaves no new chunk rows.

    Both operations run concurrently as their real implementations: a
    ``publish_output`` and a ``delete_job_and_chunks`` on separate
    connections. A helper connection holds the root ``command`` row lock so the
    publication queues on it first (winning the root lock, inserting new
    chunks), and the deletion queues behind it. Deleting the root before any
    chunk cleanup makes the deletion's chunk statement run under a fresh
    snapshot once publication releases the root, so the chunks publication just
    committed are removed rather than surviving as orphans under the deletion's
    stale statement snapshot.
    """
    job_id = insert_job(jobs_db, str(tmp_path), "sleep 30")
    job = _publish_job_for(job_id, str(tmp_path))
    job.stdout.path.write_bytes(b"x" * 9000)

    results: list[bool] = []
    blocker = psycopg.connect(jobs_db)
    try:
        with blocker.transaction():
            locked = blocker.execute(
                "SELECT id FROM lubko.jobs WHERE id = %s FOR UPDATE",
                (job_id,),
            ).fetchone()
            assert locked is not None
            publish_thread = threading.Thread(
                target=_publish_on_connection,
                args=(jobs_db, job, results),
                daemon=True,
            )
            publish_thread.start()
            _wait_for_blocked_jobs_locks(jobs_db, 1)
            delete_thread = threading.Thread(
                target=_delete_job_and_chunks_on_connection,
                args=(jobs_db, job_id),
                daemon=True,
            )
            delete_thread.start()
            _wait_for_blocked_jobs_locks(jobs_db, 2)
        publish_thread.join(timeout=30)
        delete_thread.join(timeout=30)
        assert not publish_thread.is_alive()
        assert not delete_thread.is_alive()
    finally:
        blocker.close()

    assert results == [True]
    assert count_rows(jobs_db, job_id) == 0
    assert read_chunks(jobs_db, job_id) == []
    assert job.stdout.archived_upto == 6000
    assert job.stdout.sequence == 3
    assert job.stdout.tail_end == 9000


def test_database_outage_stops_claims_and_never_lets_a_child_outlive_its_lease(
    jobs_db: str,
    pg_cluster: _pg.PgCluster,
    tmp_path: Path,
) -> None:
    """An outage keeps local supervision and terminates groups before lease expiry."""
    first = insert_job(jobs_db, str(tmp_path), "sleep 30")
    second = insert_job(jobs_db, str(tmp_path), "sleep 30")
    settings = supervisor_settings()
    settings = _with_short_lease(settings)

    with supervisor_running(settings, make_database_config(pg_cluster), jobs_db):
        wait_until(lambda: read_status(jobs_db, first) == "running")
        wait_until(lambda: read_status(jobs_db, second) == "running")
        pgids = [
            int(read_root(jobs_db, first)["state"]["process_pgid"]),
            int(read_root(jobs_db, second)["state"]["process_pgid"]),
        ]

        pg_cluster.stop()
        # No job can be claimed while the database is unreachable, and every
        # owned process group is terminated before its lease can expire.
        wait_until(lambda: not group_has_members(pgids[0]), timeout=15.0)
        wait_until(lambda: not group_has_members(pgids[1]), timeout=15.0)

        pg_cluster.start()
        wait_until(lambda: _terminal_or_failed(jobs_db, first))
        wait_until(lambda: _terminal_or_failed(jobs_db, second))

        resumed = insert_job(jobs_db, str(tmp_path), "echo resumed")
        wait_until(lambda: read_status(jobs_db, resumed) == "succeeded")

        first_payload = read_root(jobs_db, first)
        assert first_payload["state"]["status"] == "failed"
        assert not group_has_members(pgids[0])
        assert not group_has_members(pgids[1])


def _with_short_lease(settings: Settings) -> Settings:
    """Return settings with a very short lease for outage-safety testing.

    Args:
        settings: Base settings.

    Returns:
        Settings with a 1-second lease and 0.2-second safety margin.
    """
    return Settings(
        worker_id=settings.worker_id,
        poll_interval_seconds=settings.poll_interval_seconds,
        process_poll_interval_seconds=settings.process_poll_interval_seconds,
        cancel_grace_seconds=settings.cancel_grace_seconds,
        worker_incarnation=settings.worker_incarnation,
        lease_duration_seconds=1.0,
        lease_refresh_interval_seconds=0.15,
        lease_recovery_interval_seconds=0.1,
        output_publication_interval_seconds=settings.output_publication_interval_seconds,
        claim_batch_limit=settings.claim_batch_limit,
        lease_safety_margin_seconds=0.2,
        db_operation_timeout_seconds=3.0,
    )


def _terminal_or_failed(conninfo: str, job_id: UUID) -> bool:
    """Return whether a job reached a terminal state.

    Args:
        conninfo: PostgreSQL connection string.
        job_id: Job to inspect.

    Returns:
        ``True`` when terminal.
    """
    return read_status(conninfo, job_id) not in {"pending", "running"}


def _parse_iso_utc(value: str) -> float:
    """Parse a UTC ISO-8601 timestamp into a POSIX (wall-clock) float.

    Args:
        value: Timestamp such as ``2026-08-15T23:31:06.123456Z``.

    Returns:
        Seconds since the POSIX epoch (UTC).
    """
    stamp = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=UTC)
    return stamp.timestamp()


def _wait_for_group_death(pgid: int, timeout: float = 15.0) -> float:
    """Return the monotonic time at which a process group first becomes empty.

    Args:
        pgid: Process group identifier to watch.
        timeout: Maximum seconds to wait.

    Returns:
        The monotonic time the group was observed empty.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not group_has_members(pgid):
            return time.monotonic()
        time.sleep(0.02)
    msg = f"process group {pgid} never died within {timeout}s"
    pytest.fail(msg)


def _wait_until_status(conninfo: str, job_id: UUID, status: str, timeout: float = 15.0) -> float:
    """Return the monotonic time at which a job first reaches ``status``.

    Args:
        conninfo: PostgreSQL connection string.
        job_id: Job to watch.
        status: Target status.
        timeout: Maximum seconds to wait.

    Returns:
        The monotonic time the status was observed.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if read_status(conninfo, job_id) == status:
            return time.monotonic()
        time.sleep(0.02)
    msg = f"job {job_id} never reached status {status!r} within {timeout}s"
    pytest.fail(msg)


def _lease_db_expiry_mono(conninfo: str, job_id: UUID) -> float:
    """Return the monotonic time at which a job's database lease becomes stale.

    Args:
        conninfo: PostgreSQL connection string.
        job_id: Job whose ``state.lease_expires_at`` to read.

    Returns:
        The monotonic instant the committed database lease expires.
    """
    payload = read_root(conninfo, job_id)
    wall_expiry = _parse_iso_utc(payload["state"]["lease_expires_at"])
    return time.monotonic() + (wall_expiry - time.time())


def _start_recovery_worker(
    pg_cluster: _pg.PgCluster, settings: Settings
) -> tuple[Supervisor, threading.Thread]:
    """Start a second supervisor that only performs stale-job recovery.

    The recovery worker uses a distinct worker id and a short recovery interval
    so it actively scans for stale leases while the original supervisor is still
    in outage/lease-safety handling. It owns no jobs, so it can never refresh a
    lease or re-execute an abandoned job - it only marks genuinely stale jobs
    failed.

    Args:
        pg_cluster: The isolated PostgreSQL cluster.
        settings: Original supervisor settings (worker id is overridden).

    Returns:
        The recovery supervisor and its daemon thread.
    """
    recovery_settings = replace(settings, worker_id="recovery", lease_recovery_interval_seconds=0.1)
    recovery = Supervisor(recovery_settings, make_database_config(pg_cluster))
    rthread = threading.Thread(target=recovery.run, name="recovery", daemon=True)
    rthread.start()
    return recovery, rthread


def _record_group_deaths(
    first_pgid: int,
    later_pgid: int,
    deaths: dict[str, float],
    stop: threading.Event,
) -> None:
    """Poll process-group liveness and record each group's death instant.

    Runs concurrently with the recovery worker and the original supervisor's
    lease-safety eviction, so the recorded death times can be compared against
    the row recovery transitions to prove no overlap.

    Args:
        first_pgid: Process group id of the immediate job.
        later_pgid: Process group id of the delayed job.
        deaths: Mapping populated with ``"first"``/``"later"`` death instants.
        stop: Set to halt the sampler early.
    """
    first_seen = later_seen = False
    while not stop.is_set() and (not first_seen or not later_seen):
        if not first_seen and not group_has_members(first_pgid):
            deaths["first"] = time.monotonic()
            first_seen = True
        if not later_seen and not group_has_members(later_pgid):
            deaths["later"] = time.monotonic()
            later_seen = True
        if not (first_seen and later_seen):
            stop.wait(timeout=0.02)


@dataclass
class _EvictionState:
    """The original jobs' stop state captured before stale recovery re-marks them."""

    first_row_lost: bool
    later_row_lost: bool
    first_lease_evicted: bool
    later_lease_evicted: bool
    first_stop_reason: str | None
    later_stop_reason: str | None


@dataclass
class _LeaseDeadlineScenario:
    """Recorded observations from the #78 delayed-batch lease-safety scenario."""

    jobs_db: str
    db_expiry: float
    safety_margin: float
    lease_duration: float
    first_id: UUID
    later_id: UUID
    first_pgid: int
    later_pgid: int
    later_spawn_mono: float
    first_death: float
    later_death: float
    first_recovery: float
    later_recovery: float
    eviction: _EvictionState


def _capture_eviction_state(
    supervisor: Supervisor, first_id: UUID, later_id: UUID
) -> _EvictionState:
    """Record the original jobs' stop state before stale recovery re-marks them.

    This proves the jobs were evicted by the lease-safety path and never stopped
    by the refresh-result row_lost path: the refresh attempt was deterministically
    prevented, so ``apply_lease_refresh`` could never mark them row_lost, and the
    actual stop reason is captured before any recovery worker can re-mark them.

    Args:
        supervisor: The original supervisor whose active registry is inspected.
        first_id: Immediate job identifier.
        later_id: Delayed job identifier.

    Returns:
        The captured eviction/stop state of both jobs.
    """
    first = supervisor.active.get(first_id)
    later = supervisor.active.get(later_id)
    return _EvictionState(
        first_row_lost=first.row_lost if first is not None else True,
        later_row_lost=later.row_lost if later is not None else True,
        first_lease_evicted=first.lease_evicted if first is not None else False,
        later_lease_evicted=later.lease_evicted if later is not None else False,
        first_stop_reason=first.stop_reason if first is not None else None,
        later_stop_reason=later.stop_reason if later is not None else None,
    )


def _await_shared_claim_grant(jobs_db: str, first_id: UUID, later_id: UUID) -> float:
    """Wait for both jobs to run and return the monotonic lease-expiry instant.

    Both jobs are claimed in one batch, so they share the same committed claim
    grant and therefore the same database lease expiry; this asserts that and
    returns the monotonic instant at which that lease becomes stale.

    Args:
        jobs_db: PostgreSQL connection string.
        first_id: Immediate job identifier.
        later_id: Delayed job identifier.

    Returns:
        The monotonic instant the shared database lease becomes stale.
    """
    wait_until(lambda: read_status(jobs_db, first_id) == "running")
    wait_until(lambda: read_status(jobs_db, later_id) == "running")
    later_expiry = _parse_iso_utc(read_root(jobs_db, later_id)["state"]["lease_expires_at"])
    assert (
        abs(
            _parse_iso_utc(read_root(jobs_db, first_id)["state"]["lease_expires_at"]) - later_expiry
        )
        < 0.001
    )
    return time.monotonic() + (later_expiry - time.time())


def _await_recovery_transitions(
    jobs_db: str, first_id: UUID, later_id: UUID, death_stop: threading.Event
) -> tuple[float, float]:
    """Await the row recovery transitions while group liveness is sampled.

    Args:
        jobs_db: PostgreSQL connection string.
        first_id: Immediate job identifier.
        later_id: Delayed job identifier.
        death_stop: Signals the liveness sampler to halt.

    Returns:
        The monotonic instants the first and later jobs were observed failed.
    """
    first_recovery = _wait_until_status(jobs_db, first_id, "failed", timeout=15.0)
    later_recovery = _wait_until_status(jobs_db, later_id, "failed", timeout=15.0)
    death_stop.set()
    assert read_status(jobs_db, first_id) == "failed"
    assert read_status(jobs_db, later_id) == "failed"
    return first_recovery, later_recovery


def _run_delayed_batch_lease_scenario(
    jobs_db: str,
    pg_cluster: _pg.PgCluster,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    delay: float = 1.0,
) -> _LeaseDeadlineScenario:
    """Drive the #78 delayed-batch lease-safety scenario to completion.

    Claims two jobs in one batch (shared claim origin), delays the later job's
    spawn, deterministically prevents the lease-refresh attempt (so the
    production apply_lease_refresh path can never row_lost/stop the jobs with a
    false successful-empty refreshed set), cuts the database, and then makes
    recovery possible *concurrently*: PostgreSQL and a SECOND recovery worker are
    started at the measured lease-expires-at stale boundary while the original
    supervisor is still in outage/lease-safety handling. Group liveness, the
    original jobs' stop reason, and the row recovery transition are recorded.

    Args:
        jobs_db: PostgreSQL connection string.
        pg_cluster: The isolated PostgreSQL cluster.
        tmp_path: Per-test scratch directory.
        monkeypatch: Pytest monkeypatch fixture.
        delay: Seconds to delay the later job's spawn after the claim grant.

    Returns:
        The recorded death and recovery instants for no-overlap assertions.
    """
    first_id = insert_job(jobs_db, str(tmp_path), "sleep 100")
    later_id = insert_job(jobs_db, str(tmp_path), "sleep 100")
    captured: dict[str, object] = {}
    spawned = (threading.Event(), threading.Event())

    def delayed_spawn(spec: Job) -> tuple[subprocess.Popen[bytes], Path, Path, int, int]:
        if spec.id == later_id:
            time.sleep(delay)
            captured["later_spawn_mono"] = time.monotonic()
        result = spawn_job(spec)
        if spec.id == first_id:
            captured["first_pgid"] = result[3]
            spawned[0].set()
        else:
            captured["later_pgid"] = result[3]
            spawned[1].set()
        return result

    monkeypatch.setattr(worker_module, "spawn_job", delayed_spawn)
    # Deterministically prevent the lease-refresh ATTEMPT so the production
    # apply_lease_refresh path can never mark the jobs row_lost/stop them with a
    # false successful-empty refreshed set. The claim grant therefore stays the
    # only committed lease event and the lease-safety eviction is the sole stop
    # path; the separate heartbeat test covers the real refresh/commit path.
    monkeypatch.setattr(worker_module.Supervisor, "_refresh_leases", lambda *_args: None)
    settings = Settings(
        worker_id="orig",
        poll_interval_seconds=0.05,
        process_poll_interval_seconds=0.05,
        cancel_grace_seconds=0.05,
        lease_duration_seconds=2.0,
        lease_refresh_interval_seconds=1.8,
        lease_recovery_interval_seconds=5.0,
        output_publication_interval_seconds=0.1,
        claim_batch_limit=16,
        lease_safety_margin_seconds=0.3,
        db_operation_timeout_seconds=3.0,
    )
    supervisor = Supervisor(settings, make_database_config(pg_cluster))
    thread = threading.Thread(target=supervisor.run, daemon=True)
    thread.start()
    deaths: dict[str, float] = {}
    death_stop = threading.Event()
    recovery_worker: tuple[Supervisor, threading.Thread] | None = None
    try:
        # Both jobs are claimed in ONE batch, so they share the same committed
        # grant and therefore the same database lease expiry.
        db_expiry = _await_shared_claim_grant(jobs_db, first_id, later_id)
        assert spawned[0].wait(timeout=15.0)
        assert spawned[1].wait(timeout=15.0)
        # The lease was never refreshed, so the database lease stays anchored
        # to the shared claim grant.
        assert _parse_iso_utc(
            read_root(jobs_db, later_id)["state"]["lease_expires_at"]
        ) == _parse_iso_utc(read_root(jobs_db, first_id)["state"]["lease_expires_at"])
        # Take the database out so no post-spawn refresh can ever commit.
        pg_cluster.stop()
        # Wait for the supervisor to actually enter the outage path before the
        # lease-safety deadline can evict the groups.
        wait_until(lambda: supervisor.conn is None, timeout=10.0)
        # Record original group liveness concurrently with the eviction and the
        # later recovery scan.
        threading.Thread(
            target=_record_group_deaths,
            args=(
                cast("int", captured["first_pgid"]),
                cast("int", captured["later_pgid"]),
                deaths,
                death_stop,
            ),
            daemon=True,
        ).start()
        # Make recovery possible at the stale boundary, NOT after waiting for
        # group death: restart PostgreSQL and start a SECOND recovery worker
        # while the original supervisor is still in outage/lease-safety handling.
        time.sleep(max(0.0, db_expiry - time.monotonic() - 0.05))
        # Capture the original jobs' stop state BEFORE any stale recovery can
        # re-mark them: this proves they were evicted by the lease-safety path
        # (STOP_REASON_LEASE) and never stopped by the refresh-result row_lost
        # path before that eviction.
        eviction = _capture_eviction_state(supervisor, first_id, later_id)
        pg_cluster.start()
        recovery_worker = _start_recovery_worker(pg_cluster, settings)
        # Wait for the row recovery transition while the groups are observed.
        recoveries = _await_recovery_transitions(jobs_db, first_id, later_id, death_stop)
        return _LeaseDeadlineScenario(
            jobs_db=jobs_db,
            db_expiry=db_expiry,
            safety_margin=settings.lease_safety_margin_seconds,
            lease_duration=settings.lease_duration_seconds,
            first_id=first_id,
            later_id=later_id,
            first_pgid=cast("int", captured["first_pgid"]),
            later_pgid=cast("int", captured["later_pgid"]),
            later_spawn_mono=cast("float", captured["later_spawn_mono"]),
            first_death=deaths["first"],
            later_death=deaths["later"],
            first_recovery=recoveries[0],
            later_recovery=recoveries[1],
            eviction=eviction,
        )
    finally:
        death_stop.set()
        supervisor.request_shutdown()
        thread.join(timeout=30)
        if recovery_worker is not None:
            recovery_worker[0].request_shutdown()
            recovery_worker[1].join(timeout=30)
        with suppress(Exception):
            pg_cluster.start()
        _kill_leftover_groups(jobs_db)


def _assert_lease_deadline_no_overlap(obs: _LeaseDeadlineScenario) -> None:
    """Prove the #78 lease-safety fix: no stale recovery while a group is alive.

    The original jobs were evicted by the lease-safety path (stop reason
    ``lease``, never the refresh-result ``row_lost`` path), they are only
    legitimately recovered strictly after each original process group has died
    (no-overlap), both groups are evicted by the shared claim-origin deadline
    before the measured database lease expiry (within a named tolerance below the
    safety margin), and the delayed job dies before the spawn-anchored deadline
    the pre-fix post-spawn bug would produce.
    """
    assert obs.first_death < obs.first_recovery
    assert obs.later_death < obs.later_recovery
    # The original jobs were evicted by the lease-safety path, never stopped by
    # the refresh-result row_lost path: the refresh attempt was deterministically
    # prevented, so apply_lease_refresh could never mark them row_lost, and the
    # actual stop reason is the lease-safety eviction.
    assert not obs.eviction.first_row_lost
    assert not obs.eviction.later_row_lost
    assert obs.eviction.first_lease_evicted
    assert obs.eviction.later_lease_evicted
    assert obs.eviction.first_stop_reason == STOP_REASON_LEASE
    assert obs.eviction.later_stop_reason == STOP_REASON_LEASE
    stale_overlap_tolerance = obs.safety_margin - 0.1
    assert 0.0 <= stale_overlap_tolerance < obs.safety_margin
    eviction_bound = obs.db_expiry - obs.safety_margin + stale_overlap_tolerance
    assert obs.first_death <= eviction_bound
    assert obs.later_death <= eviction_bound
    assert obs.later_death < (obs.later_spawn_mono + obs.lease_duration - obs.safety_margin)
    assert not group_has_members(obs.first_pgid)
    assert not group_has_members(obs.later_pgid)
    assert read_status(obs.jobs_db, obs.first_id) == "failed"
    assert read_status(obs.jobs_db, obs.later_id) == "failed"


def test_lease_safety_deadline_bound_to_claim_grant_across_batch(
    jobs_db: str,
    pg_cluster: _pg.PgCluster,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Issue #78: the lease deadline is bound to the committed claim grant.

    Two jobs are claimed in ONE batch (a single grant), so they share the same
    committed claim monotonic origin. The first starts immediately; the later
    job's spawn is deliberately delayed after the same grant. The lease-refresh
    ATTEMPT is deterministically prevented (never a successful empty refreshed
    set), so the claim grant is the only lease event: the local lease-safety
    deadline of BOTH jobs is anchored to the shared claim origin, and the
    claim-to-spawn delay consumed lease budget, so the later process dies before
    the actual database lease stale boundary. The test explicitly proves the
    original jobs are stopped by the lease-safety path (never the refresh-result
    row_lost path) before outage eviction.

    Recovery is made possible *concurrently*: PostgreSQL and a SECOND recovery
    worker are started at the measured lease-expires-at stale boundary while the
    original supervisor is still in outage/lease-safety handling. Group liveness
    and the row recovery transition are recorded, and the test proves stale
    recovery never legitimately occurs while either original group is alive -
    the no-overlap guard for the lease-safety fix. The contrast that the later
    process dies earlier than a spawn-anchored deadline is the mutation guard
    for the original post-spawn bug, under which the delayed job would outlive
    its database lease.
    """
    obs = _run_delayed_batch_lease_scenario(jobs_db, pg_cluster, tmp_path, monkeypatch)
    _assert_lease_deadline_no_overlap(obs)


def test_heartbeat_advances_lease_deadline_only_after_commit(
    jobs_db: str,
    pg_cluster: _pg.PgCluster,
    tmp_path: Path,
) -> None:
    """Issue #78: the local deadline advances only after a committed refresh.

    Through the real production refresh path (a live Supervisor over real
    PostgreSQL) a committed bulk heartbeat advances the local lease-safety
    deadline, anchored to a conservative monotonic origin captured before the
    operation.  When the database is taken out so refresh transactions fail to
    commit, the local deadline does NOT advance: a missed heartbeat can never
    silently extend a job's safe lifetime.
    """
    job_id = insert_job(jobs_db, str(tmp_path), "sleep 30")
    settings = replace(
        supervisor_settings(),
        lease_duration_seconds=8.0,
        lease_safety_margin_seconds=0.5,
    )
    supervisor = Supervisor(settings, make_database_config(pg_cluster))
    thread = threading.Thread(target=supervisor.run, daemon=True)
    thread.start()
    try:
        wait_until(lambda: read_status(jobs_db, job_id) == "running")
        wait_until(lambda: job_id in supervisor.active)
        job = supervisor.active[job_id]

        # Committed refresh advances the local deadline.
        before = job.last_heartbeat_at
        wait_until(lambda: job.last_heartbeat_at > before, timeout=5.0)
        advanced = job.last_heartbeat_at
        assert advanced > before
        # Anchored to a conservative origin: not in the distant future.
        assert advanced <= time.monotonic() + 0.5

        # Failed/connectivity path: the database is taken out, every attempted
        # refresh fails to commit, and the local deadline must stay frozen.
        pg_cluster.stop()
        frozen = job.last_heartbeat_at
        time.sleep(1.0)  # several failed refresh attempts
        assert abs(job.last_heartbeat_at - frozen) < 1e-9

        supervisor.request_shutdown()
        thread.join(timeout=30)
        assert not thread.is_alive()
    finally:
        if thread.is_alive():
            supervisor.request_shutdown()
            thread.join(timeout=30)
        with suppress(Exception):
            pg_cluster.start()
        _kill_leftover_groups(jobs_db)


def test_graceful_shutdown_terminates_and_reaps_all_process_groups(
    jobs_db: str, pg_cluster: _pg.PgCluster, tmp_path: Path
) -> None:
    """Graceful shutdown terminates and reaps every tracked process group."""
    job_ids = [insert_job(jobs_db, str(tmp_path), "sleep 300") for _ in range(3)]
    settings = supervisor_settings()

    with supervisor_running(settings, make_database_config(pg_cluster), jobs_db):
        wait_until(lambda: all(read_status(jobs_db, j) == "running" for j in job_ids))
        pgids = [int(read_root(jobs_db, j)["state"]["process_pgid"]) for j in job_ids]

    assert not zombie_children()
    for pgid in pgids:
        assert not group_has_members(pgid)
    for job_id in job_ids:
        assert read_status(jobs_db, job_id) == "cancelled"
    for job_id in job_ids:
        payload = read_root(jobs_db, job_id)
        assert "shutting down" in (payload["result"]["cancellation_note"] or "")


def test_multi_worker_claim_safety(jobs_db: str, pg_cluster: _pg.PgCluster, tmp_path: Path) -> None:
    """Two daemons never execute the same root job concurrently."""
    markers = tmp_path / "markers"
    markers.mkdir()
    commands = [f"echo ran >> {markers}/job-{i}.out; sleep 0.3" for i in range(10)]
    job_ids = [insert_job(jobs_db, str(tmp_path), command) for command in commands]
    database = make_database_config(pg_cluster)

    first = Supervisor(supervisor_settings("worker-a"), database)
    second = Supervisor(supervisor_settings("worker-b"), database)
    threads = [
        threading.Thread(target=first.run, daemon=True),
        threading.Thread(target=second.run, daemon=True),
    ]
    try:
        for thread in threads:
            thread.start()
        wait_until(lambda: all(read_status(jobs_db, j) == "succeeded" for j in job_ids))
    finally:
        first.request_shutdown()
        second.request_shutdown()
        for thread in threads:
            thread.join(timeout=30)
        _kill_leftover_groups(jobs_db)

    for job_id in job_ids:
        payload = read_root(jobs_db, job_id)
        assert payload["state"]["status"] == "succeeded"
        assert payload["state"]["worker_id"] in {"worker-a", "worker-b"}
    for i in range(10):
        lines = (markers / f"job-{i}.out").read_text(encoding="utf-8").splitlines()
        assert lines == ["ran"]


def test_many_jobs_run_concurrently_without_an_application_limit(
    jobs_db: str, pg_cluster: _pg.PgCluster, tmp_path: Path
) -> None:
    """Dozens of independent jobs all run at the same time with no cap."""
    markers = tmp_path / "many"
    markers.mkdir()
    count = 40
    commands = [
        (f"echo start > {markers}/job-{i}.start\nsleep 2\necho end > {markers}/job-{i}.end")
        for i in range(count)
    ]
    job_ids = [insert_job(jobs_db, str(tmp_path), command) for command in commands]

    with supervisor_running(supervisor_settings(), make_database_config(pg_cluster), jobs_db):
        wait_until(
            lambda: all((markers / f"job-{i}.start").exists() for i in range(count)),
            timeout=60.0,
        )
        wait_until(
            lambda: all(read_status(jobs_db, job_id) == "running" for job_id in job_ids),
            timeout=60.0,
        )
        assert all(read_status(jobs_db, job_id) == "running" for job_id in job_ids)
        assert not any((markers / f"job-{i}.end").exists() for i in range(count))
        wait_until(
            lambda: all(read_status(jobs_db, j) == "succeeded" for j in job_ids),
            timeout=60.0,
        )

    assert not zombie_children()
    for job_id in job_ids:
        assert read_status(jobs_db, job_id) == "succeeded"


def test_bounded_claim_batch_still_drains_an_endless_queue(
    jobs_db: str, pg_cluster: _pg.PgCluster, tmp_path: Path
) -> None:
    """A fairness-bounded claim batch never starves a large pending queue."""
    count = 40
    job_ids = [insert_job(jobs_db, str(tmp_path), "echo ok") for _ in range(count)]
    settings = supervisor_settings()
    settings = Settings(
        worker_id=settings.worker_id,
        poll_interval_seconds=settings.poll_interval_seconds,
        process_poll_interval_seconds=settings.process_poll_interval_seconds,
        cancel_grace_seconds=settings.cancel_grace_seconds,
        worker_incarnation=settings.worker_incarnation,
        lease_duration_seconds=settings.lease_duration_seconds,
        lease_refresh_interval_seconds=settings.lease_refresh_interval_seconds,
        lease_recovery_interval_seconds=settings.lease_recovery_interval_seconds,
        output_publication_interval_seconds=settings.output_publication_interval_seconds,
        claim_batch_limit=4,
        lease_safety_margin_seconds=settings.lease_safety_margin_seconds,
        db_operation_timeout_seconds=settings.db_operation_timeout_seconds,
    )

    with supervisor_running(settings, make_database_config(pg_cluster), jobs_db):
        wait_until(
            lambda: all(read_status(jobs_db, j) == "succeeded" for j in job_ids),
            timeout=60.0,
        )

    for job_id in job_ids:
        assert read_status(jobs_db, job_id) == "succeeded"


def test_claim_rejects_unparseable_job_without_affecting_others(
    jobs_db: str, pg_cluster: _pg.PgCluster, tmp_path: Path
) -> None:
    """An unparseable claimed job fails cleanly while others keep running."""
    bad_payload = json.dumps({
        "v": 3,
        "type": "command",
        "request": {"cwd": ""},
        "state": {"status": "pending"},
    })
    with psycopg.connect(jobs_db) as conn:
        conn.execute("INSERT INTO lubko.jobs (payload) VALUES (%s)", (bad_payload,))
    healthy = insert_job(jobs_db, str(tmp_path), "echo healthy; sleep 0.3")

    with supervisor_running(supervisor_settings(), make_database_config(pg_cluster), jobs_db):
        wait_until(lambda: read_status(jobs_db, healthy) == "succeeded")
        with psycopg.connect(jobs_db) as conn:
            row = conn.execute(
                "SELECT count(*)::int FROM lubko.jobs WHERE id NOT IN (%s)",
                (healthy,),
            ).fetchone()
        assert row is not None
        assert row[0] == 1

    with psycopg.connect(jobs_db) as conn:
        row = conn.execute(
            "SELECT (payload::jsonb)->'state'->>'status', "
            "(payload::jsonb)->'result'->>'stderr' "
            "FROM lubko.jobs WHERE id <> %s",
            (healthy,),
        ).fetchone()
    assert row is not None
    assert row[0] == "failed"
    assert "invalid job payload" in row[1]


# ---------------------------------------------------------------------------
# Issue #94: PostgreSQL-safe output, quarantine, and connectivity tests
# ---------------------------------------------------------------------------

NUL_STDERR_PYTHON: Final = (
    sys.executable,
    "-c",
    "import sys; sys.stderr.buffer.write(b'before\\x00after'); sys.stderr.flush()",
)

NUL_STDOUT_PYTHON: Final = (
    sys.executable,
    "-c",
    "import sys; sys.stdout.buffer.write(b'before\\x00after'); sys.stdout.flush()",
)

NUL_BOTH_PYTHON: Final = (
    sys.executable,
    "-c",
    (
        "import sys\n"
        "sys.stdout.buffer.write(b'out\\x00start')\n"
        "sys.stderr.buffer.write(b'err\\x00start')\n"
        "sys.stdout.flush()\n"
        "sys.stderr.flush()\n"
    ),
)

HEAVY_NUL_PYTHON: Final = (
    sys.executable,
    "-c",
    "import sys; sys.stdout.buffer.write(b'A' * 5000 + b'\\x00' + b'B' * 5000); sys.stdout.flush()",
)

INVALID_UTF8_NUL_PYTHON: Final = (
    sys.executable,
    "-c",
    "import sys\nsys.stdout.buffer.write(b'\\xff\\x00\\xfe')\nsys.stdout.flush()\n",
)


def test_nul_in_stdout_produces_safe_output(
    jobs_db: str, pg_cluster: _pg.PgCluster, tmp_path: Path
) -> None:
    """Regression 1: stdout containing NUL bytes is published safely to PostgreSQL."""
    job_id = insert_process_job(jobs_db, str(tmp_path), list(NUL_STDOUT_PYTHON))

    with supervisor_running(supervisor_settings(), make_database_config(pg_cluster), jobs_db):
        wait_until(lambda: read_status(jobs_db, job_id) == "succeeded")

    payload = read_root(jobs_db, job_id)
    result = payload.get("result") or {}
    stdout = result.get("stdout") or ""
    assert "\x00" not in stdout
    assert "before" in stdout
    assert "after" in stdout


def test_nul_in_stderr_produces_safe_output(
    jobs_db: str, pg_cluster: _pg.PgCluster, tmp_path: Path
) -> None:
    """Regression 2: stderr containing NUL bytes is published safely to PostgreSQL."""
    job_id = insert_process_job(jobs_db, str(tmp_path), list(NUL_STDERR_PYTHON))

    with supervisor_running(supervisor_settings(), make_database_config(pg_cluster), jobs_db):
        wait_until(lambda: read_status(jobs_db, job_id) == "succeeded")

    payload = read_root(jobs_db, job_id)
    result = payload.get("result") or {}
    stderr = result.get("stderr") or ""
    assert "\x00" not in stderr
    assert "before" in stderr
    assert "after" in stderr


def test_nul_in_live_root_tail(jobs_db: str, pg_cluster: _pg.PgCluster, tmp_path: Path) -> None:
    """Regression 3: NUL in live output tail is sanitized before PostgreSQL insertion."""
    job_id = insert_process_job(jobs_db, str(tmp_path), list(NUL_STDOUT_PYTHON))

    with supervisor_running(supervisor_settings(), make_database_config(pg_cluster), jobs_db):
        wait_until(lambda: read_status(jobs_db, job_id) == "succeeded")
        tail = _root_stdout_tail(jobs_db, job_id)
        assert "\x00" not in tail
        assert "before" in tail or "after" in tail


def test_nul_in_immutable_output_chunk(
    jobs_db: str, pg_cluster: _pg.PgCluster, tmp_path: Path
) -> None:
    """Regression 4: NUL in data old enough for chunk archival is safe."""
    job_id = insert_process_job(jobs_db, str(tmp_path), list(HEAVY_NUL_PYTHON))

    with supervisor_running(supervisor_settings(), make_database_config(pg_cluster), jobs_db):
        wait_until(lambda: read_status(jobs_db, job_id) == "succeeded")
        chunks = read_chunks(jobs_db, job_id)
        for _chunk_id, chunk_payload in chunks:
            value = chunk_payload.get("value", "")
            assert "\x00" not in value


def test_terminal_stdout_stderr_with_nul(
    jobs_db: str, pg_cluster: _pg.PgCluster, tmp_path: Path
) -> None:
    """Regression 5: terminal result stdout/stderr with NUL are safe for PostgreSQL."""
    job_id = insert_process_job(jobs_db, str(tmp_path), list(NUL_BOTH_PYTHON))

    with supervisor_running(supervisor_settings(), make_database_config(pg_cluster), jobs_db):
        wait_until(lambda: read_status(jobs_db, job_id) == "succeeded")

    payload = read_root(jobs_db, job_id)
    result = payload.get("result") or {}
    assert "\x00" not in (result.get("stdout") or "")
    assert "\x00" not in (result.get("stderr") or "")
    assert "out" in (result.get("stdout") or "")
    assert "err" in (result.get("stderr") or "")


def test_invalid_utf8_combined_with_nul(
    jobs_db: str, pg_cluster: _pg.PgCluster, tmp_path: Path
) -> None:
    """Regression 6: invalid UTF-8 combined with NUL is handled safely."""
    job_id = insert_process_job(jobs_db, str(tmp_path), list(INVALID_UTF8_NUL_PYTHON))

    with supervisor_running(supervisor_settings(), make_database_config(pg_cluster), jobs_db):
        wait_until(lambda: read_status(jobs_db, job_id) == "succeeded")

    payload = read_root(jobs_db, job_id)
    result = payload.get("result") or {}
    stdout = result.get("stdout") or ""
    assert "\x00" not in stdout


def test_unrelated_job_continues_while_nul_job_present(
    jobs_db: str, pg_cluster: _pg.PgCluster, tmp_path: Path
) -> None:
    """Regression 8: an unrelated job continues while NUL-producing job runs."""
    nul_job = insert_process_job(jobs_db, str(tmp_path), list(NUL_BOTH_PYTHON))
    healthy = insert_process_job(
        jobs_db,
        str(tmp_path),
        [sys.executable, "-c", "import time; time.sleep(0.5); print('ok')"],
    )

    with supervisor_running(supervisor_settings(), make_database_config(pg_cluster), jobs_db):
        wait_until(lambda: read_status(jobs_db, nul_job) == "succeeded")
        wait_until(lambda: read_status(jobs_db, healthy) == "succeeded")

    nul_payload = read_root(jobs_db, nul_job)
    healthy_payload = read_root(jobs_db, healthy)
    assert "\x00" not in (nul_payload.get("result", {}).get("stdout") or "")
    assert healthy_payload["result"]["exit_code"] == 0


def test_nul_job_does_not_trigger_reconnect_loop(
    jobs_db: str, pg_cluster: _pg.PgCluster, tmp_path: Path
) -> None:
    """Regression 9: worker does not enter reconnect loop for deterministic NUL errors."""
    nul_job = insert_process_job(jobs_db, str(tmp_path), list(NUL_BOTH_PYTHON))
    healthy = insert_process_job(
        jobs_db,
        str(tmp_path),
        [sys.executable, "-c", "import time; print('healthy'); time.sleep(0.3)"],
    )

    with supervisor_running(supervisor_settings(), make_database_config(pg_cluster), jobs_db):
        wait_until(lambda: read_status(jobs_db, nul_job) in {"succeeded", "failed"})
        wait_until(lambda: read_status(jobs_db, healthy) == "succeeded")

    healthy_payload = read_root(jobs_db, healthy)
    assert healthy_payload["state"]["status"] == "succeeded"


def test_connectivity_error_escapes_local_catch_to_outage(
    jobs_db: str, pg_cluster: _pg.PgCluster, tmp_path: Path
) -> None:
    """A SQLSTATE class-08 error re-raises from the local catch and causes outage.

    Fault injection: ``publish_output`` raises an OperationalError with
    sqlstate ``08006`` (connection_failure) for the target job.  The per-job
    catch classifies it as connectivity and re-raises; ``run()`` then calls
    ``_enter_outage``.
    """
    target = insert_process_job(
        jobs_db,
        str(tmp_path),
        [
            sys.executable,
            "-c",
            "import sys, time; sys.stdout.write('x\\n'); sys.stdout.flush(); time.sleep(30)",
        ],
    )
    other = insert_process_job(
        jobs_db,
        str(tmp_path),
        [
            sys.executable,
            "-c",
            "import sys, time; sys.stdout.write('y\\n'); sys.stdout.flush(); time.sleep(30)",
        ],
    )

    conn_lost_msg = "simulated connection failure"

    def _raise_connectivity(
        _conn: JobsConnection, job: ActiveJob, *_args: object, **_kwargs: object
    ) -> bool:
        if job.id == target:
            exc = psycopg.OperationalError(conn_lost_msg)
            exc.sqlstate = "08006"
            raise exc
        return True

    supervisor = Supervisor(supervisor_settings(), make_database_config(pg_cluster))
    thread = threading.Thread(target=supervisor.run, daemon=True)
    try:
        with patch("lubko.worker.publish_output", side_effect=_raise_connectivity):
            thread.start()
            wait_until(lambda: read_status(jobs_db, target) == "running")
            wait_until(lambda: read_status(jobs_db, other) == "running")
            wait_until(lambda: supervisor.conn is None, timeout=10.0)
            assert supervisor.conn is None
    finally:
        supervisor.request_shutdown()
        thread.join(timeout=30)
        _kill_leftover_groups(jobs_db)


def test_deterministic_data_error_stays_local(
    jobs_db: str, pg_cluster: _pg.PgCluster, tmp_path: Path
) -> None:
    """A DataError stays local: job quarantined, no outage, other jobs unaffected.

    Fault injection: ``publish_output`` raises ``DataError`` (SQLSTATE 22P05
    class) for the target job.  The per-job catch quarantines the job without
    re-raising, so the connection stays usable.
    """
    target = insert_process_job(
        jobs_db,
        str(tmp_path),
        [
            sys.executable,
            "-c",
            "import sys, time; sys.stdout.write('x\\n'); sys.stdout.flush(); time.sleep(30)",
        ],
    )
    other = insert_process_job(
        jobs_db,
        str(tmp_path),
        [
            sys.executable,
            "-c",
            "import sys, time; sys.stdout.write('y\\n'); sys.stdout.flush(); time.sleep(30)",
        ],
    )

    data_err_msg = "unsupported Unicode escape sequence"

    def _raise_data_error(
        _conn: JobsConnection, job: ActiveJob, *_args: object, **_kwargs: object
    ) -> bool:
        if job.id == target:
            exc = psycopg.DataError(data_err_msg)
            exc.sqlstate = "22P05"
            raise exc
        return True

    supervisor = Supervisor(supervisor_settings(), make_database_config(pg_cluster))
    thread = threading.Thread(target=supervisor.run, daemon=True)
    try:
        with patch("lubko.worker.publish_output", side_effect=_raise_data_error):
            thread.start()
            wait_until(lambda: read_status(jobs_db, target) in {"running", "failed"})
            wait_until(lambda: read_status(jobs_db, other) == "running")
            time.sleep(1.0)
            assert supervisor.conn is not None
            assert read_status(jobs_db, target) == "failed"
            assert read_status(jobs_db, other) == "running"
    finally:
        supervisor.request_shutdown()
        thread.join(timeout=30)
        _kill_leftover_groups(jobs_db)


def test_deterministic_publish_failure_terminalizes_only_offending_job(
    jobs_db: str, pg_cluster: _pg.PgCluster, tmp_path: Path
) -> None:
    """A deterministic DB error durably quarantines only the offending job.

    Fault injection: ``publish_output`` raises DataError (22P05) for the
    target job.  The per-job catch quarantines the row to ``failed`` and
    the unrelated job finishes successfully.
    """
    target = insert_process_job(
        jobs_db,
        str(tmp_path),
        [
            sys.executable,
            "-c",
            "import sys, time; sys.stdout.write('x\\n'); sys.stdout.flush(); time.sleep(30)",
        ],
    )
    other = insert_process_job(
        jobs_db,
        str(tmp_path),
        [sys.executable, "-c", "import time; time.sleep(0.5); print('ok')"],
    )

    inject_msg = "injected data error for quarantine regression"

    def _raise_data_error(
        _conn: JobsConnection, job: ActiveJob, *_args: object, **_kwargs: object
    ) -> bool:
        if job.id == target:
            exc = psycopg.DataError(inject_msg)
            exc.sqlstate = "22P05"
            raise exc
        return True

    supervisor = Supervisor(supervisor_settings(), make_database_config(pg_cluster))
    thread = threading.Thread(target=supervisor.run, daemon=True)
    try:
        with patch("lubko.worker.publish_output", side_effect=_raise_data_error):
            thread.start()
            wait_until(lambda: read_status(jobs_db, target) == "failed")
            wait_until(lambda: read_status(jobs_db, other) == "succeeded")

        # Durable quarantine: the row is failed, quarantine_reason is set.
        payload = read_root(jobs_db, target)
        assert payload["state"]["status"] == "failed"
        assert "quarantine_reason" in payload["state"]
    finally:
        supervisor.request_shutdown()
        thread.join(timeout=30)
        _kill_leftover_groups(jobs_db)


def test_quarantine_sqlstate_logged(
    jobs_db: str, pg_cluster: _pg.PgCluster, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Actual SQLSTATE and exception text are logged on quarantine.

    Fault injection: ``publish_output`` raises DataError with SQLSTATE 22P05.
    The log must contain both the SQLSTATE code and the exception message.
    """
    target = insert_process_job(
        jobs_db,
        str(tmp_path),
        [
            sys.executable,
            "-c",
            "import sys, time; sys.stdout.write('x\\n'); sys.stdout.flush(); time.sleep(30)",
        ],
    )

    inject_msg = "diagnostic text for SQLSTATE logging regression"

    def _raise_data_error(
        _conn: JobsConnection, job: ActiveJob, *_args: object, **_kwargs: object
    ) -> bool:
        if job.id == target:
            exc = psycopg.DataError(inject_msg)
            exc.sqlstate = "22P05"
            raise exc
        return True

    supervisor = Supervisor(supervisor_settings(), make_database_config(pg_cluster))
    thread = threading.Thread(target=supervisor.run, daemon=True)
    try:
        with (
            caplog.at_level(logging.ERROR, logger="lubko.worker"),
            patch("lubko.worker.publish_output", side_effect=_raise_data_error),
        ):
            thread.start()
            wait_until(lambda: read_status(jobs_db, target) == "failed")

        logged = [r for r in caplog.records if r.levelno >= logging.ERROR]
        assert logged, "expected at least one ERROR log from quarantine"
        combined_msg = " ".join(r.message for r in logged)
        combined_exc = " ".join(r.exc_text or "" for r in logged)
        assert "22P05" in combined_msg
        assert inject_msg in combined_exc
    finally:
        supervisor.request_shutdown()
        thread.join(timeout=30)
        _kill_leftover_groups(jobs_db)


# ---------------------------------------------------------------------------
# Real-PostgreSQL archival regression: NUL + invalid UTF-8 chunk boundaries
# ---------------------------------------------------------------------------

ARCHIVAL_NUL_PYTHON: Final = (
    sys.executable,
    "-c",
    (
        "import sys\n"
        "sys.stdout.buffer.write(b'X' * 3000 + b'\\xff\\x00\\xfe' + b'Y' * 3000)\n"
        "sys.stdout.flush()\n"
    ),
)


def test_nul_invalid_utf8_archival_chunk_boundaries(
    jobs_db: str, pg_cluster: _pg.PgCluster, tmp_path: Path
) -> None:
    """Persisted chunk/root start/end match exact raw capture byte positions.

    A job writes 6003 bytes (3000 X + 3 invalid + 3000 Y).  The supervisor
    archives output older than the live tail into immutable output_chunk rows.
    Each chunk's ``start``/``end`` must correspond to exact byte offsets, and
    each chunk's ``value`` must equal ``pg_safe_decode`` of the exact raw slice.
    The live tail window must also match the exact raw slice.  Chunks must be
    contiguous and not overlap the live tail (accounting for the designed
    archive-margin overlap).
    """
    raw = b"X" * 3000 + b"\xff\x00\xfe" + b"Y" * 3000
    assert len(raw) == 6003

    job_id = insert_process_job(jobs_db, str(tmp_path), list(ARCHIVAL_NUL_PYTHON))

    with supervisor_running(supervisor_settings(), make_database_config(pg_cluster), jobs_db):
        wait_until(lambda: read_status(jobs_db, job_id) == "succeeded")
        wait_until(lambda: bool(read_chunks(jobs_db, job_id)))

    chunks = read_chunks(jobs_db, job_id)
    assert len(chunks) >= 1, "expected at least one archived chunk"

    # Sort chunks by start offset and validate contiguity.
    sorted_chunks = sorted(chunks, key=lambda c: c[1]["start"])
    prev_end = 0
    for _chunk_id, chunk in sorted_chunks:
        start = chunk["start"]
        end = chunk["end"]
        assert start == prev_end, (
            f"chunks must be contiguous: expected start={prev_end}, got {start}"
        )
        assert end <= len(raw), f"chunk end {end} exceeds raw length {len(raw)}"
        assert end - start <= OUTPUT_CHUNK_MAX_BYTES
        # Validate value against canonical decode of the exact raw slice.
        expected_value = pg_safe_decode(raw[start:end])
        assert chunk["value"] == expected_value, (
            f"chunk [{start}:{end}] value mismatch: "
            f"expected {expected_value!r}, got {chunk['value']!r}"
        )
        prev_end = end

    # Validate the live tail window against the exact raw slice.
    payload = read_root(jobs_db, job_id)
    output = payload.get("output") or {}
    stdout_window = output.get("stdout") or {}
    tail_start = stdout_window["start"]
    tail_end = stdout_window["end"]
    tail_text = stdout_window["tail"]

    assert tail_end == len(raw), f"tail_end={tail_end}, expected {len(raw)}"
    assert tail_start >= 0
    assert tail_start < tail_end
    # Tail must start at or after the last chunk end (archive margin overlap
    # is allowed by the protocol, but tail never shortens archived data).
    assert tail_start >= sorted_chunks[-1][1]["start"], (
        f"tail_start={tail_start} precedes last chunk start"
    )
    # Validate tail text against canonical decode of the exact raw slice.
    expected_tail = pg_safe_decode(raw[tail_start:tail_end])
    assert tail_text == expected_tail, (
        f"tail [{tail_start}:{tail_end}] value mismatch: "
        f"expected {expected_tail!r}, got {tail_text!r}"
    )
    # NUL must not appear in any persisted text.
    assert "\x00" not in tail_text


# ---------------------------------------------------------------------------
# Connectivity classifier: OperationalError on broken/closed connection
# ---------------------------------------------------------------------------


def test_operational_error_on_broken_connection_treated_as_connectivity(
    jobs_db: str, pg_cluster: _pg.PgCluster, tmp_path: Path
) -> None:
    """An OperationalError with non-08 SQLSTATE on a broken connection is outage.

    Real server shutdowns/failovers can surface as OperationalError with a
    non-08 SQLSTATE while the psycopg connection is already broken.  The
    classifier must treat this as connectivity.
    """
    target = insert_process_job(
        jobs_db,
        str(tmp_path),
        [
            sys.executable,
            "-c",
            "import sys, time; sys.stdout.write('x\\n'); sys.stdout.flush(); time.sleep(30)",
        ],
    )

    failover_msg = "server closed the connection unexpectedly"

    def _raise_broken(
        _conn: JobsConnection, job: ActiveJob, *_args: object, **_kwargs: object
    ) -> bool:
        if job.id == target:
            exc = psycopg.OperationalError(failover_msg)
            exc.sqlstate = "57P01"  # admin_shutdown, not class 08
            raise exc
        return True

    supervisor = Supervisor(supervisor_settings(), make_database_config(pg_cluster))
    thread = threading.Thread(target=supervisor.run, daemon=True)
    try:
        with (
            patch("psycopg.Connection.broken", new_callable=PropertyMock, return_value=True),
            patch("lubko.worker.publish_output", side_effect=_raise_broken),
        ):
            thread.start()
            wait_until(lambda: supervisor.conn is None, timeout=10.0)
            assert supervisor.conn is None
    finally:
        supervisor.request_shutdown()
        thread.join(timeout=30)
        _kill_leftover_groups(jobs_db)


def test_quarantine_convergence_no_active_leak(
    jobs_db: str, pg_cluster: _pg.PgCluster, tmp_path: Path
) -> None:
    """After quarantine + process group death, the job is cleaned from the active registry.

    Proves no repeated poison publication/finalization loop and no active leak.
    """
    target = insert_process_job(
        jobs_db,
        str(tmp_path),
        [
            sys.executable,
            "-c",
            "import sys, time; sys.stdout.write('x\\n'); sys.stdout.flush(); time.sleep(30)",
        ],
    )

    inject_msg = "convergence regression: persistent data error"

    def _raise_data_error(
        _conn: JobsConnection, job: ActiveJob, *_args: object, **_kwargs: object
    ) -> bool:
        if job.id == target:
            exc = psycopg.DataError(inject_msg)
            exc.sqlstate = "22P05"
            raise exc
        return True

    supervisor = Supervisor(supervisor_settings(), make_database_config(pg_cluster))
    thread = threading.Thread(target=supervisor.run, daemon=True)
    try:
        with patch("lubko.worker.publish_output", side_effect=_raise_data_error):
            thread.start()
            wait_until(lambda: read_status(jobs_db, target) == "failed")
            time.sleep(2.0)
            assert target not in supervisor.active, (
                "quarantined job must be removed from active registry after cleanup"
            )
    finally:
        supervisor.request_shutdown()
        thread.join(timeout=30)
        _kill_leftover_groups(jobs_db)


def test_quarantine_pending_excluded_from_publish_and_finalize(
    jobs_db: str, pg_cluster: _pg.PgCluster, tmp_path: Path
) -> None:
    """Regression: publish_output is not re-entered while quarantine_pending backoff.

    When _quarantine_job fails and quarantine_pending is set, _publish_all
    and _finalize_completed must not call publish_output for that job.
    Only _cleanup_quarantined_jobs may retry its safe terminalization.
    """
    target = insert_process_job(
        jobs_db,
        str(tmp_path),
        [
            sys.executable,
            "-c",
            (
                "import sys, time\n"
                "for _ in range(5):\n"
                "    sys.stdout.write('tick\\n')\n"
                "    sys.stdout.flush()\n"
                "    time.sleep(0.1)\n"
            ),
        ],
    )
    other = insert_process_job(
        jobs_db,
        str(tmp_path),
        [
            sys.executable,
            "-c",
            "import sys, time; sys.stdout.write('ok\\n'); sys.stdout.flush(); time.sleep(30)",
        ],
    )

    publish_calls: dict[UUID, int] = {}

    def _publish_side_effect(
        _conn: JobsConnection, job: ActiveJob, *_args: object, **_kwargs: object
    ) -> bool:
        count = publish_calls.get(job.id, 0)
        publish_calls[job.id] = count + 1
        if job.id == target and count == 0:
            exc = psycopg.DataError("quarantine pending regression injection")
            exc.sqlstate = "22P05"
            raise exc
        return True

    def _quarantine_always_pending(_conn: JobsConnection, _job_id: UUID, _reason: str) -> bool:
        return False

    supervisor = Supervisor(supervisor_settings(), make_database_config(pg_cluster))
    thread = threading.Thread(target=supervisor.run, daemon=True)
    try:
        with (
            patch("lubko.worker.publish_output", side_effect=_publish_side_effect),
            patch("lubko.worker._quarantine_job", side_effect=_quarantine_always_pending),
        ):
            thread.start()
            wait_until(lambda: read_status(jobs_db, other) == "running")
            wait_until(lambda: read_status(jobs_db, target) in {"running", "failed"})
            time.sleep(1.0)
            assert target in supervisor.active
            target_job = supervisor.active[target]
            assert target_job.quarantine_pending
            assert publish_calls.get(target, 0) == 1
    finally:
        supervisor.request_shutdown()
        thread.join(timeout=30)
        _kill_leftover_groups(jobs_db)


def test_start_gate_missing_ticks_fails_closed(
    jobs_db: str, pg_cluster: _pg.PgCluster, tmp_path: Path
) -> None:
    """A gated start without obtainable start-time ticks never runs user code.

    Fault injection: ``proc_start_ticks`` returns ``None`` for the spawned job
    process (never for the supervisor itself). The start gate must fail closed:
    the job is finalized failed with no persisted exact identity, the exact
    gated group is terminated, and the user program is never executed.
    """
    sentinel = tmp_path / "ran"
    target = insert_process_job(
        jobs_db,
        str(tmp_path),
        [
            sys.executable,
            "-c",
            "import sys; open(sys.argv[1], 'w').close()",
            str(sentinel),
        ],
    )
    real = health.proc_start_ticks

    def _no_job_ticks(pid: int) -> int | None:
        if pid == os.getpid():
            return real(pid)
        return None

    supervisor = Supervisor(supervisor_settings(), make_database_config(pg_cluster))
    thread = threading.Thread(target=supervisor.run, daemon=True)
    try:
        with patch("lubko.worker.proc_start_ticks", side_effect=_no_job_ticks):
            thread.start()
            wait_until(lambda: read_status(jobs_db, target) == "failed")
            time.sleep(0.5)
            assert read_status(jobs_db, target) == "failed"
        state = read_root(jobs_db, target)["state"]
        assert state["status"] == "failed"
        assert state.get("process_pid") is None
        assert state.get("process_pgid") is None
        assert state.get("process_start_time_ticks") is None
        # The user program must NEVER have executed: no side effect survives.
        assert not sentinel.exists()
        assert target not in supervisor.active
    finally:
        supervisor.request_shutdown()
        thread.join(timeout=30)
        _kill_leftover_groups(jobs_db)


def test_start_gate_release_failure_fails_start_not_completion(
    jobs_db: str, pg_cluster: _pg.PgCluster, tmp_path: Path
) -> None:
    """A failed gate release aborts the start; it never looks like completion.

    Fault injection: ``release_gate`` reports failure to write the release byte.
    The job must be finalized failed (start failure), the exact gated group
    terminated and reaped, capture files cleaned, and the user program must
    never run — the failed start must not later surface as a normally completed
    user command.
    """
    sentinel = tmp_path / "ran"
    target = insert_process_job(
        jobs_db,
        str(tmp_path),
        [
            sys.executable,
            "-c",
            "import sys; open(sys.argv[1], 'w').close()",
            str(sentinel),
        ],
    )
    supervisor = Supervisor(supervisor_settings(), make_database_config(pg_cluster))
    thread = threading.Thread(target=supervisor.run, daemon=True)
    try:
        with patch("lubko.worker.release_gate", return_value=False):
            thread.start()
            wait_until(lambda: read_status(jobs_db, target) == "failed")
            time.sleep(0.5)
            # Still failed: a failed release must never become a completion.
            assert read_status(jobs_db, target) == "failed"
        payload = read_root(jobs_db, target)
        state = payload["state"]
        assert state["status"] == "failed"
        result = payload.get("result", {})
        assert "gated start" in str(result.get("stderr", ""))
        # The user program must NEVER have executed and nothing may leak.
        assert not sentinel.exists()
        assert target not in supervisor.active
    finally:
        supervisor.request_shutdown()
        thread.join(timeout=30)
        _kill_leftover_groups(jobs_db)


def test_shutdown_withholds_drain_sentinel_when_group_proof_fails(
    jobs_db: str, pg_cluster: _pg.PgCluster, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No clean-drain sentinel is emitted while an exact group is unproven.

    Fault injection: ``group_has_members`` reports the running job's exact
    group as permanently member-occupied, so the final post-SIGKILL proof
    fails. Shutdown must NOT write the drain sentinel and must retain the job
    (never terminalize/untrack it), keeping its running row exactly
    recoverable by emergency recovery.
    """
    job_id = insert_job(jobs_db, str(tmp_path), "sleep 300")
    settings = supervisor_settings(f"sentinel-{uuid4().hex[:8]}")
    with supervisor_running(settings, make_database_config(pg_cluster), jobs_db) as supervisor:
        wait_until(lambda: read_status(jobs_db, job_id) == "running")
        payload = read_root(jobs_db, job_id)
        pgid = int(payload["state"]["process_pgid"])
        real_has_members = group_has_members

        def _survives(pgid_arg: int) -> bool:
            if pgid_arg == pgid:
                return True
            return real_has_members(pgid_arg)

        monkeypatch.setattr(worker_module, "group_has_members", _survives)
        supervisor.request_shutdown()

    # No clean-drain sentinel may exist while the exact group remains.
    assert not drain_sentinel_path(settings.worker_incarnation).exists()
    # The job was retained: its row stays recoverable, never terminalized.
    assert read_status(jobs_db, job_id) == "running"
