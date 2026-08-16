"""Integration tests for the nonblocking Lubko supervisor against real PostgreSQL.

These tests run the actual :class:`lubko.worker.Supervisor` in a thread against
an isolated PostgreSQL cluster and prove the concurrency, output, lease,
outage, shutdown, and multi-worker safety acceptance criteria with real process
groups and real row-locking.
"""

from __future__ import annotations

import itertools
import json
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
from contextlib import contextmanager, suppress
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final, cast
from uuid import UUID, uuid4

import psycopg
import pytest

from lubko.config import DatabaseConfig
from lubko.worker import (
    CHUNK_ORDER_INDEX_NAME,
    CHUNK_OWNER_INDEX_NAME,
    OUTPUT_STREAM_STDOUT,
    TYPE_AWARE_CONSTRAINT_NAME,
    ActiveJob,
    OutputStream,
    SchemaInvariantError,
    Settings,
    Supervisor,
    delete_job_and_chunks,
    group_has_members,
    publish_output,
    request_cancel,
    verify_jobs_table_invariant,
    verify_protocol_schema,
)
from tests import _process_guard as guard

if TYPE_CHECKING:
    from collections.abc import Iterator

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
