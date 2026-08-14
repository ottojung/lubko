"""Integration tests for stale running job recovery against real PostgreSQL.

These tests spin up a real, isolated PostgreSQL cluster (detected on PATH or in
the Guix store) and exercise the claim/heartbeat/recovery SQL directly, so the
row-locking and atomic ``jsonb_set`` semantics are exercised, never mocked. The
whole module skips when no server installation is available.
"""

from __future__ import annotations

import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
from contextlib import suppress
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final, cast

import psycopg
import pytest

from lubko.worker import (
    Settings,
    claim_job,
    group_has_members,
    recover_stale_jobs,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from uuid import UUID

REPO_ROOT: Final = Path(__file__).resolve().parent.parent
BASELINE_MIGRATION: Final = REPO_ROOT / "migrations" / "0001_two_column_protocol.sql"

POSTGRES_NAMES: Final = ("initdb", "postgres", "pg_ctl")


class _Cluster:
    """A running isolated PostgreSQL server on a unix socket."""

    def __init__(
        self,
        binaries: dict[str, str],
        data_dir: Path,
        socket_dir: Path,
        port: int,
        env: dict[str, str],
    ) -> None:
        self.binaries = binaries
        self.data_dir = data_dir
        self.socket_dir = socket_dir
        self.port = port
        self.env = env

    def conninfo(self) -> str:
        return f"host={self.socket_dir} port={self.port} dbname=postgres user=postgres"

    def stop(self) -> None:
        subprocess.run(
            [
                self.binaries["pg_ctl"],
                "-D",
                str(self.data_dir),
                "-m",
                "immediate",
                "stop",
            ],
            env=self.env,
            check=False,
            capture_output=True,
        )


def _postgres_binaries() -> dict[str, str] | None:
    """Locate a usable PostgreSQL server installation.

    Returns:
        A mapping of binary name to path, or ``None`` when unavailable.
    """
    resolved: dict[str, str] = {}
    for name in POSTGRES_NAMES:
        path = shutil.which(name)
        if path is None:
            break
        resolved[name] = path
    else:
        return resolved
    for store in Path("/gnu/store").glob("*postgresql-*/bin"):
        candidate = {name: str(store / name) for name in POSTGRES_NAMES}
        if all(Path(path).is_file() for path in candidate.values()):
            return candidate
    return None


def _postgres_lib_dir(binary_dir: Path) -> str | None:
    """Return a sibling ``lib`` directory for ``LD_LIBRARY_PATH``, if any.

    Args:
        binary_dir: Directory containing the PostgreSQL binaries.

    Returns:
        The library directory, or ``None`` when the loader already finds it.
    """
    lib = binary_dir.parent / "lib"
    if lib.is_dir():
        return str(lib)
    return None


def _free_port() -> int:
    """Return an ephemeral TCP port that is currently free.

    Returns:
        A port number.
    """
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@pytest.fixture(scope="module")
def cluster(tmp_path_factory: pytest.TempPathFactory) -> Iterator[_Cluster]:
    """Start an isolated PostgreSQL cluster, or skip when unavailable.

    Args:
        tmp_path_factory: Pytest temporary path factory.

    Yields:
        The running cluster.
    """
    binaries = _postgres_binaries()
    if binaries is None:
        pytest.skip("PostgreSQL server binaries not available on this host")
    root = tmp_path_factory.mktemp("lubko-pg")
    data_dir = root / "data"
    socket_dir = root / "sock"
    socket_dir.mkdir()
    port = _free_port()
    env = dict(os.environ)
    lib = _postgres_lib_dir(Path(binaries["postgres"]).parent)
    if lib is not None:
        env["LD_LIBRARY_PATH"] = lib
    subprocess.run(
        [
            binaries["initdb"],
            "-D",
            str(data_dir),
            "-U",
            "postgres",
            "--auth=trust",
        ],
        env=env,
        check=True,
        capture_output=True,
    )
    subprocess.run(
        [
            binaries["pg_ctl"],
            "-D",
            str(data_dir),
            "-o",
            f"-p {port} -k {socket_dir}",
            "-l",
            str(root / "server.log"),
            "start",
        ],
        env=env,
        check=True,
        capture_output=True,
    )
    current = _Cluster(binaries, data_dir, socket_dir, port, env)
    try:
        yield current
    finally:
        current.stop()


@pytest.fixture
def db(cluster: _Cluster) -> str:
    """Apply the baseline migration on a fresh ``lubko.jobs`` table.

    Args:
        cluster: The running PostgreSQL cluster.

    Returns:
        A connection string usable with :func:`psycopg.connect`.
    """
    with psycopg.connect(cluster.conninfo()) as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS lubko")
        conn.execute("DROP TABLE IF EXISTS lubko.jobs CASCADE")
        conn.execute(BASELINE_MIGRATION.read_text(encoding="utf-8"))
    return cluster.conninfo()


def make_settings(*, worker_id: str = "test-worker") -> Settings:
    """Build short-interval worker settings for integration tests.

    Args:
        worker_id: Worker identifier to record.

    Returns:
        Settings with fast lease timing.
    """
    return Settings(
        worker_id=worker_id,
        poll_interval_seconds=0.05,
        process_poll_interval_seconds=0.01,
        cancel_grace_seconds=0.5,
        max_output_bytes=64 * 1024,
        lease_duration_seconds=1.0,
        lease_refresh_interval_seconds=0.2,
        lease_recovery_interval_seconds=0.1,
    )


def insert_pending_job(conninfo: str, cwd: str, command: str) -> UUID:
    """Insert a protocol v1 pending command job.

    Args:
        conninfo: PostgreSQL connection string.
        cwd: Working directory for the job.
        command: Shell command to run.

    Returns:
        The job identifier.
    """
    payload = json.dumps({
        "v": 1,
        "type": "command",
        "request": {"cwd": cwd, "command": command},
        "state": {"status": "pending"},
    })
    with psycopg.connect(conninfo) as conn:
        row = conn.execute(
            "INSERT INTO lubko.jobs (payload) VALUES (%s) RETURNING id",
            (payload,),
        ).fetchone()
    assert row is not None
    return cast("UUID", row[0])


def insert_running_without_lease(conninfo: str, cwd: str, command: str) -> UUID:
    """Insert a running job that predates the lease protocol.

    Args:
        conninfo: PostgreSQL connection string.
        cwd: Working directory for the job.
        command: Shell command to run.

    Returns:
        The job identifier.
    """
    payload = json.dumps({
        "v": 1,
        "type": "command",
        "request": {"cwd": cwd, "command": command},
        "state": {
            "status": "running",
            "worker_id": "legacy-worker",
            "worker_incarnation": "legacy-incarnation",
            "started_at": "2026-01-01T00:00:00.000000Z",
        },
    })
    with psycopg.connect(conninfo) as conn:
        row = conn.execute(
            "INSERT INTO lubko.jobs (payload) VALUES (%s) RETURNING id",
            (payload,),
        ).fetchone()
    assert row is not None
    return cast("UUID", row[0])


def age_lease(conninfo: str, job_id: UUID) -> None:
    """Force a running job's lease deep into the past.

    Args:
        conninfo: PostgreSQL connection string.
        job_id: Job whose lease to age.
    """
    with psycopg.connect(conninfo) as conn:
        conn.execute(
            "UPDATE lubko.jobs\n"
            "SET payload = jsonb_set(\n"
            "    payload::jsonb,\n"
            "    '{state,lease_expires_at}',\n"
            "    to_jsonb('2020-01-01T00:00:00.000000Z'::text)\n"
            ")::text\n"
            "WHERE id = %s",
            (job_id,),
        )


def read_job(conninfo: str, job_id: UUID) -> dict[str, Any]:
    """Read and decode a job's payload.

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


def wait_until(predicate: Callable[[], bool], timeout: float = 15.0) -> None:
    """Poll until a predicate holds, raising if the deadline expires.

    Args:
        predicate: Condition to satisfy.
        timeout: Maximum seconds to wait.

    Raises:
        AssertionError: If the condition is not met within the timeout.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.05)
    msg = "condition not met within timeout"
    raise AssertionError(msg)


def test_stale_job_is_recovered_after_worker_disappears(
    db: str,
    tmp_path: Path,
) -> None:
    """A job whose worker disappeared is recovered and marked failed."""
    job_id = insert_pending_job(db, str(tmp_path), "echo hi")
    with psycopg.connect(db) as conn:
        claimed = claim_job(conn, make_settings())
    assert claimed is not None
    assert claimed.id == job_id

    age_lease(db, job_id)
    with psycopg.connect(db) as conn:
        recovered = recover_stale_jobs(conn)

    assert [job for job, _payload in recovered] == [job_id]
    payload = read_job(db, job_id)
    state = payload["state"]
    assert state["status"] == "failed"
    assert state["finished_at"]
    assert state["recovered_at"]
    result = payload["result"]
    assert result["recovery_note"]
    assert "lease expired" in result["recovery_note"]
    assert "test-worker" in result["recovery_note"]


def test_non_stale_running_job_is_not_recovered(db: str, tmp_path: Path) -> None:
    """A genuinely live job with a fresh lease is never touched."""
    job_id = insert_pending_job(db, str(tmp_path), "sleep 30")
    with psycopg.connect(db) as conn:
        claimed = claim_job(conn, make_settings())
    assert claimed is not None
    assert claimed.id == job_id

    with psycopg.connect(db) as conn:
        recovered = recover_stale_jobs(conn)

    assert recovered == []
    assert read_job(db, job_id)["state"]["status"] == "running"


def test_running_job_without_lease_is_left_for_manual_repair(
    db: str,
    tmp_path: Path,
) -> None:
    """A pre-lease running job is never auto-recovered (no compatibility path)."""
    job_id = insert_running_without_lease(db, str(tmp_path), "sleep 30")

    with psycopg.connect(db) as conn:
        recovered = recover_stale_jobs(conn)

    assert recovered == []
    assert read_job(db, job_id)["state"]["status"] == "running"


def test_recovery_is_atomic_across_concurrent_workers(
    db: str,
    tmp_path: Path,
) -> None:
    """Concurrent recovery passes recover each stale job exactly once."""
    jobs = [insert_pending_job(db, str(tmp_path), "echo hi") for _ in range(3)]
    for job_id in jobs:
        with psycopg.connect(db) as conn:
            claim_job(conn, make_settings(worker_id=f"worker-{job_id}"))
    for job_id in jobs:
        age_lease(db, job_id)

    recovered: list[list[UUID]] = []

    def recover() -> None:
        with psycopg.connect(db) as conn:
            row_ids = [job for job, _payload in recover_stale_jobs(conn)]
            recovered.append(row_ids)

    workers = [threading.Thread(target=recover) for _ in range(3)]
    for thread in workers:
        thread.start()
    for thread in workers:
        thread.join(timeout=15)

    all_recovered = [job for batch in recovered for job in batch]
    assert sorted(all_recovered) == sorted(jobs)
    for job_id in jobs:
        assert read_job(db, job_id)["state"]["status"] == "failed"


def terminate(proc: subprocess.Popen[bytes]) -> None:
    """Force-kill a worker process and reap it.

    Args:
        proc: The worker process.
    """
    proc.kill()
    proc.wait(timeout=10)


def wait_for_claimed_run(db: str, job_id: UUID, marker: Path) -> None:
    """Wait until a worker claims and starts executing a job.

    Args:
        db: PostgreSQL connection string.
        job_id: Job identifier.
        marker: File the job command writes on execution.
    """
    wait_until(lambda: read_job(db, job_id)["state"]["status"] == "running")
    wait_until(marker.exists)


def wait_for_recovery(db: str, job_id: UUID, marker: Path) -> None:
    """Wait for automatic recovery and assert a clean, single execution.

    Args:
        db: PostgreSQL connection string.
        job_id: Job identifier.
        marker: File the job command writes on execution.
    """
    wait_until(
        lambda: read_job(db, job_id)["state"]["status"] == "failed",
        timeout=30.0,
    )
    payload = read_job(db, job_id)
    assert payload["state"]["status"] == "failed"
    assert "lease expired" in payload["result"]["recovery_note"]
    assert marker.read_text(encoding="utf-8").splitlines() == ["ran"]


def test_worker_crash_is_recovered_by_replacement_worker(
    db: str,
    cluster: _Cluster,
    tmp_path: Path,
) -> None:
    """An end-to-end worker crash is recovered without duplicate execution."""
    marker = tmp_path / "runs"
    command = f"echo ran >> {marker}; sleep 30"
    job_id = insert_pending_job(db, str(tmp_path), command)

    conf = tmp_path / "database.conf"
    conf.write_text(
        f"host={cluster.socket_dir}\n"
        f"port={cluster.port}\n"
        "dbname=postgres\n"
        "user=postgres\n"
        "password=local-trust\n"
    )
    conf.chmod(0o600)
    env = dict(os.environ)
    env["LUBKO_DATABASE_CONFIG"] = str(conf)
    env["LUBKO_POLL_INTERVAL_SECONDS"] = "0.05"
    env["LUBKO_PROCESS_POLL_INTERVAL_SECONDS"] = "0.01"
    env["LUBKO_LEASE_DURATION_SECONDS"] = "1.0"
    env["LUBKO_LEASE_REFRESH_INTERVAL_SECONDS"] = "0.2"
    env["LUBKO_LEASE_RECOVERY_INTERVAL_SECONDS"] = "0.1"

    def spawn_worker() -> subprocess.Popen[bytes]:
        return subprocess.Popen(
            [sys.executable, "-m", "lubko.worker"],
            cwd=REPO_ROOT,
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    first = spawn_worker()
    replacement: subprocess.Popen[bytes] | None = None
    try:
        wait_for_claimed_run(db, job_id, marker)
        terminate(first)
        replacement = spawn_worker()
        wait_for_recovery(db, job_id, marker)
    finally:
        if replacement is not None and replacement.poll() is None:
            terminate(replacement)
        payload = read_job(db, job_id)
        pgid = (payload.get("state") or {}).get("process_pgid")
        if pgid is not None and group_has_members(pgid):
            with suppress(ProcessLookupError):
                os.killpg(pgid, signal.SIGKILL)


def test_claim_records_lease_and_incarnation_into_the_future(
    db: str,
    tmp_path: Path,
) -> None:
    """Claiming writes a future lease and the worker incarnation."""
    job_id = insert_pending_job(db, str(tmp_path), "sleep 30")
    with psycopg.connect(db) as conn:
        claimed = claim_job(conn, make_settings())
    assert claimed is not None

    state = read_job(db, job_id)["state"]
    assert state["worker_incarnation"]
    lease = state["lease_expires_at"]
    with psycopg.connect(db) as conn:
        row = conn.execute(
            "SELECT to_char(now() at time zone 'utc', %(fmt)s::text)",
            {"fmt": 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'},
        ).fetchone()
    assert row is not None
    assert lease > row[0]
