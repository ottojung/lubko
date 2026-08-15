"""Shared pytest fixtures enforcing deterministic process teardown.

The container runs under a real reaping PID 1 (tini), so this harness never
installs a reaper or calls ``waitpid(-1)``: after every test, any still-live
test-created process group is stopped by exact identity and the test fails
loudly if it leaked a process, and at the end of the session every tracked
group is asserted to be gone.
"""

from __future__ import annotations

import logging
import os
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING, Final

import pytest

from tests import _pg
from tests import _process_guard as guard

if TYPE_CHECKING:
    from collections.abc import Iterator

LOGGER = logging.getLogger(__name__)

REPO_ROOT: Final = Path(__file__).resolve().parent.parent
BASELINE_MIGRATION: Final = REPO_ROOT / "migrations" / "0001_two_column_protocol.sql"


@pytest.fixture(scope="session", autouse=True)
def _session_process_teardown() -> Iterator[None]:
    """Assert no test-created process survives the whole session.

    Yields:
        Nothing while the suite runs.
    """
    yield
    stopped = guard.teardown_tracked()
    guard.assert_no_live_tracked()
    if stopped:
        LOGGER.debug("session teardown stopped %d leaked process(es)", stopped)


@pytest.fixture(autouse=True)
def _process_teardown() -> Iterator[None]:
    """Own and deterministically stop every process a test creates.

    Yields:
        Nothing while one test runs.
    """
    yield
    stopped = guard.teardown_tracked()
    if stopped:
        LOGGER.debug("test teardown stopped %d leaked process(es)", stopped)


@pytest.fixture(scope="module")
def pg_cluster(tmp_path_factory: pytest.TempPathFactory) -> Iterator[_pg.PgCluster]:
    """Start an isolated PostgreSQL cluster, or skip when unavailable.

    Args:
        tmp_path_factory: Pytest temporary path factory.

    Yields:
        The running cluster.
    """
    binaries = _pg.postgres_binaries()
    if binaries is None:
        pytest.skip("PostgreSQL server binaries not available on this host")
    root = tmp_path_factory.mktemp("lubko-pg")
    data_dir = root / "data"
    socket_dir = root / "sock"
    socket_dir.mkdir()
    port = _pg.free_port()
    env = dict(os.environ)
    lib = _pg.postgres_lib_dir(Path(binaries["postgres"]).parent)
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
    current = _pg.PgCluster(binaries, data_dir, socket_dir, port, env)
    current.start()
    try:
        yield current
    finally:
        current.stop()


@pytest.fixture
def jobs_db(pg_cluster: _pg.PgCluster) -> str:
    """Apply the canonical baseline on a fresh ``lubko.jobs`` table.

    Args:
        pg_cluster: The running PostgreSQL cluster.

    Returns:
        A connection string usable with :func:`psycopg.connect`.
    """
    with __import__("psycopg").connect(pg_cluster.conninfo()) as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS lubko")
        conn.execute("DROP TABLE IF EXISTS lubko.jobs CASCADE")
        conn.execute(BASELINE_MIGRATION.read_text(encoding="utf-8"))
    return pg_cluster.conninfo()
