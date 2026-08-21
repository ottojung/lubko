"""Regressions for the PostgreSQL cluster teardown exact-identity safety.

The shared ``PgCluster`` harness must never force-signal a postmaster PID
whose recorded start ticks no longer match (kernel reuse) or whose identity
was never verifiable: teardown fails closed instead.
"""

from __future__ import annotations

import subprocess
from typing import TYPE_CHECKING

import pytest

from tests import _pg

if TYPE_CHECKING:
    from pathlib import Path

SLEEP_BIN: str = "/bin/sleep"


@pytest.fixture
def fake_cluster(
    tmp_path: Path,
) -> tuple[_pg.PgCluster, Path]:
    """Build a ``PgCluster`` shell without starting a real server.

    Allows deterministic manipulation of the recorded postmaster identity.

    Args:
        tmp_path: Pytest temporary directory.

    Returns:
        The harness instance and a pidfile path for ``start()`` tests.
    """
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    cluster = _pg.PgCluster(
        binaries={"pg_ctl": "/nonexistent/pg_ctl"},
        data_dir=data_dir,
        socket_dir=tmp_path / "sock",
        port=0,
        env={},
    )
    return cluster, data_dir / "postmaster.pid"


def _spawn_sleeper() -> subprocess.Popen[bytes]:
    """Spawn a long-lived sleep process.

    Returns:
        The unregistered process.
    """
    return subprocess.Popen(
        [SLEEP_BIN, "300"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def test_start_records_exact_postmaster_identity(
    monkeypatch: pytest.MonkeyPatch,
    fake_cluster: tuple[_pg.PgCluster, Path],
) -> None:
    """``start()`` records the postmaster's PID and current start ticks."""
    cluster, pidfile = fake_cluster
    proc = _spawn_sleeper()
    try:
        pidfile.write_text(f"{proc.pid}\n", encoding="utf-8")
        ticks = _pg.proc_start_ticks(proc.pid)
        assert ticks is not None

        def fake_run(*_args: object, **_kwargs: object) -> None:
            return None

        monkeypatch.setattr("subprocess.run", fake_run)
        cluster.start()
        assert cluster.postmaster_pid == proc.pid
        assert cluster.postmaster_start_ticks == ticks
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=10)


def test_stop_refuses_pg_ctl_before_signalling_stale_identity(
    monkeypatch: pytest.MonkeyPatch,
    fake_cluster: tuple[_pg.PgCluster, Path],
) -> None:
    """``pg_ctl stop`` is never invoked when the recorded identity is stale.

    ``pg_ctl stop`` signals whatever occupies the recorded postmaster PID,
    so a live occupant with mismatched recorded ticks must be refused
    before pg_ctl runs at all.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
        fake_cluster: The harness shell and pidfile path.
    """
    cluster, _pidfile = fake_cluster
    innocent = _spawn_sleeper()
    invoked: list[list[str]] = []

    def record_run(command: list[str], **_kwargs: object) -> None:
        invoked.append(command)

    try:
        ticks = _pg.proc_start_ticks(innocent.pid)
        assert ticks is not None
        cluster.postmaster_pid = innocent.pid
        cluster.postmaster_start_ticks = ticks + 1
        monkeypatch.setattr("subprocess.run", record_run)
        with pytest.raises(AssertionError, match="refusing pg_ctl stop"):
            cluster.stop()
        assert not invoked
        assert _pg.process_live(innocent.pid)
    finally:
        if innocent.poll() is None:
            innocent.kill()
            innocent.wait(timeout=10)


def test_teardown_refuses_force_kill_on_stale_postmaster_identity(
    fake_cluster: tuple[_pg.PgCluster, Path],
) -> None:
    """A live PID with mismatched recorded ticks is never signalled."""
    cluster, _pidfile = fake_cluster
    innocent = _spawn_sleeper()
    try:
        ticks = _pg.proc_start_ticks(innocent.pid)
        assert ticks is not None
        # Simulate kernel PID reuse: recorded ticks differ from the live
        # occupant of the PID while pg_ctl could not stop it.
        cluster.postmaster_pid = innocent.pid
        cluster.postmaster_start_ticks = ticks + 1
        with pytest.raises(AssertionError, match="refusing to signal"):
            cluster.assert_postmaster_gone()
        assert _pg.process_live(innocent.pid)
    finally:
        if innocent.poll() is None:
            innocent.kill()
            innocent.wait(timeout=10)


def test_teardown_refuses_force_kill_without_recorded_ticks(
    fake_cluster: tuple[_pg.PgCluster, Path],
) -> None:
    """An unverifiable (ticks-less) identity authorizes no signal."""
    cluster, _pidfile = fake_cluster
    innocent = _spawn_sleeper()
    try:
        cluster.postmaster_pid = innocent.pid
        cluster.postmaster_start_ticks = None
        with pytest.raises(AssertionError, match="refusing to signal"):
            cluster.assert_postmaster_gone()
        assert _pg.process_live(innocent.pid)
    finally:
        if innocent.poll() is None:
            innocent.kill()
            innocent.wait(timeout=10)


def test_teardown_force_kills_verified_live_identity(
    monkeypatch: pytest.MonkeyPatch,
    fake_cluster: tuple[_pg.PgCluster, Path],
) -> None:
    """A verified live postmaster is still force-killed on teardown."""
    cluster, _pidfile = fake_cluster
    proc = _spawn_sleeper()
    try:
        ticks = _pg.proc_start_ticks(proc.pid)
        assert ticks is not None
        cluster.postmaster_pid = proc.pid
        cluster.postmaster_start_ticks = ticks

        def fake_run(*_args: object, **_kwargs: object) -> None:
            return None

        monkeypatch.setattr("subprocess.run", fake_run)
        monkeypatch.setattr("time.sleep", lambda _s: None)
        # process stays live until our KILL; identity matches → allowed.
        cluster.assert_postmaster_gone()
        assert not _pg.process_live(proc.pid)
        proc.wait(timeout=10)
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=10)
