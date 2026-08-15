"""Tests for the token-scoped readiness proof (issue #30).

These tests cover the atomic marker lifecycle, the staleness guarantee (a
stale marker can never satisfy a different candidate token), the capability
detection used for staged compatibility, and the worker writing the marker
only after a schema-verified PostgreSQL connection.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import TYPE_CHECKING, Final

import psycopg

from lubko.config import DatabaseConfig
from lubko.readiness import (
    LIFECYCLE_MARKER_VAR,
    candidate_supports_readiness,
    readiness_marker_path,
    readiness_proven,
    remove_readiness_marker,
    write_readiness_marker,
)
from lubko.worker import Settings, Supervisor

if TYPE_CHECKING:
    import pytest

    from tests import _pg

TOKEN: Final = "a1b2c3d4e5f60718293a4b5c"  # ruff: ignore[hardcoded-password-string] - test token
OTHER_TOKEN: Final = "ffffffff000000001111222233334444"  # ruff: ignore[hardcoded-password-string] - test token
BASELINE_MIGRATION: Final = (
    Path(__file__).resolve().parent.parent / "migrations" / "0001_two_column_protocol.sql"
)


def worker_settings() -> Settings:
    """Build fast-timing worker settings for readiness integration tests.

    Returns:
        Settings with fast lease and polling timing.
    """
    return Settings(
        worker_id="readiness-test",
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


def test_readiness_marker_round_trip() -> None:
    """A written marker proves readiness and can be removed."""
    write_readiness_marker(TOKEN)

    assert readiness_proven(TOKEN)
    marker = json.loads(readiness_marker_path(TOKEN).read_text(encoding="utf-8"))
    assert marker["token"] == TOKEN
    assert marker["pid"] == os.getpid()

    remove_readiness_marker(TOKEN)

    assert not readiness_proven(TOKEN)


def test_remove_missing_marker_is_noop() -> None:
    """Removing a marker that does not exist is harmless."""
    remove_readiness_marker(OTHER_TOKEN)
    assert not readiness_proven(OTHER_TOKEN)


def test_stale_marker_never_satisfies_another_candidate() -> None:
    """A marker for one candidate token cannot prove a different candidate."""
    write_readiness_marker(TOKEN)

    assert readiness_proven(TOKEN)
    assert not readiness_proven(OTHER_TOKEN)

    remove_readiness_marker(TOKEN)


def test_marker_content_token_is_required() -> None:
    """A marker file at the token path must name the exact token."""
    path = readiness_marker_path(TOKEN)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"v": 1, "token": OTHER_TOKEN, "pid": os.getpid()}) + "\n")

    assert not readiness_proven(TOKEN)


def test_malformed_marker_is_not_proven() -> None:
    """A corrupt marker file never counts as a readiness proof."""
    path = readiness_marker_path(TOKEN)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("not json\n")

    assert not readiness_proven(TOKEN)


def test_candidate_supports_readiness_detects_protocol_module(tmp_path: Path) -> None:
    """Capability detection keys on the readiness module in the candidate tree."""
    with_module = tmp_path / "with"
    (with_module / "src" / "lubko").mkdir(parents=True)
    (with_module / "src" / "lubko" / "readiness.py").write_text("# x\n", encoding="utf-8")

    without_module = tmp_path / "without"
    without_module.mkdir()

    assert candidate_supports_readiness(with_module) is True
    assert candidate_supports_readiness(without_module) is False


def test_worker_writes_marker_after_schema_verified_connect(
    pg_cluster: _pg.PgCluster,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The worker proves readiness only after a verified PostgreSQL connection.

    The token is placed in the worker environment; once ``_connect`` succeeds
    (connectivity plus the canonical schema invariant), the token-scoped marker
    must exist and be proven.
    """
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state"))
    monkeypatch.setenv(LIFECYCLE_MARKER_VAR, TOKEN)
    with psycopg.connect(pg_cluster.conninfo()) as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS lubko")
        conn.execute(BASELINE_MIGRATION.read_text(encoding="utf-8"))
    supervisor = Supervisor(
        worker_settings(),
        DatabaseConfig(
            host=str(pg_cluster.socket_dir),
            port=pg_cluster.port,
            dbname="postgres",
            user="postgres",
            password="",
        ),
    )
    supervisor._connect()
    try:
        assert supervisor.conn is not None
        assert readiness_proven(TOKEN)
    finally:
        if supervisor.conn is not None:
            supervisor.conn.close()
        remove_readiness_marker(TOKEN)


def test_worker_skips_marker_without_token(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A worker without a lifecycle token never writes a readiness marker."""
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state"))
    monkeypatch.delenv(LIFECYCLE_MARKER_VAR, raising=False)
    supervisor = Supervisor(worker_settings(), DatabaseConfig("host", 1, "db", "user", ""))

    supervisor._write_readiness_marker()

    assert not readiness_proven(TOKEN)
