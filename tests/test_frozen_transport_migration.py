"""Contract tests for the frozen PostgreSQL transport baseline."""

import re
from pathlib import Path

BASELINE = Path("migrations/0001_two_column_protocol.sql")


def _sql() -> str:
    return BASELINE.read_text(encoding="utf-8").lower()


def test_baseline_preserves_frozen_structural_contract() -> None:
    """The baseline keeps the permanent two-column queue contract."""
    sql = _sql()
    assert "create schema if not exists lubko" in sql
    assert "create table if not exists lubko.jobs" in sql
    assert "id uuid primary key default gen_random_uuid()" in sql
    assert "payload text not null" in sql


def test_baseline_keeps_payload_opaque_to_postgresql() -> None:
    """The frozen catalog never interprets application payload fields."""
    sql = _sql()
    assert "add constraint" not in sql
    assert "create index" not in sql
    assert "payload::" not in sql
    assert "check (" not in sql


def test_baseline_keeps_worker_access_optional_and_frozen() -> None:
    """Existing worker access is granted without creating credentials."""
    sql = _sql()
    assert "to_regrole('lubko_worker') is not null" in sql
    assert "grant usage on schema lubko to lubko_worker" in sql
    assert "grant select, insert, update, delete on table lubko.jobs to lubko_worker" in sql
    assert "create role" not in sql


def test_baseline_destructive_cleanup_is_idempotent() -> None:
    """Any compatibility cleanup remains safe when the baseline is rerun."""
    sql = _sql()
    assert re.search(r"\bdrop constraint\s+(?!if exists\b)", sql) is None
    assert re.search(r"\bdrop index\s+(?!if exists\b)", sql) is None
