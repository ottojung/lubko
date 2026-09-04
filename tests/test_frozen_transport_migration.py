"""Contract tests for the one-time frozen-transport catalog cutover."""

from pathlib import Path

BASELINE = Path("migrations/0001_two_column_protocol.sql")
LEGACY_CONSTRAINTS = (
    "jobs_payload_has_version",
    "jobs_payload_is_json_object",
    "jobs_payload_type_shape",
)
LEGACY_INDEXES = ("jobs_queue_idx", "jobs_chunk_owner_idx", "jobs_chunk_order_idx")


def _sql() -> str:
    return BASELINE.read_text(encoding="utf-8").lower()


def test_baseline_converges_known_legacy_payload_metadata() -> None:
    """Known pre-freeze Lubko objects are removed idempotently and exactly."""
    sql = _sql()
    for name in LEGACY_CONSTRAINTS:
        assert f"drop constraint if exists {name}" in sql
    for name in LEGACY_INDEXES:
        assert f"drop index if exists lubko.{name}" in sql


def test_baseline_preserves_frozen_structural_contract() -> None:
    """Cutover keeps the only permanent table shape and primary-key contract."""
    sql = _sql()
    assert "id uuid primary key default gen_random_uuid()" in sql
    assert "payload text not null" in sql
    assert "create table if not exists lubko.jobs" in sql
    assert "grant select, insert, update, delete on table lubko.jobs to lubko_worker" in sql


def test_baseline_adds_no_payload_aware_catalog_metadata() -> None:
    """The cutover removes old payload metadata without installing replacements."""
    sql = _sql()
    assert "add constraint" not in sql
    assert "create index" not in sql
    assert "payload::jsonb" not in sql
