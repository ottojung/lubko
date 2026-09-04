"""Current application protocol-version invariants."""

import json
from pathlib import Path
from unittest.mock import MagicMock
from uuid import UUID, uuid4

import pytest

from lubko import worker
from lubko.protocol import (
    OUTPUT_CHUNK_MAX_BYTES,
    OUTPUT_TAIL_MAX_BYTES,
    ProtocolError,
    build_output_chunk_payload,
    build_payload,
    parse_chunk_payload,
    parse_payload,
)
from lubko.protocol_versioning import (
    CURRENT_PROTOCOL_VERSION,
    JobVersionDisposition,
    claim_version_predicate,
    classify_job_version,
    reaper_disposition,
    unsupported_version_diagnostic,
)
from lubko.worker import OutputStream, Settings


def _settings() -> Settings:
    """Build minimally valid worker settings.

    Returns:
        Valid settings for version-policy tests.
    """
    return Settings(
        worker_id="test-worker",
        poll_interval_seconds=1.0,
        process_poll_interval_seconds=0.1,
        cancel_grace_seconds=5.0,
        server="test-server",
    )


def test_current_protocol_is_v4() -> None:
    """This build implements exactly the real current protocol."""
    assert CURRENT_PROTOCOL_VERSION == 4


def test_unsupported_version_diagnostic_distinguishes_sides() -> None:
    """Retired and future generations both fail parser validation clearly."""
    assert unsupported_version_diagnostic(4) is None
    below = unsupported_version_diagnostic(3)
    above = unsupported_version_diagnostic(5)
    assert below is not None
    assert "below" in below
    assert above is not None
    assert "above" in above


def test_claim_decision_is_exactly_current_version() -> None:
    """The worker claims only the protocol this build actually implements."""
    assert classify_job_version(4) is JobVersionDisposition.CLAIMABLE
    assert classify_job_version(3) is JobVersionDisposition.FAIL_CLOSED
    assert classify_job_version(5) is JobVersionDisposition.FAIL_CLOSED
    fragment, params = claim_version_predicate()
    assert "BETWEEN" not in fragment
    assert "::int" not in fragment
    assert "jsonb_typeof" in fragment
    assert "= %(protocol_version)s::text" in fragment
    assert params == {"protocol_version": 4}


def test_parser_accepts_v4_and_rejects_future_version() -> None:
    """Application parsing is exact-version and independent of PostgreSQL."""
    payload = build_payload(server="s", cwd="/srv", process=["echo"])
    assert parse_payload(payload).version == 4
    payload["v"] = 5
    with pytest.raises(ProtocolError, match="above"):
        parse_payload(payload)


def test_chunk_parser_accepts_v4_and_rejects_future_version() -> None:
    """Output chunks follow the same exact application-version rule."""
    chunk = build_output_chunk_payload(
        server="s",
        thread=uuid4(),
        stream="stdout",
        sequence=0,
        start=0,
        end=2,
        value="ok",
        previous=None,
    )
    assert parse_chunk_payload(chunk).value == "ok"
    chunk["v"] = 5
    with pytest.raises(ProtocolError, match="above"):
        parse_chunk_payload(chunk)


@pytest.mark.parametrize("version", ["4", 4.5, True, None])
def test_parser_rejects_non_integer_version(version: object) -> None:
    """Malformed version values are never coerced into v4."""
    raw = build_payload(server="s", cwd="/srv", process=["echo"])
    raw["v"] = version
    with pytest.raises(ProtocolError, match="integer"):
        parse_payload(raw)


def test_parser_rejects_missing_version() -> None:
    """The runtime parser is authoritative for the required application version."""
    raw = build_payload(server="s", cwd="/srv", process=["echo"])
    raw.pop("v")
    with pytest.raises(ProtocolError, match="v"):
        parse_payload(raw)


def test_reaper_fails_only_retired_versions() -> None:
    """An old binary leaves future work pending for a potentially newer daemon."""
    assert reaper_disposition(3) is JobVersionDisposition.FAIL_CLOSED
    assert reaper_disposition(4) is JobVersionDisposition.CLAIMABLE
    assert reaper_disposition(5) is JobVersionDisposition.CLAIMABLE


def test_worker_reaper_leaves_future_versions_for_newer_binaries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Worker-level reaping never terminalizes locally unknown future work."""
    conn = MagicMock()
    cursor = MagicMock()
    conn.transaction.return_value.__enter__.return_value = None
    conn.cursor.return_value.__enter__.return_value = cursor
    cursor.fetchall.return_value = [(UUID(int=5), "5", "number")]
    fail_unsupported_job = MagicMock(return_value=True)
    monkeypatch.setattr(worker, "fail_unsupported_job", fail_unsupported_job)
    assert worker.reap_unsupported_jobs(conn, _settings(), limit=10) == []
    fail_unsupported_job.assert_not_called()


def test_chunk_emission_preserves_root_version(tmp_path: Path) -> None:
    """Every emitted chunk carries the current root command version."""
    content = b"x" * (OUTPUT_CHUNK_MAX_BYTES + OUTPUT_TAIL_MAX_BYTES + 200)
    path = tmp_path / "stdout"
    path.write_bytes(content)
    stream = OutputStream(path=path, spool_start=0, archived_upto=0, last_chunk=None, sequence=0)
    chunks, _archived, _last, _seq = worker._plan_chunks(
        UUID(int=1), "stdout", stream, len(content), "server", version=4
    )
    assert chunks
    assert all(json.loads(payload)["v"] == 4 for _chunk_id, payload in chunks)


def test_worker_reaper_targets_retired_and_malformed_versions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The bounded pass fails safe rows without letting current/future work starve them."""
    conn = MagicMock()
    cursor = MagicMock()
    conn.transaction.return_value.__enter__.return_value = None
    conn.cursor.return_value.__enter__.return_value = cursor
    cursor.fetchall.return_value = [
        (UUID(int=3), "3", "number"),
        (UUID(int=30), "3.0", "number"),
        (UUID(int=31), "bad", "string"),
        (UUID(int=32), None, None),
        # Defensive future row: SQL should exclude it, and Python still refuses
        # to terminalize it if a test double or future query change returns it.
        (UUID(int=5), "5", "number"),
    ]
    failed: list[UUID] = []

    def fail(_conn: object, job_id: UUID, _diagnostic: str, *, server: str) -> bool:
        assert server == "test-server"
        failed.append(job_id)
        return True

    monkeypatch.setattr(worker, "fail_unsupported_job", fail)

    reaped = worker.reap_unsupported_jobs(conn, _settings(), limit=10)

    assert reaped == [UUID(int=3), UUID(int=30), UUID(int=31), UUID(int=32)]
    assert failed == reaped
    query, params = cursor.execute.call_args.args
    assert "CASE" in query
    assert "ORDER BY" in query
    assert "LIMIT %(limit)s" in query
    assert params["protocol_version"] == CURRENT_PROTOCOL_VERSION
