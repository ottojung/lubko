"""Bounded mixed-version protocol upgrade invariants.

These are general, deterministic invariants of the reusable upgrade mechanism
in :mod:`lubko.protocol_versioning` and its integration into the protocol
parsers. They assert stable properties of the model (bounded window, negotiated
convergence, fail-closed on unsupported versions, preserved history) rather than
memorializing one specific cutover.

All tests run without a database.
"""

import json
import re
from pathlib import Path
from unittest.mock import MagicMock
from uuid import UUID, uuid4

import pytest

from lubko import protocol_versioning, worker
from lubko.config import load_worker_protocol_range
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
    DEFAULT_VERSION_RANGE,
    MAX_VERSION_SPAN,
    SUPPORTED_PROTOCOL_VERSIONS,
    JobVersionDisposition,
    ProtocolVersionError,
    ProtocolVersionRange,
    VersionNegotiationError,
    claim_version_predicate,
    classify_job_version,
    negotiate_submission_version,
    negotiate_version,
    reaper_disposition,
    unsupported_version_diagnostic,
)
from lubko.worker import OutputStream, Settings


def _settings(supported_protocol_range: ProtocolVersionRange | None = None) -> Settings:
    """Build a minimally valid :class:`Settings` for protocol-window tests.

    Returns:
        A :class:`Settings` instance with required fields populated and the
        default or supplied protocol window.
    """
    if supported_protocol_range is None:
        return Settings(
            worker_id="test-worker",
            poll_interval_seconds=1.0,
            process_poll_interval_seconds=0.1,
            cancel_grace_seconds=5.0,
            server="test-server",
        )
    return Settings(
        worker_id="test-worker",
        poll_interval_seconds=1.0,
        process_poll_interval_seconds=0.1,
        cancel_grace_seconds=5.0,
        server="test-server",
        supported_protocol_range=supported_protocol_range,
    )


def test_default_window_is_exactly_current_version() -> None:
    """A fresh install / converged fleet supports only the current version."""
    assert DEFAULT_VERSION_RANGE.min == CURRENT_PROTOCOL_VERSION
    assert DEFAULT_VERSION_RANGE.max == CURRENT_PROTOCOL_VERSION
    assert DEFAULT_VERSION_RANGE.span() == 0


@pytest.mark.parametrize(
    ("minimum", "maximum"),
    [(0, 4), (-1, 4), (5, 4), (4, 4 + MAX_VERSION_SPAN + 1)],
)
def test_window_rejects_unbounded_or_malformed(minimum: int, maximum: int) -> None:
    """A window must be positive, non-empty, and within the span bound."""
    with pytest.raises(ProtocolVersionError):
        ProtocolVersionRange(min=minimum, max=maximum)


def test_window_contains_is_inclusive() -> None:
    """`contains` is inclusive at both ends of the window."""
    window = ProtocolVersionRange(min=4, max=5)
    assert window.contains(4)
    assert window.contains(5)
    assert not window.contains(3)
    assert not window.contains(6)


def test_negotiate_picks_highest_common_version() -> None:
    """New submissions converge on the newest version the fleet shares."""
    server = ProtocolVersionRange(min=4, max=5)
    assert negotiate_version(client_min=4, client_max=5, server_range=server) == 5
    # Older client: the highest common version is still 4.
    assert negotiate_version(client_min=4, client_max=4, server_range=server) == 4
    # Newer client than the server understands: converge down to server's max.
    assert negotiate_version(client_min=5, client_max=6, server_range=server) == 5


def test_negotiate_fails_closed_without_overlap() -> None:
    """No shared version is a hard error, never a silent downgrade."""
    server = ProtocolVersionRange(min=4, max=4)
    with pytest.raises(VersionNegotiationError):
        negotiate_version(client_min=6, client_max=6, server_range=server)


def test_unsupported_version_diagnostic_distinguishes_sides() -> None:
    """Below the window is retired; above is unknown. Both fail closed."""
    window = ProtocolVersionRange(min=4, max=5)
    assert unsupported_version_diagnostic(4, window) is None
    assert unsupported_version_diagnostic(5, window) is None
    below = unsupported_version_diagnostic(3, window)
    assert below is not None
    assert "below" in below
    above = unsupported_version_diagnostic(6, window)
    assert above is not None
    assert "above" in above


def test_classify_job_version_drives_claim_decision() -> None:
    """A daemon claims only versions inside its window."""
    window = ProtocolVersionRange(min=4, max=5)
    assert classify_job_version(4, window) is JobVersionDisposition.CLAIMABLE
    assert classify_job_version(6, window) is JobVersionDisposition.FAIL_CLOSED


def test_claim_predicate_gates_window() -> None:
    """The claim SQL fragment bounds `v` to the supported window."""
    fragment, params = claim_version_predicate(ProtocolVersionRange(min=4, max=5))
    assert "BETWEEN" in fragment
    assert params == {"min_version": 4, "max_version": 5}


def test_parser_accepts_supported_window_and_rejects_outside() -> None:
    """A wider daemon window accepts in-window versions; outside fails closed."""
    payload = build_payload(server="s", cwd="/srv", process=["echo"], version=5)
    assert json.loads(json.dumps(payload))["v"] == 5
    parsed = parse_payload(json.dumps(payload), supported=ProtocolVersionRange(min=4, max=5))
    assert parsed.version == 5
    # The default single-version window refuses a version it cannot execute.
    with pytest.raises(ProtocolError, match="version"):
        parse_payload(json.dumps(payload))


def test_chunk_parser_honors_window_without_losing_history() -> None:
    """Immutable chunks keep their version and parse under a window."""
    chunk = build_output_chunk_payload(
        server="s",
        thread=uuid4(),
        stream="stdout",
        sequence=0,
        start=0,
        end=2,
        value="ok" * 100,
        previous=None,
        version=5,
    )
    assert chunk["v"] == 5
    parsed = parse_chunk_payload(json.dumps(chunk), supported=ProtocolVersionRange(min=4, max=5))
    assert parsed.value == "ok" * 100
    with pytest.raises(ProtocolError, match="version"):
        parse_chunk_payload(json.dumps(chunk))


def test_parser_still_rejects_non_integer_version() -> None:
    """`v` must be an integer; a string version fails closed."""
    raw = build_payload(server="s", cwd="/srv", process=["echo"])
    raw["v"] = "4"
    with pytest.raises(ProtocolError, match="integer"):
        parse_payload(raw)


def test_worker_refuses_window_it_cannot_parse() -> None:
    """Startup fails closed if the window includes an unparseable version."""
    # 6 is not in SUPPORTED_PROTOCOL_VERSIONS, so a [6, 6] window cannot be served
    # (the span is fine, but the version itself is unparseable by this build).
    with pytest.raises(ValueError, match="cannot parse"):
        _settings(supported_protocol_range=ProtocolVersionRange(min=6, max=6))


def test_worker_accepts_default_window() -> None:
    """The default window is always valid for this build."""
    settings = _settings()
    assert settings.supported_protocol_range == DEFAULT_VERSION_RANGE


# ---------------------------------------------------------------------------
# Representative shape-compatible v5: a [4, 5] rollout is genuinely executable.
# ---------------------------------------------------------------------------


def test_v5_is_a_supported_shape_compatible_version() -> None:
    """v5 is in the supported set, so a [4, 5] daemon window is executable."""
    assert 5 in SUPPORTED_PROTOCOL_VERSIONS
    # Every version a [4, 5] window spans must be parseable by this build.
    assert all(v in SUPPORTED_PROTOCOL_VERSIONS for v in range(4, 6))


def test_negotiation_converges_to_newest_supported_version() -> None:
    """A [4, 5] server makes new submissions stamp v5, not v4.

    This is the operational submission path: clients converge onto the newest
    version the fleet supports while older in-flight v4 jobs keep running on
    daemons that still advertise [4, 4].
    """
    assert negotiate_submission_version(ProtocolVersionRange(min=4, max=5)) == 5
    assert negotiate_submission_version(ProtocolVersionRange(min=4, max=4)) == 4


def test_claim_predicate_admits_the_widened_window() -> None:
    """A [4, 5] daemon's claim filter accepts v5 (and v4) jobs."""
    _fragment, params = claim_version_predicate(ProtocolVersionRange(min=4, max=5))
    assert params == {"min_version": 4, "max_version": 5}


# ---------------------------------------------------------------------------
# Fleet-wide fail-closed reaper: stranded pending jobs cannot remain forever.
# ---------------------------------------------------------------------------


def test_reaper_fails_closed_only_retired_versions() -> None:
    """The reaper never destroys a job a newer binary may understand."""
    old_daemon = ProtocolVersionRange(min=4, max=4)
    new_daemon = ProtocolVersionRange(min=4, max=5)

    assert reaper_disposition(4, old_daemon) is JobVersionDisposition.CLAIMABLE
    assert reaper_disposition(5, new_daemon) is JobVersionDisposition.CLAIMABLE
    assert reaper_disposition(5, old_daemon) is JobVersionDisposition.CLAIMABLE
    assert reaper_disposition(3, old_daemon) is JobVersionDisposition.FAIL_CLOSED
    assert reaper_disposition(6, new_daemon) is JobVersionDisposition.CLAIMABLE
    assert reaper_disposition(6, old_daemon) is JobVersionDisposition.CLAIMABLE


def test_reaper_is_conservative_across_binary_build_ceilings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A local build ceiling never becomes destructive fleet authority."""
    old_window = ProtocolVersionRange(min=4, max=4)
    for build_ceiling, future_version in ((4, 5), (5, 6)):
        monkeypatch.setattr(protocol_versioning, "_MAX_SUPPORTED_VERSION", build_ceiling)
        assert reaper_disposition(future_version, old_window) is JobVersionDisposition.CLAIMABLE


def test_worker_reaper_leaves_future_versions_for_newer_binaries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The worker-level reaper never terminalizes locally unknown future work."""
    conn = MagicMock()
    cursor = MagicMock()
    conn.transaction.return_value.__enter__.return_value = None
    conn.cursor.return_value.__enter__.return_value = cursor
    fail_unsupported_job = MagicMock(return_value=True)
    monkeypatch.setattr(worker, "fail_unsupported_job", fail_unsupported_job)

    old_window = ProtocolVersionRange(min=4, max=4)
    settings = _settings(old_window)
    for build_ceiling, future_version in ((4, 5), (5, 6)):
        monkeypatch.setattr(protocol_versioning, "_MAX_SUPPORTED_VERSION", build_ceiling)
        cursor.fetchall.return_value = [(UUID(int=future_version), future_version)]
        assert worker.reap_unsupported_jobs(conn, settings, limit=10) == []

    fail_unsupported_job.assert_not_called()


# ---------------------------------------------------------------------------
# Output chunks preserve their root job's protocol version.
# ---------------------------------------------------------------------------


def test_chunk_emission_preserves_root_version(tmp_path: Path) -> None:
    """Every chunk a worker emits carries the root command's version.

    The worker plans chunks from the active job's version, so chunk history can
    never drift to a different protocol generation than its root.
    """
    content = b"x" * (OUTPUT_CHUNK_MAX_BYTES + OUTPUT_TAIL_MAX_BYTES + 200)
    path = tmp_path / "stdout"
    path.write_bytes(content)
    stream = OutputStream(path=path, spool_start=0, archived_upto=0, last_chunk=None, sequence=0)
    chunks, _archived, _last, _seq = worker._plan_chunks(
        UUID(int=1), "stdout", stream, len(content), "server", version=5
    )
    assert chunks, "expected at least one archived chunk"
    for _chunk_id, payload in chunks:
        assert json.loads(payload)["v"] == 5


def test_parser_rejects_fractional_version() -> None:
    """A fractional JSON version is not rounded into the window; it fails closed."""
    raw = build_payload(server="s", cwd="/srv", process=["echo"])
    raw["v"] = 4.5
    with pytest.raises(ProtocolError, match="integer"):
        parse_payload(raw)


# ---------------------------------------------------------------------------
# Durable configuration of the supported window.
# ---------------------------------------------------------------------------


def test_worker_config_defaults_to_current_window(tmp_path: Path) -> None:
    """Without explicit bounds the worker config yields the current window."""
    path = tmp_path / "worker.conf"
    path.write_text("server = alpha\n")
    path.chmod(0o600)
    assert load_worker_protocol_range(path) == DEFAULT_VERSION_RANGE


def test_worker_config_loads_explicit_window(tmp_path: Path) -> None:
    """The configured [4, 5] window is loaded from the worker config file."""
    path = tmp_path / "worker.conf"
    path.write_text("server = alpha\nprotocol_min_version = 4\nprotocol_max_version = 5\n")
    path.chmod(0o600)
    assert load_worker_protocol_range(path) == ProtocolVersionRange(min=4, max=5)


@pytest.mark.parametrize("body", ["protocol_max_version = 9\n", "protocol_max_version = abc\n"])
def test_worker_config_rejects_invalid_window(tmp_path: Path, body: str) -> None:
    """A malformed window in the worker config fails closed at load time."""
    path = tmp_path / "worker.conf"
    path.write_text(f"server = alpha\n{body}")
    path.chmod(0o600)
    with pytest.raises(ValueError, match="protocol"):
        load_worker_protocol_range(path)


# ---------------------------------------------------------------------------
# Migration 0005: total, fail-closed version gate AND retained-history range.
# ---------------------------------------------------------------------------


def test_migration_0005_version_validation_is_total_and_fail_closed() -> None:
    """The window constraint rejects every malformed `v`, never silently passing.

    The version check must be total (it can never evaluate to SQL NULL, which a
    CHECK would accept) and must never rely on an unguarded ``::int`` cast (which
    would raise on an oversized value). Concretely it must reject, for every
    command/output_chunk row: a missing `v`, a JSON ``null`` `v`, a non-number
    `v`, a fractional `v`, an out-of-range `v`, and an oversized/unrepresentable
    `v`. These are encoded in the SQL as greppable guards whose absence would
    weaken the durable gate that the per-daemon parser mirrors.
    """
    migration = (
        Path(__file__).resolve().parent.parent / "migrations" / "0005_protocol_version_window.sql"
    ).read_text(encoding="utf-8")

    # Missing `v` / JSON `null` `v` / non-number `v`: the type test must treat a
    # missing key (SQL NULL from jsonb extraction) and a JSON null ('null') as
    # decisively NOT a number. `is not distinct from 'number'` is the only
    # comparison that returns false (never NULL) for both.
    assert "is not distinct from ''number''" in migration

    # Fractional `v` (e.g. 4.9): the value must equal its own floor, an integral
    # number, before the bound check.
    integral_guard = "floor(((payload::jsonb)->''v'')::numeric)"
    assert integral_guard in migration
    assert "= floor(((payload::jsonb)->''v'')::numeric)" in migration

    # Out-of-range `v`: the integral value must lie inside the retained range,
    # compared as numeric (so an oversized value is rejected by the bound, not by
    # a cast error or an integer wrap).
    assert "between %L and %L" in migration

    # Oversized / unrepresentable `v`: the constraint must NOT cast `v` to int,
    # which would raise or wrap on a huge JSON number. The version check uses
    # ::numeric only.
    assert "->''v'')::int" not in migration


def test_migration_0005_payload_type_validation_is_total_and_fail_closed() -> None:
    """The DB boundary admits only canonical JSON-string transport kinds."""
    migration = (
        Path(__file__).resolve().parent.parent / "migrations" / "0005_protocol_version_window.sql"
    ).read_text(encoding="utf-8")

    assert "jsonb_typeof((payload::jsonb)->'type')" in migration
    assert "is not distinct from 'string'" in migration
    assert "->>'type' in ('command', 'output_chunk')" in migration
    assert "jsonb_typeof((payload::jsonb)->''type'')" in migration
    assert "is not distinct from ''string''" in migration
    assert "else false\n        end'" in migration
    assert "where (payload::jsonb)->>'type' in ('command', 'output_chunk')" not in migration


def test_earlier_transport_schema_surfaces_reject_unknown_payload_types() -> None:
    """Fresh and routing schema definitions fail closed on the kind discriminator."""
    root = Path(__file__).resolve().parent.parent
    schemas = [
        root / "migrations" / "0001_two_column_protocol.sql",
        root / "migrations" / "0003_protocol_v4_server_routing.sql",
    ]

    for path in schemas:
        schema = path.read_text(encoding="utf-8")
        assert "when (payload::jsonb)->>'type' = 'command'" in schema
        assert "when (payload::jsonb)->>'type' = 'output_chunk'" in schema
        assert "else false" in schema
        assert "else true" not in schema


def test_chunk_schema_fields_are_type_strict_at_the_database_boundary() -> None:
    """Chunk ownership and offsets never gain authority through text coercion."""
    root = Path(__file__).resolve().parent.parent
    schemas = [
        root / "migrations" / "0001_two_column_protocol.sql",
        root / "migrations" / "0003_protocol_v4_server_routing.sql",
        root / "migrations" / "0005_protocol_version_window.sql",
    ]

    for path in schemas:
        schema = path.read_text(encoding="utf-8")
        assert (
            "jsonb_typeof((payload::jsonb)->'thread')" in schema
            or "jsonb_typeof((payload::jsonb)->''thread'')" in schema
        )
        assert (
            "jsonb_typeof((payload::jsonb)->'stream')" in schema
            or "jsonb_typeof((payload::jsonb)->''stream'')" in schema
        )
        for field in ("sequence", "start", "end"):
            plain = f"jsonb_typeof((payload::jsonb)->'{field}')"
            quoted = f"jsonb_typeof((payload::jsonb)->''{field}'')"
            assert plain in schema or quoted in schema
            assert "floor(" in schema
            assert "::numeric >= 0" in schema

    migration = schemas[-1].read_text(encoding="utf-8")
    assert "else true" not in migration
    assert "output_chunk structural metadata is malformed/unsupported" in migration
    for field in ("sequence", "start", "end"):
        assert f"->>''{field}'') ~ ''^[0-9]+$''" not in migration


def test_migration_0005_command_status_validation_is_total_and_fail_closed() -> None:
    """The DB boundary admits only canonical JSON-string command statuses.

    A plain ``->> status IS NOT NULL`` check would stringify arbitrary JSON and
    admit rows that no lifecycle path can later claim, recover, or collect. The
    migration must type-check the discriminator both during non-destructive
    preflight and in the installed CHECK constraint, then restrict it to exactly
    the protocol lifecycle domain.
    """
    migration = (
        Path(__file__).resolve().parent.parent / "migrations" / "0005_protocol_version_window.sql"
    ).read_text(encoding="utf-8")

    assert "(payload::jsonb)->'state'->'status'" in migration
    assert "is not distinct from 'string'" in migration
    assert "jsonb_typeof((payload::jsonb)->''state''->''status'')" in migration
    assert "is not distinct from ''string''" in migration

    assert "in ('pending', 'running', 'succeeded', 'failed', 'cancelled')" in migration
    assert "in (''pending'', ''running'', ''succeeded'', ''failed'', ''cancelled'')" in migration

    # The old permissive boundary accepted any JSON value that `->>` could
    # stringify. It must not survive in the installed command constraint.
    assert "and (((payload::jsonb)->''state''->>''status'') is not null)" not in migration
    assert "command status is malformed/unsupported" in migration


def test_migration_0005_retains_old_history_when_execution_floor_advances() -> None:
    """The stored-history range is broader than the daemon execution window.

    Raising the daemon execution floor (for example from ``[4,4]`` to ``[5,5]``)
    must NOT invalidate old terminal ``v=4`` command rows or their ``output_chunk``
    history. The table constraint therefore validates ``v`` against a retained
    range that spans the oldest supported version (``v4``) through the newest
    compatible version this build can store (``v5``), independent of any daemon's
    execution window. The execution window is a runtime-only property (applied via
    the claim predicate and the fail-closed reaper), never the table constraint.
    """
    migration = (
        Path(__file__).resolve().parent.parent / "migrations" / "0005_protocol_version_window.sql"
    ).read_text(encoding="utf-8")

    # The stored-history floor is v4 (not v1): admitting shape-compatible v1-v3
    # direct writes would weaken the fail-closed DB boundary, so they are rejected.
    assert "retained_min integer := 4" in migration
    # The retained range covers the newly supported compatible v5; future max only
    # widens.
    assert "retained_max integer := 5" in migration

    # The CHECK/preflight bound is the retained range, not an execution window:
    # the format() call feeds the retained bounds into the `between` check.
    assert "between %L and %L" in migration
    assert "retained_min, retained_max, retained_min, retained_max" in migration

    # The execution window is explicitly a runtime-only concern, never the table
    # constraint, so advancing it cannot reject old history.
    assert "execution window" in migration
    assert "NEVER the table constraint" in migration
    assert "Settings.supported_protocol_range" in migration


def _migration_retained_bounds() -> tuple[int, int]:
    """Parse the RETAINED_MIN/RETAINED_MAX constants from the 0005 migration.

    Returns:
        A ``(retained_min, retained_max)`` pair of integers.
    """
    migration = (
        Path(__file__).resolve().parent.parent / "migrations" / "0005_protocol_version_window.sql"
    ).read_text(encoding="utf-8")
    min_match = re.search(r"retained_min integer := (\d+)", migration)
    max_match = re.search(r"retained_max integer := (\d+)", migration)
    assert min_match is not None
    assert max_match is not None
    return int(min_match.group(1)), int(max_match.group(1))


def test_retained_max_admits_daemon_execution_max() -> None:
    """The DB-admitted max is the ceiling the daemon execution max may not exceed.

    Rollout order must widen the DB retained/admission max to ``C+1`` before any
    daemon advertises an execution window whose max is ``C+1``. Deterministically,
    the migration's ``RETAINED_MAX`` must be at least the highest version a daemon
    could ever advertise as an execution-window max: every valid daemon window is
    validated against ``SUPPORTED_PROTOCOL_VERSIONS``, so the largest advertiseable
    execution max equals ``max(SUPPORTED_PROTOCOL_VERSIONS)``. Asserting
    ``RETAINED_MAX >= max(SUPPORTED_PROTOCOL_VERSIONS)`` guarantees no daemon can
    advertise an execution max above the DB-admitted max. The retained floor stays
    ``4`` (history separation), never collapsing to the execution floor.
    """
    retained_min, retained_max = _migration_retained_bounds()
    assert retained_min == 4
    assert retained_max >= max(SUPPORTED_PROTOCOL_VERSIONS)

    # The code enforces the same ceiling itself: a daemon cannot be configured to
    # advertise an execution window whose max exceeds the supported (admitted) max.
    supported_top = max(SUPPORTED_PROTOCOL_VERSIONS)
    with pytest.raises(ValueError, match="cannot parse"):
        Settings(
            worker_id="w",
            poll_interval_seconds=1.0,
            process_poll_interval_seconds=0.1,
            cancel_grace_seconds=5.0,
            server="s",
            supported_protocol_range=ProtocolVersionRange(min=supported_top, max=supported_top + 1),
        )


def test_parser_rejects_missing_and_null_version() -> None:
    """The per-daemon parser fails closed on a missing or JSON-null `v`.

    This mirrors the migration's total version gate: a row that slips past the
    schema constraint must still be rejected by the runtime parser rather than
    becoming an unclaimable, unreaped orphan.
    """
    base = build_payload(server="s", cwd="/srv", process=["echo"])
    missing = dict(base)
    missing.pop("v", None)
    with pytest.raises(ProtocolError, match="v"):
        parse_payload(missing)
    nulled = dict(base)
    nulled["v"] = None
    with pytest.raises(ProtocolError, match="v"):
        parse_payload(nulled)
    chunk = build_output_chunk_payload(
        server="s",
        thread=uuid4(),
        stream="stdout",
        sequence=0,
        start=0,
        end=2,
        value="ok",
        previous=None,
        version=4,
    )
    chunk.pop("v", None)
    with pytest.raises(ProtocolError, match="v"):
        # parse_chunk_payload takes raw JSON text; emulate a missing `v` there.
        parse_chunk_payload(json.dumps(chunk))
