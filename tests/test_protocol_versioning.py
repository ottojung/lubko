"""Bounded mixed-version protocol upgrade invariants.

These are general, deterministic invariants of the reusable upgrade mechanism
in :mod:`lubko.protocol_versioning` and its integration into the protocol
parsers. They assert stable properties of the model (bounded window, negotiated
convergence, fail-closed on unsupported versions, preserved history) rather than
memorializing one specific cutover.

All tests run without a database.
"""

import json
from uuid import uuid4

import pytest

from lubko.protocol import (
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
    JobVersionDisposition,
    ProtocolVersionError,
    ProtocolVersionRange,
    VersionNegotiationError,
    claim_version_predicate,
    classify_job_version,
    negotiate_version,
    unsupported_version_diagnostic,
)
from lubko.worker import Settings


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
    with pytest.raises(ValueError, match="cannot parse"):
        _settings(supported_protocol_range=ProtocolVersionRange(min=4, max=5))


def test_worker_accepts_default_window() -> None:
    """The default window is always valid for this build."""
    settings = _settings()
    assert settings.supported_protocol_range == DEFAULT_VERSION_RANGE
