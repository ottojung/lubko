"""Regression coverage for strict steer metadata status rendering."""

from __future__ import annotations

import pytest

from lubko import agent


def _status(meta: agent.Meta) -> agent.Meta:
    return agent._status_json("abc123", meta, "idle", alive=False)


def test_status_json_preserves_canonical_steer_queue_output() -> None:
    """Canonical empty and queued metadata keep their status semantics."""
    empty = _status({})
    assert empty["steers_pending"] == 0
    assert empty["next_steer"] is None
    assert empty["steer_metadata_error"] is None

    queued = _status({
        "steer_seq": 2,
        "steer_queue": [{"seq": 2, "prompt": "first line\nsecond line", "queued_at": 1.5}],
    })
    assert queued["steers_pending"] == 1
    assert queued["next_steer"] == "first line"
    assert queued["steer_metadata_error"] is None


@pytest.mark.parametrize("value", [0, 1, False, True, {}, "", "oops"])
def test_status_json_fails_closed_on_malformed_steer_queue(value: object) -> None:
    """Malformed queue containers surface an explicit diagnostic."""
    status = _status({"steer_seq": 0, "steer_queue": value})
    assert status["steers_pending"] is None
    assert status["next_steer"] is None
    assert status["steer_metadata_error"] == "malformed persisted steer metadata"


@pytest.mark.parametrize(
    "queue",
    [
        [1],
        [{"seq": 1, "prompt": 7, "queued_at": 1.0}],
        [{"seq": 1, "prompt": "ok", "queued_at": None}],
    ],
)
def test_status_json_fails_closed_on_malformed_steer_item(queue: object) -> None:
    """Malformed queued items never reach rendering assumptions."""
    status = _status({"steer_seq": 1, "steer_queue": queue})
    assert status["steers_pending"] is None
    assert status["next_steer"] is None
    assert status["steer_metadata_error"] == "malformed persisted steer metadata"


def test_status_json_fails_closed_on_malformed_steer_sequence() -> None:
    """Malformed sequence authority fails closed in status output."""
    status = _status({"steer_seq": False, "steer_queue": []})
    assert status["steers_pending"] is None
    assert status["next_steer"] is None
    assert status["steer_metadata_error"] == "malformed persisted steer metadata"
