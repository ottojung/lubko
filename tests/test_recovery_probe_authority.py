"""Strict recovery-probe claim authority parsing."""

import pytest

from lubko import lifecycle


def test_probe_claim_state_accepts_canonical_running_identity() -> None:
    """Canonical JSON state keeps exact recovery-probe authority."""
    assert lifecycle._parse_probe_claim_state({
        "status": "running",
        "worker_id": "worker-a",
        "process_pid": 4242,
    }) == ("running", "worker-a", 4242)


def test_probe_claim_state_allows_process_pid_absence_before_publication() -> None:
    """A running claim may temporarily lack process identity before publication."""
    assert lifecycle._parse_probe_claim_state({"status": "running", "worker_id": "worker-a"}) == (
        "running",
        "worker-a",
        None,
    )


@pytest.mark.parametrize(
    "process_pid",
    ["4242", 4242.0, 4242.9, True, False, 0, -1, None, [], {}],
)
def test_probe_claim_state_rejects_malformed_process_pid(process_pid: object) -> None:
    """Malformed persisted PIDs never become adoption authority."""
    assert (
        lifecycle._parse_probe_claim_state({
            "status": "running",
            "worker_id": "worker-a",
            "process_pid": process_pid,
        })
        is None
    )


def test_probe_claim_state_rejects_malformed_worker_id() -> None:
    """Malformed persisted worker IDs never become adoption authority."""
    bad_ids: tuple[object, ...] = (1, True, None, [], {})
    for worker_id in bad_ids:
        assert (
            lifecycle._parse_probe_claim_state({
                "status": "running",
                "worker_id": worker_id,
                "process_pid": 4242,
            })
            is None
        )


def test_probe_claim_state_rejects_malformed_status() -> None:
    """Malformed persisted statuses never become adoption authority."""
    bad_statuses: tuple[object, ...] = (1, True, None, [], {})
    for status in bad_statuses:
        assert (
            lifecycle._parse_probe_claim_state({
                "status": status,
                "worker_id": "worker-a",
                "process_pid": 4242,
            })
            is None
        )
