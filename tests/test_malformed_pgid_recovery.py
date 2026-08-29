"""Regression tests for malformed persisted process-group recovery authority."""

from __future__ import annotations

from typing import cast

import pytest

from lubko import supervisor, worker


def test_owned_running_group_row_keeps_malformed_present_pgid() -> None:
    """Malformed present PGIDs remain explicit instead of disappearing."""
    assert worker._parse_owned_running_group_row("valid", "4242", "999") == (
        4242,
        999,
        "valid",
    )
    assert worker._parse_owned_running_group_row("bad-pgid", "not-an-int", "999") == (
        None,
        999,
        "bad-pgid",
    )
    assert worker._parse_owned_running_group_row("bad-zero", "0", "999") == (
        None,
        999,
        "bad-zero",
    )
    assert worker._parse_owned_running_group_row("bad-ticks", "4243", "nope") == (
        4243,
        None,
        "bad-ticks",
    )


def test_recovery_blocks_malformed_pgid_then_converges_after_repair(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Repairing durable PGID authority changes a hold into clean convergence."""
    groups: list[tuple[int | None, int | None, str]] = [(None, 999, "job-id")]
    monkeypatch.setattr(worker, "_owned_running_groups", lambda _conn, _inc: groups)
    monkeypatch.setattr(worker, "group_has_members", lambda _pgid: False)
    conn = cast("worker.JobsConnection", object())

    blocked = worker.recover_owned_job_groups(conn, "incarnation", 0.0)
    assert blocked == worker.ReclaimedGroups(
        reaped=[], surviving=[], unresolved=[], malformed=["job-id"]
    )

    groups[:] = [(4242, 999, "job-id")]
    repaired = worker.recover_owned_job_groups(conn, "incarnation", 0.0)
    assert repaired == worker.ReclaimedGroups(reaped=[], surviving=[], unresolved=[], malformed=[])


def test_supervisor_recovery_seam_holds_on_malformed_pgid() -> None:
    """Supervisor convergence rejects malformed persisted PGID authority."""
    result = worker.ReclaimedGroups(reaped=[], surviving=[], unresolved=[], malformed=["job-id"])
    with pytest.raises(supervisor.OwnedGroupRecoveryError, match="job-id"):
        supervisor._require_owned_group_recovery_converged(result, "incarnation")
