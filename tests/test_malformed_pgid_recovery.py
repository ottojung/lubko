"""Regression tests for malformed persisted process-group recovery authority."""

from __future__ import annotations

from typing import cast
from unittest.mock import MagicMock

import pytest

from lubko import supervisor, worker


def test_owned_running_group_row_requires_json_integers() -> None:
    """Only canonical JSON integers become persisted process authority."""
    assert worker._parse_owned_running_group_row("valid", 4242, 999) == (4242, 999, "valid")

    for value in ["4242", 4242.0, True, None, [], {}, 0, -1]:
        assert worker._parse_owned_running_group_row("bad-pgid", value, 999) == (
            None,
            999,
            "bad-pgid",
        )

    for value in ["999", 999.0, True, None, [], {}, 0, -1]:
        assert worker._parse_owned_running_group_row("bad-ticks", 4243, value) == (
            4243,
            None,
            "bad-ticks",
        )


def test_owned_running_groups_preserves_json_scalar_types() -> None:
    """The SQL-backed parser receives JSON scalars without text coercion."""
    cursor = MagicMock()
    cursor.__enter__.return_value = cursor
    cursor.fetchall.return_value = [
        ("int", 4242, 999),
        ("str", "4242", "999"),
        ("float", 4242.0, 999.0),
        ("bool", True, True),
    ]
    conn = MagicMock()
    conn.cursor.return_value = cursor

    groups = worker._owned_running_groups(cast("worker.JobsConnection", conn), "inc")
    assert groups == [
        (4242, 999, "int"),
        (None, None, "str"),
        (None, None, "float"),
        (None, None, "bool"),
    ]
    query = cursor.execute.call_args.args[0]
    selected = query.split("FROM lubko.jobs", 1)[0]
    assert "->'process_pgid'" in selected
    assert "->>'process_pgid'" not in selected
    assert "->'process_start_time_ticks'" in selected
    assert "->>'process_start_time_ticks'" not in selected


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
