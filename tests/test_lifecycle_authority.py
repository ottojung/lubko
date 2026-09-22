"""Stable invariants for database-backed lifecycle authority.

Crash-durable lifecycle authority lives in one row per execution server in
the existing transport table: deterministic row identity, neutral bootstrap
convergence, fencing-epoch compare-and-swap takes, fresh-read confirmation,
a documented byte bound, and permanent exemption from worker garbage
collection.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, cast
from uuid import UUID

import pytest

from lubko import lifecycle_authority as authority
from lubko import worker
from tests._fake_authority_db import FakeAuthorityConnection

if TYPE_CHECKING:
    from lubko.worker import JobsConnection


def _conn(table: FakeAuthorityConnection) -> JobsConnection:
    """Return the fake as an opaque connection for authority calls.

    Args:
        table: Fake authority connection backing the call.

    Returns:
        The fake connection, cast to the production connection type.
    """
    return cast("JobsConnection", table)


def _owner(pid: int = 100, ticks: int = 200, boot: str = "boot-1") -> authority.AuthorityOwner:
    """Build an exact test owner identity.

    Returns:
        An owner with the given identity fields.
    """
    return authority.AuthorityOwner(pid=pid, start_time_ticks=ticks, boot_id=boot)


def _claim(server: str = "srv-test", epoch: int = 1) -> authority.AuthorityClaim:
    """Build an in-memory fencing claim for tests.

    Returns:
        A claim naming the test owner at the given epoch.
    """
    owner = _owner()
    return authority.AuthorityClaim(
        server=server,
        epoch=epoch,
        pid=owner.pid,
        start_time_ticks=owner.start_time_ticks,
        boot_id=owner.boot_id,
    )


def test_row_id_is_deterministic_per_server_and_unique_across_servers() -> None:
    """Identical servers share one authority domain; distinct servers never collide."""
    assert authority.authority_row_id("srv-a") == authority.authority_row_id("srv-a")
    assert isinstance(authority.authority_row_id("srv-a"), UUID)
    assert authority.authority_row_id("srv-a") != authority.authority_row_id("srv-b")
    with pytest.raises(authority.AuthorityError):
        authority.authority_row_id("")


def test_authority_payload_round_trips_within_the_byte_bound() -> None:
    """A built payload parses back to the identical row."""
    text = authority.serialize_authority_payload(
        authority.build_authority_payload(server="srv-test", epoch=3, generation=7, owner=_owner())
    )
    assert len(text.encode("utf-8")) <= authority.AUTHORITY_MAX_BYTES
    assert authority.parse_authority_payload(text, server="srv-test") == authority.AuthorityRow(
        server="srv-test", epoch=3, generation=7, owner=_owner()
    )


def test_oversize_payload_is_refused_before_any_action() -> None:
    """Transitions that would exceed the bound never reach the database."""
    table = FakeAuthorityConnection()
    huge = authority.AuthorityOwner(pid=1, start_time_ticks=0, boot_id="b" * 10_000)
    with pytest.raises(authority.AuthorityError):
        authority.serialize_authority_payload(
            authority.build_authority_payload(server="srv-test", epoch=0, generation=0, owner=huge)
        )
    assert table.rows == {}
    assert table.statements == []
    with pytest.raises(authority.AuthorityError):
        authority.parse_authority_payload("x" * (authority.AUTHORITY_MAX_BYTES + 1), server="srv")


@pytest.mark.parametrize(
    "payload",
    [
        "not json",
        "[1, 2]",
        {
            "v": 2,
            "type": "lifecycle_authority",
            "server": "s",
            "epoch": 0,
            "generation": 0,
            "owner": None,
        },
        {"v": 1, "type": "command", "server": "s", "epoch": 0, "generation": 0, "owner": None},
        {
            "v": 1,
            "type": "lifecycle_authority",
            "server": "other",
            "epoch": 0,
            "generation": 0,
            "owner": None,
        },
        {
            "v": 1,
            "type": "lifecycle_authority",
            "server": "s",
            "epoch": -1,
            "generation": 0,
            "owner": None,
        },
        {
            "v": 1,
            "type": "lifecycle_authority",
            "server": "s",
            "epoch": True,
            "generation": 0,
            "owner": None,
        },
        {
            "v": 1,
            "type": "lifecycle_authority",
            "server": "s",
            "epoch": 0,
            "generation": 0,
            "owner": {"pid": 0, "start_time_ticks": 1, "boot_id": "b"},
        },
        {
            "v": 1,
            "type": "lifecycle_authority",
            "server": "s",
            "epoch": 0,
            "generation": 0,
            "owner": {"pid": 1, "start_time_ticks": 1, "boot_id": ""},
        },
        {
            "v": 1,
            "type": "lifecycle_authority",
            "server": "s",
            "epoch": 0,
            "generation": 0,
            "owner": {"pid": 1, "start_time_ticks": -1, "boot_id": "b"},
        },
    ],
)
def test_untrusted_authority_payloads_fail_closed(payload: object) -> None:
    """Any deviation from the binding holds instead of authorizing action."""
    with pytest.raises(authority.AuthorityError):
        authority.parse_authority_payload(payload, server="s")


def test_concurrent_bootstraps_converge_onto_one_neutral_row() -> None:
    """Exactly one neutral insert wins; every contender reads the same row."""
    first = FakeAuthorityConnection()
    second = FakeAuthorityConnection()
    second.rows = first.rows
    row_a = authority.bootstrap_authority(_conn(first), "srv-test")
    row_b = authority.bootstrap_authority(_conn(second), "srv-test")
    assert (
        row_a
        == row_b
        == authority.AuthorityRow(server="srv-test", epoch=0, generation=0, owner=None)
    )
    assert len(first.rows) == 1


def test_take_bumps_the_epoch_and_names_the_exact_owner() -> None:
    """A take commits owner, epoch, and generation atomically."""
    table = FakeAuthorityConnection()
    authority.bootstrap_authority(_conn(table), "srv-test")
    taken = authority.take_authority(_conn(table), "srv-test", _owner())
    assert taken is not None
    assert taken.epoch == 1
    assert taken.owner == _owner()
    assert authority.read_authority(_conn(table), "srv-test") == taken


def test_take_is_an_idempotent_adoption_for_the_same_incarnation() -> None:
    """Re-taking as the recorded owner changes nothing."""
    table = FakeAuthorityConnection()
    authority.bootstrap_authority(_conn(table), "srv-test")
    first = authority.take_authority(_conn(table), "srv-test", _owner())
    second = authority.take_authority(_conn(table), "srv-test", _owner())
    assert first == second
    assert second is not None
    assert second.epoch == 1


def test_stale_take_loses_and_stands_down(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A contender acting on a superseded read never commits."""
    table = FakeAuthorityConnection()
    authority.bootstrap_authority(_conn(table), "srv-test")
    authority.take_authority(_conn(table), "srv-test", _owner(pid=100))
    stale = authority.AuthorityRow(server="srv-test", epoch=0, generation=0, owner=None)
    real_read = authority.read_authority
    monkeypatch.setattr(authority, "read_authority", lambda _conn, _server: stale)
    try:
        assert authority.take_authority(_conn(table), "srv-test", _owner(pid=200)) is None
    finally:
        monkeypatch.setattr(authority, "read_authority", real_read)
    current = authority.read_authority(_conn(table), "srv-test")
    assert current is not None
    assert current.owner == _owner(pid=100)


def test_confirm_matches_only_fresh_epoch_and_exact_owner() -> None:
    """Confirmation fails on stale epoch, foreign owner, or missing rows."""
    table = FakeAuthorityConnection()
    claim = _claim(epoch=1)
    assert authority.confirm_authority(_conn(table), claim) is False
    authority.bootstrap_authority(_conn(table), "srv-test")
    assert authority.confirm_authority(_conn(table), claim) is False
    authority.take_authority(_conn(table), "srv-test", _owner())
    assert authority.confirm_authority(_conn(table), claim) is True
    assert authority.confirm_authority(_conn(table), _claim(epoch=2)) is False
    foreign = authority.AuthorityClaim(
        server="srv-test", epoch=1, pid=999, start_time_ticks=200, boot_id="boot-1"
    )
    assert authority.confirm_authority(_conn(table), foreign) is False


def test_confirm_treats_corruption_as_no_authority_but_outage_as_unavailable() -> None:
    """Absence and corruption read as False; outages stay distinguishable."""
    table = FakeAuthorityConnection()
    claim = _claim()
    row_id = str(authority.authority_row_id("srv-test"))
    table.rows[row_id] = "torn [[["
    assert authority.confirm_authority(_conn(table), claim) is False
    table.rows[row_id] = json.dumps({"v": 1, "type": "command"})
    assert authority.confirm_authority(_conn(table), claim) is False
    table.unreachable = True
    with pytest.raises(authority.AuthorityUnavailableError):
        authority.confirm_authority(_conn(table), claim)


def test_worker_collection_never_touches_non_job_rows() -> None:
    """Every GC mutation predicates on job/output kinds only."""
    table = FakeAuthorityConnection()
    table.empty_reads = True
    settings = worker.Settings(
        worker_id="w-test",
        poll_interval_seconds=0.0,
        process_poll_interval_seconds=0.0,
        cancel_grace_seconds=1.0,
        server="srv-test",
    )
    worker.collect_transport(cast("JobsConnection", table), settings)
    mutating = [sql for sql in table.statements if "DELETE" in sql or "UPDATE" in sql]
    assert mutating, "expected the collection pass to issue mutations"
    for sql in mutating:
        assert "'command'" in sql or "'output_chunk'" in sql
    assert not any("lifecycle_authority" in sql for sql in table.statements)


def test_authority_row_carries_no_terminal_job_state() -> None:
    """The authority kind can never look like a collectable terminal row."""
    payload = json.loads(
        authority.serialize_authority_payload(
            authority.build_authority_payload(
                server="srv-test", epoch=1, generation=2, owner=_owner()
            )
        )
    )
    assert payload["type"] == authority.AUTHORITY_TYPE
    assert "state" not in payload
    assert "status" not in payload
    assert "finished_at" not in payload
