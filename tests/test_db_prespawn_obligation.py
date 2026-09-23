"""Database-backed fenced pre-spawn obligation invariants.

Steady-state worker spawn must not require a successful local persistent
write: the crash-durable authority is a committed row state *before* the
spawn syscall, and local files are only a read-through cache. These tests
prove, deterministically and without any tmpfs dependence:

- the commit precedes ``Popen`` and a crash in between leaves a resolvable
  deterministic recovery obligation (no duplicate or unowned worker);
- an ``ENOSPC`` local cache failure never blocks the spawn;
- a transient database outage fails closed and recovers on reconnect;
- fencing loss or an already-committed obligation blocks the spawn;
- an epoch take preserves a committed obligation for the new owner.
"""

from __future__ import annotations

import errno
from dataclasses import replace
from typing import TYPE_CHECKING, cast

import pytest

from lubko import cli, lifecycle, supervise, supervisor
from lubko import lifecycle_authority as authority
from lubko.durable import DurabilityError
from tests._fake_authority_db import FakeAuthorityConnection

if TYPE_CHECKING:
    from pathlib import Path

    from lubko.worker import JobsConnection

COMMIT = "a" * 40
SERVER = "srv-prespawn-test"
OBLIGATION_TOKEN = f"spawn-token-{807}"
OTHER_TOKEN = f"spawn-token-{807}-other"
OWN_TOKEN = f"spawn-token-{807}-own"


def _conn(table: FakeAuthorityConnection) -> JobsConnection:
    """Expose a fake authority table as a database connection.

    Args:
        table: The fake authority connection.

    Returns:
        The table cast to the connection interface.
    """
    return cast("JobsConnection", table)


def _noop_recover(_incarnation: str) -> None:
    """Stand in for owned-group recovery that has nothing to reclaim.

    Args:
        _incarnation: Retired worker incarnation whose groups are already gone.
    """


def _spawn_record(
    *, incarnation: str = OBLIGATION_TOKEN, pid: int | None = None
) -> authority.SpawnObligation:
    """Build a minimal database spawn obligation.

    Args:
        incarnation: Lifecycle token naming the incarnation.
        pid: Committed child identity, or ``None`` for a pid-less record.

    Returns:
        The obligation.
    """
    return authority.SpawnObligation(
        token=incarnation,
        commit=COMMIT,
        creator_pid=424242,
        creator_start_time_ticks=4242,
        boot_id="boot-test",
        pid=pid,
        start_time_ticks=None,
        parent_death_signal=True,
    )


def _claim_daemon(
    monkeypatch: pytest.MonkeyPatch, table: FakeAuthorityConnection
) -> supervisor.SupervisorDaemon:
    """Build a daemon holding the fencing epoch on the fake database.

    Args:
        monkeypatch: The active monkeypatch fixture.
        table: The fake authority connection backing every row access.

    Returns:
        A daemon whose claim matches the committed row.
    """
    monkeypatch.setattr(supervisor, "load_worker_server", lambda: SERVER)
    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    daemon._authority_conn_factory = lambda: _conn(table)
    daemon._write_pidfile()
    assert daemon._authority is not None
    return daemon


def _patch_spawn_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Stub the spawn environment so no real process ever starts.

    Args:
        monkeypatch: The active monkeypatch fixture.
        tmp_path: Directory standing in for the sealed runtime root.
    """
    monkeypatch.setattr(cli, "runtime_is_usable", lambda _c: True)
    monkeypatch.setattr(cli, "cli_entry_executable", lambda _c, _n: tmp_path / "worker")
    monkeypatch.setattr(cli, "cli_commit_dir", lambda _c: tmp_path)
    monkeypatch.setattr(lifecycle, "worker_env", lambda _t: {})
    monkeypatch.setattr(supervisor, "read_desired", lambda: None)
    monkeypatch.setattr(supervise, "read_desired", lambda: None)


def _clear_local_spawning() -> None:
    """Drop any local pre-spawn cache record for test isolation."""
    supervise.write_state(replace(supervise.read_state(), spawning=None))


def test_spawn_obligation_round_trips_within_the_byte_bound() -> None:
    """A committed spawn obligation parses back exactly and stays bounded."""
    table = FakeAuthorityConnection()
    authority.bootstrap_authority(_conn(table), SERVER)
    owner = authority.AuthorityOwner(pid=1, start_time_ticks=2, boot_id="boot-test")
    assert authority.take_authority(_conn(table), SERVER, owner) is not None
    claim = authority.AuthorityClaim(
        server=SERVER, epoch=1, pid=1, start_time_ticks=2, boot_id="boot-test"
    )
    assert authority.commit_spawn_obligation(_conn(table), claim, _spawn_record()) is True
    row = authority.read_authority(_conn(table), SERVER)
    assert row is not None
    assert row.spawn == _spawn_record()
    assert len(table.rows[str(authority.authority_row_id(SERVER))].encode("utf-8")) <= (
        authority.AUTHORITY_MAX_BYTES
    )


def test_second_commit_while_obligation_outstanding_loses() -> None:
    """Two contenders committing a spawn converge on exactly one obligation."""
    table = FakeAuthorityConnection()
    authority.bootstrap_authority(_conn(table), SERVER)
    owner = authority.AuthorityOwner(pid=1, start_time_ticks=2, boot_id="boot-test")
    assert authority.take_authority(_conn(table), SERVER, owner) is not None
    claim = authority.AuthorityClaim(
        server=SERVER, epoch=1, pid=1, start_time_ticks=2, boot_id="boot-test"
    )
    assert authority.commit_spawn_obligation(_conn(table), claim, _spawn_record()) is True
    other = authority.commit_spawn_obligation(
        _conn(table), claim, _spawn_record(incarnation=OTHER_TOKEN)
    )
    assert other is False
    row = authority.read_authority(_conn(table), SERVER)
    assert row is not None
    assert row.spawn is not None
    assert row.spawn.token == OBLIGATION_TOKEN


def test_epoch_take_preserves_the_committed_obligation() -> None:
    """A new epoch owner still owes resolution of the first spawn's fate."""
    table = FakeAuthorityConnection()
    authority.bootstrap_authority(_conn(table), SERVER)
    first = authority.AuthorityOwner(pid=1, start_time_ticks=2, boot_id="boot-test")
    assert authority.take_authority(_conn(table), SERVER, first) is not None
    claim = authority.AuthorityClaim(
        server=SERVER, epoch=1, pid=1, start_time_ticks=2, boot_id="boot-test"
    )
    assert authority.commit_spawn_obligation(_conn(table), claim, _spawn_record()) is True
    second = authority.AuthorityOwner(pid=7, start_time_ticks=8, boot_id="boot-test")
    taken = authority.take_authority(_conn(table), SERVER, second)
    assert taken is not None
    assert taken.spawn == _spawn_record()
    row = authority.read_authority(_conn(table), SERVER)
    assert row is not None
    assert row.spawn == _spawn_record()


def test_malformed_spawn_payloads_fail_closed() -> None:
    """A torn spawn section never parses as usable authority."""
    for raw in ("not-an-object", {"token": COMMIT[:0], "commit": COMMIT}, {"token": COMMIT[:1]}):
        with pytest.raises(authority.AuthorityError):
            authority.parse_authority_payload(
                {
                    "v": 1,
                    "type": "lifecycle_authority",
                    "server": SERVER,
                    "epoch": 0,
                    "generation": 0,
                    "owner": None,
                    "spawn": raw,
                },
                server=SERVER,
            )


@pytest.mark.usefixtures("supervisor_token")
def test_commit_precedes_popen_and_outage_fails_closed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """No spawn syscall happens unless the obligation committed first."""
    _patch_spawn_env(monkeypatch, tmp_path)
    _clear_local_spawning()
    table = FakeAuthorityConnection()
    daemon = _claim_daemon(monkeypatch, table)
    calls: list[str] = []

    def _fake_popen(*_args: object, **_kwargs: object) -> object:
        calls.append("popen")
        msg = "simulated crash between commit and spawn"
        raise OSError(msg)

    monkeypatch.setattr(supervisor.subprocess, "Popen", _fake_popen)  # type: ignore[attr-defined]
    table.unreachable = True
    assert daemon._spawn_worker(COMMIT) is None
    assert calls == []
    assert "unreachable" in (daemon._message or "")
    table.unreachable = False
    assert daemon._spawn_worker(COMMIT) is None
    assert calls == ["popen"]
    row = authority.read_authority(_conn(table), SERVER)
    assert row is not None
    assert row.spawn is None


@pytest.mark.usefixtures("supervisor_token")
def test_enospc_cache_failure_never_blocks_spawn(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Exhausted local storage drops the cache write; the spawn proceeds."""
    _patch_spawn_env(monkeypatch, tmp_path)
    _clear_local_spawning()
    table = FakeAuthorityConnection()
    daemon = _claim_daemon(monkeypatch, table)
    calls: list[str] = []

    def _no_space_write(_state: object) -> None:
        msg = "No space left on device"
        raise DurabilityError(msg) from OSError(errno.ENOSPC, msg)

    monkeypatch.setattr(supervisor, "write_state", _no_space_write)

    def _fake_popen(*_args: object, **_kwargs: object) -> object:
        calls.append("popen")
        msg = "simulated spawn failure after the fenced commit"
        raise OSError(msg)

    monkeypatch.setattr(supervisor.subprocess, "Popen", _fake_popen)  # type: ignore[attr-defined]
    assert daemon._spawn_worker(COMMIT) is None
    assert calls == ["popen"]
    row = authority.read_authority(_conn(table), SERVER)
    assert row is not None
    assert row.spawn is None


@pytest.mark.usefixtures("supervisor_token")
def test_committed_obligation_blocks_a_second_spawn(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A crash between commit and spawn never yields two consumers."""
    _patch_spawn_env(monkeypatch, tmp_path)
    _clear_local_spawning()
    table = FakeAuthorityConnection()
    daemon = _claim_daemon(monkeypatch, table)
    assert daemon._authority is not None
    committed = authority.commit_spawn_obligation(_conn(table), daemon._authority, _spawn_record())
    assert committed is True

    def _forbidden_popen(*_args: object, **_kwargs: object) -> object:
        msg = "a second spawn must never follow a committed obligation"
        raise AssertionError(msg)

    monkeypatch.setattr(supervisor.subprocess, "Popen", _forbidden_popen)  # type: ignore[attr-defined]
    assert daemon._spawn_worker(COMMIT) is None
    assert "already committed" in (daemon._message or "")


@pytest.mark.usefixtures("supervisor_token")
def test_fencing_loss_blocks_spawn(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """A superseded incarnation cannot commit and never spawns."""
    _patch_spawn_env(monkeypatch, tmp_path)
    _clear_local_spawning()
    table = FakeAuthorityConnection()
    daemon = _claim_daemon(monkeypatch, table)
    foreign = authority.AuthorityOwner(pid=999, start_time_ticks=888, boot_id="boot-9")
    assert authority.take_authority(_conn(table), SERVER, foreign) is not None

    def _forbidden_popen(*_args: object, **_kwargs: object) -> object:
        msg = "a superseded incarnation must never spawn"
        raise AssertionError(msg)

    monkeypatch.setattr(supervisor.subprocess, "Popen", _forbidden_popen)  # type: ignore[attr-defined]
    assert daemon._spawn_worker(COMMIT) is None
    assert "superseded" in (daemon._message or "")


@pytest.mark.usefixtures("supervisor_token")
def test_crash_between_commit_and_spawn_resolves_without_duplicate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A successor resolves the deterministic row obligation and spawns once."""
    _clear_local_spawning()
    table = FakeAuthorityConnection()
    authority.bootstrap_authority(_conn(table), SERVER)
    dead = authority.AuthorityOwner(pid=424242, start_time_ticks=4242, boot_id="boot-test")
    assert authority.take_authority(_conn(table), SERVER, dead) is not None
    claim = authority.AuthorityClaim(
        server=SERVER, epoch=1, pid=424242, start_time_ticks=4242, boot_id="boot-test"
    )
    assert authority.commit_spawn_obligation(_conn(table), claim, _spawn_record()) is True

    monkeypatch.setattr(supervisor, "current_boot_id", lambda: "boot-test")
    successor = _claim_daemon(monkeypatch, table)
    assert successor._authority is not None
    assert successor._authority.epoch == 2
    monkeypatch.setattr(supervisor, "_pdeathsig_supported", lambda: True)
    recovered: list[str] = []
    monkeypatch.setattr(supervisor, "recover_owned_groups", recovered.append)

    def _forbidden_popen(*_args: object, **_kwargs: object) -> object:
        msg = "resolution itself must never spawn"
        raise AssertionError(msg)

    monkeypatch.setattr(supervisor.subprocess, "Popen", _forbidden_popen)  # type: ignore[attr-defined]
    assert successor._resolve_spawning_obligation() is True
    assert recovered == [OBLIGATION_TOKEN]
    row = authority.read_authority(_conn(table), SERVER)
    assert row is not None
    assert row.spawn is None
    assert supervise.read_state().spawning is None


@pytest.mark.usefixtures("supervisor_token")
def test_unreachable_database_keeps_the_obligation_blocking(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Recovery holds (never spawns, never clears) while partitioned."""
    _clear_local_spawning()
    table = FakeAuthorityConnection()
    daemon = _claim_daemon(monkeypatch, table)
    assert daemon._authority is not None
    assert (
        authority.commit_spawn_obligation(_conn(table), daemon._authority, _spawn_record()) is True
    )
    supervise.write_state(
        replace(
            supervise.read_state(),
            spawning=supervise.SpawningObligation(
                token=OBLIGATION_TOKEN,
                commit=COMMIT,
                creator_pid=424242,
                creator_start_time_ticks=4242,
                pid=None,
                start_time_ticks=None,
                created_at=0.0,
                boot_id="boot-test",
                parent_death_signal=True,
            ),
        )
    )
    monkeypatch.setattr(supervisor, "_pdeathsig_supported", lambda: True)
    table.unreachable = True

    def _failing_recover(_token: str) -> None:
        msg = "database unreachable during recovery"
        raise supervisor.OwnedGroupRecoveryError(msg)

    monkeypatch.setattr(supervisor, "recover_owned_groups", _failing_recover)
    assert daemon._resolve_spawning_obligation() is False
    table.unreachable = False
    monkeypatch.setattr(supervisor, "recover_owned_groups", _noop_recover)
    assert daemon._resolve_spawning_obligation() is True
    row = authority.read_authority(_conn(table), SERVER)
    assert row is not None
    assert row.spawn is None


@pytest.mark.usefixtures("supervisor_token")
def test_own_dead_inflight_spawn_resolves_for_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Our own gone spawn recovers its groups instead of wedging the daemon."""
    _clear_local_spawning()
    table = FakeAuthorityConnection()
    daemon = _claim_daemon(monkeypatch, table)
    assert daemon._authority is not None
    me = daemon._authority
    own = authority.SpawnObligation(
        token=OWN_TOKEN,
        commit=COMMIT,
        creator_pid=me.pid,
        creator_start_time_ticks=me.start_time_ticks,
        boot_id=me.boot_id,
        pid=None,
        start_time_ticks=None,
        parent_death_signal=True,
    )
    assert authority.commit_spawn_obligation(_conn(table), me, own) is True
    daemon.proc = None
    recovered: list[str] = []
    monkeypatch.setattr(supervisor, "recover_owned_groups", recovered.append)
    assert daemon._resolve_spawning_obligation() is True
    assert recovered == [OWN_TOKEN]
    row = authority.read_authority(_conn(table), SERVER)
    assert row is not None
    assert row.spawn is None


def test_capacity_failure_classification() -> None:
    """Only exhausted-storage errnos count as capacity failures."""
    assert supervisor._capacity_failure(OSError(errno.ENOSPC, "x")) is True
    assert supervisor._capacity_failure(OSError(errno.EDQUOT, "x")) is True
    assert supervisor._capacity_failure(OSError(errno.EIO, "x")) is False
    chained = DurabilityError("y")
    chained.__cause__ = OSError(errno.ENOSPC, "x")
    assert supervisor._capacity_failure(chained) is True
    assert supervisor._capacity_failure(ValueError("z")) is False
