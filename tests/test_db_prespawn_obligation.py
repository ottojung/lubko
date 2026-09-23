"""Database-backed fenced pre-spawn obligation invariants.

Steady-state worker spawn and publication must not require a successful
local persistent write: the crash-durable authority is a committed row
state *before* the spawn syscall, and local files are only a read-through
cache. These tests prove, deterministically and without any tmpfs
dependence:

- the commit precedes ``Popen`` and a crash in between leaves a resolvable
  deterministic recovery obligation (no duplicate or unowned worker);
- an ``ENOSPC`` local cache failure never blocks the spawn, and a fully
  zero-space success path still reaches a usable published worker that is
  neither killed nor retried;
- a transient database outage fails closed and recovers on reconnect;
- fencing loss or an already-committed obligation blocks the spawn;
- an epoch take preserves a committed obligation for the new owner, and a
  stale takeover racing a commit can never erase it;
- the safety-critical parent-death flag is never defaulted: omission or a
  non-boolean fails closed.
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
                    "v": authority.AUTHORITY_VERSION,
                    "type": "lifecycle_authority",
                    "server": SERVER,
                    "epoch": 0,
                    "generation": 0,
                    "owner": None,
                    "spawn": raw,
                    "worker": None,
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


class _LiveStubProc:
    """Minimal live ``Popen`` stand-in that never exits on its own."""

    def __init__(self, pid: int) -> None:
        self.pid = pid

    @staticmethod
    def poll() -> None:
        """Report the child as still running."""


def _enospc_durable(_state: object) -> None:
    """Simulate a local durable write refused with exhausted storage.

    Args:
        _state: The state that could not be cached.

    Raises:
        DurabilityError: Always, chained from ``ENOSPC``.
    """
    msg = "No space left on device"
    raise DurabilityError(msg) from OSError(errno.ENOSPC, msg)


def _eio_durable(_state: object) -> None:
    """Simulate a local durable write failing with a non-capacity I/O error.

    Args:
        _state: The state that could not be cached.

    Raises:
        OSError: Always, with bare ``EIO`` and no chained cause.
    """
    raise OSError(errno.EIO, "Input/output error")


@pytest.mark.usefixtures("supervisor_token")
def test_zero_space_success_path_publishes_usable_worker(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A full spawn publishes while every local write fails with ENOSPC.

    ``Popen`` succeeds and the database publication commits, but no local
    state or meta write succeeds. The worker must reach a usable published
    state: it is not converged, no replacement is attempted, and a later
    decision still sees exactly one published worker.
    """
    _patch_spawn_env(monkeypatch, tmp_path)
    _clear_local_spawning()
    table = FakeAuthorityConnection()
    daemon = _claim_daemon(monkeypatch, table)
    monkeypatch.setattr(supervisor, "write_state", _enospc_durable)
    monkeypatch.setattr(lifecycle, "write_meta", _enospc_durable)
    monkeypatch.setattr(supervisor, "proc_start_ticks", lambda _pid: 777)
    monkeypatch.setattr(
        daemon,
        "_wait_for_identity",
        lambda _pid: lifecycle.ProcessIdentity(pid=4242, pgid=4242, sid=4242, start_time_ticks=777),
    )
    monkeypatch.setattr(daemon, "_derive_action", lambda _state: ("run", COMMIT))
    pops: list[int] = []

    def _fake_popen(*_args: object, **_kwargs: object) -> _LiveStubProc:
        pops.append(4242)
        return _LiveStubProc(pid=4242)

    monkeypatch.setattr(supervisor.subprocess, "Popen", _fake_popen)  # type: ignore[attr-defined]
    converged: list[int] = []

    def _forbidden_converge(proc: object) -> bool:
        del proc
        msg = "published worker must never be converged"
        raise AssertionError(msg)

    monkeypatch.setattr(daemon, "_converge_direct_child", _forbidden_converge)
    daemon._spawn_and_publish(COMMIT)
    assert pops == [4242]
    assert converged == []
    assert daemon._active_child is not None
    assert daemon._active_child.pid == 4242
    row = authority.read_authority(_conn(table), SERVER)
    assert row is not None
    assert row.spawn is None
    assert row.worker is not None
    assert row.worker.pid == 4242
    assert row.worker.start_time_ticks == 777

    daemon._ensure_consumer_locked(COMMIT)
    assert pops == [4242]
    assert converged == []
    assert daemon._active_child is not None
    assert daemon._active_child.pid == 4242
    stayed = authority.read_authority(_conn(table), SERVER)
    assert stayed is not None
    assert stayed.worker is not None
    assert stayed.worker.pid == 4242


@pytest.mark.usefixtures("supervisor_token")
def test_eio_success_path_publishes_usable_worker(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A full spawn publishes while every local write fails with bare EIO.

    Unlike exhausted storage, a non-capacity I/O error carries no signal
    that the failure is benign, yet after the database publication has
    committed every local cache projection is still diagnostic-only: the
    worker must reach a usable published state exactly as on the
    zero-space path — not converged, no replacement attempted, and a
    later decision still sees exactly one published worker.
    """
    _patch_spawn_env(monkeypatch, tmp_path)
    _clear_local_spawning()
    table = FakeAuthorityConnection()
    daemon = _claim_daemon(monkeypatch, table)
    monkeypatch.setattr(supervisor, "write_state", _eio_durable)
    monkeypatch.setattr(lifecycle, "write_meta", _eio_durable)
    monkeypatch.setattr(supervisor, "proc_start_ticks", lambda _pid: 777)
    monkeypatch.setattr(
        daemon,
        "_wait_for_identity",
        lambda _pid: lifecycle.ProcessIdentity(pid=4242, pgid=4242, sid=4242, start_time_ticks=777),
    )
    monkeypatch.setattr(daemon, "_derive_action", lambda _state: ("run", COMMIT))
    pops: list[int] = []

    def _fake_popen(*_args: object, **_kwargs: object) -> _LiveStubProc:
        pops.append(4242)
        return _LiveStubProc(pid=4242)

    monkeypatch.setattr(supervisor.subprocess, "Popen", _fake_popen)  # type: ignore[attr-defined]
    converged: list[int] = []

    def _forbidden_converge(proc: object) -> bool:
        del proc
        msg = "published worker must never be converged"
        raise AssertionError(msg)

    monkeypatch.setattr(daemon, "_converge_direct_child", _forbidden_converge)
    daemon._spawn_and_publish(COMMIT)
    assert pops == [4242]
    assert converged == []
    assert daemon._message is None
    assert daemon._active_child is not None
    assert daemon._active_child.pid == 4242
    row = authority.read_authority(_conn(table), SERVER)
    assert row is not None
    assert row.spawn is None
    assert row.worker is not None
    assert row.worker.pid == 4242
    assert row.worker.start_time_ticks == 777

    daemon._ensure_consumer_locked(COMMIT)
    assert pops == [4242]
    assert converged == []
    assert daemon._active_child is not None
    assert daemon._active_child.pid == 4242
    stayed = authority.read_authority(_conn(table), SERVER)
    assert stayed is not None
    assert stayed.worker is not None
    assert stayed.worker.pid == 4242


@pytest.mark.usefixtures("supervisor_token")
def test_stale_takeover_cannot_erase_committed_spawn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A takeover racing a spawn commit loses without touching the row.

    Owner A and contender B both observe epoch N with no spawn; A commits
    its obligation; B then takes on its stale observation. The stale take
    must fail and A's obligation must survive intact, so A may Popen while
    B stands down instead of both spawning.
    """
    table = FakeAuthorityConnection()
    authority.bootstrap_authority(_conn(table), SERVER)
    first = authority.AuthorityOwner(pid=1, start_time_ticks=2, boot_id="boot-1")
    assert authority.take_authority(_conn(table), SERVER, first) is not None
    stale = authority.read_authority(_conn(table), SERVER)
    assert stale is not None
    assert stale.spawn is None
    stale_text = table.rows[str(authority.authority_row_id(SERVER))]
    claim_a = authority.AuthorityClaim(
        server=SERVER, epoch=1, pid=1, start_time_ticks=2, boot_id="boot-1"
    )
    assert authority.commit_spawn_obligation(_conn(table), claim_a, _spawn_record()) is True
    monkeypatch.setattr(authority, "_read_observed", lambda _conn, _server: (stale_text, stale))
    try:
        lost = authority.take_authority(
            _conn(table), SERVER, authority.AuthorityOwner(pid=9, start_time_ticks=9, boot_id="b9")
        )
    finally:
        monkeypatch.undo()
    assert lost is None
    row = authority.read_authority(_conn(table), SERVER)
    assert row is not None
    assert row.epoch == 1
    assert row.owner == first
    assert row.spawn == _spawn_record()


@pytest.mark.usefixtures("supervisor_token")
def test_stale_takeover_cannot_erase_published_worker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A stale take racing a publication loses with the worker intact."""
    table = FakeAuthorityConnection()
    authority.bootstrap_authority(_conn(table), SERVER)
    first = authority.AuthorityOwner(pid=1, start_time_ticks=2, boot_id="boot-1")
    assert authority.take_authority(_conn(table), SERVER, first) is not None
    stale = authority.read_authority(_conn(table), SERVER)
    assert stale is not None
    assert stale.spawn is None
    stale_text = table.rows[str(authority.authority_row_id(SERVER))]
    claim_a = authority.AuthorityClaim(
        server=SERVER, epoch=1, pid=1, start_time_ticks=2, boot_id="boot-1"
    )
    assert authority.commit_spawn_obligation(_conn(table), claim_a, _spawn_record()) is True
    published = authority.WorkerRecord(
        token=OBLIGATION_TOKEN,
        commit=COMMIT,
        pid=4242,
        pgid=4242,
        sid=4242,
        start_time_ticks=777,
        worker_id="w-test",
    )
    won = authority.publish_worker(
        _conn(table), claim_a, spawn_token=OBLIGATION_TOKEN, worker=published
    )
    assert won is True
    monkeypatch.setattr(authority, "_read_observed", lambda _conn, _server: (stale_text, stale))
    try:
        lost = authority.take_authority(
            _conn(table), SERVER, authority.AuthorityOwner(pid=9, start_time_ticks=9, boot_id="b9")
        )
    finally:
        monkeypatch.undo()
    assert lost is None
    row = authority.read_authority(_conn(table), SERVER)
    assert row is not None
    assert row.worker == published
    assert row.spawn is None


def _spawn_section(**overrides: object) -> dict[str, object]:
    """Build a complete spawn section with field overrides.

    Args:
        overrides: Fields to replace in the canonical section.

    Returns:
        The spawn mapping.
    """
    section: dict[str, object] = {
        "token": OBLIGATION_TOKEN,
        "commit": COMMIT,
        "creator_pid": 424242,
        "creator_start_time_ticks": 4242,
        "boot_id": "boot-test",
        "pid": None,
        "start_time_ticks": None,
        "parent_death_signal": True,
    }
    section.update(overrides)
    return section


def _authority_envelope(spawn: object) -> dict[str, object]:
    """Wrap a raw spawn section in a minimal authority envelope.

    Args:
        spawn: The raw ``spawn`` value under test.

    Returns:
        The payload mapping.
    """
    return {
        "v": authority.AUTHORITY_VERSION,
        "type": "lifecycle_authority",
        "server": SERVER,
        "epoch": 0,
        "generation": 0,
        "owner": None,
        "spawn": spawn,
        "worker": None,
    }


def test_parent_death_signal_explicit_booleans_round_trip() -> None:
    """An explicit boolean flag parses; anything else fails closed."""
    for flag in (True, False):
        section = _spawn_section()
        section["parent_death_signal"] = flag
        parsed = authority.parse_authority_payload(_authority_envelope(section), server=SERVER)
        assert parsed.spawn is not None
        assert parsed.spawn.parent_death_signal is flag


def test_parent_death_signal_omission_and_non_bool_fail_closed() -> None:
    """A missing or non-boolean flag never parses as usable authority."""
    bad_flags: tuple[object, ...] = (None, "yes", "true", 1, 0, [], {})
    for flag in bad_flags:
        section = _spawn_section()
        section["parent_death_signal"] = flag
        with pytest.raises(authority.AuthorityError):
            authority.parse_authority_payload(_authority_envelope(section), server=SERVER)
    omitted = _spawn_section()
    del omitted["parent_death_signal"]
    with pytest.raises(authority.AuthorityError):
        authority.parse_authority_payload(_authority_envelope(omitted), server=SERVER)
