"""Canonical v2 authority, one-time v1 migration, and DB-only steady state.

The lifecycle authority row has exactly one canonical schema (v2 with
explicit ``spawn`` and ``worker`` keys). Normal parsing rejects v1 rows;
a single startup migration boundary converts them atomically. In steady
state, destructive maintained-worker actions require the canonical DB row
plus kernel liveness proof; stale local caches never authorize action,
and missing database authority holds fail-closed.
"""

from __future__ import annotations

import errno
import json
from dataclasses import replace
from typing import TYPE_CHECKING, cast

import pytest

from lubko import lifecycle, supervise, supervisor
from lubko import lifecycle_authority as authority

if TYPE_CHECKING:
    from lubko.worker import JobsConnection

from tests._fake_authority_db import FakeAuthorityConnection, seed_db_worker

SERVER = "srv-v2-test"
COMMIT = "b" * 40
WORKER_TOKEN = f"worker-token-{807}"
SPAWN_TOKEN = f"spawn-token-{807}"
STALE_TOKEN = f"stale-token-{807}"


def _conn(table: FakeAuthorityConnection) -> JobsConnection:
    """Expose a fake authority table as a database connection.

    Args:
        table: The fake authority connection backing the call.

    Returns:
        The fake connection, cast to the production connection type.
    """
    return cast("JobsConnection", table)


def _owner() -> authority.AuthorityOwner:
    """Build an exact test owner identity.

    Returns:
        An owner with fixed test identity fields.
    """
    return authority.AuthorityOwner(pid=100, start_time_ticks=200, boot_id="boot-1")


def _worker() -> authority.WorkerRecord:
    """Build an exact test worker record.

    Returns:
        A worker record naming the fixed test incarnation.
    """
    return authority.WorkerRecord(
        token=WORKER_TOKEN,
        commit=COMMIT,
        pid=4242,
        pgid=4242,
        sid=4242,
        start_time_ticks=777,
        worker_id="worker-1",
    )


def _spawn() -> authority.SpawnObligation:
    """Build an exact test spawn obligation.

    Returns:
        A pid-less obligation naming the fixed test incarnation.
    """
    return authority.SpawnObligation(
        token=SPAWN_TOKEN,
        commit=COMMIT,
        creator_pid=100,
        creator_start_time_ticks=200,
        boot_id="boot-1",
        pid=None,
        start_time_ticks=None,
        parent_death_signal=True,
    )


def _v1_text(
    *,
    epoch: int = 0,
    generation: int = 0,
    spawn: object = "__absent__",
    worker: object = "__absent__",
) -> str:
    """Serialize an exact legacy v1 row (keys may be absent).

    Args:
        epoch: Fencing epoch carried by the legacy row.
        generation: Lifecycle generation carried by the legacy row.
        spawn: Raw ``spawn`` value, or absent when unset.
        worker: Raw ``worker`` value, or absent when unset.

    Returns:
        Canonical sorted-key JSON text of the legacy row.
    """
    payload: dict[str, object] = {
        "v": 1,
        "type": "lifecycle_authority",
        "server": SERVER,
        "epoch": epoch,
        "generation": generation,
        "owner": None,
    }
    if spawn != "__absent__":
        payload["spawn"] = spawn
    if worker != "__absent__":
        payload["worker"] = worker
    return json.dumps(payload, sort_keys=True)


def _seed_v1(table: FakeAuthorityConnection, text: str) -> None:
    """Seed the fake table with a legacy v1 row.

    Args:
        table: The fake authority connection backing the call.
        text: Exact legacy payload text to store.
    """
    table.rows[str(authority.authority_row_id(SERVER))] = text


def test_normal_parser_rejects_v1_and_missing_keys() -> None:
    """Strict v2 parsing accepts only the exact new shape."""
    with pytest.raises(authority.AuthorityError):
        authority.parse_authority_payload(_v1_text(), server=SERVER)
    envelope: dict[str, object] = {
        "v": authority.AUTHORITY_VERSION,
        "type": authority.AUTHORITY_TYPE,
        "server": SERVER,
        "epoch": 0,
        "generation": 0,
        "owner": None,
        "spawn": None,
    }
    with pytest.raises(authority.AuthorityError):
        authority.parse_authority_payload(envelope, server=SERVER)
    envelope = {
        "v": authority.AUTHORITY_VERSION,
        "type": authority.AUTHORITY_TYPE,
        "server": SERVER,
        "epoch": 0,
        "generation": 0,
        "owner": None,
        "worker": None,
    }
    with pytest.raises(authority.AuthorityError):
        authority.parse_authority_payload(envelope, server=SERVER)


def test_neutral_migration_converts_v1_to_explicit_v2() -> None:
    """A v1 row without legacy state becomes explicit-null v2 atomically."""
    table = FakeAuthorityConnection()
    _seed_v1(table, _v1_text(epoch=3, generation=7))
    assert authority.migrate_v1_to_v2(_conn(table), SERVER) is True
    row = authority.read_authority(_conn(table), SERVER)
    assert row == authority.AuthorityRow(
        server=SERVER, epoch=3, generation=7, owner=None, spawn=None, worker=None
    )
    stored = table.rows[str(authority.authority_row_id(SERVER))]
    assert json.loads(stored)["spawn"] is None
    assert json.loads(stored)["worker"] is None
    assert json.loads(stored)["v"] == authority.AUTHORITY_VERSION
    assert authority.migrate_v1_to_v2(_conn(table), SERVER) is True


def test_migration_transfers_live_worker_identity() -> None:
    """A proven live maintained worker is encoded in the v2 record."""
    table = FakeAuthorityConnection()
    _seed_v1(table, _v1_text())
    evidence = authority.V1LegacyEvidence(live_worker=_worker())
    assert authority.migrate_v1_to_v2(_conn(table), SERVER, evidence) is True
    row = authority.read_authority(_conn(table), SERVER)
    assert row is not None
    assert row.worker == _worker()
    assert row.spawn is None


def test_migration_transfers_blocking_spawn_obligation() -> None:
    """A proven blocking incarnation survives as the v2 spawn obligation."""
    table = FakeAuthorityConnection()
    _seed_v1(table, _v1_text())
    evidence = authority.V1LegacyEvidence(spawn=_spawn())
    assert authority.migrate_v1_to_v2(_conn(table), SERVER, evidence) is True
    row = authority.read_authority(_conn(table), SERVER)
    assert row is not None
    assert row.spawn == _spawn()
    assert row.worker is None


@pytest.mark.parametrize(
    "evidence",
    [
        authority.V1LegacyEvidence(worker_ambiguous=True),
        authority.V1LegacyEvidence(spawn_ambiguous=True),
        authority.V1LegacyEvidence(live_worker=_worker(), spawn=_spawn()),
    ],
)
def test_migration_leaves_v1_untouched_on_ambiguity(
    evidence: authority.V1LegacyEvidence,
) -> None:
    """Malformed or ambiguous legacy state fails closed on v1."""
    table = FakeAuthorityConnection()
    original = _v1_text()
    _seed_v1(table, original)
    with pytest.raises(authority.AuthorityError):
        authority.migrate_v1_to_v2(_conn(table), SERVER, evidence)
    assert table.rows[str(authority.authority_row_id(SERVER))] == original


def test_migration_cas_race_leaves_winner_intact() -> None:
    """A migration onto an already-migrated row converges without clobbering."""
    table = FakeAuthorityConnection()
    _seed_v1(table, _v1_text())
    winner = authority.AuthorityRow(
        server=SERVER, epoch=5, generation=5, owner=_owner(), spawn=None, worker=None
    )
    table.rows[str(authority.authority_row_id(SERVER))] = authority.canonical_row_text(winner)
    assert authority.migrate_v1_to_v2(_conn(table), SERVER) is True
    assert authority.read_authority(_conn(table), SERVER) == winner


def test_stale_migration_cas_race_returns_false(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A migration losing the commit race reports it instead of acting."""
    table = FakeAuthorityConnection()
    original = _v1_text()
    _seed_v1(table, original)
    monkeypatch.setattr(authority, "_compare_and_swap", lambda *_a, **_k: False)
    assert authority.migrate_v1_to_v2(_conn(table), SERVER) is False
    assert table.rows[str(authority.authority_row_id(SERVER))] == original


def test_stale_migration_guard_never_overwrites_concurrent_change() -> None:
    """Direct CAS with a superseded expectation fails instead of overwriting."""
    table = FakeAuthorityConnection()
    authority.bootstrap_authority(_conn(table), SERVER)
    stale_text = table.rows[str(authority.authority_row_id(SERVER))]
    authority.take_authority(_conn(table), SERVER, _owner())
    assert authority._compare_and_swap(_conn(table), SERVER, stale_text, stale_text) is False
    assert authority.read_authority(_conn(table), SERVER) is not None


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


OBLIGATION_TOKEN = f"obligation-token-{807}"


@pytest.mark.usefixtures("supervisor_token")
def test_stale_local_cache_never_authorizes_destruction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A stale cache beside an empty v2 row causes no signal or retirement."""
    table = FakeAuthorityConnection()
    daemon = _claim_daemon(monkeypatch, table)
    stale_child = supervise.WorkerChild(
        pid=999999,
        pgid=999999,
        sid=999999,
        start_time_ticks=111,
        token=STALE_TOKEN,
        worker_id="stale-worker",
        spawned_at=0.0,
    )
    state = supervise.read_state()
    supervise.write_state(replace(state, child=stale_child, commit=COMMIT))
    calls: list[str] = []

    def _refuse_stop(_meta: object, _grace: float) -> bool:
        calls.append("stop")
        return True

    monkeypatch.setattr(lifecycle, "stop_worker", _refuse_stop)
    assert daemon._retire_child() is False
    assert calls == []
    assert supervise.read_state().child == stale_child


@pytest.mark.usefixtures("supervisor_token")
def test_missing_authority_holds_every_gate(monkeypatch: pytest.MonkeyPatch) -> None:
    """Without an established claim no spawn-related gate may pass."""
    monkeypatch.setattr(supervisor, "load_worker_server", lambda: SERVER)
    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    assert daemon._authority is None
    assert daemon._confirm_authority() is False
    assert daemon._gate_db_spawn() is False
    assert daemon._reconcile_db_worker(COMMIT) is False
    assert daemon._db_worker_running(COMMIT) is None
    assert (
        daemon._commit_spawn_authority(
            supervise.SpawningObligation(
                token=OBLIGATION_TOKEN,
                commit=COMMIT,
                creator_pid=1,
                creator_start_time_ticks=2,
                pid=None,
                start_time_ticks=None,
                created_at=0.0,
                boot_id="boot-1",
                parent_death_signal=True,
            )
        )
        is False
    )


def _unclaimed_daemon(
    monkeypatch: pytest.MonkeyPatch, table: FakeAuthorityConnection
) -> supervisor.SupervisorDaemon:
    """Build a daemon whose fencing claim is not established yet.

    Args:
        monkeypatch: The active monkeypatch fixture.
        table: The fake authority connection backing every row access.

    Returns:
        A daemon pointed at the fake table without a claim.
    """
    monkeypatch.setattr(supervisor, "load_worker_server", lambda: SERVER)
    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    daemon._authority_conn_factory = lambda: _conn(table)
    return daemon


def _write_local_spawning() -> None:
    """Persist a pid-less blocking pre-spawn obligation in local state."""
    obligation = supervise.SpawningObligation(
        token=SPAWN_TOKEN,
        commit=COMMIT,
        creator_pid=7,
        creator_start_time_ticks=8,
        pid=None,
        start_time_ticks=None,
        created_at=0.0,
        boot_id="boot-1",
        parent_death_signal=True,
    )
    supervise.write_state(replace(supervise.read_state(), spawning=obligation))


def _live_meta() -> lifecycle.WorkerMeta:
    """Build an exact live maintained-worker metadata record.

    Returns:
        Metadata naming the fixed test worker incarnation.
    """
    return lifecycle.WorkerMeta(
        schema_version=lifecycle.SCHEMA_VERSION,
        state=lifecycle.STATE_RUNNING,
        pid=4242,
        pgid=4242,
        sid=4242,
        start_time_ticks=777,
        token=WORKER_TOKEN,
        repo="/workspace/repo",
        git_commit=COMMIT,
        worker_id="worker-1",
        log_path="",
        started_at=None,
        stopped_at=None,
    )


@pytest.mark.usefixtures("supervisor_token")
def test_startup_migrates_v1_blocking_spawn_before_bootstrap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Startup converts an existing v1 row before any strict v2 take."""
    table = FakeAuthorityConnection()
    _seed_v1(table, _v1_text())
    _write_local_spawning()
    daemon = _unclaimed_daemon(monkeypatch, table)
    daemon._write_pidfile()
    row = authority.read_authority(_conn(table), SERVER)
    assert row is not None
    assert row.spawn is not None
    assert row.spawn.token == SPAWN_TOKEN
    assert row.worker is None
    assert daemon._authority is not None


@pytest.mark.usefixtures("supervisor_token")
def test_startup_migrates_v1_live_worker_before_bootstrap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Startup encodes a proven live worker into the migrated v2 row."""
    table = FakeAuthorityConnection()
    _seed_v1(table, _v1_text())
    monkeypatch.setattr(lifecycle, "read_meta_strict", _live_meta)
    monkeypatch.setattr(lifecycle, "worker_alive", lambda _meta: True)
    daemon = _unclaimed_daemon(monkeypatch, table)
    daemon._write_pidfile()
    row = authority.read_authority(_conn(table), SERVER)
    assert row is not None
    assert row.worker == _worker()
    assert row.spawn is None
    assert daemon._authority is not None


@pytest.mark.usefixtures("supervisor_token")
def test_startup_migrates_v1_unresolved_hold_as_blocking_spawn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Startup carries an unresolved hold's exact identity into v2."""
    table = FakeAuthorityConnection()
    _seed_v1(table, _v1_text())
    hold = supervise.UnresolvedChild(
        pid=999, start_time_ticks=111, token=SPAWN_TOKEN, spawned_at=0.0
    )
    supervise.write_state(replace(supervise.read_state(), commit=COMMIT, unresolved_child=hold))
    daemon = _unclaimed_daemon(monkeypatch, table)
    daemon._write_pidfile()
    row = authority.read_authority(_conn(table), SERVER)
    assert row is not None
    assert row.spawn is not None
    assert row.spawn.pid == 999
    assert row.spawn.start_time_ticks == 111
    assert row.spawn.token == SPAWN_TOKEN
    assert daemon._authority is not None


@pytest.mark.usefixtures("supervisor_token")
def test_startup_leaves_v1_untouched_on_ambiguous_legacy_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Malformed legacy state fails startup closed without touching v1."""
    table = FakeAuthorityConnection()
    original = _v1_text()
    _seed_v1(table, original)

    def _torn_meta() -> lifecycle.WorkerMeta | None:
        msg = "torn legacy meta"
        raise lifecycle.WorkerMetadataError(msg)

    monkeypatch.setattr(lifecycle, "read_meta_strict", _torn_meta)
    daemon = _unclaimed_daemon(monkeypatch, table)
    with pytest.raises(SystemExit):
        daemon._write_pidfile()
    assert table.rows[str(authority.authority_row_id(SERVER))] == original
    assert daemon._authority is None


@pytest.mark.usefixtures("supervisor_token")
def test_startup_passes_an_existing_v2_row_through_untouched(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An already-v2 row needs no migration at startup."""
    table = FakeAuthorityConnection()
    authority.bootstrap_authority(_conn(table), SERVER)
    before = table.rows[str(authority.authority_row_id(SERVER))]
    daemon = _unclaimed_daemon(monkeypatch, table)
    daemon._write_pidfile()
    row = authority.read_authority(_conn(table), SERVER)
    assert row is not None
    assert row.spawn is None
    assert row.worker is None
    assert daemon._authority is not None
    assert json.loads(before)["v"] == authority.AUTHORITY_VERSION


@pytest.mark.parametrize("field", ["pid", "start_time_ticks"])
@pytest.mark.usefixtures("supervisor_token")
def test_retire_child_holds_on_partial_identity_mismatch(
    monkeypatch: pytest.MonkeyPatch, field: str
) -> None:
    """A same-token record with a recycled PID or start time authorizes nothing."""
    table = FakeAuthorityConnection()
    daemon = _claim_daemon(monkeypatch, table)
    cached = supervise.WorkerChild(
        pid=4242,
        pgid=4242,
        sid=4242,
        start_time_ticks=99,
        token=STALE_TOKEN,
        worker_id="stale-worker",
        spawned_at=0.0,
    )
    supervise.write_state(replace(supervise.read_state(), child=cached, commit=COMMIT))
    mismatched = {"pid": 4243, "start_time_ticks": 100}
    record = authority.WorkerRecord(
        token=STALE_TOKEN,
        commit=COMMIT,
        pid=mismatched["pid"] if field == "pid" else 4242,
        pgid=4242,
        sid=4242,
        start_time_ticks=99 if field == "pid" else mismatched["start_time_ticks"],
        worker_id="stale-worker",
    )
    seed_db_worker(daemon, record)
    calls: list[str] = []

    def _refuse_stop(_meta: object, _grace: float) -> bool:
        calls.append("stop")
        return True

    monkeypatch.setattr(lifecycle, "stop_worker", _refuse_stop)
    assert daemon._retire_child() is False
    assert calls == []
    assert supervise.read_state().child == cached


@pytest.mark.parametrize("cache", ["state", "meta"])
@pytest.mark.usefixtures("supervisor_token")
def test_eio_cache_failure_after_db_publication_keeps_worker(
    monkeypatch: pytest.MonkeyPatch, cache: str
) -> None:
    """A non-capacity cache failure cannot revoke a DB-published worker."""
    table = FakeAuthorityConnection()
    daemon = _claim_daemon(monkeypatch, table)
    conn = daemon._spawn_authority_connection()
    claim = daemon._authority
    assert conn is not None
    assert claim is not None
    assert authority.commit_spawn_obligation(conn, claim, _spawn()) is True
    child = supervise.WorkerChild(
        pid=4242,
        pgid=4242,
        sid=4242,
        start_time_ticks=99,
        token=SPAWN_TOKEN,
        worker_id="worker-1",
        spawned_at=0.0,
    )
    worker = authority.WorkerRecord(
        token=SPAWN_TOKEN,
        commit=COMMIT,
        pid=4242,
        pgid=4242,
        sid=4242,
        start_time_ticks=99,
        worker_id="worker-1",
    )
    assert authority.publish_worker(conn, claim, spawn_token=SPAWN_TOKEN, worker=worker) is True

    def _eio(_payload: object) -> None:
        raise OSError(errno.EIO, "I/O error")

    if cache == "state":
        monkeypatch.setattr(supervisor, "write_state", _eio)
        monkeypatch.setattr(lifecycle, "write_meta", lambda _meta: None)
    else:
        monkeypatch.setattr(lifecycle, "write_meta", _eio)
    obligation = supervise.SpawningObligation(
        token=SPAWN_TOKEN,
        commit=COMMIT,
        creator_pid=100,
        creator_start_time_ticks=200,
        pid=None,
        start_time_ticks=None,
        created_at=0.0,
        boot_id="boot-1",
        parent_death_signal=True,
    )
    daemon._cache_published_child(child, COMMIT, 0.0, obligation)
    row = authority.read_authority(_conn(table), SERVER)
    assert row is not None
    assert row.worker == worker
    assert row.spawn is None
