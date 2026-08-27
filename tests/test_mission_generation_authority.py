"""Mission generation authority allocation invariants.

Generation allocation must observe the durable supervised-deployment mission
so a newer intent can never be outranked by an older open mission. The
authority rule is strictly absence-vs-corruption:

* genuine mission absence contributes generation 0;
* a present mission authority that cannot yield a trustworthy canonical
  positive integer ``generation`` blocks allocation (fails closed) rather than
  silently degrading to absence.

Allocation stays monotonic and is serialized by the shared generation lock.
"""

from __future__ import annotations

import json
import threading
from pathlib import Path

import pytest

from lubko import deployctl as dc
from lubko import lifecycle, supervise
from lubko.state import rollback_state_path

OLD = "1" * 40
NEW = "2" * 40


def _meta(commit: str, pid: int) -> lifecycle.WorkerMeta:
    """Return a minimal valid worker metadata record."""
    return lifecycle.WorkerMeta(
        schema_version=lifecycle.SCHEMA_VERSION,
        state=lifecycle.STATE_RUNNING,
        pid=pid,
        pgid=pid,
        sid=pid,
        start_time_ticks=pid,
        token=f"token-{pid}",
        repo="/workspace/repo",
        git_commit=commit,
        worker_id="w",
        log_path="",
        started_at=1.0,
        stopped_at=None,
    )


def _mission(
    generation: int, status: str = dc.STATUS_PENDING, commit: str = NEW
) -> dc.RollbackState:
    """Return a minimal valid supervised-deployment mission."""
    meta = _meta(commit, 200)
    return dc.RollbackState(
        schema_version=dc.ROLLBACK_SCHEMA_VERSION,
        generation=generation,
        status=status,
        commit=commit,
        previous_commit=OLD,
        challenge_hash=None,
        deadline=0.0,
        repo="/workspace/repo",
        uv_path="uv",
        stop_grace_seconds=1.0,
        git_timeout_seconds=5.0,
        previous_retiring=False,
        previous_meta=meta,
        new_meta=meta,
        supervisor_owned=True,
    )


def _write_mission(state: dc.RollbackState) -> None:
    dc._write_state(state)


def _write_raw(text: str) -> None:
    path = rollback_state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def test_absent_mission_contributes_zero() -> None:
    """Genuine mission absence leaves allocation driven only by applied/desired."""
    assert not rollback_state_path().exists()
    assert supervise._mission_generation() == 0
    assert supervise.next_generation() == 1


def test_present_valid_mission_participates_monotonically() -> None:
    """An open mission's generation is strictly observed by allocation."""
    _write_mission(_mission(5))
    assert supervise._mission_generation() == 5
    assert supervise.next_generation() == 6


def test_present_valid_mission_outranks_older_desired() -> None:
    """A valid mission generation is not outranked by an older desired intent."""
    supervise.write_desired(
        supervise.SupervisorDesired(
            schema_version=supervise.SCHEMA_VERSION,
            generation=3,
            commit=OLD,
            repo="/workspace/repo",
            uv_path="uv",
            worker_id=None,
        )
    )
    _write_mission(_mission(5))
    assert supervise.next_generation() == 6


def test_present_unreadable_mission_blocks_without_overwrite() -> None:
    """A present directory at the authority path is corruption, not absence."""
    path = rollback_state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.mkdir()
    with pytest.raises(supervise.MissionAuthorityError):
        supervise._mission_generation()
    assert path.is_dir()


def test_present_malformed_json_blocks_without_overwrite() -> None:
    """Invalid JSON is corruption and the authority is left untouched."""
    _write_raw("{this is not valid json")
    with pytest.raises(supervise.MissionAuthorityError):
        supervise._mission_generation()
    assert rollback_state_path().read_text(encoding="utf-8") == "{this is not valid json"


def test_present_non_object_blocks() -> None:
    """A present non-object JSON value is corruption."""
    _write_raw("123")
    with pytest.raises(supervise.MissionAuthorityError):
        supervise._mission_generation()


def test_present_missing_generation_blocks() -> None:
    """A present object missing a generation is corruption."""
    _write_raw(json.dumps({"schema_version": dc.ROLLBACK_SCHEMA_VERSION}))
    with pytest.raises(supervise.MissionAuthorityError):
        supervise._mission_generation()


def test_present_string_generation_blocks() -> None:
    """A present non-integer (string) generation is corruption."""
    payload = _mission(5).to_dict()
    payload["generation"] = "5"
    _write_raw(json.dumps(payload))
    with pytest.raises(supervise.MissionAuthorityError):
        supervise._mission_generation()


def test_present_bool_generation_blocks() -> None:
    """A present boolean generation is corruption, never trusted as 1."""
    payload = _mission(5).to_dict()
    payload["generation"] = True
    _write_raw(json.dumps(payload))
    with pytest.raises(supervise.MissionAuthorityError):
        supervise._mission_generation()


def test_present_nonpositive_generation_blocks() -> None:
    """A present zero or negative generation is corruption."""
    payload = _mission(5).to_dict()
    payload["generation"] = -3
    _write_raw(json.dumps(payload))
    with pytest.raises(supervise.MissionAuthorityError):
        supervise._mission_generation()


def test_recovery_after_explicit_removal() -> None:
    """Removing corrupt authority restores genuine absence."""
    _write_raw("not json")
    with pytest.raises(supervise.MissionAuthorityError):
        supervise._mission_generation()
    rollback_state_path().unlink()
    assert supervise._mission_generation() == 0
    assert supervise.next_generation() == 1


def test_recovery_after_repair() -> None:
    """Repairing the authority into a valid mission restores participation."""
    _write_raw("not json")
    with pytest.raises(supervise.MissionAuthorityError):
        supervise._mission_generation()
    _write_mission(_mission(7))
    assert supervise._mission_generation() == 7
    assert supervise.next_generation() == 8


def test_deployctl_mission_generation_fails_closed_on_corrupt() -> None:
    """The deployctl allocation entry point fails closed and recovers cleanly."""
    _write_raw("{bad")
    with pytest.raises(dc.DeployCtlError):
        dc.next_mission_generation()
    rollback_state_path().unlink()
    assert dc.next_mission_generation() == 1


def test_request_run_refuses_corrupt_mission_without_overwrite() -> None:
    """An ordinary run request refuses and never writes a desired intent."""
    _write_raw("not json")
    desired = supervise.desired_path()
    with pytest.raises(supervise.MissionAuthorityError):
        supervise.request_run(NEW, repo="/workspace/repo", uv_path="uv", worker_id=None)
    assert not desired.exists()
    assert rollback_state_path().read_text(encoding="utf-8") == "not json"


def test_concurrent_monotonic_allocation_under_shared_lock() -> None:
    """Concurrent allocations never collide or reuse a generation.

    The shared generation lock serializes each allocation, so every returned
    generation is unique and the set forms the contiguous window above the
    mission floor. Collection order is scheduler-dependent (appends happen
    after the lock is released), so only the set/range are asserted.
    """
    _write_mission(_mission(5))  # floor at generation 6
    iterations = 10
    thread_count = 3
    expected = iterations * thread_count
    generations: list[int] = []
    lock = threading.Lock()

    def allocate() -> None:
        for _ in range(iterations):
            with supervise.generation_lock():
                generation = supervise.next_generation()
                supervise.write_desired(
                    supervise.SupervisorDesired(
                        schema_version=supervise.SCHEMA_VERSION,
                        generation=generation,
                        commit=OLD,
                        repo="/workspace/repo",
                        uv_path="uv",
                        worker_id=None,
                    )
                )
            with lock:
                generations.append(generation)

    threads = [threading.Thread(target=allocate) for _ in range(thread_count)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(generations) == expected
    # No duplicate or reused generation, and exactly the contiguous window above
    # the mission floor: 6 .. 6 + expected - 1.
    assert sorted(set(generations)) == list(range(6, 6 + expected))


def test_lifecycle_migration_recovers_from_corrupt_mission() -> None:
    """An explicit recovery path supersedes a corrupt mission before allocating."""
    _write_raw("not json")
    assert rollback_state_path().exists()
    with lifecycle.deploy_lock(0.0):
        lifecycle._migrate_locked(NEW, Path("/workspace/repo"), "uv")
    # The corrupt authority was explicitly superseded, not left in place.
    assert not rollback_state_path().exists()
    desired = supervise.read_desired_strict()
    assert desired is not None
    assert desired.generation >= 1
