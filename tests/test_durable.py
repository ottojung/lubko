"""Fault-injection regressions for the crash-durable write primitive.

These tests exercise the storage-confirmation boundary of
:mod:`lubko.durable`: a durable write must be fully written + fsynced,
atomically renamed into place, and have its directory fsynced; any failure to
confirm must raise :class:`lubko.durable.DurabilityError`, leave the previous
value intact, and leave no in-progress temporary artifact behind. Concurrent
readers must always observe a complete value. Callers that depend on an
authoritative write must not advance an irreversible action when the write is
not confirmed.
"""

from __future__ import annotations

import threading
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from lubko import cli, deployctl, durable, lifecycle, supervise, toolchain
from lubko.durable import (
    FSYNC_STAGE_DIR,
    FSYNC_STAGE_FILE,
    FSYNC_STAGE_REPLACE,
    DurabilityError,
    clear_fsync_failure_injector,
    set_fsync_failure_injector,
    write_bytes_durable,
    write_json_durable,
    write_symlink_durable,
)

if TYPE_CHECKING:
    from collections.abc import Iterator


class _InjectedFsyncError(OSError):
    """Injected storage-confirmation failure used by the fault-injection tests."""


@pytest.fixture(autouse=True)
def _no_injector_leak() -> Iterator[None]:
    """Clear any fault injector installed by a test."""
    yield
    clear_fsync_failure_injector()


def _raise_at(stage: str) -> None:
    """Install a fault injector that raises at a single confirmation stage.

    Args:
        stage: One of the ``FSYNC_STAGE_*`` constants.
    """

    def _inject(_path: Path, current: str) -> None:
        if current == stage:
            raise _InjectedFsyncError

    set_fsync_failure_injector(_inject)


def _temp_artifacts(directory: Path) -> list[Path]:
    """Return any in-progress durable temporary files under ``directory``.

    Args:
        directory: Directory to scan.

    Returns:
        The list of temporary paths (should always be empty after a write).
    """
    return sorted(p for p in directory.iterdir() if p.name.startswith(".lubko-durable-"))


def _seed(path: Path, data: bytes) -> None:
    """Write an initial durable value with no fault injection.

    Args:
        path: Destination path.
        data: Initial payload.
    """
    clear_fsync_failure_injector()
    write_bytes_durable(path, data)


# ---------------------------------------------------------------------------
# File fsync failure
# ---------------------------------------------------------------------------


def test_file_fsync_failure_preserves_previous_and_cleans_temp(tmp_path: Path) -> None:
    """A file-fsync failure must not corrupt or overwrite the previous value."""
    path = tmp_path / "state.json"
    _seed(path, b"previous")
    _raise_at(FSYNC_STAGE_FILE)
    with pytest.raises(DurabilityError):
        write_bytes_durable(path, b"next")
    assert path.read_bytes() == b"previous"
    assert _temp_artifacts(tmp_path) == []


# ---------------------------------------------------------------------------
# Replace boundary failure
# ---------------------------------------------------------------------------


def test_replace_boundary_failure_preserves_previous_and_cleans_temp(tmp_path: Path) -> None:
    """A rename failure must leave the previous value intact and no temp file."""
    path = tmp_path / "state.json"
    _seed(path, b"previous")
    _raise_at(FSYNC_STAGE_REPLACE)
    with pytest.raises(DurabilityError):
        write_bytes_durable(path, b"next")
    assert path.read_bytes() == b"previous"
    assert _temp_artifacts(tmp_path) == []


# ---------------------------------------------------------------------------
# Directory fsync failure
# ---------------------------------------------------------------------------


def test_dir_fsync_failure_fails_closed_and_cleans_temp(tmp_path: Path) -> None:
    """A directory-fsync failure must fail closed and leave no temp artifact."""
    path = tmp_path / "state.json"
    _seed(path, b"previous")
    _raise_at(FSYNC_STAGE_DIR)
    with pytest.raises(DurabilityError):
        write_bytes_durable(path, b"next")
    # The rename may have occurred, but the entry is not confirmed durable; the
    # caller must treat the write as unconfirmed.
    assert _temp_artifacts(tmp_path) == []


# ---------------------------------------------------------------------------
# Temporary artifact safety
# ---------------------------------------------------------------------------


def test_success_leaves_no_temporary_artifact(tmp_path: Path) -> None:
    """A successful durable write must not leave an in-progress temporary file."""
    path = tmp_path / "state.json"
    write_bytes_durable(path, b"first")
    write_bytes_durable(path, b"second")
    assert path.read_bytes() == b"second"
    assert _temp_artifacts(tmp_path) == []


def test_temporary_names_are_unique_per_write(tmp_path: Path) -> None:
    """Concurrent writes must use distinct temporary names so they never clash."""
    path = tmp_path / "state.json"
    names = {durable.temporary_path(path) for _ in range(1000)}
    assert len(names) == 1000


# ---------------------------------------------------------------------------
# Concurrent complete reads
# ---------------------------------------------------------------------------


def test_concurrent_reads_observe_only_complete_values(tmp_path: Path) -> None:
    """Concurrent readers must never observe a torn or partial durable value."""
    path = tmp_path / "state.json"
    payload_a = b"a" * 4096
    payload_b = b"b" * 4096
    _seed(path, payload_a)

    stop = threading.Event()

    def writer() -> None:
        toggle = False
        while not stop.is_set():
            write_bytes_durable(path, payload_b if toggle else payload_a)
            toggle = not toggle

    def reader() -> None:
        while not stop.is_set():
            data = path.read_bytes()
            assert data in {payload_a, payload_b}

    threads = [threading.Thread(target=writer)] + [
        threading.Thread(target=reader) for _ in range(4)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5.0)
        stop.set()
    for thread in threads:
        thread.join(timeout=5.0)


def test_concurrent_writers_do_not_leave_temporary_artifacts(tmp_path: Path) -> None:
    """Concurrent durable writers must converge without leftover temp files."""
    path = tmp_path / "state.json"
    stop = threading.Event()

    def writer() -> None:
        while not stop.is_set():
            write_bytes_durable(path, b"x" * 2048)

    threads = [threading.Thread(target=writer) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5.0)
        stop.set()
    for thread in threads:
        thread.join(timeout=5.0)
    assert path.read_bytes() == b"x" * 2048
    assert _temp_artifacts(tmp_path) == []


# ---------------------------------------------------------------------------
# Symlink durable pointer
# ---------------------------------------------------------------------------


def test_symlink_file_stage_failure_cleans_temp(tmp_path: Path) -> None:
    """A symlink pre-rename failure must remove the temporary symlink."""
    path = tmp_path / "current"
    path.symlink_to("a" * 40)
    _raise_at(FSYNC_STAGE_FILE)
    with pytest.raises(DurabilityError):
        write_symlink_durable(path, "b" * 40)
    assert path.readlink() == Path("a" * 40)
    assert _temp_artifacts(tmp_path) == []


def test_symlink_dir_stage_failure_fails_closed(tmp_path: Path) -> None:
    """A symlink directory-fsync failure must fail closed."""
    path = tmp_path / "current"
    path.symlink_to("a" * 40)
    _raise_at(FSYNC_STAGE_DIR)
    with pytest.raises(DurabilityError):
        write_symlink_durable(path, "b" * 40)
    assert _temp_artifacts(tmp_path) == []


# ---------------------------------------------------------------------------
# Directory durability (bounded, not root-walking)
# ---------------------------------------------------------------------------


def test_make_directory_durable_creates_nested_hierarchy(tmp_path: Path) -> None:
    """A missing nested hierarchy must be created and durably anchored."""
    target = tmp_path / "a" / "b" / "c" / "d"
    durable.make_directory_durable(target)
    assert target.is_dir()
    assert _temp_artifacts(tmp_path) == []


def test_make_directory_durable_is_idempotent_for_existing(tmp_path: Path) -> None:
    """Re-anchoring an existing directory must be safe and leave no temp file."""
    target = tmp_path / "a" / "b"
    target.mkdir(parents=True)
    durable.make_directory_durable(target)
    durable.make_directory_durable(target)
    assert target.is_dir()
    assert _temp_artifacts(tmp_path) == []


# ---------------------------------------------------------------------------
# Wired authoritative call sites fail closed
# ---------------------------------------------------------------------------


def _sample_worker_meta() -> lifecycle.WorkerMeta:
    """Build a minimal valid worker metadata record.

    Returns:
        A worker metadata instance for durable persistence tests.
    """
    return lifecycle.WorkerMeta.from_dict({
        "schema_version": lifecycle.SCHEMA_VERSION,
        "state": "running",
        "pid": None,
        "pgid": None,
        "sid": None,
        "start_time_ticks": None,
        "token": None,
        "repo": "/repo",
        "git_commit": "a" * 40,
        "worker_id": "w1",
        "log_path": "/log",
        "started_at": 1.0,
        "stopped_at": None,
    })


def test_write_meta_dir_fsync_failure_raises() -> None:
    """``lifecycle.write_meta`` must fail closed on a directory-fsync failure."""
    lifecycle.write_meta(_sample_worker_meta())
    _raise_at(FSYNC_STAGE_DIR)
    with pytest.raises(DurabilityError):
        lifecycle.write_meta(_sample_worker_meta())


def test_write_desired_file_fsync_failure_raises() -> None:
    """``supervise.write_desired`` must fail closed on a file-fsync failure."""
    desired = supervise.SupervisorDesired(
        schema_version=supervise.SCHEMA_VERSION,
        generation=1,
        commit="a" * 40,
        repo="/repo",
        uv_path="/uv",
        worker_id=None,
    )
    supervise.write_desired(desired)
    _raise_at(FSYNC_STAGE_FILE)
    with pytest.raises(DurabilityError):
        supervise.write_desired(desired)


def test_write_state_dir_fsync_failure_raises() -> None:
    """``supervise.write_state`` must fail closed on a directory-fsync failure."""
    state = supervise.fresh_state()
    supervise.write_state(state)
    _raise_at(FSYNC_STAGE_DIR)
    with pytest.raises(DurabilityError):
        supervise.write_state(state)


def test_write_supervisor_pid_file_fsync_failure_raises() -> None:
    """``supervise.write_supervisor_pid`` must fail closed on file-fsync failure."""
    supervise.write_supervisor_pid(123, 456)
    _raise_at(FSYNC_STAGE_FILE)
    with pytest.raises(DurabilityError):
        supervise.write_supervisor_pid(123, 456)


def test_write_supervisor_runtime_override_file_fsync_failure_raises() -> None:
    """The runtime override must fail closed on a file-fsync failure."""
    supervise.write_supervisor_runtime_override("a" * 40)
    _raise_at(FSYNC_STAGE_FILE)
    with pytest.raises(DurabilityError):
        supervise.write_supervisor_runtime_override("a" * 40)


def _sample_rollback_state() -> deployctl.RollbackState:
    """Build a minimal valid rollback mission.

    Returns:
        A rollback state instance for durable persistence tests.
    """
    meta = lifecycle.WorkerMeta.from_dict({
        "schema_version": lifecycle.SCHEMA_VERSION,
        "state": "running",
        "repo": "/repo",
        "git_commit": "a" * 40,
        "worker_id": "w1",
        "log_path": "/log",
    })
    return deployctl.RollbackState.from_dict({
        "schema_version": deployctl.ROLLBACK_SCHEMA_VERSION,
        "generation": 1,
        "status": deployctl.STATUS_CONFIRMED,
        "commit": "a" * 40,
        "previous_commit": "b" * 40,
        "challenge_hash": "0" * 64,
        "deadline": 1.0,
        "repo": "/repo",
        "uv_path": "/uv",
        "stop_grace_seconds": 5.0,
        "git_timeout_seconds": 10.0,
        "previous_retiring": False,
        "previous_meta": meta.to_dict(),
        "new_meta": meta.to_dict(),
    })


def test_write_rollback_state_dir_fsync_failure_raises() -> None:
    """``deployctl`` rollback persistence must fail closed on dir-fsync failure."""
    deployctl.archive_mission(_sample_rollback_state(), deployctl.STATUS_ROLLED_BACK)
    _raise_at(FSYNC_STAGE_DIR)
    with pytest.raises(DurabilityError):
        deployctl.archive_mission(_sample_rollback_state(), deployctl.STATUS_ROLLED_BACK)


def test_write_toolchain_file_fsync_failure_raises() -> None:
    """``toolchain.write_toolchain`` must fail closed on a file-fsync failure."""
    toolchain.write_toolchain("/uv")
    _raise_at(FSYNC_STAGE_FILE)
    with pytest.raises(DurabilityError):
        toolchain.write_toolchain("/uv")


def test_set_current_dir_fsync_failure_raises_clierror() -> None:
    """``cli.set_current`` must fail closed and report a CLI error, not success."""
    cli.set_current("a" * 40)
    _raise_at(FSYNC_STAGE_DIR)
    with pytest.raises(cli.CliError):
        cli.set_current("b" * 40)
    # The active pointer must remain on the originally confirmed commit.
    assert cli.current_commit() == "a" * 40


# ---------------------------------------------------------------------------
# Caller must not advance an irreversible action on an unconfirmed write
# ---------------------------------------------------------------------------


def test_caller_does_not_advance_after_unconfirmed_meta_write() -> None:
    """A caller must not perform its dependent action when the meta write fails.

    This mirrors a real flow: persist the worker lifecycle metadata (recovery
    authority) and only then mark the worker as live. When the durable write is
    not confirmed, the irreversible "mark live" step must not run.
    """
    advanced = []

    def publish_then_advance(meta: lifecycle.WorkerMeta) -> None:
        lifecycle.write_meta(meta)
        advanced.append(meta.worker_id)

    meta = _sample_worker_meta()
    publish_then_advance(meta)
    assert advanced == ["w1"]

    advanced.clear()
    _raise_at(FSYNC_STAGE_DIR)
    with pytest.raises(DurabilityError):
        publish_then_advance(meta)
    assert advanced == []


def test_caller_does_not_advance_after_unconfirmed_desired_write() -> None:
    """A caller must not advance when the desired-intent write is unconfirmed."""
    applied = []

    def settle_then_apply(desired: supervise.SupervisorDesired) -> None:
        supervise.write_desired(desired)
        applied.append(desired.generation)

    desired = supervise.SupervisorDesired(
        schema_version=supervise.SCHEMA_VERSION,
        generation=2,
        commit="a" * 40,
        repo="/repo",
        uv_path="/uv",
        worker_id=None,
    )
    settle_then_apply(desired)
    assert applied == [2]

    applied.clear()
    _raise_at(FSYNC_STAGE_FILE)
    with pytest.raises(DurabilityError):
        settle_then_apply(desired)
    assert applied == []


# ---------------------------------------------------------------------------
# Normal-operation round trips (no regression)
# ---------------------------------------------------------------------------


def test_json_durable_roundtrip(tmp_path: Path) -> None:
    """A durable JSON write must round-trip through read after a crash-less run."""
    path = tmp_path / "state.json"
    clear_fsync_failure_injector()
    write_json_durable(path, {"a": 1, "b": [1, 2, 3]})
    assert path.read_text() == '{"a": 1, "b": [1, 2, 3]}\n'
