"""Crash-durable write semantics: atomicity, completeness, fail-closed errors."""

import fcntl
import os
import threading
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest

from lubko.durable import (
    DURABLE_LOCK_PREFIX,
    DURABLE_TEMP_PREFIX,
    FSYNC_STAGE_DIR,
    FSYNC_STAGE_FILE,
    FSYNC_STAGE_REPLACE,
    DurabilityError,
    make_directory_durable,
    remove_durable,
    set_fsync_failure_injector,
    set_one_shot_fsync_failure_injector,
    set_short_write_injector,
    temporary_path,
    write_bytes_durable,
    write_json_durable,
    write_symlink_durable,
)


@pytest.fixture(autouse=True)
def _clean_injectors() -> Iterator[None]:
    set_short_write_injector(None)
    set_fsync_failure_injector(None)
    yield
    set_short_write_injector(None)
    set_fsync_failure_injector(None)


def test_durable_writes_round_trip_without_leftovers(tmp_path: Path) -> None:
    """Durable writes create, replace, retry short writes, and leave no debris."""
    target = tmp_path / "state" / "nested"
    make_directory_durable(target)
    make_directory_durable(target)  # idempotent
    path = target / "file.json"

    payload = bytes(range(256)) * 64
    set_short_write_injector(0.1)
    write_bytes_durable(path, payload)
    assert path.read_bytes() == payload

    set_short_write_injector(None)
    write_json_durable(path, {"b": 1, "a": 2})
    assert path.read_text(encoding="utf-8") == '{"a": 2, "b": 1}\n'

    link = target / "link"
    write_symlink_durable(link, "file.json")
    for index in range(5):
        write_bytes_durable(path, str(index).encode())
    leftovers = [
        p.name
        for p in target.iterdir()
        if p.name.startswith(DURABLE_TEMP_PREFIX) and not p.name.startswith(DURABLE_LOCK_PREFIX)
    ]
    assert leftovers == []
    # Stable per-destination lock sidecars persist by design (unlinking them
    # would race with waiters on the old inode).
    assert sorted(p.name for p in target.iterdir()) == [
        ".lubko-durable-lock-file.json",
        ".lubko-durable-lock-link",
        "file.json",
        "link",
    ]

    names = {temporary_path(path).name for _ in range(20)}
    assert len(names) == 20
    assert all(name.endswith("-file.json") for name in names)

    remove_durable(path)
    assert not path.exists()
    remove_durable(path)


def test_unconfirmed_writes_fail_closed_and_restore_prior_value(tmp_path: Path) -> None:
    """Every fsync confirmation failure leaves the prior durable value intact."""
    file_path = tmp_path / "f"
    write_bytes_durable(file_path, b"old")

    set_one_shot_fsync_failure_injector(stage=FSYNC_STAGE_FILE)
    with pytest.raises(DurabilityError):
        write_bytes_durable(file_path, b"new")
    assert file_path.read_bytes() == b"old"

    def inject_dir(destination: Path, stage: str) -> None:
        if stage == FSYNC_STAGE_DIR and Path(destination) == tmp_path:
            msg = "injected"
            raise DurabilityError(msg)

    set_fsync_failure_injector(inject_dir)
    with pytest.raises(DurabilityError):
        write_bytes_durable(file_path, b"new")
    assert file_path.read_bytes() == b"old"
    set_fsync_failure_injector(None)

    fresh = tmp_path / "sub" / "g"
    set_one_shot_fsync_failure_injector(stage=FSYNC_STAGE_DIR, path=fresh.parent)
    with pytest.raises(DurabilityError):
        write_bytes_durable(fresh, b"data")
    assert not fresh.exists()


def test_symlink_switch_is_atomic_and_restores_prior_pointer(tmp_path: Path) -> None:
    """Symlink switches are atomic and restore the prior pointer on failure."""
    link = tmp_path / "current"
    write_bytes_durable(tmp_path / "a", b"a")
    write_bytes_durable(tmp_path / "b", b"b")
    write_symlink_durable(link, "a")
    assert link.is_symlink()
    assert link.readlink() == Path("a")
    set_one_shot_fsync_failure_injector(stage=FSYNC_STAGE_REPLACE)
    with pytest.raises(DurabilityError):
        write_symlink_durable(link, "b")
    assert link.readlink() == Path("a")


def test_failed_symlink_switch_restores_prior_regular_file_bytes(tmp_path: Path) -> None:
    """A failed symlink switch restores prior regular-file bytes exactly."""
    file_path = tmp_path / "entry"
    payload = b"prior regular bytes"
    write_bytes_durable(file_path, payload)

    set_one_shot_fsync_failure_injector(stage=FSYNC_STAGE_DIR, path=tmp_path)
    with pytest.raises(DurabilityError):
        write_symlink_durable(file_path, "elsewhere")
    assert file_path.is_file()
    assert not file_path.is_symlink()
    assert file_path.read_bytes() == payload


def test_failed_symlink_switch_restores_prior_symlink_target(tmp_path: Path) -> None:
    """A failed symlink switch over a symlink restores the prior target."""
    link = tmp_path / "entry"
    write_symlink_durable(link, "old-target")
    set_one_shot_fsync_failure_injector(stage=FSYNC_STAGE_DIR, path=tmp_path)
    with pytest.raises(DurabilityError):
        write_symlink_durable(link, "new-target")
    assert link.is_symlink()
    assert link.readlink() == Path("old-target")


def test_failed_first_symlink_write_neutralizes_destination(tmp_path: Path) -> None:
    """A first failed symlink write removes the unconfirmed destination."""
    link = tmp_path / "fresh"
    set_one_shot_fsync_failure_injector(stage=FSYNC_STAGE_DIR, path=tmp_path)
    with pytest.raises(DurabilityError):
        write_symlink_durable(link, "target")
    assert not link.exists(follow_symlinks=False)
    assert not link.is_symlink()


def _sidecar_is_free(directory: Path, name: str) -> bool:
    """Probe the destination sidecar flock without blocking.

    Args:
        directory: Directory holding the destination and its sidecar lock.
        name: Destination file name the sidecar lock guards.

    Returns:
        ``True`` when the probe acquired and released the flock, meaning no
        durable operation currently holds the destination lock.
    """
    fd = os.open(str(directory / f"{DURABLE_LOCK_PREFIX}{name}"), os.O_RDWR)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return False
        fcntl.flock(fd, fcntl.LOCK_UN)
        return True
    finally:
        os.close(fd)


def _fail_dir_once_for(directory: Path, hook: Callable[[Path], None]) -> None:
    """Install an injector that runs ``hook`` then fails A's dir fsync once."""

    def inject(destination: Path, stage: str) -> None:
        if stage == FSYNC_STAGE_DIR and Path(destination) == directory:
            set_fsync_failure_injector(None)
            hook(destination)
            msg = "injected"
            raise DurabilityError(msg)

    set_fsync_failure_injector(inject)


def test_failed_write_cannot_revert_newer_committed_writer(tmp_path: Path) -> None:
    """A concurrent writer serialized behind a failing write is never reverted."""
    file_path = tmp_path / "f"
    write_bytes_durable(file_path, b"old")
    b_done = threading.Event()

    def during_a(destination: Path) -> None:
        # A holds the destination lock for its whole critical section: writer
        # B's durable write cannot proceed until A's cleanup has finished.
        assert not _sidecar_is_free(destination, "f")

        def writer_b() -> None:
            write_bytes_durable(file_path, b"newer")
            b_done.set()

        threading.Thread(target=writer_b, daemon=True).start()
        assert not b_done.is_set()

    _fail_dir_once_for(tmp_path, during_a)
    with pytest.raises(DurabilityError):
        write_bytes_durable(file_path, b"unconfirmed")

    assert b_done.wait(timeout=5.0)
    assert file_path.read_bytes() == b"newer"


def test_failed_symlink_switch_cannot_revert_newer_pointer(tmp_path: Path) -> None:
    """A newer committed symlink pointer survives an older failed switch."""
    link = tmp_path / "current"
    write_symlink_durable(link, "gen1")
    b_done = threading.Event()

    def during_a(destination: Path) -> None:
        assert not _sidecar_is_free(destination, "current")

        def writer_b() -> None:
            write_symlink_durable(link, "gen3")
            b_done.set()

        threading.Thread(target=writer_b, daemon=True).start()
        assert not b_done.is_set()

    _fail_dir_once_for(tmp_path, during_a)
    with pytest.raises(DurabilityError):
        write_symlink_durable(link, "gen2")

    assert b_done.wait(timeout=5.0)
    assert link.readlink() == Path("gen3")


def test_failed_removal_cannot_resurrect_over_newer_writer(tmp_path: Path) -> None:
    """A failed removal restores only before any newer writer can commit."""
    file_path = tmp_path / "authority"
    write_bytes_durable(file_path, b"v1")
    b_done = threading.Event()

    def during_a(destination: Path) -> None:
        assert not _sidecar_is_free(destination, "authority")

        def writer_b() -> None:
            write_bytes_durable(file_path, b"v3")
            b_done.set()

        threading.Thread(target=writer_b, daemon=True).start()
        assert not b_done.is_set()

    _fail_dir_once_for(tmp_path, during_a)
    with pytest.raises(DurabilityError):
        remove_durable(file_path)

    assert b_done.wait(timeout=5.0)
    assert file_path.read_bytes() == b"v3"


def test_single_writer_removal_still_restores_on_failure(tmp_path: Path) -> None:
    """Without a concurrent writer, failed removal restores prior authority."""
    file_path = tmp_path / "solo"
    write_bytes_durable(file_path, b"prior")
    set_one_shot_fsync_failure_injector(stage=FSYNC_STAGE_DIR, path=file_path.parent)
    with pytest.raises(DurabilityError):
        remove_durable(file_path)
    assert file_path.read_bytes() == b"prior"


def test_recursive_restore_releases_lock_depth(tmp_path: Path) -> None:
    """Nested cleanup reenters the lock and fully releases it afterwards.

    The failing write's restore re-enters ``_serialized`` for the same path;
    if that recursion leaked gate depth or held the sidecar lock, this write —
    and the follow-up write from another thread — would deadlock or corrupt
    ownership state instead of completing.
    """
    file_path = tmp_path / "depthcheck"
    write_bytes_durable(file_path, b"keep")
    set_one_shot_fsync_failure_injector(stage=FSYNC_STAGE_DIR, path=tmp_path)
    with pytest.raises(DurabilityError):
        write_bytes_durable(file_path, b"fails")
    assert file_path.read_bytes() == b"keep"
    assert _sidecar_is_free(tmp_path, "depthcheck")

    done = threading.Event()

    def other_thread() -> None:
        write_bytes_durable(file_path, b"after")
        done.set()

    thread = threading.Thread(target=other_thread, daemon=True)
    thread.start()
    assert done.wait(timeout=5.0)
    thread.join(timeout=1.0)
    assert file_path.read_bytes() == b"after"
