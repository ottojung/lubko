"""Crash-durable write semantics: atomicity, completeness, fail-closed errors."""

from collections.abc import Iterator
from pathlib import Path

import pytest

from lubko.durable import (
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


def test_write_and_overwrite(tmp_path: Path) -> None:
    """Check that write and overwrite holds."""
    path = tmp_path / "state" / "nested" / "file.json"
    write_bytes_durable(path, b"first")
    assert path.read_bytes() == b"first"
    write_bytes_durable(path, b"second")
    assert path.read_bytes() == b"second"
    write_json_durable(path, {"b": 1, "a": 2})
    assert path.read_text(encoding="utf-8") == '{"a": 2, "b": 1}\n'


def test_no_temporary_leftovers(tmp_path: Path) -> None:
    """Check that no temporary leftovers holds."""
    directory = tmp_path / "d"
    for index in range(5):
        write_bytes_durable(directory / "f", str(index).encode())
        write_symlink_durable(directory / "link", "f")
    leftovers = [p.name for p in directory.iterdir() if p.name.startswith(DURABLE_TEMP_PREFIX)]
    assert leftovers == []
    assert sorted(p.name for p in directory.iterdir()) == ["f", "link"]


def test_short_write_still_writes_everything(tmp_path: Path) -> None:
    """Check that short write still writes everything holds."""
    set_short_write_injector(0.1)
    path = tmp_path / "f"
    payload = bytes(range(256)) * 64
    write_bytes_durable(path, payload)
    assert path.read_bytes() == payload


def test_failure_before_rename_keeps_previous_value(tmp_path: Path) -> None:
    """Check that failure before rename keeps previous value holds."""
    path = tmp_path / "f"
    write_bytes_durable(path, b"old")
    set_one_shot_fsync_failure_injector(stage=FSYNC_STAGE_FILE)
    with pytest.raises(DurabilityError):
        write_bytes_durable(path, b"new")
    assert path.read_bytes() == b"old"
    assert [p.name for p in tmp_path.iterdir()] == ["f"]


def test_failed_dir_fsync_raises_and_restores_previous(tmp_path: Path) -> None:
    """Check that failed dir fsync raises and restores previous holds."""
    path = tmp_path / "f"
    write_bytes_durable(path, b"old")

    def inject(destination: Path, stage: str) -> None:
        if stage == FSYNC_STAGE_DIR and Path(destination) == path.parent:
            msg = "injected"
            raise DurabilityError(msg)

    set_fsync_failure_injector(inject)
    with pytest.raises(DurabilityError):
        write_bytes_durable(path, b"new")
    # Fail-closed: the unconfirmed value must not silently replace the prior
    # authority.
    assert path.read_bytes() == b"old"


def test_first_write_neutralized_on_unconfirmed_dir_fsync(tmp_path: Path) -> None:
    """Check that first write neutralized on unconfirmed dir fsync holds."""
    path = tmp_path / "f"
    set_one_shot_fsync_failure_injector(stage=FSYNC_STAGE_DIR, path=path.parent)
    with pytest.raises(DurabilityError):
        write_bytes_durable(path, b"data")
    assert not path.exists()


def test_symlink_switch_round_trip_and_restore(tmp_path: Path) -> None:
    """Check that symlink switch round trip and restore holds."""
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


def test_remove_durable(tmp_path: Path) -> None:
    """Check that remove durable holds."""
    path = tmp_path / "gone"
    write_bytes_durable(path, b"x")
    remove_durable(path)
    assert not path.exists()
    remove_durable(path)  # idempotent


def test_make_directory_durable_is_idempotent(tmp_path: Path) -> None:
    """Check that make directory durable is idempotent holds."""
    target = tmp_path / "a" / "b" / "c"
    make_directory_durable(target)
    assert target.is_dir()
    make_directory_durable(target)


def test_temporary_names_are_unique(tmp_path: Path) -> None:
    """Check that temporary names are unique holds."""
    destination = tmp_path / "f"
    names = {temporary_path(destination).name for _ in range(20)}
    assert len(names) == 20
    assert all(name.endswith("-f") for name in names)
