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
    leftovers = [p.name for p in target.iterdir() if p.name.startswith(DURABLE_TEMP_PREFIX)]
    assert leftovers == []
    assert sorted(p.name for p in target.iterdir()) == ["file.json", "link"]

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
