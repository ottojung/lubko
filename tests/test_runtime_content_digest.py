"""Byte-for-byte equivalence tests for _runtime_content_digest.

A frozen reference implementation copied verbatim from the pre-optimisation
code lives here.  Every test asserts ``cli._runtime_content_digest`` produces
identical output to the reference across representative tree shapes, edge
cases, and injected failures.
"""

from __future__ import annotations

import hashlib
import os
import stat
from operator import itemgetter
from pathlib import Path

import pytest

# Explicit submodule import required: strict mypy rejects `from lubko import cli`
# because the package __init__ does not re-export the submodule.
import lubko.cli as cli  # ruff: ignore[manual-from-import]

_RUNTIME_MANIFEST_NAME = cli.RUNTIME_MANIFEST_NAME
_WRITE_BITS = cli._WRITE_BITS
_file_content_digest = cli._file_content_digest


# ---------------------------------------------------------------------------
# Frozen reference — verbatim pre-optimisation implementation
# ---------------------------------------------------------------------------


def _reference_digest(root: Path) -> bytes:
    """Pre-optimisation ``_runtime_content_digest`` (Path + relative_to).

    Returns:
        The raw SHA-256 digest bytes.
    """
    entries: list[tuple[str, Path, os.stat_result]] = []

    def collect(directory: Path) -> None:
        with os.scandir(directory) as iterator:
            for entry in iterator:
                path = Path(entry.path)
                rel = path.relative_to(root).as_posix()
                if rel == _RUNTIME_MANIFEST_NAME:
                    continue
                info = path.lstat()
                entries.append((rel, path, info))
                if stat.S_ISDIR(info.st_mode):
                    collect(path)

    collect(root)
    hasher = hashlib.sha256()

    def update(data: bytes) -> None:
        hasher.update(len(data).to_bytes(8, "big"))
        hasher.update(data)

    for rel, path, info in sorted(entries, key=itemgetter(0)):
        update(rel.encode("utf-8"))
        if stat.S_ISREG(info.st_mode):
            update(b"f")
            update((info.st_mode & ~_WRITE_BITS & 0o777).to_bytes(2, "big"))
            update(_file_content_digest(path))
        elif stat.S_ISLNK(info.st_mode):
            update(b"l")
            update(os.fspath(path.readlink()).encode("utf-8"))
        elif stat.S_ISDIR(info.st_mode):
            update(b"d")
        else:
            update(b"o")
            update((info.st_mode & ~_WRITE_BITS & 0o777).to_bytes(2, "big"))
    return hasher.digest()


# ---------------------------------------------------------------------------
# Equivalence tests
# ---------------------------------------------------------------------------

_FIXED_BINARY = b"\x00\x01\x02\xff" * 256  # deterministic 1 KiB content


def test_empty_tree(tmp_path: Path) -> None:
    """Empty tree produces same digest."""
    assert cli._runtime_content_digest(tmp_path) == _reference_digest(tmp_path)


def test_only_manifest(tmp_path: Path) -> None:
    """Root-level manifest is excluded from digest."""
    (tmp_path / _RUNTIME_MANIFEST_NAME).write_bytes(b"excluded")
    assert cli._runtime_content_digest(tmp_path) == _reference_digest(tmp_path)


def test_regular_files(tmp_path: Path) -> None:
    """Regular files contribute path, mode, and content digest."""
    (tmp_path / "a.txt").write_bytes(b"alpha")
    (tmp_path / "b.txt").write_bytes(b"beta\n")
    (tmp_path / "c.dat").write_bytes(_FIXED_BINARY)
    assert cli._runtime_content_digest(tmp_path) == _reference_digest(tmp_path)


def test_nested_dirs_and_symlinks(tmp_path: Path) -> None:
    """Nested dirs are traversed; symlinks contribute raw target."""
    (tmp_path / "a" / "b" / "c").mkdir(parents=True)
    (tmp_path / "a" / "b" / "c" / "f").write_bytes(b"x")
    (tmp_path / "a" / "g").write_bytes(b"y")
    (tmp_path / "target").write_bytes(b"t")
    (tmp_path / "link").symlink_to("target")
    (tmp_path / "dangling").symlink_to("nope")
    (tmp_path / "l2").symlink_to("link")  # symlink-to-symlink
    sub = tmp_path / "sub"
    sub.mkdir()
    (sub / "link").symlink_to("../target")
    assert cli._runtime_content_digest(tmp_path) == _reference_digest(tmp_path)


def test_mode_bits_and_write_masking(tmp_path: Path) -> None:
    """Write-bit masking is deterministic across seal/unseal."""
    for mode in [0o444, 0o644, 0o755, 0o777]:
        f = tmp_path / f"m{mode:o}"
        f.write_bytes(b"d")
        f.chmod(mode)
    assert cli._runtime_content_digest(tmp_path) == _reference_digest(tmp_path)
    for f in tmp_path.iterdir():
        if f.is_file():
            f.chmod(f.stat().st_mode & ~_WRITE_BITS)
    assert cli._runtime_content_digest(tmp_path) == _reference_digest(tmp_path)


def test_non_ascii_names(tmp_path: Path) -> None:
    """Non-ASCII file and directory names are digested correctly."""
    (tmp_path / "données.txt").write_bytes(b"fr")
    (tmp_path / "日本語").write_bytes(b"jp")
    sub = tmp_path / "Ünïcödé"
    sub.mkdir()
    (sub / "f").write_bytes(b"in")
    (tmp_path / "link").symlink_to("données.txt")
    assert cli._runtime_content_digest(tmp_path) == _reference_digest(tmp_path)


def test_large_tree(tmp_path: Path) -> None:
    """Representative runtime tree: dirs, files, symlinks, nested subdirs."""
    for i in range(50):
        d = tmp_path / f"dir{i:02d}"
        d.mkdir()
        (d / "a").write_bytes(b"a")
        (d / "b").write_bytes(b"b" * 200)
        if i % 3 == 0:
            (d / "l").symlink_to("a")
        if i % 5 == 0:
            (d / "deep" / "n").mkdir(parents=True)
            (d / "deep" / "n" / "f").write_bytes(b"d")
    (tmp_path / _RUNTIME_MANIFEST_NAME).write_bytes(b"root")
    assert cli._runtime_content_digest(tmp_path) == _reference_digest(tmp_path)


def test_root_manifest_excluded_nested_included(tmp_path: Path) -> None:
    """Changing root manifest does NOT change digest; nested manifest DOES."""
    (tmp_path / "f").write_bytes(b"x")
    baseline = cli._runtime_content_digest(tmp_path)

    # Changing root manifest content: digest must stay the same.
    (tmp_path / _RUNTIME_MANIFEST_NAME).write_bytes(b"v1")
    assert cli._runtime_content_digest(tmp_path) == baseline
    (tmp_path / _RUNTIME_MANIFEST_NAME).write_bytes(b"v2")
    assert cli._runtime_content_digest(tmp_path) == baseline

    # Adding a nested manifest-named file: digest must change.
    sub = tmp_path / "sub"
    sub.mkdir()
    (sub / _RUNTIME_MANIFEST_NAME).write_bytes(b"nested")
    assert cli._runtime_content_digest(tmp_path) != baseline

    # Must also match frozen reference.
    assert cli._runtime_content_digest(tmp_path) == _reference_digest(tmp_path)


# ---------------------------------------------------------------------------
# Failure propagation tests — deterministic, no races
# ---------------------------------------------------------------------------


def test_scandir_nonexistent_root_propagates() -> None:
    """Nonexistent root raises FileNotFoundError from os.scandir."""
    with pytest.raises(FileNotFoundError):
        cli._runtime_content_digest(Path("/no/such/dir/x9f3a7b2c1d0e"))


def test_lstat_failure_propagates(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Injected os.lstat failure propagates as OSError."""
    (tmp_path / "a").write_bytes(b"x")
    victim = str(tmp_path / "a")
    real_lstat = os.lstat

    def _patched_lstat(path: str | Path, **kw: object) -> os.stat_result:
        if os.fspath(path) == victim:
            msg = "injected lstat failure"
            raise OSError(msg)
        return real_lstat(path, **kw)  # type: ignore[arg-type]

    monkeypatch.setattr(os, "lstat", _patched_lstat)
    with pytest.raises(OSError, match="injected lstat failure"):
        cli._runtime_content_digest(tmp_path)


def test_readlink_failure_propagates(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Injected os.readlink failure propagates as OSError."""
    (tmp_path / "target").write_bytes(b"t")
    (tmp_path / "bad_link").symlink_to("target")
    victim = str(tmp_path / "bad_link")
    real_readlink = os.readlink

    def _patched_readlink(path: str | Path) -> str | os.PathLike[str]:
        if os.fspath(path) == victim:
            msg = "injected readlink failure"
            raise OSError(msg)
        return real_readlink(path)

    monkeypatch.setattr(os, "readlink", _patched_readlink)
    with pytest.raises(OSError, match="injected readlink failure"):
        cli._runtime_content_digest(tmp_path)


def test_open_failure_propagates(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Injected Path.open failure propagates as OSError."""
    (tmp_path / "secret").write_bytes(b"content")
    real_path_open = Path.open

    def _patched_path_open(self: Path, mode: str = "r", *args: object, **kw: object) -> object:
        if self.name == "secret":
            msg = "injected open failure"
            raise OSError(msg)
        return real_path_open(self, mode, *args, **kw)  # type: ignore[call-overload]

    monkeypatch.setattr(Path, "open", _patched_path_open)
    with pytest.raises(OSError, match="injected open failure"):
        cli._runtime_content_digest(tmp_path)
