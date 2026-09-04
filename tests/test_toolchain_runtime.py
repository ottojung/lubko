"""Runtime resolution of the external ``uv`` executable."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from lubko import toolchain
from lubko.toolchain import UvResolutionError, resolve_uv, toolchain_path, write_toolchain


def _fake_uv(tmp_path: Path, name: str = "uv") -> str:
    """Create an executable placeholder named like ``uv``.

    Returns:
        Absolute path of the placeholder executable.
    """
    path = tmp_path / name
    path.write_text("#!/bin/sh\nexit 0\n")
    path.chmod(0o755)
    return str(path)


def test_explicit_executable_uv_resolves_regardless_of_patch_version(tmp_path: Path) -> None:
    """Runtime resolution depends on executability, not `uv --version`."""
    uv = _fake_uv(tmp_path)
    assert resolve_uv(uv) == uv


def test_path_uv_resolves(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """An executable `uv` on PATH is selected."""
    uv = _fake_uv(tmp_path)
    monkeypatch.setenv("PATH", str(tmp_path))
    assert resolve_uv(None) == uv


def test_recorded_uv_resolves_without_version_metadata(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The durable fallback records only the executable path."""
    uv = _fake_uv(tmp_path)
    write_toolchain(uv)
    record = json.loads(toolchain_path().read_text())
    assert record == {"schema_version": toolchain.TOOLCHAIN_SCHEMA_VERSION, "uv_path": uv}
    empty = tmp_path / "empty"
    empty.mkdir()
    monkeypatch.setenv("PATH", str(empty))
    assert resolve_uv(None) == uv


def test_legacy_record_with_uv_version_is_still_readable(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Removing runtime version authority does not strand older path records."""
    uv = _fake_uv(tmp_path)
    toolchain_path().parent.mkdir(parents=True, exist_ok=True)
    toolchain_path().write_text(
        json.dumps({"schema_version": 1, "uv_path": uv, "uv_version": "0.10.12"})
    )
    empty = tmp_path / "empty"
    empty.mkdir()
    monkeypatch.setenv("PATH", str(empty))
    assert resolve_uv(None) == uv


def test_recorded_toolchain_rejects_malformed_schema_version(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Only the exact integer metadata schema can authorize a fallback path."""
    uv = _fake_uv(tmp_path)
    toolchain_path().parent.mkdir(parents=True, exist_ok=True)
    schema_versions: tuple[object, ...] = (True, 1.0, "1", None, [], {}, 2)
    for schema_version in schema_versions:
        toolchain_path().write_text(json.dumps({"schema_version": schema_version, "uv_path": uv}))
        assert toolchain.read_toolchain() is None
    empty = tmp_path / "empty"
    empty.mkdir()
    monkeypatch.setenv("PATH", str(empty))
    with pytest.raises(UvResolutionError, match="no usable uv executable is recorded"):
        resolve_uv(None)


def test_recorded_path_must_still_be_executable(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A removed or non-executable recorded path is never trusted."""
    uv = _fake_uv(tmp_path)
    write_toolchain(uv)
    Path(uv).chmod(0o644)
    empty = tmp_path / "empty"
    empty.mkdir()
    monkeypatch.setenv("PATH", str(empty))
    with pytest.raises(UvResolutionError, match="recorded uv executable is unusable"):
        resolve_uv(None)


def test_nonexecutable_explicit_path_fails_closed(tmp_path: Path) -> None:
    """An explicit candidate must be an executable regular file."""
    uv = tmp_path / "uv"
    uv.write_text("#!/bin/sh\nexit 0\n")
    with pytest.raises(UvResolutionError, match="not executable"):
        resolve_uv(str(uv))
