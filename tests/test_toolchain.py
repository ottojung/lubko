"""Tests for uv toolchain resolution and persistence."""

import json
from pathlib import Path

import pytest

from lubko import toolchain

SCHEMA_VERSION: int = toolchain.TOOLCHAIN_SCHEMA_VERSION


def write_uv_executable(directory: Path, *, name: str = "uv", executable: bool = True) -> str:
    """Write a trivial executable named for resolution tests.

    Args:
        directory: Directory to write the script into.
        name: Executable file name.
        executable: Whether the file should be marked executable.

    Returns:
        The absolute path of the executable.
    """
    script = directory / name
    script.write_text("#!/bin/sh\nexit 0\n")
    if executable:
        script.chmod(0o755)
    return str(script)


def write_toolchain_file(state: Path, payload: str) -> None:
    """Write raw toolchain metadata JSON into the isolated state root.

    Args:
        state: The temporary XDG state home.
        payload: Raw JSON text.
    """
    directory = state / "lubko"
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "toolchain.json").write_text(payload)


@pytest.fixture(autouse=True)
def state_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point the Lubko state root at a temporary location.

    Returns:
        The temporary XDG state home.
    """
    state = tmp_path / "state"
    monkeypatch.setenv("XDG_STATE_HOME", str(state))
    return state


def test_toolchain_path_under_state_root(state_dir: Path) -> None:
    """The toolchain metadata file lives directly under the state root."""
    assert toolchain.toolchain_path() == state_dir / "lubko" / "toolchain.json"


def test_write_read_roundtrip(tmp_path: Path) -> None:
    """A recorded uv path survives a write/read round trip without temp residue."""
    uv = write_uv_executable(tmp_path)
    toolchain.write_toolchain(uv)
    assert toolchain.read_toolchain() == toolchain.ToolchainMeta(
        schema_version=SCHEMA_VERSION,
        uv_path=uv,
    )
    assert not (toolchain.toolchain_path().parent / "toolchain.json.tmp").exists()


def test_read_missing_returns_none() -> None:
    """Absent toolchain state resolves to no record."""
    assert toolchain.read_toolchain() is None


def test_read_malformed_json_returns_none(state_dir: Path) -> None:
    """Malformed JSON never crashes resolution and yields no record."""
    write_toolchain_file(state_dir, "{not json")
    assert toolchain.read_toolchain() is None


def test_read_non_object_json_returns_none(state_dir: Path) -> None:
    """JSON that is not an object yields no record."""
    write_toolchain_file(state_dir, '["uv"]')
    assert toolchain.read_toolchain() is None


def test_read_unsupported_schema_version_returns_none(state_dir: Path) -> None:
    """An unsupported schema version is treated as stale metadata."""
    write_toolchain_file(state_dir, json.dumps({"schema_version": 99, "uv_path": "/bin/uv"}))
    assert toolchain.read_toolchain() is None


def test_read_non_string_uv_path_returns_none(state_dir: Path) -> None:
    """A non-string uv_path is treated as unusable metadata."""
    write_toolchain_file(state_dir, json.dumps({"schema_version": SCHEMA_VERSION, "uv_path": 42}))
    assert toolchain.read_toolchain() is None


def test_read_empty_uv_path_returns_none(state_dir: Path) -> None:
    """An empty uv_path is treated as unusable metadata."""
    write_toolchain_file(state_dir, json.dumps({"schema_version": SCHEMA_VERSION, "uv_path": ""}))
    assert toolchain.read_toolchain() is None


def test_is_executable(tmp_path: Path) -> None:
    """Only existing regular executable files count as executable."""
    ok = write_uv_executable(tmp_path)
    assert toolchain.is_executable(ok)
    assert not toolchain.is_executable(str(tmp_path / "missing"))
    assert not toolchain.is_executable(str(tmp_path))
    assert not toolchain.is_executable("uv")


def test_resolve_prefers_explicit(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """An explicit --uv wins even when another uv is on PATH."""
    explicit = write_uv_executable(tmp_path, name="explicit-uv")
    on_path = write_uv_executable(tmp_path)
    monkeypatch.setenv("PATH", str(tmp_path))
    assert toolchain.resolve_uv(explicit) == explicit
    assert on_path != explicit


def test_resolve_resolves_explicit_name(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A bare explicit name is resolved to its absolute path on PATH."""
    on_path = write_uv_executable(tmp_path)
    monkeypatch.setenv("PATH", str(tmp_path))
    assert toolchain.resolve_uv("uv") == on_path


def test_resolve_rejects_broken_explicit_without_falling_back(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A broken explicit --uv is rejected even though uv is on PATH."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    write_uv_executable(bin_dir)
    monkeypatch.setenv("PATH", str(bin_dir))
    broken = tmp_path / "missing-uv"
    with pytest.raises(toolchain.UvResolutionError, match="explicit"):
        toolchain.resolve_uv(str(broken))


def test_resolve_rejects_non_executable_explicit(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A non-executable explicit --uv is rejected."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    write_uv_executable(bin_dir)
    monkeypatch.setenv("PATH", str(bin_dir))
    not_executable = write_uv_executable(tmp_path, name="not-exec", executable=False)
    with pytest.raises(toolchain.UvResolutionError, match="explicit"):
        toolchain.resolve_uv(not_executable)


def test_resolve_uses_path_uv_before_recorded(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Uv on PATH wins over a recorded executable."""
    path_dir = tmp_path / "path"
    path_dir.mkdir()
    on_path = write_uv_executable(path_dir)
    recorded = write_uv_executable(tmp_path, name="recorded-uv")
    toolchain.write_toolchain(recorded)
    monkeypatch.setenv("PATH", str(path_dir))
    assert toolchain.resolve_uv(None) == on_path


def test_resolve_uses_recorded_when_not_on_path(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The recorded executable is used when uv is not on PATH."""
    recorded = write_uv_executable(tmp_path, name="recorded-uv")
    toolchain.write_toolchain(recorded)
    monkeypatch.setenv("PATH", "/nonexistent")
    assert toolchain.resolve_uv(None) == recorded


def test_resolve_fails_on_stale_recorded_path(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A recorded path that no longer exists is not used."""
    toolchain.write_toolchain(str(tmp_path / "gone-uv"))
    monkeypatch.setenv("PATH", "/nonexistent")
    with pytest.raises(toolchain.UvResolutionError, match="recorded"):
        toolchain.resolve_uv(None)


def test_resolve_fails_on_non_executable_recorded_path(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A recorded path that is not executable is not used."""
    recorded = write_uv_executable(tmp_path, name="recorded-uv", executable=False)
    toolchain.write_toolchain(recorded)
    monkeypatch.setenv("PATH", "/nonexistent")
    with pytest.raises(toolchain.UvResolutionError, match="recorded"):
        toolchain.resolve_uv(None)


def test_resolve_fails_without_any_uv(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No uv anywhere yields an actionable error."""
    monkeypatch.setenv("PATH", "/nonexistent")
    with pytest.raises(toolchain.UvResolutionError, match="lubko-install"):
        toolchain.resolve_uv(None)
