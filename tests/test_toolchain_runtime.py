"""Runtime enforcement of the pinned uv contract in ``lubko.toolchain``.

These tests prove the pin is enforced at resolution time for every candidate
source (explicit ``--uv``, ``uv`` on PATH, and the recorded fallback), and that
resolution fails closed on a version mismatch, malformed/unreadable output, a
non-zero ``uv --version``, or a timeout. A recorded candidate is re-validated
at use time, so a binary swapped in place at the recorded path cannot bypass
the pin.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from lubko import toolchain
from lubko.toolchain import UvResolutionError, resolve_uv, toolchain_path, write_toolchain

SUPPORTED = toolchain.SUPPORTED_UV_VERSION


def _fake_uv(tmp_path: Path, body: str, name: str = "uv") -> str:
    """Create an executable fake ``uv`` script returning ``body`` to stdout/stderr.

    Returns:
        Absolute path of the created fake executable.
    """
    path = tmp_path / name
    path.write_text("#!/bin/sh\n" + body + "\n")
    path.chmod(0o755)
    return str(path)


def test_explicit_supported_uv_resolves(tmp_path: Path) -> None:
    """An explicit --uv reporting the pinned version resolves successfully."""
    uv = _fake_uv(tmp_path, f'echo "uv {SUPPORTED} (fake)"')
    assert resolve_uv(uv) == uv


def test_explicit_mismatched_uv_fails_closed(tmp_path: Path) -> None:
    """An explicit --uv reporting a different version is rejected."""
    uv = _fake_uv(tmp_path, 'echo "uv 99.0.0 (fake)"')
    with pytest.raises(UvResolutionError, match="does not match the supported pin"):
        resolve_uv(uv)


def test_path_supported_uv_resolves(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """A uv on PATH reporting the pinned version resolves successfully."""
    uv = _fake_uv(tmp_path, f'echo "uv {SUPPORTED} (fake)"')
    monkeypatch.setenv("PATH", str(tmp_path))
    assert resolve_uv(None) == uv


def test_path_mismatched_uv_fails_closed(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """A uv on PATH reporting a different version is rejected."""
    _fake_uv(tmp_path, 'echo "uv 99.0.0 (fake)"')
    monkeypatch.setenv("PATH", str(tmp_path))
    with pytest.raises(UvResolutionError, match="does not match the supported pin"):
        resolve_uv(None)


def test_recorded_supported_uv_resolves(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """The recorded fallback reporting the pinned version resolves at use time."""
    uv = _fake_uv(tmp_path, f'echo "uv {SUPPORTED} (fake)"')
    write_toolchain(uv)
    empty = tmp_path / "empty"
    empty.mkdir()
    monkeypatch.setenv("PATH", str(empty))
    assert resolve_uv(None) == uv


def test_recorded_swapped_in_place_fails_closed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A recorded path whose binary now reports a different version is rejected."""
    uv = _fake_uv(tmp_path, f'echo "uv {SUPPORTED} (fake)"')
    write_toolchain(uv)
    uv_path = Path(uv)
    uv_path.write_text('#!/bin/sh\necho "uv 99.0.0 (fake)"\n', encoding="utf-8")
    uv_path.chmod(0o755)
    empty = tmp_path / "empty"
    empty.mkdir()
    monkeypatch.setenv("PATH", str(empty))
    with pytest.raises(UvResolutionError, match="does not match the supported pin"):
        resolve_uv(None)


def test_recorded_corrupt_metadata_falls_through(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A recorded entry pointing at a mismatched uv is rejected, not trusted."""
    uv = _fake_uv(tmp_path, 'echo "uv 99.0.0 (fake)"')
    record = {
        "schema_version": toolchain.TOOLCHAIN_SCHEMA_VERSION,
        "uv_path": uv,
        "uv_version": SUPPORTED,
    }
    toolchain_path().parent.mkdir(parents=True, exist_ok=True)
    toolchain_path().write_text(json.dumps(record))
    empty = tmp_path / "empty"
    empty.mkdir()
    monkeypatch.setenv("PATH", str(empty))
    with pytest.raises(UvResolutionError, match="does not match the supported pin"):
        resolve_uv(None)


def test_malformed_output_fails_closed(tmp_path: Path) -> None:
    """A uv whose --version output is unparseable is rejected."""
    uv = _fake_uv(tmp_path, 'echo "not a uv version line"')
    with pytest.raises(UvResolutionError, match="no parseable version"):
        resolve_uv(uv)


def test_command_failure_fails_closed(tmp_path: Path) -> None:
    """A uv whose --version exits non-zero is rejected."""
    uv = _fake_uv(tmp_path, 'echo "boom"; exit 3')
    with pytest.raises(UvResolutionError, match="uv --version failed"):
        resolve_uv(uv)


def test_nonexecutable_path_fails_closed(tmp_path: Path) -> None:
    """A candidate that is not an executable file is rejected."""
    uv = tmp_path / "uv"
    uv.write_text("#!/bin/sh\necho hi\n")
    with pytest.raises(UvResolutionError, match="not executable"):
        resolve_uv(str(uv))


def test_timeout_fails_closed(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """A uv that hangs past the bounded check is rejected, not hung."""
    uv = _fake_uv(tmp_path, "exec sleep 10")
    monkeypatch.setattr(toolchain, "UV_VERSION_CHECK_TIMEOUT_SECONDS", 0.3)
    with pytest.raises(UvResolutionError, match="timed out"):
        resolve_uv(uv)
