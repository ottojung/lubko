"""Tests for the Lubko command line tool installer."""

from pathlib import Path

import pytest

from lubko import install

REQUIRED_ENTRY_POINTS: frozenset[str] = frozenset(install.ENTRY_POINTS)


def test_bin_home_uses_xdg_bin_home(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """XDG_BIN_HOME determines the user bin directory."""
    target = tmp_path / "bin"
    monkeypatch.setenv("XDG_BIN_HOME", str(target))
    assert install.bin_home() == target


def test_bin_home_falls_back_to_local_bin(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Without XDG_BIN_HOME the bin directory falls back under the home."""
    monkeypatch.delenv("XDG_BIN_HOME", raising=False)
    monkeypatch.setenv("HOME", str(tmp_path))
    assert install.bin_home() == tmp_path / ".local" / "bin"


def test_bin_home_on_path(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """The bin directory is reported present on PATH only when it is there."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    monkeypatch.setenv("XDG_BIN_HOME", str(bin_dir))
    monkeypatch.setenv("PATH", f"{bin_dir}:/usr/bin")
    assert install.bin_home_on_path()
    monkeypatch.setenv("PATH", "/usr/bin")
    assert not install.bin_home_on_path()


def test_missing_entry_points(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Only entry points absent from the bin directory are reported missing."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "lubko-agent").write_text("#!/bin/sh\n")
    monkeypatch.setenv("XDG_BIN_HOME", str(bin_dir))
    missing = set(install.missing_entry_points())
    assert "lubko-agent" not in missing
    assert missing == REQUIRED_ENTRY_POINTS - {"lubko-agent"}


def test_main_rejects_non_repo(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The installer refuses a directory that is not a Lubko checkout."""
    code = install.main(["--repo", str(tmp_path)])
    assert code == install.EXIT_ERROR
    assert "not a Lubko repository checkout" in capsys.readouterr().err


def test_main_dry_run_reports_installed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A dry run verifies the installation state without invoking uv."""
    repo = Path(__file__).resolve().parents[1]
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    for entry in install.ENTRY_POINTS:
        script = bin_dir / entry
        script.write_text("#!/bin/sh\n")
        script.chmod(0o755)
    monkeypatch.setenv("XDG_BIN_HOME", str(bin_dir))
    monkeypatch.setenv("PATH", f"{bin_dir}:/usr/bin")
    code = install.main(["--repo", str(repo), "--dry-run"])
    assert code == install.EXIT_OK
    out = capsys.readouterr().out
    assert "Lubko tools installed and resolvable on PATH" in out
    for entry in install.ENTRY_POINTS:
        assert entry in out
