"""Tests for the Lubko command line tool installer."""

from pathlib import Path

import pytest

from lubko import install, toolchain

REQUIRED_ENTRY_POINTS: frozenset[str] = frozenset(install.ENTRY_POINTS)


def write_uv_executable(directory: Path, *, name: str = "uv") -> str:
    """Write a trivial executable named for resolution tests.

    Args:
        directory: Directory to write the script into.
        name: Executable file name.

    Returns:
        The absolute path of the executable.
    """
    script = directory / name
    script.write_text("#!/bin/sh\nexit 0\n")
    script.chmod(0o755)
    return str(script)


def make_installed_bin(bin_dir: Path) -> None:
    """Populate a bin directory with every maintained entry point.

    Args:
        bin_dir: Directory to populate.
    """
    bin_dir.mkdir(parents=True, exist_ok=True)
    for entry in install.ENTRY_POINTS:
        script = bin_dir / entry
        script.write_text("#!/bin/sh\n")
        script.chmod(0o755)


@pytest.fixture(autouse=True)
def toolchain_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Isolate the toolchain state from the real user state."""
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state"))


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


def test_main_persists_resolved_uv(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A successful install records the exact resolved uv executable used."""
    repo = Path(__file__).resolve().parents[1]
    bin_dir = tmp_path / "bin"
    make_installed_bin(bin_dir)
    uv_dir = tmp_path / "uv-dir"
    uv_dir.mkdir()
    uv_path = write_uv_executable(uv_dir)
    monkeypatch.setenv("XDG_BIN_HOME", str(bin_dir))
    monkeypatch.setenv("PATH", f"{uv_dir}:{bin_dir}:/usr/bin")

    recorded: list[str] = []

    def fake_tool_install(_repo: Path, uv_path_arg: str) -> int:
        recorded.append(uv_path_arg)
        return 0

    monkeypatch.setattr(install, "tool_install", fake_tool_install)

    code = install.main(["--repo", str(repo)])
    assert code == install.EXIT_OK
    assert recorded == [uv_path]
    meta = toolchain.read_toolchain()
    assert meta is not None
    assert meta.uv_path == uv_path


def test_main_persists_exact_resolved_explicit_uv(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A bare --uv name is persisted as its resolved absolute path."""
    repo = Path(__file__).resolve().parents[1]
    bin_dir = tmp_path / "bin"
    make_installed_bin(bin_dir)
    uv_dir = tmp_path / "uv-dir"
    uv_dir.mkdir()
    uv_path = write_uv_executable(uv_dir)
    monkeypatch.setenv("XDG_BIN_HOME", str(bin_dir))
    monkeypatch.setenv("PATH", f"{uv_dir}:{bin_dir}:/usr/bin")

    monkeypatch.setattr(install, "tool_install", lambda _repo, _uv: 0)

    code = install.main(["--repo", str(repo), "--uv", "uv"])
    assert code == install.EXIT_OK
    meta = toolchain.read_toolchain()
    assert meta is not None
    assert meta.uv_path == uv_path


def test_main_does_not_persist_on_failed_install(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A failed uv tool install leaves the toolchain state untouched."""
    repo = Path(__file__).resolve().parents[1]
    bin_dir = tmp_path / "bin"
    make_installed_bin(bin_dir)
    uv_dir = tmp_path / "uv-dir"
    uv_dir.mkdir()
    write_uv_executable(uv_dir)
    monkeypatch.setenv("XDG_BIN_HOME", str(bin_dir))
    monkeypatch.setenv("PATH", f"{uv_dir}:{bin_dir}:/usr/bin")

    monkeypatch.setattr(install, "tool_install", lambda _repo, _uv: 3)

    code = install.main(["--repo", str(repo)])
    assert code == install.EXIT_ERROR
    assert toolchain.read_toolchain() is None


def test_main_fails_when_uv_unresolvable(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Without uv on PATH or a recorded toolchain, the installer fails clearly."""
    repo = Path(__file__).resolve().parents[1]
    bin_dir = tmp_path / "bin"
    make_installed_bin(bin_dir)
    monkeypatch.setenv("XDG_BIN_HOME", str(bin_dir))
    monkeypatch.setenv("PATH", "/nonexistent")

    code = install.main(["--repo", str(repo)])
    assert code == install.EXIT_ERROR
    assert "uv" in capsys.readouterr().err
    assert toolchain.read_toolchain() is None


def test_main_fails_on_broken_explicit_uv(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A broken explicit --uv is refused even when uv is on PATH."""
    repo = Path(__file__).resolve().parents[1]
    bin_dir = tmp_path / "bin"
    make_installed_bin(bin_dir)
    uv_dir = tmp_path / "uv-dir"
    uv_dir.mkdir()
    write_uv_executable(uv_dir)
    monkeypatch.setenv("XDG_BIN_HOME", str(bin_dir))
    monkeypatch.setenv("PATH", f"{uv_dir}:{bin_dir}:/usr/bin")

    code = install.main(["--repo", str(repo), "--uv", str(tmp_path / "missing-uv")])
    assert code == install.EXIT_ERROR
    assert "explicit uv executable" in capsys.readouterr().err
