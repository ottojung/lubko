"""Tests for per-commit maintained CLI environments and stable launchers."""

from __future__ import annotations

import shutil
import subprocess
from typing import TYPE_CHECKING, Final

import pytest

from lubko import cli
from lubko.state import cli_root_dir, state_root

if TYPE_CHECKING:
    from pathlib import Path

GIT_AUTHOR: Final = "lubko-test"
GIT_EMAIL: Final = "lubko-test@example.com"
ENTRY_POINTS: Final = cli.ENTRY_POINTS
GIT_BIN: Final = shutil.which("git") or "git"


def fake_uv_sync(_uv_path: str, root: Path, _timeout_seconds: float) -> None:
    """Write fake entry-point scripts so no real ``uv sync`` runs.

    Each script echoes its entry point and the owning commit, which lets the
    stable launcher be verified end to end.

    Args:
        _uv_path: Unused ``uv`` path.
        root: Extracted commit tree.
        _timeout_seconds: Unused timeout.
    """
    bin_dir = root / ".venv" / "bin"
    bin_dir.mkdir(parents=True, exist_ok=True)
    for entry in ENTRY_POINTS:
        script = bin_dir / entry
        script.write_text(f"#!/bin/sh\necho {entry}@{root.name}\n", encoding="utf-8")
        script.chmod(0o755)


def git(*args: str, cwd: Path) -> str:
    """Run one git command and return its trimmed stdout.

    Args:
        args: Git arguments.
        cwd: Repository directory.

    Returns:
        Trimmed standard output.
    """
    proc = subprocess.run(
        [GIT_BIN, *args],
        cwd=cwd,
        capture_output=True,
        text=True,
        check=True,
    )
    return proc.stdout.strip()


def make_repo(path: Path) -> tuple[Path, str, str]:
    """Create a small git repository with two commits.

    Args:
        path: Directory to create the repository in.

    Returns:
        The repository path and the two commit hashes.
    """
    path.mkdir()
    git("init", "-q", cwd=path)
    git("config", "user.name", GIT_AUTHOR, cwd=path)
    git("config", "user.email", GIT_EMAIL, cwd=path)
    (path / "marker.txt").write_text("commit-A\n", encoding="utf-8")
    git("add", "marker.txt", cwd=path)
    git("commit", "-q", "-m", "first", cwd=path)
    first = git("rev-parse", "HEAD", cwd=path)
    (path / "marker.txt").write_text("commit-B\n", encoding="utf-8")
    git("add", "marker.txt", cwd=path)
    git("commit", "-q", "-m", "second", cwd=path)
    second = git("rev-parse", "HEAD", cwd=path)
    return path, first, second


def add_commit(path: Path, label: str) -> str:
    """Append one commit to a test repository.

    Args:
        path: Repository directory.
        label: Marker value and commit message.

    Returns:
        The new commit hash.
    """
    (path / "marker.txt").write_text(f"{label}\n", encoding="utf-8")
    git("add", "marker.txt", cwd=path)
    git("commit", "-q", "-m", label, cwd=path)
    return git("rev-parse", "HEAD", cwd=path)


@pytest.fixture(autouse=True)
def state_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point the Lubko state root at a temporary location.

    Returns:
        The temporary XDG state home.
    """
    state = tmp_path / "state"
    monkeypatch.setenv("XDG_STATE_HOME", str(state))
    return state


@pytest.fixture
def two_commit_repo(tmp_path: Path) -> tuple[Path, str, str]:
    """Return a real two-commit git repository.

    Returns:
        Repository path and commit hashes ``(first, second)``.
    """
    return make_repo(tmp_path / "repo")


def test_current_commit_is_none_without_link() -> None:
    """No active commit is reported before any pointer exists."""
    assert cli.current_commit() is None


def test_set_current_switches_atomically() -> None:
    """set_current atomically moves the pointer without temp residue."""
    cli.set_current("a" * 40)
    assert cli.current_commit() == "a" * 40
    cli.set_current("b" * 40)
    assert cli.current_commit() == "b" * 40
    assert not (cli_root_dir() / "current.tmp").exists()
    assert cli_root_dir().is_dir()


def test_build_cli_root_creates_immutable_root(
    two_commit_repo: tuple[Path, str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Each commit owns a separate immutable environment."""
    repo, first, second = two_commit_repo
    monkeypatch.setattr(cli, "_sync_venv", fake_uv_sync)
    cli.build_cli_root(repo, first, "uv", 60.0)
    cli.build_cli_root(repo, second, "uv", 60.0)
    first_root = cli.cli_commit_dir(first)
    second_root = cli.cli_commit_dir(second)
    assert first_root.is_dir()
    assert second_root.is_dir()
    assert (first_root / "marker.txt").read_text(encoding="utf-8") == "commit-A\n"
    assert (second_root / "marker.txt").read_text(encoding="utf-8") == "commit-B\n"
    assert cli.cli_entry_executable(first, "lubko-agent") is not None
    assert cli.cli_entry_executable(second, "lubko-deploy-ctl") is not None
    assert cli.cli_entry_executable(second, "lubko-install") is not None


def test_build_cli_root_is_idempotent(
    two_commit_repo: tuple[Path, str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Rebuilding the same commit reuses the existing environment."""
    repo, first, _second = two_commit_repo
    calls: list[Path] = []

    def recording_sync(_uv_path: str, root: Path, timeout_seconds: float) -> None:
        calls.append(root)
        fake_uv_sync(_uv_path, root, timeout_seconds)

    monkeypatch.setattr(cli, "_sync_venv", recording_sync)
    first_root = cli.build_cli_root(repo, first, "uv", 60.0)
    again = cli.build_cli_root(repo, first, "uv", 60.0)
    assert again == first_root
    assert len(calls) == 1


def test_build_cli_root_failure_cleans_up(
    two_commit_repo: tuple[Path, str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed build leaves no partial CLI environment behind."""
    repo, first, _second = two_commit_repo

    def broken_sync(_uv_path: str, _root: Path, _timeout_seconds: float) -> None:
        msg = "sync boom"
        raise cli.CliError(msg)

    monkeypatch.setattr(cli, "_sync_venv", broken_sync)
    with pytest.raises(cli.CliError, match="sync boom"):
        cli.build_cli_root(repo, first, "uv", 60.0)
    assert not cli.cli_commit_dir(first).exists()


def test_launcher_resolves_active_commit(
    two_commit_repo: tuple[Path, str, str],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The stable launcher follows the current pointer end to end."""
    repo, first, second = two_commit_repo
    monkeypatch.setattr(cli, "_sync_venv", fake_uv_sync)
    bin_dir = tmp_path / "bin"
    cli.build_cli_root(repo, first, "uv", 60.0)
    cli.build_cli_root(repo, second, "uv", 60.0)
    cli.install_launchers(bin_dir)
    cli.set_current(first)
    assert _run_launcher(bin_dir / "lubko-agent") == f"lubko-agent@{first}"
    cli.set_current(second)
    assert _run_launcher(bin_dir / "lubko-agent") == f"lubko-agent@{second}"
    assert _run_launcher(bin_dir / "lubko-deploy-ctl") == f"lubko-deploy-ctl@{second}"


def test_launcher_errors_without_current(tmp_path: Path) -> None:
    """A launcher with no active commit fails with a clear message."""
    bin_dir = tmp_path / "bin"
    cli.install_launchers(bin_dir)
    proc = subprocess.run(
        [str(bin_dir / "lubko-agent")],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode != 0
    assert "no maintained Lubko CLI" in proc.stderr


def test_gc_cli_roots_keeps_only_listed(
    two_commit_repo: tuple[Path, str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Garbage collection removes unkept roots but never the pointer."""
    repo, first, second = two_commit_repo
    monkeypatch.setattr(cli, "_sync_venv", fake_uv_sync)
    third = add_commit(repo, "third")
    cli.build_cli_root(repo, first, "uv", 60.0)
    cli.build_cli_root(repo, second, "uv", 60.0)
    cli.build_cli_root(repo, third, "uv", 60.0)
    cli.set_current(first)
    cli.gc_cli_roots((second, third))
    assert not cli.cli_commit_dir(first).exists()
    assert cli.cli_commit_dir(second).is_dir()
    assert cli.cli_commit_dir(third).is_dir()
    assert cli.current_commit() == first


def test_remove_cli_root_skips_the_active_commit(
    two_commit_repo: tuple[Path, str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The active commit's environment is never removed by cleanup."""
    repo, first, second = two_commit_repo
    monkeypatch.setattr(cli, "_sync_venv", fake_uv_sync)
    cli.build_cli_root(repo, first, "uv", 60.0)
    cli.build_cli_root(repo, second, "uv", 60.0)
    cli.set_current(first)
    cli.remove_cli_root(first)
    assert cli.cli_commit_dir(first).is_dir()
    cli.remove_cli_root(second)
    assert not cli.cli_commit_dir(second).exists()


def test_launcher_source_embeds_state_root() -> None:
    """The launcher bakes in the state root it was installed under."""
    source = cli.launcher_source("lubko-agent")
    assert str(state_root()) in source
    assert "lubko-agent" in source


def test_git_commit_reads_head(two_commit_repo: tuple[Path, str, str]) -> None:
    """git_commit reads HEAD without mutating the checkout."""
    repo, _first, second = two_commit_repo
    assert cli.git_commit(repo, 10.0) == second
    assert any(repo.iterdir())


def _run_launcher(path: Path) -> str:
    """Run a launcher script and return its trimmed stdout.

    Args:
        path: Launcher path.

    Returns:
        The launcher's standard output.
    """
    proc = subprocess.run([str(path)], capture_output=True, text=True, check=True)
    return proc.stdout.strip()
