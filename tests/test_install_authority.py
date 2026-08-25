"""Installer/CLI-runtime coherence with supervisor-authoritative state.

The supervisor's desired intent, applied state, and runtime override each
name a commit that must stay startable; the installer and garbage collection
must never strand those runtimes or diverge ``cli/current`` from the
maintained worker.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import threading
from contextlib import contextmanager
from typing import TYPE_CHECKING, Final

import pytest

from lubko import cli, install, lifecycle, supervise, toolchain

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

ENTRY_POINTS: Final = cli.ENTRY_POINTS
GIT_BIN: Final = shutil.which("git") or "git"


def fake_uv_sync(_uv_path: str, root: Path, _timeout_seconds: float) -> None:
    """Materialize fake entry-point scripts so no real ``uv sync`` runs."""
    bin_dir = root / ".venv" / "bin"
    bin_dir.mkdir(parents=True, exist_ok=True)
    for entry in ENTRY_POINTS:
        script = bin_dir / entry
        script.write_text(f"#!/bin/sh\necho {entry}@{root.name}\n", encoding="utf-8")
        script.chmod(0o755)


def git(*args: str, cwd: Path) -> str:
    """Run one git command and return its trimmed stdout.

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


def make_repo_with_pyproject(path: Path) -> tuple[Path, str]:
    """Create a two-commit git repo plus a HEAD commit adding pyproject.toml.

    Returns:
        The repository path and the first (pre-HEAD) commit hash.
    """
    path.mkdir(parents=True)
    git("init", "-q", cwd=path)
    git("config", "user.name", "lubko-test", cwd=path)
    git("config", "user.email", "lubko-test@example.com", cwd=path)
    (path / "marker.txt").write_text("A\n", encoding="utf-8")
    git("add", "marker.txt", cwd=path)
    git("commit", "-q", "-m", "first", cwd=path)
    first = git("rev-parse", "HEAD", cwd=path)
    (path / "marker.txt").write_text("B\n", encoding="utf-8")
    git("add", "marker.txt", cwd=path)
    git("commit", "-q", "-m", "second", cwd=path)
    (path / "pyproject.toml").write_text('[project]\nname = "lubko"\n', encoding="utf-8")
    git("add", "pyproject.toml", cwd=path)
    git("commit", "-q", "-m", "pyproject", cwd=path)
    return path, first


@pytest.fixture(autouse=True)
def isolated_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Point Lubko state at a temporary location."""
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state"))


def installable_bin(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    """Prepare bin/PATH so ``install.main`` can run to completion.

    Returns:
        The prepared bin directory.
    """
    uv_dir = tmp_path / "uv-dir"
    uv_dir.mkdir()
    uv = uv_dir / "uv"
    uv.write_text("#!/bin/sh\nexit 0\n")
    uv.chmod(0o755)
    bin_dir = tmp_path / "bin"
    cli.install_launchers(bin_dir)
    monkeypatch.setenv("XDG_BIN_HOME", str(bin_dir))
    monkeypatch.setenv("PATH", os.pathsep.join((str(uv_dir), os.environ.get("PATH", ""))))
    return bin_dir


def write_desired_commit(commit: str, repo: Path) -> None:
    """Persist a minimal desired intent naming one commit."""
    supervise.write_desired(
        supervise.SupervisorDesired(
            schema_version=supervise.SCHEMA_VERSION,
            generation=1,
            commit=commit,
            repo=str(repo),
            uv_path="uv",
            worker_id=None,
        )
    )


def test_gc_preserves_desired_applied_and_override_runtimes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """GC and explicit removal never delete supervisor-authoritative runtimes."""
    repo, first = make_repo_with_pyproject(tmp_path / "repo")
    second = git("rev-parse", "HEAD", cwd=repo)
    monkeypatch.setattr(cli, "_sync_venv", fake_uv_sync)
    for commit in (first, second):
        cli.build_cli_root(repo, commit, "uv", 60.0)
    cli.set_current(first)
    write_desired_commit(second, repo)
    supervise.write_state(
        supervise.SupervisorState(
            schema_version=supervise.SCHEMA_VERSION,
            applied_generation=1,
            mode=supervise.MODE_RUN,
            commit=first,
            child=None,
            unresolved_child=None,
            unresolved_hold_malformed=False,
            intent="run",
            restart_count=0,
            next_attempt_at=None,
            last_exit=None,
            last_spawn_at=None,
            ready=True,
            next_readiness_at=None,
            boot_id=None,
        )
    )
    override = "c" * 40
    supervise.write_supervisor_runtime_override(override)
    cli.cli_commit_dir(override).mkdir(parents=True)

    cli.gc_cli_roots(())

    for commit in (first, second, override):
        assert cli.cli_commit_dir(commit).is_dir()
    cli.remove_cli_root(first)
    cli.remove_cli_root(second)
    assert cli.cli_commit_dir(first).is_dir()
    assert cli.cli_commit_dir(second).is_dir()


def test_install_refuses_version_change_over_desired_worker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Installing a different commit than the desired worker fails closed."""
    repo, first = make_repo_with_pyproject(tmp_path / "repo")
    monkeypatch.setattr(cli, "_sync_venv", fake_uv_sync)
    installable_bin(monkeypatch, tmp_path)
    write_desired_commit(first, repo)

    code = install.main(["--repo", str(repo)])

    assert code == install.EXIT_ERROR
    err = capsys.readouterr().err
    assert "refusing to install commit" in err
    assert "lubko-deploy" in err
    assert cli.current_commit() is None


def test_same_commit_install_keeps_worker_runtime_startable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """After refusal the supervisor can still recover its maintained runtime."""
    repo, _first = make_repo_with_pyproject(tmp_path / "repo")
    head = git("rev-parse", "HEAD", cwd=repo)
    monkeypatch.setattr(cli, "_sync_venv", fake_uv_sync)
    installable_bin(monkeypatch, tmp_path)
    cli.build_cli_root(repo, head, "uv", 60.0)
    cli.set_current(head)
    write_desired_commit(head, repo)

    code = install.main(["--repo", str(repo)])

    assert code == install.EXIT_OK
    assert cli.current_commit() == head
    assert cli.runtime_is_usable(head)
    assert cli.reconcile_pointer(head) is True


def test_same_commit_install_succeeds_and_gcs_stale_roots(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A same-commit fresh install succeeds and GC keeps only authority roots."""
    repo, first = make_repo_with_pyproject(tmp_path / "repo")
    head = git("rev-parse", "HEAD", cwd=repo)
    monkeypatch.setattr(cli, "_sync_venv", fake_uv_sync)
    installable_bin(monkeypatch, tmp_path)
    cli.build_cli_root(repo, first, "uv", 60.0)
    cli.build_cli_root(repo, head, "uv", 60.0)
    cli.set_current(head)
    write_desired_commit(head, repo)

    code = install.main(["--repo", str(repo)])

    assert code == install.EXIT_OK
    assert cli.current_commit() == head
    assert not cli.cli_commit_dir(first).exists()
    meta = toolchain.read_toolchain()
    assert meta is not None


def test_install_refuses_version_change_over_override(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A bootstrap override naming another commit blocks version-changing installs."""
    repo, _first = make_repo_with_pyproject(tmp_path / "repo")
    head = git("rev-parse", "HEAD", cwd=repo)
    monkeypatch.setattr(cli, "_sync_venv", fake_uv_sync)
    bin_dir = installable_bin(monkeypatch, tmp_path)
    override = "d" * 40
    supervise.write_supervisor_runtime_override(override)

    code = install.main(["--repo", str(repo)])

    assert code == install.EXIT_ERROR
    err = capsys.readouterr().err
    assert f"refusing to install commit {head}" in err
    assert override in err
    assert cli.current_commit() is None
    assert (bin_dir / "lubko-agent").is_file()


def test_install_rechecks_authority_under_deploy_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Authority appearing between pre-check and lock still aborts.

    A concurrent deploy may write its desired intent right after the
    installer's unlocked pre-check; the guard re-evaluated inside the
    deployment-lock critical section must catch it.
    """
    repo, first = make_repo_with_pyproject(tmp_path / "repo")
    monkeypatch.setattr(cli, "_sync_venv", fake_uv_sync)
    installable_bin(monkeypatch, tmp_path)
    real_lock = lifecycle.deploy_lock

    @contextmanager
    def racing_lock(timeout_seconds: float) -> Iterator[None]:
        with real_lock(timeout_seconds):
            write_desired_commit(first, repo)
            yield

    monkeypatch.setattr(lifecycle, "deploy_lock", racing_lock)

    code = install.main(["--repo", str(repo)])

    assert code == install.EXIT_ERROR
    assert "refusing to install commit" in capsys.readouterr().err
    assert cli.current_commit() is None


def test_install_fails_closed_when_deploy_lock_is_busy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A held deployment lock makes the installer refuse without mutating CLIs."""
    repo, _first = make_repo_with_pyproject(tmp_path / "repo")
    monkeypatch.setattr(cli, "_sync_venv", fake_uv_sync)
    bin_dir = installable_bin(monkeypatch, tmp_path)
    monkeypatch.setattr(lifecycle, "DEFAULT_LOCK_TIMEOUT_SECONDS", 0.2)

    release = threading.Event()

    def hold_lock() -> None:
        with lifecycle.deploy_lock(10.0):
            release.wait(timeout=30)

    holder = threading.Thread(target=hold_lock)
    holder.start()
    try:
        while True:
            try:
                with lifecycle.deploy_lock(0.05):
                    continue
            except lifecycle.LockTimeoutError:
                break
        code = install.main(["--repo", str(repo)])
    finally:
        release.set()
        holder.join()

    assert code == install.EXIT_ERROR
    assert "deployment lock" in capsys.readouterr().err
    assert cli.current_commit() is None
    assert (bin_dir / "lubko-agent").is_file()
