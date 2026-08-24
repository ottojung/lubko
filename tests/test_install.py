"""Tests for the Lubko command line tool installer."""

import os
import subprocess
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import pytest

from lubko import cli, install, lifecycle, supervise, toolchain
from tests.test_cli import fake_uv_sync, git, make_repo

REQUIRED_ENTRY_POINTS: frozenset[str] = frozenset(cli.ENTRY_POINTS)


def patch_path(monkeypatch: pytest.MonkeyPatch, *directories: Path | str) -> None:
    """Prepend directories to PATH, preserving the rest of the environment.

    ``git`` lives outside ``/usr/bin`` on this host, so replacing PATH would
    break the installer's read-only git calls.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
        directories: Directories to prepend to PATH.
    """
    prefixes = tuple(str(directory) for directory in directories)
    monkeypatch.setenv("PATH", os.pathsep.join((*prefixes, os.environ.get("PATH", ""))))


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
    """Populate a bin directory with every maintained launcher.

    Args:
        bin_dir: Directory to populate.
    """
    cli.install_launchers(bin_dir)


@pytest.fixture(autouse=True)
def toolchain_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Isolate the toolchain and CLI state from the real user state."""
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state"))


@pytest.fixture
def repo_with_commit(tmp_path: Path) -> Path:
    """Return a real Lubko-checkout-shaped git repository.

    Returns:
        The repository path.
    """
    repo, _first, _second = make_repo(tmp_path / "repo")
    (repo / "pyproject.toml").write_text('[project]\nname = "lubko"\n', encoding="utf-8")
    git("add", "pyproject.toml", cwd=repo)
    git("commit", "-q", "-m", "add pyproject", cwd=repo)
    return repo


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
    patch_path(monkeypatch, bin_dir)
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
    repo_with_commit: Path,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A dry run verifies a coherent installation without invoking uv."""
    bin_dir = tmp_path / "bin"
    monkeypatch.setenv("XDG_BIN_HOME", str(bin_dir))
    patch_path(monkeypatch, bin_dir)
    commit = cli.git_commit(repo_with_commit, 10.0)
    assert commit is not None
    monkeypatch.setattr(cli, "_sync_venv", fake_uv_sync)
    cli.build_cli_root(repo_with_commit, commit, "uv", 60.0)
    make_installed_bin(bin_dir)
    cli.set_current(commit)
    code = install.main(["--repo", str(repo_with_commit), "--dry-run"])
    assert code == install.EXIT_OK
    out = capsys.readouterr().out
    assert "Lubko tools installed and resolvable on PATH" in out
    for entry in cli.ENTRY_POINTS:
        assert entry in out


def test_main_installs_launchers_and_activates_commit(
    monkeypatch: pytest.MonkeyPatch,
    repo_with_commit: Path,
    tmp_path: Path,
) -> None:
    """A successful install installs launchers and activates the repo commit."""
    bin_dir = tmp_path / "bin"
    uv_dir = tmp_path / "uv-dir"
    uv_dir.mkdir()
    uv_path = write_uv_executable(uv_dir)
    monkeypatch.setenv("XDG_BIN_HOME", str(bin_dir))
    patch_path(monkeypatch, uv_dir, bin_dir)
    monkeypatch.setattr(cli, "_sync_venv", fake_uv_sync)

    commit = cli.git_commit(repo_with_commit, 10.0)
    assert commit is not None
    code = install.main(["--repo", str(repo_with_commit)])
    assert code == install.EXIT_OK
    assert cli.current_commit() == commit
    assert (bin_dir / "lubko-agent").is_file()
    assert (bin_dir / "lubko-deploy-ctl").is_file()
    assert (bin_dir / "lubko-install").is_file()
    assert (bin_dir / "my-lubko-agent").is_file()
    meta = toolchain.read_toolchain()
    assert meta is not None
    assert meta.uv_path == uv_path


def test_main_persists_resolved_uv(
    monkeypatch: pytest.MonkeyPatch,
    repo_with_commit: Path,
    tmp_path: Path,
) -> None:
    """A successful install records the exact resolved uv executable used."""
    bin_dir = tmp_path / "bin"
    make_installed_bin(bin_dir)
    uv_dir = tmp_path / "uv-dir"
    uv_dir.mkdir()
    uv_path = write_uv_executable(uv_dir)
    monkeypatch.setenv("XDG_BIN_HOME", str(bin_dir))
    patch_path(monkeypatch, uv_dir, bin_dir)

    recorded: list[str] = []

    def recording_build(_repo: Path, commit: str, uv_path_arg: str, timeout: float) -> Path:
        recorded.append(uv_path_arg)
        fake_uv_sync(uv_path_arg, cli.cli_commit_dir(commit), timeout)
        return cli.cli_commit_dir(commit)

    monkeypatch.setattr(cli, "build_cli_root", recording_build)

    code = install.main(["--repo", str(repo_with_commit)])
    assert code == install.EXIT_OK
    assert recorded == [uv_path]
    meta = toolchain.read_toolchain()
    assert meta is not None
    assert meta.uv_path == uv_path


def test_main_persists_exact_resolved_explicit_uv(
    monkeypatch: pytest.MonkeyPatch,
    repo_with_commit: Path,
    tmp_path: Path,
) -> None:
    """A bare --uv name is persisted as its resolved absolute path."""
    bin_dir = tmp_path / "bin"
    make_installed_bin(bin_dir)
    uv_dir = tmp_path / "uv-dir"
    uv_dir.mkdir()
    uv_path = write_uv_executable(uv_dir)
    monkeypatch.setenv("XDG_BIN_HOME", str(bin_dir))
    patch_path(monkeypatch, uv_dir, bin_dir)

    def recording_build(_repo: Path, commit: str, uv_path_arg: str, timeout: float) -> Path:
        fake_uv_sync(uv_path_arg, cli.cli_commit_dir(commit), timeout)
        return cli.cli_commit_dir(commit)

    monkeypatch.setattr(cli, "build_cli_root", recording_build)

    code = install.main(["--repo", str(repo_with_commit), "--uv", "uv"])
    assert code == install.EXIT_OK
    meta = toolchain.read_toolchain()
    assert meta is not None
    assert meta.uv_path == uv_path


def test_main_does_not_persist_on_failed_install(
    monkeypatch: pytest.MonkeyPatch,
    repo_with_commit: Path,
    tmp_path: Path,
) -> None:
    """A failed CLI environment build leaves the toolchain state untouched."""
    bin_dir = tmp_path / "bin"
    make_installed_bin(bin_dir)
    uv_dir = tmp_path / "uv-dir"
    uv_dir.mkdir()
    write_uv_executable(uv_dir)
    monkeypatch.setenv("XDG_BIN_HOME", str(bin_dir))
    patch_path(monkeypatch, uv_dir, bin_dir)

    def broken_build(_repo: Path, _commit: str, _uv_path_arg: str, _timeout: float) -> Path:
        msg = "build boom"
        raise cli.CliError(msg)

    monkeypatch.setattr(cli, "build_cli_root", broken_build)

    code = install.main(["--repo", str(repo_with_commit)])
    assert code == install.EXIT_ERROR
    assert toolchain.read_toolchain() is None


def test_main_fails_when_uv_unresolvable(
    monkeypatch: pytest.MonkeyPatch,
    repo_with_commit: Path,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Without uv on PATH or a recorded toolchain, the installer fails clearly."""
    bin_dir = tmp_path / "bin"
    make_installed_bin(bin_dir)
    monkeypatch.setenv("XDG_BIN_HOME", str(bin_dir))
    monkeypatch.setenv("PATH", "/nonexistent")

    code = install.main(["--repo", str(repo_with_commit)])
    assert code == install.EXIT_ERROR
    assert "uv" in capsys.readouterr().err
    assert toolchain.read_toolchain() is None


def test_main_fails_on_broken_explicit_uv(
    monkeypatch: pytest.MonkeyPatch,
    repo_with_commit: Path,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A broken explicit --uv is refused even when uv is on PATH."""
    bin_dir = tmp_path / "bin"
    make_installed_bin(bin_dir)
    uv_dir = tmp_path / "uv-dir"
    uv_dir.mkdir()
    write_uv_executable(uv_dir)
    monkeypatch.setenv("XDG_BIN_HOME", str(bin_dir))
    patch_path(monkeypatch, uv_dir, bin_dir)

    code = install.main(["--repo", str(repo_with_commit), "--uv", str(tmp_path / "missing-uv")])
    assert code == install.EXIT_ERROR
    assert "explicit uv executable" in capsys.readouterr().err


def test_main_activation_failure_preserves_prior_cli(
    monkeypatch: pytest.MonkeyPatch,
    repo_with_commit: Path,
    tmp_path: Path,
) -> None:
    """A failed CLI switch keeps the previous environment intact and usable."""
    old_commit = cli.git_commit(repo_with_commit, 10.0)
    assert old_commit is not None
    monkeypatch.setattr(cli, "_sync_venv", fake_uv_sync)
    bin_dir = tmp_path / "bin"
    cli.build_cli_root(repo_with_commit, old_commit, "uv", 60.0)
    cli.install_launchers(bin_dir)
    cli.set_current(old_commit)

    uv_dir = tmp_path / "uv-dir"
    uv_dir.mkdir()
    write_uv_executable(uv_dir)
    monkeypatch.setenv("XDG_BIN_HOME", str(bin_dir))
    patch_path(monkeypatch, uv_dir, bin_dir)

    def broken_set_current(_commit: str) -> None:
        msg = "switch boom"
        raise cli.CliError(msg)

    monkeypatch.setattr(cli, "set_current", broken_set_current)

    code = install.main(["--repo", str(repo_with_commit)])
    assert code == install.EXIT_ERROR
    assert cli.current_commit() == old_commit
    assert cli.cli_commit_dir(old_commit).is_dir()
    assert toolchain.read_toolchain() is None
    proc = subprocess.run(
        [str(bin_dir / "lubko-agent")],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0
    assert proc.stdout.strip() == f"lubko-agent@{old_commit}"


# ---------------------------------------------------------------------------
# Supervisor-authoritative runtime preservation (issue #183)
# ---------------------------------------------------------------------------


def make_repo_with_pyproject(
    tmp_path: Path,
) -> tuple[Path, str, str]:
    """Return a two-commit repo plus a third HEAD commit adding pyproject.toml.

    Returns:
        The repository path, the first two commit hashes, with HEAD at the
        ``lubko``-shaped checkout commit.
    """
    repo, first, second = make_repo(tmp_path / "repo")
    (repo / "pyproject.toml").write_text('[project]\nname = "lubko"\n', encoding="utf-8")
    git("add", "pyproject.toml", cwd=repo)
    git("commit", "-q", "-m", "add pyproject", cwd=repo)
    return repo, first, second


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


def installable_bin(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> Path:
    """Prepare bin/PATH/uv so ``install.main`` can run to completion.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
        tmp_path: Temporary directory for bin and uv helpers.

    Returns:
        The prepared bin directory.
    """
    uv_dir = tmp_path / "uv-dir"
    uv_dir.mkdir()
    write_uv_executable(uv_dir)
    bin_dir = tmp_path / "bin"
    make_installed_bin(bin_dir)
    monkeypatch.setenv("XDG_BIN_HOME", str(bin_dir))
    patch_path(monkeypatch, uv_dir, bin_dir)
    return bin_dir


def test_main_refuses_version_changing_install_over_desired_worker(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Installing a different commit than the desired worker fails closed."""
    repo, first, _second = make_repo_with_pyproject(tmp_path)
    monkeypatch.setattr(cli, "_sync_venv", fake_uv_sync)
    installable_bin(monkeypatch, tmp_path)
    write_desired_commit(first, repo)

    code = install.main(["--repo", str(repo)])

    assert code == install.EXIT_ERROR
    err = capsys.readouterr().err
    assert "refusing to install commit" in err
    assert "lubko-deploy" in err
    assert cli.current_commit() is None


def test_refused_install_keeps_worker_runtime_startable(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """After refusal the supervisor can still recover its maintained runtime."""
    repo, _first, second = make_repo_with_pyproject(tmp_path)
    head = git("rev-parse", "HEAD", cwd=repo)
    monkeypatch.setattr(cli, "_sync_venv", fake_uv_sync)
    installable_bin(monkeypatch, tmp_path)
    cli.build_cli_root(repo, second, "uv", 60.0)
    cli.set_current(second)
    write_desired_commit(second, repo)

    code = install.main(["--repo", str(repo)])

    assert code == install.EXIT_ERROR
    err = capsys.readouterr().err
    assert f"refusing to install commit {head}" in err
    assert cli.runtime_is_usable(second)
    assert cli.current_commit() == second
    assert cli.reconcile_pointer(second) is True


def test_main_allows_same_commit_install_with_authority(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A same-commit fresh install succeeds and GC keeps only authority roots."""
    repo, first, _second = make_repo_with_pyproject(tmp_path)
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
    assert cli.cli_commit_dir(head).is_dir()
    assert not cli.cli_commit_dir(first).exists()
    meta = toolchain.read_toolchain()
    assert meta is not None


def test_main_refuses_version_changing_install_over_override(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A bootstrap override naming another commit blocks version-changing installs."""
    repo, first, second = make_repo_with_pyproject(tmp_path)
    head = git("rev-parse", "HEAD", cwd=repo)
    monkeypatch.setattr(cli, "_sync_venv", fake_uv_sync)
    installable_bin(monkeypatch, tmp_path)
    cli.build_cli_root(repo, first, "uv", 60.0)
    cli.set_current(first)
    supervise.write_supervisor_runtime_override(second)

    code = install.main(["--repo", str(repo)])

    assert code == install.EXIT_ERROR
    err = capsys.readouterr().err
    assert f"refusing to install commit {head}" in err
    assert second in err
    assert cli.current_commit() == first
    assert cli.runtime_is_usable(first)


def test_main_rechecks_authority_under_deploy_lock(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Authority appearing between the pre-check and the lock still aborts.

    A concurrent deploy may write its desired intent right after the
    installer's unlocked pre-check; the guard re-evaluated inside the
    deployment-lock critical section must catch it (TOCTOU regression).
    """
    repo, first, _second = make_repo_with_pyproject(tmp_path)
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


def test_main_fails_closed_when_deploy_lock_is_busy(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A held deployment lock makes the installer refuse without mutating CLIs."""
    repo, _first, _second = make_repo_with_pyproject(tmp_path)
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
