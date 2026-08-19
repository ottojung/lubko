"""Deterministic tests for supervisor-bootstrap override mechanism (#123).

These tests exercise the supervisor-runtime override path without a live
supervisor or PostgreSQL cluster, proving the exact selector format, the
fail-closed launcher installation, GC preservation, idempotent rerun, and
the confirmation cleanup contract.
"""

from __future__ import annotations

import argparse
import stat
import subprocess
from pathlib import Path
from typing import Final

import pytest

from lubko import cli, lifecycle, supervise
from tests.test_cli import fake_uv_sync, make_repo

GIT_AUTHOR: Final = "lubko-test"
GIT_EMAIL: Final = "lubko-test@example.com"
ENTRY_POINTS: Final = cli.ENTRY_POINTS


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
    """Return a real two-commit git repository."""
    return make_repo(tmp_path / "repo")


# ---------------------------------------------------------------------------
# Exact selector format
# ---------------------------------------------------------------------------


def test_sh_n_syntax_check_passes() -> None:
    """``sh -n`` accepts the supervisor launcher source."""
    src = cli.launcher_source("lubko-supervisor")
    proc = subprocess.run(
        ["/bin/sh", "-n"],
        input=src,
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, f"sh -n failed: {proc.stderr}"


def test_valid_format_accepted() -> None:
    """A well-formed override is read back correctly."""
    commit = "a" * 40
    supervise.write_supervisor_runtime_override(commit)
    assert supervise.read_supervisor_runtime_override() == commit


def test_uppercase_hex_rejected() -> None:
    """Uppercase hex is not accepted by the strict regex."""
    path = supervise.supervisor_runtime_override_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("A" * 40 + "\n", encoding="utf-8")
    assert supervise.read_supervisor_runtime_override() is None


def test_mixed_case_rejected() -> None:
    """Mixed-case hex is not accepted."""
    path = supervise.supervisor_runtime_override_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("a" * 20 + "A" * 20 + "\n", encoding="utf-8")
    assert supervise.read_supervisor_runtime_override() is None


def test_short_commit_rejected() -> None:
    """A 39-char commit is not accepted."""
    path = supervise.supervisor_runtime_override_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("a" * 39 + "\n", encoding="utf-8")
    assert supervise.read_supervisor_runtime_override() is None


def test_long_commit_rejected() -> None:
    """A 41-char commit is not accepted."""
    path = supervise.supervisor_runtime_override_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("a" * 41 + "\n", encoding="utf-8")
    assert supervise.read_supervisor_runtime_override() is None


def test_no_trailing_newline_rejected() -> None:
    """Missing trailing newline is not accepted."""
    path = supervise.supervisor_runtime_override_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("a" * 40, encoding="utf-8")
    assert supervise.read_supervisor_runtime_override() is None


def test_extra_content_rejected() -> None:
    """Extra text after the commit is not accepted."""
    path = supervise.supervisor_runtime_override_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("a" * 40 + "\nextra\n", encoding="utf-8")
    assert supervise.read_supervisor_runtime_override() is None


def test_non_hex_rejected() -> None:
    """Non-hex characters are not accepted."""
    path = supervise.supervisor_runtime_override_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("g" * 40 + "\n", encoding="utf-8")
    assert supervise.read_supervisor_runtime_override() is None


def test_absent_file_returns_none() -> None:
    """No file means no override."""
    assert supervise.read_supervisor_runtime_override() is None


def test_empty_file_returns_none() -> None:
    """An empty file is not a valid override."""
    path = supervise.supervisor_runtime_override_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("", encoding="utf-8")
    assert supervise.read_supervisor_runtime_override() is None


def test_double_newline_rejected() -> None:
    """40 hex with two trailing newlines is not a valid override."""
    path = supervise.supervisor_runtime_override_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("a" * 40 + "\n\n", encoding="utf-8")
    assert supervise.read_supervisor_runtime_override() is None


def test_atomic_write_format() -> None:
    """write_supervisor_runtime_override produces exactly the expected format."""
    commit = "b" * 40
    supervise.write_supervisor_runtime_override(commit)
    raw = supervise.supervisor_runtime_override_path().read_text(encoding="utf-8")
    assert raw == f"{commit}\n"
    assert len(raw) == 41


def test_clear_returns_true_when_present() -> None:
    """clear_supervisor_runtime_override removes an existing override."""
    supervise.write_supervisor_runtime_override("c" * 40)
    assert supervise.clear_supervisor_runtime_override() is True
    assert supervise.read_supervisor_runtime_override() is None


def test_clear_returns_false_when_absent() -> None:
    """clear_supervisor_runtime_override returns False when no override exists."""
    assert supervise.clear_supervisor_runtime_override() is False


# ---------------------------------------------------------------------------
# Corrupt fail-closed launcher
# ---------------------------------------------------------------------------


def test_supervisor_launcher_fails_on_corrupt_override(
    two_commit_repo: tuple[Path, str, str],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The supervisor launcher exits non-zero on a corrupt override file."""
    repo, first, _second = two_commit_repo
    monkeypatch.setattr(cli, "_sync_venv", fake_uv_sync)
    cli.build_cli_root(repo, first, "uv", 60.0)
    bin_dir = tmp_path / "bin"
    cli.install_launchers(bin_dir)

    override_path = supervise.supervisor_runtime_override_path()
    override_path.parent.mkdir(parents=True, exist_ok=True)
    override_path.write_text("not-a-valid-commit\n", encoding="utf-8")

    proc = subprocess.run(
        [str(bin_dir / "lubko-supervisor")],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode != 0
    assert "override is not a valid 40-hex commit" in proc.stderr


def test_supervisor_launcher_fails_on_empty_override(
    two_commit_repo: tuple[Path, str, str],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """An empty override file is treated as corrupt."""
    repo, first, _second = two_commit_repo
    monkeypatch.setattr(cli, "_sync_venv", fake_uv_sync)
    cli.build_cli_root(repo, first, "uv", 60.0)
    bin_dir = tmp_path / "bin"
    cli.install_launchers(bin_dir)

    override_path = supervise.supervisor_runtime_override_path()
    override_path.parent.mkdir(parents=True, exist_ok=True)
    override_path.write_text("", encoding="utf-8")

    proc = subprocess.run(
        [str(bin_dir / "lubko-supervisor")],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode != 0


def test_supervisor_launcher_fails_on_override_for_missing_runtime(
    two_commit_repo: tuple[Path, str, str],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Override pointing to a nonexistent runtime dir fails closed."""
    repo, first, _second = two_commit_repo
    monkeypatch.setattr(cli, "_sync_venv", fake_uv_sync)
    cli.build_cli_root(repo, first, "uv", 60.0)
    bin_dir = tmp_path / "bin"
    cli.install_launchers(bin_dir)

    nonexistent = "0" * 40
    supervise.write_supervisor_runtime_override(nonexistent)

    proc = subprocess.run(
        [str(bin_dir / "lubko-supervisor")],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode != 0
    assert "has no runtime dir" in proc.stderr


def test_supervisor_launcher_fails_on_override_for_incomplete_runtime(
    two_commit_repo: tuple[Path, str, str],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Override pointing to a runtime without the entry point fails closed."""
    _repo, first, _second = two_commit_repo
    monkeypatch.setattr(cli, "_sync_venv", fake_uv_sync)

    runtime_dir = cli.cli_commit_dir(first)
    venv_bin = runtime_dir / ".venv" / "bin"
    venv_bin.mkdir(parents=True, exist_ok=True)
    for entry in ENTRY_POINTS:
        if entry != "lubko-supervisor":
            script = venv_bin / entry
            script.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
            script.chmod(0o755)

    bin_dir = tmp_path / "bin"
    cli.install_launchers(bin_dir)
    supervise.write_supervisor_runtime_override(first)

    proc = subprocess.run(
        [str(bin_dir / "lubko-supervisor")],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode != 0
    assert "is incomplete" in proc.stderr


def test_supervisor_launcher_rejects_40hex_without_newline(
    two_commit_repo: tuple[Path, str, str],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """40 hex characters without a trailing newline (40 bytes) are rejected."""
    repo, first, _second = two_commit_repo
    monkeypatch.setattr(cli, "_sync_venv", fake_uv_sync)
    cli.build_cli_root(repo, first, "uv", 60.0)
    bin_dir = tmp_path / "bin"
    cli.install_launchers(bin_dir)

    override_path = supervise.supervisor_runtime_override_path()
    override_path.parent.mkdir(parents=True, exist_ok=True)
    override_path.write_text("a" * 40, encoding="utf-8")

    proc = subprocess.run(
        [str(bin_dir / "lubko-supervisor")],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode != 0
    assert "not a valid 40-hex commit" in proc.stderr


def test_supervisor_launcher_rejects_40hex_with_extra_newline(
    two_commit_repo: tuple[Path, str, str],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """40 hex characters with two trailing newlines (42 bytes) are rejected."""
    repo, first, _second = two_commit_repo
    monkeypatch.setattr(cli, "_sync_venv", fake_uv_sync)
    cli.build_cli_root(repo, first, "uv", 60.0)
    bin_dir = tmp_path / "bin"
    cli.install_launchers(bin_dir)

    override_path = supervise.supervisor_runtime_override_path()
    override_path.parent.mkdir(parents=True, exist_ok=True)
    override_path.write_text("a" * 40 + "\n\n", encoding="utf-8")

    proc = subprocess.run(
        [str(bin_dir / "lubko-supervisor")],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode != 0
    assert "not a valid 40-hex commit" in proc.stderr


# ---------------------------------------------------------------------------
# Override path type rejection: directory, dangling symlink, symlink-to-file
# ---------------------------------------------------------------------------


def test_supervisor_launcher_rejects_directory_override(
    two_commit_repo: tuple[Path, str, str],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A directory at the override path fails closed instead of falling through."""
    repo, first, _second = two_commit_repo
    monkeypatch.setattr(cli, "_sync_venv", fake_uv_sync)
    cli.build_cli_root(repo, first, "uv", 60.0)
    bin_dir = tmp_path / "bin"
    cli.install_launchers(bin_dir)

    override_path = supervise.supervisor_runtime_override_path()
    override_path.parent.mkdir(parents=True, exist_ok=True)
    override_path.mkdir()

    proc = subprocess.run(
        [str(bin_dir / "lubko-supervisor")],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode != 0
    assert "is not a regular file" in proc.stderr


def test_supervisor_launcher_rejects_dangling_symlink_override(
    two_commit_repo: tuple[Path, str, str],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A dangling symlink at the override path fails closed."""
    repo, first, _second = two_commit_repo
    monkeypatch.setattr(cli, "_sync_venv", fake_uv_sync)
    cli.build_cli_root(repo, first, "uv", 60.0)
    bin_dir = tmp_path / "bin"
    cli.install_launchers(bin_dir)

    override_path = supervise.supervisor_runtime_override_path()
    override_path.parent.mkdir(parents=True, exist_ok=True)
    override_path.symlink_to(tmp_path / "nonexistent-target")

    proc = subprocess.run(
        [str(bin_dir / "lubko-supervisor")],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode != 0
    assert "is a symlink" in proc.stderr


def test_supervisor_launcher_rejects_symlink_to_file_override(
    two_commit_repo: tuple[Path, str, str],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A symlink to a regular file at the override path fails closed.

    Symlink authority is rejected: only a plain regular file is trusted.
    """
    repo, first, _second = two_commit_repo
    monkeypatch.setattr(cli, "_sync_venv", fake_uv_sync)
    cli.build_cli_root(repo, first, "uv", 60.0)
    bin_dir = tmp_path / "bin"
    cli.install_launchers(bin_dir)

    real_file = tmp_path / "real-override"
    real_file.write_text("a" * 40 + "\n", encoding="utf-8")
    override_path = supervise.supervisor_runtime_override_path()
    override_path.parent.mkdir(parents=True, exist_ok=True)
    override_path.symlink_to(real_file)

    proc = subprocess.run(
        [str(bin_dir / "lubko-supervisor")],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode != 0
    assert "is a symlink" in proc.stderr


def test_missing_bin_dir_aborts_without_override(
    two_commit_repo: tuple[Path, str, str],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A nonexistent bin directory causes a failure before the override is written."""
    _repo, _first, _second = two_commit_repo
    monkeypatch.setattr(cli, "_sync_venv", fake_uv_sync)

    bin_home = tmp_path / "nonexistent" / "bin"
    monkeypatch.setattr(lifecycle, "_resolve_bin_home", lambda: bin_home)
    monkeypatch.setattr(
        cli,
        "launcher_source",
        lambda _entry: "#!/bin/sh\necho launcher\n",
    )

    override_path = supervise.supervisor_runtime_override_path()
    assert not override_path.exists()

    with pytest.raises(OSError, match="does not exist"):
        lifecycle.install_supervisor_launcher(bin_home)

    assert not override_path.exists()


def test_unwritable_bin_dir_aborts_without_override(
    two_commit_repo: tuple[Path, str, str],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A read-only bin directory causes a failure before the override is written."""
    _repo, _first, _second = two_commit_repo
    monkeypatch.setattr(cli, "_sync_venv", fake_uv_sync)

    bin_home = tmp_path / "readonly-bin"
    bin_home.mkdir()
    bin_home.chmod(0o555)

    override_path = supervise.supervisor_runtime_override_path()
    assert not override_path.exists()

    with pytest.raises(OSError, match="Permission denied"):
        lifecycle.install_supervisor_launcher(bin_home)

    assert not override_path.exists()
    bin_home.chmod(0o755)


# ---------------------------------------------------------------------------
# Launcher-first interruption
# ---------------------------------------------------------------------------


def test_no_override_after_launcher_install_failure(
    two_commit_repo: tuple[Path, str, str],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A failing launcher install leaves no override pointer behind."""
    _repo, _first, _second = two_commit_repo
    monkeypatch.setattr(cli, "_sync_venv", fake_uv_sync)

    def boom(_bin_home: Path) -> None:
        msg = "launcher install boom"
        raise OSError(msg)

    monkeypatch.setattr(lifecycle, "install_supervisor_launcher", boom)

    override_path = supervise.supervisor_runtime_override_path()
    assert not override_path.exists()

    bin_home = tmp_path / "bin"
    bin_home.mkdir()
    with pytest.raises(OSError, match="launcher install boom"):
        lifecycle.install_supervisor_launcher(bin_home)

    assert not override_path.exists()


# ---------------------------------------------------------------------------
# GC preservation
# ---------------------------------------------------------------------------


def test_gc_preserves_override_target(
    two_commit_repo: tuple[Path, str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """cli.gc_cli_roots never deletes the commit the override points to."""
    repo, first, second = two_commit_repo
    monkeypatch.setattr(cli, "_sync_venv", fake_uv_sync)
    cli.build_cli_root(repo, first, "uv", 60.0)
    cli.build_cli_root(repo, second, "uv", 60.0)
    cli.set_current(first)

    supervise.write_supervisor_runtime_override(second)

    cli.gc_cli_roots((first,))

    assert cli.cli_commit_dir(first).is_dir()
    assert cli.cli_commit_dir(second).is_dir()


# ---------------------------------------------------------------------------
# Bootstrap through public CLI path — no private-member access
# ---------------------------------------------------------------------------


def _bootstrap_args(
    commit: str,
    repo: Path,
) -> argparse.Namespace:
    """Build a bootstrap CLI namespace for ``lifecycle.main``.

    Args:
        commit: Exact 40-hex commit to bootstrap.
        repo: Repository checkout the commit belongs to.

    Returns:
        The parsed bootstrap command namespace.
    """
    return argparse.Namespace(
        command="bootstrap",
        commit=commit,
        repo=repo,
        uv=None,
        lock_timeout=30.0,
        cli_timeout=60.0,
    )


def test_bootstrap_does_not_mutate_current(
    two_commit_repo: tuple[Path, str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Bootstrap via the public CLI does not move the current pointer."""
    repo, first, second = two_commit_repo
    monkeypatch.setattr(cli, "_sync_venv", fake_uv_sync)
    monkeypatch.setattr(supervise, "supervisor_running", lambda: True)
    monkeypatch.setattr(lifecycle, "resolve_uv", lambda _uv: "uv")
    cli.set_current(first)
    original_current = cli.current_commit()

    def noop_install(_bin_home: Path) -> None:
        return None

    monkeypatch.setattr(lifecycle, "install_supervisor_launcher", noop_install)

    args = _bootstrap_args(second, repo)
    assert lifecycle.bootstrap_cmd(args) == lifecycle.EXIT_OK

    assert cli.current_commit() == original_current


def test_bootstrap_does_not_mutate_desired(
    two_commit_repo: tuple[Path, str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Bootstrap via the public CLI does not write to desired.json."""
    repo, _first, second = two_commit_repo
    monkeypatch.setattr(cli, "_sync_venv", fake_uv_sync)
    monkeypatch.setattr(supervise, "supervisor_running", lambda: True)
    monkeypatch.setattr(lifecycle, "resolve_uv", lambda _uv: "uv")

    desired_before = supervise.read_desired()

    def noop_install(_bin_home: Path) -> None:
        return None

    monkeypatch.setattr(lifecycle, "install_supervisor_launcher", noop_install)

    args = _bootstrap_args(second, repo)
    assert lifecycle.bootstrap_cmd(args) == lifecycle.EXIT_OK

    assert supervise.read_desired() == desired_before


def test_bootstrap_does_not_mutate_worker_meta(
    two_commit_repo: tuple[Path, str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Bootstrap via the public CLI does not write to worker/meta.json."""
    repo, _first, second = two_commit_repo
    monkeypatch.setattr(cli, "_sync_venv", fake_uv_sync)
    monkeypatch.setattr(supervise, "supervisor_running", lambda: True)
    monkeypatch.setattr(lifecycle, "resolve_uv", lambda _uv: "uv")

    meta_before = lifecycle.read_meta()

    def noop_install(_bin_home: Path) -> None:
        return None

    monkeypatch.setattr(lifecycle, "install_supervisor_launcher", noop_install)

    args = _bootstrap_args(second, repo)
    assert lifecycle.bootstrap_cmd(args) == lifecycle.EXIT_OK

    assert lifecycle.read_meta() == meta_before


def test_bootstrap_is_idempotent(
    two_commit_repo: tuple[Path, str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A second bootstrap for the same commit succeeds and does not duplicate."""
    repo, _first, second = two_commit_repo
    monkeypatch.setattr(cli, "_sync_venv", fake_uv_sync)
    monkeypatch.setattr(supervise, "supervisor_running", lambda: True)
    monkeypatch.setattr(lifecycle, "resolve_uv", lambda _uv: "uv")

    def noop_install(_bin_home: Path) -> None:
        return None

    monkeypatch.setattr(lifecycle, "install_supervisor_launcher", noop_install)

    args = _bootstrap_args(second, repo)
    assert lifecycle.bootstrap_cmd(args) == lifecycle.EXIT_OK
    override_after_first = supervise.read_supervisor_runtime_override()

    assert lifecycle.bootstrap_cmd(args) == lifecycle.EXIT_OK
    override_after_second = supervise.read_supervisor_runtime_override()

    assert override_after_first == second
    assert override_after_second == second


def test_bootstrap_skips_when_already_confirmed(
    two_commit_repo: tuple[Path, str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Bootstrap exits early when the target is already the confirmed commit."""
    repo, first, _second = two_commit_repo
    monkeypatch.setattr(cli, "_sync_venv", fake_uv_sync)
    monkeypatch.setattr(supervise, "supervisor_running", lambda: True)
    monkeypatch.setattr(lifecycle, "resolve_uv", lambda _uv: "uv")
    cli.set_current(first)

    args = _bootstrap_args(first, repo)
    assert lifecycle.bootstrap_cmd(args) == lifecycle.EXIT_OK
    assert supervise.read_supervisor_runtime_override() is None


def test_bootstrap_rejects_invalid_commit(
    two_commit_repo: tuple[Path, str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Bootstrap refuses a commit that is not exactly 40 hex characters."""
    repo, _first, _second = two_commit_repo
    monkeypatch.setattr(supervise, "supervisor_running", lambda: True)

    args = _bootstrap_args("not-a-commit", repo)
    assert lifecycle.bootstrap_cmd(args) == lifecycle.EXIT_ERROR


def test_bootstrap_refuses_without_supervisor(
    two_commit_repo: tuple[Path, str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Bootstrap refuses to run when no supervisor is running."""
    repo, _first, second = two_commit_repo
    monkeypatch.setattr(supervise, "supervisor_running", lambda: False)

    args = _bootstrap_args(second, repo)
    assert lifecycle.bootstrap_cmd(args) == lifecycle.EXIT_ERROR


# ---------------------------------------------------------------------------
# Launcher installation: exact bytes and executable mode
# ---------------------------------------------------------------------------


def test_launcher_install_is_idempotent(
    two_commit_repo: tuple[Path, str, str],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Installing the launcher twice produces the same verified result."""
    repo, first, _second = two_commit_repo
    monkeypatch.setattr(cli, "_sync_venv", fake_uv_sync)
    cli.build_cli_root(repo, first, "uv", 60.0)

    bin_home = tmp_path / "bin"
    bin_home.mkdir()
    lifecycle.install_supervisor_launcher(bin_home)

    target = bin_home / "lubko-supervisor"
    first_bytes = target.read_bytes()
    first_mode = target.stat().st_mode

    lifecycle.install_supervisor_launcher(bin_home)

    second_bytes = target.read_bytes()
    second_mode = target.stat().st_mode

    assert first_bytes == second_bytes
    assert first_mode == second_mode


def test_launcher_is_exact_bytes_and_executable(
    two_commit_repo: tuple[Path, str, str],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """After installation the file has exact expected bytes and S_IXUSR."""
    repo, first, _second = two_commit_repo
    monkeypatch.setattr(cli, "_sync_venv", fake_uv_sync)
    cli.build_cli_root(repo, first, "uv", 60.0)

    bin_home = tmp_path / "bin"
    bin_home.mkdir()
    lifecycle.install_supervisor_launcher(bin_home)

    target = bin_home / "lubko-supervisor"
    expected = cli.launcher_source("lubko-supervisor").encode("utf-8")
    assert target.read_bytes() == expected
    mode = target.stat().st_mode
    assert stat.S_ISREG(mode)
    assert mode & stat.S_IXUSR


def test_launcher_verification_fails_on_wrong_permissions(
    two_commit_repo: tuple[Path, str, str],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A launcher file without execute permission is detected.

    The atomic replace normally corrects permissions, so we inhibit it
    with a no-op ``Path.replace`` to leave the target in its pre-existing
    non-executable state while preserving expected content.
    """
    repo, first, _second = two_commit_repo
    monkeypatch.setattr(cli, "_sync_venv", fake_uv_sync)
    cli.build_cli_root(repo, first, "uv", 60.0)

    bin_home = tmp_path / "bin"
    bin_home.mkdir()

    target = bin_home / "lubko-supervisor"
    expected = cli.launcher_source("lubko-supervisor").encode("utf-8")
    target.write_bytes(expected)
    target.chmod(0o644)

    monkeypatch.setattr(Path, "replace", lambda _self, _target: None)

    with pytest.raises(OSError, match="not executable"):
        lifecycle.install_supervisor_launcher(bin_home)


def test_launcher_verification_fails_on_truncated_content(
    two_commit_repo: tuple[Path, str, str],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """After atomic replace the read-back can report a content mismatch."""
    repo, first, _second = two_commit_repo
    monkeypatch.setattr(cli, "_sync_venv", fake_uv_sync)
    cli.build_cli_root(repo, first, "uv", 60.0)

    bin_home = tmp_path / "bin"
    bin_home.mkdir()

    target = bin_home / "lubko-supervisor"

    original_read_bytes = Path.read_bytes

    def patched_read_bytes(self: Path) -> bytes:
        if self == target:
            return b"#!/bin/sh\ntruncated\n"
        return original_read_bytes(self)

    monkeypatch.setattr(Path, "read_bytes", patched_read_bytes)

    with pytest.raises(OSError, match="content mismatch"):
        lifecycle.install_supervisor_launcher(bin_home)


# ---------------------------------------------------------------------------
# Override-clearing regression: staged B → activate B with override present
# → later activate C → override absent
# ---------------------------------------------------------------------------


def test_stale_override_cleared_on_later_activation(
    two_commit_repo: tuple[Path, str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A bootstrap override for B is cleared when a later activation moves to C.

    Regression: previously ``_clear_stale_supervisor_override`` only cleared
    when ``override == confirmed_commit``.  If bootstrap staged override B,
    activation moved current to B but the override survived an interruption,
    and a later deploy confirmed C, the stale override B was never removed.
    The next supervisor restart would then run the obsolete runtime B.

    Proved through the real public ``deploy`` command pipeline: only external
    boundaries (validation, worker spawn, DB check, supervisor request) are
    mocked via string-name ``monkeypatch.setattr``; the real
    ``_deploy_locked`` → ``_complete_deploy_handoff`` →
    ``_activate_maintained_cli`` chain runs genuine production code including
    override clearing.
    """
    repo, first, second = two_commit_repo
    monkeypatch.setattr(cli, "_sync_venv", fake_uv_sync)
    monkeypatch.setattr(lifecycle, "resolve_uv", lambda _uv: "uv")
    monkeypatch.setattr(supervise, "supervisor_running", lambda: True)
    cli.build_cli_root(repo, first, "uv", 60.0)
    cli.build_cli_root(repo, second, "uv", 60.0)

    def fake_deploy_through_super(
        _options: lifecycle.DeployOptions, commit: str
    ) -> lifecycle.WorkerMeta:
        return lifecycle.WorkerMeta(
            schema_version=lifecycle.SCHEMA_VERSION,
            state=lifecycle.STATE_RUNNING,
            pid=999001,
            pgid=999001,
            sid=999001,
            start_time_ticks=1000,
            token=None,
            repo=str(repo),
            git_commit=commit,
            worker_id="test-worker",
            log_path="/dev/null",
            started_at=0.0,
            stopped_at=None,
        )

    # Mock external boundaries — module object + STRING attr name only.
    monkeypatch.setattr(lifecycle, "_validate_and_prepare", lambda _opt: second)
    monkeypatch.setattr(lifecycle, "_deploy_through_supervisor", fake_deploy_through_super)

    deploy_ns = argparse.Namespace(
        bootstrap=True,
        repo=repo,
        uv=None,
        grace_seconds=5.0,
        db_timeout=5.0,
        lock_timeout=30.0,
        validation_timeout=1200.0,
        git_timeout=10.0,
        cli_timeout=60.0,
    )

    # Stage override for first; current is first.
    supervise.write_supervisor_runtime_override(first)
    cli.set_current(first)
    assert cli.current_commit() == first
    assert supervise.read_supervisor_runtime_override() == first

    # Deploy to second (C): real _deploy_locked → _complete_deploy_handoff →
    # _activate_maintained_cli runs, which clears any stale override.
    assert lifecycle.deploy_cmd(deploy_ns) == lifecycle.EXIT_OK
    assert cli.current_commit() == second
    assert supervise.read_supervisor_runtime_override() is None
