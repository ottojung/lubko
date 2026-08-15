"""Reproducible installation of the Lubko command line tools.

The ``lubko-install`` command makes the maintained entry points
(``lubko-agent``, ``lubko-worker``, ``lubko-deploy``, ``lubko-deploy-ctl``,
``lubko-install``, and ``my-lubko-agent``) resolve on PATH to the code of one
exact Lubko checkout commit.

Instead of rewriting a ``uv`` tool environment in place, every global entry
point becomes a small stable launcher script in the user's bin directory
(``$XDG_BIN_HOME`` or ``~/.local/bin``), which already sits on PATH for login
and interactive shells. Each launcher resolves the single ``current`` symlink
under ``$XDG_STATE_HOME/lubko/cli`` and executes the matching entry point from
the immutable per-commit CLI environment. Deployments then keep the CLIs
coherent with the confirmed worker commit purely by switching that symlink,
never by rewriting the launchers. See :mod:`lubko.cli`.

The exact ``uv`` executable that successfully installs the tools is recorded
into the per-user Lubko state (``$XDG_STATE_HOME/lubko/toolchain.json``), so
later deployments keep working even when ``uv`` is no longer on PATH.
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
from pathlib import Path
from typing import Final

from lubko import cli
from lubko.toolchain import UvResolutionError, resolve_uv, write_toolchain

DEFAULT_GIT_TIMEOUT_SECONDS: Final = 10.0
EXIT_OK: Final = 0
EXIT_ERROR: Final = 1


def bin_home() -> Path:
    """Return the directory where user-installed executables live.

    Returns:
        ``$XDG_BIN_HOME`` when set, otherwise ``~/.local/bin``.
    """
    explicit = os.environ.get("XDG_BIN_HOME")
    return Path(explicit) if explicit else (Path.home() / ".local" / "bin")


def bin_home_on_path() -> bool:
    """Return whether the user bin directory is on PATH.

    Returns:
        ``True`` when the bin directory is present in the current PATH.
    """
    target = bin_home().resolve()
    entries = (entry for entry in os.environ.get("PATH", "").split(os.pathsep) if entry)
    return any(Path(entry).resolve() == target for entry in entries)


def missing_entry_points() -> list[str]:
    """Return the maintained entry points absent from the bin directory.

    Returns:
        The names of launchers that are not installed.
    """
    return [entry for entry in cli.ENTRY_POINTS if not (bin_home() / entry).is_file()]


def _out(message: str) -> None:
    """Write a user-facing line to standard output.

    Args:
        message: Message to write.
    """
    sys.stdout.write(message + "\n")


def _err(message: str) -> None:
    """Write a user-facing line to standard error.

    Args:
        message: Message to write.
    """
    sys.stderr.write(message + "\n")


def _verify_installed() -> int:
    """Verify the launchers and the active CLI environment resolve.

    Returns:
        ``EXIT_OK`` when every entry point resolves, ``EXIT_ERROR`` otherwise.
    """
    missing = missing_entry_points()
    if missing:
        _err("installed tools missing from the bin directory: " + ", ".join(missing))
        return EXIT_ERROR
    commit = cli.current_commit()
    if commit is None:
        _err("no maintained Lubko CLI commit is active; run lubko-install --repo <checkout>")
        return EXIT_ERROR
    stale = [entry for entry in cli.ENTRY_POINTS if cli.cli_entry_executable(commit, entry) is None]
    if stale:
        _err(f"active CLI commit {commit} is missing entry points: " + ", ".join(stale))
        return EXIT_ERROR
    return EXIT_OK


def build_parser() -> argparse.ArgumentParser:
    """Build the ``lubko-install`` command line parser.

    Returns:
        The configured parser.
    """
    parser = argparse.ArgumentParser(
        prog="lubko-install",
        description="Install the Lubko command line tools onto PATH reproducibly.",
    )
    parser.add_argument(
        "--repo",
        type=Path,
        default=Path.cwd(),
        help="Lubko repository checkout to install from (default: current directory)",
    )
    parser.add_argument(
        "--uv",
        default=None,
        help="uv executable (default: uv on PATH, then the recorded Lubko toolchain path)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="only verify the current installation state",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Install the Lubko tools and verify they resolve on PATH.

    Args:
        argv: Command line arguments, or ``None`` to use ``sys.argv``.

    Returns:
        A process exit code.
    """
    args = build_parser().parse_args(argv)
    repo = args.repo.resolve()
    if not (repo / "pyproject.toml").is_file():
        _err(f"not a Lubko repository checkout: {repo}")
        return EXIT_ERROR

    if args.dry_run:
        if _verify_installed() != EXIT_OK:
            return EXIT_ERROR
        _out("Lubko tools installed and resolvable on PATH:")
        _out_cli_resolution()
        return EXIT_OK

    return _install_repo(repo, args.uv)


def _install_repo(repo: Path, uv: str | None) -> int:
    """Install stable launchers and activate one repo commit.

    Args:
        repo: Repository checkout to install from.
        uv: Explicit ``--uv`` value, or ``None``.

    Returns:
        ``EXIT_OK`` on success, ``EXIT_ERROR`` otherwise.
    """
    try:
        uv_path = resolve_uv(uv)
    except UvResolutionError as exc:
        _err(str(exc))
        return EXIT_ERROR

    commit = cli.git_commit(repo, DEFAULT_GIT_TIMEOUT_SECONDS)
    if commit is None:
        _err(f"could not read the git commit of {repo}")
        return EXIT_ERROR

    try:
        cli.build_cli_root(repo, commit, uv_path, cli.DEFAULT_BUILD_TIMEOUT_SECONDS)
    except cli.CliError as exc:
        _err("could not prepare the maintained CLI environment: " + str(exc))
        return EXIT_ERROR

    cli.install_launchers(bin_home())
    try:
        cli.set_current(commit)
    except cli.CliError as exc:
        _err("could not activate the maintained CLI environment: " + str(exc))
        return EXIT_ERROR
    cli.gc_cli_roots((commit,))
    write_toolchain(uv_path)

    if _verify_installed() != EXIT_OK:
        return EXIT_ERROR
    if not bin_home_on_path():
        _err(f"warning: {bin_home()} is not on the current PATH; log in again to pick it up")

    _out("Lubko tools installed and resolvable on PATH:")
    _out_cli_resolution()
    return EXIT_OK


def _out_cli_resolution() -> None:
    """Print the resolved launcher path for every maintained entry point."""
    commit = cli.current_commit()
    active = commit or "unknown"
    _out(f"  active commit: {active}")
    for entry in cli.ENTRY_POINTS:
        _out(f"  {entry} -> {shutil.which(entry)}")


if __name__ == "__main__":
    raise SystemExit(main())
