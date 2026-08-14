"""Reproducible installation of the Lubko command line tools.

The ``lubko-install`` command installs the maintained ``lubko-agent``,
``lubko-worker``, and ``lubko-deploy`` entry points into the user's bin
directory (``$XDG_BIN_HOME`` or ``~/.local/bin``) using ``uv tool install``.
That directory is already on PATH for login and interactive shells, so the
maintained commands stay available everywhere without any hand-maintained
copies or ad-hoc shell aliases.
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Final

ENTRY_POINTS: Final = ("lubko-agent", "lubko-worker", "lubko-deploy")
PACKAGE_NAME: Final = "lubko"
UV_HTTP_TIMEOUT: Final = "30"
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


def tool_install(repo: Path, uv_path: str) -> int:
    """Install the Lubko tools with ``uv tool install``.

    Args:
        repo: Repository checkout to install from.
        uv_path: Path to the ``uv`` executable.

    Returns:
        The exit code of ``uv tool install``.
    """
    env = dict(os.environ)
    env.setdefault("UV_HTTP_TIMEOUT", UV_HTTP_TIMEOUT)
    proc = subprocess.run(
        [uv_path, "tool", "install", "--force", "--from", str(repo), PACKAGE_NAME],
        env=env,
        check=False,
    )
    return proc.returncode


def missing_entry_points() -> list[str]:
    """Return the maintained entry points absent from the bin directory.

    Returns:
        The names of entry points that are not installed.
    """
    return [entry for entry in ENTRY_POINTS if not (bin_home() / entry).is_file()]


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
        default="uv",
        help="uv executable (default: uv on PATH)",
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

    if not args.dry_run:
        uv_path = shutil.which(args.uv) or args.uv
        if shutil.which(uv_path) is None and not (
            Path(uv_path).is_file() and os.access(uv_path, os.X_OK)
        ):
            _err(f"uv executable not found: {args.uv}")
            return EXIT_ERROR
        code = tool_install(repo, uv_path)
        if code != 0:
            _err(f"uv tool install failed with exit code {code}")
            return EXIT_ERROR

    missing = missing_entry_points()
    if missing:
        _err("installed tools missing from the bin directory: " + ", ".join(missing))
        return EXIT_ERROR
    if not bin_home_on_path():
        _err(f"warning: {bin_home()} is not on the current PATH; log in again to pick it up")

    unresolved = [entry for entry in ENTRY_POINTS if shutil.which(entry) is None]
    if unresolved:
        _err("installed tools not found on PATH: " + ", ".join(unresolved))
        _err(f"ensure {bin_home()} is on PATH (login shells source ~/.profile)")
        return EXIT_ERROR

    _out("Lubko tools installed and resolvable on PATH:")
    for entry in ENTRY_POINTS:
        _out(f"  {entry} -> {shutil.which(entry)}")
    return EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main())
