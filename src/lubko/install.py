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

from lubko import cli, deployctl, startup_contract, supervise
from lubko import lifecycle as _lifecycle
from lubko.durable import DurabilityError
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


def _version_change_refusal(commit: str) -> str | None:
    """Return why a version-changing install must be refused, if so.

    The supervisor daemon and its maintained worker are authoritative for the
    runtime they run: the durable desired intent, the applied state, and the
    supervisor-runtime override each name a commit that must stay startable
    and coherent with the global CLIs. Installing a different commit would
    switch ``cli/current`` away from the worker's commit and garbage-collect
    or strand the worker's runtime, silently diverging the worker from the
    CLIs and setting up a later outage (for example after a restart).

    Same-commit installs are always allowed: they repair or freshly rebuild
    the exact runtime the supervisor already maintains, which is idempotent
    and never diverges anything.

    The guard is evaluated under the deployment lock so a concurrent deploy
    cannot change the supervisor authority between this check and the CLI
    mutation (fail closed against the check-to-mutation TOCTOU window).

    Args:
        commit: Exact commit the installer wants to activate.

    Returns:
        A user-facing refusal message, or ``None`` when the install may
        proceed.
    """
    conflicting = cli.supervisor_authoritative_commits() - {commit}
    if not conflicting:
        return None
    listed = ", ".join(sorted(conflicting))
    return (
        f"refusing to install commit {commit}: the supervisor maintains a different "
        f"runtime ({listed}); switch the maintained worker with "
        "'lubko-deploy --repo <checkout>' instead, then re-run lubko-install"
    )


def _install_refusal(commit: str) -> str | None:
    """Return why an install must not mutate maintained authority, if so.

    Args:
        commit: Exact commit the installer wants to activate.

    Returns:
        A user-facing refusal message, or ``None`` when installation may proceed.
    """
    refusal = _version_change_refusal(commit)
    if refusal is not None:
        return refusal
    try:
        desired = supervise.read_desired_strict()
    except supervise.DesiredIntentError as exc:
        return "refusing to install with untrusted supervisor desired state: " + str(exc)
    try:
        mission = deployctl.read_rollback_state()
    except deployctl.DeployCtlError as exc:
        return "refusing to install with untrusted supervised mission state: " + str(exc)
    desired_generation = desired.generation if desired is not None else 0
    if mission is not None and mission.generation >= desired_generation:
        return (
            "refusing to install while active supervised deployment mission authority "
            f"exists at generation {mission.generation}"
        )
    if desired is not None and desired.migration and cli.current_commit() != commit:
        return (
            "refusing to activate a pending cold-migration commit before the supervisor "
            "has proven it ready and settled the maintained CLI pointer"
        )
    return None


def _activate_under_deploy_lock(repo: Path, commit: str, uv_path: str) -> int:
    """Build, activate, and garbage-collect under the deployment lock.

    The supervisor-authoritative divergence guard is re-evaluated inside the
    lock so a concurrent deploy cannot change desired/applied/override
    authority between the check and the CLI mutation (fail closed against
    the check-to-mutation TOCTOU window).

    Args:
        repo: Repository checkout to install from.
        commit: Exact commit to activate.
        uv_path: Resolved ``uv`` executable path.

    Returns:
        ``EXIT_OK`` on success, ``EXIT_ERROR`` otherwise.
    """
    try:
        with _lifecycle.deploy_lock(_lifecycle.DEFAULT_LOCK_TIMEOUT_SECONDS):
            return _activate_mutation_locked(repo, commit, uv_path)
    except _lifecycle.LockTimeoutError:
        _err(
            "timed out waiting for the deployment lock; a lifecycle mutation "
            "(lubko-deploy/restart/recover) may be in flight, refusing to mutate the "
            "maintained CLI runtimes concurrently"
        )
        return EXIT_ERROR


def _activate_mutation_locked(repo: Path, commit: str, uv_path: str) -> int:
    """Perform the guarded runtime and CLI mutation while holding the lock.

    Args:
        repo: Repository checkout to install from.
        commit: Exact commit to activate.
        uv_path: Resolved ``uv`` executable path.

    Returns:
        ``EXIT_OK`` on success, ``EXIT_ERROR`` otherwise.
    """
    refusal = _install_refusal(commit)
    if refusal is not None:
        _err(refusal)
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
    try:
        supervise.ensure_run_intent(
            commit,
            repo=str(repo),
            uv_path=uv_path,
            worker_id=None,
        )
    except (
        DurabilityError,
        supervise.DesiredAuthorityConflictError,
        supervise.DesiredIntentError,
    ) as exc:
        _err("could not establish supervisor desired state: " + str(exc))
        return EXIT_ERROR
    cli.gc_cli_roots((commit,))
    return EXIT_OK


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

    refusal = _install_refusal(commit)
    if refusal is not None:
        _err(refusal)
        return EXIT_ERROR

    if _activate_under_deploy_lock(repo, commit, uv_path) != EXIT_OK:
        return EXIT_ERROR
    write_toolchain(uv_path)

    install_error = startup_contract.install_and_validate_startup_definition(bin_home())
    if _verify_installed() != EXIT_OK or install_error is not None:
        if install_error is not None:
            _err(install_error)
        return EXIT_ERROR
    if not bin_home_on_path():
        _err(f"warning: {bin_home()} is not on the current PATH; log in again to pick it up")

    _out("Lubko tools installed and resolvable on PATH:")
    _out_cli_resolution()
    startup_contract.write_contract()
    _out(
        f"startup contract version {startup_contract.CONTRACT_SCHEMA_VERSION}, launcher, "
        f"and startup definition installed; the container must run "
        f"'{startup_contract.STARTUP_LAUNCHER_NAME}' (tini-static -- lubko-supervisor)"
    )
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
