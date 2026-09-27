"""Runtime instantiability across a change of the maintained entry-point set.

The entry-point requirements that matter during an upgrade belong to the
process that has to launch the candidate runtime, not to the candidate's own
declared set. A predecessor whose requirement set is a strict superset of the
maintained one is therefore safe to upgrade from only when the candidate
runtime still provides the names the predecessor insists on; a candidate that
merely declares fewer entry points is not a smaller requirement, it is a
runtime the predecessor cannot launch.
"""

from __future__ import annotations

from importlib import metadata
from typing import TYPE_CHECKING, Final

from lubko import cli

if TYPE_CHECKING:
    from pathlib import Path

    import pytest

TARGET: Final = "b" * 40
PREDECESSOR_REQUIREMENTS: Final = frozenset(cli.ENTRY_POINTS) | frozenset(cli.RETIRED_ENTRY_POINTS)


def build_target_runtime(
    monkeypatch: pytest.MonkeyPatch, repo: Path, commit: str, mode: int = 0o755
) -> None:
    """Materialize one sealed runtime exposing only the maintained entry points.

    Args:
        monkeypatch: The active monkeypatch fixture.
        repo: Stand-in repository path; extraction is faked.
        commit: Exact commit hash to materialize.
        mode: Permission bits of the materialized entry-point scripts.
    """

    def fake_sync(_uv_path: str, root: Path, _timeout_seconds: float) -> None:
        """Create only the entry points the target's own tree declares."""
        bin_dir = root / ".venv" / "bin"
        bin_dir.mkdir(parents=True, exist_ok=True)
        for entry in cli.ENTRY_POINTS:
            script = bin_dir / entry
            script.write_text(f"#!/bin/sh\necho {entry}\n", encoding="utf-8")
            script.chmod(mode)

    monkeypatch.setattr(cli, "_sync_venv", fake_sync)
    monkeypatch.setattr(
        cli,
        "_extract_archive",
        lambda _repo, _commit, _destination, _timeout_seconds: None,
    )
    cli.build_cli_root(repo, commit, "uv", 60.0)


def test_retired_names_are_package_entry_points_for_predecessor_builders() -> None:
    """An old builder sees every retired name immediately after ``uv sync``.

    Predecessor deploy controllers run their own completeness check directly
    after syncing the target package, before any target-version post-processing
    can execute. The compatibility names therefore have to be package console
    scripts, not only bridges installed by the new builder.
    """
    distribution = metadata.distribution("lubko")
    scripts = {
        entry.name: entry.value
        for entry in distribution.entry_points
        if entry.group == "console_scripts"
    }
    for entry in cli.RETIRED_ENTRY_POINTS:
        assert scripts.get(entry) == "lubko.cli:retired_entry_point_main", (
            f"{entry} is not installed by target package metadata, so a supported "
            "predecessor builder will reject the runtime before bridge installation"
        )


def test_retired_package_entry_point_is_inert(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """The package compatibility entry point does not resurrect retired behavior."""
    monkeypatch.setattr("sys.argv", ["lubko-agent"])
    assert cli.retired_entry_point_main() == 127
    err = capsys.readouterr().err
    assert "removed from the maintained CLIs" in err
    assert "upgrade to this runtime" in err


def test_maintained_set_is_a_strict_subset_of_a_predecessor_requirement_set() -> None:
    """The maintained set is smaller, so it can never stand in for a predecessor's."""
    assert frozenset(cli.ENTRY_POINTS) < PREDECESSOR_REQUIREMENTS
    assert not cli.satisfies_entry_points(frozenset(cli.ENTRY_POINTS), PREDECESSOR_REQUIREMENTS), (
        "the target's declared set must not be treated as a predecessor's capability"
    )


def test_built_runtime_satisfies_a_predecessor_entry_point_superset(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A built runtime stays instantiable for a predecessor needing more names."""
    build_target_runtime(monkeypatch, tmp_path / "repo", TARGET)

    assert cli.runtime_is_usable(TARGET)
    assert cli.runtime_satisfies(TARGET, PREDECESSOR_REQUIREMENTS), (
        "a runtime that omits an entry point a predecessor still requires cannot be "
        "launched by that predecessor, so the upgrade must not be attempted"
    )


def test_bridged_retired_entry_point_runs_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A bridge keeps a retired name resolvable without resurrecting its command."""
    build_target_runtime(monkeypatch, tmp_path / "repo", TARGET)

    for entry in cli.RETIRED_ENTRY_POINTS:
        executable = cli.cli_entry_executable(TARGET, entry)
        assert executable is not None, f"{entry} is not bridged into the runtime"
        script = executable.read_text(encoding="utf-8")
        assert script.startswith("#!/bin/sh")
        assert "exit 127" in script, "a bridged entry point must refuse to run a command"
        assert entry not in cli.ENTRY_POINTS, "a retired name is never maintained again"


def test_runtime_capability_probe_agrees_with_entry_point_resolution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A runtime's reported entry-point set is exactly what it can resolve.

    The capability probe and the per-entry lookup answer the same question, so
    a runtime must never pass the usability gate while reporting no usable entry
    point: that combination makes the supervisor refuse every upgrade to the
    commit forever. Entry-point files that are not executable are still
    resolvable, and must therefore still be reported.
    """
    build_target_runtime(monkeypatch, tmp_path / "repo", TARGET, mode=0o644)

    reported = cli.root_entry_points(TARGET)
    resolved = frozenset(
        entry
        for entry in cli.known_entry_points()
        if cli.cli_entry_executable(TARGET, entry) is not None
    )
    assert reported == resolved == cli.known_entry_points()
    assert cli.runtime_is_usable(TARGET)
    assert cli.runtime_satisfies(TARGET, frozenset(cli.ENTRY_POINTS)), (
        "a runtime whose entries are all resolvable must stay instantiable"
    )
