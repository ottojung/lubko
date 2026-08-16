"""Regressions proving the whole test suite is hermetically isolated.

The validation suite must be safe to run from the same Unix user and container
as the live Lubko worker. These tests prove the default isolation is real:
state roots resolve under the current test's temporary directory, subprocesses
inherit the isolated root, the destructive lifecycle helpers fail closed when
state is not test-owned, and an ambient production-like state tree and live
worker are never mutated or signalled.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from lubko.lifecycle import read_meta
from lubko.state import state_root
from tests import _isolation as isolation


def _subprocess_state_home() -> str:
    """Return the XDG_STATE_HOME a plain inherited subprocess observes."""
    proc = subprocess.run(
        [sys.executable, "-c", "import os; print(os.environ.get('XDG_STATE_HOME', ''))"],
        capture_output=True,
        text=True,
        check=True,
    )
    return proc.stdout.strip()


def _subprocess_state_root() -> str:
    """Return the Lubko state root an inherited subprocess resolves."""
    proc = subprocess.run(
        [
            sys.executable,
            "-c",
            "from lubko.state import state_root; print(state_root())",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    return proc.stdout.strip()


def test_state_root_resolves_under_the_current_test_tmp(
    tmp_path: Path,
) -> None:
    """Lifecycle state resolves under the test-owned temporary root by default."""
    test_tmp = tmp_path.resolve()
    assert state_root().is_relative_to(test_tmp)
    raw = os.environ["XDG_STATE_HOME"]
    assert Path(raw).resolve().is_relative_to(test_tmp)
    assert test_tmp == isolation.CURRENT_TEST_TMP


def test_subprocesses_inherit_the_isolated_state_root() -> None:
    """Plain inherited subprocesses observe the same isolated XDG state root."""
    test_tmp = isolation.CURRENT_TEST_TMP
    assert test_tmp is not None
    expected_home = str(Path(os.environ["XDG_STATE_HOME"]).resolve())
    assert _subprocess_state_home() == expected_home
    resolved_root = Path(_subprocess_state_root()).resolve()
    assert resolved_root.is_relative_to(test_tmp.resolve())
    assert resolved_root == state_root().resolve()


def test_guard_fails_closed_when_state_home_is_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Without XDG_STATE_HOME the guard refuses instead of using live state."""
    monkeypatch.delenv("XDG_STATE_HOME", raising=False)
    with pytest.raises(AssertionError, match="unset"):
        isolation.assert_test_owned_state_root()


def test_guard_fails_closed_against_ambient_production_root(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A state root outside the current test's tmp dir is never accepted.

    This is the exact pre-fix failure mode: pointing the state root at a
    production-like tree (as the deployment E2E helpers did before isolation)
    must abort loudly rather than read, write, or signal it.
    """
    ambient = isolation.ambient_state_root().parent
    monkeypatch.setenv("XDG_STATE_HOME", str(ambient))
    with pytest.raises(AssertionError, match="not under the current test"):
        isolation.assert_test_owned_state_root()


def test_kill_recorded_workers_fails_closed_before_signal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The destructive helper refuses ambient metadata and never signals.

    The ambient production-like tree records the sentinel live worker under
    ``worker_id="test-worker"`` (the incident corruption signature). A helper
    bug that resolved state against that tree must raise before it can read or
    signal the recorded identity.
    """
    ambient = isolation.ambient_state_root().parent
    monkeypatch.setenv("XDG_STATE_HOME", str(ambient))
    with pytest.raises(AssertionError, match="not under the current test"):
        _kill_recorded_workers()
    assert isolation.ambient_sentinel_alive()
    tree = isolation.ambient_state_root()
    before = isolation.snapshot_tree(tree)
    with pytest.raises(AssertionError, match="not under the current test"):
        _kill_recorded_workers()
    assert isolation.snapshot_tree(tree) == before


def test_ambient_sentinel_survives_this_module() -> None:
    """The ambient live worker is still running after this module's tests."""
    assert isolation.ambient_sentinel_alive()


def _kill_recorded_workers() -> None:
    """Stand-in for the deployment helper: guard then record reads.

    Mirrors ``test_deployctl.kill_recorded_workers``: the ownership guard must
    run before any metadata is read or any recorded identity is signalled.
    """
    isolation.assert_test_owned_state_root()
    read_meta()
