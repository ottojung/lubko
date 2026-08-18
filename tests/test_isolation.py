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
from unittest.mock import patch

import pytest

from lubko.lifecycle import read_meta
from lubko.state import state_root
from tests import _isolation as isolation
from tests import _process_guard as guard


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
    assert test_tmp == isolation.RUNTIME.current_test_tmp


def test_subprocesses_inherit_the_isolated_state_root() -> None:
    """Plain inherited subprocesses observe the same isolated XDG state root."""
    test_tmp = isolation.RUNTIME.current_test_tmp
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


def test_teardown_generator_cleans_on_body_runtime_error(tmp_path: Path) -> None:
    """A RuntimeError in the body still tears down tracked processes and clears CURRENT_TEST_TMP.

    Drives the shared ``teardown_generator`` directly (not through pytest
    fixture machinery) to prove that the outer try/finally catches
    ``BaseException`` and deterministically cleans up before the error
    propagates.
    """
    proc = subprocess.Popen(
        ["/bin/sleep", "300"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    guard.register(proc)
    pid = proc.pid
    isolation.RUNTIME.current_test_tmp = tmp_path
    gen = isolation.teardown_generator()
    next(gen)
    caught = _throw_and_collect(gen, RuntimeError, "body failure")
    assert caught is not None
    assert "body failure" in str(caught)
    assert not guard.process_alive(pid)
    assert pid not in guard.TRACKED
    assert isolation.RUNTIME.current_test_tmp is None


def test_teardown_generator_cleans_on_body_keyboard_interrupt(tmp_path: Path) -> None:
    """A KeyboardInterrupt in the body still tears down and clears CURRENT_TEST_TMP.

    ``KeyboardInterrupt`` is a ``BaseException`` (not an ``Exception``), so
    the cleanup path must catch it.
    """
    proc = subprocess.Popen(
        ["/bin/sleep", "300"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    guard.register(proc)
    pid = proc.pid
    isolation.RUNTIME.current_test_tmp = tmp_path
    gen = isolation.teardown_generator()
    next(gen)
    caught = _throw_and_collect(gen, KeyboardInterrupt, "user interrupt")
    assert caught is not None
    assert not guard.process_alive(pid)
    assert pid not in guard.TRACKED
    assert isolation.RUNTIME.current_test_tmp is None


def _throw_and_collect(
    gen: object,
    exc_type: type[BaseException],
    msg: str,
) -> BaseException | None:
    """Throw an exception into a generator and collect any raised instance.

    Args:
        gen: The generator to throw into.
        exc_type: Exception type to throw.
        msg: Exception message.

    Returns:
        The caught exception, or ``None`` if ``StopIteration`` was raised.
    """
    caught: BaseException | None = None
    try:
        with patch.object(isolation, "ambient_sentinel_alive", return_value=True):
            try:
                gen.throw(exc_type, msg, None)  # type: ignore[attr-defined]
            except BaseException as exc:  # ruff: ignore[blind-except]
                caught = exc
    except StopIteration:
        pass
    return caught


def test_teardown_generator_chains_cleanup_error_with_body_error(
    tmp_path: Path,
) -> None:
    """When both body and cleanup raise, the body error is re-raised with cleanup as cause."""
    isolation.RUNTIME.current_test_tmp = tmp_path
    gen = isolation.teardown_generator()
    next(gen)
    with (
        patch.object(guard, "teardown_tracked", side_effect=RuntimeError("cleanup fail")),
        pytest.raises(RuntimeError, match="body fail") as exc_info,
    ):
        gen.throw(RuntimeError, "body fail", None)
    assert isinstance(exc_info.value.__cause__, RuntimeError)
    assert "cleanup fail" in str(exc_info.value.__cause__)
    assert isolation.RUNTIME.current_test_tmp is None


def test_teardown_generator_propagates_cleanup_only_failure(
    tmp_path: Path,
) -> None:
    """When the body succeeds but cleanup raises, the cleanup error propagates."""
    isolation.RUNTIME.current_test_tmp = tmp_path
    gen = isolation.teardown_generator()
    next(gen)
    with (
        patch.object(guard, "teardown_tracked", side_effect=RuntimeError("cleanup only")),
        pytest.raises(RuntimeError, match="cleanup only"),
    ):
        next(gen)
    assert isolation.RUNTIME.current_test_tmp is None


def _kill_recorded_workers() -> None:
    """Stand-in for the deployment helper: guard then record reads.

    Mirrors ``test_deployctl.kill_recorded_workers``: the ownership guard must
    run before any metadata is read or any recorded identity is signalled.
    """
    isolation.assert_test_owned_state_root()
    read_meta()
