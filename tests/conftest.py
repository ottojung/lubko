"""Shared test fixtures isolating tests from ambient machine state."""

from __future__ import annotations

import os
import shutil
import tempfile
from itertools import count
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

pytest_plugins = ("tests._pytest_budget",)

if TYPE_CHECKING:
    from collections.abc import Iterator

_STATE_HOME_IDS = count()
_EXEC_TMP_ROOT = Path(__file__).resolve().parents[1] / ".pytest_cache" / "exec-tmp"
_TEST_PATH_IDS = count()


def _noop_fsync(_fd: int) -> None:
    """No-op replacement for ``os.fsync`` used by the session fixture."""


@pytest.fixture(scope="session", autouse=True)
def _noop_successful_fsync() -> Iterator[None]:
    """Replace successful ``os.fsync`` with a no-op for the entire session.

    Real disk flush latency is filesystem- and host-dependent; a successful
    ``fsync`` return does not prove crash persistence, so exercising the
    syscall adds variance without exercising a meaningful product invariant.
    The production durability control flow (write-temp, fsync-temp, replace,
    fsync-dir, serialization locks, fault-injection points) is fully
    preserved; only the kernel flush is elided.  Deterministic failure
    injectors fire *before* the ``os.fsync`` call, so durability boundary
    tests that inject ``DurabilityError`` at the file/replace/dir stages
    are unaffected.

    Yields:
        ``None`` while the no-op is active.
    """
    patcher = pytest.MonkeyPatch()
    patcher.setattr(os, "fsync", _noop_fsync)
    try:
        yield
    finally:
        patcher.undo()


@pytest.fixture(scope="session")
def _exec_session_root() -> Iterator[Path]:
    """Create one session-scoped root for executable test temporary directories.

    A single ``mkdtemp`` call creates the root; individual tests get cheap
    ``mkdir`` subdirectories inside it.  The entire tree is removed once at
    session teardown, replacing per-test ``rmtree`` calls.

    Yields:
        A session-scoped directory on the repository filesystem.
    """
    _EXEC_TMP_ROOT.mkdir(parents=True, exist_ok=True)
    root = Path(tempfile.mkdtemp(prefix="session-", dir=_EXEC_TMP_ROOT))
    try:
        yield root
    finally:
        shutil.rmtree(root, ignore_errors=True)


@pytest.fixture
def tmp_path(_exec_session_root: Path) -> Path:
    """Provide a per-test temporary directory on the executable workspace filesystem.

    Some Lubko tests intentionally create and execute fake programs. Pytest's
    built-in ``tmp_path`` inherits the system temporary filesystem, which is
    mounted ``noexec`` in the production-like Lubko container. Keep the familiar
    fixture name while rooting it under the repository's ignored pytest cache so
    ordinary tests need no special setup and executable-fixture tests remain
    representative.  Each test gets a cheap ``mkdir`` subdirectory under the
    session-scoped root; the recursive cleanup happens once at session end.

    Returns:
        A unique per-test directory on the repository filesystem.
    """
    path = _exec_session_root / f"test-{next(_TEST_PATH_IDS)}"
    path.mkdir()
    return path


@pytest.fixture(autouse=True)
def _isolated_state_home(
    monkeypatch: pytest.MonkeyPatch, tmp_path_factory: pytest.TempPathFactory
) -> None:
    """Point Lubko's XDG state root at a private lazy per-test path.

    Tests must be independent of any ambient production lifecycle state
    (worker metadata, rollback missions, CLI pointers, and especially the
    deployment lock), so candidate validation can run the suite while it
    holds the real deployment lock. The unique path is not created eagerly:
    tests that never touch Lubko state pay no filesystem-allocation cost.
    """
    state_home = tmp_path_factory.getbasetemp() / f"xdg-state-{next(_STATE_HOME_IDS)}"
    monkeypatch.setenv("XDG_STATE_HOME", str(state_home))
