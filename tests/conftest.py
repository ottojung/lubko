"""Shared test fixtures isolating tests from ambient machine state."""

from __future__ import annotations

import shutil
import tempfile
from collections.abc import Iterator
from itertools import count
from pathlib import Path

import pytest

_STATE_HOME_IDS = count()
_EXEC_TMP_ROOT = Path(__file__).resolve().parents[1] / ".pytest_cache" / "exec-tmp"


@pytest.fixture
def tmp_path() -> Iterator[Path]:
    """Provide a per-test temporary directory on the executable workspace filesystem.

    Some Lubko tests intentionally create and execute fake programs. Pytest's
    built-in ``tmp_path`` inherits the system temporary filesystem, which is
    mounted ``noexec`` in the production-like Lubko container. Keep the familiar
    fixture name while rooting it under the repository's ignored pytest cache so
    ordinary tests need no special setup and executable-fixture tests remain
    representative. ``mkdtemp`` gives parallel test runs disjoint directories.
    """
    _EXEC_TMP_ROOT.mkdir(parents=True, exist_ok=True)
    path = Path(tempfile.mkdtemp(prefix="test-", dir=_EXEC_TMP_ROOT))
    try:
        yield path
    finally:
        shutil.rmtree(path, ignore_errors=True)


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
