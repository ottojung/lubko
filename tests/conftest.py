"""Shared test fixtures isolating tests from ambient machine state."""

from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _isolated_state_home(
    monkeypatch: pytest.MonkeyPatch, tmp_path_factory: pytest.TempPathFactory
) -> None:
    """Point Lubko's XDG state root at a private per-test directory.

    Tests must be independent of any ambient production lifecycle state
    (worker metadata, rollback missions, CLI pointers, and especially the
    deployment lock), so candidate validation can run the suite while it
    holds the real deployment lock.
    """
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path_factory.mktemp("xdg-state")))
