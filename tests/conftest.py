"""Shared test fixtures isolating tests from ambient machine state."""

from __future__ import annotations

from itertools import count

import pytest

_STATE_HOME_IDS = count()


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
