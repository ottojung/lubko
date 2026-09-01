"""Regression coverage for intentional fresh-state supervisor holds."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from lubko import supervisor

if TYPE_CHECKING:
    from pathlib import Path

    import pytest


def test_bootstrap_hold_is_visible_and_not_relogged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Expose fresh bootstrap hold once without spawning or log spam."""
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state"))
    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    spawns: list[str] = []
    monkeypatch.setattr(daemon, "_ensure_worker", spawns.append)

    with caplog.at_level(logging.INFO, logger="lubko.supervisor"):
        daemon.reconcile(0.0)
        first_message = daemon._message
        daemon.reconcile(0.0)

    assert first_message == supervisor.BOOTSTRAP_HOLD_MESSAGE
    assert daemon._message == supervisor.BOOTSTRAP_HOLD_MESSAGE
    assert spawns == []
    assert caplog.messages.count(supervisor.BOOTSTRAP_HOLD_MESSAGE) == 1
