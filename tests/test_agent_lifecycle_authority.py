"""Managed-agent lifecycle authority invariants."""

from __future__ import annotations

import time

import pytest

from lubko.agent import derive_state


def _running_meta(**overrides: object) -> dict[str, object]:
    meta: dict[str, object] = {
        "state": "running",
        "pid": None,
        "started_at": 100.0,
        "created_at": 90.0,
    }
    meta.update(overrides)
    return meta


def test_derive_state_uses_liveness_for_canonical_pid(monkeypatch: pytest.MonkeyPatch) -> None:
    """Canonical positive PIDs use strict liveness evidence."""
    meta = _running_meta(pid=123)
    monkeypatch.setattr("lubko.agent.is_alive", lambda value: value is meta)
    assert derive_state(meta) == "running"


def test_derive_state_allows_only_genuine_pid_absence_launch_grace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Only genuine PID absence receives bounded launch grace."""
    monkeypatch.setattr(time, "time", lambda: 120.0)
    assert derive_state(_running_meta(pid=None)) == "running"

    malformed_values: tuple[object, ...] = (False, 0, 0.0, "", [], {})
    for malformed in malformed_values:
        assert derive_state(_running_meta(pid=malformed)) == "unknown"


@pytest.mark.parametrize("field", ["started_at", "created_at"])
@pytest.mark.parametrize("malformed", [False, "", [], {}, float("inf"), float("nan")])
def test_derive_state_fails_closed_on_malformed_launch_timestamp(
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    malformed: object,
) -> None:
    """Malformed present launch timestamps fail closed."""
    monkeypatch.setattr(time, "time", lambda: 120.0)
    meta = _running_meta(pid=None)
    if field == "created_at":
        meta["started_at"] = None
    meta[field] = malformed
    assert derive_state(meta) == "unknown"
