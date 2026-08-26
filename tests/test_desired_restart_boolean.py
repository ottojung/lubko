"""Strict JSON-boolean handling of the ``restart`` flag in desired intents."""

from __future__ import annotations

import json
from dataclasses import replace
from typing import TYPE_CHECKING

import pytest

from lubko import supervise, supervisor

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

COMMIT = "a" * 40


def intent_payload(**overrides: object) -> dict[str, object]:
    """Return a minimal valid desired-intent payload."""
    payload: dict[str, object] = {
        "schema_version": supervise.SCHEMA_VERSION,
        "generation": 7,
        "commit": COMMIT,
        "repo": "/workspace/repo",
        "uv_path": "uv",
        "worker_id": None,
    }
    payload.update(overrides)
    return payload


@pytest.mark.parametrize(("restart", "expected"), [(None, False), (True, True), (False, False)])
def test_missing_and_boolean_restart_values_parse(restart: object, expected: object) -> None:
    """Missing parses as false; only literal JSON booleans are accepted."""
    payload = intent_payload() if restart is None else intent_payload(restart=restart)
    desired = supervise.SupervisorDesired.from_dict(payload)
    assert desired.restart is expected


@pytest.mark.parametrize("malformed", [None, 1, 0, "true", "", {}, [], [True]])
def test_present_non_boolean_restart_fails_closed(malformed: object) -> None:
    """A present non-boolean ``restart`` (including null) enters malformed handling."""
    with pytest.raises((TypeError, ValueError), match="malformed"):
        supervise.SupervisorDesired.from_dict(intent_payload(restart=malformed))


def test_present_null_restart_is_malformed_unlike_absent_restart() -> None:
    """Absent ``restart`` parses as false; an explicit null is corruption."""
    assert supervise.SupervisorDesired.from_dict(intent_payload()).restart is False
    with pytest.raises((TypeError, ValueError), match="malformed"):
        supervise.SupervisorDesired.from_dict(intent_payload(restart=None))


def _write_intent(raw: dict[str, object]) -> None:
    supervise.desired_path().parent.mkdir(parents=True, exist_ok=True)
    supervise.desired_path().write_text(json.dumps(raw), encoding="utf-8")


LIVE_CHILD = supervise.WorkerChild(
    pid=4242,
    pgid=4242,
    sid=4242,
    start_time_ticks=99,
    token=f"token-{4242}",
    worker_id="w",
    spawned_at=1.0,
)


@pytest.fixture
def settled_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Callable[[], supervise.SupervisorState]:
    """Isolate state and seed a durable live worker child at the same commit.

    Returns:
        A callable reading the durable supervisor state.
    """
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
    supervise.write_state(
        replace(
            supervise.fresh_state(),
            mode="run",
            intent="run",
            applied_generation=5,
            commit=COMMIT,
            child=LIVE_CHILD,
        )
    )
    return supervise.read_state


def test_malformed_restart_is_never_a_settlement(
    settled_state: Callable[[], supervise.SupervisorState],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A malformed ``restart`` cannot settle as restart=false at the live worker."""
    del settled_state
    _write_intent(intent_payload(restart="true"))
    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    monkeypatch.setattr(daemon, "_child_alive", lambda _state: True)
    retired: list[int] = []

    def confirmed_retirement() -> bool:
        """Record the hold retirement and clear the child like a real stop.

        Returns:
            Always ``True``: the stop is treated as confirmed.
        """
        child = supervise.read_state().child
        retired.append(child.pid if child is not None else 0)
        supervise.write_state(replace(supervise.read_state(), child=None))
        return True

    monkeypatch.setattr(daemon, "_retire_child", confirmed_retirement)
    monkeypatch.setattr(
        daemon,
        "_ensure_worker",
        lambda _commit: pytest.fail("malformed intent authorized a replacement worker"),
    )

    daemon.reconcile(0.0)

    with pytest.raises(supervise.DesiredIntentError):
        supervise.read_desired_strict()
    assert retired == [4242]
    assert supervise.read_state().applied_generation == 5


@pytest.mark.parametrize("restart", [None, False])
def test_same_commit_settlement_advances_without_retirement(
    settled_state: Callable[[], supervise.SupervisorState],
    monkeypatch: pytest.MonkeyPatch,
    restart: object,
) -> None:
    """A valid non-restart intent records the generation and keeps the worker."""
    del settled_state
    _write_intent(intent_payload() if restart is None else intent_payload(restart=restart))
    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    monkeypatch.setattr(daemon, "_child_alive", lambda _state: True)
    monkeypatch.setattr(
        daemon,
        "_retire_child",
        lambda: pytest.fail("same-commit settlement must not retire the live worker"),
    )
    monkeypatch.setattr(
        daemon,
        "_ensure_worker",
        lambda _commit: pytest.fail("same-commit settlement must not spawn a worker"),
    )

    daemon.reconcile(0.0)

    state = supervise.read_state()
    assert state.applied_generation == 7
    assert state.commit == COMMIT
    assert state.child == LIVE_CHILD
