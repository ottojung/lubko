"""Strict JSON-boolean handling of the ``restart`` flag in desired intents."""

from __future__ import annotations

import json
from dataclasses import replace
from types import SimpleNamespace
from typing import TYPE_CHECKING, cast

import pytest

from lubko import lifecycle_authority, supervise, supervisor
from tests._fake_authority_db import claim_every_daemon, seed_db_worker

if TYPE_CHECKING:
    import subprocess
    from collections.abc import Callable
    from pathlib import Path

COMMIT = "a" * 40
_DB_CLAIM_SERVER = "srv-desired-restart-test"


@pytest.fixture(autouse=True)
def _db_fencing_claim(monkeypatch: pytest.MonkeyPatch) -> None:
    """Establish a fake-database fencing claim on every daemon under test.

    Steady-state decisions require canonical database authority; local
    caches alone never authorize action.
    """
    claim_every_daemon(monkeypatch, supervisor, _DB_CLAIM_SERVER)


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


@pytest.mark.usefixtures("supervisor_token")
def test_missing_and_boolean_restart_values_parse() -> None:
    """Missing parses as false; only literal JSON booleans are accepted."""
    for restart, expected in [(None, False), (True, True), (False, False)]:
        payload = intent_payload() if restart is None else intent_payload(restart=restart)
        desired = supervise.SupervisorDesired.from_dict(payload)
        assert desired.restart is expected


@pytest.mark.usefixtures("supervisor_token")
def test_present_non_boolean_restart_fails_closed() -> None:
    """A present non-boolean ``restart`` (including null) enters malformed handling."""
    for malformed in [None, 1, 0, "true", "", {}, [], [True]]:  # type: ignore[var-annotated]
        with pytest.raises((TypeError, ValueError), match="malformed"):
            supervise.SupervisorDesired.from_dict(intent_payload(restart=malformed))


@pytest.mark.usefixtures("supervisor_token")
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
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    supervisor_token: str,  # ruff: ignore[unused-function-argument]
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


def _publish_live_worker(
    monkeypatch: pytest.MonkeyPatch, daemon: supervisor.SupervisorDaemon
) -> None:
    """Publish the canonical DB record and live direct-child view for the worker.

    Same-commit settlement keeps the worker only when the canonical row's
    WorkerRecord names the exact live direct child.

    Args:
        monkeypatch: The active monkeypatch fixture.
        daemon: The daemon under test.
    """
    seed_db_worker(
        daemon,
        lifecycle_authority.WorkerRecord(
            token=f"token-{4242}",
            commit=COMMIT,
            pid=4242,
            pgid=4242,
            sid=4242,
            start_time_ticks=99,
            worker_id="w",
        ),
    )
    daemon._active_child = supervise.WorkerChild(
        pid=4242,
        pgid=4242,
        sid=4242,
        start_time_ticks=99,
        token=f"token-{4242}",
        worker_id="w",
        spawned_at=1.0,
    )
    daemon.proc = cast("subprocess.Popen[bytes]", SimpleNamespace(pid=4242, poll=lambda: None))
    monkeypatch.setattr(supervisor, "proc_start_ticks", lambda pid: 99 if pid == 4242 else None)


@pytest.mark.usefixtures("supervisor_token")
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
@pytest.mark.usefixtures("supervisor_token")
def test_same_commit_settlement_advances_without_retirement(
    settled_state: Callable[[], supervise.SupervisorState],
    monkeypatch: pytest.MonkeyPatch,
    restart: object,
) -> None:
    """A valid non-restart intent records the generation and keeps the worker."""
    del settled_state
    _write_intent(intent_payload() if restart is None else intent_payload(restart=restart))
    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    _publish_live_worker(monkeypatch, daemon)
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


@pytest.mark.usefixtures("supervisor_token")
def test_request_run_preserves_malformed_desired_authority(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A malformed desired file cannot be erased by a new lifecycle request."""
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
    _write_intent(intent_payload(generation=100, restart="true"))
    before = supervise.desired_path().read_bytes()

    with pytest.raises(supervise.DesiredIntentError):
        supervise.request_run("b" * 40, repo="/workspace/new", uv_path="uv", worker_id="w")

    assert supervise.desired_path().read_bytes() == before

    supervise.desired_path().unlink()
    generation = supervise.request_run("b" * 40, repo="/workspace/new", uv_path="uv", worker_id="w")
    assert generation == 1
    desired = supervise.read_desired_strict()
    assert desired is not None
    assert desired.generation == 1


@pytest.mark.usefixtures("supervisor_token")
def test_request_run_keeps_valid_desired_generation_monotonic(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A valid pending desired generation still participates in ordering."""
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
    _write_intent(intent_payload(generation=41, restart=False))

    generation = supervise.request_run("b" * 40, repo="/workspace/new", uv_path="uv", worker_id="w")

    assert generation == 42
    desired = supervise.read_desired_strict()
    assert desired is not None
    assert desired.generation == 42
