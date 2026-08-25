"""Durable supervisor ownership-state invariants."""

import json
from pathlib import Path

import pytest

from lubko import supervise, supervisor

COMMIT = "a" * 40


@pytest.fixture
def state_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Use an isolated state root for each persistence test.

    Returns:
        The supervisor state path.
    """
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
    path = supervise.state_path()
    path.parent.mkdir(parents=True)
    return path


def test_genuine_child_absence_remains_idle(state_path: Path) -> None:
    """Missing state and an explicit null child remain valid absence."""
    assert supervise.read_state().ownership_hold_malformed is False
    state_path.write_text(
        json.dumps({"schema_version": supervise.SCHEMA_VERSION, "child": None}),
        encoding="utf-8",
    )
    state = supervise.read_state()
    assert state.child is None
    assert state.ownership_hold_malformed is False


@pytest.mark.parametrize(
    "raw",
    [
        "{not json",
        "[]",
        json.dumps({"schema_version": supervise.SCHEMA_VERSION + 1}),
        json.dumps({"schema_version": supervise.SCHEMA_VERSION, "child": {"pid": "unknown"}}),
    ],
)
def test_present_corrupt_authority_is_durable_hold(state_path: Path, raw: str) -> None:
    """Corrupt authority never becomes absence, including after a rewrite."""
    state_path.write_text(raw, encoding="utf-8")
    state = supervise.read_state()
    assert state.ownership_hold_malformed is True

    supervise.write_state(state)
    rewritten = supervise.read_state()
    assert rewritten.ownership_hold_malformed is True


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("ownership_hold_malformed", None),
        ("ownership_hold_malformed", "true"),
        ("ownership_hold_malformed", 1),
        ("unresolved_hold_malformed", None),
        ("unresolved_hold_malformed", "true"),
        ("unresolved_hold_malformed", 1),
    ],
)
def test_present_non_boolean_safety_bit_is_durable_hold(
    state_path: Path, field: str, value: object
) -> None:
    """A malformed persisted safety bit cannot erase its obligation."""
    state_path.write_text(
        json.dumps({"schema_version": supervise.SCHEMA_VERSION, field: value}),
        encoding="utf-8",
    )

    state = supervise.read_state()
    assert getattr(state, field) is True
    supervise.write_state(state)
    assert getattr(supervise.read_state(), field) is True


@pytest.mark.parametrize("generation", [0, 7, 10**12])
def test_valid_non_negative_generations_parse(state_path: Path, generation: int) -> None:
    """Genuine non-negative integer generations keep their value."""
    state_path.write_text(
        json.dumps({
            "schema_version": supervise.SCHEMA_VERSION,
            "applied_generation": generation,
        }),
        encoding="utf-8",
    )
    state = supervise.read_state()
    assert state.applied_generation == generation
    assert state.ownership_hold_malformed is False


def test_absent_generation_remains_fresh(state_path: Path) -> None:
    """A missing applied_generation stays a valid fresh state."""
    state_path.write_text(
        json.dumps({"schema_version": supervise.SCHEMA_VERSION}),
        encoding="utf-8",
    )
    state = supervise.read_state()
    assert state.applied_generation == 0
    assert state.ownership_hold_malformed is False


@pytest.mark.parametrize(
    "raw_generation",
    [True, False, "5", "", -1, 1.5, None, [5], {"n": 5}, "seven"],
)
def test_present_malformed_generation_is_durable_hold(
    state_path: Path, raw_generation: object
) -> None:
    """A present malformed applied generation cannot degrade to absence."""
    state_path.write_text(
        json.dumps({
            "schema_version": supervise.SCHEMA_VERSION,
            "applied_generation": raw_generation,
        }),
        encoding="utf-8",
    )
    state = supervise.read_state()
    assert state.ownership_hold_malformed is True

    supervise.write_state(state)
    rewritten = supervise.read_state()
    assert rewritten.ownership_hold_malformed is True


def test_reconcile_holds_before_retire_or_spawn_on_malformed_generation(
    state_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Corrupt applied-generation authority never retires or spawns a worker."""
    supervise.desired_path().parent.mkdir(parents=True, exist_ok=True)
    supervise.desired_path().write_text(
        json.dumps({
            "schema_version": supervise.SCHEMA_VERSION,
            "generation": 9,
            "commit": COMMIT,
            "repo": "/workspace/repo",
            "uv_path": "uv",
            "worker_id": None,
            "restart": True,
        }),
        encoding="utf-8",
    )
    state_path.write_text(
        json.dumps({
            "schema_version": supervise.SCHEMA_VERSION,
            "applied_generation": "five",
            "mode": supervise.MODE_RUN,
            "commit": COMMIT,
            "child": {
                "pid": 4242,
                "pgid": 4242,
                "sid": 4242,
                "start_time_ticks": 99,
                "token": "token-4242",
                "worker_id": "w",
                "spawned_at": 1.0,
            },
        }),
        encoding="utf-8",
    )
    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    monkeypatch.setattr(type(daemon), "_child_alive", staticmethod(lambda _state: True))
    monkeypatch.setattr(
        daemon,
        "_retire_child",
        lambda: pytest.fail("malformed applied generation authorized retirement"),
    )
    monkeypatch.setattr(
        daemon,
        "_ensure_worker",
        lambda _commit: pytest.fail("malformed applied generation authorized spawn"),
    )

    daemon.reconcile(0.0)

    assert daemon._message is not None
    assert "malformed" in daemon._message
    rewritten = supervise.read_state()
    assert rewritten.ownership_hold_malformed is True
    assert rewritten.child is not None
    assert rewritten.child.pid == 4242


@pytest.mark.parametrize("field", ["ownership_hold_malformed", "unresolved_hold_malformed"])
@pytest.mark.parametrize("value", [False, True])
def test_boolean_safety_bit_values_are_preserved(
    state_path: Path, field: str, value: object
) -> None:
    """Actual JSON booleans retain their explicit safety-bit values."""
    state_path.write_text(
        json.dumps({"schema_version": supervise.SCHEMA_VERSION, field: value}),
        encoding="utf-8",
    )

    state = supervise.read_state()
    assert getattr(state, field) is value


def test_reconcile_holds_before_worker_spawn_on_ownership_corruption(
    state_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Reconcile returns before any worker path for a corrupt ownership bit."""
    state_path.write_text(
        json.dumps({
            "schema_version": supervise.SCHEMA_VERSION,
            "ownership_hold_malformed": "not-a-boolean",
        }),
        encoding="utf-8",
    )
    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    monkeypatch.setattr(
        daemon,
        "_ensure_worker",
        lambda _commit: pytest.fail("corrupt ownership state reached worker spawn path"),
    )

    daemon.reconcile(0.0)

    assert daemon._message is not None
    assert "malformed" in daemon._message
