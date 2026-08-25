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


def write_raw_state(state_path: Path, **fields: object) -> None:
    """Persist one raw state mapping."""
    payload = {"schema_version": supervise.SCHEMA_VERSION, **fields}
    state_path.write_text(json.dumps(payload), encoding="utf-8")


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
    write_raw_state(state_path, **{field: value})

    state = supervise.read_state()
    assert getattr(state, field) is True
    supervise.write_state(state)
    assert getattr(supervise.read_state(), field) is True


@pytest.mark.parametrize("field", ["ownership_hold_malformed", "unresolved_hold_malformed"])
@pytest.mark.parametrize("value", [False, True])
def test_boolean_safety_bit_values_are_preserved(
    state_path: Path, field: str, value: object
) -> None:
    """Actual JSON booleans retain their explicit safety-bit values."""
    write_raw_state(state_path, **{field: value})
    assert getattr(supervise.read_state(), field) is value


@pytest.mark.parametrize("generation", [0, 7, 10**12])
def test_valid_non_negative_generations_parse(state_path: Path, generation: int) -> None:
    """Genuine non-negative integer generations keep their value."""
    write_raw_state(state_path, applied_generation=generation)
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
    write_raw_state(state_path, applied_generation=raw_generation)
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


@pytest.mark.parametrize("count", [0, 1, 42, 10**12])
def test_valid_non_negative_restart_counts_parse(state_path: Path, count: int) -> None:
    """Genuine non-negative integer restart counts keep their value."""
    write_raw_state(state_path, restart_count=count)
    state = supervise.read_state()
    assert state.restart_count == count
    assert state.ownership_hold_malformed is False


def test_absent_restart_count_remains_fresh(state_path: Path) -> None:
    """A missing restart_count stays a valid zero crash history."""
    state_path.write_text(
        json.dumps({"schema_version": supervise.SCHEMA_VERSION}),
        encoding="utf-8",
    )
    state = supervise.read_state()
    assert state.restart_count == 0
    assert state.ownership_hold_malformed is False


@pytest.mark.parametrize(
    "raw_count",
    [True, False, "5", "", -1, 1.5, None, [3], {"n": 3}, "three"],
)
def test_present_malformed_restart_count_is_durable_hold(
    state_path: Path, raw_count: object
) -> None:
    """A present malformed restart count cannot degrade to absence."""
    write_raw_state(state_path, restart_count=raw_count)
    state = supervise.read_state()
    assert state.ownership_hold_malformed is True

    supervise.write_state(state)
    rewritten = supervise.read_state()
    assert rewritten.ownership_hold_malformed is True


@pytest.mark.parametrize("deadline", [0.0, 12345.75, 10**15])
@pytest.mark.parametrize("count", [0, 5])
def test_valid_crash_history_fields_parse(state_path: Path, count: int, deadline: float) -> None:
    """Genuine crash-history fields keep their values without any hold."""
    write_raw_state(state_path, restart_count=count, next_attempt_at=deadline)
    state = supervise.read_state()
    assert state.restart_count == count
    assert state.next_attempt_at == deadline
    assert state.ownership_hold_malformed is False


@pytest.mark.parametrize("raw_deadline", [None, "absent"])
def test_null_and_absent_deadlines_are_no_deadline(state_path: Path, raw_deadline: object) -> None:
    """Null and absent backoff deadlines stay valid no-deadline states."""
    if raw_deadline == "absent":
        write_raw_state(state_path)
    else:
        write_raw_state(state_path, next_attempt_at=None)
    state = supervise.read_state()
    assert state.next_attempt_at is None
    assert state.ownership_hold_malformed is False


@pytest.mark.parametrize(
    "raw_deadline",
    [
        True,
        False,
        "later",
        "",
        [1],
        {"at": 1},
        float("nan"),
        float("inf"),
        float("-inf"),
    ],
)
def test_present_malformed_backoff_deadline_is_durable_hold(
    state_path: Path, raw_deadline: object
) -> None:
    """A present malformed backoff deadline cannot clear an active backoff."""
    write_raw_state(state_path, next_attempt_at=raw_deadline)
    state = supervise.read_state()
    assert state.ownership_hold_malformed is True

    supervise.write_state(state)
    rewritten = supervise.read_state()
    assert rewritten.ownership_hold_malformed is True


def test_reconcile_holds_before_crash_handling_on_malformed_history(
    state_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Corrupt crash-history authority never retires or spawns a worker."""
    state_path.write_text(
        json.dumps({
            "schema_version": supervise.SCHEMA_VERSION,
            "mode": supervise.MODE_RUN,
            "commit": COMMIT,
            "restart_count": "many",
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
    monkeypatch.setattr(type(daemon), "_child_alive", staticmethod(lambda _state: False))
    monkeypatch.setattr(
        daemon,
        "_handle_crash",
        lambda _state, _now: pytest.fail("malformed crash history authorized handling"),
    )
    monkeypatch.setattr(
        daemon,
        "_ensure_worker",
        lambda _commit: pytest.fail("malformed crash history authorized spawn"),
    )

    daemon.reconcile(0.0)

    assert daemon._message is not None
    assert "malformed" in daemon._message
    rewritten = supervise.read_state()
    assert rewritten.ownership_hold_malformed is True
    assert rewritten.child is not None
    assert rewritten.child.pid == 4242
    assert rewritten.restart_count == 0


def test_reconcile_holds_before_worker_spawn_on_ownership_corruption(
    state_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Reconcile returns before any worker path for a corrupt ownership bit."""
    write_raw_state(state_path, ownership_hold_malformed="not-a-boolean")
    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    monkeypatch.setattr(
        daemon,
        "_ensure_worker",
        lambda _commit: pytest.fail("corrupt ownership state reached worker spawn path"),
    )

    daemon.reconcile(0.0)

    assert daemon._message is not None
    assert "malformed" in daemon._message
