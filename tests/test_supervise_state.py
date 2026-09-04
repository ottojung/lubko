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


def test_present_non_boolean_safety_bit_is_durable_hold(state_path: Path) -> None:
    """Malformed persisted safety bits parse closed and remain durable."""
    fields = ("ownership_hold_malformed", "unresolved_hold_malformed")
    for field in fields:
        for value in (None, "true", 1):
            assert supervise._strict_safety_hold({field: value}, field) is True

        write_raw_state(state_path, **{field: None})
        state = supervise.read_state()
        assert getattr(state, field) is True
        supervise.write_state(state)
        assert getattr(supervise.read_state(), field) is True


def test_boolean_safety_bit_values_are_preserved() -> None:
    """Actual JSON booleans retain their explicit safety-bit values."""
    for field in ("ownership_hold_malformed", "unresolved_hold_malformed"):
        for value in (False, True):
            assert supervise._strict_safety_hold({field: value}, field) is value


def test_valid_non_negative_generations_parse() -> None:
    """Genuine non-negative integer generations keep their value."""
    for generation in (0, 7, 10**12):
        assert supervise._parse_present_strict(
            {"applied_generation": generation},
            "applied_generation",
            supervise._strict_non_negative_int,
        ) == (generation, False)


def test_absent_generation_remains_fresh(state_path: Path) -> None:
    """A missing applied_generation stays a valid fresh state."""
    state_path.write_text(
        json.dumps({"schema_version": supervise.SCHEMA_VERSION}),
        encoding="utf-8",
    )
    state = supervise.read_state()
    assert state.applied_generation == 0
    assert state.ownership_hold_malformed is False


def test_present_malformed_generation_is_durable_hold(state_path: Path) -> None:
    """Malformed applied generations parse closed and the hold persists."""
    malformed: tuple[object, ...] = (True, False, "5", "", -1, 1.5, None, [5], {"n": 5}, "seven")
    for raw_generation in malformed:
        assert supervise._parse_present_strict(
            {"applied_generation": raw_generation},
            "applied_generation",
            supervise._strict_non_negative_int,
        ) == (0, True)

    write_raw_state(state_path, applied_generation="five")
    state = supervise.read_state()
    assert state.ownership_hold_malformed is True
    supervise.write_state(state)
    assert supervise.read_state().ownership_hold_malformed is True


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


def test_valid_non_negative_restart_counts_parse() -> None:
    """Genuine non-negative integer restart counts keep their value."""
    for count in (0, 1, 42, 10**12):
        assert supervise._parse_present_strict(
            {"restart_count": count},
            "restart_count",
            supervise._strict_non_negative_int,
        ) == (count, False)


def test_absent_restart_count_remains_fresh(state_path: Path) -> None:
    """A missing restart_count stays a valid zero crash history."""
    state_path.write_text(
        json.dumps({"schema_version": supervise.SCHEMA_VERSION}),
        encoding="utf-8",
    )
    state = supervise.read_state()
    assert state.restart_count == 0
    assert state.ownership_hold_malformed is False


def test_present_malformed_restart_count_is_durable_hold(state_path: Path) -> None:
    """Malformed restart counts parse closed and the hold persists."""
    malformed: tuple[object, ...] = (True, False, "5", "", -1, 1.5, None, [3], {"n": 3}, "three")
    for raw_count in malformed:
        assert supervise._parse_present_strict(
            {"restart_count": raw_count},
            "restart_count",
            supervise._strict_non_negative_int,
        ) == (0, True)

    write_raw_state(state_path, restart_count="three")
    state = supervise.read_state()
    assert state.ownership_hold_malformed is True
    supervise.write_state(state)
    assert supervise.read_state().ownership_hold_malformed is True


def test_valid_crash_history_fields_parse() -> None:
    """Genuine crash-history fields keep their values without any hold."""
    for count in (0, 5):
        for deadline in (0.0, 12345.75, 10**15):
            assert supervise._parse_present_strict(
                {"restart_count": count},
                "restart_count",
                supervise._strict_non_negative_int,
            ) == (count, False)
            assert supervise._parse_present_nullable_float(
                {"next_attempt_at": deadline}, "next_attempt_at"
            ) == (deadline, False)


def test_null_and_absent_deadlines_are_no_deadline() -> None:
    """Null and absent backoff deadlines stay valid no-deadline states."""
    assert supervise._parse_present_nullable_float({}, "next_attempt_at") == (None, False)
    assert supervise._parse_present_nullable_float(
        {"next_attempt_at": None}, "next_attempt_at"
    ) == (None, False)


def test_present_malformed_backoff_deadline_is_durable_hold(state_path: Path) -> None:
    """Malformed backoff deadlines parse closed and the hold persists."""
    malformed: tuple[object, ...] = (
        True,
        False,
        "later",
        "",
        [1],
        {"at": 1},
        float("nan"),
        float("inf"),
        float("-inf"),
    )
    for raw_deadline in malformed:
        assert supervise._parse_present_nullable_float(
            {"next_attempt_at": raw_deadline}, "next_attempt_at"
        ) == (None, True)

    write_raw_state(state_path, next_attempt_at="later")
    state = supervise.read_state()
    assert state.ownership_hold_malformed is True
    supervise.write_state(state)
    assert supervise.read_state().ownership_hold_malformed is True


def test_supervisor_monotonic_timestamps_are_strict_nullable_numbers(state_path: Path) -> None:
    """Lifecycle monotonic timestamps accept only non-negative finite JSON numbers or null."""
    malformed_values: tuple[object, ...] = (
        True,
        False,
        "1.5",
        "nan",
        "inf",
        [],
        {},
        float("nan"),
        float("inf"),
        float("-inf"),
        -0.001,
        -1,
    )
    for field in ("last_spawn_at", "next_readiness_at"):
        for numeric_value in (0, 1.25, 10**15):
            parsed, malformed = supervise._parse_present_nullable_monotonic_float(
                {field: numeric_value}, field
            )
            assert parsed == pytest.approx(float(numeric_value))
            assert malformed is False

        assert supervise._parse_present_nullable_monotonic_float({}, field) == (None, False)
        assert supervise._parse_present_nullable_monotonic_float({field: None}, field) == (
            None,
            False,
        )
        for malformed_value in malformed_values:
            assert supervise._parse_present_nullable_monotonic_float(
                {field: malformed_value}, field
            ) == (None, True)

        write_raw_state(state_path, **{field: "1.5"})
        state = supervise.read_state()
        assert getattr(state, field) is None
        assert state.ownership_hold_malformed is True
        supervise.write_state(state)
        assert supervise.read_state().ownership_hold_malformed is True


def test_reconcile_holds_before_malformed_monotonic_timing_can_drive_lifecycle(
    state_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Corrupt stability/readiness timing cannot reach scheduling decisions."""
    for field, value in (("last_spawn_at", "nan"), ("next_readiness_at", "inf")):
        write_raw_state(
            state_path,
            mode=supervise.MODE_RUN,
            commit=COMMIT,
            restart_count=1,
            child={
                "pid": 4242,
                "pgid": 4242,
                "sid": 4242,
                "start_time_ticks": 99,
                "token": "token-4242",
                "worker_id": "w",
                "spawned_at": 1.0,
            },
            **{field: value},
        )
        daemon = supervisor.SupervisorDaemon(supervisor.Settings())
        monkeypatch.setattr(
            daemon,
            "_maybe_reset_backoff",
            lambda _state, _now: pytest.fail("malformed stability timing reached backoff reset"),
        )
        monkeypatch.setattr(
            daemon,
            "_probe_readiness",
            lambda _now: pytest.fail("malformed readiness timing reached readiness scheduling"),
        )
        monkeypatch.setattr(
            daemon,
            "_ensure_worker",
            lambda _commit: pytest.fail("malformed timing authority reached worker lifecycle"),
        )

        daemon.reconcile(100.0)

        assert daemon._message is not None
        assert "malformed" in daemon._message
        assert supervise.read_state().ownership_hold_malformed is True


def test_unrepresentably_large_integer_deadline_is_durable_hold(state_path: Path) -> None:
    """An integer beyond float range fails closed instead of raising."""
    huge = "1" + "0" * 10000
    state_path.write_text(
        json.dumps({"schema_version": supervise.SCHEMA_VERSION})[:-1]
        + f', "next_attempt_at": {huge}}}',
        encoding="utf-8",
    )
    state = supervise.read_state()
    assert state.next_attempt_at is None
    assert state.ownership_hold_malformed is True

    supervise.write_state(state)
    assert supervise.read_state().ownership_hold_malformed is True


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


BOOT_A = "0aaaaaaa-0000-4000-8000-00000000000a"
BOOT_B = "0bbbbbbb-0000-4000-8000-00000000000b"


@pytest.mark.parametrize("deadline", [0.0, 12345.75])
def test_same_boot_keeps_active_backoff_deadline(
    state_path: Path, monkeypatch: pytest.MonkeyPatch, deadline: float
) -> None:
    """A matching boot identity proves the clock domain and keeps backoff."""
    write_raw_state(state_path, boot_id=BOOT_B, next_attempt_at=deadline)
    monkeypatch.setattr(supervise, "current_boot_id", lambda: BOOT_B)

    supervisor.normalize_cross_boot_state()

    state = supervise.read_state()
    assert state.boot_id == BOOT_B
    assert state.next_attempt_at == deadline
    assert state.ownership_hold_malformed is False


def test_genuine_cross_boot_mismatch_resets_monotonic_deadlines(
    state_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A proven prior-boot record has its monotonic deadlines reset."""
    write_raw_state(
        state_path,
        boot_id=BOOT_A,
        next_attempt_at=1000.0,
        last_spawn_at=888.25,
        next_readiness_at=777.125,
    )
    monkeypatch.setattr(supervise, "current_boot_id", lambda: BOOT_B)

    supervisor.normalize_cross_boot_state()

    state = supervise.read_state()
    assert state.boot_id == BOOT_B
    assert state.next_attempt_at is None
    assert state.last_spawn_at is None
    assert state.next_readiness_at is None
    assert state.ownership_hold_malformed is False


def test_absent_boot_identity_is_treated_as_prior_boot(
    state_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Genuine absence of the boot identity stays a resettable unknown."""
    write_raw_state(state_path, next_attempt_at=1000.0)
    monkeypatch.setattr(supervise, "current_boot_id", lambda: BOOT_B)

    supervisor.normalize_cross_boot_state()

    state = supervise.read_state()
    assert state.boot_id == BOOT_B
    assert state.next_attempt_at is None
    assert state.ownership_hold_malformed is False


def test_fresh_state_round_trip_has_no_boot_identity_key(state_path: Path) -> None:
    """Legacy unknown-boot state omits the key instead of writing null."""
    supervise.write_state(supervise.fresh_state())
    payload = json.loads(state_path.read_text(encoding="utf-8"))
    assert "boot_id" not in payload
    state = supervise.read_state()
    assert state.boot_id is None
    assert state.ownership_hold_malformed is False


def test_present_malformed_boot_identity_is_durable_hold(state_path: Path) -> None:
    """Malformed boot identifiers parse closed and the hold persists."""
    malformed: tuple[object, ...] = (1, True, 1.5, [], {}, ["x"], {"b": BOOT_A}, None, "")
    for raw_boot_id in malformed:
        assert supervise._parse_present_boot_identity({"boot_id": raw_boot_id}, "boot_id") == (
            None,
            True,
        )

    write_raw_state(state_path, boot_id="")
    state = supervise.read_state()
    assert state.ownership_hold_malformed is True
    supervise.write_state(state)
    assert supervise.read_state().ownership_hold_malformed is True


def test_explicit_null_boot_identity_preserves_active_backoff(
    state_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Explicit JSON null is corruption: it holds instead of resetting."""
    write_raw_state(state_path, boot_id=None, next_attempt_at=1000.0)
    monkeypatch.setattr(supervise, "current_boot_id", lambda: BOOT_B)

    supervisor.normalize_cross_boot_state()

    state = supervise.read_state()
    assert state.next_attempt_at is not None
    assert state.ownership_hold_malformed is True

    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    monkeypatch.setattr(
        daemon,
        "_ensure_worker",
        lambda _commit: pytest.fail("corrupted boot identity authorized spawn"),
    )
    daemon.reconcile(0.0)
    assert daemon._message is not None
    assert "malformed" in daemon._message


def test_corrupted_boot_identity_never_erases_active_backoff(
    state_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Representative corrupt boot identity holds instead of clearing backoff."""
    write_raw_state(state_path, boot_id="", next_attempt_at=1000.0)
    monkeypatch.setattr(supervise, "current_boot_id", lambda: BOOT_B)

    supervisor.normalize_cross_boot_state()

    state = supervise.read_state()
    assert state.next_attempt_at is not None
    assert state.ownership_hold_malformed is True

    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    monkeypatch.setattr(
        daemon,
        "_ensure_worker",
        lambda _commit: pytest.fail("corrupted boot identity authorized spawn"),
    )
    daemon.reconcile(0.0)
    assert daemon._message is not None
    assert "malformed" in daemon._message


@pytest.mark.parametrize("token", ["", "bad token", "a/b", ".", "..", "bad.token"])
def test_invalid_incarnation_tokens_make_persisted_worker_records_malformed(
    state_path: Path, token: str
) -> None:
    """Persisted lifecycle records share the canonical incarnation-token domain."""
    child = {
        "pid": 42,
        "pgid": 42,
        "sid": 42,
        "start_time_ticks": 7,
        "token": token,
        "worker_id": "worker",
        "spawned_at": 1.0,
    }
    write_raw_state(state_path, child=child)
    state = supervise.read_state()
    assert state.child is None
    assert state.ownership_hold_malformed is True

    with pytest.raises(ValueError, match="unresolved child hold is malformed"):
        supervise.UnresolvedChild.from_dict({
            "pid": 42,
            "start_time_ticks": 7,
            "token": token,
            "spawned_at": 1.0,
        })
    with pytest.raises(ValueError, match="spawning obligation is malformed"):
        supervise.SpawningObligation.from_dict({
            "token": token,
            "commit": COMMIT,
            "creator_pid": 11,
            "creator_start_time_ticks": 12,
            "pid": None,
            "start_time_ticks": None,
            "created_at": 1.0,
            "boot_id": None,
            "parent_death_signal": True,
        })


@pytest.mark.parametrize("field", ["mode", "intent"])
@pytest.mark.parametrize("raw", [123, True, 1.5, [], {}, None])
def test_present_malformed_state_enum_enters_durable_hold(
    field: str,
    raw: object,
) -> None:
    """Malformed present mode/intent never becomes healthy default authority."""
    data = supervise.fresh_state().to_dict()
    data[field] = raw

    state = supervise.SupervisorState.from_dict(data)

    assert state.ownership_hold_malformed is True


@pytest.mark.parametrize(
    ("field", "raw"),
    [
        ("mode", "unsupported"),
        ("intent", "unsupported"),
    ],
)
def test_unsupported_state_enum_enters_durable_hold(field: str, raw: object) -> None:
    """Unsupported mode/intent strings fail closed instead of becoming defaults."""
    data = supervise.fresh_state().to_dict()
    data[field] = raw

    state = supervise.SupervisorState.from_dict(data)

    assert state.ownership_hold_malformed is True


@pytest.mark.parametrize("raw", [123, True, 1.5, [], {}])
def test_present_malformed_commit_enters_durable_hold(raw: object) -> None:
    """Malformed present commit cannot silently degrade to ordinary absence."""
    data = supervise.fresh_state().to_dict()
    data["commit"] = raw

    state = supervise.SupervisorState.from_dict(data)

    assert state.commit is None
    assert state.ownership_hold_malformed is True


def test_state_string_absence_and_commit_null_remain_compatible() -> None:
    """Documented absence defaults and nullable commit retain healthy semantics."""
    data = supervise.fresh_state().to_dict()
    data.pop("mode")
    data.pop("intent")
    data["commit"] = None

    state = supervise.SupervisorState.from_dict(data)

    assert state.mode == supervise.MODE_IDLE
    assert state.intent == supervise.INTENT_RUN
    assert state.commit is None
    assert state.ownership_hold_malformed is False


def test_state_string_valid_values_round_trip() -> None:
    """Canonical lifecycle strings remain exact and healthy."""
    data = supervise.fresh_state().to_dict()
    data.update(
        mode=supervise.MODE_RUN,
        intent=supervise.INTENT_RETIRING,
        commit=COMMIT,
    )

    state = supervise.SupervisorState.from_dict(data)

    assert state.mode == supervise.MODE_RUN
    assert state.intent == supervise.INTENT_RETIRING
    assert state.commit == COMMIT
    assert state.ownership_hold_malformed is False
