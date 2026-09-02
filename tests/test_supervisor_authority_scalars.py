"""Strict JSON scalar parsing for durable supervisor authority."""

from __future__ import annotations

import math

import pytest

from lubko import supervise

COMMIT = "a" * 40
PROCESS_TOKEN = f"worker-token-{4242}"


def desired_payload(**overrides: object) -> dict[str, object]:
    """Return a valid desired-intent payload with optional overrides."""
    payload = supervise.SupervisorDesired(
        schema_version=supervise.SCHEMA_VERSION,
        generation=7,
        commit=COMMIT,
        repo="/workspace/repo",
        uv_path="uv",
        worker_id="worker",
        requested_at=1.5,
    ).to_dict()
    payload.update(overrides)
    return payload


def child_payload(**overrides: object) -> dict[str, object]:
    """Return a valid durable worker-child identity mapping."""
    payload: dict[str, object] = {
        "pid": 4242,
        "pgid": 4242,
        "sid": 4242,
        "start_time_ticks": 99,
        "token": PROCESS_TOKEN,
        "worker_id": "worker",
        "spawned_at": 2.5,
    }
    payload.update(overrides)
    return payload


def unresolved_payload(**overrides: object) -> dict[str, object]:
    """Return a valid unresolved-child hold mapping."""
    payload = supervise.UnresolvedChild(
        pid=4242,
        start_time_ticks=99,
        token=PROCESS_TOKEN,
        spawned_at=2.5,
    ).to_dict()
    payload.update(overrides)
    return payload


def spawning_payload(**overrides: object) -> dict[str, object]:
    """Return a valid pre-spawn obligation mapping."""
    payload = supervise.SpawningObligation(
        token=PROCESS_TOKEN,
        commit=COMMIT,
        creator_pid=4000,
        creator_start_time_ticks=88,
        pid=4242,
        start_time_ticks=99,
        created_at=2.5,
        boot_id="boot",
    ).to_dict()
    payload.update(overrides)
    return payload


def test_current_and_legacy_durable_authority_round_trip() -> None:
    """Current records round-trip while genuine legacy path absence stays supported."""
    desired = supervise.SupervisorDesired.from_dict(desired_payload())
    assert desired.to_dict() == desired_payload()
    legacy = desired_payload()
    del legacy["repo"]
    del legacy["uv_path"]
    parsed_legacy = supervise.SupervisorDesired.from_dict(legacy)
    assert not parsed_legacy.repo
    assert not parsed_legacy.uv_path

    child, malformed = supervise._parse_optional_child({"child": child_payload()})
    assert not malformed
    assert child is not None
    assert supervise._child_to_dict(child) == child_payload()
    assert (
        supervise.UnresolvedChild.from_dict(unresolved_payload()).to_dict() == unresolved_payload()
    )
    assert (
        supervise.SpawningObligation.from_dict(spawning_payload()).to_dict() == spawning_payload()
    )


def test_zero_supervisor_wall_clock_values_are_valid() -> None:
    """Epoch-zero compatibility remains valid for supervisor wall-clock metadata."""
    assert supervise.SupervisorDesired.from_dict(
        desired_payload(requested_at=0)
    ).requested_at == pytest.approx(0.0)
    child, malformed = supervise._parse_optional_child({"child": child_payload(spawned_at=0)})
    assert not malformed
    assert child is not None
    assert child.spawned_at == pytest.approx(0.0)
    assert supervise.UnresolvedChild.from_dict(
        unresolved_payload(spawned_at=0)
    ).spawned_at == pytest.approx(0.0)
    assert supervise.SpawningObligation.from_dict(
        spawning_payload(created_at=0)
    ).created_at == pytest.approx(0.0)


def test_malformed_desired_scalars_fail_closed() -> None:
    """Desired lifecycle authority rejects coercible malformed JSON scalars."""
    malformed_fields: tuple[tuple[str, object], ...] = (
        ("schema_version", "1"),
        ("schema_version", True),
        ("generation", "7"),
        ("generation", 7.0),
        ("generation", True),
        ("commit", []),
        ("repo", ["repo"]),
        ("uv_path", 123),
        ("worker_id", {}),
        ("requested_at", "1.5"),
        ("requested_at", True),
        ("requested_at", math.inf),
        ("requested_at", -math.inf),
        ("requested_at", math.nan),
        ("requested_at", -0.5),
    )
    for field, malformed in malformed_fields:
        with pytest.raises((TypeError, ValueError), match="malformed"):
            supervise.SupervisorDesired.from_dict(desired_payload(**{field: malformed}))
    assert supervise.SupervisorDesired.from_dict(
        desired_payload(requested_at=2)
    ).requested_at == pytest.approx(2.0)
    assert supervise.SupervisorDesired.from_dict(
        desired_payload(requested_at=0)
    ).requested_at == pytest.approx(0.0)


def test_malformed_child_scalars_preserve_ownership_hold() -> None:
    """Malformed maintained-child identity stays replacement-blocking."""
    for field, malformed in (
        ("pid", "4242"),
        ("pid", -1),
        ("pgid", 4242.0),
        ("sid", True),
        ("start_time_ticks", "99"),
        ("spawned_at", "2.5"),
        ("spawned_at", -0.5),
    ):
        state = supervise.SupervisorState.from_dict({"child": child_payload(**{field: malformed})})
        assert state.child is None
        assert state.ownership_hold_malformed


def test_malformed_unresolved_scalars_preserve_hold() -> None:
    """Malformed unresolved-child records stay durably replacement-blocking."""
    for field, malformed in (
        ("pid", "4242"),
        ("pid", -1),
        ("start_time_ticks", 99.0),
        ("spawned_at", math.inf),
        ("spawned_at", -0.5),
    ):
        state = supervise.SupervisorState.from_dict({
            "unresolved_child": unresolved_payload(**{field: malformed})
        })
        assert state.unresolved_child is None
        assert state.unresolved_hold_malformed


def test_malformed_spawning_scalars_preserve_hold() -> None:
    """Malformed pre-spawn records stay durably replacement-blocking."""
    for field, malformed in (
        ("creator_pid", "4000"),
        ("creator_pid", -1),
        ("creator_start_time_ticks", 88.0),
        ("pid", True),
        ("start_time_ticks", "99"),
        ("commit", 123),
        ("created_at", math.nan),
        ("created_at", -0.5),
        ("boot_id", []),
    ):
        state = supervise.SupervisorState.from_dict({
            "spawning": spawning_payload(**{field: malformed})
        })
        assert state.spawning is None
        assert state.spawning_hold_malformed
