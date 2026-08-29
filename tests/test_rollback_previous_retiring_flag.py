"""Strict durable parsing of the rollback ``previous_retiring`` flag."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest

from lubko import deployctl as dc
from lubko.state import rollback_state_path

if TYPE_CHECKING:
    from pathlib import Path

COMMIT = "a" * 40


def rollback_payload(**overrides: object) -> dict[str, object]:
    """Return a minimal valid rollback-state payload.

    Args:
        overrides: Payload keys to replace.

    Returns:
        A payload accepted by ``RollbackState.from_dict``.
    """
    meta = {
        "schema_version": 1,
        "state": "running",
        "pid": 100,
        "pgid": 100,
        "sid": 100,
        "start_time_ticks": 1000,
        "token": "t",
        "repo": "/workspace/repo",
        "git_commit": COMMIT,
        "worker_id": "w",
        "log_path": "/workspace/worker.log",
        "started_at": 1.0,
        "stopped_at": None,
    }
    payload: dict[str, object] = {
        "schema_version": 2,
        "generation": 1,
        "status": dc.STATUS_PENDING,
        "commit": "b" * 40,
        "previous_commit": COMMIT,
        "deadline": 1.0,
        "repo": "/workspace/repo",
        "uv_path": "uv",
        "stop_grace_seconds": 1.0,
        "git_timeout_seconds": 5.0,
        "previous_meta": dict(meta),
        "new_meta": dict(meta, pid=200),
    }
    payload.update(overrides)
    return payload


@pytest.mark.parametrize(
    ("value", "expected"),
    [(True, True), (False, False), (dc._ABSENT, False)],
)
def test_absent_and_boolean_previous_retiring_parse(*, value: object, expected: bool) -> None:
    """Absent parses as false; only literal JSON booleans are stored."""
    payload = (
        rollback_payload() if value is dc._ABSENT else rollback_payload(previous_retiring=value)
    )
    assert dc.RollbackState.from_dict(payload).previous_retiring is expected


@pytest.mark.parametrize("malformed", [None, 1, 0, "true", "", {}, [], [True]])
def test_present_non_boolean_previous_retiring_fails_closed(malformed: object) -> None:
    """A present non-boolean ``previous_retiring`` (including null) is malformed."""
    with pytest.raises(dc.DeployCtlError):
        dc.RollbackState.from_dict(rollback_payload(previous_retiring=malformed))


def test_present_null_previous_retiring_is_malformed_unlike_absent(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Absent parses as false from disk; corrupt null fails closed."""
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
    path = rollback_state_path()
    path.parent.mkdir(parents=True, exist_ok=True)

    path.write_text(json.dumps(rollback_payload()), encoding="utf-8")
    parsed = dc._read_state()
    assert parsed is not None
    assert parsed.previous_retiring is False

    path.write_text(json.dumps(rollback_payload(previous_retiring=None)), encoding="utf-8")
    with pytest.raises(dc.DeployCtlError):
        dc._read_state()


def test_malformed_previous_retiring_never_reuses_live_worker(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A corrupt durable flag cannot settle as false while the old worker lives."""
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
    path = rollback_state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = rollback_payload(previous_retiring="true")
    path.write_text(json.dumps(payload), encoding="utf-8")

    monkeypatch.setattr(
        dc, "worker_alive", lambda _meta: pytest.fail("corrupt state was reused as False")
    )
    monkeypatch.setattr(
        dc, "stop_worker", lambda _meta, _grace: pytest.fail("corrupt state reached stop path")
    )

    with pytest.raises(dc.DeployCtlError):
        dc._read_state()


@pytest.mark.parametrize(
    ("field", "malformed"),
    [
        ("schema_version", "2"),
        ("schema_version", 2.0),
        ("schema_version", True),
        ("status", [dc.STATUS_PENDING]),
        ("status", "unknown"),
        ("commit", 123),
        ("commit", "b" * 39),
        ("previous_commit", [COMMIT]),
        ("repo", ["/workspace/repo"]),
        ("uv_path", 123),
        ("deadline", "1.0"),
        ("deadline", True),
        ("deadline", float("nan")),
        ("stop_grace_seconds", "1.0"),
        ("stop_grace_seconds", float("inf")),
        ("git_timeout_seconds", False),
        ("git_timeout_seconds", "5.0"),
        ("challenge_hash", []),
        ("supervisor_owned", "true"),
    ],
)
def test_present_malformed_authority_scalar_fails_closed(field: str, malformed: object) -> None:
    """Present authority fields keep their exact JSON type and shape."""
    with pytest.raises(dc.DeployCtlError):
        dc.RollbackState.from_dict(rollback_payload(**{field: malformed}))


@pytest.mark.parametrize("field", ["deadline", "stop_grace_seconds", "git_timeout_seconds"])
@pytest.mark.parametrize("value", [1, 1.25])
def test_finite_json_numbers_remain_accepted(field: str, value: float) -> None:
    """Finite JSON integers and floats remain valid for durable numeric fields."""
    parsed = dc.RollbackState.from_dict(rollback_payload(**{field: value}))
    assert getattr(parsed, field) == pytest.approx(value)
