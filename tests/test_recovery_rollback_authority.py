"""Recovery must preserve present rollback authority until it is proven inert."""

from __future__ import annotations

import json

import pytest

from lubko import deployctl, lifecycle
from lubko.state import rollback_state_path

_MISSING = object()


def _stopped_meta() -> dict[str, object]:
    return lifecycle.WorkerMeta(
        schema_version=lifecycle.SCHEMA_VERSION,
        state=lifecycle.STATE_STOPPED,
        pid=None,
        pgid=None,
        sid=None,
        start_time_ticks=None,
        token=None,
        repo="/repo",
        git_commit="a" * 40,
        worker_id=None,
        log_path="",
        started_at=None,
        stopped_at=1.0,
    ).to_dict()


def _running_meta(pid: int) -> dict[str, object]:
    return lifecycle.WorkerMeta(
        schema_version=lifecycle.SCHEMA_VERSION,
        state=lifecycle.STATE_RUNNING,
        pid=pid,
        pgid=pid,
        sid=pid,
        start_time_ticks=pid * 10,
        token=f"{pid:032x}",
        repo="/repo",
        git_commit="a" * 40,
        worker_id="worker",
        log_path="/worker.log",
        started_at=1.0,
        stopped_at=None,
    ).to_dict()


def _legacy_supervisor_placeholder(commit: str = "b" * 40) -> dict[str, object]:
    return {
        "schema_version": lifecycle.SCHEMA_VERSION,
        "state": lifecycle.STATE_RUNNING,
        "pid": 0,
        "pgid": 0,
        "sid": 0,
        "start_time_ticks": 0,
        "token": None,
        "repo": "/repo",
        "git_commit": commit,
        "worker_id": "",
        "log_path": "",
        "started_at": None,
        "stopped_at": None,
    }


def _legacy_supervised_rollback() -> dict[str, object]:
    return {
        "schema_version": 3,
        "generation": 7,
        "status": lifecycle.STATE_PENDING,
        "commit": "b" * 40,
        "previous_commit": "a" * 40,
        "deadline": 0.0,
        "repo": "/repo",
        "uv_path": "uv",
        "stop_grace_seconds": 1.0,
        "git_timeout_seconds": 1.0,
        "previous_retiring": False,
        "previous_meta": _stopped_meta(),
        "new_meta": _legacy_supervisor_placeholder(),
        "supervisor_owned": True,
    }


def _write(data: object) -> None:
    path = rollback_state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data), encoding="utf-8")


def test_historical_supervisor_placeholder_repair_is_safe_and_idempotent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The exact Lubko-written zero sentinel is non-process compatibility state."""
    _write(_legacy_supervised_rollback())
    observed: list[lifecycle.WorkerMeta] = []

    def not_alive(meta: lifecycle.WorkerMeta) -> bool:
        observed.append(meta)
        return False

    monkeypatch.setattr(lifecycle, "worker_alive", not_alive)

    lifecycle._repair_rollback_state(222)
    assert not rollback_state_path().exists()
    assert all(meta.pid != 0 for meta in observed)

    lifecycle._repair_rollback_state(222)
    assert not rollback_state_path().exists()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("supervisor_owned", False),
        ("commit", "c" * 40),
        ("repo", "/other"),
    ],
)
def test_historical_placeholder_requires_exact_supervisor_authority(
    field: str, value: object
) -> None:
    """A sentinel is compatible only inside its exact supervisor-owned envelope."""
    data = _legacy_supervised_rollback()
    data[field] = value
    _write(data)
    before = rollback_state_path().read_text(encoding="utf-8")

    with pytest.raises(lifecycle._AdoptionError, match="present but malformed"):
        lifecycle._repair_rollback_state(222)

    assert rollback_state_path().read_text(encoding="utf-8") == before


def test_near_miss_historical_placeholder_remains_untrusted() -> None:
    """Arbitrary malformed identity metadata cannot borrow legacy compatibility."""
    data = _legacy_supervised_rollback()
    candidate = _legacy_supervisor_placeholder()
    candidate["worker_id"] = "unexpected"
    data["new_meta"] = candidate
    _write(data)
    before = rollback_state_path().read_text(encoding="utf-8")

    with pytest.raises(lifecycle._AdoptionError, match="present but malformed"):
        lifecycle._repair_rollback_state(222)

    assert rollback_state_path().read_text(encoding="utf-8") == before


def test_historical_placeholder_does_not_override_live_previous_authority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Compatibility for candidate metadata never authorizes a different live worker."""
    data = _legacy_supervised_rollback()
    data["previous_meta"] = _running_meta(333)
    _write(data)
    monkeypatch.setattr(lifecycle, "worker_alive", lambda _meta: True)

    with pytest.raises(lifecycle._AdoptionError, match="previous worker pid 333"):
        lifecycle._repair_rollback_state(222)

    assert rollback_state_path().exists()


@pytest.mark.parametrize("contents", ["not-json", "[]"])
def test_malformed_rollback_document_blocks_repair(contents: str) -> None:
    """Present malformed rollback documents remain durable and block repair."""
    path = rollback_state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(contents, encoding="utf-8")

    with pytest.raises(lifecycle._AdoptionError, match="present but malformed"):
        lifecycle._repair_rollback_state(222)

    assert path.read_text(encoding="utf-8") == contents


@pytest.mark.parametrize("field", ["new_meta", "previous_meta"])
def test_malformed_nested_worker_authority_blocks_repair(field: str) -> None:
    """Malformed nested worker authority cannot be discarded during repair."""
    data: dict[str, object] = {
        "status": lifecycle.STATE_PENDING,
        "deadline": 0.0,
        "new_meta": _stopped_meta(),
        "previous_meta": _stopped_meta(),
    }
    data[field] = {"schema_version": "1"}
    _write(data)
    before = rollback_state_path().read_text(encoding="utf-8")

    with pytest.raises(lifecycle._AdoptionError, match="present but malformed"):
        lifecycle._repair_rollback_state(222)

    assert rollback_state_path().read_text(encoding="utf-8") == before


@pytest.mark.parametrize("field", ["new_meta", "previous_meta"])
def test_unsupported_nested_worker_state_blocks_repair(field: str) -> None:
    """Unsupported nested worker states remain durable malformed authority."""
    data: dict[str, object] = {
        "status": lifecycle.STATE_PENDING,
        "deadline": 0.0,
        "new_meta": _stopped_meta(),
        "previous_meta": _stopped_meta(),
    }
    malformed = _stopped_meta()
    malformed["state"] = "unknown"
    data[field] = malformed
    _write(data)
    before = rollback_state_path().read_text(encoding="utf-8")

    with pytest.raises(lifecycle._AdoptionError, match="present but malformed"):
        lifecycle._repair_rollback_state(222)

    assert rollback_state_path().read_text(encoding="utf-8") == before


@pytest.mark.parametrize(
    "deadline",
    [
        _MISSING,
        None,
        False,
        "1",
        -1,
        float("nan"),
        float("inf"),
        [],
        {},
    ],
)
def test_malformed_rollback_deadline_blocks_repair(deadline: object) -> None:
    """Malformed rollback deadlines remain durable and block repair."""
    data: dict[str, object] = {
        "status": lifecycle.STATE_PENDING,
        "new_meta": _stopped_meta(),
        "previous_meta": _stopped_meta(),
    }
    if deadline is not _MISSING:
        data["deadline"] = deadline
    _write(data)
    before = rollback_state_path().read_text(encoding="utf-8")

    with pytest.raises(lifecycle._AdoptionError, match="present but malformed"):
        lifecycle._repair_rollback_state(222)

    assert rollback_state_path().read_text(encoding="utf-8") == before


@pytest.mark.parametrize(
    "status",
    [_MISSING, None, False, 1, "", "unknown", [], {}],
)
def test_malformed_rollback_status_blocks_repair(status: object) -> None:
    """Malformed rollback lifecycle state remains durable and blocks repair."""
    data: dict[str, object] = {
        "deadline": 0.0,
        "new_meta": _stopped_meta(),
        "previous_meta": _stopped_meta(),
    }
    if status is not _MISSING:
        data["status"] = status
    _write(data)
    before = rollback_state_path().read_text(encoding="utf-8")

    with pytest.raises(lifecycle._AdoptionError, match="present but malformed"):
        lifecycle._repair_rollback_state(222)

    assert rollback_state_path().read_text(encoding="utf-8") == before


@pytest.mark.parametrize(
    "status",
    [lifecycle.STATE_PENDING, deployctl.STATUS_CONFIRMED, deployctl.STATUS_ROLLED_BACK],
)
def test_canonical_rollback_status_is_accepted(
    status: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Canonical rollback lifecycle states retain their cleanup semantics."""
    _write({
        "status": status,
        "deadline": 0.0,
        "new_meta": _stopped_meta(),
        "previous_meta": _stopped_meta(),
    })
    monkeypatch.setattr(lifecycle, "worker_alive", lambda _meta: False)
    lifecycle._repair_rollback_state(222)
    assert not rollback_state_path().exists()


def test_valid_inert_rollback_state_is_still_removed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A valid inert terminal rollback record remains eligible for cleanup."""
    _write({
        "status": deployctl.STATUS_ROLLED_BACK,
        "deadline": 0.0,
        "new_meta": _stopped_meta(),
        "previous_meta": _stopped_meta(),
    })
    monkeypatch.setattr(lifecycle, "worker_alive", lambda _meta: False)

    lifecycle._repair_rollback_state(222)

    assert not rollback_state_path().exists()


def test_absent_rollback_state_does_not_block_repair() -> None:
    """Genuine rollback-state absence remains non-blocking."""
    assert not rollback_state_path().exists()
    lifecycle._repair_rollback_state(222)
