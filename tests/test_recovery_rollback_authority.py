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


def _write(data: object) -> None:
    path = rollback_state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data), encoding="utf-8")


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
