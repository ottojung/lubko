"""Process-identity matching and worker signalling invariants."""

import json
import os
import signal
from pathlib import Path

import pytest

from lubko import lifecycle, worker
from lubko.lifecycle import SCHEMA_VERSION, ProcessIdentity, WorkerMeta, identity_matches

LIVE = ProcessIdentity(pid=42, pgid=42, sid=7, start_time_ticks=1234)
PIN_AND_SIGNAL = worker._pin_and_signal


def meta(**overrides: object) -> WorkerMeta:
    """Return worker metadata matching :data:`LIVE` with fields overridden."""
    defaults: dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "state": "running",
        "pid": 42,
        "pgid": 42,
        "sid": 7,
        "start_time_ticks": 1234,
        "token": "t",
        "repo": "/repo",
        "git_commit": None,
        "worker_id": None,
        "log_path": "/log",
        "started_at": 1.0,
        "stopped_at": None,
    }
    return WorkerMeta.from_dict({**defaults, **overrides})


def test_metadata_round_trip() -> None:
    """Worker metadata survives a serialization round trip unchanged."""
    original = meta()
    assert WorkerMeta.from_dict(original.to_dict()) == original


def test_identity_match_requires_every_recorded_field() -> None:
    """Live identity must match every recorded field, defeating PID reuse."""
    assert identity_matches(meta(), LIVE)
    assert not identity_matches(meta(pid=None), LIVE)
    assert not identity_matches(meta(pid=43), LIVE)
    assert not identity_matches(meta(pgid=99), LIVE)
    assert not identity_matches(meta(sid=99), LIVE)
    assert not identity_matches(meta(start_time_ticks=None), LIVE)
    assert not identity_matches(meta(start_time_ticks=999), LIVE)
    recorded = meta(pgid=None, sid=None)
    assert identity_matches(recorded, LIVE)
    recycled = ProcessIdentity(pid=42, pgid=LIVE.pgid, sid=LIVE.sid, start_time_ticks=999)
    assert not identity_matches(recorded, recycled)


def test_pinned_signal_fails_closed_when_pin_binding_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Missing pidfd-open capability never falls back to an unpinned signal."""

    def missing(_pid: int) -> int:
        message = "pidfd_open unavailable"
        raise AttributeError(message)

    broad: list[tuple[int, int]] = []
    monkeypatch.setattr(worker, "_pidfd_open", missing)
    monkeypatch.setattr(os, "kill", lambda pid, sig: broad.append((pid, sig)))
    monkeypatch.setattr(os, "killpg", lambda pgid, sig: broad.append((pgid, sig)))

    assert not PIN_AND_SIGNAL(12345, signal.SIGTERM, 77)
    assert broad == []


def test_pinned_signal_rechecks_identity_before_delivery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A changed start identity after pinning prevents signal delivery."""
    delivered: list[tuple[int, int]] = []
    closed: list[int] = []

    def record_close(fd: int) -> None:
        closed.append(fd)

    monkeypatch.setattr(worker, "_pidfd_open", lambda _pid: 91)
    monkeypatch.setattr(worker, "proc_start_ticks", lambda _pid: 88)
    monkeypatch.setattr(
        worker, "_pidfd_send_signal", lambda pidfd, sig: delivered.append((pidfd, sig))
    )
    monkeypatch.setattr(os, "close", record_close)

    assert not PIN_AND_SIGNAL(12345, signal.SIGTERM, 77)
    assert delivered == []
    assert closed == [91]


def test_pinned_signal_fails_closed_when_send_binding_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Missing pidfd-send capability never falls back after identity proof."""

    def missing(_pidfd: int, _sig: int) -> None:
        message = "pidfd_send_signal unavailable"
        raise AttributeError(message)

    broad: list[tuple[int, int]] = []
    closed: list[int] = []

    def record_close(fd: int) -> None:
        closed.append(fd)

    monkeypatch.setattr(worker, "_pidfd_open", lambda _pid: 92)
    monkeypatch.setattr(worker, "proc_start_ticks", lambda _pid: 77)
    monkeypatch.setattr(worker, "_pidfd_send_signal", missing)
    monkeypatch.setattr(os, "close", record_close)
    monkeypatch.setattr(os, "kill", lambda pid, sig: broad.append((pid, sig)))
    monkeypatch.setattr(os, "killpg", lambda pgid, sig: broad.append((pgid, sig)))

    assert not PIN_AND_SIGNAL(12345, signal.SIGTERM, 77)
    assert broad == []
    assert closed == [92]


@pytest.mark.parametrize(
    ("field", "bad_value"),
    [
        ("schema_version", "1"),
        ("schema_version", 1.0),
        ("schema_version", True),
        ("schema_version", 2),
        ("pid", "42"),
        ("pid", 42.0),
        ("pid", True),
        ("pgid", "42"),
        ("sid", "7"),
        ("start_time_ticks", "1234"),
        ("started_at", "1.0"),
        ("started_at", True),
        ("started_at", float("nan")),
        ("stopped_at", float("inf")),
        ("state", 1),
        ("token", 1),
        ("repo", 1),
        ("git_commit", 1),
        ("worker_id", 1),
        ("log_path", 1),
    ],
)
def test_metadata_rejects_malformed_present_scalars(field: str, bad_value: object) -> None:
    """Present malformed scalars never become maintained-worker authority."""
    data = meta().to_dict()
    data[field] = bad_value
    with pytest.raises(
        (TypeError, ValueError), match=r"worker metadata|unsupported worker metadata"
    ):
        WorkerMeta.from_dict(data)


def test_metadata_requires_explicit_supported_schema() -> None:
    """Missing schema authority is not silently upgraded to the current schema."""
    data = meta().to_dict()
    del data["schema_version"]
    with pytest.raises(ValueError, match=r"schema_version.*missing"):
        WorkerMeta.from_dict(data)


def test_read_meta_fails_closed_on_malformed_identity(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Malformed on-disk identity cannot become usable lifecycle metadata."""
    path = tmp_path / "meta.json"
    data = meta().to_dict()
    data["pid"] = "42"
    path.write_text(json.dumps(data))
    monkeypatch.setattr(lifecycle, "meta_path", lambda: path)
    assert lifecycle.read_meta() is None
