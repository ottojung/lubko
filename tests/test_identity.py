"""Process-identity matching and worker metadata serialization invariants."""

from lubko.lifecycle import SCHEMA_VERSION, ProcessIdentity, WorkerMeta, identity_matches

LIVE = ProcessIdentity(pid=42, pgid=42, sid=7, start_time_ticks=1234)


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
    # PID reuse protection: same pid, different start time.
    assert not identity_matches(meta(start_time_ticks=999), LIVE)
    recorded = meta(pgid=None, sid=None)
    assert identity_matches(recorded, LIVE)
    recycled = ProcessIdentity(pid=42, pgid=LIVE.pgid, sid=LIVE.sid, start_time_ticks=999)
    assert not identity_matches(recorded, recycled)
