"""Stable invariants for already-deployed operation under exhausted storage.

Exhausting the host's general-purpose persistent filesystem must not prevent
an already-deployed supervisor and worker from starting, supervising, and
carrying jobs: diagnostic publication degrades silently, restart validation
is read-only, and immutable startup artifacts are never rewritten outside an
explicit deployment transition.
"""

from __future__ import annotations

import errno
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest

from lubko import deployctl as dc
from lubko import lifecycle, supervise, supervisor, worker
from lubko import startup_contract as sc
from lubko import state as _state_mod
from lubko.config import DatabaseConfig

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path


def _no_space(*_args: object, **_kwargs: object) -> None:
    """Simulate exhausted persistent storage deterministically.

    Raises:
        OSError: Always, with ``ENOSPC``.
    """
    raise OSError(errno.ENOSPC, "No space left on device")


def _supervisor_daemon() -> supervisor.SupervisorDaemon:
    return supervisor.SupervisorDaemon(supervisor.Settings())


@pytest.mark.usefixtures("supervisor_token")
def test_status_snapshot_failure_is_dropped_without_affecting_supervision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed status write never fails, blocks, or alters a lifecycle decision."""
    daemon = _supervisor_daemon()
    daemon._message = "holding for an explicit run intent"
    daemon._next_db_check_at = float("inf")
    monkeypatch.setattr(supervisor, "read_state", lambda: supervise.SupervisorState.from_dict({}))
    monkeypatch.setattr(supervisor, "read_worker_health", lambda: None)
    monkeypatch.setattr(supervise, "read_supervisor_pid", lambda: None)
    monkeypatch.setattr(dc, "read_rollback_state", lambda: None)
    monkeypatch.setattr(supervisor, "write_status", _no_space)
    daemon._write_status()
    daemon._write_status()
    assert daemon._status_write_drops == 2
    assert daemon._message == "holding for an explicit run intent"


@pytest.mark.usefixtures("supervisor_token")
def test_status_publication_recovers_after_storage_returns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Dropped snapshots do not poison later publication."""
    daemon = _supervisor_daemon()
    daemon._next_db_check_at = float("inf")
    monkeypatch.setattr(supervisor, "read_state", lambda: supervise.SupervisorState.from_dict({}))
    monkeypatch.setattr(supervisor, "read_worker_health", lambda: None)
    monkeypatch.setattr(supervise, "read_supervisor_pid", lambda: None)
    monkeypatch.setattr(dc, "read_rollback_state", lambda: None)
    monkeypatch.setattr(supervisor, "write_status", _no_space)
    daemon._write_status()
    assert daemon._status_write_drops == 1
    published: list[supervise.SupervisorStatus] = []
    monkeypatch.setattr(supervisor, "write_status", published.append)
    daemon._write_status()
    assert len(published) == 1
    assert daemon._status_write_drops == 1


@pytest.mark.usefixtures("supervisor_token")
def test_restart_validation_writes_nothing_to_startup_artifacts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Validating an already-deployed restart performs no persistent writes."""
    monkeypatch.setattr(sc, "state_root", lambda: tmp_path)
    monkeypatch.setattr(dc, "state_root", lambda: tmp_path)
    monkeypatch.setattr(_state_mod, "state_root", lambda: tmp_path)
    monkeypatch.setattr(supervise, "state_root", lambda: tmp_path)
    for name in sc.CURRENT_CONTRACT.required_state_dirs:
        (tmp_path / name).mkdir(mode=0o700, exist_ok=True)
    bin_home = tmp_path / "bin"
    bin_home.mkdir(mode=0o700)
    sc.write_contract()
    sc.write_startup_definition()
    sc.write_startup_launcher(bin_home)
    assert sc.validate_startup_artifacts(bin_home) is None

    launcher = bin_home / sc.STARTUP_LAUNCHER_NAME
    before = {
        "launcher": (launcher.read_bytes(), launcher.stat().st_mtime_ns),
        "contract": (sc.contract_path().read_bytes(), sc.contract_path().stat().st_mtime_ns),
        "definition": (
            sc.startup_definition_path().read_bytes(),
            sc.startup_definition_path().stat().st_mtime_ns,
        ),
    }

    writes: list[str] = []

    def _record_write(name: str) -> Callable[..., None]:
        def _recorder(*_args: object, **_kwargs: object) -> None:
            writes.append(name)

        return _recorder

    monkeypatch.setattr(sc, "write_contract", _record_write("contract"))
    monkeypatch.setattr(sc, "write_startup_definition", _record_write("definition"))
    monkeypatch.setattr(sc, "write_startup_launcher", _record_write("launcher"))

    assert sc.validate_startup_artifacts(bin_home) is None
    assert sc.assess_recorded_contract().state == "current"
    assert sc.validate_startup_launcher(bin_home)
    assert sc.validate_startup_definition().ok

    daemon = _supervisor_daemon()
    monkeypatch.setattr(lifecycle, "_resolve_bin_home", lambda: bin_home)
    daemon._converge_startup_artifacts()

    assert writes == []
    assert launcher.read_bytes() == before["launcher"][0]
    assert launcher.stat().st_mtime_ns == before["launcher"][1]
    assert sc.contract_path().read_bytes() == before["contract"][0]
    assert sc.contract_path().stat().st_mtime_ns == before["contract"][1]
    assert sc.startup_definition_path().read_bytes() == before["definition"][0]
    assert sc.startup_definition_path().stat().st_mtime_ns == before["definition"][1]


def test_worker_health_failure_leaves_supervision_unaffected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed worker health snapshot never escapes into lifecycle logic."""
    settings = worker.Settings(
        worker_id="w-test",
        poll_interval_seconds=0.0,
        process_poll_interval_seconds=0.0,
        cancel_grace_seconds=1.0,
        server="srv-test",
    )
    database = DatabaseConfig(host="h", port=1, dbname="d", user="u", password=str(uuid4()))
    daemon = worker.Supervisor(settings, database)
    monkeypatch.setattr(worker, "write_worker_health", _no_space)
    daemon._publish_health(force=True)
    assert daemon.active == {}
    assert not daemon._stopping
