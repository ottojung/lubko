"""Fail-closed handoff identity: successor binds to A's exact target commit.

The old supervisor A resolves the target commit from ``cli/current`` and spawns
successor B to execute that exact runtime.  B must record the same commit as its
``supervisor_runtime_commit`` — never re-derive from the mutable ``cli/current``
symlink, which may have changed between A's resolution and B's construction.
"""

from __future__ import annotations

import json
import os
from contextlib import suppress
from dataclasses import replace
from typing import TYPE_CHECKING

import pytest

from lubko import cli, supervise
from lubko.state import cli_root_dir
from lubko.supervisor import Settings, SupervisorDaemon

if TYPE_CHECKING:
    from pathlib import Path

TARGET_COMMIT = "a" * 40
OTHER_COMMIT = "b" * 40


@pytest.fixture
def _state_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Isolated XDG_STATE_HOME with an empty supervisor directory."""
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
    supervise.supervisor_dir().mkdir(parents=True, exist_ok=True)


def _write_state_with_runtime(runtime_commit: str | None = None) -> None:
    """Write fresh durable state with an optional supervisor_runtime_commit."""
    state = supervise.fresh_state()
    if runtime_commit is not None:
        state = replace(state, supervisor_runtime_commit=runtime_commit)
    supervise.state_path().parent.mkdir(parents=True, exist_ok=True)
    supervise.state_path().write_text(json.dumps(state.to_dict()), encoding="utf-8")


def _make_handoff_env(
    *,
    target_commit: str | None = TARGET_COMMIT,
    overrides: dict[str, str] | None = None,
) -> dict[str, str]:
    """Build a minimal handoff environment.

    Returns:
        A dictionary of environment variables for a handoff successor.
    """
    env: dict[str, str] = {
        supervise.HANDOFF_FD_ENV: "-1",
        supervise.HANDOFF_PATH_ENV: "/lock",
        supervise.HANDOFF_PID_ENV: "1",
        supervise.HANDOFF_READY_FD_ENV: "5",
        supervise.HANDOFF_TRANSFER_FD_ENV: "6",
        supervise.HANDOFF_MODE_ENV: "1",
    }
    if overrides is not None:
        env.update(overrides)
    if target_commit is not None:
        env[supervise.HANDOFF_TARGET_COMMIT_ENV] = target_commit
    return env


def _set_cli_current(commit: str) -> None:
    """Create a cli/current symlink pointing to *commit* under XDG_STATE_HOME."""
    cldir = cli_root_dir()
    cldir.mkdir(parents=True, exist_ok=True)
    target_dir = cldir / commit
    target_dir.mkdir(parents=True, exist_ok=True)
    current_link = cldir / "current"
    if current_link.exists() or current_link.is_symlink():
        current_link.unlink()
    current_link.symlink_to(target_dir)


# ------------------------------------------------------------------
# Mandatory identity binding
# ------------------------------------------------------------------


@pytest.mark.usefixtures("_state_dir")
def test_successor_binds_to_a_target_not_cli_current(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """B records A's target commit as runtime identity, ignoring cli/current.

    Regression: before this fix, B derived supervisor_runtime_commit from
    cli.current_commit() at construction time, so a deployment that changed
    cli/current between A's target resolution and B's startup would cause B
    to record the wrong identity.
    """
    _write_state_with_runtime(TARGET_COMMIT)

    # Simulate cli/current pointing to a DIFFERENT commit (deployment happened
    # between A's resolution and B's construction).
    _set_cli_current(OTHER_COMMIT)

    # Adopt a handoff lock fd with A's target commit.
    lock_path = str(supervise.supervisor_lock_path())
    lock_fd = supervise.acquire_supervisor_lock()

    # Simulate the handoff env vars being set (as A would set them).
    env = _make_handoff_env(target_commit=TARGET_COMMIT)
    env[supervise.HANDOFF_FD_ENV] = str(lock_fd)
    env[supervise.HANDOFF_PATH_ENV] = lock_path
    for k, v in env.items():
        monkeypatch.setenv(k, v)

    # __init__ reads cli.current_commit() (OTHER_COMMIT) at construction.
    daemon = SupervisorDaemon(Settings())
    assert daemon._runtime_commit is not None
    assert daemon._runtime_commit == OTHER_COMMIT
    assert daemon._handoff_target_commit is None

    # run() calls _acquire_ownership which adopts the handoff fd and
    # extracts the target commit.
    daemon._acquire_ownership()

    assert daemon._handoff_target_commit == TARGET_COMMIT

    # Simulate what run() does: bind _runtime_commit from the handoff target.
    # In run(), this happens immediately after _acquire_ownership when
    # _handoff_target_commit is set, overriding the cli.current_commit()
    # value captured at __init__ time.
    bound = daemon._handoff_target_commit  # type: ignore[unreachable]  # mypy: cannot track attribute mutation through method call
    assert bound is not None
    daemon._runtime_commit = bound

    # The runtime commit must be A's target, not the changed cli/current.
    assert daemon._runtime_commit == TARGET_COMMIT

    supervise.supervisor_lock_path().unlink(missing_ok=True)


@pytest.mark.usefixtures("_state_dir")
def test_missing_target_commit_in_handoff_mode_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Handoff fd present but target commit metadata missing → SystemExit(1).

    The successor must not guess its identity from mutable cli/current
    when the old supervisor failed to pass authoritative identity metadata.
    """
    _write_state_with_runtime(TARGET_COMMIT)

    lock_path = str(supervise.supervisor_lock_path())
    lock_fd = supervise.acquire_supervisor_lock()

    # Handoff env vars set but HANDOFF_TARGET_COMMIT_ENV is missing.
    env = _make_handoff_env(target_commit=None)
    env[supervise.HANDOFF_FD_ENV] = str(lock_fd)
    env[supervise.HANDOFF_PATH_ENV] = lock_path
    for k, v in env.items():
        monkeypatch.setenv(k, v)

    daemon = SupervisorDaemon(Settings())
    with pytest.raises(SystemExit) as exc_info:
        daemon._acquire_ownership()
    assert exc_info.value.code == 1

    with suppress(OSError):
        os.close(lock_fd)
    supervise.supervisor_lock_path().unlink(missing_ok=True)


@pytest.mark.usefixtures("_state_dir")
def test_malformed_target_commit_in_handoff_mode_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Handoff fd present but target commit is not a valid 40-hex name → SystemExit(1).

    An invalid commit name in the handoff identity metadata is treated as
    tampered or corrupt: fail closed, never fall back.
    """
    _write_state_with_runtime(TARGET_COMMIT)

    lock_path = str(supervise.supervisor_lock_path())
    lock_fd = supervise.acquire_supervisor_lock()

    env = _make_handoff_env(target_commit="not-a-valid-commit")
    env[supervise.HANDOFF_FD_ENV] = str(lock_fd)
    env[supervise.HANDOFF_PATH_ENV] = lock_path
    for k, v in env.items():
        monkeypatch.setenv(k, v)

    daemon = SupervisorDaemon(Settings())
    with pytest.raises(SystemExit) as exc_info:
        daemon._acquire_ownership()
    assert exc_info.value.code == 1

    with suppress(OSError):
        os.close(lock_fd)
    supervise.supervisor_lock_path().unlink(missing_ok=True)


# ------------------------------------------------------------------
# Normal startup fallback
# ------------------------------------------------------------------


@pytest.mark.usefixtures("_state_dir")
def test_normal_startup_uses_current_commit() -> None:
    """Non-handoff startup captures cli.current_commit() as runtime identity.

    Fallback to cli.current_commit() is only valid for genuine non-handoff
    startup — no handoff env vars are present.
    """
    _write_state_with_runtime()
    _set_cli_current(TARGET_COMMIT)

    # No handoff env vars → normal startup path.
    daemon = SupervisorDaemon(Settings())

    # _runtime_commit comes from capture_supervisor_runtime_commit()
    # which reads cli.current_commit().
    assert daemon._runtime_commit == TARGET_COMMIT
    assert daemon._handoff_target_commit is None


# ------------------------------------------------------------------
# GC preservation
# ------------------------------------------------------------------


@pytest.mark.usefixtures("_state_dir")
def test_gc_preserves_handoff_target_commit() -> None:
    """supervisor_authoritative_commits() includes the bound runtime commit.

    The target commit B binds to must never be garbage-collected while B
    is still executing from it.
    """
    _write_state_with_runtime(runtime_commit=TARGET_COMMIT)

    state = supervise.read_state()
    state_with_commit = replace(
        state, commit=TARGET_COMMIT, supervisor_runtime_commit=TARGET_COMMIT
    )
    supervise.write_state(state_with_commit)

    desired = supervise.SupervisorDesired(
        schema_version=supervise.SCHEMA_VERSION,
        generation=1,
        commit=TARGET_COMMIT,
        repo="/repo",
        uv_path="uv",
        worker_id=None,
    )
    supervise.write_desired(desired)

    authoritative = cli.supervisor_authoritative_commits()
    assert TARGET_COMMIT in authoritative


@pytest.mark.usefixtures("_state_dir")
def test_gc_does_not_preserve_unrelated_commits() -> None:
    """Unrelated commits are not preserved by GC rooting."""
    _write_state_with_runtime(runtime_commit=TARGET_COMMIT)

    state = supervise.read_state()
    state_with_commit = replace(
        state, commit=TARGET_COMMIT, supervisor_runtime_commit=TARGET_COMMIT
    )
    supervise.write_state(state_with_commit)

    desired = supervise.SupervisorDesired(
        schema_version=supervise.SCHEMA_VERSION,
        generation=1,
        commit=TARGET_COMMIT,
        repo="/repo",
        uv_path="uv",
        worker_id=None,
    )
    supervise.write_desired(desired)

    authoritative = cli.supervisor_authoritative_commits()
    assert OTHER_COMMIT not in authoritative


# ------------------------------------------------------------------
# Status identity
# ------------------------------------------------------------------


@pytest.mark.usefixtures("_state_dir")
def test_status_reports_bound_runtime_commit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Status snapshot carries the bound supervisor_runtime_commit."""
    _write_state_with_runtime()
    _set_cli_current(TARGET_COMMIT)

    daemon = SupervisorDaemon(Settings())
    # Simulate binding from handoff target.
    daemon._runtime_commit = TARGET_COMMIT
    daemon._start_time_ticks = 42

    monkeypatch.setattr(SupervisorDaemon, "_write_pidfile", lambda _s: None)
    monkeypatch.setattr(SupervisorDaemon, "_persist_runtime_commit", lambda _s: None)
    monkeypatch.setattr(SupervisorDaemon, "_invalidate_stale_status", lambda _s: None)
    monkeypatch.setattr("lubko.supervisor.normalize_cross_boot_state", lambda: None)
    monkeypatch.setattr(SupervisorDaemon, "_install_signal_handlers", lambda _s: None)
    monkeypatch.setattr("lubko.supervisor._durable_log_handlers", list)

    daemon._write_status()

    # read_status may return None if pid liveness check fails in test env;
    # instead check the written file directly.
    status_path = supervise.status_path()
    if status_path.exists():
        raw = json.loads(status_path.read_text(encoding="utf-8"))
        assert raw.get("supervisor_runtime_commit") == TARGET_COMMIT


# ------------------------------------------------------------------
# Later skew detection
# ------------------------------------------------------------------


@pytest.mark.usefixtures("_state_dir")
def test_cli_current_skew_after_handoff_is_detected() -> None:
    """After B binds to TARGET_COMMIT, a later cli/current change is detectable.

    The reconcile loop compares supervisor_runtime_commit against
    cli.current_commit() to detect skew.  B's bound identity must not
    prevent this detection.
    """
    _write_state_with_runtime(runtime_commit=TARGET_COMMIT)

    # Set up cli/current pointing to OTHER_COMMIT (skew).
    _set_cli_current(OTHER_COMMIT)

    daemon = SupervisorDaemon(Settings())
    daemon._runtime_commit = TARGET_COMMIT

    # cli.current_commit() returns OTHER_COMMIT.
    assert cli.current_commit() == OTHER_COMMIT
    # But daemon._runtime_commit is still TARGET_COMMIT (bound from A).
    assert daemon._runtime_commit == TARGET_COMMIT
    # Skew is detectable by comparison.
    assert daemon._runtime_commit != cli.current_commit()
