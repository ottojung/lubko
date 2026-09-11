"""Deterministic regression coverage for bounded generation-lock acquisition.

Exercises uncontended success, deadline timeout, no-mutation on timeout,
release on normal/exceptional exits, eventual acquisition after contention
clears, generation monotonicity under contention, and call-site timeout
propagation through deployctl and lifecycle domains.
"""

from __future__ import annotations

import fcntl
import threading
from pathlib import Path
from unittest.mock import patch

import pytest

from lubko import deployctl, lifecycle, supervise
from lubko.lifecycle import WorkerMeta

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_desired(gen: int, commit: str = "abc123") -> None:
    """Write a minimal desired intent for generation *gen*."""
    supervise.write_desired(
        supervise.SupervisorDesired(
            schema_version=supervise.SCHEMA_VERSION,
            generation=gen,
            commit=commit,
            repo="/workspace/repo",
            uv_path="uv",
            worker_id=None,
        )
    )


def _hold_lock_in_thread() -> tuple[threading.Event, threading.Event, threading.Thread]:
    """Return (held, release, thread) that holds the generation lock file."""
    lock_path = supervise.supervisor_dir() / ".generation.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    held = threading.Event()
    release = threading.Event()

    def _hold() -> None:
        with lock_path.open("a+") as fh:
            fcntl.flock(fh, fcntl.LOCK_EX)
            held.set()
            release.wait()
            fcntl.flock(fh, fcntl.LOCK_UN)

    t = threading.Thread(target=_hold)
    return held, release, t


def _make_rollback_state() -> deployctl.RollbackState:
    """Return a minimal RollbackState for call-site tests."""
    previous = WorkerMeta(
        schema_version=1,
        state="running",
        pid=1000,
        pgid=1000,
        sid=1000,
        start_time_ticks=1000,
        token=None,
        repo="/repo",
        git_commit="def",
        worker_id=None,
        log_path="worker.log",
        started_at=0.0,
        stopped_at=None,
    )
    return deployctl.RollbackState(
        schema_version=deployctl.ROLLBACK_SCHEMA_VERSION,
        generation=1,
        status=deployctl.STATUS_PENDING,
        commit="abc",
        previous_commit="def",
        deadline=0.0,
        repo="/repo",
        uv_path="uv",
        stop_grace_seconds=10.0,
        git_timeout_seconds=10.0,
        previous_retiring=False,
        previous_meta=previous,
        new_meta=None,
        supervisor_owned=False,
    )


# ---------------------------------------------------------------------------
# Uncontended acquisition
# ---------------------------------------------------------------------------


def test_generation_lock_acquire_and_release() -> None:
    """generation_lock succeeds immediately when uncontended."""
    with supervise.generation_lock():
        _write_desired(1)
    desired = supervise.read_desired_strict()
    assert desired is not None
    assert desired.generation == 1


def test_generation_lock_reentrant_after_release() -> None:
    """A second acquisition succeeds after the first releases."""
    with supervise.generation_lock():
        _write_desired(1)
    with supervise.generation_lock():
        _write_desired(2)
    desired = supervise.read_desired_strict()
    assert desired is not None
    assert desired.generation == 2


# ---------------------------------------------------------------------------
# Timeout when contended
# ---------------------------------------------------------------------------


def test_generation_lock_timeout_with_real_flock_contention() -> None:
    """A second acquisition times out while the first holds the lock."""
    held, release, t = _hold_lock_in_thread()
    t.start()
    held.wait(timeout=5.0)
    try:
        with (
            pytest.raises(supervise.GenerationLockTimeoutError),
            supervise.generation_lock(timeout_seconds=0.1),
        ):
            pass  # pragma: no cover
    finally:
        release.set()
        t.join(timeout=5.0)


def test_generation_lock_timeout_message_content() -> None:
    """The timeout error message mentions the generation lock."""
    held, release, t = _hold_lock_in_thread()
    t.start()
    held.wait(timeout=5.0)
    try:
        with (
            pytest.raises(supervise.GenerationLockTimeoutError, match="generation lock"),
            supervise.generation_lock(timeout_seconds=0.1),
        ):
            pass  # pragma: no cover
    finally:
        release.set()
        t.join(timeout=5.0)


# ---------------------------------------------------------------------------
# No mutation on timeout
# ---------------------------------------------------------------------------


def test_generation_lock_no_mutation_on_timeout() -> None:
    """read_desired_strict returns unchanged state after a timeout."""
    _write_desired(42, commit="before")
    held, release, t = _hold_lock_in_thread()
    t.start()
    held.wait(timeout=5.0)
    try:
        with (
            pytest.raises(supervise.GenerationLockTimeoutError),
            supervise.generation_lock(timeout_seconds=0.1),
        ):
            _write_desired(99, commit="stale")  # pragma: no cover
    finally:
        release.set()
        t.join(timeout=5.0)

    desired = supervise.read_desired_strict()
    assert desired is not None
    assert desired.generation == 42
    assert desired.commit == "before"


# ---------------------------------------------------------------------------
# Release on normal and exceptional exits
# ---------------------------------------------------------------------------


def test_generation_lock_release_on_normal_exit() -> None:
    """Lock is released after a normal with-block completes."""
    with supervise.generation_lock():
        _write_desired(1)
    with supervise.generation_lock():
        _write_desired(2)


def test_generation_lock_release_on_exception() -> None:
    """Lock is released even when the body raises.

    Raises:
        ValueError: Always, to exercise the exceptional release path.
    """
    err_msg = "boom"
    with pytest.raises(ValueError, match=err_msg), supervise.generation_lock():
        raise ValueError(err_msg)
    with supervise.generation_lock():
        _write_desired(3)
    desired = supervise.read_desired_strict()
    assert desired is not None
    assert desired.generation == 3


# ---------------------------------------------------------------------------
# Eventual acquisition after contention clears
# ---------------------------------------------------------------------------


def test_generation_lock_eventual_acquisition() -> None:
    """Second holder acquires once the first releases."""
    held, release, t_hold = _hold_lock_in_thread()
    acquired = threading.Event()

    def _wait_and_acquire() -> None:
        with supervise.generation_lock(timeout_seconds=5.0):
            acquired.set()

    t_hold.start()
    held.wait(timeout=5.0)
    t_wait = threading.Thread(target=_wait_and_acquire)
    t_wait.start()

    release.set()
    t_hold.join(timeout=5.0)
    assert acquired.wait(timeout=5.0), "second thread never acquired"
    t_wait.join(timeout=5.0)


# ---------------------------------------------------------------------------
# Generation monotonicity under contention
# ---------------------------------------------------------------------------


def test_generation_lock_contended_monotonicity() -> None:
    """Two threads allocating under the lock produce unique generations."""
    generations: list[int] = []
    lock = threading.Lock()
    iterations = 3
    thread_count = 2

    def allocate() -> None:
        for _ in range(iterations):
            with supervise.generation_lock():
                gen = supervise.next_generation()
                _write_desired(gen)
            with lock:
                generations.append(gen)

    threads = [threading.Thread(target=allocate) for _ in range(thread_count)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10.0)

    expected = iterations * thread_count
    assert len(generations) == expected
    assert len(set(generations)) == expected
    assert sorted(generations) == list(range(1, expected + 1))


# ---------------------------------------------------------------------------
# Call-site timeout propagation
# ---------------------------------------------------------------------------


def test_next_mission_generation_wraps_timeout_as_deployctl_error() -> None:
    """deployctl.next_mission_generation wraps timeout as DeployCtlError."""
    with (
        patch(
            "lubko.deployctl.supervise.generation_lock",
            side_effect=supervise.GenerationLockTimeoutError("test"),
        ),
        pytest.raises(deployctl.DeployCtlError, match="generation lock"),
    ):
        deployctl.next_mission_generation()


def test_finalize_rollback_wraps_timeout_as_deployctl_error() -> None:
    """_finalize_supervised_rollback wraps timeout as DeployCtlError."""
    state = _make_rollback_state()
    with (
        patch(
            "lubko.deployctl.supervise.generation_lock",
            side_effect=supervise.GenerationLockTimeoutError("test"),
        ),
        pytest.raises(deployctl.DeployCtlError, match="generation lock"),
    ):
        deployctl._finalize_supervised_rollback(state, 1)


def test_finalize_confirmation_wraps_timeout_as_deployctl_error() -> None:
    """_finalize_supervised_confirmation wraps timeout as DeployCtlError."""
    state = _make_rollback_state()
    with (
        patch(
            "lubko.deployctl.supervise.generation_lock",
            side_effect=supervise.GenerationLockTimeoutError("test"),
        ),
        pytest.raises(deployctl.DeployCtlError, match="generation lock"),
    ):
        deployctl._finalize_supervised_confirmation(state, 1)


def test_queue_deploy_candidate_converged_returns_false_on_timeout() -> None:
    """_queue_deploy_candidate_converged returns False on timeout."""
    with patch(
        "lubko.lifecycle.supervise.generation_lock",
        side_effect=supervise.GenerationLockTimeoutError("test"),
    ):
        assert lifecycle._queue_deploy_candidate_converged("abc") is False


def test_migrate_locked_propagates_generation_lock_timeout() -> None:
    """_migrate_locked propagates GenerationLockTimeoutError on timeout."""
    with (
        patch(
            "lubko.lifecycle.supervise.generation_lock",
            side_effect=supervise.GenerationLockTimeoutError("test"),
        ),
        patch("lubko.lifecycle.remove_durable"),
        patch.object(
            deployctl,
            "read_rollback_state",
            side_effect=deployctl.DeployCtlError,
        ),
        pytest.raises(supervise.GenerationLockTimeoutError),
    ):
        lifecycle._migrate_locked("abc123", Path("/workspace/repo"), "uv")


# ---------------------------------------------------------------------------
# Default timeout constant
# ---------------------------------------------------------------------------


def test_default_timeout_is_positive() -> None:
    """DEFAULT_GENERATION_LOCK_TIMEOUT_SECONDS is a positive number."""
    assert supervise.DEFAULT_GENERATION_LOCK_TIMEOUT_SECONDS > 0


def test_poll_interval_is_bounded() -> None:
    """GENERATION_LOCK_POLL_SECONDS is a small positive fraction."""
    assert 0 < supervise.GENERATION_LOCK_POLL_SECONDS < 1.0
