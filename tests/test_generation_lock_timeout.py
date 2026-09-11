"""Deterministic regression coverage for bounded generation-lock acquisition.

Exercises uncontended success, deadline timeout via fake monotonic/flock,
release on normal/exceptional exits, eventual acquisition, generation
monotonicity, and call-site timeout propagation through every public/domain
boundary that previously assumed indefinite lock acquisition.
"""

from __future__ import annotations

import fcntl
import threading
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from lubko import deployctl, lifecycle, supervise
from lubko.lifecycle import DeployOptions, WorkerMeta

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_BLOCKING_MSG = "Resource temporarily unavailable"


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


def _deadline_exceeded_monotonic() -> tuple[list[int], type]:
    """Return a fake ``time.monotonic`` whose second call exceeds any deadline."""
    call_count = [0]

    def _fake() -> float:
        call_count[0] += 1
        return 100.0 if call_count[0] > 1 else 0.0

    return call_count, _fake  # type: ignore[return-value]


def _always_contend_flock(_fd: object, operation: int) -> None:
    """Fake ``fcntl.flock`` that always contends on LOCK_NB.

    Raises:
        BlockingIOError: Always when LOCK_NB is set.
    """
    if operation & fcntl.LOCK_NB:
        raise BlockingIOError(_BLOCKING_MSG)


def _make_deploy_options() -> DeployOptions:
    """Return minimal DeployOptions for lifecycle tests."""
    return DeployOptions(
        repo=Path("/repo"),
        uv_path="uv",
        lock_timeout_seconds=10.0,
        postgres_timeout_seconds=5.0,
        stop_grace_seconds=10.0,
        validation_timeout_seconds=10.0,
        git_timeout_seconds=10.0,
        cli_timeout_seconds=10.0,
        bootstrap=False,
        direct_spawn=False,
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
# Timeout via fake fcntl/monotonic (no real waits)
# ---------------------------------------------------------------------------


def test_generation_lock_timeout_on_deadline() -> None:
    """Timeout fires when monotonic exceeds the deadline during contention.

    Uses fake fcntl.flock (always BlockingIOError) and fake time.monotonic
    (returns 0 on first call, then past deadline) to prove the polling loop
    checks the deadline without any real sleep.
    """
    _call_count, monotonic_fn = _deadline_exceeded_monotonic()
    with (
        patch("lubko.supervise.time.monotonic", side_effect=monotonic_fn),
        patch("lubko.supervise.time.sleep"),
        patch("lubko.supervise.fcntl.flock", side_effect=_always_contend_flock),
        pytest.raises(supervise.GenerationLockTimeoutError, match="generation lock"),
        supervise.generation_lock(timeout_seconds=1.0),
    ):
        pass  # pragma: no cover


def test_generation_lock_timeout_message_content() -> None:
    """The timeout error message mentions the generation lock."""
    _call_count, monotonic_fn = _deadline_exceeded_monotonic()
    with (
        patch("lubko.supervise.time.monotonic", side_effect=monotonic_fn),
        patch("lubko.supervise.time.sleep"),
        patch("lubko.supervise.fcntl.flock", side_effect=_always_contend_flock),
        pytest.raises(supervise.GenerationLockTimeoutError, match="generation lock"),
        supervise.generation_lock(timeout_seconds=1.0),
    ):
        pass  # pragma: no cover


def test_generation_lock_no_mutation_on_timeout() -> None:
    """No durable state changes when the lock times out.

    Proves the lock body never executes by making the lock always contend,
    then verifying pre-existing state is preserved.
    """
    _write_desired(42, commit="before")
    _call_count, monotonic_fn = _deadline_exceeded_monotonic()
    with (
        patch("lubko.supervise.time.monotonic", side_effect=monotonic_fn),
        patch("lubko.supervise.time.sleep"),
        patch("lubko.supervise.fcntl.flock", side_effect=_always_contend_flock),
        pytest.raises(supervise.GenerationLockTimeoutError),
        supervise.generation_lock(timeout_seconds=1.0),
    ):
        pass  # pragma: no cover

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
# Eventual acquisition after contention clears (real flock, fast)
# ---------------------------------------------------------------------------


def test_generation_lock_eventual_acquisition() -> None:
    """Second holder acquires once the first releases."""
    lock_path = supervise.supervisor_dir() / ".generation.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    held = threading.Event()
    release = threading.Event()
    acquired = threading.Event()

    def _hold() -> None:
        with lock_path.open("a+") as fh:
            fcntl.flock(fh, fcntl.LOCK_EX)
            held.set()
            release.wait()
            fcntl.flock(fh, fcntl.LOCK_UN)

    def _wait_and_acquire() -> None:
        with supervise.generation_lock(timeout_seconds=5.0):
            acquired.set()

    t_hold = threading.Thread(target=_hold)
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
# Call-site propagation: deployctl
# ---------------------------------------------------------------------------


def test_next_mission_generation_wraps_timeout() -> None:
    """deployctl.next_mission_generation wraps timeout as DeployCtlError."""
    with (
        patch(
            "lubko.deployctl.supervise.generation_lock",
            side_effect=supervise.GenerationLockTimeoutError("test"),
        ),
        pytest.raises(deployctl.DeployCtlError, match="generation lock"),
    ):
        deployctl.next_mission_generation()


def test_settle_desired_wraps_timeout() -> None:
    """deployctl.settle_desired wraps timeout as DeployCtlError."""
    with (
        patch(
            "lubko.deployctl.supervise.request_run",
            side_effect=supervise.GenerationLockTimeoutError("test"),
        ),
        pytest.raises(deployctl.DeployCtlError, match="generation lock"),
    ):
        deployctl.settle_desired("abc", "/repo", "uv")


def test_finalize_rollback_wraps_timeout() -> None:
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


def test_finalize_confirmation_wraps_timeout() -> None:
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


# ---------------------------------------------------------------------------
# Call-site propagation: lifecycle
# ---------------------------------------------------------------------------


def test_queue_deploy_candidate_converged_returns_false() -> None:
    """_queue_deploy_candidate_converged returns False on timeout."""
    with patch(
        "lubko.lifecycle.supervise.generation_lock",
        side_effect=supervise.GenerationLockTimeoutError("test"),
    ):
        assert lifecycle._queue_deploy_candidate_converged("abc") is False


def test_restore_after_handoff_logs_on_timeout() -> None:
    """_restore_after_handoff_failure logs and returns on request_run timeout."""
    options = _make_deploy_options()
    with (
        patch("lubko.lifecycle.supervise.supervisor_running", return_value=True),
        patch("lubko.lifecycle._queue_deploy_candidate_converged", return_value=False),
        patch(
            "lubko.lifecycle.supervise.request_run",
            side_effect=supervise.GenerationLockTimeoutError("test"),
        ),
    ):
        # Should not raise — logs and returns.
        lifecycle._restore_after_handoff_failure(options, "abc", None)


def test_deploy_through_supervisor_wraps_timeout() -> None:
    """_deploy_through_supervisor wraps timeout as DeployAbortedError."""
    options = _make_deploy_options()
    with (
        patch(
            "lubko.lifecycle.supervise.request_run",
            side_effect=supervise.GenerationLockTimeoutError("test"),
        ),
        pytest.raises(lifecycle.DeployAbortedError, match="generation lock"),
    ):
        lifecycle._deploy_through_supervisor(options, "abc")


def test_restart_intent_locked_returns_error() -> None:
    """_restart_intent_locked returns error string on timeout."""
    state = MagicMock()
    state.commit = "abc123"
    with (
        patch("lubko.lifecycle._supervised_mutation_blocker", return_value=None),
        patch("lubko.lifecycle.supervise.supervisor_running", return_value=True),
        patch("lubko.lifecycle.supervise.read_state", return_value=state),
        patch("lubko.lifecycle.cli.runtime_is_usable", return_value=True),
        patch("lubko.lifecycle.supervise.read_status", return_value=None),
        patch("lubko.lifecycle.supervise.read_desired", return_value=None),
        patch(
            "lubko.lifecycle.supervise.request_restart",
            side_effect=supervise.GenerationLockTimeoutError("test"),
        ),
    ):
        gen, pid, error = lifecycle._restart_intent_locked()
    assert gen is None
    assert pid is None
    assert error is not None
    assert "generation lock" in error


def test_request_restart_intent_locked_wraps_timeout() -> None:
    """_request_restart_intent_locked raises DeployAbortedError on timeout."""
    with (
        patch("lubko.lifecycle._supervised_mutation_blocker", return_value=None),
        patch("lubko.lifecycle.supervise.read_state"),
        patch("lubko.lifecycle.cli.runtime_is_usable", return_value=True),
        patch("lubko.lifecycle.supervise.read_status", return_value=None),
        patch("lubko.lifecycle.supervise.read_desired", return_value=None),
        patch(
            "lubko.lifecycle.supervise.request_restart",
            side_effect=supervise.GenerationLockTimeoutError("test"),
        ),
        pytest.raises(lifecycle.DeployAbortedError, match="generation lock"),
    ):
        lifecycle._request_restart_intent_locked()


def test_migrate_locked_propagates_timeout() -> None:
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
