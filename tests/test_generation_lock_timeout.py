"""Deterministic regression tests for bounded generation-lock acquisition.

The generation lock must use non-blocking flock with a monotonic deadline
and polling, raising ``GenerationLockTimeoutError`` on expiry. Tests verify
uncontended success, deadline timeout, no state mutation on timeout, lock
release on normal and exceptional exits, and generation monotonicity under
contention -- all without real sleeps.
"""

from __future__ import annotations

import fcntl
import threading
from pathlib import Path  # ruff: ignore[typing-only-standard-library-import]

import pytest

from lubko import supervise
from lubko.supervise import (
    DEFAULT_GENERATION_LOCK_TIMEOUT_SECONDS,
    GenerationLockTimeoutError,
    generation_lock,
)


def _lock_path() -> Path:
    path = supervise.supervisor_dir() / ".generation.lock"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def test_uncontended_acquisition_succeeds() -> None:
    """Lock is acquired immediately when no other holder exists."""
    with generation_lock(timeout_seconds=1.0):
        pass


def test_deadline_timeout_raises() -> None:
    """Lock acquisition fails with GenerationLockTimeoutError when contended."""
    path = _lock_path()
    held = threading.Event()
    release = threading.Event()

    def hold_lock() -> None:
        with path.open("a+") as handle:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            held.set()
            release.wait()
            fcntl.flock(handle, fcntl.LOCK_UN)

    holder = threading.Thread(target=hold_lock)
    holder.start()
    held.wait()

    try:
        with (
            pytest.raises(GenerationLockTimeoutError, match="timed out"),
            generation_lock(timeout_seconds=0.0),
        ):
            pass
    finally:
        release.set()
        holder.join()


def test_no_mutation_on_timeout() -> None:
    """Critical-section writes must not occur when the lock is not held."""
    marker = _lock_path().parent / ".test_marker"
    path = _lock_path()
    held = threading.Event()
    release = threading.Event()

    def hold_lock() -> None:
        with path.open("a+") as handle:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            held.set()
            release.wait()
            fcntl.flock(handle, fcntl.LOCK_UN)

    holder = threading.Thread(target=hold_lock)
    holder.start()
    held.wait()

    try:
        with (
            pytest.raises(GenerationLockTimeoutError),
            generation_lock(timeout_seconds=0.0),
        ):
            marker.write_text("should-not-exist")
        assert not marker.exists()
    finally:
        release.set()
        holder.join()


def test_release_on_normal_exit() -> None:
    """Lock is released after a successful critical section."""
    path = _lock_path()
    acquired = threading.Event()

    def try_acquire() -> None:
        with generation_lock(timeout_seconds=5.0):
            acquired.set()

    t = threading.Thread(target=try_acquire)
    t.start()
    acquired.wait(timeout=5.0)
    t.join(timeout=5.0)
    assert not t.is_alive()

    with path.open("a+") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.flock(handle, fcntl.LOCK_UN)


def test_release_on_exception() -> None:
    """Lock is released even when the critical section raises.

    Raises:
        _TestError: Always, to verify lock release on exception.
    """

    class _TestError(Exception):
        pass

    test_msg = "boom"

    with pytest.raises(_TestError), generation_lock(timeout_seconds=5.0):
        raise _TestError(test_msg)

    path = _lock_path()
    with path.open("a+") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.flock(handle, fcntl.LOCK_UN)


def test_generation_monotonicity_under_contention() -> None:
    """Concurrent allocations produce unique contiguous generations."""
    iterations = 3
    thread_count = 3
    expected = iterations * thread_count
    generations: list[int] = []
    lock = threading.Lock()

    def allocate() -> None:
        for _ in range(iterations):
            with generation_lock(timeout_seconds=5.0):
                generation = supervise.next_generation()
                supervise.write_desired(
                    supervise.SupervisorDesired(
                        schema_version=supervise.SCHEMA_VERSION,
                        generation=generation,
                        commit="a" * 40,
                        repo="/workspace/repo",
                        uv_path="uv",
                        worker_id=None,
                    )
                )
            with lock:
                generations.append(generation)

    threads = [threading.Thread(target=allocate) for _ in range(thread_count)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(generations) == expected
    assert sorted(set(generations)) == list(range(1, 1 + expected))


def test_timeout_constant_has_sensible_value() -> None:
    """Default timeout is a positive finite number."""
    assert DEFAULT_GENERATION_LOCK_TIMEOUT_SECONDS > 0
    assert DEFAULT_GENERATION_LOCK_TIMEOUT_SECONDS < 600
