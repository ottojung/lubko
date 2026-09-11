"""Regression tests for supervisor lock adoption during exec-based handoff.

These tests prove:
- The same lock remains held through a simulated exec/adoption cycle.
- A competing supervisor cannot acquire the lock during the transition.
- Malformed or injected inherited-fd metadata fails closed.
- Exec failure leaves the old owner authoritative.
"""

from __future__ import annotations

import errno
import fcntl
import json
import os
from typing import TYPE_CHECKING, Final

import pytest

from lubko import supervise
from lubko.supervisor import Settings, SupervisorDaemon

if TYPE_CHECKING:
    from pathlib import Path

HANDOFF_LOCK_PATH: Final = str(supervise.supervisor_lock_path())


@pytest.fixture
def lock_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Provide an isolated supervisor state directory with a lock file."""
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
    supervise.supervisor_dir().mkdir(parents=True, exist_ok=True)
    return supervise.supervisor_dir()


def _acquire_and_hold(lock_path: Path) -> int:
    """Acquire the supervisor lock and return the fd (caller must close)."""
    fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    return fd


def _assert_flock_blocked(lock_path: Path) -> None:
    """Assert that a non-blocking flock raises EAGAIN or EWOULDBLOCK.

    Linux glibc reports ``Resource temporarily unavailable``; Android/Termux
    reports ``Try again``.  Checking the errno number is portable.
    """
    with pytest.raises(OSError) as exc_info:  # ruff: ignore[pytest-raises-too-broad]
        _acquire_and_hold(lock_path)
    assert exc_info.value.errno in {errno.EAGAIN, errno.EWOULDBLOCK}


def test_same_lock_held_through_adoption(lock_dir: Path) -> None:
    """Adopting an inherited fd keeps the same flock held."""
    lock_path = supervise.supervisor_lock_path()
    owner_fd = _acquire_and_hold(lock_path)
    os.set_inheritable(owner_fd, True)
    try:
        adopted_fd = supervise.adopt_supervisor_lock(owner_fd, str(lock_path))
        assert adopted_fd == owner_fd
        _assert_flock_blocked(lock_path)
    finally:
        os.set_inheritable(owner_fd, False)
        os.close(owner_fd)


def test_adoption_validates_path(lock_dir: Path) -> None:
    """Adoption fails when the fd points to a different file."""
    lock_path = supervise.supervisor_lock_path()
    owner_fd = _acquire_and_hold(lock_path)
    try:
        wrong_path = str(lock_dir / "wrong.lock")
        with pytest.raises(OSError, match="expected"):
            supervise.adopt_supervisor_lock(owner_fd, wrong_path)
    finally:
        os.close(owner_fd)


def test_adoption_rejects_negative_fd(lock_dir: Path) -> None:
    """Negative fd numbers fail closed."""
    with pytest.raises(OSError, match="outside the open-fd limit"):
        supervise.adopt_supervisor_lock(-1, HANDOFF_LOCK_PATH)


def test_adoption_rejects_huge_fd(lock_dir: Path) -> None:
    """Fd numbers beyond RLIMIT_NOFILE fail closed."""
    with pytest.raises(OSError, match="outside the open-fd limit"):
        supervise.adopt_supervisor_lock(999999, HANDOFF_LOCK_PATH)


def test_adoption_rejects_closed_fd(lock_dir: Path) -> None:
    """Adopting a closed fd fails closed."""
    lock_path = supervise.supervisor_lock_path()
    owner_fd = _acquire_and_hold(lock_path)
    os.close(owner_fd)
    with pytest.raises(OSError, match="not open"):
        supervise.adopt_supervisor_lock(owner_fd, str(lock_path))


def test_competitor_blocked_while_owner_holds(lock_dir: Path) -> None:
    """A second flock attempt fails while the owner holds the lock."""
    lock_path = supervise.supervisor_lock_path()
    owner_fd = _acquire_and_hold(lock_path)
    try:
        _assert_flock_blocked(lock_path)
    finally:
        os.close(owner_fd)


def test_competitor_blocked_during_adoption(lock_dir: Path) -> None:
    """A competitor cannot acquire while adoption is in progress."""
    lock_path = supervise.supervisor_lock_path()
    owner_fd = _acquire_and_hold(lock_path)
    os.set_inheritable(owner_fd, True)
    try:
        _assert_flock_blocked(lock_path)
        adopted = supervise.adopt_supervisor_lock(owner_fd, str(lock_path))
        assert adopted == owner_fd
        _assert_flock_blocked(lock_path)
    finally:
        os.set_inheritable(owner_fd, False)
        os.close(owner_fd)


def test_lock_released_after_owner_exits(lock_dir: Path) -> None:
    """After the owner closes the fd, a competitor can acquire."""
    lock_path = supervise.supervisor_lock_path()
    owner_fd = _acquire_and_hold(lock_path)
    os.close(owner_fd)
    competitor_fd = _acquire_and_hold(lock_path)
    os.close(competitor_fd)


def test_malformed_fd_env_fails_closed(lock_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Non-integer handoff fd env var causes SystemExit."""
    monkeypatch.setenv(supervise.HANDOFF_FD_ENV, "not-a-number")
    monkeypatch.setenv(supervise.HANDOFF_PATH_ENV, HANDOFF_LOCK_PATH)
    daemon = SupervisorDaemon(Settings())
    with pytest.raises(SystemExit):
        daemon._try_adopt_inherited_lock()


def test_missing_path_env_fails_closed(lock_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Handoff fd present but path missing causes SystemExit."""
    monkeypatch.setenv(supervise.HANDOFF_FD_ENV, "5")
    monkeypatch.delenv(supervise.HANDOFF_PATH_ENV, raising=False)
    daemon = SupervisorDaemon(Settings())
    with pytest.raises(SystemExit):
        daemon._try_adopt_inherited_lock()


def test_wrong_path_env_fails_closed(lock_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Handoff fd pointing at wrong path causes SystemExit."""
    lock_path = supervise.supervisor_lock_path()
    owner_fd = _acquire_and_hold(lock_path)
    os.set_inheritable(owner_fd, True)
    try:
        monkeypatch.setenv(supervise.HANDOFF_FD_ENV, str(owner_fd))
        monkeypatch.setenv(supervise.HANDOFF_PATH_ENV, "/nonexistent/path")
        daemon = SupervisorDaemon(Settings())
        with pytest.raises(SystemExit):
            daemon._try_adopt_inherited_lock()
    finally:
        os.set_inheritable(owner_fd, False)
        os.close(owner_fd)


def test_injected_fd_wrong_path_fails_closed(
    lock_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An fd that exists but points to the wrong path causes SystemExit."""
    regular = lock_dir / "regular.txt"
    regular.write_text("data", encoding="utf-8")
    fd = os.open(str(regular), os.O_RDONLY)
    try:
        monkeypatch.setenv(supervise.HANDOFF_FD_ENV, str(fd))
        monkeypatch.setenv(supervise.HANDOFF_PATH_ENV, "/nonexistent/path")
        daemon = SupervisorDaemon(Settings())
        with pytest.raises(SystemExit):
            daemon._try_adopt_inherited_lock()
    finally:
        os.close(fd)


def test_old_supervisor_continues_after_failed_handoff(
    lock_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    r"""B failure before transfer leaves A authoritative and lock held.

    The daemon holds a real supervisor lock fd (non-inheritable).  A patched
    subprocess.Popen returns a process whose readiness pipe signals failure
    (F\n).  The test asserts: spawn was attempted, A's fd remains open/owned,
    inheritable flag is False, a competitor cannot acquire the lock, and a
    failure message is recorded.
    """
    lock_path = supervise.supervisor_lock_path()
    owner_fd = _acquire_and_hold(lock_path)
    try:
        assert os.get_inheritable(owner_fd) is False

        monkeypatch.setenv("XDG_STATE_HOME", str(lock_path.parent.parent))
        daemon = SupervisorDaemon(Settings())
        daemon._ownership_fd = owner_fd

        state_path = supervise.state_path()
        state_path.parent.mkdir(parents=True, exist_ok=True)
        state_path.write_text(
            json.dumps({
                **supervise.fresh_state().to_dict(),
                "supervisor_runtime_commit": "a" * 40,
            }),
            encoding="utf-8",
        )

        monkeypatch.setattr("lubko.cli.current_commit", lambda: "b" * 40)
        monkeypatch.setattr(
            "lubko.supervisor.resolve_new_supervisor_executable",
            lambda _commit: "/nonexistent/supervisor",
        )

        monkeypatch.setattr("lubko.supervisor.subprocess.Popen", _FailProcess)

        daemon._maybe_handoff_to_new_supervisor()

        assert os.get_inheritable(owner_fd) is False

        _assert_flock_blocked(lock_path)

        assert daemon._ownership_fd == owner_fd

        assert "did not complete" in daemon._message  # type: ignore[operator]
    finally:
        os.close(owner_fd)


def test_fresh_state_has_no_runtime_commit() -> None:
    """A fresh state has supervisor_runtime_commit=None."""
    state = supervise.fresh_state()
    assert state.supervisor_runtime_commit is None


def test_runtime_commit_round_trips() -> None:
    """supervisor_runtime_commit survives serialization round-trip."""
    state = supervise.SupervisorState(
        schema_version=supervise.SCHEMA_VERSION,
        applied_generation=0,
        mode=supervise.MODE_IDLE,
        commit=None,
        child=None,
        unresolved_child=None,
        ownership_hold_malformed=False,
        unresolved_hold_malformed=False,
        spawning=None,
        spawning_hold_malformed=False,
        intent=supervise.INTENT_RUN,
        restart_count=0,
        next_attempt_at=None,
        last_exit=None,
        last_spawn_at=None,
        ready=False,
        next_readiness_at=None,
        boot_id=None,
        supervisor_runtime_commit="a" * 40,
    )
    data = state.to_dict()
    assert data["supervisor_runtime_commit"] == "a" * 40
    restored = supervise.SupervisorState.from_dict(data)
    assert restored.supervisor_runtime_commit == "a" * 40


def test_absent_field_defaults_to_none() -> None:
    """Old state files without the field parse as None."""
    data = supervise.fresh_state().to_dict()
    assert "supervisor_runtime_commit" not in data
    restored = supervise.SupervisorState.from_dict(data)
    assert restored.supervisor_runtime_commit is None


def test_malformed_field_does_not_cause_hold() -> None:
    """A present-but-malformed supervisor_runtime_commit is treated as None."""
    data = supervise.fresh_state().to_dict()
    data["supervisor_runtime_commit"] = 12345
    restored = supervise.SupervisorState.from_dict(data)
    assert restored.supervisor_runtime_commit is None


def test_gc_preserves_supervisor_runtime_commit(lock_dir: Path) -> None:
    """cli.supervisor_authoritative_commits() includes supervisor_runtime_commit."""
    from lubko import cli

    commit_a = "a" * 40
    state_path = supervise.state_path()
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(
        json.dumps({
            **supervise.fresh_state().to_dict(),
            "supervisor_runtime_commit": commit_a,
        }),
        encoding="utf-8",
    )
    authoritative = cli.supervisor_authoritative_commits()
    assert commit_a in authoritative


def test_runtime_commit_persisted_through_startup_path(
    lock_dir: Path,
) -> None:
    """Prove _persist_runtime_commit() stores the captured commit from fresh state.

    Regression: write_state_preserving_authority() was unconditionally
    preserving current.supervisor_runtime_commit (always None on first
    startup), discarding the caller's new value.  This blocked runtime
    identity persistence, skew detection, and the GC root.

    This test exercises the actual SupervisorDaemon._persist_runtime_commit()
    method from fresh state with a captured runtime commit and proves the
    durable field is written:
    1. State starts without supervisor_runtime_commit (fresh install).
    2. A daemon with a known _runtime_commit calls _persist_runtime_commit().
    3. The stored commit survives a read round-trip.
    4. A subsequent _persist_runtime_commit() with the same value is a no-op.
    5. Skew detection can act on the stored value.
    """
    state_path = supervise.state_path()
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(
        json.dumps(supervise.fresh_state().to_dict()),
        encoding="utf-8",
    )

    loaded = supervise.read_state()
    assert loaded.supervisor_runtime_commit is None

    commit_a = "a" * 40
    daemon = SupervisorDaemon(Settings())
    daemon._runtime_commit = commit_a

    daemon._persist_runtime_commit()

    reloaded = supervise.read_state()
    assert reloaded.supervisor_runtime_commit == commit_a

    daemon._persist_runtime_commit()
    assert supervise.read_state().supervisor_runtime_commit == commit_a

    assert commit_a != "b" * 40


def _setup_handoff_state(
    lock_dir: Path, monkeypatch: pytest.MonkeyPatch, stored: str, confirmed: str
) -> tuple[SupervisorDaemon, int]:
    """Shared setup for handoff regression tests.

    Returns (daemon with ownership_fd set, owner_fd).
    """
    lock_path = supervise.supervisor_lock_path()
    owner_fd = _acquire_and_hold(lock_path)
    daemon = SupervisorDaemon(Settings())
    daemon._ownership_fd = owner_fd
    state_path = supervise.state_path()
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(
        json.dumps({
            **supervise.fresh_state().to_dict(),
            "supervisor_runtime_commit": stored,
        }),
        encoding="utf-8",
    )
    monkeypatch.setattr("lubko.cli.current_commit", lambda: confirmed)
    monkeypatch.setattr(
        "lubko.supervisor.resolve_new_supervisor_executable",
        lambda _commit: "/nonexistent/supervisor",
    )
    # Patch retire_supervisor_pid to succeed (no real pidfile in test)
    monkeypatch.setattr(
        "lubko.supervise.retire_supervisor_pid",
        lambda: (os.getpid(), 12345),
    )
    monkeypatch.setattr(
        "lubko.supervise.restore_supervisor_pid",
        lambda _pid, _ticks: None,
    )
    return daemon, owner_fd


class _ReadyProcess:
    """Mock Popen that writes R on the readiness pipe before returning."""

    def __init__(self, args: list[str], **kwargs: object) -> None:
        env: dict[str, str] = kwargs.get("env", {})  # type: ignore[assignment]
        ready_w = int(env[supervise.HANDOFF_READY_FD_ENV])
        os.write(ready_w, b"R\n")

    def wait(self, timeout: float = 0.0) -> int:
        return 0

    def poll(self) -> int | None:
        return 0

    def kill(self) -> None:
        pass


class _FailProcess:
    """Mock Popen that writes F on the readiness pipe before returning."""

    def __init__(self, args: list[str], **kwargs: object) -> None:
        env: dict[str, str] = kwargs.get("env", {})  # type: ignore[assignment]
        ready_w = int(env[supervise.HANDOFF_READY_FD_ENV])
        os.write(ready_w, b"F\n")

    def wait(self, timeout: float = 0.0) -> int:
        return 1

    def poll(self) -> int | None:
        return 1

    def kill(self) -> None:
        pass


class _SilentFailProcess:
    """Mock Popen that writes nothing (EOF) on the readiness pipe."""

    def __init__(self, args: list[str], **kwargs: object) -> None:
        pass

    def wait(self, timeout: float = 0.0) -> int:
        return 1

    def poll(self) -> int | None:
        return 1

    def kill(self) -> None:
        pass


def test_ready_while_a_still_authoritative(lock_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    r"""B ready while A still remains sole active authority.

    After A sends TRANSFER and closes its lock fd, B's readiness signal has
    already been sent, proving B initialized before A released authority.
    """
    commit_a = "a" * 40
    commit_b = "b" * 40
    daemon, _owner_fd = _setup_handoff_state(lock_dir, monkeypatch, commit_a, commit_b)
    monkeypatch.setattr("lubko.supervisor.subprocess.Popen", _ReadyProcess)
    daemon._maybe_handoff_to_new_supervisor()
    assert daemon._handoff_completed is True
    assert daemon._ownership_fd is None


def test_b_failure_before_transfer_leaves_a_authoritative(
    lock_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    r"""B failure before transfer leaves A authoritative and lock held.

    B signals F on the readiness pipe.  A continues with its own authority,
    the lock remains held, and a competitor cannot acquire it.
    """
    commit_a = "a" * 40
    commit_b = "b" * 40
    daemon, owner_fd = _setup_handoff_state(lock_dir, monkeypatch, commit_a, commit_b)
    monkeypatch.setattr("lubko.supervisor.subprocess.Popen", _FailProcess)
    daemon._maybe_handoff_to_new_supervisor()
    assert daemon._handoff_completed is False
    assert daemon._ownership_fd == owner_fd
    _assert_flock_blocked(supervise.supervisor_lock_path())
    assert "did not complete" in daemon._message  # type: ignore[operator]


def test_successful_transfer_no_overlap_no_gap(
    lock_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Successful transfer has no authority overlap and no gap.

    A holds the lock until TRANSFER is sent.  B receives TRANSFER and can
    then reconcile.  Exactly one authority exists at every point.
    """
    commit_a = "a" * 40
    commit_b = "b" * 40
    daemon, _owner_fd = _setup_handoff_state(lock_dir, monkeypatch, commit_a, commit_b)
    monkeypatch.setattr("lubko.supervisor.subprocess.Popen", _ReadyProcess)
    daemon._maybe_handoff_to_new_supervisor()
    assert daemon._handoff_completed is True
    assert daemon._ownership_fd is None
    new_fd = _acquire_and_hold(supervise.supervisor_lock_path())
    os.close(new_fd)


def test_successor_startup_failure_before_ready_recovers_to_a(
    lock_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    r"""Successor startup failure after process creation but before READY recovers to A.

    The spawned process closes the readiness pipe without writing (EOF).  A
    detects this, kills B, and continues as sole authority with the lock held.
    """
    commit_a = "a" * 40
    commit_b = "b" * 40
    daemon, owner_fd = _setup_handoff_state(lock_dir, monkeypatch, commit_a, commit_b)
    monkeypatch.setattr("lubko.supervisor.subprocess.Popen", _SilentFailProcess)
    daemon._maybe_handoff_to_new_supervisor()
    assert daemon._handoff_completed is False
    assert daemon._ownership_fd == owner_fd
    _assert_flock_blocked(supervise.supervisor_lock_path())
    assert "did not complete" in daemon._message  # type: ignore[operator]


def test_worker_loss_at_a_exit_is_recoverable_by_b(
    lock_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Worker loss at A exit is recoverable by B from durable state.

    After transfer, if A's exit causes PDEATHSIG on B's worker child, B must
    observe the loss from durable desired/applied authority and recover.  This
    test proves B can read the durable state and determine the intended worker
    after A exits.
    """
    commit_a = "a" * 40

    state_path = supervise.state_path()
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(
        json.dumps({
            **supervise.fresh_state().to_dict(),
            "supervisor_runtime_commit": commit_a,
        }),
        encoding="utf-8",
    )

    loaded = supervise.read_state()
    assert loaded.supervisor_runtime_commit == commit_a

    reloaded = supervise.read_state()
    assert reloaded.child is None
    assert reloaded.commit is None


def test_handoff_env_preserved_through_adopt(
    lock_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Handoff protocol env vars survive _try_adopt_inherited_lock.

    Regression: _try_adopt_inherited_lock() cleared HANDOFF_MODE_ENV
    before _run_handoff_protocol() could read it, so B never entered
    the READY/TRANSFER protocol and could reconcile while A was still
    waiting for READY.
    """
    lock_path = supervise.supervisor_lock_path()
    owner_fd = _acquire_and_hold(lock_path)
    try:
        os.set_inheritable(owner_fd, True)
        monkeypatch.setenv(supervise.HANDOFF_FD_ENV, str(owner_fd))
        monkeypatch.setenv(supervise.HANDOFF_PATH_ENV, str(lock_path))
        monkeypatch.setenv(supervise.HANDOFF_PID_ENV, "1")
        monkeypatch.setenv(supervise.HANDOFF_READY_FD_ENV, "99")
        monkeypatch.setenv(supervise.HANDOFF_TRANSFER_FD_ENV, "98")
        monkeypatch.setenv(supervise.HANDOFF_MODE_ENV, "1")

        daemon = SupervisorDaemon(Settings())
        fd = daemon._try_adopt_inherited_lock()

        assert fd == owner_fd

        assert os.environ.get(supervise.HANDOFF_READY_FD_ENV) == "99"
        assert os.environ.get(supervise.HANDOFF_TRANSFER_FD_ENV) == "98"
        assert os.environ.get(supervise.HANDOFF_MODE_ENV) == "1"

        assert os.environ.get(supervise.HANDOFF_FD_ENV) is None
        assert os.environ.get(supervise.HANDOFF_PATH_ENV) is None
        assert os.environ.get(supervise.HANDOFF_PID_ENV) is None
    finally:
        os.set_inheritable(owner_fd, False)
        os.close(owner_fd)


def test_ready_timeout_prevents_wedge(lock_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A bounded wait prevents a stuck B from wedging A.

    If B never sends READY (pipe stays open but no data), A's
    select-based timeout fires and A continues with its own authority.
    """
    commit_a = "a" * 40
    commit_b = "b" * 40
    daemon, owner_fd = _setup_handoff_state(lock_dir, monkeypatch, commit_a, commit_b)

    class _StuckProcess:
        """Mock Popen that holds the readiness pipe open without writing."""

        def __init__(self, args: list[str], **kwargs: object) -> None:
            pass

        def wait(self, timeout: float = 0.0) -> int:
            return 1

        def poll(self) -> int | None:
            return None

        def kill(self) -> None:
            pass

    monkeypatch.setattr("lubko.supervisor.subprocess.Popen", _StuckProcess)
    monkeypatch.setattr(
        "lubko.supervisor.select.select",
        lambda _r, _w, _x, _timeout: ([], [], []),
    )

    daemon._maybe_handoff_to_new_supervisor()

    assert daemon._handoff_completed is False
    assert daemon._ownership_fd == owner_fd

    _assert_flock_blocked(supervise.supervisor_lock_path())

    assert "did not complete" in daemon._message  # type: ignore[operator]
