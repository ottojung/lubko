"""Rollback spawns must be converged before any retry can start another."""

from __future__ import annotations

import signal
import subprocess
import time
from dataclasses import replace
from typing import cast

import pytest

from lubko import cli as deploy_cli
from lubko import deployctl as dc
from lubko import lifecycle
from lubko.lifecycle import ProcessIdentity


class NumericSignalError(AssertionError):
    """Raised when a numeric kill primitive is used instead of a pidfd."""


def _numeric_signal_forbidden() -> None:
    """Fail the test: only pinned ``pidfd_send_signal`` may deliver signals.

    Raises:
        NumericSignalError: Always.
    """
    raise NumericSignalError


def _forbid_os_kill(_pid: int, _sig: int) -> None:
    """Stand-in for ``os.kill`` that must never run."""
    _numeric_signal_forbidden()


class FakePopen:
    """Deterministic stand-in for the spawned previous-worker ``Popen``."""

    def __init__(self, pid: int, *, mode: str) -> None:
        """Record the fake behaviour mode.

        Args:
            pid: Fake process id.
            mode: One of ``converges``, ``needs_kill``, or ``exited``.
        """
        self.pid = pid
        self.mode = mode
        self.returncode: int | None = 0 if mode == "exited" else None
        self.signals: list[str] = []

    def terminate(self) -> None:
        """Exact convergence never uses numeric ``Popen`` signals."""
        del self
        _numeric_signal_forbidden()

    def kill(self) -> None:
        """Exact convergence never uses numeric ``Popen`` signals."""
        del self
        _numeric_signal_forbidden()

    def poll(self) -> int | None:
        """Return the exit status while the child is still alive."""
        return self.returncode

    def wait(self, timeout: float | None = None) -> int:
        """Reap the child unless it has not yet been signalled enough.

        Args:
            timeout: How long a real ``Popen`` would wait.

        Returns:
            The exit status.

        Raises:
            subprocess.TimeoutExpired: When the child refuses to exit yet.
        """
        expired: float = timeout if timeout is not None else 0.0
        if timeout is not None:
            if self.mode == "converges" and "SIGTERM" not in self.signals:
                raise subprocess.TimeoutExpired(cmd="worker", timeout=expired)
            if self.mode == "needs_kill" and "SIGKILL" not in self.signals:
                raise subprocess.TimeoutExpired(cmd="worker", timeout=expired)
        # An unbounded reap models the child eventually exiting on its own.
        if self.returncode is None:
            self.returncode = -1
        return self.returncode


def pending_state(*, previous_retiring: bool = False) -> dc.RollbackState:
    """Return a live pending deployment state.

    Args:
        previous_retiring: Whether the previous worker's retirement has begun.

    Returns:
        A pending rollback state with distinct old/new commits.
    """

    def worker_meta(commit: str, *, pid: int) -> lifecycle.WorkerMeta:
        return lifecycle.WorkerMeta(
            schema_version=lifecycle.SCHEMA_VERSION,
            state=lifecycle.STATE_RUNNING,
            pid=pid,
            pgid=pid,
            sid=pid,
            start_time_ticks=pid * 10,
            token=f"token-{pid}",
            repo="/workspace/Lubko",
            git_commit=commit,
            worker_id="test-worker",
            log_path="worker.log",
            started_at=1.0,
            stopped_at=None,
        )

    old = "1" * 40
    new = "2" * 40
    return dc.RollbackState(
        schema_version=dc.ROLLBACK_SCHEMA_VERSION,
        generation=1,
        status=dc.STATUS_PENDING,
        commit=new,
        previous_commit=old,
        deadline=time.time() + 60,
        repo="/workspace/Lubko",
        uv_path="uv",
        stop_grace_seconds=1.0,
        git_timeout_seconds=5.0,
        previous_retiring=previous_retiring,
        previous_meta=worker_meta(old, pid=100),
        new_meta=worker_meta(new, pid=200),
        supervisor_owned=False,
    )


@pytest.fixture
def retiring_state() -> dc.RollbackState:
    """Return a rollback mission whose previous worker is retiring.

    Returns:
        A rollback mission in the ``previous_retiring`` phase, so the
        fresh-spawn replacement path is exercised.
    """
    return pending_state(previous_retiring=True)


@pytest.fixture
def forbid_numeric_signals(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fail the test if any numeric kill primitive is used."""
    monkeypatch.setattr("os.kill", _forbid_os_kill)
    monkeypatch.setattr("os.killpg", _forbid_os_kill)


def unproven_anchor(fake: FakePopen) -> ProcessIdentity:
    """Return a plausible non-private pre-transition identity for ``fake``.

    Args:
        fake: The spawned fake child.

    Returns:
        An observed identity without a private session or group.
    """
    return ProcessIdentity(
        pid=fake.pid,
        pgid=1,
        sid=1,
        start_time_ticks=fake.pid * 10,
    )


def install_pinned_convergence(
    monkeypatch: pytest.MonkeyPatch,
    fakes: dict[int, FakePopen],
    occupants: dict[int, ProcessIdentity | None],
) -> list[tuple[int, str]]:
    """Make pidfd pinning, under-pin proof, and signalling deterministic.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
        fakes: Direct children keyed by PID.
        occupants: Identity observed through ``/proc`` per PID during the
            under-pin re-proof.

    Returns:
        The ``(pid, signal name)`` pairs delivered through the pin.
    """
    delivered: list[tuple[int, str]] = []
    pins: dict[int, int] = {}

    def open_pin(pid: int) -> int:
        fd = 10000 + pid
        pins[fd] = pid
        return fd

    def send(pin: int, sig: int) -> None:
        pid = pins[pin]
        name = signal.Signals(sig).name
        delivered.append((pid, name))
        fake = fakes[pid]
        fake.signals.append(name)
        if name == "SIGTERM":
            if fake.mode == "converges":
                fake.returncode = -15
        elif fake.mode != "exited":
            fake.returncode = -9

    def close_pin(_pin: int) -> None:
        return

    monkeypatch.setattr(lifecycle, "_open_exact_pidfd", open_pin)

    def occupant(pid: int) -> ProcessIdentity | None:
        return occupants.get(pid)

    monkeypatch.setattr(lifecycle, "process_identity", occupant)
    monkeypatch.setattr(lifecycle, "pidfd_send_signal", send)
    monkeypatch.setattr("os.close", close_pin)
    return delivered


def _install_failing_identity(
    monkeypatch: pytest.MonkeyPatch,
    fake: FakePopen,
    spawned: list[FakePopen],
) -> list[tuple[int, str]]:
    """Force the spawn path to produce ``fake`` and never prove its session.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
        fake: The fake child every spawn returns.
        spawned: List receiving every spawned fake child.

    Returns:
        The pinned signals the helper delivers for ``fake``.
    """
    occupants: dict[int, ProcessIdentity | None] = {fake.pid: unproven_anchor(fake)}
    delivered = install_pinned_convergence(monkeypatch, {fake.pid: fake}, occupants)
    monkeypatch.setattr(dc, "worker_alive", lambda _meta: False)
    monkeypatch.setattr(dc, "_wait_for_identity", lambda _proc: occupants[fake.pid])

    def fake_spawn(*_args: object, **_kwargs: object) -> FakePopen:
        spawned.append(fake)
        return fake

    monkeypatch.setattr(subprocess, "Popen", fake_spawn)
    return delivered


def test_unproven_live_child_is_converged_and_retry_stays_possible(
    monkeypatch: pytest.MonkeyPatch,
    forbid_numeric_signals: None,
    retiring_state: dc.RollbackState,
) -> None:
    """A live child whose identity timed out is converged before returning."""
    del forbid_numeric_signals
    fake = FakePopen(41001, mode="converges")
    spawned: list[FakePopen] = []
    delivered = _install_failing_identity(monkeypatch, fake, spawned)

    restored = dc.restart_previous(retiring_state)

    assert restored is None
    assert spawned == [fake]
    assert delivered == [(fake.pid, "SIGTERM")]
    assert fake.poll() == -15


def test_already_exited_child_needs_no_convergence(
    monkeypatch: pytest.MonkeyPatch,
    forbid_numeric_signals: None,
    retiring_state: dc.RollbackState,
) -> None:
    """An already-exited child remains an ordinary retryable failure."""
    del forbid_numeric_signals
    fake = FakePopen(41002, mode="exited")
    spawned: list[FakePopen] = []
    delivered = _install_failing_identity(monkeypatch, fake, spawned)

    restored = dc.restart_previous(retiring_state)

    assert restored is None
    assert delivered == []


def test_child_ignoring_sigterm_is_killed_and_reaped(
    monkeypatch: pytest.MonkeyPatch,
    forbid_numeric_signals: None,
    retiring_state: dc.RollbackState,
) -> None:
    """A child that ignores SIGTERM is pinned-SIGKILLed and reaped."""
    del forbid_numeric_signals
    fake = FakePopen(41003, mode="needs_kill")
    spawned: list[FakePopen] = []
    delivered = _install_failing_identity(monkeypatch, fake, spawned)

    restored = dc.restart_previous(retiring_state)

    assert restored is None
    assert delivered == [(fake.pid, "SIGTERM"), (fake.pid, "SIGKILL")]
    assert fake.poll() == -9


def test_repeated_retries_never_leave_a_live_worker_behind(
    monkeypatch: pytest.MonkeyPatch,
    forbid_numeric_signals: None,
    retiring_state: dc.RollbackState,
) -> None:
    """Repeated watchdog retries converge every spawn before the next one."""
    del forbid_numeric_signals
    fakes: dict[int, FakePopen] = {}
    occupants: dict[int, ProcessIdentity | None] = {}
    install_pinned_convergence(monkeypatch, fakes, occupants)
    monkeypatch.setattr(dc, "worker_alive", lambda _meta: False)
    monkeypatch.setattr(dc, "_wait_for_identity", lambda proc: occupants[proc.pid])

    def counting_spawn(*_args: object, **_kwargs: object) -> FakePopen:
        fake = FakePopen(41004 + len(fakes), mode="converges")
        fakes[fake.pid] = fake
        occupants[fake.pid] = unproven_anchor(fake)
        return fake

    monkeypatch.setattr(subprocess, "Popen", counting_spawn)

    results = [dc.restart_previous(retiring_state) for _ in range(3)]

    assert results == [None, None, None]
    # Every abandoned spawn was positively converged: no live worker from any
    # earlier retry can coexist with a later replacement.
    assert all(fake.poll() is not None for fake in fakes.values())
    assert all(fake.signals == ["SIGTERM"] for fake in fakes.values())


def test_reused_occupant_between_proof_and_pin_is_never_signalled(
    monkeypatch: pytest.MonkeyPatch,
    forbid_numeric_signals: None,
    retiring_state: dc.RollbackState,
) -> None:
    """A recycled PID occupant observed under the pin absorbs no signal."""
    del forbid_numeric_signals
    fake = FakePopen(41005, mode="needs_kill")
    anchor = unproven_anchor(fake)
    recycled = ProcessIdentity(
        pid=fake.pid,
        pgid=fake.pid,
        sid=fake.pid,
        start_time_ticks=anchor.start_time_ticks + 777,
    )
    # The occupant under the pin is a different process instance than the one
    # the anchor described: the original child exited and its PID was reused.
    delivered = install_pinned_convergence(monkeypatch, {fake.pid: fake}, {fake.pid: recycled})
    monkeypatch.setattr(dc, "worker_alive", lambda _meta: False)
    monkeypatch.setattr(dc, "_wait_for_identity", lambda _proc: anchor)
    spawned: list[FakePopen] = []

    def spawn(*_args: object, **_kwargs: object) -> FakePopen:
        spawned.append(fake)
        return fake

    monkeypatch.setattr(subprocess, "Popen", spawn)

    restored = dc.restart_previous(retiring_state)

    assert restored is None
    assert spawned == [fake]
    assert delivered == []
    # Fail closed: the unresolved child is positively reaped, nothing else.
    assert fake.returncode == -1


def test_restore_retry_reuses_durable_previous_worker_after_state_write_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A published restored worker survives a crash without a duplicate spawn."""
    state = pending_state(previous_retiring=True)
    spawned: list[FakePopen] = []
    published: list[lifecycle.WorkerMeta] = []
    state_writes: list[dc.RollbackState] = []
    fake = FakePopen(42001, mode="converges")
    identity = ProcessIdentity(
        pid=fake.pid,
        pgid=fake.pid,
        sid=fake.pid,
        start_time_ticks=fake.pid * 10,
    )
    monkeypatch.setattr(dc, "_checkout", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(dc, "read_meta_strict", lambda: published[-1] if published else None)

    def spawn(*_args: object, **_kwargs: object) -> FakePopen:
        spawned.append(fake)
        return fake

    monkeypatch.setattr(subprocess, "Popen", spawn)
    monkeypatch.setattr(dc, "_wait_for_identity", lambda _proc: identity)
    monkeypatch.setattr(dc, "_release_gate", dc._close_gate)
    monkeypatch.setattr(dc, "_wait_for_released_worker", lambda _meta: True)
    monkeypatch.setattr(dc, "check_postgres", lambda _timeout: True)
    monkeypatch.setattr(
        dc,
        "worker_alive",
        lambda meta: meta.pid == fake.pid and meta.start_time_ticks == identity.start_time_ticks,
    )
    monkeypatch.setattr(dc, "write_meta", published.append)

    terminal_crash_pending = True

    def write_state(value: dc.RollbackState) -> None:
        nonlocal terminal_crash_pending
        state_writes.append(value)
        if value.status == dc.STATUS_ROLLED_BACK and terminal_crash_pending:
            terminal_crash_pending = False
            msg = "simulated crash after worker metadata publication"
            raise OSError(msg)

    monkeypatch.setattr(dc, "_write_state", write_state)
    monkeypatch.setattr(deploy_cli, "remove_cli_root", lambda _commit: None)
    monkeypatch.setattr(deploy_cli, "reconcile_pointer", lambda _commit: True)

    with pytest.raises(OSError, match="simulated crash"):
        dc._restore_previous_locked(state)

    assert len(spawned) == 1
    assert len(published) == 1
    restored = published[0]
    assert restored.git_commit == state.previous_commit
    assert dc._restore_previous_locked(state)
    assert len(spawned) == 1
    assert published == [restored, restored]
    assert state_writes[-1].status == dc.STATUS_ROLLED_BACK


def test_released_previous_worker_is_adopted_after_metadata_publication_crash(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A released exact worker remains durable authority before ``write_meta``."""
    state = pending_state(previous_retiring=True)
    fake = FakePopen(42005, mode="converges")
    restored = replace(
        state.previous_meta,
        pid=fake.pid,
        pgid=fake.pid,
        sid=fake.pid,
        start_time_ticks=fake.pid * 10,
        token="restart-token",  # ruff: ignore[hardcoded-password-func-arg]
    )
    gated = dc.GatedWorker(
        proc=cast("subprocess.Popen[bytes]", fake), gate_writer=99, meta=restored
    )
    spawned: list[dc.GatedWorker] = []
    released: list[int] = []
    state_writes: list[dc.RollbackState] = []
    published: list[lifecycle.WorkerMeta] = []
    metadata_crash_pending = True

    monkeypatch.setattr(dc, "_checkout", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(dc, "read_meta_strict", lambda: None)
    monkeypatch.setattr(dc, "worker_alive", lambda meta: meta == restored)
    monkeypatch.setattr(dc, "check_postgres", lambda _timeout: True)
    monkeypatch.setattr(dc, "_wait_for_released_worker", lambda _meta: True)

    def spawn_gated(_state: dc.RollbackState, _previous: lifecycle.WorkerMeta) -> dc.GatedWorker:
        spawned.append(gated)
        return gated

    monkeypatch.setattr(dc, "_spawn_gated_previous_worker", spawn_gated)
    monkeypatch.setattr(dc, "_release_gate", released.append)
    monkeypatch.setattr(dc, "_write_state", state_writes.append)

    def write_meta(meta: lifecycle.WorkerMeta) -> None:
        nonlocal metadata_crash_pending
        if metadata_crash_pending:
            metadata_crash_pending = False
            msg = "simulated crash before worker metadata publication"
            raise OSError(msg)
        published.append(meta)

    monkeypatch.setattr(dc, "write_meta", write_meta)
    monkeypatch.setattr(deploy_cli, "remove_cli_root", lambda _commit: None)
    monkeypatch.setattr(deploy_cli, "reconcile_pointer", lambda _commit: True)

    with pytest.raises(OSError, match="before worker metadata publication"):
        dc._restore_previous_locked(state)

    assert spawned == [gated]
    assert released == [gated.gate_writer]
    assert published == []
    recovery = state_writes[-1]
    assert recovery.previous_restart_meta == restored
    assert recovery.previous_restart_released is True

    assert dc._restore_previous_locked(recovery)
    assert spawned == [gated]
    assert published == [restored]
    terminal = state_writes[-1]
    assert terminal.status == dc.STATUS_ROLLED_BACK
    assert terminal.previous_restart_meta is None
    assert terminal.previous_restart_released is False


def test_unreleased_previous_restart_is_converged_before_another_spawn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retry retires an anchored gated worker before permitting replacement."""
    base = pending_state(previous_retiring=True)
    restart = replace(
        base.previous_meta,
        pid=42006,
        pgid=42006,
        sid=42006,
        start_time_ticks=420060,
        token="unreleased-restart-token",  # ruff: ignore[hardcoded-password-func-arg]
    )
    state = replace(
        base,
        previous_restart_meta=restart,
        previous_restart_released=False,
    )
    restart_alive = True
    events: list[tuple[str, object]] = []

    monkeypatch.setattr(dc, "read_meta_strict", lambda: None)

    def alive(meta: lifecycle.WorkerMeta) -> bool:
        return restart_alive and meta == restart

    def stop(meta: lifecycle.WorkerMeta, _grace: float) -> bool:
        nonlocal restart_alive
        assert meta == restart
        events.append(("stop", meta))
        restart_alive = False
        return True

    def write_state(value: dc.RollbackState) -> None:
        events.append(("state", value))

    def spawn_gated(_state: dc.RollbackState, _previous: lifecycle.WorkerMeta) -> None:
        events.append(("spawn", _previous))

    monkeypatch.setattr(dc, "worker_alive", alive)
    monkeypatch.setattr(dc, "stop_worker", stop)
    monkeypatch.setattr(dc, "_write_state", write_state)
    monkeypatch.setattr(dc, "_spawn_gated_previous_worker", spawn_gated)

    assert dc.restart_previous(state) is None
    assert [kind for kind, _value in events] == ["stop", "state", "spawn"]
    cleared = cast("dc.RollbackState", events[1][1])
    assert cleared.previous_restart_meta is None
    assert cleared.previous_restart_released is False


def test_live_mismatched_durable_worker_blocks_legacy_restart(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A live durable worker outside rollback authority is never adopted."""
    state = pending_state(previous_retiring=True)
    current = replace(
        state.previous_meta,
        pid=42002,
        pgid=42002,
        sid=42002,
        start_time_ticks=420020,
        token=f"{state.previous_meta.token}-other",
        git_commit=state.commit,
    )
    monkeypatch.setattr(dc, "read_meta_strict", lambda: current)
    monkeypatch.setattr(dc, "worker_alive", lambda meta: meta is current)

    def forbid_spawn(*_args: object, **_kwargs: object) -> FakePopen:
        pytest.fail("mismatched live authority must block spawning")

    monkeypatch.setattr(subprocess, "Popen", forbid_spawn)
    assert dc.restart_previous(state) is None


def test_dead_durable_restored_metadata_allows_one_safe_restart(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Dead durable metadata does not prevent one verified replacement."""
    state = pending_state(previous_retiring=True)
    dead = replace(
        state.previous_meta,
        pid=42003,
        pgid=42003,
        sid=42003,
        start_time_ticks=420030,
        token=f"{state.previous_meta.token}-dead",
    )
    fake = FakePopen(42004, mode="converges")
    identity = ProcessIdentity(
        pid=fake.pid,
        pgid=fake.pid,
        sid=fake.pid,
        start_time_ticks=fake.pid * 10,
    )
    spawned: list[FakePopen] = []
    monkeypatch.setattr(dc, "read_meta_strict", lambda: dead)
    monkeypatch.setattr(
        dc,
        "worker_alive",
        lambda meta: meta.pid == fake.pid and meta.start_time_ticks == identity.start_time_ticks,
    )

    def spawn(*_args: object, **_kwargs: object) -> FakePopen:
        spawned.append(fake)
        return fake

    monkeypatch.setattr(subprocess, "Popen", spawn)
    monkeypatch.setattr(dc, "_wait_for_identity", lambda _proc: identity)
    monkeypatch.setattr(dc, "_release_gate", dc._close_gate)
    monkeypatch.setattr(dc, "_wait_for_released_worker", lambda _meta: True)
    monkeypatch.setattr(dc, "check_postgres", lambda _timeout: True)
    restored = dc.restart_previous(state)
    assert restored is not None
    assert restored.pid == fake.pid
    assert spawned == [fake]


def test_retiring_original_durable_worker_is_still_replaced(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The retirement marker still forces replacement of the original worker."""
    state = pending_state(previous_retiring=True)
    original = state.previous_meta
    stopped: list[lifecycle.WorkerMeta] = []
    monkeypatch.setattr(dc, "read_meta_strict", lambda: original)
    monkeypatch.setattr(dc, "worker_alive", lambda meta: meta is original)

    def stop(meta: lifecycle.WorkerMeta, _grace: float) -> bool:
        stopped.append(meta)
        return True

    monkeypatch.setattr(dc, "stop_worker", stop)

    def failed_spawn(*_args: object, **_kwargs: object) -> FakePopen:
        raise OSError

    monkeypatch.setattr(subprocess, "Popen", failed_spawn)
    assert dc.restart_previous(state) is None
    assert stopped == [original]


def test_controller_requests_must_be_json_objects() -> None:
    """Controller requests must be JSON objects; the type defaults to empty."""
    request = dc.parse_request('{"type": "status", "x": 1}')
    assert dc.request_type(request) == "status"
    assert not dc.request_type({})
    assert not dc.request_type({"type": 3})
    with pytest.raises(dc.DeployCtlError, match="not valid JSON"):
        dc.parse_request("{oops")
    with pytest.raises(dc.DeployCtlError, match="JSON object"):
        dc.parse_request("[1]")


def test_only_failed_checkout_reports_failure_via_exit_code() -> None:
    """Only a failed checkout reports failure via the exit code."""
    failed: dict[str, object] = {"ok": False}
    succeeded: dict[str, object] = {"ok": True}
    assert dc.checkout_failure_exit_code("checkout", failed) != 0
    assert dc.checkout_failure_exit_code("checkout", succeeded) == 0
    assert dc.checkout_failure_exit_code("confirm", failed) == 0
