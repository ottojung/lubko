"""Stop/kill convergence of runner-owned work.

A successful ``stop`` or ``kill`` must mean no accepted managed work from
before the command can start afterwards. When no invocation process group is
live but a committed runner reservation, a proven-live runner between
invocations, or an accepted pending prompt remains, stop/kill must cancel
exactly that owned work under the per-agent metadata lock, converge the exact
observed runner identity with positive death evidence, and fail closed when
convergence cannot be proven. The runner's spawn gate must likewise refuse to
start a prompt once a stop-like decision is durably recorded.
"""

from __future__ import annotations

import contextlib
import errno
import os
import pathlib
import shutil
import signal
import subprocess
import time
from typing import TYPE_CHECKING, Final

import pytest

from lubko import agent

if TYPE_CHECKING:
    from pathlib import Path

SLEEP_BIN: Final = shutil.which("sleep") or "/bin/sleep"


@pytest.fixture(autouse=True)
def state_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point the agent state root at a throwaway directory.

    Returns:
        The isolated Lubko state root.
    """
    state = tmp_path / "state"
    monkeypatch.setenv("XDG_STATE_HOME", str(state))
    return state


class _MarkedProcess:
    """A real test-owned process carrying one exact agent marker."""

    def __init__(self, aid: str) -> None:
        env = dict(os.environ)
        env["LUBKO_AGENT_ID"] = aid
        self.proc = subprocess.Popen(
            [SLEEP_BIN, "300"],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
            close_fds=True,
            env=env,
        )

    @property
    def pid(self) -> int:
        """The exact process ID."""
        return self.proc.pid

    def kill_and_reap(self) -> None:
        """Converge the exact test-owned process and reap it."""
        if self.proc.poll() is None:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(self.pid, signal.SIGKILL)
        self.proc.wait(timeout=5)


def _assert_runner_work_converged(meta: agent.Meta | None, mode: str) -> None:
    """Assert stop/kill left no runner-owned work that could later start.

    Args:
        meta: Freshly read agent metadata.
        mode: The applied stop-like intent (``stop`` or ``kill``).
    """
    assert meta is not None
    expected_state = "stopped" if mode == "stop" else "killed"
    assert meta["state"] == expected_state
    assert meta["stop_reason"] == mode
    assert meta["intent"] is None
    assert meta["pending_prompt"] is None
    assert meta["steer_queue"] == []
    assert meta["active_runner"] is False
    assert meta["runner_reservation"] is None
    assert not agent.reservation_in_flight(meta)


@pytest.mark.parametrize("mode", ["stop", "kill"])
def test_stop_kill_converges_reserved_runner_work(
    mode: str,
    state_dir: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Stop/kill cancels a committed runner reservation instead of false success.

    A prompt transition durably commits the accepted pending prompt and a
    reserved runner generation before any invocation process exists, so neither
    ``is_alive`` nor ``group_alive`` holds. Stop/kill must converge that owned
    work under the metadata lock; afterwards an exact-generation runner claim
    fails closed and can never start the cancelled prompt.
    """
    aid = "aaaaaaaa"
    meta = agent.idle_meta(aid, str(state_dir), None)
    meta["state"] = "running"
    meta["pending_prompt"] = "accepted early"
    meta["active_runner"] = True
    meta["runner_gen"] = 1
    meta["runner_reservation"] = {
        "gen": 1,
        "owner_pid": os.getpid(),
        "owner_start_ticks": agent.proc_start_ticks(os.getpid()),
        "state": "reserved",
        "reserved_at": time.time(),
        "mode": "new",
    }
    agent.write_meta(aid, meta)

    # No invocation process exists yet, but the reservation is fully live.
    assert not agent.is_alive(meta)
    assert not agent.group_alive(meta)
    assert agent.reservation_in_flight(meta)

    code = agent.main([mode, aid])
    assert code == agent.EXIT_OK
    out = capsys.readouterr().out
    assert ("stopped" if mode == "stop" else "killed") in out
    assert "already" not in out
    _assert_runner_work_converged(agent.read_meta(aid), mode)

    # An exact-generation runner arriving afterwards claims nothing and runs
    # nothing: the reservation was invalidated by the stop/kill.
    monkeypatch.setenv("LUBKO_RUNNER_GEN", "1")
    agent.runner(aid, "new")
    final = agent.read_meta(aid)
    _assert_runner_work_converged(final, mode)
    assert final is not None
    assert not (agent.agents_dir() / aid / "output.log").exists()


@pytest.mark.parametrize("mode", ["stop", "kill"])
def test_stop_kill_converges_live_runner_queued_prompt(
    mode: str,
    state_dir: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Stop/kill cancels a prompt queued for a proven-live runner.

    Between invocations a runner stays proven-live while no invocation group
    exists, and the prompt protocol accepts new work into ``pending_prompt``
    for exactly that runner. Stop/kill must cancel that queued work under the
    metadata lock, and the runner's own reclaim boundary must then go idle
    without starting it.
    """
    aid = "aaaaaaaa"
    proc = _MarkedProcess(aid)
    try:
        meta = agent.idle_meta(aid, str(state_dir), None)
        meta["state"] = "running"
        meta["pending_prompt"] = "queued for the live runner"
        meta["active_runner"] = True
        meta["runner_pid"] = proc.pid
        meta["runner_start_time"] = agent.proc_start_ticks(proc.pid)
        meta["runner_reservation"] = {
            "gen": 1,
            "owner_pid": os.getpid(),
            "owner_start_ticks": agent.proc_start_ticks(os.getpid()),
            "state": "claimed",
            "mode": "new",
        }
        agent.write_meta(aid, meta)

        assert agent.runner_alive(meta)
        assert not agent.is_alive(meta)
        assert not agent.group_alive(meta)

        code = agent.main([mode, aid])
        assert code == agent.EXIT_OK
        out = capsys.readouterr().out
        assert ("stopped" if mode == "stop" else "killed") in out
        assert "already" not in out
        converged = agent.read_meta(aid)
        _assert_runner_work_converged(converged, mode)

        # The runner's between-invocations boundary observes the durable stop
        # reason and goes idle instead of claiming the cancelled prompt, and
        # no invocation was ever recorded or executed for it. Success also
        # proves the exact observed runner was converged (actually gone).
        assert converged is not None
        assert agent._reclaim_prompt(aid) is False
        _assert_runner_work_converged(agent.read_meta(aid), mode)
        # poll() reaps the exact child, proving it was signalled to death.
        assert proc.proc.wait(timeout=5) is not None
        log_path = agent.agents_dir() / aid / "output.log"
        assert not log_path.exists()
    finally:
        proc.kill_and_reap()


@pytest.mark.parametrize("mode", ["stop", "kill"])
def test_stop_kill_fails_closed_when_runner_convergence_fails(
    mode: str,
    state_dir: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Stop/kill never reports success while the observed runner survives.

    If the exact recorded runner cannot be proven converged, the command
    fails closed instead of reporting false quiescence and leaves a coherent
    retryable record (intent and reason kept, cancelled work dropped so
    nothing new can claim).
    """
    aid = "aaaaaaaa"
    meta = agent.idle_meta(aid, str(state_dir), None)
    meta["state"] = "running"
    meta["pending_prompt"] = "queued for the live runner"
    meta["active_runner"] = True
    meta["runner_pid"] = 123456
    meta["runner_start_time"] = 42
    agent.write_meta(aid, meta)
    monkeypatch.setattr(agent, "_converge_observed_runner", lambda *_a, **_k: False)

    code = agent.main([mode, aid])
    assert code == agent.EXIT_ERROR
    captured = capsys.readouterr()
    assert "did not converge" in captured.err

    mid = agent.read_meta(aid)
    assert mid is not None
    assert mid["state"] == "running", "no false terminal record while unconverged"
    assert mid["intent"] == mode, "retryable stop-like intent kept"
    assert mid["stop_reason"] == mode
    assert mid["pending_prompt"] is None


class _FakeClock:
    """Deterministic clock replacing wall time and sleep in convergence."""

    def __init__(self) -> None:
        self.now = 0.0

    def time(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


@pytest.mark.parametrize("mode", ["stop", "kill"])
def test_runner_convergence_signals_and_verifies_exact_identity(
    mode: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Runner convergence signals the exact identity and verifies its death.

    ``stop`` escalates from SIGTERM to SIGKILL only after its grace period;
    ``kill`` uses SIGKILL immediately.  Success requires a verified-dead exact
    identity (start ticks plus agent marker); no wall time passes.
    """
    aid = "aaaaaaaa"
    observed: agent.Meta = {"runner_pid": 123456, "runner_start_time": 42, "id": aid}
    monkeypatch.setattr(agent, "STOP_WAIT_SECONDS", 0.1)
    monkeypatch.setattr(agent, "KILL_WAIT_SECONDS", 0.1)
    clock = _FakeClock()
    monkeypatch.setattr(time, "time", clock.time)
    monkeypatch.setattr(time, "sleep", clock.sleep)
    signals: list[int] = []

    def fake_signal(pid: int, ticks: object, sig: int, *, marker_aid: str | None) -> None:
        assert pid == 123456
        assert ticks == 42
        assert marker_aid == aid
        signals.append(sig)

    monkeypatch.setattr(agent, "signal_identity_checked", fake_signal)

    # The runner dies at the first verification: exactly one signal.
    states: list[str] = ["gone"]
    monkeypatch.setattr(agent, "_runner_identity_state", lambda *_a: states.pop(0))
    assert agent._converge_observed_runner(observed, mode) is True
    expected_grace = signal.SIGTERM if mode == "stop" else signal.SIGKILL
    assert signals == [expected_grace]

    # A surviving runner through stop's grace period is escalated to SIGKILL;
    # kill has no grace step to escalate from.
    signals.clear()
    if mode == "stop":
        state_iter = iter(["alive", "alive", "alive", "gone"])
        monkeypatch.setattr(agent, "_runner_identity_state", lambda *_a: next(state_iter))
        assert agent._converge_observed_runner(observed, mode) is True
        assert signals == [signal.SIGTERM, signal.SIGKILL]
        assert clock.now < 1.0, "no real waiting occurs"

    # A runner that survives everything fails closed without success.
    signals.clear()
    monkeypatch.setattr(agent, "_runner_identity_state", lambda *_a: "alive")
    assert agent._converge_observed_runner(observed, mode) is False

    # Unprovable identity (unreadable /proc data) is never counted as death:
    # the bounded attempt exhausts and fails closed even though every probe
    # answered.
    signals.clear()
    monkeypatch.setattr(agent, "_runner_identity_state", lambda *_a: "unprovable")
    assert agent._converge_observed_runner(observed, mode) is False


@pytest.mark.parametrize(
    ("runner_pid", "runner_start_time"),
    [
        (4242.9, 42),
        ("4242", 42),
        (True, 42),
        (0, 42),
        (-1, 42),
        (4242, "42"),
        (4242, 42.0),
        (4242, True),
        (4242, -1),
    ],
)
def test_runner_convergence_rejects_malformed_persisted_identity(
    runner_pid: object,
    runner_start_time: object,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Malformed durable runner identity never becomes signalling authority."""
    observed: agent.Meta = {
        "id": "aaaaaaaa",
        "runner_pid": runner_pid,
        "runner_start_time": runner_start_time,
    }

    def reject_signal(*_args: object, **_kwargs: object) -> None:
        pytest.fail("malformed runner identity reached signal_identity_checked")

    def reject_state(*_args: object, **_kwargs: object) -> str:
        pytest.fail("malformed runner identity reached positive-gone reasoning")

    monkeypatch.setattr(agent, "signal_identity_checked", reject_signal)
    monkeypatch.setattr(agent, "_runner_identity_state", reject_state)

    assert agent._converge_observed_runner(observed, "kill") is False


@pytest.mark.parametrize("force", [False, True])
@pytest.mark.parametrize(
    ("runner_pid", "runner_start_time"),
    [
        (4242.9, 42),
        ("4242", 42),
        (True, 42),
        (0, 42),
        (4242, "42"),
        (4242, 42.0),
        (4242, True),
    ],
)
def test_delete_convergence_rejects_malformed_persisted_runner_identity(
    runner_pid: object,
    runner_start_time: object,
    monkeypatch: pytest.MonkeyPatch,
    *,
    force: bool,
) -> None:
    """Delete keeps malformed present runner authority blocking."""
    meta: agent.Meta = {
        "id": "aaaaaaaa",
        "delete_pending": True,
        "runner_pid": runner_pid,
        "runner_start_time": runner_start_time,
    }
    monkeypatch.setattr(agent, "read_meta", lambda _aid: meta)

    def reject_signal(*_args: object, **_kwargs: object) -> None:
        pytest.fail("malformed runner identity reached signal_identity_checked")

    monkeypatch.setattr(agent, "signal_identity_checked", reject_signal)

    assert agent._converge_for_delete("aaaaaaaa", force=force, deadline=0.0) is False


def test_runner_identity_state_distinguishes_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Only positive evidence proves runner death; unreadable data cannot.

    An exited process or a PID provably reused by a different start-time
    identity counts as gone; a live process whose start ticks cannot be read
    stays unprovable and must keep convergence fail-closed.
    """
    aid = "aaaaaaaa"
    monkeypatch.setattr(agent, "pid_alive", lambda _pid: False)
    assert agent._runner_identity_state(123456, 42, aid) == "gone"

    monkeypatch.setattr(agent, "pid_alive", lambda _pid: True)
    monkeypatch.setattr(agent, "proc_start_ticks", lambda _pid: 999)
    assert agent._runner_identity_state(123456, 42, aid) == "gone"

    monkeypatch.setattr(agent, "proc_start_ticks", lambda _pid: None)
    assert agent._runner_identity_state(123456, 42, aid) == "unprovable"


def test_runner_identity_state_unprovable_on_environ_read_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A marker read failure on a matching live process is never death.

    When start ticks positively match but ``/proc/<pid>/environ`` raises, the
    identity is unprovable rather than gone, so convergence stays fail-closed
    instead of mistaking a transient ``/proc`` error for runner death.
    """
    aid = "aaaaaaaa"

    def _fail_read_bytes(_self: Path) -> bytes:
        raise OSError(errno.EACCES, "transient /proc failure")

    monkeypatch.setattr(agent, "pid_alive", lambda _pid: True)
    monkeypatch.setattr(agent, "proc_start_ticks", lambda _pid: 42)
    monkeypatch.setattr(pathlib.Path, "read_bytes", _fail_read_bytes)
    assert agent._runner_identity_state(123456, 42, aid) == "unprovable"


@pytest.mark.parametrize("mode", ["stop", "kill"])
def test_convergence_unprovable_then_gone_succeeds(
    mode: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A transiently unprovable runner that later reads as gone converges.

    Transient ``/proc`` failures delay but never fake convergence: within the
    bounded window an eventually readable identity that proves gone yields
    success with exactly one signal delivered.
    """
    observed: agent.Meta = {"runner_pid": 123456, "runner_start_time": 42, "id": "aaaaaaaa"}
    monkeypatch.setattr(agent, "STOP_WAIT_SECONDS", 0.2)
    monkeypatch.setattr(agent, "KILL_WAIT_SECONDS", 0.2)
    clock = _FakeClock()
    monkeypatch.setattr(time, "time", clock.time)
    monkeypatch.setattr(time, "sleep", clock.sleep)
    signals: list[int] = []

    def fake_signal(pid: int, ticks: object, sig: int, *, marker_aid: str | None) -> None:
        del pid, ticks, marker_aid
        signals.append(sig)

    monkeypatch.setattr(agent, "signal_identity_checked", fake_signal)
    state_iter = iter(["unprovable"] * 3 + ["gone"])
    monkeypatch.setattr(agent, "_runner_identity_state", lambda *_a: next(state_iter))
    assert agent._converge_observed_runner(observed, mode) is True
    assert signals == [signal.SIGTERM if mode == "stop" else signal.SIGKILL]


@pytest.mark.parametrize("mode", ["stop", "kill"])
def test_invocation_spawn_gate_refuses_cancelled_prompt(
    mode: str,
    state_dir: Path,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A stale in-flight runner can never spawn a cancelled pending prompt.

    A runner may have already read ``pending_prompt`` when a concurrent
    stop/kill durably cancels it under the lock. The spawn gate re-checks the
    durable stop-like intent/reason under the same lock before any process is
    started, so whichever side wins the lock decides and the cancelled prompt
    never executes.
    """
    aid = "aaaaaaaa"
    sentinel = tmp_path / "sentinel"

    def fake_command(_meta: agent.Meta, _prompt: str, *, is_continue: bool) -> list[str]:
        del is_continue
        return ["sh", "-c", f"touch {sentinel}; sleep 300"]

    meta = agent.idle_meta(aid, str(state_dir), None)
    meta["state"] = "running"
    meta["pending_prompt"] = "read before cancellation"
    agent.write_meta(aid, meta)

    # The concurrent stop/kill wins the lock and cancels the accepted work.
    assert agent.main([mode, aid]) == agent.EXIT_OK
    capsys.readouterr()

    ctx = agent._RunnerContext(
        aid=aid,
        log_path=agent.agents_dir() / aid / "output.log",
        cwd=str(tmp_path),
        env=dict(os.environ),
    )
    assert agent._run_invocation(ctx, "read before cancellation", is_continue=False) is None
    assert not sentinel.exists(), "cancelled prompt must never be spawned"
    result = agent.read_meta(aid)
    assert result is not None
    assert result["state"] == ("stopped" if mode == "stop" else "killed")


@pytest.mark.parametrize("bad_id", [123, True, "", "ABC", []])
def test_runner_convergence_rejects_malformed_persisted_agent_id(
    bad_id: object,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Malformed durable agent IDs cannot become runner marker authority."""
    observed: agent.Meta = {
        "id": bad_id,
        "runner_pid": 4242,
        "runner_start_time": 777,
    }

    def reject_signal(*_args: object, **_kwargs: object) -> None:
        pytest.fail("malformed agent id reached runner signalling")

    def reject_state(*_args: object, **_kwargs: object) -> str:
        pytest.fail("malformed agent id reached runner identity proof")

    monkeypatch.setattr(agent, "signal_identity_checked", reject_signal)
    monkeypatch.setattr(agent, "_runner_identity_state", reject_state)

    assert agent._converge_observed_runner(observed, "kill") is False


@pytest.mark.parametrize("bad_id", [123, True, "", "ABC", []])
def test_forced_delete_rejects_malformed_persisted_agent_id(
    bad_id: object,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Forced delete cannot stringify malformed durable IDs into authority."""
    meta: agent.Meta = {
        "id": bad_id,
        "delete_pending": True,
        "runner_pid": 4242,
        "runner_start_time": 777,
    }
    monkeypatch.setattr(agent, "read_meta", lambda _aid: meta)

    def reject_signal(*_args: object, **_kwargs: object) -> None:
        pytest.fail("malformed agent id reached forced-delete runner signalling")

    monkeypatch.setattr(agent, "signal_identity_checked", reject_signal)

    assert agent._converge_for_delete("aaaaaaaa", force=True, deadline=0.0) is False
