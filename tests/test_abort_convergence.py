"""Exact-identity safety of abnormal runner cleanup.

Abnormal cleanup paths (``_abort_runner`` and the exceptional
``_spawn_and_run``/unrecorded-invocation convergence) must never signal from a
numeric PID/PGID alone: the durable ``(pid, start_time, invocation_id)``
identity is re-proven under a kernel-stable pin at signal time, ownership that
cannot be proven fails closed, and genuine owned descendants are still
converged.
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
import types
from typing import TYPE_CHECKING

import pytest

from lubko import agent

if TYPE_CHECKING:
    from pathlib import Path

SLEEP_BIN: str = shutil.which("sleep") or "/bin/sleep"


@pytest.fixture(autouse=True)
def state_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point the agent state root at a throwaway directory.

    Returns:
        The isolated Lubko state root.
    """
    state = tmp_path / "state"
    monkeypatch.setenv("XDG_STATE_HOME", str(state))
    return state


def _running_meta(aid: str, pid: int, start: object, iid: str) -> agent.Meta:
    """Return running-agent metadata recording one exact invocation identity."""
    return {
        "id": aid,
        "state": "running",
        "pid": pid,
        "pgid": pid,
        "start_time": start,
        "invocation_id": iid,
        "active_runner": True,
    }


def test_abort_runner_signals_via_exact_identity_helper(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Abnormal runner cleanup re-proves exact invocation ownership at signal time.

    ``_abort_runner`` must delegate to the kernel-stable exact-identity group
    signalling with the full durable identity, never a bare numeric
    ``killpg``, before finalizing terminal metadata.
    """
    aid = "aaaaaaaa"
    iid = "inv-1"
    meta = _running_meta(aid, 424242, 111, iid)
    agent.write_meta(aid, meta)

    seen: list[tuple[agent.Meta, int]] = []
    monkeypatch.setattr(agent, "send_signal_group", lambda m, sig: seen.append((m, sig)))
    monkeypatch.setattr(agent, "wait_group_dead", lambda _m, _t: True)

    agent._abort_runner(aid)

    assert len(seen) == 1
    signalled, sig = seen[0]
    assert sig == signal.SIGKILL
    assert signalled["pid"] == 424242
    assert signalled["start_time"] == 111
    assert signalled["invocation_id"] == iid
    assert signalled["pgid"] == 424242
    final = agent.read_meta(aid)
    assert final is not None
    assert final["state"] == "failed"
    assert final["active_runner"] is False


def test_abort_runner_holds_safety_when_death_unproven(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unproven convergence leaves a nonterminal hold that blocks replacement.

    When exact group death cannot be positively proven after the identity-
    exact signal, the agent must stay ``running`` with a stop-like intent and
    an active runner authority, so neither a new prompt claim nor a new runner
    can start replacement work.
    """
    aid = "aaaaaaaa"
    iid = "inv-hold"
    agent.write_meta(aid, _running_meta(aid, 424242, 111, iid))
    monkeypatch.setattr(agent, "send_signal_group", lambda _m, _sig: None)
    monkeypatch.setattr(agent, "wait_group_dead", lambda _m, _t: False)

    agent._abort_runner(aid)

    final = agent.read_meta(aid)
    assert final is not None
    assert final["state"] == "running", "no terminal record while unconverged"
    assert final["intent"] == "kill"
    assert final["active_runner"] is True
    assert agent._claim_pending_prompt(aid, "replacement prompt") is False


def test_abort_runner_never_terminalizes_newer_invocation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A concurrently recorded newer invocation is never aborted or held.

    If metadata moved on to a different invocation while cleanup ran, neither
    the terminal failure nor the safety hold may be applied to the newer
    record.
    """
    aid = "aaaaaaaa"
    old = _running_meta(aid, 424242, 111, "inv-old")
    agent.write_meta(aid, old)

    def steal_record(_m: agent.Meta, _t: float) -> bool:
        # A newer invocation is concurrently recorded while cleanup runs.
        newer = _running_meta(aid, 535353, 222, "inv-new")
        newer["runner_pid"] = 777
        newer["runner_start_time"] = 888
        agent.write_meta(aid, newer)
        return True

    monkeypatch.setattr(agent, "send_signal_group", lambda _m, _sig: None)
    monkeypatch.setattr(agent, "wait_group_dead", steal_record)

    agent._abort_runner(aid)

    final = agent.read_meta(aid)
    assert final is not None
    assert final["state"] == "running"
    assert final.get("intent") is None
    assert final["pid"] == 535353
    assert final["invocation_id"] == "inv-new"


def test_send_signal_group_never_signals_reused_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A recycled PID hosting a different identity is never group-signalled.

    When the pinned occupant of the recorded PID carries a different start
    time / invocation identity, neither the leader ``killpg`` nor any member
    signal may fire: fail closed instead of guessing.
    """
    meta = _running_meta("aaaaaaaa", 424242, 111, "inv-1")
    pin_fd = os.open(os.devnull, os.O_RDONLY)
    monkeypatch.setattr(agent, "open_pidfd", lambda _pid: os.dup(pin_fd))
    monkeypatch.setattr(agent, "proc_start_ticks", lambda _pid: 999)
    monkeypatch.setattr(os, "killpg", lambda *_a: pytest.fail("killpg fired on reused PID"))
    monkeypatch.setattr(os, "kill", lambda *_a: pytest.fail("member signal fired"))
    agent.send_signal_group(meta, signal.SIGKILL)
    os.close(pin_fd)


def test_send_signal_group_converges_owned_descendants_after_leader_exit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Genuine surviving descendants of the failed invocation are converged.

    With the recorded leader gone, each surviving group member carrying both
    the exact agent marker and the exact invocation marker is signalled
    individually through its own pinned descriptor; foreign processes are
    left alone and no numeric signal fires.
    """
    aid = "aaaaaaaa"
    iid = "inv-1"
    meta = _running_meta(aid, 424242, 111, iid)
    owned_fd, foreign_fd = 21, 22

    def fake_members(pgid: int, got_aid: str, got_iid: str) -> list[tuple[int, int]]:
        assert pgid == 424242
        assert got_aid == aid
        assert got_iid == iid
        return [(5001, owned_fd), (6000, foreign_fd)]

    delivered: list[tuple[int, int]] = []
    closed: list[int] = []
    monkeypatch.setattr(agent, "open_pidfd", lambda _pid: None)
    monkeypatch.setattr(agent, "_pinned_invocation_members", fake_members)
    monkeypatch.setattr(agent, "pidfd_send_signal", lambda fd, sig: delivered.append((fd, sig)))
    monkeypatch.setattr(os, "kill", lambda *_a: pytest.fail("numeric kill fired"))
    monkeypatch.setattr(os, "killpg", lambda *_a: pytest.fail("numeric killpg fired"))
    monkeypatch.setattr(os, "close", closed.append)
    agent.send_signal_group(meta, signal.SIGKILL)

    assert delivered == [(owned_fd, signal.SIGKILL), (foreign_fd, signal.SIGKILL)]
    assert sorted(closed) == sorted([owned_fd, foreign_fd])


def test_send_signal_group_delivers_live_leader_through_its_pin_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A proven live leader is signalled through its pinned descriptor only.

    Even when every proof succeeds, the recorded PID and PGID may have been
    recycled into an unrelated session between proof and delivery: numeric
    ``killpg``/``kill`` could retarget onto that occupant. Delivery must go
    exclusively through ``pidfd_send_signal`` on the descriptor that pinned
    the verified leader.
    """
    aid = "aaaaaaaa"
    iid = "inv-live"
    meta = _running_meta(aid, 424242, 111, iid)
    closed: list[int] = []
    monkeypatch.setattr(agent, "open_pidfd", lambda pid: 77 if pid == 424242 else None)
    monkeypatch.setattr(agent, "proc_start_ticks", lambda _pid: 111)
    monkeypatch.setattr(agent, "env_has_marker", lambda _pid, _aid: True)
    monkeypatch.setattr(agent, "env_has_invocation", lambda _pid, _iid: True)
    monkeypatch.setattr(
        pathlib.Path,
        "iterdir",
        lambda _self: iter([]),
    )
    monkeypatch.setattr(os, "killpg", lambda *_a: pytest.fail("numeric killpg fired"))
    monkeypatch.setattr(os, "kill", lambda *_a: pytest.fail("numeric kill fired"))
    monkeypatch.setattr(os, "close", closed.append)
    delivered: list[tuple[int, int]] = []
    monkeypatch.setattr(
        agent,
        "pidfd_send_signal",
        lambda fd, sig: delivered.append((fd, sig)),
    )
    agent.send_signal_group(meta, signal.SIGKILL)

    assert delivered == [(77, signal.SIGKILL)]
    assert closed == [77]


def test_send_signal_group_member_path_survives_numeric_reuse_after_proof(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A member whose numeric PID was recycled after its proof is never hit.

    Membership was already proven under the member's own pin; by delivery time
    the numeric PID slot may host an innocent process. Delivery must address
    the pinned kernel process itself, never the recycled number.
    """
    aid = "aaaaaaaa"
    iid = "inv-m"
    meta = _running_meta(aid, 424242, 111, iid)
    member_fd = os.open(os.devnull, os.O_RDONLY)
    try:
        monkeypatch.setattr(agent, "open_pidfd", lambda _pid: None)
        monkeypatch.setattr(
            agent,
            "_pinned_invocation_members",
            lambda _pgid, _aid, _iid: [(7777, member_fd)],
        )
        # Simulate full reuse: the number 7777 now resolves to an unrelated
        # occupant; any numeric delivery here would signal that occupant.
        monkeypatch.setattr(os, "kill", lambda *_a: pytest.fail("numeric kill fired"))
        monkeypatch.setattr(os, "killpg", lambda *_a: pytest.fail("numeric killpg fired"))
        delivered: list[tuple[int, int]] = []
        monkeypatch.setattr(
            agent,
            "pidfd_send_signal",
            lambda fd, sig: delivered.append((fd, sig)),
        )
        # Record every close while still performing it for real, so the
        # production path genuinely closes the pidfd it owns.
        closed: list[int] = []
        real_close = os.close

        def recording_close(fd: int) -> None:
            real_close(fd)
            closed.append(fd)

        monkeypatch.setattr(os, "close", recording_close)
        agent.send_signal_group(meta, signal.SIGKILL)
        assert delivered == [(member_fd, signal.SIGKILL)]
        assert closed == [member_fd]
        assert not _fd_open(member_fd), "owned pin must be truly closed"
    finally:
        with contextlib.suppress(OSError):
            os.close(member_fd)


def _fd_open(fd: int) -> bool:
    """Return whether ``fd`` still refers to an open file description.

    Args:
        fd: Descriptor number to probe.

    Returns:
        ``True`` when the descriptor is still open.
    """
    try:
        os.fstat(fd)
    except OSError:
        return False
    return True


def test_send_signal_group_fails_closed_without_invocation_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No durable invocation identity means nothing is ever signalled."""
    meta = _running_meta("aaaaaaaa", 424242, 111, "inv-z")
    del meta["invocation_id"]
    monkeypatch.setattr(agent, "open_pidfd", lambda _pid: None)
    monkeypatch.setattr(
        agent,
        "_pinned_invocation_members",
        lambda *_a: pytest.fail("member scan without invocation identity"),
    )
    monkeypatch.setattr(os, "killpg", lambda *_a: pytest.fail("numeric killpg fired"))
    monkeypatch.setattr(os, "kill", lambda *_a: pytest.fail("numeric kill fired"))
    agent.send_signal_group(meta, signal.SIGKILL)


def _observable_ticks(pid: int) -> int:
    """Return the process's start ticks once positively observable in /proc.

    ``Popen`` returns before fork+exec completes, so identity data may not be
    readable yet; exact signalling must fail closed in that window and tests
    must wait for bounded observability instead of racing it.

    Args:
        pid: The freshly spawned process ID.

    Fails the test when the process never became observable.

    Returns:
        The observed start time in clock ticks.
    """
    deadline = time.time() + 5
    while True:
        ticks = agent.proc_start_ticks(pid)
        if ticks is not None:
            return ticks
        if time.time() > deadline:
            pytest.fail(f"process {pid} never became observable")
        time.sleep(0.01)


class _MarkedProcess:
    """A real test-owned process carrying one exact agent marker."""

    def __init__(self, aid: str, iid: str | None = None) -> None:
        env = dict(os.environ)
        env["LUBKO_AGENT_ID"] = aid
        self.iid = iid
        if iid is not None:
            env[agent.INVOCATION_ID_VAR] = iid
        self.proc = subprocess.Popen(
            [SLEEP_BIN, "300"],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
            close_fds=True,
            env=env,
        )
        # Bounded readiness barrier: Popen returns before fork+exec completes,
        # so start ticks, environment markers, and session identity are not
        # yet observable. Exact signalling must fail closed in that window;
        # tests therefore wait until every recorded identity datum is
        # positively observable through /proc before exposing the process.
        deadline = time.time() + 5
        while True:
            ticks = agent.proc_start_ticks(self.pid)
            ready = (
                ticks is not None
                and agent.env_has_marker(self.pid, aid)
                and (iid is None or agent.env_has_invocation(self.pid, iid))
                and os.getpgid(self.pid) == self.pid
            )
            if ready:
                self.start_ticks = ticks
                break
            if time.time() > deadline:
                failure = TimeoutError(f"marked process {self.pid} never became observable")
                raise failure
            time.sleep(0.01)

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


def test_send_signal_group_kills_real_live_leader_and_spares_recycled_slot() -> None:
    """End-to-end: an exact live leader is group-killed; a stale one is not.

    A real marked child acting as invocation A is killed through its recorded
    identity. Re-running against metadata that names a PID slot occupied by an
    unrelated marked process with a different identity delivers no signal to
    that occupant.
    """
    aid = "aaaaaaaa"
    owner = _MarkedProcess(aid, "inv-a")
    try:
        live = _running_meta(aid, owner.pid, agent.proc_start_ticks(owner.pid), "inv-a")
        agent.send_signal_group(live, signal.SIGKILL)
        assert owner.proc.wait(timeout=5) is not None

        impostor = _MarkedProcess("bbbbbbbb", "inv-b")
        try:
            stale = _running_meta(aid, impostor.pid, 1, "inv-a")
            agent.send_signal_group(stale, signal.SIGKILL)
            assert impostor.proc.poll() is None, "recycled occupant must survive"
        finally:
            impostor.kill_and_reap()
    finally:
        owner.kill_and_reap()


def test_spawn_and_run_exceptional_cleanup_uses_exact_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The exceptional ``_spawn_and_run`` cleanup cannot go stale before signalling.

    When waiting for the invocation raises into the abnormal-exit handler, the
    convergence goes through the exact-identity helper with the spawn-time
    ``(aid, pid, start, iid)`` evidence, and no bare group signal fires.
    """
    aid = "aaaaaaaa"
    iid = "inv-x"

    def boom(_proc: subprocess.Popen[bytes], _aid: str, *, is_continue: bool) -> int:
        del is_continue
        failure = RuntimeError("injected runner failure")
        raise failure

    monkeypatch.setattr(agent, "_wait_for_invocation_exit", boom)
    bare_killpg: list[tuple[int, int]] = []

    def guard_killpg(pgid: int, sig: int) -> None:
        bare_killpg.append((pgid, sig))

    monkeypatch.setattr(os, "killpg", guard_killpg)
    seen: list[tuple[str, int, object, str]] = []
    monkeypatch.setattr(
        agent,
        "_kill_spawned_invocation",
        lambda a, p, s, i: seen.append((a, p, s, i)),
    )
    monkeypatch.setattr(agent, "wait_group_dead", lambda _m, _t: True)
    seed = agent.idle_meta(aid, str(tmp_path), None)
    seed["state"] = "running"
    seed["invocation_id"] = iid
    seed["runner_pid"] = os.getpid()
    seed["runner_start_time"] = agent.proc_start_ticks(os.getpid())
    agent.write_meta(aid, seed)

    ctx = agent._RunnerContext(
        aid=aid,
        log_path=tmp_path / "output.log",
        cwd=str(tmp_path),
        env=dict(os.environ),
    )
    with pytest.raises(RuntimeError):
        agent._spawn_and_run(ctx, aid, [SLEEP_BIN, "0"], iid, is_continue=False)

    assert len(seen) == 1
    got_aid, _got_pid, got_start, got_iid = seen[0]
    assert got_aid == aid
    assert got_iid == iid
    assert isinstance(got_start, int)
    assert bare_killpg == [], "no bare group signal may fire"
    final = agent.read_meta(aid)
    assert final is not None
    assert final["state"] == "failed"


def test_unrecorded_invocation_cleanup_uses_exact_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The stop-race loser is converged through the exact-identity helper too."""
    aid = "aaaaaaaa"
    iid = "inv-y"

    monkeypatch.setattr(
        agent,
        "_record_running",
        lambda _proc, _start, _iid, blocked: lambda _m: blocked.update(stopped=True),
    )
    seen: list[tuple[str, int, object, str]] = []
    monkeypatch.setattr(
        agent,
        "_kill_spawned_invocation",
        lambda a, p, s, i: seen.append((a, p, s, i)),
    )
    seed = agent.idle_meta(aid, str(tmp_path), None)
    seed["state"] = "running"
    seed["invocation_id"] = iid
    seed["runner_pid"] = os.getpid()
    seed["runner_start_time"] = agent.proc_start_ticks(os.getpid())
    agent.write_meta(aid, seed)

    ctx = agent._RunnerContext(
        aid=aid,
        log_path=tmp_path / "output.log",
        cwd=str(tmp_path),
        env=dict(os.environ),
    )
    rc = agent._spawn_and_run(ctx, aid, [SLEEP_BIN, "0"], iid, is_continue=False)
    assert rc is None
    assert len(seen) == 1
    got_aid, _, _, got_iid = seen[0]
    assert got_aid == aid
    assert got_iid == iid


def test_spawn_and_run_exceptional_cleanup_never_touches_newer_invocation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exceptional cleanup applies its mutation only to the exact invocation.

    If a newer invocation was concurrently recorded before the abnormal-exit
    cleanup finalizes, neither the terminal failure nor the hold may land on
    the newer record.
    """
    aid = "aaaaaaaa"
    iid = "inv-old"
    seed = agent.idle_meta(aid, str(tmp_path), None)
    seed["state"] = "running"
    seed["invocation_id"] = iid
    seed["runner_pid"] = os.getpid()
    seed["runner_start_time"] = agent.proc_start_ticks(os.getpid())
    agent.write_meta(aid, seed)

    def steal_record(_m: agent.Meta, _t: float) -> bool:
        newer = dict(seed)
        newer["pid"] = 535353
        newer["pgid"] = 535353
        newer["start_time"] = 222
        newer["invocation_id"] = "inv-new"
        agent.write_meta(aid, newer)
        return True

    monkeypatch.setattr(agent, "wait_group_dead", steal_record)

    def boom(_proc: subprocess.Popen[bytes], _aid: str, *, is_continue: bool) -> int:
        del is_continue
        failure = RuntimeError("injected runner failure")
        raise failure

    monkeypatch.setattr(agent, "_wait_for_invocation_exit", boom)
    ctx = agent._RunnerContext(
        aid=aid,
        log_path=tmp_path / "output.log",
        cwd=str(tmp_path),
        env=dict(os.environ),
    )
    with pytest.raises(RuntimeError):
        agent._spawn_and_run(ctx, aid, [SLEEP_BIN, "0"], iid, is_continue=False)

    final = agent.read_meta(aid)
    assert final is not None
    assert final["invocation_id"] == "inv-new", "newer record must stay untouched"
    assert final["state"] == "running"
    assert final["intent"] is None


def test_spawn_and_run_unprovable_live_child_records_hold_without_wedging(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A live child with unprovable ownership cannot wedge the cleanup.

    When the exact-identity signal fails closed and the spawned child stays
    live, the bounded reap gives up, and a durable nonterminal safety hold is
    recorded instead of blocking forever or terminalizing.
    """
    aid = "aaaaaaaa"
    iid = "inv-live"
    seed = agent.idle_meta(aid, str(tmp_path), None)
    seed["state"] = "running"
    seed["invocation_id"] = iid
    seed["runner_pid"] = os.getpid()
    seed["runner_start_time"] = agent.proc_start_ticks(os.getpid())
    agent.write_meta(aid, seed)

    # Exact signalling fails closed: no signal reaches the child.
    monkeypatch.setattr(agent, "_kill_spawned_invocation", lambda *_a: None)
    monkeypatch.setattr(agent, "ABORT_REAP_SECONDS", 0.05)
    monkeypatch.setattr(os, "killpg", lambda *_a: pytest.fail("bare killpg fired"))

    def boom(_proc: subprocess.Popen[bytes], _aid: str, *, is_continue: bool) -> int:
        del is_continue
        failure = RuntimeError("injected runner failure")
        raise failure

    monkeypatch.setattr(agent, "_wait_for_invocation_exit", boom)

    ctx = agent._RunnerContext(
        aid=aid,
        log_path=tmp_path / "output.log",
        cwd=str(tmp_path),
        env=dict(os.environ),
    )
    with pytest.raises(RuntimeError):
        agent._spawn_and_run(ctx, aid, [SLEEP_BIN, "300"], iid, is_continue=False)

    child_pid = agent.read_meta(aid)
    assert child_pid is not None
    victim = int(child_pid["pid"])
    try:
        assert agent.pid_alive(victim), "child must still be live (fail-closed, no signal)"
        final = child_pid
        assert final["state"] == "running", "no terminal record while child survives"
        assert final["intent"] == "kill"
        assert final["active_runner"] is True
        assert final["invocation_id"] == iid
        assert agent._claim_pending_prompt(aid, "replacement prompt") is False
    finally:
        os.kill(victim, signal.SIGKILL)
        os.waitpid(victim, 0)


def test_unrecorded_invocation_live_child_keeps_blocking_hold(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A live stop-race loser with unprovable ownership cannot wedge or unblock.

    When exact signalling fails closed and the direct child survives, the
    bounded reap gives up, the call returns boundedly, existing stop/kill
    semantics stay untouched, and durable blocking authority prevents
    replacement work while the child lives.
    """
    aid = "aaaaaaaa"
    iid = "inv-loser"
    seed = agent.idle_meta(aid, str(tmp_path), None)
    seed["state"] = "running"
    seed["intent"] = "stop"
    agent.write_meta(aid, seed)

    monkeypatch.setattr(agent, "_kill_spawned_invocation", lambda *_a: None)
    monkeypatch.setattr(agent, "ABORT_REAP_SECONDS", 0.05)
    monkeypatch.setattr(os, "killpg", lambda *_a: pytest.fail("bare killpg fired"))

    proc = subprocess.Popen(
        [SLEEP_BIN, "300"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        close_fds=True,
    )
    try:
        start = _observable_ticks(proc.pid)
        agent._kill_unrecorded_invocation(aid, proc, start, iid)

        assert agent.pid_alive(proc.pid), "child must survive (fail-closed, no signal)"
        final = agent.read_meta(aid)
        assert final is not None
        assert final["state"] == "running", "stop/kill decision must not be overwritten"
        assert final["intent"] == "stop", "existing stop-like intent preserved"
        assert agent._claim_pending_prompt(aid, "replacement prompt") is False
    finally:
        proc.kill()
        proc.wait(timeout=5)


def test_unrecorded_invocation_dead_child_leaves_stop_semantics_alone(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A converged loser adds no metadata: stop/kill owns the lifecycle."""
    aid = "aaaaaaaa"
    iid = "inv-dead"
    seed = agent.idle_meta(aid, str(tmp_path), None)
    seed["state"] = "running"
    seed["intent"] = "kill"
    agent.write_meta(aid, seed)

    proc = subprocess.Popen([SLEEP_BIN, "0"])
    proc.wait(timeout=5)
    monkeypatch.setattr(os, "killpg", lambda *_a: pytest.fail("bare killpg fired"))
    # The child intentionally exited before cleanup: post-exit ticks are
    # legitimately unobservable, and convergence must not depend on them.
    agent._kill_unrecorded_invocation(aid, proc, agent.proc_start_ticks(proc.pid), iid)

    final = agent.read_meta(aid)
    assert final is not None
    assert final["state"] == "running"
    assert final["intent"] == "kill"


def _decide_with_marker(meta: agent.Meta) -> tuple[agent.Meta, dict[str, object]]:
    """Run one locked prompt decision against ``meta``.

    Args:
        meta: Agent metadata carrying an unresolved-child record.

    Returns:
        The mutated metadata and the recorded decision.
    """
    decision: dict[str, object] = {}
    agent._decide_invocation(meta, decision, prompt="replacement", steer=False)
    return meta, decision


def test_unresolved_child_ambiguous_inspection_blocks_prompt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unreadable procfs keeps the unresolved-child block; nothing fails open.

    When the recorded PID still exists but its start time cannot be read,
    death is not proven: the later prompt stays busy and the durable marker
    persists.
    """
    marker = {
        "pid": 424242,
        "pgid": 424242,
        "start_time": 111,
        "invocation_id": "inv-u",
    }
    meta: agent.Meta = {"id": "aaaaaaaa", "state": "stopped", "stop_reason": "stop"}
    meta["unresolved_invocation"] = marker
    monkeypatch.setattr(os, "kill", lambda _pid, _sig: None)
    monkeypatch.setattr(agent, "proc_start_ticks", lambda _pid: None)

    meta, decision = _decide_with_marker(meta)

    assert decision["action"] == "busy"
    assert meta["unresolved_invocation"] == marker, "marker must persist"


def test_unresolved_child_malformed_marker_blocks_and_persists() -> None:
    """Corrupt persisted obligation state fails closed instead of erasing it.

    A present-but-malformed ``unresolved_invocation`` mapping — wrong shape,
    invalid PID, wrong-type start time, or missing/empty invocation ID — is
    treated as ambiguous: the later prompt is blocked and the marker is never
    cleared, even when a naive type-coercing comparison would call the
    identity "recycled".
    """
    cases = [
        {"start_time": 111},  # missing pid and invocation_id
        {"pid": 0, "start_time": 111, "invocation_id": "inv-u"},
        {"pid": -5, "start_time": 111, "invocation_id": "inv-u"},
        {"pid": "424242", "start_time": 111, "invocation_id": "inv-u"},
        {"pid": 424242, "pgid": 424242, "start_time": "111", "invocation_id": "inv-u"},
        {"pid": 424242, "pgid": 424242, "start_time": None, "invocation_id": "inv-u"},
        {"pid": 424242, "start_time": 111, "invocation_id": "inv-u"},  # missing pgid
        {"pid": 424242, "pgid": None, "start_time": 111, "invocation_id": "inv-u"},
        {"pid": 424242, "pgid": 424242, "start_time": 111},
        {"pid": 424242, "pgid": 424242, "start_time": 111, "invocation_id": ""},
    ]
    for broken in cases:
        meta: agent.Meta = {"id": "aaaaaaaa", "state": "stopped", "stop_reason": "stop"}
        meta["unresolved_invocation"] = broken
        meta_after, decision = _decide_with_marker(meta)
        assert decision["action"] == "busy", f"malformed {broken!r} must block"
        assert meta_after["unresolved_invocation"] == broken, (
            f"malformed {broken!r} must never be cleared"
        )


def test_unresolved_child_proven_recycled_clears_block() -> None:
    """A readable different start time positively proves recycling and clears."""
    marker = {
        "pid": 424242,
        "pgid": 424242,
        "start_time": 111,
        "invocation_id": "inv-u",
    }
    meta: agent.Meta = {"id": "aaaaaaaa", "state": "stopped", "stop_reason": "stop"}
    meta["unresolved_invocation"] = marker
    patcher = pytest.MonkeyPatch()
    patcher.setattr(os, "kill", lambda _pid, _sig: None)
    patcher.setattr(agent, "proc_start_ticks", lambda _pid: 999)
    patcher.setattr(agent, "_pinned_invocation_members", lambda *_a: [])
    try:
        meta, decision = _decide_with_marker(meta)
        assert decision["action"] != "busy"
        assert meta["unresolved_invocation"] is None
    finally:
        patcher.undo()


def test_delete_honors_unresolved_child_until_exactly_proven_gone(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Deletion cannot remove state while an unrecorded loser stays alive.

    A tombstoned agent whose leaked spawn-gate loser could not be exactly
    signalled must fail closed for both forced and non-forced deletion,
    keep its metadata and marker, accept only exact-identity-safe
    signalling, and converge once the child's death is positively proven.
    """
    aid = "aaaaaaaa"
    iid = "inv-del"
    seed = agent.idle_meta(aid, str(tmp_path), None)
    seed["state"] = "killed"
    seed["stop_reason"] = "kill"
    seed["intent"] = None
    seed["delete_pending"] = True
    agent.write_meta(aid, seed)

    proc = subprocess.Popen(
        [SLEEP_BIN, "300"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        close_fds=True,
    )
    try:
        marker = {
            "pid": proc.pid,
            "pgid": proc.pid,
            "start_time": _observable_ticks(proc.pid),
            "invocation_id": iid,
        }
        seed["unresolved_invocation"] = marker
        agent.write_meta(aid, seed)

        monkeypatch.setattr(os, "killpg", lambda *_a: pytest.fail("bare killpg fired"))

        # Non-forced deletion must not signal at all and must fail closed.
        def refuse_signal(_m: agent.Meta, _sig: int) -> None:
            pytest.fail("non-forced delete must never signal")

        monkeypatch.setattr(agent, "send_signal_group", refuse_signal)
        assert agent._converge_for_delete(aid, force=False, deadline=time.time() + 0.05) is False
        kept = agent.read_meta(aid)
        assert kept is not None
        assert kept["delete_pending"] is True
        assert kept["unresolved_invocation"] == marker

        # Forced deletion signals only through the exact-identity helper and
        # still fails closed while the child survives.
        signalled: list[tuple[int, object, str]] = []

        def record_signal(m: agent.Meta, sig: int) -> None:
            del sig
            signalled.append((int(m["pid"]), m.get("start_time"), str(m["invocation_id"])))

        monkeypatch.setattr(agent, "send_signal_group", record_signal)
        assert agent._converge_for_delete(aid, force=True, deadline=time.time() + 0.05) is False
        assert any(pid == proc.pid for pid, _, _ in signalled)
        kept = agent.read_meta(aid)
        assert kept is not None
        assert kept["state"] == "killed"
        assert kept["unresolved_invocation"] == marker

        # Positive proof of the child's death lets forced deletion converge.
        proc.kill()
        proc.wait(timeout=5)
        assert agent._converge_for_delete(aid, force=True, deadline=time.time() + 0.5) is True
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5)


def test_spawn_gate_refusal_durably_records_child_before_any_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The spawn-gate refusal itself hands off the child's exact obligation.

    If a runner dies right after ``_record_running`` returns ``blocked``
    (before any cleanup runs), the already-spawned child must still be
    durably recorded as unresolved in the same locked transaction, so a
    concurrent force-delete can never falsely remove state while it lives.
    """
    aid = "aaaaaaaa"
    iid = "inv-gate"
    seed = agent.idle_meta(aid, str(tmp_path), None)
    seed["state"] = "running"
    seed["stop_reason"] = "kill"
    agent.write_meta(aid, seed)

    proc = subprocess.Popen(
        [SLEEP_BIN, "300"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        close_fds=True,
    )
    try:
        start = _observable_ticks(proc.pid)
        blocked: dict[str, bool] = {}
        update_meta = agent.update_meta
        update_meta(aid, agent._record_running(proc, start, iid, blocked))

        assert blocked.get("stopped") is True
        cur = agent.read_meta(aid)
        assert cur is not None
        assert cur["unresolved_invocation"] == {
            "pid": proc.pid,
            "pgid": proc.pid,
            "start_time": start,
            "invocation_id": iid,
        }

        # Model runner death followed by a forced delete: the tombstone goes
        # up while the leaked child lives and no cleanup ever ran.
        def tombstone(m: agent.Meta) -> None:
            m["delete_pending"] = True
            m["runner_pid"] = None
            m["runner_start_time"] = None

        update_meta(aid, tombstone)

        monkeypatch.setattr(os, "killpg", lambda *_a: pytest.fail("bare killpg fired"))

        exact_signals: list[object] = []

        def record_signal(meta: agent.Meta) -> None:
            rec = meta.get("unresolved_invocation")
            exact_signals.append(rec.get("pid") if isinstance(rec, dict) else None)

        monkeypatch.setattr(agent, "_signal_unresolved_child", record_signal)
        assert agent._delete_converged(agent.read_meta(aid)) is False
        assert agent._converge_for_delete(aid, force=True, deadline=time.time() + 0.05) is False
        assert exact_signals[-1] == proc.pid, "forced delete uses exact helper only"
        kept = agent.read_meta(aid)
        assert kept is not None
        assert kept["delete_pending"] is True
        assert kept["unresolved_invocation"]["pid"] == proc.pid

        # Positive proof of the child's exact death lets deletion converge.
        proc.kill()
        proc.wait(timeout=5)
        assert agent._delete_converged(agent.read_meta(aid)) is True
        assert agent._converge_for_delete(aid, force=True, deadline=time.time() + 0.5) is True
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5)


def _refused_leader_leaves_marked_descendant(
    tmp_path: Path, aid: str, iid: str
) -> tuple[subprocess.Popen[bytes], int]:
    """Spawn a refused leader that exits, leaving one exact-marked descendant.

    Args:
        tmp_path: Throwaway state root backing directory.
        aid: Exact agent ID stamped into the descendant environment.
        iid: Exact invocation ID stamped into the descendant environment.

    Returns:
        The (already exited) leader process and the surviving descendant PID.
    """
    seed = agent.idle_meta(aid, str(tmp_path), None)
    seed["state"] = "running"
    seed["stop_reason"] = "kill"
    agent.write_meta(aid, seed)

    env = dict(os.environ)
    env["LUBKO_AGENT_ID"] = aid
    env[agent.INVOCATION_ID_VAR] = iid
    leader = subprocess.Popen(
        ["/bin/sh", "-c", f"{SLEEP_BIN} 300 < /dev/null &"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        close_fds=True,
        env=env,
    )
    start = agent.proc_start_ticks(leader.pid)
    blocked: dict[str, bool] = {}
    agent.update_meta(aid, agent._record_running(leader, start, iid, blocked))
    assert blocked.get("stopped") is True

    # The refused leader exits; its marked descendant survives in the old
    # session group.
    leader.wait(timeout=5)
    deadline = time.time() + 5
    members: list[tuple[int, int]] = []
    while time.time() < deadline:
        members = agent._pinned_invocation_members(leader.pid, aid, iid)
        for _, fd in members:
            os.close(fd)
        if members:
            break
        time.sleep(0.05)
    assert members, "marked descendant must survive the leader's exit"

    def tombstone(m: agent.Meta) -> None:
        m["delete_pending"] = True
        m["runner_pid"] = None
        m["runner_start_time"] = None

    agent.update_meta(aid, tombstone)
    return leader, members[0][0]


def test_delete_blocked_until_marked_descendants_of_dead_leader_are_gone(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Leader death alone never clears the obligation: marked descendants count.

    A spawn-gate-refused leader exits before any cleanup, leaving a genuine
    descendant with the exact agent and invocation markers alive in the old
    group. Deletion stays non-converged, forced delete converges that exact
    descendant through pinned per-invocation signalling, and only then may
    deletion succeed.
    """
    aid = "aaaaaaaa"
    iid = "inv-desc"
    leader, descendant_pid = _refused_leader_leaves_marked_descendant(tmp_path, aid, iid)
    try:
        monkeypatch.setattr(os, "killpg", lambda *_a: pytest.fail("bare killpg fired"))

        cur = agent.read_meta(aid)
        assert cur is not None
        assert agent._delete_converged(cur) is False, (
            "marked descendant keeps deletion non-converged"
        )
        # Non-forced deletion refuses without signalling anything.
        original_signal_unresolved = agent._signal_unresolved_child
        monkeypatch.setattr(
            agent,
            "_signal_unresolved_child",
            lambda _m: pytest.fail("non-forced delete must never signal"),
        )
        assert agent._converge_for_delete(aid, force=False, deadline=time.time() + 0.05) is False
        kept = agent.read_meta(aid)
        assert kept is not None
        assert kept["unresolved_invocation"] is not None

        # Forced delete converges the exact marked descendant through the
        # pinned per-invocation path (send_signal_group's dead-leader branch)
        # and only then reports convergence.
        monkeypatch.setattr(agent, "_signal_unresolved_child", original_signal_unresolved)
        assert agent._converge_for_delete(aid, force=True, deadline=time.time() + 5) is True
        assert not agent.pid_alive(descendant_pid)
        final = agent.read_meta(aid)
        assert final is not None
    finally:
        if leader.poll() is None:
            leader.kill()
            leader.wait(timeout=5)


def test_unresolved_group_reuse_by_foreign_invocation_is_gone() -> None:
    """A recycled PGID hosting a foreign invocation proves the record gone."""
    owner = _MarkedProcess("aaaaaaaa", "inv-foreign")
    try:
        marker = {
            "pid": 424242,  # long-dead leader slot
            "pgid": owner.pid,  # PGID recycled by a newer/foreign invocation
            "start_time": 1,
            "invocation_id": "inv-old",
        }
        recycled_pgid: int = owner.pid
        meta: agent.Meta = {"id": "aaaaaaaa", "state": "stopped"}
        meta["unresolved_invocation"] = marker
        assert agent._unresolved_child_state(meta) == "gone"

        # The same group occupied by an exact-marker member of *this*
        # invocation stays live.
        owned = _MarkedProcess("aaaaaaaa", "inv-old")
        try:
            os.setpgid(owned.pid, recycled_pgid)
        except PermissionError:
            pass
        else:
            try:
                assert agent._unresolved_child_state(meta) == "live"
            finally:
                owned.kill_and_reap()
    finally:
        owner.kill_and_reap()


def _unresolved_marker_for(pgid: int) -> tuple[agent.Meta, dict[str, object]]:
    """Metadata with a dead-leader unresolved marker pinned to ``pgid``.

    Args:
        pgid: The recorded process group of the old invocation.

    Returns:
        The metadata mapping and the marker placed inside it.
    """
    marker: dict[str, object] = {
        "pid": 424242,  # long-dead leader slot
        "pgid": pgid,
        "start_time": 1,
        "invocation_id": "inv-old",
    }
    meta: agent.Meta = {"id": "aaaaaaaa", "state": "stopped"}
    meta["unresolved_invocation"] = marker
    return meta, marker


def test_unresolved_scan_proc_enumeration_failure_stays_blocked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """/proc enumeration failure keeps the obligation ambiguous and blocking."""
    owner = _MarkedProcess("aaaaaaaa", "inv-foreign")
    try:
        meta, _ = _unresolved_marker_for(owner.pid)

        def broken_iterdir(_self: Path) -> object:
            failure = OSError(errno.EIO, "procfs unavailable")
            raise failure

        monkeypatch.setattr(pathlib.Path, "iterdir", broken_iterdir)
        assert agent._unresolved_child_state(meta) == "ambiguous"
        assert agent._delete_converged(meta) is False
    finally:
        owner.kill_and_reap()


def test_unresolved_scan_uninspectable_marker_keeps_block_and_no_numeric_signal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An in-group candidate with unreadable markers stays ambiguously blocked.

    The scan cannot prove membership or absence, so deletion stays blocked
    and forced convergence never falls back to numeric PID/PGID signalling.
    """
    meta, _ = _unresolved_marker_for(525252)
    pin_fd = os.open(os.devnull, os.O_RDONLY)

    # Deterministic enumeration: exactly one synthetic candidate (a member
    # distinct from the recorded PGID so it is not skipped as the leader slot).
    entries = [types.SimpleNamespace(name="525253")]
    monkeypatch.setattr(pathlib.Path, "iterdir", lambda _self: iter(entries))
    monkeypatch.setattr(agent, "open_pidfd", lambda _pid: os.dup(pin_fd))
    monkeypatch.setattr(os, "getpgid", lambda _pid: 525252)

    def unreadable_environ(_self: Path) -> bytes:
        failure = OSError(errno.EACCES, "environ uninspectable")
        raise failure

    monkeypatch.setattr(pathlib.Path, "read_bytes", unreadable_environ)
    monkeypatch.setattr(os, "killpg", lambda *_a: pytest.fail("bare killpg fired"))
    try:
        assert agent._unresolved_child_state(meta) == "ambiguous"
        assert agent._delete_converged(meta) is False
    finally:
        os.close(pin_fd)


def test_proven_scan_fallback_eperm_is_incomplete_not_raised(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed liveness probe after a failed pin is ambiguity, not a crash."""
    pin_fd = os.open(os.devnull, os.O_RDONLY)
    try:
        entries = [types.SimpleNamespace(name="525253")]
        monkeypatch.setattr(pathlib.Path, "iterdir", lambda _self: iter(entries))
        monkeypatch.setattr(agent, "open_pidfd", lambda _pid: None)

        def forbidden_kill(pid: int, sig: int) -> None:
            del pid, sig
            failure = PermissionError(errno.EPERM, "probe refused")
            raise failure

        monkeypatch.setattr(os, "kill", forbidden_kill)
        members, complete = agent._proven_invocation_members(525252, "aaaaaaaa", "inv-x")
        assert members == []
        assert complete is False
    finally:
        os.close(pin_fd)


def test_group_alive_and_wait_group_dead_fail_closed_on_enumeration_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """/proc ambiguity conservatively counts the exact group as still alive."""
    aid = "aaaaaaaa"
    iid = "inv-g"
    seed = agent.idle_meta(aid, str(tmp_path), None)
    seed["pgid"] = 525252
    seed["invocation_id"] = iid
    seed["runner_pid"] = os.getpid()
    seed["runner_start_time"] = agent.proc_start_ticks(os.getpid())
    # Ensure the recorded leader itself cannot look alive: start_time mismatch.
    seed["pid"] = 424242
    seed["start_time"] = 1
    agent.write_meta(aid, seed)

    def broken_iterdir(_self: Path) -> object:
        failure = OSError(errno.EIO, "procfs unavailable")
        raise failure

    monkeypatch.setattr(pathlib.Path, "iterdir", broken_iterdir)
    cur = agent.read_meta(aid)
    assert cur is not None
    assert agent.group_alive(cur) is True, "incomplete scan must count as alive"
    assert agent.wait_group_dead(cur, 0.05) is False


def test_proven_scan_incomplete_after_member_keeps_and_closes_fds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Proven members survive an incompleting scan with their pins closable."""
    marker_env = f"LUBKO_AGENT_ID=aaaaaaaa\0{agent.INVOCATION_ID_VAR}=inv-x\0".encode()
    pin_fd = os.open(os.devnull, os.O_RDONLY)
    try:
        payloads = {
            "525253": marker_env,  # exact member
            "525254": None,  # uninspectable: scan becomes incomplete
        }

        entries = [
            types.SimpleNamespace(name="525253"),
            types.SimpleNamespace(name="525254"),
        ]
        monkeypatch.setattr(pathlib.Path, "iterdir", lambda _self: iter(entries))
        monkeypatch.setattr(agent, "open_pidfd", lambda _pid: os.dup(pin_fd))
        monkeypatch.setattr(os, "getpgid", lambda _pid: 525252)

        def selective_environ(self: pathlib.Path) -> bytes:
            path = str(self)
            data = payloads.get("525253" if "525253" in path else "525254")
            if data is None:
                failure = OSError(errno.EACCES, "environ uninspectable")
                raise failure
            return data

        monkeypatch.setattr(pathlib.Path, "read_bytes", selective_environ)
        members, complete = agent._proven_invocation_members(525252, "aaaaaaaa", "inv-x")
        assert complete is False
        assert [pid for pid, _ in members] == [525253]
        # The caller must be able to close every returned pin exactly once.
        for _, fd in members:
            os.close(fd)
        for _, fd in members:
            with contextlib.suppress(OSError):
                os.close(fd)
                pytest.fail("double-close would mean a leaked or reused fd")
    finally:
        os.close(pin_fd)


def test_group_alive_stays_true_when_live_leader_environ_unreadable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Matching start ticks with unreadable leader markers is never 'gone'.

    A live recorded leader whose environ cannot be inspected must count as
    alive so wait_group_dead cannot falsely prove convergence; a recycled
    leader slot still falls through to the exact descendant scan.
    """
    aid = "aaaaaaaa"
    iid = "inv-lead"
    owner = _MarkedProcess(aid, iid)
    try:
        meta = agent.idle_meta(aid, str(tmp_path), None)
        meta["pgid"] = owner.pid
        meta["invocation_id"] = iid
        meta["pid"] = owner.pid
        meta["start_time"] = agent.proc_start_ticks(owner.pid)
        agent.write_meta(aid, meta)

        def unreadable_environ(_self: Path) -> bytes:
            failure = OSError(errno.EACCES, "environ uninspectable")
            raise failure

        # is_alive collapses the marker-read failure to False; group_alive
        # must still refuse to declare the group dead.
        monkeypatch.setattr(pathlib.Path, "read_bytes", unreadable_environ)
        cur = agent.read_meta(aid)
        assert cur is not None
        assert agent.is_alive(cur) is False  # the collapse being guarded against
        assert agent.group_alive(cur) is True
        with monkeypatch.context() as guard:
            guard.setattr(os, "killpg", lambda *_a: pytest.fail("bare killpg fired"))
            assert agent.wait_group_dead(cur, 0.05) is False

        # A positively recycled leader slot (different ticks) still falls
        # through to the exact member scan, which proves the group empty.
        def readable_foreign(_self: Path) -> bytes:
            return b"LUBKO_AGENT_ID=other\0"

        monkeypatch.setattr(pathlib.Path, "read_bytes", readable_foreign)
        stale = dict(meta)
        stale["pid"] = os.getpid()  # live process with mismatching ticks
        stale["start_time"] = 1
        assert agent.group_alive(stale) is False
    finally:
        owner.kill_and_reap()


def test_signal_identity_checked_delivers_through_the_pin(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A proven identity is signalled through its pinned descriptor only.

    The numeric PID may be recycled between the proof and delivery: numeric
    ``kill`` could retarget onto an unrelated occupant even after a successful
    proof. Delivery must go exclusively through ``pidfd_send_signal`` on the
    descriptor that pinned the verified process.
    """
    closed: list[int] = []
    monkeypatch.setattr(agent, "open_pidfd", lambda _pid: 55)
    monkeypatch.setattr(agent, "proc_start_ticks", lambda _pid: 111)
    monkeypatch.setattr(os, "kill", lambda *_a: pytest.fail("numeric kill fired"))
    monkeypatch.setattr(os, "close", closed.append)
    delivered: list[tuple[int, int]] = []
    monkeypatch.setattr(agent, "pidfd_send_signal", lambda fd, sig: delivered.append((fd, sig)))

    agent.signal_identity_checked(424242, 111, signal.SIGKILL)

    assert delivered == [(55, signal.SIGKILL)]
    assert closed == [55]


def test_signal_identity_checked_ignores_pid_reused_after_proof(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A recorded PID whose start ticks no longer match is never signalled."""
    monkeypatch.setattr(agent, "open_pidfd", lambda _pid: 56)
    monkeypatch.setattr(agent, "proc_start_ticks", lambda _pid: 999)
    monkeypatch.setattr(os, "kill", lambda *_a: pytest.fail("numeric kill fired"))
    monkeypatch.setattr(
        agent,
        "pidfd_send_signal",
        lambda *_a: pytest.fail("signalled a recycled PID"),
    )
    closed: list[int] = []
    monkeypatch.setattr(os, "close", closed.append)

    agent.signal_identity_checked(424242, 111, signal.SIGKILL)

    assert closed == [56]


def test_signal_identity_checked_fails_closed_without_a_pin(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Without pidfd support nothing is signalled and no numeric path runs."""
    monkeypatch.setattr(agent, "open_pidfd", lambda _pid: None)
    monkeypatch.setattr(os, "kill", lambda *_a: pytest.fail("numeric kill fired"))
    monkeypatch.setattr(
        agent,
        "pidfd_send_signal",
        lambda *_a: pytest.fail("signalled without a pin"),
    )

    agent.signal_identity_checked(424242, 111, signal.SIGKILL)


def test_signal_identity_checked_live_delivery(monkeypatch: pytest.MonkeyPatch) -> None:
    """A genuinely live matching process is signalled through its real pin."""
    owner = subprocess.Popen([SLEEP_BIN, "30"])
    try:
        ticks = agent.proc_start_ticks(owner.pid)
        sent: list[tuple[int, int]] = []

        def fake_send(pidfd: int, sig: int) -> None:
            os.kill(owner.pid, sig)
            sent.append((pidfd, sig))

        monkeypatch.setattr(agent, "pidfd_send_signal", fake_send)
        agent.signal_identity_checked(owner.pid, ticks, signal.SIGTERM)
        assert len(sent) == 1
        assert sent[0][1] == signal.SIGTERM
        assert sent[0][0] > 2  # delivery used a real descriptor
        assert owner.wait(timeout=5) == -signal.SIGTERM
    finally:
        with contextlib.suppress(OSError):
            owner.kill()
            owner.wait(timeout=5)
