"""Durable consumption authority at the runner exit boundary.

A prompt or steer may only be reused by a runner that still holds durable
consumption authority (``active_runner`` true with its reservation intact).
Once a runner relinquishes that authority at its exit boundary, a lingering
process identity must never cause new work to be queued onto it: accepted
work must gain fresh, durable execution authority (a new reserved
generation) instead.
"""

from __future__ import annotations

import contextlib
import os
import shutil
import signal
import subprocess
from pathlib import Path
from typing import Final

import pytest

from lubko import agent

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


class _MarkedRunner:
    """A real live process recorded as the agent's exact runner identity."""

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

    def identity_fields(self) -> dict[str, object]:
        """Return the metadata fields anchoring this exact process."""
        return {
            "runner_pid": self.pid,
            "runner_start_time": agent.proc_start_ticks(self.pid),
        }

    def kill_and_reap(self) -> None:
        """Converge the exact test-owned process and reap it."""
        if self.proc.poll() is None:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(self.pid, signal.SIGKILL)
        self.proc.wait(timeout=5)


def _claimed_reservation() -> dict[str, object]:
    return {
        "gen": 1,
        "owner_pid": os.getpid(),
        "owner_start_ticks": agent.proc_start_ticks(os.getpid()),
        "state": "claimed",
        "mode": "new",
    }


def _decide(m: agent.Meta, *, prompt: str, steer: bool) -> dict[str, object]:
    decision: dict[str, object] = {}
    agent._decide_invocation(m, decision, prompt=prompt, steer=steer)
    return decision


def _decide_into(m: agent.Meta, decision: dict[str, object], prompt: str, *, steer: bool) -> None:
    agent._decide_invocation(m, decision, prompt=prompt, steer=steer)


def test_reuse_requires_durable_consumption_authority() -> None:
    """A live runner without consumption authority must not accept work.

    After the exit boundary dropped ``active_runner`` and the reservation,
    the still-lingering process identity is irrelevant: both an ordinary
    prompt and a steer fall through to a fresh reserved generation instead of
    being queued onto a runner that will never consume them.
    """
    m = agent.idle_meta("aaaaaaaa", str(os.environ["XDG_STATE_HOME"]), None)
    m["state"] = "running"
    # idle_meta starts with no consumption authority, mirroring the state
    # right after the exit boundary dropped it.

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(agent, "runner_alive", lambda _meta: True)
        decision = _decide(m, prompt="work", steer=False)
        assert decision["action"] == "spawn"
        assert m["pending_prompt"] == "work"
        assert m["active_runner"] is True
        res = m["runner_reservation"]
        assert isinstance(res, dict)
        assert res.get("state") == "reserved"


def test_authoritative_runner_accepts_exactly_one_prompt(
    tmp_path: Path,
) -> None:
    """A runner holding consumption authority keeps accepting one prompt."""
    aid = "aaaaaaaa"
    proc = _MarkedRunner(aid)
    try:
        meta = agent.idle_meta(aid, str(tmp_path), None)
        meta["state"] = "running"
        meta["active_runner"] = True
        meta.update(proc.identity_fields())
        meta["runner_reservation"] = _claimed_reservation()
        assert agent.runner_alive(meta)

        decision = _decide(meta, prompt="queued", steer=False)
        assert decision["action"] == "reuse"
        assert meta["pending_prompt"] == "queued"

        second = _decide(meta, prompt="second", steer=False)
        assert second["action"] == "busy"
        assert meta["pending_prompt"] == "queued"
    finally:
        proc.kill_and_reap()


@pytest.mark.parametrize("bad_pid", [4242.9, "4242", True])
def test_invocation_liveness_rejects_malformed_persisted_pid(
    monkeypatch: pytest.MonkeyPatch,
    bad_pid: object,
) -> None:
    """Malformed durable PIDs cannot be normalized into invocation authority."""
    meta = agent.idle_meta("aaaaaaaa", str(os.environ["XDG_STATE_HOME"]), None)
    meta.update({"state": "running", "pid": bad_pid, "start_time": 111})
    opened: list[int] = []

    def fake_open_pidfd(pid: int) -> int:
        opened.append(pid)
        return 77

    monkeypatch.setattr(agent, "open_pidfd", fake_open_pidfd)

    assert not agent.is_alive(meta)
    assert opened == []


@pytest.mark.parametrize("bad_ticks", [111.0, "111", True])
def test_invocation_liveness_rejects_malformed_persisted_start_time(
    monkeypatch: pytest.MonkeyPatch,
    bad_ticks: object,
) -> None:
    """Malformed start ticks cannot compare equal to a real invocation identity."""
    meta = agent.idle_meta("aaaaaaaa", str(os.environ["XDG_STATE_HOME"]), None)
    meta.update({"state": "running", "pid": 4242, "start_time": bad_ticks})
    opened: list[int] = []

    def fake_open_pidfd(pid: int) -> int:
        opened.append(pid)
        return 77

    monkeypatch.setattr(agent, "open_pidfd", fake_open_pidfd)

    assert not agent.is_alive(meta)
    assert opened == []


def test_runner_liveness_rejects_fractional_pid_of_matching_live_process(tmp_path: Path) -> None:
    """A fractional durable runner PID cannot truncate into a marked live runner."""
    aid = "aaaaaaaa"
    proc = _MarkedRunner(aid)
    try:
        meta = agent.idle_meta(aid, str(tmp_path), None)
        meta["active_runner"] = True
        meta.update(proc.identity_fields())
        meta["runner_pid"] = proc.pid + 0.9

        assert not agent.runner_alive(meta)
    finally:
        proc.kill_and_reap()


@pytest.mark.parametrize("bad_pid", ["4242", True])
def test_runner_liveness_rejects_other_malformed_persisted_pids(
    monkeypatch: pytest.MonkeyPatch,
    bad_pid: object,
) -> None:
    """Strings and booleans cannot name persisted runner process identity."""
    meta = agent.idle_meta("aaaaaaaa", str(os.environ["XDG_STATE_HOME"]), None)
    meta.update({"runner_pid": bad_pid, "runner_start_time": 111})
    opened: list[int] = []

    def fake_open_pidfd(pid: int) -> int:
        opened.append(pid)
        return 77

    monkeypatch.setattr(agent, "open_pidfd", fake_open_pidfd)

    assert not agent.runner_alive(meta)
    assert opened == []


@pytest.mark.parametrize("bad_ticks", [111.0, "111", True])
def test_runner_liveness_rejects_malformed_persisted_start_time(
    monkeypatch: pytest.MonkeyPatch,
    bad_ticks: object,
) -> None:
    """Malformed runner ticks cannot compare equal to a real process identity."""
    meta = agent.idle_meta("aaaaaaaa", str(os.environ["XDG_STATE_HOME"]), None)
    meta.update({"runner_pid": 4242, "runner_start_time": bad_ticks})
    opened: list[int] = []

    def fake_open_pidfd(pid: int) -> int:
        opened.append(pid)
        return 77

    monkeypatch.setattr(agent, "open_pidfd", fake_open_pidfd)

    assert not agent.runner_alive(meta)
    assert opened == []


@pytest.mark.parametrize("bad_pid", [4242.9, "4242", True, 0])
def test_recorded_leader_state_keeps_malformed_present_pid_ambiguous(
    bad_pid: object,
) -> None:
    """Malformed persisted leader identity must never become a proven-dead PID."""
    meta = agent.idle_meta("aaaaaaaa", str(os.environ["XDG_STATE_HOME"]), None)
    meta.update({"pid": bad_pid, "start_time": 111})

    assert agent._recorded_leader_state(meta) == "ambiguous"


@pytest.mark.parametrize("bad_ticks", [111.0, "111", True, -1])
def test_recorded_leader_state_keeps_malformed_start_ticks_ambiguous(
    bad_ticks: object,
) -> None:
    """Malformed persisted ticks keep stop/convergence fail closed."""
    meta = agent.idle_meta("aaaaaaaa", str(os.environ["XDG_STATE_HOME"]), None)
    meta.update({"pid": 4242, "start_time": bad_ticks})

    assert agent._recorded_leader_state(meta) == "ambiguous"


def test_invocation_liveness_stays_bound_to_pinned_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Invocation liveness is accepted only through the pinned process."""
    meta = agent.idle_meta("aaaaaaaa", str(os.environ["XDG_STATE_HOME"]), None)
    meta.update({"state": "running", "pid": 4242, "start_time": 111})
    probes: list[tuple[int, int]] = []
    closed: list[int] = []

    monkeypatch.setattr(agent, "open_pidfd", lambda _pid: 77)
    monkeypatch.setattr(agent, "proc_start_ticks", lambda _pid: 111)
    monkeypatch.setattr(agent, "env_has_marker", lambda _pid, _aid: True)
    monkeypatch.setattr(agent, "pidfd_send_signal", lambda fd, sig: probes.append((fd, sig)))
    monkeypatch.setattr(os, "close", closed.append)

    assert agent.is_alive(meta)
    assert probes == [(77, 0)]
    assert closed == [77]


def test_dead_pinned_invocation_cannot_authorize_prompt_reuse(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A vanished invocation makes ordinary prompts and steers start fresh."""
    probes: list[tuple[int, int]] = []
    closed: list[int] = []

    monkeypatch.setattr(agent, "open_pidfd", lambda _pid: 77)
    monkeypatch.setattr(agent, "proc_start_ticks", lambda _pid: 111)
    monkeypatch.setattr(agent, "env_has_marker", lambda _pid, _aid: True)

    def dead_pinned_invocation(fd: int, sig: int) -> None:
        probes.append((fd, sig))
        raise ProcessLookupError

    monkeypatch.setattr(agent, "pidfd_send_signal", dead_pinned_invocation)
    monkeypatch.setattr(os, "close", closed.append)

    for steer in (False, True):
        meta = agent.idle_meta("aaaaaaaa", str(os.environ["XDG_STATE_HOME"]), None)
        meta.update({"state": "running", "pid": 4242, "start_time": 111})

        decision = _decide(meta, prompt="work", steer=steer)

        assert decision["action"] == "spawn"
        assert meta["pending_prompt"] == "work"
        assert meta["active_runner"] is True
        reservation = meta["runner_reservation"]
        assert isinstance(reservation, dict)
        assert reservation["state"] == "reserved"

    assert probes == [(77, 0), (77, 0)]
    assert closed == [77, 77]


def test_runner_disappearing_during_identity_proof_gets_fresh_authority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PID reuse during runner proof cannot authorize stale prompt reuse."""
    m = agent.idle_meta("aaaaaaaa", str(os.environ["XDG_STATE_HOME"]), None)
    m.update({
        "state": "running",
        "active_runner": True,
        "runner_gen": 1,
        "runner_pid": 4242,
        "runner_start_time": 111,
        "runner_reservation": _claimed_reservation(),
    })
    probes: list[tuple[int, int]] = []
    closed: list[int] = []

    monkeypatch.setattr(agent, "open_pidfd", lambda _pid: 77)
    monkeypatch.setattr(agent, "proc_start_ticks", lambda _pid: 111)
    monkeypatch.setattr(agent, "env_has_marker", lambda _pid, _aid: True)

    def dead_pinned_runner(fd: int, sig: int) -> None:
        probes.append((fd, sig))
        raise ProcessLookupError

    monkeypatch.setattr(agent, "pidfd_send_signal", dead_pinned_runner)
    monkeypatch.setattr(os, "close", closed.append)

    decision = _decide(m, prompt="work", steer=False)

    assert probes
    assert all(probe == (77, 0) for probe in probes)
    assert closed == [77] * len(probes)
    assert decision["action"] == "spawn"
    assert m["pending_prompt"] == "work"
    assert int(m["runner_gen"]) == 2
    reservation = m["runner_reservation"]
    assert isinstance(reservation, dict)
    assert reservation["state"] == "reserved"


def _dead_claimed_meta(aid: str, tmp_path: Path) -> agent.Meta:
    """Build metadata for a runner that claimed and then died.

    The recorded runner identity belongs to a fully reaped process, so
    ``runner_alive`` deterministically probes false without any sleeping.

    Returns:
        Agent metadata in the dead claimed-runner state.
    """
    proc = subprocess.Popen(
        [SLEEP_BIN, "300"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    identity = (proc.pid, agent.proc_start_ticks(proc.pid))
    with contextlib.suppress(ProcessLookupError):
        os.kill(proc.pid, signal.SIGKILL)
    proc.wait(timeout=5)

    meta = agent.idle_meta(aid, str(tmp_path), None)
    meta["state"] = "running"
    meta["active_runner"] = True
    meta["runner_gen"] = 1
    meta["runner_pid"], meta["runner_start_time"] = identity
    meta["runner_reservation"] = _claimed_reservation()
    agent.write_meta(aid, meta)
    return meta


def test_dead_pinned_reservation_owner_recovers_accepted_steer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A vanished reservation owner cannot authorize queued work reuse."""
    m = agent.idle_meta("aaaaaaaa", str(os.environ["XDG_STATE_HOME"]), None)
    m.update({
        "state": "running",
        "active_runner": True,
        "runner_gen": 1,
        "runner_reservation": {
            "gen": 1,
            "owner_pid": 4242,
            "owner_start_ticks": 111,
            "state": "reserved",
            "mode": "new",
        },
        "pending_prompt": "original",
    })
    probes: list[tuple[int, int]] = []
    closed: list[int] = []

    monkeypatch.setattr(agent, "is_alive", lambda _meta: False)
    monkeypatch.setattr(agent, "runner_alive", lambda _meta: False)
    monkeypatch.setattr(agent, "open_pidfd", lambda _pid: 77)
    monkeypatch.setattr(agent, "proc_start_ticks", lambda _pid: 111)
    monkeypatch.setattr(agent, "_is_zombie", lambda _pid: False)
    monkeypatch.setattr(agent, "_runner_marker_alive", lambda _aid, _gen: False)

    def dead_owner(fd: int, sig: int) -> None:
        probes.append((fd, sig))
        raise ProcessLookupError

    monkeypatch.setattr(agent, "pidfd_send_signal", dead_owner)
    monkeypatch.setattr(os, "close", closed.append)

    decision: dict[str, object] = {}
    agent._apply_locked_transition(m, decision, prompt="steer", steer=True, mode="new")

    assert probes == [(77, 0), (77, 0)]
    assert closed == [77, 77]
    assert decision["action"] == "spawn"
    assert decision.get("steer_accepted") is True
    assert m["pending_prompt"] == "original"
    assert int(m["runner_gen"]) == 2
    reservation = m["runner_reservation"]
    assert isinstance(reservation, dict)
    assert reservation["state"] == "reserved"
    assert reservation["gen"] == 2
    assert m["steer_queue"][0]["prompt"] == "steer"

    # The same helper accepts a genuinely live pinned owner and rejects a
    # start-time-mismatched (reused) numeric PID without probing it as live.
    monkeypatch.setattr(agent, "pidfd_send_signal", lambda fd, sig: probes.append((fd, sig)))
    assert agent._owner_alive(4242, 111)
    assert not agent._owner_alive(4242, 999)
    assert probes == [(77, 0), (77, 0), (77, 0)]
    assert closed == [77, 77, 77, 77]


def test_reserved_runner_marker_requires_pinned_live_candidate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A generation marker authorizes reuse only while its pinned process lives."""
    state = {"alive": True, "vanish_on_read": True}
    closed: list[int] = []

    proc_root = Path("/proc")
    candidate = Path("/proc/4242")
    environ_path = candidate / "environ"

    monkeypatch.setattr(Path, "is_dir", lambda self: self == proc_root)
    monkeypatch.setattr(
        Path,
        "iterdir",
        lambda self: iter([candidate]) if self == proc_root else iter(()),
    )

    def read_markers(path: Path) -> bytes:
        """Return exact markers and optionally model exit immediately after read."""
        if path != environ_path:
            return b""
        if state["vanish_on_read"]:
            state["alive"] = False
        return b"LUBKO_AGENT_ID=aaaaaaaa\0LUBKO_RUNNER_GEN=1\0"

    monkeypatch.setattr(Path, "read_bytes", read_markers)

    def probe_pinned(_fd: int, _sig: int) -> None:
        if not state["alive"]:
            raise ProcessLookupError

    monkeypatch.setattr(agent, "open_pidfd", lambda _pid: 77)
    monkeypatch.setattr(agent, "pidfd_send_signal", probe_pinned)
    monkeypatch.setattr(os, "close", closed.append)

    assert not agent._runner_marker_alive("aaaaaaaa", 1)
    assert closed == [77]

    state.update({"alive": True, "vanish_on_read": False})
    assert agent._runner_marker_alive("aaaaaaaa", 1)
    assert not agent._runner_marker_alive("bbbbbbbb", 1)

    m = agent.idle_meta("aaaaaaaa", str(os.environ["XDG_STATE_HOME"]), None)
    m.update({
        "state": "running",
        "active_runner": True,
        "runner_gen": 1,
        "runner_reservation": {
            "gen": 1,
            "owner_pid": 4242,
            "owner_start_ticks": 111,
            "state": "reserved",
            "mode": "new",
        },
        "pending_prompt": "original",
    })
    monkeypatch.setattr(agent, "is_alive", lambda _meta: False)
    monkeypatch.setattr(agent, "runner_alive", lambda _meta: False)
    monkeypatch.setattr(agent, "_owner_alive", lambda _pid, _ticks: False)
    monkeypatch.setattr(agent, "owned_by_me", lambda _meta, _pid: False)
    state.update({"alive": True, "vanish_on_read": True})

    decision: dict[str, object] = {}
    agent._apply_locked_transition(m, decision, prompt="steer", steer=True, mode="new")

    assert decision["action"] == "spawn"
    assert decision.get("steer_accepted") is True
    assert m["pending_prompt"] == "original"
    assert m["steer_queue"][0]["prompt"] == "steer"
    reservation = m["runner_reservation"]
    assert isinstance(reservation, dict)
    assert reservation["gen"] == 2
    assert reservation["state"] == "reserved"


def test_dead_claimed_runner_preserves_accepted_prompt_exactly_once(
    tmp_path: Path,
) -> None:
    """A prompt accepted before a runner's death survives later prompts once.

    When an exact runner durably claims its reservation but dies before
    consuming the accepted pending prompt, the very next ordinary prompt must
    not fall through to a fresh start that overwrites it. Instead the accepted
    prompt is preserved exactly once and exactly one replacement runner under
    a fresh generation becomes responsible for consuming it; the late caller
    itself is explicitly busy.
    """
    _dead_claimed_meta("aaaaaaaa", tmp_path)
    assert agent.read_meta("aaaaaaaa") is not None
    stored = agent.read_meta("aaaaaaaa")
    assert stored is not None
    assert not agent.runner_alive(stored)
    assert not agent.reservation_in_flight(stored)

    # Accept the original prompt into the durable metadata after claim.
    agent.update_meta("aaaaaaaa", lambda m: m.__setitem__("pending_prompt", "original"))

    decision: dict[str, object] = {}
    agent.update_meta("aaaaaaaa", lambda m: _decide_into(m, decision, "late", steer=False))

    assert decision["action"] == "spawn"
    # The late caller's own prompt was explicitly rejected as busy.
    assert decision.get("recover_busy") is True
    final = agent.read_meta("aaaaaaaa")
    assert final is not None
    # The older accepted prompt was preserved exactly once.
    assert final["pending_prompt"] == "original"
    assert int(final["runner_gen"]) == 2
    res = final["runner_reservation"]
    assert isinstance(res, dict)
    assert res["state"] == "reserved"
    assert final["active_runner"] is True


def test_dead_claimed_runner_that_consumed_prompts_starts_fresh(tmp_path: Path) -> None:
    """No replay when a dead claimed runner already consumed its prompt.

    A claimed reservation whose runner died after consuming the accepted
    prompt carries no pending work; the next ordinary prompt starts a fresh
    generation running only that new prompt, never the consumed one.
    """
    meta = _dead_claimed_meta("aaaaaaaa", tmp_path)
    assert meta.get("pending_prompt") is None

    decision: dict[str, object] = {}
    agent.update_meta("aaaaaaaa", lambda m: _decide_into(m, decision, "fresh", steer=False))

    assert decision["action"] == "spawn"
    final = agent.read_meta("aaaaaaaa")
    assert final is not None
    assert final["pending_prompt"] == "fresh"
    assert int(final["runner_gen"]) == 2
    res = final["runner_reservation"]
    assert isinstance(res, dict)
    assert res["state"] == "reserved"
