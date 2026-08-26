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


def test_exit_boundary_forces_fresh_generation_for_late_work(
    tmp_path: Path,
) -> None:
    """Work arriving after the reclaim boundary gains fresh execution authority.

    The reclaim boundary durably drops ``active_runner`` and the reservation
    while the runner process is demonstrably still alive; the very next locked
    transition must reserve a replacement generation rather than reuse the
    exiting runner.
    """
    aid = "aaaaaaaa"
    proc = _MarkedRunner(aid)
    try:
        meta = agent.idle_meta(aid, str(tmp_path), None)
        meta["state"] = "running"
        meta["active_runner"] = True
        meta["runner_gen"] = 1
        meta.update(proc.identity_fields())
        meta["runner_reservation"] = _claimed_reservation()
        agent.write_meta(aid, meta)
        stored = agent.read_meta(aid)
        assert stored is not None
        assert agent.runner_alive(stored)

        # The runner's own exit boundary relinquishes consumption authority.
        assert agent._reclaim_prompt(aid) is False
        after = agent.read_meta(aid)
        assert after is not None
        assert after["active_runner"] is False
        assert after["runner_reservation"] is None
        # This is precisely the vulnerable window: authority is gone but the
        # exact runner process still probes as alive.
        assert proc.proc.poll() is None
        assert agent.runner_alive(after)

        decision: dict[str, object] = {}
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(agent, "_begin_invocation", lambda *_a, **_k: None)
            agent.update_meta(aid, lambda m: _decide_into(m, decision, "late", steer=False))
        final = agent.read_meta(aid)
        assert decision["action"] == "spawn"
        assert final is not None
        assert int(final["runner_gen"]) == 2
        res = final["runner_reservation"]
        assert isinstance(res, dict)
        assert res["state"] == "reserved"
        assert final["active_runner"] is True
    finally:
        proc.kill_and_reap()
