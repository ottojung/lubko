"""Regressions for GitHub issue #179.

The dead-leader orphan-member fallback of ``send_signal_group`` used to
authorize surviving members of the recorded process group by the reusable
PGID plus the agent-wide ``LUBKO_AGENT_ID`` marker. When the OS recycled the
group ID into a newer invocation of the *same* agent, that newer invocation's
non-leader children matched both checks and were signalled by a stale stop/
kill aimed at the long-gone old invocation.

The fix stamps every spawned invocation with a durable, invocation-specific
identity (``LUBKO_INVOCATION_ID``, freshly generated per spawned invocation,
inherited by the whole process tree, and recorded in metadata). Orphan
convergence now requires an exact per-member match on PGID *and* the exact
invocation marker, so stale signalling can never cross an invocation
boundary; without a recorded invocation ID it fails closed.

These tests are deterministic: instead of racing the kernel's PID allocator,
they arrange the exact post-recycling shape (recorded PGID hosted by a newer
same-agent invocation with children) with real processes and prove the stale
signal leaves the newer invocation untouched while a genuinely surviving old
descendant still converges.
"""

from __future__ import annotations

import contextlib
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from lubko import agent

pytestmark = pytest.mark.skipif(
    sys.platform != "linux" or agent.open_pidfd(os.getpid()) is None,
    reason="requires Linux pidfd support",
)


def _spawn_invocation_tree(aid: str, iid: str, tmp_path: Path) -> tuple[int, list[int]]:
    """Spawn a real session-leader process tree carrying exact markers.

    The leader ignores nothing special: it is a Python process that spawns one
    long-lived child of its own (inheriting the environment markers) and then
    sleeps, so the group has a non-leader member exactly like a real agent
    invocation.

    Args:
        aid: Agent ID stamped as ``LUBKO_AGENT_ID``.
        iid: Invocation ID stamped as ``LUBKO_INVOCATION_ID``.
        tmp_path: Unused scratch directory (pytest fixture convention).

    Returns:
        The leader PID and the list of all spawned PIDs (leader included).
    """
    del tmp_path
    script = (
        "import os, subprocess, sys, time\n"
        "child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(600)'])\n"
        "print(child.pid, flush=True)\n"
        "time.sleep(600)\n"
    )
    env = dict(os.environ)
    env["LUBKO_AGENT_ID"] = aid
    env["LUBKO_INVOCATION_ID"] = iid
    leader = subprocess.Popen(
        [sys.executable, "-c", script],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        env=env,
    )
    assert leader.stdout is not None
    child_pid = int(leader.stdout.readline())
    return leader.pid, [leader.pid, child_pid]


def _is_gone(pid: int) -> bool:
    """Return whether a PID is dead or an unreaped zombie."""
    try:
        stat = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
    except OSError:
        return True
    fields = stat[stat.rfind(")") + 1 :].split()
    return bool(fields) and fields[0] == "Z"


def _wait_gone(pids: list[int], timeout: float = 10.0) -> bool:
    """Return whether every listed PID stopped being live within the timeout."""
    deadline = time.time() + timeout

    def any_live() -> bool:
        return any(not _is_gone(pid) for pid in pids)

    while any_live() and time.time() < deadline:
        time.sleep(0.05)
    return not any_live()


def _terminate(pids: list[int]) -> None:
    """Best-effort cleanup of test processes."""
    for pid in pids:
        with contextlib.suppress(ProcessLookupError, OSError):
            os.kill(pid, signal.SIGKILL)


def test_stale_signal_leaves_reused_pgid_newer_invocation_untouched(tmp_path: Path) -> None:
    """A stale signal against a recycled PGID never touches the newer invocation.

    The old invocation is fully gone; the OS recycled its PGID (and recorded
    leader PID) into a newer invocation of the same agent, whose group holds a
    non-leader child. The stale signal must leave the entire newer invocation
    untouched, and liveness checks must not attribute it to the old record.
    """
    aid = "issue179-agent"
    old_iid = "old" * 16
    new_iid = "new" * 16
    leader_pid, new_pids = _spawn_invocation_tree(aid, new_iid, tmp_path)
    try:
        # Recorded identity of the dead old invocation whose PGID/PID were
        # recycled into exactly the newer invocation now running. The start
        # time deliberately mismatches so the pinned live-leader path fails.
        stale_meta: agent.Meta = {
            "id": aid,
            "pid": leader_pid,
            "pgid": leader_pid,
            "start_time": 1,
            "invocation_id": old_iid,
        }
        fresh_meta: agent.Meta = {
            "id": aid,
            "pid": leader_pid,
            "pgid": leader_pid,
            "start_time": agent.proc_start_ticks(leader_pid),
            "invocation_id": new_iid,
        }

        agent.send_signal_group(stale_meta, signal.SIGTERM)
        agent.send_signal_group(stale_meta, signal.SIGKILL)

        assert not _wait_gone(new_pids, timeout=2.0), "newer invocation was signalled"
        assert agent.group_alive(fresh_meta), "newer invocation lost liveness"
        assert not agent.group_alive(stale_meta), "stale record claims foreign liveness"

        # Positive control: the exact newer-invocation identity converges the
        # whole group, proving the untouched outcome was not a delivery bug.
        agent.send_signal_group(fresh_meta, signal.SIGKILL)
        assert _wait_gone(new_pids)
    finally:
        _terminate(new_pids)


def test_surviving_old_descendant_is_signalled(tmp_path: Path) -> None:
    """Orphan convergence still reaches genuine survivors of the old invocation.

    After the old leader died, one of its descendants survives inside the
    recorded group carrying the exact old invocation marker. The stale-signal
    path must converge that survivor (and its subtree) exactly.
    """
    del tmp_path
    aid = "issue179-agent"
    old_iid = "old" * 16
    # Helper becomes a session leader, spawns a surviving child inheriting the
    # markers, records the child PID and its own start ticks, then exits. The
    # child remains in the group whose PGID equals the (now reusable) helper
    # PID — the exact orphaned-member shape after leader death.
    script = (
        "import os, subprocess, sys\n"
        "child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(600)'])\n"
        "print(child.pid, os.getpid(), flush=True)\n"
    )
    env = dict(os.environ)
    env["LUBKO_AGENT_ID"] = aid
    env["LUBKO_INVOCATION_ID"] = old_iid
    helper = subprocess.Popen(
        [sys.executable, "-c", script],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        env=env,
    )
    assert helper.stdout is not None
    child_pid_raw, helper_pid_raw = helper.stdout.readline().split()
    child_pid, helper_pid = int(child_pid_raw), int(helper_pid_raw)
    helper_ticks = agent.proc_start_ticks(helper_pid)
    helper.wait(timeout=10)
    assert helper_ticks is not None
    pids = [helper_pid, child_pid]
    try:
        orphan_meta: agent.Meta = {
            "id": aid,
            "pid": helper_pid,
            "pgid": helper_pid,
            "start_time": helper_ticks,
            "invocation_id": old_iid,
        }
        assert agent.group_alive(orphan_meta)
        agent.send_signal_group(orphan_meta, signal.SIGKILL)
        assert _wait_gone([child_pid]), "surviving old descendant was not converged"
        assert not agent.group_alive(orphan_meta)
    finally:
        _terminate(pids)


def test_missing_invocation_id_fails_closed(tmp_path: Path) -> None:
    """Without a recorded invocation ID the orphan fallback signals nothing."""
    aid = "issue179-agent"
    old_iid = "old" * 16
    _, pids = _spawn_invocation_tree(aid, old_iid, tmp_path)
    try:
        legacy_meta: agent.Meta = {
            "id": aid,
            "pid": pids[0],
            "pgid": pids[0],
            "start_time": 1,  # mismatch: forces the orphan fallback path
            "invocation_id": None,
        }
        agent.send_signal_group(legacy_meta, signal.SIGKILL)
        assert not _wait_gone(pids, timeout=2.0), "unverifiable record signalled anyway"
    finally:
        _terminate(pids)
