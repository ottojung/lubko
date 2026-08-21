"""Independent exact-identity process owner for nested pytest runs.

A nested (subprocess) pytest session that is interrupted — SIGINT, SIGTERM,
enforced timeout, or abrupt SIGKILL — can no longer run its own teardown.
This wrapper is the independent owner that makes containment hold anyway:

- it marks itself as a child subreaper, so a descendant orphaned by the
  death of the nested pytest reparents to this wrapper, never to container
  PID 1;
- the nested run records every process it spawns as an exact identity
  (PID plus start-time ticks) in a marker file;
- once the nested pytest has exited — however it exited — the wrapper
  synchronously verifies each recorded identity and stops and reaps any
  survivor by its exact process group before exiting.

The wrapper never observes an orphan under PID 1 and never signals an
identity whose recorded start ticks do not match: a reused PID is reported,
not signalled. The result JSON records exactly what happened so tests can
assert full containment.
"""

from __future__ import annotations

import argparse
import contextlib
import ctypes
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Final

GRACE_SECONDS: Final = 5.0
POLL_SECONDS: Final = 0.02
PR_SET_CHILD_SUBREAPER: Final = 36


def _proc_start_ticks(pid: int) -> int | None:
    """Return the start time in clock ticks of ``pid``, or ``None`` when gone."""
    try:
        stat = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
    except OSError:
        return None
    rest = stat[stat.rfind(")") + 1 :].split()
    try:
        return int(rest[19])
    except (ValueError, IndexError):
        return None


def _pid_state_ppid(pid: int) -> tuple[str, int] | None:
    """Return ``(state, ppid)`` of ``pid``, or ``None`` when gone/zombie-reaped."""
    try:
        stat = Path(f"/proc/{pid}/stat").read_bytes()
    except OSError:
        return None
    close = stat.rfind(b")")
    if close == -1:
        return None
    fields = stat[close + 2 :].split()
    if len(fields) < 4:
        return None
    return fields[0].decode(), int(fields[1])


def _become_subreaper() -> bool:
    """Mark this process as a child subreaper.

    Returns:
        ``True`` when the subreaper flag was set successfully.
    """
    libc = ctypes.CDLL(None, use_errno=True)
    result: int = libc.prctl(PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0)
    return result == 0


def _signal_group_checked(pid: int, ticks: int, sig: signal.Signals) -> bool:
    """Signal an exact group only while its recorded identity still matches.

    Identity verification is mandatory: a signal is only ever authorized by
    a valid recorded tick value that still matches the live occupant of the
    PID.  Missing or unreadable ticks never authorize a signal.

    Args:
        pid: Exact PID (and expected group leader) to signal.
        ticks: Recorded start ticks; must be valid and current.
        sig: Signal to deliver.

    Returns:
        ``True`` when the signal was delivered.
    """
    if ticks <= 0 or _proc_start_ticks(pid) != ticks:
        return False
    with contextlib.suppress(ProcessLookupError):
        os.killpg(pid, sig)
    return True


def _observe(entry_pid: int, result: dict[str, object]) -> bool:
    """Observe the process until it is gone, zombie, or the grace expires.

    Every observation asserts the parent is never container PID 1.

    Args:
        entry_pid: Exact PID to observe.
        result: Result dictionary updated in place.

    Returns:
        ``True`` when the process was seen alive during the window.
    """
    deadline = time.monotonic() + GRACE_SECONDS
    seen_alive = False
    while time.monotonic() < deadline:
        state = _pid_state_ppid(entry_pid)
        if state is None:
            break
        seen_alive = True
        state_letter, parent_pid = state
        if parent_pid == 1:
            result["observed_ppid_1"] = True
        if state_letter == "Z":
            break
        time.sleep(POLL_SECONDS)
    return seen_alive


def _stop_exact(entry_pid: int, entry_ticks: int, result: dict[str, object]) -> bool:
    """Stop a live survivor by its exact verified group (TERM, then KILL).

    Args:
        entry_pid: Exact PID (and expected group leader) to stop.
        entry_ticks: Recorded start ticks; must be valid and current.
        result: Result dictionary updated in place.

    Returns:
        ``False`` when the identity no longer matches and nothing signalled.
    """
    state = _pid_state_ppid(entry_pid)
    if state is None or state[0] == "Z":
        return True
    if not _signal_group_checked(entry_pid, entry_ticks, signal.SIGTERM):
        # The live occupant no longer matches the recorded identity.
        result["identity_mismatch"] = True
        result["contained"] = False
        return False
    stop = time.monotonic() + GRACE_SECONDS
    while time.monotonic() < stop:
        current = _pid_state_ppid(entry_pid)
        if current is None or current[0] == "Z":
            break
        time.sleep(POLL_SECONDS)
    current = _pid_state_ppid(entry_pid)
    if current is not None and current[0] != "Z":
        _signal_group_checked(entry_pid, entry_ticks, signal.SIGKILL)
    return True


def _reap_exact(entry_pid: int, result: dict[str, object]) -> None:
    """Reap an adopted descendant by exact PID until it is gone.

    Args:
        entry_pid: Exact PID to reap.
        result: Result dictionary updated in place.
    """
    reap_deadline = time.monotonic() + GRACE_SECONDS
    while time.monotonic() < reap_deadline:
        with contextlib.suppress(ChildProcessError):
            os.waitpid(entry_pid, os.WNOHANG)
        if _pid_state_ppid(entry_pid) is None:
            return
        time.sleep(POLL_SECONDS)
    result["contained"] = False


def _contain(entry_pid: int, entry_ticks: int, result: dict[str, object]) -> None:
    """Synchronously own and reap one recorded descendant identity.

    Observes the process without ever accepting a reparent under container
    PID 1; stops a survivor by its exact verified group, then reaps it by
    exact PID once it is our adopted child.

    Args:
        entry_pid: The recorded exact PID.
        entry_ticks: The recorded start ticks, if any.
        result: Result dictionary updated in place.
    """
    if not _observe(entry_pid, result):
        result["survivor_seen"] = False
        return
    result["survivor_seen"] = True
    if not _stop_exact(entry_pid, entry_ticks, result):
        return
    _reap_exact(entry_pid, result)


def main() -> int:
    """Run the nested command, then contain and reap its recorded identities.

    Returns:
        The nested command's exit status, or ``1`` on harness misuse.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--marker", type=Path, required=True)
    parser.add_argument("--result", type=Path, required=True)
    parser.add_argument("--pidfile", type=Path, required=True)
    parser.add_argument("--deadline", type=float, default=120.0)
    parser.add_argument("command", nargs="+")
    args = parser.parse_args()

    result: dict[str, object] = {
        "subreaper": _become_subreaper(),
        "observed_ppid_1": False,
        "identity_mismatch": False,
        "unresolved_ticks": False,
        "contained": True,
    }
    proc = subprocess.Popen(
        args.command,
        cwd=os.environ.get("PYTEST_NESTED_CWD", "."),
        env=dict(os.environ),
        stdin=subprocess.DEVNULL,
    )
    args.pidfile.write_text(
        json.dumps({"pid": proc.pid, "ticks": _proc_start_ticks(proc.pid)}),
        encoding="utf-8",
    )
    status: int | None = None
    hard_deadline = time.monotonic() + args.deadline
    timed_out = False
    while status is None:
        try:
            status = proc.wait(timeout=POLL_SECONDS)
        except subprocess.TimeoutExpired:
            if time.monotonic() > hard_deadline:
                proc.kill()
                timed_out = True
                status = proc.wait()
    result["returncode"] = status
    result["timed_out"] = timed_out

    entries: list[dict[str, object]] = []
    try:
        payload = json.loads(args.marker.read_text(encoding="utf-8"))
        if isinstance(payload, list):
            entries = [e for e in payload if isinstance(e, dict)]
    except (OSError, ValueError):
        entries = []

    for entry in entries:
        pid = entry.get("pid")
        ticks = entry.get("ticks")
        if not isinstance(pid, int):
            continue
        if not isinstance(ticks, int) or ticks <= 0:
            # Missing or invalid recorded ticks: containment is unresolved
            # and no signal is ever authorized for this identity.
            result["unresolved_ticks"] = True
            result["contained"] = False
            continue
        current = _proc_start_ticks(pid)
        if current is not None and current != ticks:
            # A different live process occupies the PID: never signal it.
            result["identity_mismatch"] = True
            result["contained"] = False
            continue
        _contain(pid, ticks, result)

    args.result.write_text(json.dumps(result, sort_keys=True) + "\n", encoding="utf-8")
    return 0 if result["contained"] is True else 1


if __name__ == "__main__":
    sys.exit(main())
