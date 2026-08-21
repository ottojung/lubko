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
from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    from collections.abc import Callable

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


def _signal_exact_or_group(
    pid: int,
    ticks: int,
    sig: signal.Signals,
    owned_group: int | None = None,
) -> bool:
    """Signal one descendant using the shared guard's ownership semantics.

    The PID+start-ticks identity is re-verified immediately before every
    signal.  When ``owned_group`` names a dedicated group whose ownership
    was established (while the leader's identity was valid) before an
    earlier TERM, that group is signalled even if the leader itself has
    since exited.  Otherwise a session/process-group leader's whole
    dedicated group is signalled; a non-leader receives an exact-PID signal
    only, never its shared group.

    Args:
        pid: Exact PID to signal.
        ticks: Recorded start ticks; must currently match.
        sig: Signal to deliver.
        owned_group: A previously-proven dedicated group, if any.

    Returns:
        ``True`` when the signal was delivered.
    """
    current = _proc_start_ticks(pid)
    if ticks <= 0 or current is None or current != ticks:
        return False
    with contextlib.suppress(ProcessLookupError):
        if owned_group is not None:
            os.killpg(owned_group, sig)
            return True
        pgid = os.getpgid(pid)
        if pgid == pid:
            os.killpg(pgid, sig)
        else:
            os.kill(pid, sig)
        return True
    return False


def _observe(entry_pid: int, result: dict[str, object]) -> str | None:
    """Observe the process until it exits, becomes zombie, or grace expires.

    Every observation asserts the parent is never container PID 1.

    Args:
        entry_pid: Exact PID to observe.
        result: Result dictionary updated in place.

    Returns:
        The last observed state letter, or ``None`` when truly absent.
    """
    deadline = time.monotonic() + GRACE_SECONDS
    state_letter: str | None = None
    while time.monotonic() < deadline:
        state = _pid_state_ppid(entry_pid)
        if state is None:
            return None
        state_letter = state[0]
        parent_pid = state[1]
        if parent_pid == 1:
            result["observed_ppid_1"] = True
        if state_letter == "Z":
            break
        time.sleep(POLL_SECONDS)
    return state_letter


def _escalate(
    entry_pid: int,
    entry_ticks: int,
    owned_group: int | None,
    result: dict[str, object],
) -> bool:
    """Deliver the escalation KILL by exact/group ownership rules.

    An already-proven dedicated group is signalled even when its leader has
    since exited; a non-leader PID is revalidated (PID plus start ticks)
    immediately before its exact-PID KILL and never receives shared-group
    signalling.

    Args:
        entry_pid: Exact PID of the survivor.
        entry_ticks: Recorded start ticks.
        owned_group: Dedicated group proven owned before TERM, if any.
        result: Result dictionary updated in place.

    Returns:
        ``True`` when containment may continue; ``False`` when the identity
        went unresolved and nothing was signalled.
    """
    if owned_group is not None:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(owned_group, signal.SIGKILL)
        return True
    if not _signal_exact_or_group(entry_pid, entry_ticks, signal.SIGKILL):
        result["identity_mismatch"] = True
        result["contained"] = False
        return False
    return True


def _contain(entry_pid: int, entry_ticks: int, result: dict[str, object]) -> None:
    """Synchronously own and reap one recorded descendant identity.

    Mirrors the shared guard's ownership semantics: dedicated-group
    ownership is established while the identity is verifiable, before the
    TERM; escalation KILL preserves already-proven group ownership across
    leader exit; non-leader PIDs are revalidated (PID plus start ticks)
    immediately before their exact-PID KILL and never receive shared-group
    signalling.

    Args:
        entry_pid: The recorded exact PID.
        entry_ticks: The recorded start ticks.
        result: Result dictionary updated in place.
    """
    pgid: int | None = None
    with contextlib.suppress(ProcessLookupError):
        pgid = os.getpgid(entry_pid)
    owned_group = pgid if pgid == entry_pid else None

    seen = _observe(entry_pid, result)
    if not seen:
        return
    # Aggregation: multiple entries must never overwrite a prior true.
    result["survivor_seen"] = True

    if not _signal_exact_or_group(entry_pid, entry_ticks, signal.SIGTERM):
        # The live occupant no longer matches the recorded identity.
        result["identity_mismatch"] = True
        result["contained"] = False
        return
    stop = time.monotonic() + GRACE_SECONDS
    while time.monotonic() < stop:
        current = _pid_state_ppid(entry_pid)
        if current is None or current[0] == "Z":
            break
        time.sleep(POLL_SECONDS)
    current = _pid_state_ppid(entry_pid)
    if (
        current is not None
        and current[0] != "Z"
        and not _escalate(entry_pid, entry_ticks, owned_group, result)
    ):
        return

    reap_deadline = time.monotonic() + GRACE_SECONDS
    while time.monotonic() < reap_deadline:
        with contextlib.suppress(ChildProcessError):
            os.waitpid(entry_pid, os.WNOHANG)
        if _pid_state_ppid(entry_pid) is None:
            return
        time.sleep(POLL_SECONDS)
    result["contained"] = False


def _adopted_children(owner_pid: int) -> list[int]:
    """Enumerate direct adopted children of the subreaper owner.

    After the nested command dies, every orphaned descendant — at any depth
    — reparents directly to the nearest subreaper, so a single PPID scan
    discovers them all without trusting marker coverage.

    Args:
        owner_pid: The owner process whose adopted children to enumerate.

    Returns:
        Their PIDs.
    """
    found: list[int] = []
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            stat = (entry / "stat").read_bytes()
        except OSError:
            continue
        close = stat.rfind(b")")
        if close == -1:
            continue
        fields = stat[close + 2 :].split()
        if len(fields) < 4:
            continue
        if int(fields[1]) == owner_pid:
            found.append(int(entry.name))
    return found


def main(
    argv: list[str] | None = None,
    *,
    become_subreaper: Callable[[], bool] | None = None,
) -> int:
    """Run the nested command, then contain and reap its recorded identities.

    Fails closed: without proven subreaper ownership the nested command is
    never spawned, and marker coverage that is missing, unreadable,
    malformed, not a list, or contains non-object entries reports a
    containment failure instead of silently passing.  After the nested
    command exits, the owner repeatedly scans its adopted-descendant tree —
    reaping zombies and containing verifiable live children by the shared
    exact/group rules — until a full pass observes no adopted children; it
    never retires while a live/unreaped descendant exists.

    Args:
        argv: Argument vector; defaults to ``sys.argv[1:]``.
        become_subreaper: Injectable subreaper setup for tests.

    Returns:
        ``0`` when containment is positively proven, otherwise ``1``.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--marker", type=Path, required=True)
    parser.add_argument("--result", type=Path, required=True)
    parser.add_argument("--pidfile", type=Path, required=True)
    parser.add_argument("--deadline", type=float, default=120.0)
    parser.add_argument("command", nargs="+")
    args = parser.parse_args(argv)

    result: dict[str, object] = {
        "observed_ppid_1": False,
        "identity_mismatch": False,
        "unresolved_ticks": False,
        "survivor_seen": False,
        "contained": True,
    }
    if (become_subreaper or _become_subreaper)() is False:
        # Without subreaper ownership an orphaned descendant would reparent
        # to PID 1: refuse to run the nested command at all.
        result["subreaper"] = False
        result["contained"] = False
        args.result.write_text(json.dumps(result, sort_keys=True) + "\n", encoding="utf-8")
        return 1
    result["subreaper"] = True

    # Initialize marker storage as infrastructure only: an empty file means
    # "no process was ever registered", which is provable because every
    # successful guard registration appends its exact identity before
    # returning.  After the nested command exits the marker must still
    # exist and parse, or containment fails closed.
    args.marker.touch()

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

    entries, coverage_unproven = _read_marker(args.marker)
    if coverage_unproven:
        result["coverage_unproven"] = True
        result["contained"] = False

    for entry in entries:
        _process_entry(entry, result)

    _converge_adopted_tree(args, result)

    args.result.write_text(json.dumps(result, sort_keys=True) + "\n", encoding="utf-8")
    return 0 if result["contained"] is True else 1


def _converge_adopted_tree(args: argparse.Namespace, result: dict[str, object]) -> None:
    """Repeat discovery/containment until the adopted tree converges empty.

    The lifetime invariant: after the nested pytest exits, repeatedly
    enumerate the owner's direct adopted children (deeper descendants
    reparent to the subreaper when their intermediate parent is killed),
    reap zombies, and contain live children by exact PID+start-ticks with
    dedicated-group semantics; then rescan.  The owner may return only
    after a rescan finds zero children.  If a live child has unverifiable
    ticks or cannot be retired, diagnostics are persisted but the owner
    stays alive, keeps rescanning, and never orphans that child to PID 1.

    Args:
        args: Parsed CLI arguments (result path for durable diagnostics).
        result: Result dictionary updated in place.
    """
    while True:
        progressed = False
        stuck: list[int] = []
        for child_pid in _adopted_children(os.getpid()):
            outcome = _handle_adopted_child(child_pid, result)
            if outcome == "gone":
                progressed = True
            elif outcome == "stuck":
                stuck.append(child_pid)
        if not stuck:
            break
        if not progressed:
            # Unresolvable stall: persist diagnostics but never retire the
            # subreaper while a live/unreaped descendant exists.  Keep
            # rescanning on a bounded sleep so a later independent retirement
            # still converges the tree to empty.
            result["stalled_descendants"] = stuck
            result["contained"] = False
            args.result.write_text(json.dumps(result, sort_keys=True) + "\n", encoding="utf-8")
            time.sleep(POLL_SECONDS * 50)


def _handle_adopted_child(child_pid: int, result: dict[str, object]) -> str:
    """Contain one discovered adopted child of the subreaper.

    Args:
        child_pid: Exact PID of the discovered child.
        result: Result dictionary updated in place.

    Returns:
        ``"gone"`` when the child made a terminal transition or was already
        absent; ``"stuck"`` when it is live/unreaped and could not be
        retired this pass.
    """
    state = _pid_state_ppid(child_pid)
    if state is None:
        return "gone"
    if state[0] == "Z":
        with contextlib.suppress(ChildProcessError):
            os.waitpid(child_pid, os.WNOHANG)
        return "gone" if _pid_state_ppid(child_pid) is None else "stuck"
    child_ticks = _proc_start_ticks(child_pid)
    if child_ticks is None or child_ticks <= 0:
        # An unverifiable live child must not be signalled, and the owner
        # must not retire underneath it.
        result["unresolved_ticks"] = True
        result["contained"] = False
        return "stuck"
    before = _pid_state_ppid(child_pid)
    result["survivor_seen"] = True
    _contain(child_pid, child_ticks, result)
    after = _pid_state_ppid(child_pid)
    if before != after or (after is not None and after[0] == "Z"):
        return "progressed"
    return "stuck"


def _read_marker(marker: Path) -> tuple[list[dict[str, object]], bool]:
    """Read and validate the append-only JSONL marker file.

    The file is one ``{"pid": ..., "ticks": ...}`` JSON object per line,
    appended by every successful guard registration.  An empty file is
    valid proof that no process was ever registered.  A missing file, any
    unparseable line, or a torn final line without a terminating newline is
    fail-closed unproven coverage; a non-object line likewise.

    Args:
        marker: Marker path written by the nested run.

    Returns:
        The valid entries and whether coverage is unproven (fail closed).
    """
    try:
        raw = marker.read_text(encoding="utf-8")
    except OSError:
        return [], True
    if raw and not raw.endswith("\n"):
        # Torn write from a crash mid-append: never accept partial JSON.
        return [], True
    entries: list[dict[str, object]] = []
    for line in raw.splitlines():
        if not line.strip():
            continue
        try:
            entry = json.loads(line)
        except ValueError:
            return [], True
        if not isinstance(entry, dict):
            return [], True
        entries.append(entry)
    return entries, False


def _process_entry(entry: dict[str, object], result: dict[str, object]) -> None:
    """Contain one recorded descendant identity, failing closed.

    Args:
        entry: The raw marker entry.
        result: Result dictionary updated in place.
    """
    pid = entry.get("pid")
    ticks = entry.get("ticks")
    if not isinstance(pid, int):
        result["coverage_unproven"] = True
        result["contained"] = False
        return
    if not isinstance(ticks, int) or ticks <= 0:
        # Missing or invalid recorded ticks: containment is unresolved and
        # no signal is ever authorized for this identity.
        result["unresolved_ticks"] = True
        result["contained"] = False
        return
    current = _proc_start_ticks(pid)
    if current is not None and current != ticks:
        # A different live process occupies the PID: never signal it.
        result["identity_mismatch"] = True
        result["contained"] = False
        return
    _contain(pid, ticks, result)


if __name__ == "__main__":
    sys.exit(main())
