"""Deterministic process ownership and teardown for process-level tests.

Every process a process-level test spawns must be owned with an exact
identity and stopped deterministically on both success and assertion/error
paths.  This module provides:

- ``TRACKED`` — a session registry of every owned process, keyed by PID;
- ``register`` / ``register_owned`` / ``unregister`` — helpers for
  registering Popen children or exact incarnation identities;
- ``teardown_tracked`` — stops any still-live registered process, reaps
  Popen children normally and waits for /proc identity disappearance for
  adopted non-child identities, and raises when a test leaked a process;
- ``assert_no_live_tracked`` — asserts that no registered process survives;
- ``snapshot_pids`` / ``assert_no_persistent_leaks`` — external process-list
  proof that no test-spawned worker, supervisor, or PostgreSQL survives after
  teardown, even when processes were not registered with the guard.

Adoption from test-owned lifecycle/rollback metadata
-----------------------------------------------------

``OwnedProcess`` models a non-child process adopted from verified
test-owned metadata.  It stores the exact incarnation identity (PID,
start-time ticks, PGID/SID, token) and checks liveness through
``/proc/<pid>/stat`` incarnation proof rather than ``waitpid`` — adopted
processes are not children of the pytest process and would raise
``ChildProcessError`` on ``waitpid``.

The container runs under a real reaping PID 1 (tini) which reaps adopted
children, so this guard never installs a reaper or calls ``waitpid(-1)``:
it only ever waits on the exact processes the tests own.  Only exact
identities are ever signalled; nothing here inspects the production code
and nothing performs a broad process kill.
"""

from __future__ import annotations

import os
import signal
import time
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Final

from lubko.worker import group_has_members

if TYPE_CHECKING:
    import subprocess

KILL_GRACE_SECONDS: Final = 5.0
GROUP_POLL_SECONDS: Final = 0.02
_STAT_STATE_FIELD: Final = 0
_STAT_STARTTIME_FIELD_INDEX: Final = 19
_STAT_MIN_FIELDS: Final = 20

TRACKED: dict[int, subprocess.Popen[bytes] | OwnedProcess] = {}

# Module/session-scoped fixture process incarnations that are legitimately
# alive during per-test teardown.  Maps PID to the start-time ticks captured
# at registration.  Per-test ``assert_no_persistent_leaks`` exempts a PID only
# when its current /proc start ticks still match the registered value; a
# reused PID with different ticks is never exempt.
HIGHER_SCOPE_INCARNATIONS: dict[int, int] = {}


def register_persistent_fixture_incarnation(pid: int, ticks: int) -> None:
    """Register a module/session-scoped fixture's exact incarnation.

    Verifies the current /proc start-time ticks still match the provided
    value before recording, so a stale registration is never silently
    accepted.

    Args:
        pid: The exact PID of the higher-scope fixture process.
        ticks: The start-time ticks to register and verify.

    Raises:
        AssertionError: If the current /proc ticks do not match.
    """
    actual = _proc_start_ticks(pid)
    if actual is None or actual != ticks:
        msg = (
            f"cannot register higher-scope fixture pid {pid}: "
            f"expected ticks={ticks} but actual={actual}"
        )
        raise AssertionError(msg)
    HIGHER_SCOPE_INCARNATIONS[pid] = ticks


def unregister_persistent_fixture_incarnation(pid: int, ticks: int) -> None:
    """Remove a higher-scope fixture incarnation after teardown confirms it gone.

    Only removes when the registered value matches ``ticks``, so a stale
    callback cannot remove a newer registration for a reused PID.

    Args:
        pid: The exact PID to remove from the higher-scope registry.
        ticks: Expected registered ticks; must match to remove.
    """
    registered = HIGHER_SCOPE_INCARNATIONS.get(pid)
    if registered is not None and registered == ticks:
        HIGHER_SCOPE_INCARNATIONS.pop(pid, None)


def install_stale_ticks(pid: int, ticks: int) -> None:
    """Overwrite the registered start-time ticks for a PID (test-only helper).

    Installs a deliberate mismatch so ``assert_no_persistent_leaks`` will
    detect a stale/reused incarnation and NOT exempt it.  Only for use in
    unit regressions that prove the incarnation check is active.

    Args:
        pid: PID whose registered ticks to overwrite.
        ticks: Deliberately wrong tick value.
    """
    HIGHER_SCOPE_INCARNATIONS[pid] = ticks


def is_registered_incarnation_alive(pid: int) -> bool:
    """Return whether a higher-scope registered incarnation is still live.

    Checks both PID liveness and start-time-ticks match.  Used by fixture
    teardown to prove the exact incarnation is gone before unregistering.

    Args:
        pid: PID whose registered incarnation to verify.

    Returns:
        ``True`` when the PID is alive and its start ticks match the
        registered value.  ``False`` when the PID is absent, dead, or
        has different ticks (PID reuse).
    """
    expected = HIGHER_SCOPE_INCARNATIONS.get(pid)
    if expected is None:
        return False
    return _incarnation_alive(pid, expected)


# ---------------------------------------------------------------------------
# Incarnation helpers
# ---------------------------------------------------------------------------


def _proc_alive(pid: int) -> bool:
    """Return whether a process exists and is not a zombie.

    Args:
        pid: Process ID to probe.

    Returns:
        ``True`` when a running (non-zombie) process with that ID exists.
    """
    try:
        stat = (Path("/proc") / str(pid) / "stat").read_bytes()
    except OSError:
        return False
    close_paren = stat.rfind(b")")
    if close_paren == -1:
        return True
    fields = stat[close_paren + 2 :].split()
    if not fields:
        return True
    return fields[0] not in {b"Z", b"X"}


def _proc_start_ticks(pid: int) -> int | None:
    """Return a process start time in clock ticks, or ``None``.

    The start time is unique per process on a given boot and survives PID
    reuse, making it the reliable incarnation anchor.

    Args:
        pid: Process ID to inspect.

    Returns:
        The start time in clock ticks, or ``None`` when unreadable.
    """
    try:
        stat = (Path("/proc") / str(pid) / "stat").read_bytes()
    except OSError:
        return None
    close_paren = stat.rfind(b")")
    if close_paren == -1:
        return None
    fields = stat[close_paren + 2 :].split()
    if len(fields) < _STAT_MIN_FIELDS:
        return None
    try:
        return int(fields[_STAT_STARTTIME_FIELD_INDEX])
    except ValueError:
        return None


def _incarnation_alive(pid: int, ticks: int) -> bool:
    """Return whether the exact PID+ticks incarnation is live.

    Args:
        pid: Process ID to probe.
        ticks: Expected start-time in clock ticks.

    Returns:
        ``True`` when the PID is alive and the incarnation matches.
    """
    if not _proc_alive(pid):
        return False
    actual = _proc_start_ticks(pid)
    return actual is not None and actual == ticks


def process_alive(pid: int) -> bool:
    """Return whether a process exists and is not a zombie.

    Args:
        pid: Process ID to probe.

    Returns:
        ``True`` when a running (non-zombie) process with that ID exists.
    """
    return _proc_alive(pid)


def proc_start_ticks(pid: int) -> int | None:
    """Return a process start time in clock ticks, or ``None``.

    Public wrapper for ``_proc_start_ticks``.

    Args:
        pid: Process ID to inspect.

    Returns:
        The start time in clock ticks, or ``None`` when unreadable.
    """
    return _proc_start_ticks(pid)


# ---------------------------------------------------------------------------
# OwnedProcess — non-child process adopted from verified metadata
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class OwnedProcess:
    """Exact incarnation identity of an adopted non-child process.

    The process is not a child of the pytest process, so ``waitpid`` would
    raise ``ChildProcessError``.  Liveness is checked through
    ``/proc/<pid>/stat`` incarnation proof (PID + start-time ticks), and
    after signalling, the guard waits for the incarnation to disappear from
    ``/proc`` rather than calling ``waitpid``.
    """

    pid: int
    pgid: int
    start_time_ticks: int
    token: str | None = None

    def poll(self) -> int | None:
        """Return ``None`` while the incarnation is live, ``0`` when gone.

        Never raises ``ChildProcessError`` — liveness is checked via
        ``/proc`` incarnation proof, not ``waitpid``.
        """
        if _incarnation_alive(self.pid, self.start_time_ticks):
            return None
        return 0

    def is_alive(self) -> bool:
        """Return whether the exact incarnation is still live.

        Verifies PID+ticks coherence.  When a token is recorded, also
        checks the lifecycle-token environment marker.
        """
        if not _incarnation_alive(self.pid, self.start_time_ticks):
            return False
        if self.token is not None:
            marker = f"LUBKO_LIFECYCLE_TOKEN={self.token}".encode()
            try:
                environ = (Path("/proc") / str(self.pid) / "environ").read_bytes()
            except OSError:
                return False
            if marker not in environ.split(b"\0"):
                return False
        return True

    def wait_gone(self, timeout: float) -> bool:
        """Wait until the incarnation disappears from ``/proc``.

        Args:
            timeout: Maximum seconds to wait.

        Returns:
            ``True`` when the process is confirmed gone.
        """
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if not self.is_alive():
                return True
            time.sleep(GROUP_POLL_SECONDS)
        return not self.is_alive()


# ---------------------------------------------------------------------------
# Registry operations
# ---------------------------------------------------------------------------


def register(proc: subprocess.Popen[bytes]) -> None:
    """Track a spawned Popen child so teardown can stop it deterministically.

    Args:
        proc: The spawned process to own.
    """
    TRACKED[proc.pid] = proc


def register_owned(owned: OwnedProcess) -> None:
    """Track an adopted non-child process by its exact incarnation identity.

    Args:
        owned: The verified incarnation identity to own.
    """
    TRACKED[owned.pid] = owned


def unregister(proc: subprocess.Popen[bytes] | OwnedProcess) -> None:
    """Stop tracking a process that a test has already reaped or that is gone.

    Args:
        proc: The process to forget.
    """
    TRACKED.pop(proc.pid, None)


def tracked_pids() -> tuple[int, ...]:
    """Return the PIDs currently owned by the registry.

    Returns:
        The tracked process IDs.
    """
    return tuple(TRACKED)


def live_pids() -> list[int]:
    """Return the tracked PIDs whose processes are still running.

    Returns:
        The live tracked process IDs.
    """
    return [pid for pid, proc in TRACKED.items() if proc.poll() is None]


# ---------------------------------------------------------------------------
# Signalling helpers
# ---------------------------------------------------------------------------


def _process_group_of(pid: int) -> int | None:
    """Return the exact process group of ``pid``, or ``None`` when gone.

    Args:
        pid: Process whose group to resolve.

    Returns:
        The process group ID, or ``None`` when the process is gone.
    """
    try:
        return os.getpgid(pid)
    except ProcessLookupError:
        return None


def _signal_exact(pid: int, pgid: int | None, sig: int) -> None:
    """Signal a tracked process without ever touching a shared group.

    When the process leads its own dedicated group (``pgid == pid``) the
    whole group is signalled; otherwise only the exact PID is signalled, so
    a non-leader child that shares the pytest orchestrator's process group
    can never cause the orchestrator or its siblings to be killed.

    Args:
        pid: Exact process ID to signal.
        pgid: The process's group, or ``None``.
        sig: Signal to deliver.
    """
    if pgid is not None and pgid == pid:
        with suppress(ProcessLookupError):
            os.killpg(pgid, sig)
    else:
        with suppress(ProcessLookupError):
            os.kill(pid, sig)


def _group_clear(pgid: int | None, pid: int) -> bool:
    """Return whether no dedicated group remains to wait for.

    Args:
        pgid: The process's group, or ``None``.
        pid: The tracked process ID.

    Returns:
        ``True`` when teardown need not wait for group members.
    """
    if pgid is None or pgid != pid:
        return True
    return not group_has_members(pgid)


def _wait_group_gone(pgid: int) -> None:
    """Wait until a leader's dedicated process group has no live members.

    Args:
        pgid: The dedicated group to await.
    """
    deadline = time.monotonic() + KILL_GRACE_SECONDS
    while time.monotonic() < deadline and group_has_members(pgid):
        time.sleep(GROUP_POLL_SECONDS)


# ---------------------------------------------------------------------------
# Teardown
# ---------------------------------------------------------------------------


def _stop_popen(proc: subprocess.Popen[bytes]) -> None:
    """Stop a Popen child deterministically and reap it.

    Args:
        proc: The Popen process to stop.
    """
    pid = proc.pid
    pgid = _process_group_of(pid)
    _signal_exact(pid, pgid, signal.SIGTERM)
    deadline = time.monotonic() + KILL_GRACE_SECONDS
    while time.monotonic() < deadline:
        if proc.poll() is not None and _group_clear(pgid, pid):
            break
        time.sleep(GROUP_POLL_SECONDS)
    if proc.poll() is None and (pgid is None or pgid != pid or group_has_members(pgid)):
        _signal_exact(pid, pgid, signal.SIGKILL)
    if proc.poll() is None:
        with suppress(Exception):
            proc.wait(timeout=KILL_GRACE_SECONDS)
    if pgid is not None and pgid == pid:
        _wait_group_gone(pgid)


def _stop_owned(owned: OwnedProcess) -> None:
    """Stop an adopted non-child process by exact incarnation identity.

    The process is not a child of pytest, so ``waitpid`` would raise
    ``ChildProcessError``.  After signalling, the guard waits for the exact
    incarnation to disappear from ``/proc``; PID 1/tini performs the reap.

    Args:
        owned: The exact incarnation identity to stop.
    """
    pid = owned.pid
    pgid = owned.pgid or pid
    if not owned.is_alive():
        return
    _signal_exact(pid, pgid, signal.SIGTERM)
    owned.wait_gone(timeout=KILL_GRACE_SECONDS)
    if owned.is_alive():
        _signal_exact(pid, pgid, signal.SIGKILL)
        owned.wait_gone(timeout=KILL_GRACE_SECONDS)
    if pgid == pid:
        _wait_group_gone(pgid)


def _stop_one(proc: subprocess.Popen[bytes] | OwnedProcess) -> None:
    """Stop one tracked process deterministically.

    Popen children are reaped normally via ``waitpid``.  OwnedProcess
    non-children are waited on through ``/proc`` incarnation proof.

    Args:
        proc: The tracked process to stop.
    """
    if isinstance(proc, OwnedProcess):
        _stop_owned(proc)
    else:
        _stop_popen(proc)


def teardown_tracked(*, fail_on_leak: bool = True) -> int:
    """Stop every tracked process still live, returning how many leaked.

    Args:
        fail_on_leak: Whether a leftover live process is a hard failure.

    Returns:
        How many tracked processes were still live and had to be stopped.

    Raises:
        AssertionError: If ``fail_on_leak`` and any tracked process was still
            live, meaning a test failed to own and stop its own process.
    """
    procs = list(TRACKED.values())
    live = [proc for proc in procs if proc.poll() is None]
    for proc in live:
        _stop_one(proc)
    for proc in procs:
        TRACKED.pop(proc.pid, None)
    if fail_on_leak and live:
        msg = "test leaked process(es) that teardown had to stop: " + ", ".join(
            str(proc.pid) for proc in live
        )
        raise AssertionError(msg)
    return len(live)


def assert_no_live_tracked() -> None:
    """Assert that no test-created tracked process remains live.

    Raises:
        AssertionError: If any tracked process is still running after
            teardown, which would violate the acceptance criterion that
            repeated test runs do not increase the live process count.
    """
    live = live_pids()
    if live:
        msg = f"test-created processes still live after teardown: {live}"
        raise AssertionError(msg)


# ---------------------------------------------------------------------------
# External process-list proof (non-registry audit)
# ---------------------------------------------------------------------------


def _read_proc_stat(pid: int) -> tuple[str, int] | None:
    """Return the (state, ppid) pair from ``/proc/<pid>/stat``, or ``None``.

    Args:
        pid: Process ID to inspect.

    Returns:
        A ``(state, ppid)`` tuple, or ``None``.
    """
    try:
        stat = (Path("/proc") / str(pid) / "stat").read_bytes()
    except OSError:
        return None
    close_paren = stat.rfind(b")")
    if close_paren == -1:
        return None
    fields = stat[close_paren + 2 :].split()
    if len(fields) < 3:
        return None
    state = fields[_STAT_STATE_FIELD].decode("ascii", errors="replace")
    ppid = int(fields[1])
    return state, ppid


def _collect_live_pids() -> set[int]:
    """Iterate ``/proc`` and return all live (non-zombie) PIDs.

    Returns:
        A set of live process IDs.
    """
    live: set[int] = set()
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        pid = int(entry.name)
        info = _read_proc_stat(pid)
        if info is not None and info[0] not in {"Z", "X"}:
            live.add(pid)
    return live


def snapshot_pids() -> set[int]:
    """Return the set of all live (non-zombie) PIDs in the process table.

    Returns:
        A set of live process IDs.
    """
    try:
        return set(snapshot_incarnations())
    except OSError:
        return set()


def snapshot_incarnations() -> dict[int, int]:
    """Return all live PIDs mapped to their start-time ticks.

    A process is pre-existing only when both PID and start ticks match the
    snapshot; a reused PID with different ticks is treated as new and subject
    to owned-path leak checks.

    Returns:
        A mapping of live PID to start-time-in-clock-ticks.
    """
    incidences: dict[int, int] = {}
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        pid = int(entry.name)
        info = _read_proc_stat(pid)
        if info is not None and info[0] not in {"Z", "X"}:
            ticks = _proc_start_ticks(pid)
            if ticks is not None:
                incidences[pid] = ticks
    return incidences


def incarnation_is_preexisting(before: dict[int, int], pid: int, current_ticks: int) -> bool:
    """Return whether a process incarnation existed in the before snapshot.

    A process is pre-existing only when both PID and start ticks match the
    ``before`` snapshot.  A reused PID with different ticks is NOT
    pre-existing and is subject to owned-path leak checks.

    Args:
        before: Incarnation snapshot (PID → start-time ticks).
        pid: Current PID to check.
        current_ticks: Current start-time ticks for that PID.

    Returns:
        ``True`` when the PID existed in ``before`` with the same ticks.
    """
    before_ticks = before.get(pid)
    return before_ticks is not None and before_ticks == current_ticks


def _read_cmdline_bytes(pid: int) -> list[bytes]:
    """Read the NUL-separated argv of a process as raw bytes.

    Args:
        pid: Process ID to inspect.

    Returns:
        The list of argv entries, or ``[]`` when unreadable.
    """
    try:
        raw = (Path("/proc") / str(pid) / "cmdline").read_bytes()
    except OSError:
        return []
    return [part for part in raw.split(b"\0") if part]


def read_cmdline_bytes(pid: int) -> list[bytes]:
    """Read the NUL-separated argv of a process as raw bytes.

    Public alias for ``_read_cmdline_bytes``.

    Args:
        pid: Process ID to inspect.

    Returns:
        The list of argv entries, or ``[]`` when unreadable.
    """
    return _read_cmdline_bytes(pid)


def _argv_references_path(argv: list[bytes], owned: Path) -> bool:
    """Return whether any argv entry is or is under an owned path.

    Args:
        argv: Raw NUL-separated process arguments.
        owned: Owned directory prefix to match against.

    Returns:
        ``True`` when an argument references the owned path.
    """
    prefix = str(owned).encode() + b"/"
    exact = str(owned).encode()
    return any(arg == exact or arg.startswith(prefix) for arg in argv)


def argv_references_path(argv: list[bytes], owned: Path) -> bool:
    """Return whether any argv entry is or is under an owned path.

    Public alias for ``_argv_references_path``.

    Args:
        argv: Raw NUL-separated process arguments.
        owned: Owned directory prefix to match against.

    Returns:
        ``True`` when an argument references the owned path.
    """
    return _argv_references_path(argv, owned)


def _prune_stale_higher_scope() -> set[int]:
    """Revalidate higher-scope incarnations, prune stale entries, return alive PIDs.

    Returns:
        Set of PIDs whose registered incarnation still matches /proc.
    """
    alive: set[int] = set()
    stale_pids: list[int] = []
    for pid, expected_ticks in HIGHER_SCOPE_INCARNATIONS.items():
        actual_ticks = _proc_start_ticks(pid)
        if actual_ticks is not None and actual_ticks == expected_ticks:
            alive.add(pid)
        else:
            stale_pids.append(pid)
    for pid in stale_pids:
        HIGHER_SCOPE_INCARNATIONS.pop(pid, None)
    return alive


def _classify_incarnation(
    pid: int,
    current_ticks: int,
    before: dict[int, int],
    effective_allowed: set[int],
    owned: set[Path],
) -> tuple[int, list[bytes], int] | None:
    """Classify one current incarnation as a leak or not.

    Returns:
        A ``(pid, argv, ppid)`` leak record, or ``None`` if not a leak.
    """
    dominated = (
        pid in effective_allowed
        or incarnation_is_preexisting(before, pid, current_ticks)
        or not owned
    )
    if dominated:
        return None
    info = _read_proc_stat(pid)
    if info is None or info[0] in {"Z", "X"}:
        return None
    argv = _read_cmdline_bytes(pid)
    if not any(_argv_references_path(argv, path) for path in owned):
        return None
    return pid, argv, info[1]


def _format_leak_details(leaked: list[tuple[int, list[bytes], int]]) -> str:
    """Format leak records into a human-readable diagnostic message.

    Args:
        leaked: List of ``(pid, argv, ppid)`` leak records.

    Returns:
        Formatted diagnostic string.
    """
    details = []
    for pid, argv, ppid in leaked:
        cmdline_display = b" ".join(argv).decode("utf-8", "replace")
        reparented = "(reparented to PID 1)" if ppid == 1 else f"(ppid={ppid})"
        details.append(f"  pid={pid} {reparented} argv={cmdline_display!r}")
    return "persistent process leak detected after teardown:\n" + "\n".join(details)


def assert_no_persistent_leaks(
    before: dict[int, int],
    *,
    allowed: set[int] | None = None,
    owned_paths: set[Path] | None = None,
) -> None:
    """Assert no test-spawned process survives after teardown.

    Uses incarnation snapshots (PID + start-time ticks) so PID reuse is
    detected: a current process is pre-existing only when both PID and
    start ticks match the ``before`` snapshot; a reused PID with different
    ticks is new and subject to owned-path leak checks.

    Detection is purely assertion-based: no process is signalled.

    Args:
        before: Incarnation snapshot (PID → start-time ticks) taken before
            the test(s).
        allowed: PIDs that are allowed to appear as new processes.
        owned_paths: Directory prefixes that identify test-owned processes.

    Raises:
        AssertionError: If a persistent leak is detected.
    """
    effective_allowed = (allowed or set()) | _prune_stale_higher_scope()
    after = snapshot_incarnations()
    leaked = [
        record
        for pid, current_ticks in after.items()
        if (
            record := _classify_incarnation(
                pid, current_ticks, before, effective_allowed, owned_paths or set()
            )
        )
        is not None
    ]
    if leaked:
        raise AssertionError(_format_leak_details(leaked))
