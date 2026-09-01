#!/usr/bin/env python3
"""Lubko agent manager.

Manages long-running local AI agent sessions behind a stable, simple
command-line interface.  The orchestrator deals only with Lubko agent IDs
and Lubko commands; the underlying agent implementation, its session IDs,
its process tree, and its storage are hidden implementation details.

State lives per-user under ``$XDG_STATE_HOME/lubko`` (default
``$HOME/.local/state/lubko``).  Each agent is a directory under
``agents/<id>/`` containing ``meta.json``, ``output.log`` and a ``.lock``
file used to serialize metadata updates.
"""

from __future__ import annotations

import argparse
import codecs
import contextlib
import copy
import ctypes
import fcntl
import json
import math
import os
import platform
import re
import shutil
import signal
import sqlite3
import subprocess
import sys
import textwrap
import time
import uuid
from dataclasses import dataclass
from itertools import starmap
from pathlib import Path
from typing import TYPE_CHECKING, Any, BinaryIO, Final, cast

from lubko._exact_signal import pidfd_send_signal
from lubko.durable import write_text_durable
from lubko.worker import group_has_members

if TYPE_CHECKING:
    from collections.abc import Callable

# Agent metadata: a JSON-serializable mapping with heterogeneous values.
Meta = dict[str, Any]

# Implementation details (hidden from the user-facing interface).
AGENT_MODEL: Final = "opencode-go/hy3"
DEFAULT_VARIANT: Final = "low"
OPENCODE_TITLE_PREFIX: Final = "lubko-"  # native session title prefix used for discovery
TERMINAL_STATES: Final = ("succeeded", "failed", "stopped", "killed")
STOP_REASONS: Final = frozenset({"stop", "kill"})
PERSISTED_AGENT_STATES: Final = frozenset(("idle", "running", *TERMINAL_STATES))
PROG: Final = "lubko-agent"
HEX_DIGITS: Final = frozenset("0123456789abcdef")

# Environment variable carrying the durable, invocation-specific identity of a
# spawned agent invocation. Unlike the agent-wide ``LUBKO_AGENT_ID`` (shared by
# every invocation of the same agent across time), this token is freshly
# generated for each spawned invocation and inherited by its whole process
# tree, so signalling can never cross an invocation boundary even when the OS
# recycles a process-group ID into a newer invocation of the same agent.
INVOCATION_ID_VAR: Final = "LUBKO_INVOCATION_ID"
INVOCATION_ID_HEX_LENGTH: Final = 32

# ``SYS_pidfd_open`` uses the unified syscall number 434 on every architecture
# with a shared generic syscall table (x86_64, aarch64, riscv64, arm32, ppc64,
# s390x, loongarch). Architectures with private numbering (alpha, mips,
# parisc, sparc) are absent: there pinning is unsupported and signalling
# fails closed.
_PIDFD_OPEN_SYSCALL_NR: Final[dict[str, int]] = {
    "x86_64": 434,
    "aarch64": 434,
    "armv7l": 434,
    "armv8l": 434,
    "riscv64": 434,
    "ppc64": 434,
    "ppc64le": 434,
    "s390x": 434,
    "loongarch64": 434,
}
_LIBC_CACHE: Final[dict[str, ctypes.CDLL]] = {}

# Exit codes.
EXIT_OK: Final = 0
EXIT_ERROR: Final = 1
EXIT_USAGE: Final = 2
EXIT_NOT_FOUND: Final = 3
EXIT_TIMEOUT: Final = 124

# Tuning constants.
SECONDS_PER_MINUTE: Final = 60
MINUTES_PER_HOUR: Final = 60
HOURS_PER_DAY: Final = 24
SECONDS_PER_DAY: Final = 86400
PID_START_WINDOW_SECONDS: Final = 60
SESSION_DISCOVER_TIMEOUT_SECONDS: Final = 60
SESSION_DISCOVER_POLL_SECONDS: Final = 1
STOP_WAIT_SECONDS: Final = 10.0
KILL_WAIT_SECONDS: Final = 5.0
ABORT_WAIT_SECONDS: Final = 5.0
ABORT_REAP_SECONDS: Final = 5.0
IDLE_BREAK_SECONDS: Final = 5
STABLE_TERMINAL_SECONDS: Final = 0.5
STATUS_TAIL_LINES: Final = 50
FOLD_WIDTH: Final = 80
DEFAULT_RETENTION_DAYS: Final = 14
RUNNER_ARGV_LENGTH: Final = 3
AGENT_META_VERSION: Final = 3

_ANSI_CSI_RE = re.compile(r"\x1b\[[0-9:;<=>?]*[ -/]*[@-~]")
_ANSI_OSC_RE = re.compile(r"\x1b\][^\x07\x1b\n]*(?:\x07|\x1b\\)")
_ANSI_RE = re.compile(_ANSI_CSI_RE.pattern + "|" + _ANSI_OSC_RE.pattern)
_ANSI_CSI_PREFIX_RE = re.compile(r"\x1b(?:\[[0-9:;<=>?]*[ -/]*)?\Z")
_ANSI_OSC_PREFIX_RE = re.compile(r"\x1b\][^\x07\x1b\n]*(?:\x1b)?\Z")


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------


def _home() -> Path:
    return Path.home()


def state_root() -> Path:
    """Return the per-user Lubko state root following XDG conventions.

    Returns:
        ``$XDG_STATE_HOME/lubko``, falling back to ``~/.local/state/lubko``.
    """
    base = Path(os.environ.get("XDG_STATE_HOME") or (_home() / ".local" / "state"))
    return base / "lubko"


def agents_dir() -> Path:
    """Return the directory holding per-agent state.

    Returns:
        The agents state directory.
    """
    return state_root() / "agents"


def agent_dir(aid: str) -> Path:
    """Return the state directory for one agent.

    Args:
        aid: Lubko agent ID.

    Returns:
        The agent's state directory.
    """
    return agents_dir() / aid


def normalize_agent_id(raw: str | None) -> str | None:
    """Validate and normalize a caller-supplied base-16 agent ID.

    The ID must be a non-empty base-16 string. It is normalized by stripping
    surrounding whitespace and lower-casing hex digits; the result is preserved
    exactly as the stable Lubko agent identity.

    Args:
        raw: The raw caller-supplied ID.

    Returns:
        The normalized ID, or ``None`` when it is malformed.
    """
    if not raw:
        return None
    value = raw.strip().lower()
    if not value or any(char not in HEX_DIGITS for char in value):
        return None
    return value


def opencode_db_path() -> str:
    """Return the path of the underlying agent's session database, if present.

    Returns:
        The absolute database path, or an empty string when no database exists.
    """
    base = Path(os.environ.get("XDG_DATA_HOME") or (_home() / ".local" / "share"))
    db = base / "opencode" / "opencode.db"
    return str(db) if db.is_file() else ""


# ---------------------------------------------------------------------------
# Metadata
# ---------------------------------------------------------------------------


def read_meta(aid: str) -> Meta | None:
    """Load an agent's metadata, tolerating absence and corruption.

    Args:
        aid: Lubko agent ID.

    Returns:
        The metadata mapping, or ``None`` when unavailable.
    """
    path = agent_dir(aid) / "meta.json"
    try:
        with path.open(encoding="utf-8") as fh:
            data: Meta = json.load(fh)
            return data
    except (OSError, ValueError):
        return None


def write_meta(aid: str, meta: Meta) -> None:
    """Atomically replace an agent's metadata file.

    Args:
        aid: Lubko agent ID.
        meta: Metadata mapping to persist.
    """
    directory = agent_dir(aid)
    directory.mkdir(parents=True, exist_ok=True)
    # Crash-durable replace: fsync of file contents and the directory entry so
    # reconciled metadata can never be lost or half-written on power failure.
    payload = json.dumps(meta, indent=2, sort_keys=True) + "\n"
    write_text_durable(directory / "meta.json", payload)


def update_meta(aid: str, fn: Callable[[Meta], None]) -> None:
    """Apply ``fn(meta)`` to an agent's metadata under an exclusive lock.

    If the agent has been deleted, this is a no-op: a late background runner
    must never resurrect a deleted agent's directory.

    Args:
        aid: Lubko agent ID.
        fn: Mutation to apply to the metadata under the lock.
    """
    directory = agent_dir(aid)
    if not directory.is_dir():
        return
    lock_path = directory / ".lock"
    with contextlib.suppress(OSError), lock_path.open("w", encoding="utf-8") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            meta = read_meta(aid)
            if meta is None:
                return
            fn(meta)
            write_meta(aid, meta)
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def idle_meta(aid: str, cwd: str, title: str | None) -> Meta:
    """Build the metadata mapping of a freshly created, never-prompted agent.

    ``lubko-agent new`` only creates the managed session record: it launches no
    underlying AI invocation. The agent is idle until the first ``prompt``
    creates and starts the native session.

    Args:
        aid: Lubko agent ID.
        cwd: Working directory for the agent.
        title: Optional display title.

    Returns:
        The idle metadata mapping.
    """
    now = time.time()
    return {
        "id": aid,
        "created_at": now,
        "last_activity_at": now,
        "state": "idle",
        "cwd": cwd,
        "title": title,
        "variant": DEFAULT_VARIANT,
        "native_session_id": None,
        "pid": None,
        "pgid": None,
        "start_time": None,
        "invocation_id": None,
        "runner_pid": None,
        "runner_start_time": None,
        "started_at": None,
        "finished_at": None,
        "exit_code": None,
        "exit_signal": None,
        "intent": None,
        "delete_pending": False,
        "stop_reason": None,
        "active_runner": False,
        "runner_gen": 0,
        "runner_reservation": None,
        "unresolved_invocation": None,
        "steer_queue": [],
        "steer_seq": 0,
        "prompt_count": 0,
        "agent_version": AGENT_META_VERSION,
    }


# ---------------------------------------------------------------------------
# Process identity
# ---------------------------------------------------------------------------


def proc_start_ticks(pid: int) -> int | None:
    """Return the process start time in clock ticks (unique per boot).

    Args:
        pid: Process ID to inspect.

    Returns:
        The start time in clock ticks, or ``None`` when unavailable.
    """
    try:
        stat = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
    except OSError:
        return None
    rest = stat[stat.rfind(")") + 1 :].split()
    try:
        return int(rest[19])
    except (ValueError, IndexError):
        return None


def proc_cpu_seconds(pid: int | None) -> float | None:
    """Return the total CPU time in seconds used by a process, or ``None``.

    Reads the user and system CPU time of the process from Linux
    ``/proc/<pid>/stat`` and converts clock ticks to seconds.

    Args:
        pid: Process ID to inspect, or ``None``.

    Returns:
        The total CPU time in seconds, or ``None`` when unavailable.
    """
    if not pid:
        return None
    try:
        stat = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
    except OSError:
        return None
    rest = stat[stat.rfind(")") + 1 :].split()
    try:
        ticks = int(rest[11]) + int(rest[12])
    except (ValueError, IndexError):
        return None
    try:
        ticks_per_second = os.sysconf("SC_CLK_TCK")
    except (ValueError, OSError):
        return None
    if not ticks_per_second:
        return None
    return ticks / ticks_per_second


def env_has_marker(pid: int, aid: str) -> bool:
    """Return whether a process environment carries the exact agent marker.

    The marker is matched against whole NUL-separated environment entries so
    an ID that is a prefix of another (for example ``a1b2c3d4`` inside
    ``a1b2c3d45``) can never be mistaken for the exact session identity.

    Args:
        pid: Process whose environment to inspect.
        aid: Exact agent ID the marker must match.

    Returns:
        ``True`` only when an exact ``LUBKO_AGENT_ID=<aid>`` entry is present.
    """
    return _env_has_entry(pid, f"LUBKO_AGENT_ID={aid}".encode())


def env_has_invocation(pid: int, iid: str) -> bool:
    """Return whether a process environment carries the exact invocation marker.

    Args:
        pid: Process whose environment to inspect.
        iid: Exact invocation ID the marker must match.

    Returns:
        ``True`` only when an exact ``LUBKO_INVOCATION_ID=<iid>`` entry is
        present.
    """
    return _env_has_entry(pid, f"{INVOCATION_ID_VAR}={iid}".encode())


def _env_has_entry(pid: int, entry: bytes) -> bool:
    """Return whether a process environment contains the exact NUL-delimited entry.

    Args:
        pid: Process whose environment to inspect.
        entry: Whole ``KEY=VALUE`` environment entry required.

    Returns:
        ``True`` only when the exact entry is present.
    """
    try:
        environ = Path(f"/proc/{pid}/environ").read_bytes()
    except OSError:
        return False
    return entry in environ.split(b"\0")


def _proven_invocation_members(pgid: int, aid: str, iid: str) -> tuple[list[tuple[int, int]], bool]:
    """Pinned exact-member scan with proven-complete evidence.

    Like ``_pinned_invocation_members`` for signalling, but the caller also
    learns whether the scan *completed*: ``/proc`` enumeration failure, an
    alive-but-unpinnable candidate, or uninspectable marker data all yield
    ``complete=False`` so ownership decisions can fail closed. A candidate
    that positively vanishes mid-scan is a benign race and stays complete.

    Args:
        pgid: The recorded process group ID.
        aid: Exact agent ID whose environment marker members must carry.
        iid: Exact invocation ID whose environment marker members must carry.

    Returns:
        ``(members, complete)`` — pinned member PIDs with open pidfds (caller
        closes) and whether absence of further members was positively proven.
    """
    try:
        entries = list(Path("/proc").iterdir())
    except OSError:
        return [], False  # enumeration failure: membership unknown
    members: list[tuple[int, int]] = []
    for entry in entries:
        name = entry.name
        if not name.isdigit():
            continue
        member = int(name)
        if member == pgid:
            continue
        fd = open_pidfd(member)
        if fd is None:
            # Distinguish a benign mid-scan disappearance from an unprovable
            # pin (platform without pidfd support, or another pin failure).
            try:
                os.kill(member, 0)
            except ProcessLookupError:
                continue  # positively vanished: benign race
            except OSError:
                return members, False  # probe itself failed: ambiguity
            return members, False  # alive but unpinnable: ambiguity
        matched = _matches_invocation_group_proven(member, pgid, aid, iid)
        if matched is None:
            os.close(fd)
            return members, False  # marker inspection uninspectable: ambiguity
        if not matched:
            os.close(fd)
            continue
        members.append((member, fd))
    return members, True


def _matches_invocation_group_proven(member: int, pgid: int, aid: str, iid: str) -> bool | None:
    """Tri-state exact invocation-group membership check.

    Args:
        member: Candidate process ID.
        pgid: The recorded process group ID.
        aid: Exact agent ID whose environment marker must be present.
        iid: Exact invocation ID whose environment marker must be present.

    Returns:
        ``True`` when group and both markers match exactly, ``False`` on a
        positive mismatch or benign disappearance, ``None`` when inspection
        is ambiguous (unreadable procfs).
    """
    try:
        if os.getpgid(member) != pgid:
            return False
    except ProcessLookupError:
        return False
    except OSError:
        return None
    environ: bytes | None
    try:
        environ = Path(f"/proc/{member}/environ").read_bytes()
    except FileNotFoundError:
        return False  # vanished mid-scan: benign race
    except OSError:
        return None  # exists but uninspectable: ambiguity
    entries = environ.split(b"\0")
    has_aid = f"LUBKO_AGENT_ID={aid}".encode() in entries
    has_iid = f"{INVOCATION_ID_VAR}={iid}".encode() in entries
    return has_aid and has_iid


def open_pidfd(pid: int) -> int | None:
    """Return a file descriptor pinning ``pid``, or ``None`` when unavailable.

    A pidfd holds a reference to the kernel's PID structure, so the operating
    system can never recycle that PID while the descriptor is open — even if
    the process exits and is reaped. This is what makes check-then-signal
    sequences race-free: identity verified through the pin refers to exactly
    the process that is later signalled.

    Prefers ``os.pidfd_open``; falls back to a narrowly encapsulated raw
    ``SYS_pidfd_open`` syscall via ``ctypes`` on architectures with the
    unified syscall number. Returns ``None`` when the process is gone
    (``ESRCH``) or when the platform cannot pin PIDs at all, in which case
    callers must fail closed rather than signal an unpinned target.

    Args:
        pid: Process ID to pin.

    Returns:
        A pidfd file descriptor, or ``None``.
    """
    pidfd_open = getattr(os, "pidfd_open", None)
    if pidfd_open is not None:
        try:
            return int(pidfd_open(int(pid)))
        except OSError:
            return None
    nr = _PIDFD_OPEN_SYSCALL_NR.get(platform.machine())
    libc = _load_libc()
    if nr is None or libc is None:
        return None
    result = libc.syscall(ctypes.c_long(nr), ctypes.c_int(pid), ctypes.c_uint(0))
    return int(result) if result >= 0 else None


def _load_libc() -> ctypes.CDLL | None:
    """Return a cached handle to the C library, or ``None``.

    Returns:
        The ``ctypes`` C library handle, or ``None`` when unavailable.
    """
    if "libc" not in _LIBC_CACHE:
        try:
            libc = ctypes.CDLL(None, use_errno=True)
            libc.syscall.restype = ctypes.c_long
        except OSError:
            return None
        _LIBC_CACHE["libc"] = libc
    return _LIBC_CACHE["libc"]


def signal_identity_checked(
    pid: int,
    start_time: object,
    sig: int,
    marker_aid: str | None = None,
) -> None:
    """Deliver ``sig`` to exactly the pinned-and-verified single process.

    The PID is pinned with a pidfd before verification, so it cannot be
    recycled between verification and delivery: a recorded runner PID that was
    already reused by an unrelated process never matches and is never
    signalled, and delivery itself goes through ``pidfd_send_signal`` on the
    very descriptor that pinned the verified process — a numeric ``kill``
    could still retarget onto an unrelated occupant of the recycled PID even
    after a successful proof, because the kernel frees a numeric PID for reuse
    before the pinned reference is released. When the platform cannot pin PIDs
    — or can pin them but has no ``pidfd_send_signal`` binding — the signal is
    withheld (fail closed).

    Args:
        pid: Recorded runner PID to signal.
        start_time: Recorded start time in clock ticks that must match.
        sig: Signal to deliver.
        marker_aid: Exact agent ID whose environment marker must be present.
    """
    fd = open_pidfd(int(pid))
    if fd is None:
        return
    try:
        if proc_start_ticks(int(pid)) != start_time:
            return
        if marker_aid is not None and not env_has_marker(int(pid), marker_aid):
            return
        # Fail closed when the platform can pin PIDs but cannot deliver
        # pidfd signals: withhold the signal rather than crash or fall back
        # to a numeric kill.
        with contextlib.suppress(OSError, AttributeError):
            pidfd_send_signal(fd, sig)
    finally:
        os.close(fd)


def send_signal_group(meta: Meta, sig: int) -> None:
    """Deliver a signal to the agent's exact recorded invocation group.

    Race-free by construction: every signalled process is first pinned with a
    pidfd and then verified against the recorded invocation identity at the
    signal point itself — and every delivery goes through ``pidfd_send_signal``
    on the very descriptor that pinned the proven process. No numeric
    ``killpg``/``kill`` syscall is ever issued: the kernel frees a numeric PID
    or PGID for reuse before the pinned process's ``struct pid`` reference is
    released, so a numeric signal could retarget onto an unrelated occupant
    even after a successful proof. Two paths:

    - Live leader: the leader is pinned, its start time and both markers are
      verified under the pin, and it is signalled through its own pin.
    - Dead (or unverifiable) leader: any surviving members of the recorded
      group are converged one member at a time, each individually pinned and
      required to carry the recorded PGID, the exact agent marker, *and* the
      exact durable per-invocation marker. A newer invocation of the same
      agent (with a different invocation ID) is therefore never signalled,
      no matter how the OS recycled PIDs or the group ID.

    When no process can be pinned (platform without pidfd support), or when
    the platform cannot deliver pidfd signals, nothing is signalled: fail
    closed instead of guessing. The same holds when no durable invocation
    identity is recorded.

    Args:
        meta: Agent metadata.
        sig: Signal to deliver.
    """
    leader = _process_identity_int(meta.get("pid"), minimum=1)
    start_time = _process_identity_int(meta.get("start_time"), minimum=0)
    aid = _persisted_agent_id(meta.get("id"))
    iid = _persisted_invocation_id(meta.get("invocation_id"))
    raw_pgid = meta.get("pgid")
    pgid = leader if raw_pgid is None else _process_identity_int(raw_pgid, minimum=1)
    if leader is None or start_time is None or pgid is None or aid is None or iid is None:
        return
    fd = open_pidfd(leader)
    if fd is not None:
        try:
            verified = (
                proc_start_ticks(leader) == start_time
                and env_has_marker(leader, aid)
                and env_has_invocation(leader, iid)
            )
            if verified:
                # Deliver through the pin itself: a numeric killpg on the
                # recorded group could retarget after PGID reuse.
                with contextlib.suppress(OSError, AttributeError):
                    pidfd_send_signal(fd, sig)
        finally:
            os.close(fd)
    # Converge surviving members of the recorded group through the exact
    # per-member pinned path — only with a durable invocation-specific
    # identity to authorize them.
    for _member, member_fd in _pinned_invocation_members(pgid, aid, iid):
        try:
            with contextlib.suppress(OSError, AttributeError):
                pidfd_send_signal(member_fd, sig)
        finally:
            os.close(member_fd)


def _pinned_invocation_members(pgid: int, aid: str, iid: str) -> list[tuple[int, int]]:
    """Return ``(pid, pidfd)`` pairs for surviving members of one invocation group.

    Each candidate process is pinned with a pidfd before any check, so the
    pgid/marker verification refers to exactly the returned, unrecyclable
    process. Only processes inside the recorded group carrying both the exact
    agent marker and the exact invocation marker qualify. Candidates whose own
    PID equals the recorded PGID are always skipped: after the recorded leader
    died, that slot may have been recycled into a newer session (or a newer
    invocation's leader), and the invocation marker alone decides membership.

    Args:
        pgid: The recorded process group ID.
        aid: Exact agent ID whose environment marker members must carry.
        iid: Exact invocation ID whose environment marker members must carry.

    Returns:
        Pinned member PIDs with their still-open pidfds (caller closes).
    """
    members: list[tuple[int, int]] = []
    with contextlib.suppress(OSError):
        for entry in Path("/proc").iterdir():
            name = entry.name
            if not name.isdigit():
                continue
            member = int(name)
            if member == pgid:
                continue
            fd = open_pidfd(member)
            if fd is None:
                continue
            try:
                matched = _matches_invocation_group(member, pgid, aid, iid)
            except BaseException:
                os.close(fd)
                raise
            if not matched:
                # Ownership: a rejected candidate must never keep its pin.
                os.close(fd)
                continue
            members.append((member, fd))
    return members


def _matches_invocation_group(member: int, pgid: int, aid: str, iid: str) -> bool:
    """Return whether a process belongs to one exact recorded invocation group.

    Args:
        member: Candidate process ID.
        pgid: The recorded process group ID.
        aid: Exact agent ID whose environment marker must be present.
        iid: Exact invocation ID whose environment marker must be present.

    Returns:
        ``True`` only when group and both markers match exactly.
    """
    try:
        if os.getpgid(member) != pgid:
            return False
    except ProcessLookupError:
        return False
    return env_has_marker(member, aid) and env_has_invocation(member, iid)


def _process_identity_int(value: object, *, minimum: int) -> int | None:
    """Return a strict persisted JSON integer used as process identity.

    Python's ``bool`  is an ``int`` subclass and ``int()`` also accepts
    floats/strings, so durable identity fields must be validated before they
    can name a process or establish a start-time match.
    """
    if not isinstance(value, int) or isinstance(value, bool) or value < minimum:
        return None
    return value


def _persisted_agent_id(value: object) -> str | None:
    """Return an already-canonical persisted agent ID without normalizing it."""
    if not isinstance(value, str) or normalize_agent_id(value) != value:
        return None
    return value


def _persisted_invocation_id(value: object) -> str | None:
    """Return an exact canonical UUID-hex invocation marker."""
    if (
        not isinstance(value, str)
        or len(value) != INVOCATION_ID_HEX_LENGTH
        or any(char not in HEX_DIGITS for char in value)
    ):
        return None
    return value


def is_alive(meta: Meta) -> bool:
    """Return whether the recorded process is really our agent process.

    The numeric invocation PID is pinned before its start-time and per-agent
    environment marker are checked. Final liveness is then proven through the
    same pidfd, so exit and PID reuse after the identity checks cannot make an
    unrelated process justify the recorded invocation. Platforms that cannot
    provide the stable pin or pidfd liveness probe fail closed.

    Args:
        meta: Agent metadata.

    Returns:
        ``True`` only when the same pinned live process matches every recorded
        invocation identity field.
    """
    pid = _process_identity_int(meta.get("pid"), minimum=1)
    start_time = _process_identity_int(meta.get("start_time"), minimum=0)
    aid = _persisted_agent_id(meta.get("id"))
    if pid is None or start_time is None or aid is None:
        return False
    invocation_pid = pid
    fd = open_pidfd(invocation_pid)
    if fd is None:
        return False
    try:
        if proc_start_ticks(invocation_pid) != start_time:
            return False
        if not env_has_marker(invocation_pid, aid):
            return False
        try:
            pidfd_send_signal(fd, 0)
        except (OSError, AttributeError):
            return False
        return True
    finally:
        os.close(fd)


def _recorded_leader_state(meta: Meta) -> str:
    """Tri-state survival evidence for a metadata record's leader.

    Args:
        meta: Agent metadata carrying ``pid``, ``pgid``, ``start_time``,
            ``id``, and ``invocation_id``.

    Returns:
        ``"live"`` when the pinned identity positively matches including its
        environment markers, ``"gone"`` when the PID is provably dead or
        recycled by an unrelated occupant, ``"ambiguous"`` when evidence
        cannot be inspected (unreadable procfs) — never collapsing ambiguity
        into death.
    """
    raw_pid = meta.get("pid")
    if raw_pid is None:
        return "gone"
    pid = _process_identity_int(raw_pid, minimum=1)
    start_time = _process_identity_int(meta.get("start_time"), minimum=0)
    if pid is None or start_time is None:
        return "ambiguous"
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return "gone"
    except OSError:
        return "ambiguous"  # exists but uninspectable
    ticks = proc_start_ticks(pid)
    if ticks is None or ticks != start_time:
        # Unreadable ticks are ambiguity; readable different ticks prove the
        # slot was recycled by an unrelated occupant.
        return "ambiguous" if ticks is None else "gone"
    state = _leader_marker_state(meta, pid)
    return state if state is not None else "ambiguous"


def _leader_marker_state(meta: Meta, pid: int) -> str | None:
    """Marker-based verdict for a start-time-matching recorded leader.

    Args:
        meta: Agent metadata.
        pid: The live leader PID whose markers decide exact identity.

    Returns:
        ``"live"``/``"gone"`` when markers positively decide, ``None`` when
        marker inspection is ambiguous and the caller must stay conservative.
    """
    aid = _persisted_agent_id(meta.get("id"))
    if aid is None:
        return None
    iid_raw = meta.get("invocation_id")
    if iid_raw is None:
        return "live" if env_has_marker(pid, aid) else "gone"
    iid = _persisted_invocation_id(iid_raw)
    if iid is None:
        return None
    try:
        environ = Path(f"/proc/{pid}/environ").read_bytes()
    except FileNotFoundError:
        return "gone"  # vanished mid-check: benign race
    except OSError:
        return None  # marker evidence uninspectable: stay conservative
    entries = environ.split(b"\0")
    exact = (
        f"LUBKO_AGENT_ID={aid}".encode() in entries
        and f"{INVOCATION_ID_VAR}={iid}".encode() in entries
    )
    return "live" if exact else "gone"


def group_alive(meta: Meta) -> bool:  # ruff: ignore[too-many-return-statements]
    """Return whether any live process remains in the agent's exact invocation group.

    When a durable invocation ID was recorded, group membership is decided by
    the invocation-exact pinned scan: a recycled PGID hosting a newer
    invocation of the same agent never counts as this invocation's survivors,
    an *incomplete* scan (procfs enumeration failure, unpinnable or
    uninspectable candidate) conservatively counts as alive so convergence
    can never fail open, and an ambiguous recorded-leader identity likewise
    counts as alive. Without a recorded invocation ID the plain
    process-group membership is used (legacy metadata).

    Args:
        meta: Agent metadata.

    Returns:
        ``True`` when the recorded invocation still has live group members,
        or when their absence could not be positively proven.
    """
    raw_pgid = meta.get("pgid")
    if raw_pgid is None:
        return False
    pgid = _process_identity_int(raw_pgid, minimum=1)
    if pgid is None:
        return True
    iid_raw = meta.get("invocation_id")
    if iid_raw is None:
        return bool(group_has_members(pgid))
    aid = _persisted_agent_id(meta.get("id"))
    iid = _persisted_invocation_id(iid_raw)
    leader = _process_identity_int(meta.get("pid"), minimum=1)
    start_time = _process_identity_int(meta.get("start_time"), minimum=0)
    if aid is None or iid is None or leader is None or start_time is None:
        return True
    if is_alive(meta):
        # The verified live leader implies its whole session group.
        return True
    leader_state = _recorded_leader_state(meta)
    if leader_state != "gone":
        # A live leader implies its session group; ambiguous leader evidence
        # (e.g. unreadable environ with matching start ticks) must never be
        # collapsed into a proven-dead group.
        return True
    fds, complete = _proven_invocation_members(pgid, aid, iid)
    for _, fd in fds:
        os.close(fd)
    if not complete:
        # Ambiguous evidence must never be read as a proven-empty group.
        return True
    return bool(fds)


def wait_group_dead(meta: Meta, timeout: float) -> bool:
    """Wait until the agent's exact process group has no live members.

    The recorded leader PID alone is not enough: an agent run may leave a
    member of its own process group behind (for example an OpenCode child
    that ignored ``SIGTERM``), so stop/kill must confirm the whole exact
    group is gone rather than only the tracked leader.

    Args:
        meta: Agent metadata.
        timeout: Maximum seconds to wait.

    Returns:
        ``True`` when the process group is gone within the timeout.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        if not group_alive(meta):
            return True
        time.sleep(0.2)
    return not group_alive(meta)


def runner_alive(meta: Meta) -> bool:
    """Return whether the recorded background runner is really our runner.

    The numeric runner PID is pinned before its start-time and per-agent
    environment marker are checked. Final liveness is then proven through the
    same pidfd, so an exit and PID reuse after the identity checks can never
    make an unrelated process justify the recorded runner. Platforms that
    cannot provide the stable pin or pidfd liveness probe fail closed.

    Args:
        meta: Agent metadata.

    Returns:
        ``True`` only when the same pinned live process matches every recorded
        runner identity field.
    """
    pid = _process_identity_int(meta.get("runner_pid"), minimum=1)
    start_time = _process_identity_int(meta.get("runner_start_time"), minimum=0)
    aid = _persisted_agent_id(meta.get("id"))
    if pid is None or start_time is None or aid is None:
        return False
    runner_pid = pid
    fd = open_pidfd(runner_pid)
    if fd is None:
        return False
    try:
        if proc_start_ticks(runner_pid) != start_time:
            return False
        if not env_has_marker(runner_pid, aid):
            return False
        try:
            pidfd_send_signal(fd, 0)
        except (OSError, AttributeError):
            return False
        return True
    finally:
        os.close(fd)


def pid_alive(pid: int | None) -> bool:
    """Return whether a process ID still names a live process.

    Args:
        pid: Process ID to probe, or ``None``.

    Returns:
        ``True`` only when ``pid`` is a live process.
    """
    if not pid:
        return False
    try:
        os.kill(int(pid), 0)
    except OSError:
        return False
    return True


def _runner_marker_alive(aid: str, expected_gen: int) -> bool:
    """Return whether a live runner for ``expected_gen`` exists for this agent.

    A reserved-but-not-yet-claimed runner (or its children) is the only process
    that sets both ``LUBKO_AGENT_ID`` and ``LUBKO_RUNNER_GEN`` for this agent,
    so a marker carrying the *exact* generation proves a runner for the
    current reservation is genuinely being brought up.  A stale process from an
    older (or newer) generation must never justify the current reservation:
    an alive old-generation runner that bailed without claiming must not block
    recovery of a newer reservation.

    Args:
        aid: Exact agent ID whose marker to look for.
        expected_gen: The runner reservation generation that must be proven.

    Returns:
        ``True`` when a live process with the exact agent and generation marker
        exists.
    """
    agent_marker = f"LUBKO_AGENT_ID={aid}".encode()
    gen_marker = f"LUBKO_RUNNER_GEN={expected_gen}".encode()
    proc_root = Path("/proc")
    if not proc_root.is_dir():
        return False
    for entry in proc_root.iterdir():
        name = entry.name
        if not name.isdigit():
            continue
        fd = open_pidfd(int(name))
        if fd is None:
            continue
        try:
            try:
                environ = (entry / "environ").read_bytes()
            except OSError:
                continue
            fields = environ.split(b"\0")
            if agent_marker not in fields or gen_marker not in fields:
                continue
            try:
                pidfd_send_signal(fd, 0)
            except (OSError, AttributeError):
                continue
            return True
        finally:
            os.close(fd)
    return False


def _is_zombie(pid: int) -> bool:
    """Return whether a live PID names a zombie (defunct) process.

    A zombie can no longer do work, so it must never be trusted as a live
    owner of a reservation.

    Args:
        pid: Process ID to inspect.

    Returns:
        ``True`` when the process is a zombie and cannot bring up a runner.
    """
    try:
        stat = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
    except OSError:
        return False
    rest = stat[stat.rfind(")") + 1 :].split()
    if not rest:
        return False
    return rest[0] == "Z"


def _owner_alive(owner: object, owner_ticks: object) -> bool:
    """Return whether ``owner`` is still the exact live reservation owner.

    The recorded PID is pinned before its start-time proof and final liveness
    acceptance. This prevents an owner that exits during validation, or a later
    process that reuses its numeric PID, from justifying the reservation.
    Unavailable pidfd/procfs evidence and zombie owners fail closed.

    Args:
        owner: Recorded owner process ID, or ``None``.
        owner_ticks: Recorded owner start time in clock ticks, or ``None``.

    Returns:
        ``True`` only when the same pinned live process is the exact recorded
        reservation owner.
    """
    if not isinstance(owner, int) or isinstance(owner, bool):
        return False
    owner_pid = int(owner)
    fd = open_pidfd(owner_pid)
    if fd is None:
        return False
    try:
        if proc_start_ticks(owner_pid) != owner_ticks:
            return False
        if _is_zombie(owner_pid):
            return False
        try:
            pidfd_send_signal(fd, 0)
        except (OSError, AttributeError):
            return False
        return True
    finally:
        os.close(fd)


def _runner_generation(value: object, *, minimum: int) -> int | None:
    """Return a canonical persisted runner generation or ``None``.

    Durable generation authority is JSON-integer only. Booleans, numeric
    strings, floats, and values below the caller's domain minimum fail closed.
    """
    if type(value) is not int or value < minimum:
        return None
    return value


def _runner_reservation_mode(reservation: object) -> str | None:
    """Return canonical durable runner native-session mode, or ``None``.

    A runner reservation carries execution authority for exactly one of the two
    supported native-session modes. Missing, malformed, or unsupported values
    must fail closed rather than being defaulted through truthiness or coerced
    to strings later in the spawn path.
    """
    if not isinstance(reservation, dict):
        return None
    mode = reservation.get("mode")
    if type(mode) is not str or mode not in {"new", "continue"}:
        return None
    return mode


def _next_prompt_count(meta: Meta) -> int | None:
    """Return the next canonical durable prompt count, or fail closed.

    Genuine absence is the legacy zero-count state. A present value must be an
    actual non-negative JSON integer; booleans and coercible strings/floats are
    malformed durable state and must never be normalized by an acceptance path.
    """
    if "prompt_count" not in meta:
        return 1
    value = meta["prompt_count"]
    if type(value) is not int or value < 0:
        return None
    return value + 1


def _active_runner_flag(meta: Meta) -> bool | None:
    """Return canonical durable runner-consumption authority.

    ``active_runner`` is persisted JSON authority, so only literal booleans are
    usable. Historical records that genuinely omit the field predate runner
    reservations and safely mean inactive; present malformed values remain
    distinguishable as ``None`` and must block authority-changing operations.
    """
    if "active_runner" not in meta:
        return False
    value = meta.get("active_runner")
    if type(value) is not bool:
        return None
    return value


def _delete_pending_flag(meta: Meta) -> bool | None:
    """Return canonical durable deletion-tombstone authority.

    Genuine field absence remains the legacy non-tombstone state. A present
    value is lifecycle authority only when it is a literal JSON boolean;
    malformed values remain distinguishable as ``None`` so callers can block
    execution or deletion rather than normalizing through truthiness.
    """
    if "delete_pending" not in meta:
        return False
    value = meta.get("delete_pending")
    if type(value) is not bool:
        return None
    return value


def reservation_in_flight(meta: Meta) -> bool:
    """Return whether a reserved runner is still being brought up.

    A reservation is in flight when the runner's exact identity is already
    proven live, or a reserved (not yet claimed) runner is still owned by the
    exact live spawner (matching PID and start ticks), or a reserved runner
    process exists but has not yet recorded its exact identity.

    Args:
        meta: Agent metadata.

    Returns:
        ``True`` while a reserved runner is expected to claim the agent.
    """
    if _active_runner_flag(meta) is not True:
        return False
    if runner_alive(meta):
        return True
    res = meta.get("runner_reservation")
    if (
        not isinstance(res, dict)
        or res.get("state") != "reserved"
        or _runner_reservation_mode(res) is None
    ):
        return False
    gen = _runner_generation(res.get("gen"), minimum=1)
    if gen is None:
        return False
    if _owner_alive(res.get("owner_pid"), res.get("owner_start_ticks")):
        return True
    aid = _persisted_agent_id(meta.get("id"))
    return aid is not None and _runner_marker_alive(aid, gen)


def owned_by_me(meta: Meta, caller_pid: int) -> bool:
    """Return whether the current runner reservation is owned by ``caller_pid``.

    The reservation is owned by the caller only when its PID *and* its start
    ticks both match.  Both the current and the recorded start ticks must be
    valid (not ``None``) and equal, so a missing/unreadable tick record or a
    reused PID that belongs to an unrelated process can never be mistaken for
    the exact original owner.

    Args:
        meta: Agent metadata.
        caller_pid: PID of the calling process.

    Returns:
        ``True`` when the live reservation names ``caller_pid`` as the exact
        owner.
    """
    res = meta.get("runner_reservation")
    if not isinstance(res, dict):
        return False
    owner_pid = _process_identity_int(res.get("owner_pid"), minimum=1)
    recorded = _process_identity_int(res.get("owner_start_ticks"), minimum=0)
    if owner_pid != caller_pid or recorded is None:
        return False
    current = proc_start_ticks(caller_pid)
    return current is not None and current == recorded


def is_genuinely_running(meta: Meta) -> bool:
    """Return whether the agent is really executing an invocation.

    Genuinely running means a live agent invocation process, a proven-live
    runner, or a reserved runner still being brought up.  A merely reserved
    agent whose spawner died and whose runner never claimed is *not* genuinely
    running and must be recoverable.

    Args:
        meta: Agent metadata, or ``None``.

    Returns:
        ``True`` when an invocation is genuinely in progress.
    """
    if not meta:
        return False
    if is_alive(meta):
        return True
    if runner_alive(meta):
        return True
    return reservation_in_flight(meta)


def active_runner_justified(meta: Meta) -> bool:
    """Return whether ``active_runner`` is backed by a real or reserved runner.

    ``active_runner`` may be true only with a proven-live runner identity or an
    explicit recoverable reservation.  This is the central invariant the
    linearizable prompt protocol must preserve.

    Args:
        meta: Agent metadata.

    Returns:
        ``True`` when ``active_runner`` is justified.
    """
    active_runner = _active_runner_flag(meta)
    if active_runner is None:
        return False
    if not active_runner:
        return True
    if runner_alive(meta):
        return True
    res = meta.get("runner_reservation")
    # A ``reserved`` (not yet claimed) reservation is explicitly recoverable by
    # another caller, so it justifies ``active_runner``; a ``claimed``
    # reservation whose runner is no longer provably alive is stuck and must
    # never justify a persistent ``active_runner``.
    return (
        isinstance(res, dict)
        and res.get("state") == "reserved"
        and _runner_reservation_mode(res) is not None
    )


def _set_active_runner(meta: Meta, *, value: bool) -> None:
    """Set ``active_runner`` and keep the reservation invariant consistent.

    When the runner becomes inactive its reservation is dropped so a stale
    reservation can never leave ``active_runner`` stuck true.

    Args:
        meta: Agent metadata under the metadata lock.
        value: The new active-runner state.
    """
    meta["active_runner"] = value
    if not value:
        meta["runner_reservation"] = None


def _test_sync(step: str) -> None:
    """Pause for deterministic multiprocessing tests at a named boundary.

    Only active when ``LUBKO_TEST_SYNC`` names a directory.  The caller writes
    a ``.reached`` token and blocks until the test drops a ``.release`` file,
    letting tests force exact interleavings of independent processes at the
    vulnerable points of the prompt protocol.

    Args:
        step: Named synchronization point.
    """
    sync_dir = os.environ.get("LUBKO_TEST_SYNC")
    if not sync_dir:
        return
    base = Path(sync_dir)
    base.mkdir(parents=True, exist_ok=True)
    pid = os.getpid()
    (base / f"{step}.{pid}.reached").touch()
    # Record the exact start ticks so teardown can prove the reaching process
    # is still the same identity and never signal a reused PID.
    ticks = proc_start_ticks(pid)
    if ticks is not None:
        (base / f"{step}.{pid}.ticks").write_text(str(ticks))
    release = base / f"{step}.release"
    while not release.exists():
        time.sleep(0.005)


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------


class MalformedLifecycleStateError(ValueError):
    """Persisted managed-agent lifecycle state is not canonical."""


def _persisted_lifecycle_state(meta: Meta) -> str | None:
    """Return canonical durable lifecycle state, preserving legacy absence as idle."""
    if "state" not in meta:
        return "idle"
    value = meta["state"]
    if type(value) is not str or value not in PERSISTED_AGENT_STATES:
        return None
    return value


def _require_persisted_lifecycle_state(meta: Meta) -> str:
    """Return canonical lifecycle state or reject malformed durable authority.

    Raises:
        MalformedLifecycleStateError: If persisted lifecycle state is malformed.
    """
    state = _persisted_lifecycle_state(meta)
    if state is None:
        raise MalformedLifecycleStateError
    return state


def _persisted_timestamp(value: object) -> float | None:
    """Return a canonical finite persisted timestamp without coercion."""
    if (
        not isinstance(value, (int, float))
        or isinstance(value, bool)
        or not math.isfinite(value)
        or value < 0
    ):
        return None
    return float(value)


def _launch_timestamp(meta: Meta) -> float | None:
    """Return a canonical persisted launch timestamp."""
    value = meta.get("started_at")
    if value is None:
        value = meta.get("created_at")
    return _persisted_timestamp(value)


def derive_state(meta: Meta | None) -> str:
    """Return the live state, verifying process liveness rather than trusting metadata.

    Args:
        meta: Agent metadata, or ``None``.

    Returns:
        The effective agent state.
    """
    if not meta:
        return "unknown"
    state = _persisted_lifecycle_state(meta)
    if state != "running":
        return state if state is not None else "unknown"
    pid_value = meta.get("pid")
    if pid_value is None:
        launched = _launch_timestamp(meta)
        return (
            "running"
            if launched is not None and time.time() - launched < PID_START_WINDOW_SECONDS
            else "unknown"
        )
    if _process_identity_int(pid_value, minimum=1) is None:
        return "unknown"
    if is_alive(meta):
        return "running"
    finished = _persisted_timestamp(meta.get("finished_at"))
    return str(state) if finished is not None else "unknown"


DISAPPEARED_NOTE: Final = "runner/model process disappeared without a captured exit status"


def _reconcile_dead_invocation(m: Meta) -> None:
    """Reconcile metadata that claims a running invocation nothing can justify.

    Runs under the per-agent metadata lock. When the recorded agent and runner
    identities are both gone (checked by exact identity: PID + start ticks +
    environment marker, so PID reuse never fools this) and no reservation is
    genuinely in flight, the durable record converges to an explicit terminal
    state. A stale-but-recoverable ``reserved`` reservation is deliberately
    left alone: it is justified by the protocol and recovered by the next
    caller under a fresh generation.

    The reconciliation is idempotent: once reconciled the state is no longer
    ``running`` and ``active_runner`` is false, so repeated calls are no-ops.
    The old process identities are preserved for diagnostics; ``exit_code`` /
    ``exit_signal`` remain unset exactly because the process disappeared
    without a captured return code, distinguishing this from a normal model
    exit (exit code set) or a signal crash (signal set).

    Args:
        m: Agent metadata under the lock.
    """
    active_runner = _active_runner_flag(m)
    if active_runner is None:
        return  # malformed durable authority requires explicit repair
    if not active_runner and m.get("state") != "running":
        return  # already terminal/reconciled; nothing to converge
    if is_genuinely_running(m):
        return
    pid_value = m.get("pid")
    if pid_value is None:
        # Launched but the runner has not recorded its identity yet; give the
        # exact startup window the same grace derive_state grants it.
        launched = _launch_timestamp(m)
        if launched is not None and time.time() - launched < PID_START_WINDOW_SECONDS:
            return
    if m.get("state") == "running":
        _finalize_terminal(m, None, None, "failed", DISAPPEARED_NOTE)
    _set_active_runner(m, value=False)


def reconcile_meta(aid: str) -> bool:
    """Reconcile an agent's durable metadata after process disappearance.

    Idempotent, PID-reuse safe convergence pass: if the durable record still
    claims an active running invocation whose exact processes are provably
    gone, it is rewritten to an explicit terminal state under the per-agent
    lock. Safe to call any number of times from any number of observers.

    The metadata is only rewritten when reconciliation actually changes it, so
    hot polling paths (log follow ticks) never pay a needless fsync per tick.

    Args:
        aid: Lubko agent ID.

    Returns:
        ``True`` when the durable record was changed and rewritten.
    """
    current = read_meta(aid)
    if current is None:
        return False
    candidate = copy.deepcopy(current)
    _reconcile_dead_invocation(candidate)
    if candidate == current:
        return False
    update_meta(aid, _reconcile_dead_invocation)
    return True


# ---------------------------------------------------------------------------
# Formatting
# ---------------------------------------------------------------------------


def _first_line(text: str) -> str:
    line = text.splitlines()[0] if text.splitlines() else ""
    return line[:80] if line else "(untitled)"


def _truncate(text: str, width: int) -> str:
    if len(text) <= width:
        return text
    return text[: width - 1] + "~"


def fold_line(line: str, width: int = FOLD_WIDTH) -> list[str]:
    """Fold one logical line into display lines of at most ``width`` characters.

    Folding is presentation only: the durable log is never rewritten, and a
    logical line simply hard-wraps at every ``width``-th character. An empty
    logical line yields one empty display line.

    Args:
        line: One logical line, without its trailing newline.
        width: Maximum characters per displayed line.

    Returns:
        The folded display lines.
    """
    if not line:
        return [""]
    return [line[i : i + width] for i in range(0, len(line), width)]


def fmt_time(epoch: float | None) -> str:
    """Format an epoch timestamp for display.

    Args:
        epoch: Epoch timestamp, or ``None``.

    Returns:
        A ``YYYY-MM-DD HH:MM:SS`` string, or ``-`` when unknown.
    """
    if epoch is None:
        return "-"
    try:
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(epoch))
    except (ValueError, OSError):
        return "-"


def fmt_age(epoch: float | None) -> str:
    """Format an epoch timestamp as a compact age.

    Args:
        epoch: Epoch timestamp, or ``None``.

    Returns:
        A compact age string, or ``-`` when unknown.
    """
    if epoch is None:
        return "-"
    seconds = max(0, int(time.time() - epoch))
    if seconds < SECONDS_PER_MINUTE:
        return f"{seconds}s"
    minutes = seconds // SECONDS_PER_MINUTE
    if minutes < MINUTES_PER_HOUR:
        return f"{minutes}m"
    hours = minutes // MINUTES_PER_HOUR
    if hours < HOURS_PER_DAY:
        return f"{hours}h{minutes % MINUTES_PER_HOUR}m"
    return f"{hours // HOURS_PER_DAY}d{hours % HOURS_PER_DAY}h"


def fmt_cpu(cpu: float | None) -> str:
    """Format total CPU time in seconds for display.

    Args:
        cpu: Total CPU seconds, or ``None`` when unavailable.

    Returns:
        A compact CPU time string, or ``-`` when unknown.
    """
    if cpu is None:
        return "-"
    if cpu < SECONDS_PER_MINUTE:
        return f"{cpu:.1f}s"
    minutes = int(cpu // SECONDS_PER_MINUTE)
    if minutes < MINUTES_PER_HOUR:
        return f"{minutes}m{cpu % SECONDS_PER_MINUTE:.0f}s"
    hours = minutes // MINUTES_PER_HOUR
    return f"{hours}h{minutes % MINUTES_PER_HOUR}m"


def log_excerpt(path: Path, max_lines: int = STATUS_TAIL_LINES) -> list[str]:
    """Return a short plain-text tail of a log file for display.

    Logical lines have ANSI escape sequences stripped and are folded to
    ``FOLD_WIDTH`` characters, then only the newest ``max_lines`` displayed
    lines are kept. Folding is presentation only; the durable log is never
    modified.

    Args:
        path: Log file path.
        max_lines: Maximum displayed lines (``<= 0`` for every line).

    Returns:
        The plain-text displayed lines.
    """
    return _folded_tail(path, max_lines, strip_ansi=True)


def print_box(lines: list[str], max_width: int = 80) -> None:
    """Render an ASCII box around a list of lines, folding to ``max_width``.

    Long lines are wrapped (word-wise, hard-breaking overlong tokens) so the
    completed box is never wider than ``max_width`` characters.

    Args:
        lines: Lines to render.
        max_width: Maximum rendered width.
    """
    if not lines:
        return
    fold_width = max(10, max_width - 6)
    folded: list[str] = []
    for line in lines:
        wrapped = textwrap.wrap(
            line.expandtabs(4),
            width=fold_width,
            drop_whitespace=False,
            replace_whitespace=False,
            break_long_words=True,
            break_on_hyphens=True,
        )
        folded.extend(wrapped or [""])
    width = max(len(line) for line in folded) + 4
    bar = "|" + "-" * width + "|"
    _out(bar)
    for line in folded:
        _out("| " + line.ljust(width - 1) + "|")
    _out(bar)


# ---------------------------------------------------------------------------
# Underlying agent integration (internal)
# ---------------------------------------------------------------------------


def discover_session_id(aid: str) -> str | None:
    """Find the underlying session id for a Lubko agent, if discoverable.

    Args:
        aid: Lubko agent ID.

    Returns:
        The underlying session ID, or ``None`` when undiscoverable.
    """
    db = opencode_db_path()
    if not db:
        return None
    try:
        conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    except sqlite3.Error:
        return None
    try:
        row = conn.execute(
            "SELECT id FROM session WHERE title=? ORDER BY time_created DESC LIMIT 1",
            (OPENCODE_TITLE_PREFIX + aid,),
        ).fetchone()
    except sqlite3.Error:
        return None
    finally:
        conn.close()
    return row[0] if row else None


def _persisted_native_session_id(meta: Meta) -> str | None:
    """Return canonical durable native-session continuation authority.

    A persisted native session is either genuinely absent/``None`` or a
    non-empty string. Malformed present values are ambiguous authority and
    must fail closed rather than being normalized through truthiness.

    Raises:
        ValueError: The durable native session identity is malformed.
    """
    session_id = meta.get("native_session_id")
    if session_id is None:
        return None
    if not isinstance(session_id, str) or not session_id:
        message = "managed-agent native_session_id is malformed"
        raise ValueError(message)
    return session_id


def _persisted_variant(meta: Meta) -> str:
    """Return canonical durable managed-agent variant configuration.

    Raises:
        ValueError: The durable variant value is malformed.
    """
    if "variant" not in meta:
        return DEFAULT_VARIANT
    variant = meta["variant"]
    if not isinstance(variant, str) or not variant:
        message = "managed-agent variant is malformed"
        raise ValueError(message)
    return variant


def build_agent_command(meta: Meta, prompt: str, *, is_continue: bool) -> list[str] | None:
    """Return the argv used to launch the underlying agent for this invocation.

    Args:
        meta: Agent metadata.
        prompt: Instruction to run.
        is_continue: Whether to continue the existing underlying session.

    Returns:
        The command argv, or ``None`` when continuation is impossible.
    """
    model = AGENT_MODEL
    variant = _persisted_variant(meta)
    cwd = _persisted_agent_cwd(meta)
    if is_continue:
        recorded = _persisted_native_session_id(meta)
        session_id = recorded or discover_session_id(meta.get("id", ""))
        if not session_id:
            return None
        return [
            "opencode",
            "run",
            "--auto",
            "--session",
            session_id,
            "--model",
            model,
            "--variant",
            variant,
            "--thinking",
            "--dir",
            cwd,
            prompt,
        ]
    return [
        "opencode",
        "run",
        "--auto",
        "--title",
        OPENCODE_TITLE_PREFIX + meta.get("id", ""),
        "--model",
        model,
        "--variant",
        variant,
        "--thinking",
        "--dir",
        cwd,
        prompt,
    ]


def _persisted_agent_cwd(meta: Meta) -> str:
    """Return the exact durable managed-agent working directory.

    Once an agent record exists, its ``cwd`` is execution authority. A
    malformed present value must never be normalized or replaced with the
    runner process current directory.

    Raises:
        ValueError: The durable working directory is missing or malformed.
    """
    cwd = meta.get("cwd")
    if not isinstance(cwd, str) or not cwd:
        message = "managed-agent cwd is malformed"
        raise ValueError(message)
    return cwd


# ---------------------------------------------------------------------------
# Runner (background monitor for one agent invocation)
# ---------------------------------------------------------------------------


def _clear_pending(meta: Meta, prompt: str) -> None:
    """Clear the pending prompt only when it is the exact one being claimed.

    A prompt queued concurrently (for example a continuation issued while
    this invocation was already being claimed) must never be cleared: it is
    owned by the next loop iteration.  Clearing only an exact match prevents
    the prompt-loss race where a runner blindly pops a freshly queued prompt.

    Args:
        meta: Agent metadata under the metadata lock.
        prompt: The exact prompt this invocation claimed.
    """
    if _pending_prompt(meta) == prompt:
        meta["pending_prompt"] = None


def _runner_env(aid: str) -> dict[str, str]:
    """Build the detached runner environment with the exact agent marker.

    The runner's own environment must carry ``LUBKO_AGENT_ID=<aid>`` so the
    runner's exact identity can be verified later (start time plus marker);
    a stale marker inherited from the invoking environment is overwritten
    with the exact current agent ID.  No other environment entry is altered.

    Args:
        aid: Agent ID the runner monitors.

    Returns:
        The environment with the exact agent marker.
    """
    env = dict(os.environ)
    env["LUBKO_AGENT_ID"] = aid
    return env


def runner(aid: str, mode: str) -> None:
    """Detached monitor: run the current invocation, then any queued steers.

    Runs one invocation, records its result, then — if steer instructions
    are queued — immediately runs them in FIFO order until the queue drains.
    A runner that finds no prompt re-checks under the metadata lock so a
    prompt or steer that arrived concurrently is never skipped, and an
    unexpected failure never leaves the agent stuck with a live invocation
    and no monitor.

    The runner claims the exact runner reservation it was spawned for before
    doing any work.  A runner whose generation does not match the live
    reservation (for example a duplicate spawned before the protocol was
    fixed, or a replacement that arrived after a takeover) bails immediately,
    so a second runner can never execute the same invocation.

    Args:
        aid: Lubko agent ID.
        mode: Invocation mode (``new`` or ``continue``).
    """
    meta = read_meta(aid)
    if meta is None:
        return
    gen = int(os.environ.get("LUBKO_RUNNER_GEN") or "0")
    claimed = {}

    def claim(m: Meta) -> None:
        if _delete_pending_flag(m) is not False:
            # A concurrent delete owns the lifecycle: a runner must never
            # claim (and later recreate state) once deletion was decided.
            claimed["ok"] = False
            return
        res = m.get("runner_reservation")
        if not isinstance(res, dict):
            # No reservation: a production runner must never execute without an
            # exact reserved generation.  Fail closed.
            claimed["ok"] = False
            return
        if res.get("state") != "reserved":
            # Already claimed or otherwise owned: a second runner must never
            # execute.  (A genuinely live claimed runner is the only owner.)
            claimed["ok"] = False
            return
        if gen == 0 or res.get("gen") != gen:
            # Only the exact reserved generation may run.  A missing or zero
            # generation, or a duplicate/stale replacement whose generation no
            # longer matches, bails instead of double-executing.
            claimed["ok"] = False
            return
        reservation_mode = _runner_reservation_mode(res)
        if reservation_mode is None or reservation_mode != mode:
            # The reservation's durable native-session mode is execution
            # authority. A malformed value, or a runner spawned for a different
            # mode, must never claim and reinterpret it as a fresh session.
            claimed["ok"] = False
            return
        m["runner_pid"] = os.getpid()
        m["runner_start_time"] = proc_start_ticks(os.getpid())
        m["runner_reservation"] = {**res, "state": "claimed"}
        m["active_runner"] = True
        m["state"] = "running"
        claimed["ok"] = True

    _test_sync("runner_preclaim")
    update_meta(aid, claim)
    if not claimed.get("ok"):
        return
    # Deterministic test boundary after the exact claim committed but before
    # any state recreation, so tests can force the delete/runner race.
    _test_sync("runner_prestart")
    directory = agent_dir(aid)
    directory.mkdir(parents=True, exist_ok=True)
    log_path = directory / "output.log"
    env = dict(os.environ)
    env["LUBKO_AGENT_ID"] = aid
    env["NO_COLOR"] = "1"
    try:
        cwd = _persisted_agent_cwd(meta)
        ctx = _RunnerContext(aid=aid, log_path=log_path, cwd=cwd, env=env)
        _runner_loop(ctx, is_continue=mode == "continue")
    except BaseException:
        _abort_runner(aid)
        raise


def _runner_loop(ctx: _RunnerContext, *, is_continue: bool) -> None:
    """Run invocations and queued steers until the queue drains.

    Every invocation is sourced from ``pending_prompt``; the first one uses the
    caller-supplied mode, later ones continue the exact native session.

    Args:
        ctx: Shared runner context.
        is_continue: Whether the first invocation continues an existing session.
    """
    while True:
        meta = read_meta(ctx.aid)
        if meta is None:
            return
        if _delete_pending_flag(meta) is not False:
            # Deleted, being deleted, or tombstone authority is malformed.
            return
        prompt = _pending_prompt(meta)
        if prompt is None:
            if _reclaim_prompt(ctx.aid):
                continue
            # Exit boundary seam: the runner has durably relinquished
            # consumption authority (``active_runner`` false, reservation
            # dropped) while its process is still alive.  Tests use this point
            # to force a concurrent prompt into exactly this window.
            _test_sync("reclaim_idle")
            return
        if _run_invocation(ctx, prompt, is_continue=is_continue) is None:
            return
        is_continue = True


def _reclaim_prompt(aid: str) -> bool:
    """Keep a runner alive when a prompt or steer arrived concurrently.

    The decision to go idle is re-checked under the metadata lock so a
    ``prompt``/``steer`` that landed just after this runner read its state is
    never stranded without a monitor.

    Args:
        aid: Lubko agent ID.

    Returns:
        ``True`` when the runner should loop again, ``False`` when idle.
    """
    holder: dict[str, bool] = {"busy": False}

    def apply(m: Meta) -> None:
        if m.get("stop_reason") in STOP_REASONS:
            _set_active_runner(m, value=False)
            return
        sequence = _steer_sequence(m)
        if sequence is None:
            raise MalformedSteerMetadataError
        queue = _steer_queue(m, sequence=sequence)
        if queue is None:
            raise MalformedSteerMetadataError
        pending = _pending_prompt(m)
        if pending is not None or queue:
            if queue and pending is None:
                _pop_into_pending(m, time.time())
            m["active_runner"] = True
            holder["busy"] = True
            return
        _set_active_runner(m, value=False)

    update_meta(aid, apply)
    return holder["busy"]


def _abort_runner(aid: str) -> None:
    """Clean up an agent after an unexpected runner failure.

    The exact recorded invocation process group is signalled through the
    kernel-stable identity-exact path — a stale numeric PID/PGID alone is
    never authority for SIGKILL. Terminalization happens only after exact
    group death is positively proven *and* the metadata still records the
    same invocation; otherwise a durable stop-like safety hold blocks
    replacement work instead of falsely reporting convergence.

    Args:
        aid: Lubko agent ID.
    """
    meta = read_meta(aid)
    if meta is None or meta.get("state") != "running":
        return
    identity = _invocation_identity(meta)
    send_signal_group(meta, signal.SIGKILL)
    if not wait_group_dead(meta, ABORT_WAIT_SECONDS):
        # Fail closed: ownership or death could not be proven, so the agent
        # must stay nonterminal and safety-blocked rather than admit
        # replacement work while the old invocation may still be live.
        _update_meta_if_same_invocation(aid, identity, _hold_abort())
        return
    # Terminalize only when no newer invocation has taken over the record.
    _update_meta_if_same_invocation(aid, identity, _finalize_abort())


def _hold_abort() -> Callable[[Meta], None]:
    """Return a mutation that keeps an unproven abort as a blocking hold.

    The stop-like intent durably refuses new prompt claims and runner work,
    and ``active_runner`` stays set so no second runner starts, until an
    explicit safe recovery converges or clears the invocation.

    Returns:
        The metadata mutation.
    """

    def hold(m: Meta) -> None:
        if m.get("state") != "running":
            return  # already finalized (e.g. by stop/kill)
        m["intent"] = "kill"
        m["last_activity_at"] = time.time()

    return hold


@dataclass(frozen=True, slots=True)
class _RunnerContext:
    """Shared state for one runner invocation stream."""

    aid: str
    log_path: Path
    cwd: str
    env: dict[str, str]


def _claim_pending_prompt(aid: str, prompt: str) -> bool:
    """Claim the accepted pending prompt for this runner invocation.

    Linearizes the spawn against stop/kill (issue #185): the claim under the
    metadata lock refuses when a durably recorded stop-like intent or reason
    exists, so a prompt accepted before a concurrent stop can never be started
    by a stale runner read — whichever side wins the lock decides.

    Args:
        aid: Lubko agent ID.
        prompt: The exact prompt this invocation is about to run.

    Returns:
        ``True`` when the prompt was claimed and may be spawned.
    """
    claimed: dict[str, bool] = {}

    def claim(m: Meta) -> None:
        if (
            _persisted_lifecycle_state(m) is None
            or _delete_pending_flag(m) is not False
            or m.get("intent") in STOP_REASONS
            or m.get("stop_reason") in STOP_REASONS
        ):
            return
        _clear_pending(m, prompt)
        claimed["ok"] = True

    update_meta(aid, claim)
    if not claimed.get("ok"):
        update_meta(aid, lambda m: _set_active_runner(m, value=False))
        return False
    return True


def _spawn_and_run(
    ctx: _RunnerContext,
    aid: str,
    cmd: list[str],
    iid: str,
    *,
    is_continue: bool,
) -> int | None:
    """Spawn one invocation process, wait for it, and record its result.

    The spawn is linearized against stop/kill (issue #185): if a concurrent
    stop/kill durably records its intent between the prompt claim and the
    running-record lock, the freshly spawned process is never tracked as
    running and is killed immediately instead.

    Args:
        ctx: Shared runner context.
        aid: Lubko agent ID.
        cmd: The exact agent command argv to execute.
        iid: The durable invocation ID stamped into the spawned environment.
        is_continue: Whether this invocation continues an existing session.

    Returns:
        The invocation's return code, or ``None`` when the runner must stop
        without draining further queued work.
    """
    try:
        log = ctx.log_path.open("ab")
    except OSError as exc:
        # Fail closed on real spool/log failures (e.g. EACCES, EIO, EDQUOT);
        # only an intentionally deleted agent directory exits benignly.
        if isinstance(exc, FileNotFoundError) and not agent_dir(aid).is_dir():
            return None  # agent directory intentionally deleted; metadata is gone
        _fail_invocation_closed(aid, f"failed to open agent log: {exc}")
        return None
    with log:
        try:
            proc = subprocess.Popen(
                cmd,
                stdin=subprocess.DEVNULL,
                stdout=log,
                stderr=log,
                cwd=ctx.cwd,
                start_new_session=True,
                close_fds=True,
                env=ctx.env,
            )
        except OSError as exc:
            error = str(exc)
            log.write(f"LUBKO RUNNER: failed to start agent: {error}\n".encode("utf-8", "replace"))
            _fail_invocation_closed(aid, error, exit_code=127)
            return None

        start = proc_start_ticks(proc.pid)
        # The record is conditional (issue #185): see ``_spawn_and_run``.
        blocked: dict[str, bool] = {}
        update_meta(aid, _record_running(proc, start, iid, blocked))
        if blocked.get("stopped"):
            # The freshly spawned invocation lost the race against stop/kill;
            # converge it instead of leaving it running untracked.
            _kill_unrecorded_invocation(aid, proc, start, iid)
            return None

        try:
            rc = _wait_for_invocation_exit(proc, aid, is_continue=is_continue)
        except BaseException:
            # Abnormal runner exit: never leave the invocation process group
            # running untracked, and never leave the agent stuck "running".
            # The earlier spawn-time observation is re-proven at signal time
            # under a kernel-stable pin, so a recycled PID can never redirect
            # SIGKILL to an unrelated group. Terminalization still requires
            # positively proven group death.
            _kill_spawned_invocation(aid, proc.pid, start, iid)
            # Bounded reap: when exact ownership was unprovable the signal
            # failed closed and the child may still be live, so an unbounded
            # wait here could wedge the runner before any durable safety hold
            # is recorded.
            reaped = True
            try:
                proc.wait(timeout=ABORT_REAP_SECONDS)
            except subprocess.TimeoutExpired:
                reaped = False
            except OSError:
                reaped = False
            observed = {
                "id": aid,
                "pid": proc.pid,
                "pgid": proc.pid,
                "start_time": start,
                "invocation_id": iid,
            }
            # The CAS guard keeps the terminal/hold mutation on exactly this
            # spawned invocation: a concurrently recorded newer invocation is
            # never clobbered by this cleanup.
            identity = (
                proc.pid,
                proc.pid,
                start,
                os.getpid(),
                proc_start_ticks(os.getpid()),
            )
            converged = reaped and wait_group_dead(observed, ABORT_WAIT_SECONDS)
            _update_meta_if_same_invocation(
                aid,
                identity,
                _finalize_abort() if converged else _hold_abort(),
            )
            raise
        update_meta(aid, _finalize_after(rc))

    return rc


def _run_invocation(ctx: _RunnerContext, prompt: str, *, is_continue: bool) -> str | None:
    """Run one agent invocation and decide whether a queued steer follows.

    Args:
        ctx: Shared runner context.
        prompt: Instruction to run.
        is_continue: Whether to continue the underlying session.

    Returns:
        The next queued prompt, or ``None`` when the runner should stop.
    """
    aid = ctx.aid
    meta = read_meta(aid)
    if meta is None:
        return None
    # A fresh, durable identity for exactly this invocation. It is stamped
    # into the spawned process environment (inherited by the whole process
    # tree) and recorded in metadata, so later signalling can prove
    # invocation-exact membership even across PGID/PID recycling.
    iid = uuid.uuid4().hex
    ctx.env[INVOCATION_ID_VAR] = iid
    ctx.env["LUBKO_PROMPT"] = prompt
    cmd = build_agent_command(meta, prompt, is_continue=is_continue)
    if cmd is None:
        update_meta(
            aid,
            lambda m: _finalize_terminal(
                m,
                None,
                None,
                "failed",
                "cannot continue: underlying session not available",
            ),
        )
        update_meta(aid, lambda m: _set_active_runner(m, value=False))
        return None
    # Linearize the spawn against stop/kill (issue #185): see
    # ``_claim_pending_prompt``.
    if not _claim_pending_prompt(aid, prompt):
        return None

    rc = _spawn_and_run(ctx, aid, cmd, iid, is_continue=is_continue)
    if rc is None:
        return None

    return _drain_next(aid)


def _fail_invocation_closed(aid: str, error: str, *, exit_code: int | None = None) -> None:
    """Atomically finalize a failed invocation and deactivate the runner.

    Terminal failure and runner deactivation (including reservation cleanup)
    land in one metadata mutation under the lock, so a crash between the two
    can never leave terminal state with ``active_runner`` stuck true.

    Args:
        aid: Lubko agent ID.
        error: Failure note recorded with the terminal state.
        exit_code: Optional synthetic exit code for the terminal record.
    """

    def fail(m: Meta) -> None:
        _finalize_terminal(m, exit_code, None, "failed", error)
        _set_active_runner(m, value=False)

    update_meta(aid, fail)


def _wait_for_invocation_exit(
    proc: subprocess.Popen[bytes],
    aid: str,
    *,
    is_continue: bool,
) -> int:
    """Discover the native session if needed, then wait for the invocation.

    Args:
        proc: The spawned invocation process.
        aid: Lubko agent ID.
        is_continue: Whether this invocation continues an existing session.

    Returns:
        The invocation's return code.
    """
    if not is_continue:
        deadline = time.time() + SESSION_DISCOVER_TIMEOUT_SECONDS
        while time.time() < deadline and proc.poll() is None:
            sid = discover_session_id(aid)
            if sid:
                update_meta(aid, _set_native_session(sid))
                break
            time.sleep(SESSION_DISCOVER_POLL_SECONDS)
    return proc.wait()


def _kill_spawned_invocation(aid: str, pid: int, start: object, iid: str) -> None:
    """SIGKILL a freshly spawned invocation through exact-identity signalling.

    The spawn-time observation (``pid``, ``start``, ``iid``) is re-proven at
    signal time under a kernel-stable pidfd pin, so a numeric PID recycled
    into an unrelated process group can never be signalled. When exact
    ownership cannot be proven nothing is signalled: fail closed.

    Args:
        aid: Lubko agent ID whose marker the invocation environment carries.
        pid: The recorded invocation leader PID (its own group).
        start: The observed start time in clock ticks.
        iid: The durable per-invocation ID stamped into its environment.
    """
    send_signal_group(
        {
            "id": aid,
            "pid": pid,
            "pgid": pid,
            "start_time": start,
            "invocation_id": iid,
        },
        signal.SIGKILL,
    )


def _kill_unrecorded_invocation(
    aid: str, proc: subprocess.Popen[bytes], start: object, iid: str
) -> None:
    """Kill a freshly spawned invocation that lost the stop/kill race.

    The spawn gate (``_record_running``) refused to track the process because a
    concurrent stop/kill durably recorded its intent first, so the invocation
    is converged immediately instead of running untracked — via the same
    exact-identity signalling discipline as every other cleanup path.

    Args:
        aid: Lubko agent ID whose marker the invocation environment carries.
        proc: The just-spawned agent process.
        start: The observed start time in clock ticks.
        iid: The durable per-invocation ID stamped into its environment.
    """
    _kill_spawned_invocation(aid, proc.pid, start, iid)
    try:
        proc.wait(timeout=ABORT_REAP_SECONDS)
    except (subprocess.TimeoutExpired, OSError):
        # Exact signalling failed closed and the direct child is still live:
        # keep a durable unresolved-child hold (never terminalizing over the
        # concurrent stop/kill decision) so no replacement work can start
        # while this untracked invocation survives.
        _hold_unrecorded(aid, proc.pid, start, iid)
        return
    observed = {
        "id": aid,
        "pid": proc.pid,
        "pgid": proc.pid,
        "start_time": start,
        "invocation_id": iid,
    }
    if wait_group_dead(observed, ABORT_WAIT_SECONDS):
        # Positively converged: clear exactly this child's obligation, never
        # a newer one that may have been recorded meanwhile.
        _clear_unresolved(aid, proc.pid, start, iid)


def _clear_unresolved(aid: str, pid: int, start: object, iid: str) -> None:
    """Clear the unresolved obligation of exactly one converged child.

    Args:
        aid: Lubko agent ID.
        pid: The converged child's PID.
        start: Its observed start time in clock ticks.
        iid: Its durable per-invocation ID.
    """

    def clear(m: Meta) -> None:
        cur = _persisted_unresolved_identity(m.get("unresolved_invocation"))
        if (
            cur is not None
            and cur["pid"] == pid
            and cur["start_time"] == start
            and cur["invocation_id"] == iid
        ):
            m["unresolved_invocation"] = None

    update_meta(aid, clear)


def _hold_unrecorded(aid: str, pid: int, start: object, iid: str) -> None:
    """Keep durable blocking authority while an untracked child survives.

    The exact unresolved-child record survives terminal stop/kill metadata
    (which clears ``intent``) so a later prompt cannot reserve replacement
    work while the unproven invocation lives; it is cleared only once exact
    convergence of that child is proven. An existing stop-like intent is
    preserved untouched, delete-pending stays owned by deletion, and nothing
    is ever terminalized here.

    Args:
        aid: Lubko agent ID.
        pid: The unrecorded child's PID.
        start: Its observed start time in clock ticks.
        iid: Its durable per-invocation ID.
    """

    def hold(m: Meta) -> None:
        # The marker survives even delete-pending metadata: deletion
        # convergence only knows recorded runner/group/reservation
        # identities, so an unrecorded loser must carry its own authority
        # through the tombstone until positively proven gone.
        m["unresolved_invocation"] = {
            "pid": pid,
            "pgid": pid,
            "start_time": start,
            "invocation_id": iid,
        }
        if m.get("state") == "running" and m.get("intent") not in STOP_REASONS:
            m["intent"] = "kill"
            m["last_activity_at"] = time.time()

    update_meta(aid, hold)


def _unresolved_leader_state(rec: Meta) -> str | None:
    """Classify the recorded leader's survival evidence for an unresolved record.

    Args:
        rec: The persisted unresolved-invocation marker.

    Returns:
        ``"live"`` or ``"ambiguous"`` when the leader decides the outcome,
        ``None`` when the leader is positively dead/recycled and the group
        scan must decide instead.
    """
    pid = rec["pid"]
    try:
        os.kill(int(pid), 0)
    except ProcessLookupError:
        return None  # leader positively dead: the group scan decides
    except OSError:
        return "ambiguous"  # exists but uninspectable (e.g. EPERM)
    ticks = proc_start_ticks(int(pid))
    if ticks is None:
        return "ambiguous"  # unreadable procfs cannot prove anything
    if ticks == rec["start_time"]:
        return "live"  # the recorded leader itself still lives
    return None  # leader positively recycled by an unrelated occupant


def _persisted_unresolved_identity(value: object) -> Meta | None:
    """Return one canonical durable unresolved-invocation identity.

    Malformed present authority must never participate in liveness or cleanup
    through Python coercion or equality.
    """
    if not isinstance(value, dict):
        return None
    rec = cast("Meta", value)
    if (
        _process_identity_int(rec.get("pid"), minimum=1) is None
        or _process_identity_int(rec.get("pgid"), minimum=1) is None
        or _process_identity_int(rec.get("start_time"), minimum=0) is None
        or _persisted_invocation_id(rec.get("invocation_id")) is None
    ):
        return None
    return rec


def _unresolved_child_state(m: Meta) -> str:
    """Classify the exact recorded unresolved invocation's survival evidence.

    The obligation spans the whole recorded invocation group, not just its
    leader: leader death or PID recycling alone never proves it gone because
    genuine descendants can survive in the old process group. The record is
    gone only when the leader is positively dead or recycled and a complete
    pinned per-invocation scan finds no surviving exact-marker member. Any
    uninspectable or malformed durable evidence remains ambiguous.

    Args:
        m: Agent metadata.

    Returns:
        ``"live"``, ``"gone"``, or ``"ambiguous"`` according to exact survival evidence.
    """
    raw = m.get("unresolved_invocation")
    if raw is None:
        return "gone"
    rec = _persisted_unresolved_identity(raw)
    if rec is None:
        return "ambiguous"
    leader_state = _unresolved_leader_state(rec)
    if leader_state is not None:
        return leader_state
    aid = _persisted_agent_id(m.get("id"))
    if aid is None:
        return "ambiguous"
    pgid = cast("int", rec["pgid"])
    iid = cast("str", rec["invocation_id"])
    members, complete = _proven_invocation_members(pgid, aid, iid)
    for _, fd in members:
        os.close(fd)
    if not complete:
        return "ambiguous"
    return "live" if members else "gone"


def _record_running(
    proc: subprocess.Popen[bytes],
    start: int | None,
    iid: str,
    blocked: dict[str, bool],
) -> Callable[[Meta], None]:
    """Return a metadata mutation that records a running agent process.

    Args:
        proc: The spawned agent process.
        start: The process start time in clock ticks.
        iid: The durable invocation ID stamped into the spawned environment.
        blocked: Caller-owned mapping set to ``{"stopped": True}`` when a
            concurrently recorded stop/kill intent (or deletion tombstone)
            won the linearization race and the invocation must not be
            tracked as running.

    Returns:
        The metadata mutation.
    """

    def record(m: Meta) -> None:
        if (
            _delete_pending_flag(m) is not False
            or m.get("intent") in STOP_REASONS
            or m.get("stop_reason") in STOP_REASONS
        ):
            # The just-spawned child is refused tracking, but it already
            # exists in its own session: durably hand its exact identity
            # over as an unresolved obligation *in this same locked
            # transaction*, so a concurrent force-delete that converges the
            # runner can never remove state while this child still executes.
            blocked["stopped"] = True
            m["unresolved_invocation"] = {
                "pid": proc.pid,
                "pgid": proc.pid,
                "start_time": start,
                "invocation_id": iid,
            }
            return
        m["pid"] = proc.pid
        m["pgid"] = proc.pid
        m["start_time"] = start
        m["invocation_id"] = iid
        m["runner_pid"] = os.getpid()
        m["runner_start_time"] = proc_start_ticks(os.getpid())
        m["started_at"] = time.time()
        m["last_activity_at"] = time.time()
        m["state"] = "running"
        m["finished_at"] = None
        m["exit_code"] = None
        m["exit_signal"] = None
        m["intent"] = None
        m["active_runner"] = True
        m["unresolved_invocation"] = None

    return record


def _set_native_session(sid: str) -> Callable[[Meta], None]:
    """Return a metadata mutation that records the underlying session ID.

    Args:
        sid: Underlying session ID to record.

    Returns:
        The metadata mutation.
    """

    def setter(m: Meta) -> None:
        m.update(native_session_id=sid)

    return setter


def _finalize_after(rc: int) -> Callable[[Meta], None]:
    """Return a metadata mutation that records the result of an invocation.

    Args:
        rc: The invocation's return code.

    Returns:
        The metadata mutation.
    """

    def finalize(m: Meta) -> None:
        if m.get("state") != "running":
            return  # already finalized (e.g. by stop/kill/steer)
        sig = -rc if rc < 0 else None
        intent = m.get("intent")
        m["stop_reason"] = intent
        if intent == "stop":
            state = "stopped"
        elif intent == "kill":
            state = "killed"
        elif intent == "steer":
            state = "stopped" if rc < 0 else ("succeeded" if rc == 0 else "failed")
        elif rc == 0:
            state = "succeeded"
        else:
            state = "failed"
        _finalize_terminal(m, rc, sig, state, None)

    return finalize


def _drain_next(aid: str) -> str | None:
    """Move the next queued steer into a pending invocation, if any.

    Args:
        aid: Lubko agent ID.

    Returns:
        The next prompt to run, or ``None`` when the runner should stop.
    """
    holder: dict[str, str | None] = {"prompt": None}

    def drain(m: Meta) -> None:
        if _persisted_lifecycle_state(m) is None:
            raise MalformedLifecycleStateError
        if m.get("stop_reason") in STOP_REASONS:
            _set_active_runner(m, value=False)
            return
        sequence = _steer_sequence(m)
        if sequence is None:
            raise MalformedSteerMetadataError
        queue = _steer_queue(m, sequence=sequence)
        if queue is None:
            raise MalformedSteerMetadataError
        pending = _pending_prompt(m)
        if pending is not None:
            # A new invocation was queued while this one was running; run it
            # rather than going idle, so a second runner is never needed.
            m["active_runner"] = True
            holder["prompt"] = pending
            return
        if not queue:
            _set_active_runner(m, value=False)
            return
        item = _pop_into_pending(m, time.time())
        m["active_runner"] = True
        holder["prompt"] = item.get("prompt") if item else None

    update_meta(aid, drain)
    return holder["prompt"]


def _finalize_abort() -> Callable[[Meta], None]:
    """Return a metadata mutation that records an aborted runner invocation.

    Returns:
        The metadata mutation.
    """

    def finalize(m: Meta) -> None:
        if m.get("state") != "running":
            return  # already finalized (e.g. by stop/kill)
        _finalize_terminal(m, None, None, "failed", "runner aborted unexpectedly")
        _set_active_runner(m, value=False)

    return finalize


def _finalize_terminal(
    meta: Meta,
    exit_code: int | None,
    exit_signal: int | None,
    state: str,
    note: str | None,
) -> None:
    meta["state"] = state
    meta["exit_code"] = exit_code
    meta["exit_signal"] = exit_signal
    meta["finished_at"] = time.time()
    meta["last_activity_at"] = time.time()
    meta["intent"] = None
    if note:
        meta["error"] = note


class MalformedPendingPromptMetadataError(ValueError):
    """Persisted pending prompt authority is not canonical."""


def _pending_prompt(meta: Meta) -> str | None:
    """Return canonical durable pending prompt authority, or fail closed.

    Raises:
        MalformedPendingPromptMetadataError: If present prompt authority is malformed.
    """
    if "pending_prompt" not in meta or meta["pending_prompt"] is None:
        return None
    value = meta["pending_prompt"]
    if type(value) is not str or not value:
        raise MalformedPendingPromptMetadataError
    return value


class MalformedSteerMetadataError(ValueError):
    """Persisted steer ordering metadata is not canonical."""


def _steer_sequence(meta: Meta) -> int | None:
    """Return canonical durable steer sequence authority."""
    if "steer_seq" not in meta:
        return 0
    value = meta["steer_seq"]
    if type(value) is not int or value < 0:
        return None
    return value


def _valid_steer_item(item: object, *, previous: int, sequence: int) -> bool:
    """Return whether one persisted steer item has canonical JSON shape."""
    if type(item) is not dict:
        return False
    item_seq = item.get("seq")
    prompt = item.get("prompt")
    queued_at = item.get("queued_at")
    return (
        type(item_seq) is int
        and previous < item_seq <= sequence
        and type(prompt) is str
        and isinstance(queued_at, (int, float))
        and not isinstance(queued_at, bool)
        and math.isfinite(queued_at)
    )


def _steer_queue(meta: Meta, *, sequence: int) -> list[Meta] | None:
    """Return a canonical persisted FIFO steer queue, or fail closed."""
    if "steer_queue" not in meta:
        return []
    value = meta["steer_queue"]
    if type(value) is not list:
        return None
    previous = 0
    for item in value:
        if not _valid_steer_item(item, previous=previous, sequence=sequence):
            return None
        previous = item["seq"]
    return value


def _queue_steer(meta: Meta, prompt: str, now: float) -> None:
    """Append a steer instruction only when durable queue metadata is canonical.

    Raises:
        MalformedSteerMetadataError: If persisted queue/sequence metadata is malformed.
    """
    sequence = _steer_sequence(meta)
    if sequence is None:
        raise MalformedSteerMetadataError
    queue = _steer_queue(meta, sequence=sequence)
    if queue is None:
        raise MalformedSteerMetadataError
    seq = sequence + 1
    queue.append({"seq": seq, "prompt": prompt, "queued_at": now})
    meta["steer_queue"] = queue
    meta["steer_seq"] = seq
    meta["last_activity_at"] = now


def _pop_into_pending(meta: Meta, now: float) -> Meta | None:
    """Move the head of the steer queue into a runnable pending invocation.

    Args:
        meta: Agent metadata.
        now: Invocation timestamp.

    Returns:
        The popped steer item, or ``None`` when the queue is empty.
    """
    next_prompt_count = _next_prompt_count(meta)
    sequence = _steer_sequence(meta)
    if next_prompt_count is None or sequence is None:
        return None
    queue = _steer_queue(meta, sequence=sequence)
    if queue is None or not queue:
        return None
    item: Meta = queue.pop(0)
    meta["steer_queue"] = queue
    meta["state"] = "running"
    meta["pending_prompt"] = item.get("prompt", "")
    meta["started_at"] = now
    meta["last_activity_at"] = now
    meta["finished_at"] = None
    meta["exit_code"] = None
    meta["exit_signal"] = None
    meta["intent"] = None
    meta["stop_reason"] = None
    meta["pid"] = None
    meta["pgid"] = None
    meta["start_time"] = None
    meta["prompt_count"] = next_prompt_count
    return item


def _begin_stop_like(meta: Meta, intent: str) -> None:
    """Prepare an agent for stop/kill: drop pending steers, mark intent.

    Args:
        meta: Agent metadata.
        intent: The stop-like intent (``stop`` or ``kill``).
    """
    meta["intent"] = intent
    meta["last_activity_at"] = time.time()
    meta["steer_queue"] = []
    meta["pending_prompt"] = None


def _mark_terminal(
    meta: Meta,
    exit_code: int | None,
    exit_signal: int | None,
    state: str,
    stop_reason: str,
) -> None:
    _finalize_terminal(meta, exit_code, exit_signal, state, None)
    meta["stop_reason"] = stop_reason
    _set_active_runner(meta, value=False)


def _invocation_identity(meta: Meta) -> tuple[object, ...]:
    """Return the exact invocation identity recorded in metadata.

    The identity spans every field the runner rewrites when a new invocation
    is recorded: the process triple (``pid``, ``pgid``, ``start_time``) plus
    the stronger runner anchor (``runner_pid``, ``runner_start_time``), so a
    newer live invocation can never be mistaken for the targeted one.

    Args:
        meta: Agent metadata.

    Returns:
        A comparable identity tuple.
    """
    return (
        meta.get("pid"),
        meta.get("pgid"),
        meta.get("start_time"),
        meta.get("runner_pid"),
        meta.get("runner_start_time"),
    )


def _update_meta_if_same_invocation(
    aid: str,
    identity: tuple[object, ...],
    mutate: Callable[[Meta], None],
) -> bool:
    """Apply ``mutate`` under the metadata lock only if identity still matches.

    A background runner may record a newer invocation while a stop-like
    command waits for the old one to die. This guard ensures the mutation is
    skipped instead of clobbering the newer live invocation's record.

    Args:
        aid: Lubko agent ID.
        identity: The exact invocation identity targeted.
        mutate: Mutation to apply under the lock when identity matches.

    Returns:
        ``True`` only when the mutation was applied.
    """
    applied = False

    def fn(m: Meta) -> None:
        nonlocal applied
        if _invocation_identity(m) != identity:
            return
        mutate(m)
        applied = True

    update_meta(aid, fn)
    return applied


# ---------------------------------------------------------------------------
# Exit status mapping
# ---------------------------------------------------------------------------


def exit_code_for(meta: Meta | None) -> int:
    """Map an agent's live state to a process exit code.

    Args:
        meta: Agent metadata, or ``None``.

    Returns:
        The exit code implied by the agent state.
    """
    state = derive_state(meta) if meta else "unknown"
    if state == "succeeded":
        return EXIT_OK
    if state == "failed":
        code = meta.get("exit_code") if meta else None
        return code if isinstance(code, int) and code > 0 else 1
    return 1  # stopped, killed, unknown, idle


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------


def _out(message: str) -> None:
    """Write a user-facing line to standard output.

    Args:
        message: Message to write.
    """
    sys.stdout.write(message + "\n")


def _err(message: str) -> None:
    """Write a user-facing line to standard error.

    Args:
        message: Message to write.
    """
    sys.stderr.write(message + "\n")


# ---------------------------------------------------------------------------
# Subcommands
# ---------------------------------------------------------------------------


def spawn_runner(aid: str, mode: str, *, gen: int | None = None) -> None:
    """Detach a background runner monitor for an agent.

    The runner is spawned with the exact agent marker set so its identity
    can be verified later; no other environment entry is altered.  The runner
    generation it must claim is carried from the locked reservation decision
    (``gen``) rather than reread from mutable metadata, so the spawned runner
    claims exactly the reservation this caller reserved and never a competing
    one.

    Args:
        aid: Lubko agent ID.
        mode: Invocation mode (``new`` or ``continue``).
        gen: Exact reserved runner generation to carry into the runner, or
            ``None`` to fall back to metadata (used only by direct callers).

    Raises:
        ValueError: The requested runner mode is not canonical.
    """
    if type(mode) is not str or mode not in {"new", "continue"}:
        msg = "managed-agent runner mode is malformed"
        raise ValueError(msg)
    script = Path(__file__).resolve()
    env = _runner_env(aid)
    if gen is not None:
        env["LUBKO_RUNNER_GEN"] = str(int(gen))
    else:
        meta = read_meta(aid)
        if meta:
            res = meta.get("runner_reservation")
            if isinstance(res, dict) and res.get("gen"):
                env["LUBKO_RUNNER_GEN"] = str(int(res["gen"]))
    subprocess.Popen(
        [sys.executable, str(script), "_runner", aid, mode],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        close_fds=True,
        env=env,
    )


def cmd_new(args: argparse.Namespace) -> int:
    """Create an idle managed agent session with a caller-supplied ID.

    ``new`` only creates the managed Lubko agent record. It never launches the
    underlying AI agent and never accepts an initial prompt; the first
    invocation happens later through ``lubko-agent prompt --id <ID> PROMPT``.

    Args:
        args: Parsed command arguments.

    Returns:
        A process exit code.
    """
    aid = normalize_agent_id(args.id)
    if aid is None:
        _err(f"{PROG}: new: --id is required and must be a base-16 string")
        return EXIT_USAGE
    if agent_dir(aid).exists():
        _err(f"{PROG}: new: agent {aid} already exists")
        return EXIT_ERROR
    cwd = str(Path(args.cwd or Path.cwd()).resolve())
    if not Path(cwd).is_dir():
        _err(f"{PROG}: new: working directory does not exist: {cwd}")
        return EXIT_ERROR

    meta = idle_meta(aid, cwd, args.title)
    agent_dir(aid).mkdir(parents=True, exist_ok=True)
    write_meta(aid, meta)

    if args.json:
        _out(
            json.dumps({
                "id": aid,
                "state": "idle",
                "cwd": cwd,
                "created_at": meta["created_at"],
            })
        )
    else:
        _out(
            f"Created agent with id {aid} (idle). Start work with "
            f"`{PROG} prompt --id {aid} 'task'`."
        )
    sys.stdout.flush()
    return EXIT_OK


def cmd_prompt(args: argparse.Namespace) -> int:
    """Send an instruction to an agent, creating its native session on first use.

    ``prompt`` follows/streams the invocation by default and returns only when
    that invocation finishes, propagating the mapped invocation exit status.
    ``--detach`` is the explicit fire-and-forget mode. ``--steer`` only changes
    behavior while the agent is currently running; on an idle, finished, or
    never-started agent it is exactly equivalent to an ordinary prompt.

    Prompt submission and runner ownership form one atomic protocol under the
    per-agent lock: a prompt that arrives while another invocation is genuinely
    reserved or running is either serialized (steer) or explicitly rejected
    (ordinary prompt on a busy agent) rather than silently overwriting the
    pending prompt or spawning a second runner.

    Args:
        args: Parsed command arguments.

    Returns:
        A process exit code.
    """
    aid = args.id
    if not aid:
        _err(f"{PROG}: prompt: an agent ID is required via --id")
        return EXIT_USAGE
    prompt = args.prompt_text or args.prompt
    if not prompt:
        _err(f"{PROG}: prompt: a prompt is required")
        return EXIT_USAGE
    meta = read_meta(aid)
    if meta is None:
        _err(f"{PROG}: unknown agent: {aid}")
        return EXIT_NOT_FOUND
    # An ordinary prompt on a genuinely busy agent is rejected; a steer is
    # serialized.  A reserved agent whose runner never claimed (stale
    # reservation) is not genuinely busy and is recovered below instead of
    # being rejected, so an agent can never get stuck "running" forever.
    running = derive_state(meta) == "running"
    if running and not args.steer and (is_alive(meta) or reservation_in_flight(meta)):
        _err(f"{PROG}: agent {aid} is still running; use --steer to redirect it")
        return EXIT_ERROR
    return _dispatch_invocation(args, prompt)


def _follow_attached(aid: str) -> int:
    """Stream an invocation's output until the agent stably reaches a terminal state.

    A stable terminal is required because the background runner transitions
    through a brief terminal state between queued steers; following must not
    stop on that transient blip.

    Args:
        aid: Lubko agent ID.

    Returns:
        The mapped invocation exit status.
    """
    stream_log_until_terminal(aid)
    return exit_code_for(read_meta(aid))


def _interrupt_steer_if_needed(aid: str) -> None:
    """Send SIGTERM to a live agent that is mid-steer, if applicable.

    A reuse decision that interrupted the running invocation only matters when
    the runner is still executing the agent under a ``steer`` intent; an agent
    that has already finished or never entered the steer intent needs no
    signal.

    Args:
        aid: Lubko agent ID.
    """
    current = read_meta(aid)
    if current is None or current.get("intent") != "steer" or not is_alive(current):
        return
    send_signal_group(current, signal.SIGTERM)


def _recover_stale_reservation(
    m: Meta,
    decision: dict[str, object],
    *,
    prompt: str,
    steer: bool,
) -> bool:
    """Recover a stale reserved or pre-consumption claimed runner.

    Preserves the accepted pending prompt. Returns ``True`` only when ``m``
    holds a reserved runner whose spawner or
    runner died before claiming its exact identity, or a runner that durably
    claimed its identity but died before consuming the accepted pending prompt
    (nothing genuinely in flight). The caller re-owns the reservation under a
    fresh generation so the stale old generation is invalidated, and starts
    exactly one replacement runner.

    A claimed runner that already consumed its prompt (no ``pending_prompt``
    survives) is not recovered here: the stale claimed authority is dropped and
    the caller falls through to an ordinary fresh start, so the consumed prompt
    is never replayed.

    The already-accepted pending prompt is preserved and never overwritten. When
    an accepted prompt exists:

    * an ordinary recovery caller is explicitly rejected (its own prompt is
      discarded with a busy disposition) while recovery of the original prompt
      still proceeds; and
    * a ``--steer`` recovery caller queues the steer deterministically behind
      the recovered invocation using the existing steer semantics, is durably
      accepted (the caller receives success), and still lets exactly one
      replacement runner execute the original prompt.

    When no accepted prompt survived, the recovery caller's prompt is the one to
    run and is accepted; a stale/idle ``--steer`` is then equivalent to an
    ordinary prompt. In every case the recovery caller's text is never silently
    discarded behind a success code.

    A concurrent second recovery caller acquires the lock after the first has
    re-owned the reservation, observes it genuinely in flight, and is rejected
    as busy (ordinary) or serialized as a queued steer (``--steer``) without
    spawning a second replacement.

    Args:
        m: Agent metadata under the lock.
        decision: Caller-owned mapping filled with the resulting action.
        prompt: The new caller's prompt or steer.
        steer: Whether the recovery caller is a ``--steer``.

    Returns:
        ``True`` when a stale reservation was recovered here.
    """
    pending = _pending_prompt(m)
    res = m.get("runner_reservation")
    if not (isinstance(res, dict) and res.get("state") in {"reserved", "claimed"}):
        return False
    if reservation_in_flight(m):
        return False
    take_mode = _runner_reservation_mode(res)
    current_gen = _runner_generation(m.get("runner_gen", 0), minimum=0)
    if take_mode is None or current_gen is None:
        decision["action"] = "busy"
        return True
    if res.get("state") == "claimed" and pending is None:
        # The exact runner consumed the accepted prompt before dying; there is
        # nothing to preserve and nothing to replay. Drop the dead claimed
        # authority and let the caller's fresh start own the next invocation.
        _set_active_runner(m, value=False)
        return False
    now = time.time()
    caller_pid = os.getpid()
    gen = current_gen + 1
    accepted = pending is not None
    if accepted:
        if steer:
            # A --steer that discovers the same stale reservation queues the
            # steer deterministically behind the recovered invocation using the
            # existing steer semantics, is durably accepted (success), and lets
            # exactly one replacement runner execute the original prompt. The
            # accepted pending prompt is never overwritten.
            _queue_steer(m, prompt, now)
            decision["steer_accepted"] = True
        else:
            # An ordinary recovery caller must not overwrite the accepted prompt;
            # its own prompt is explicitly rejected (busy) while recovery of the
            # original prompt proceeds via the spawned replacement runner.
            decision["recover_busy"] = True
    else:
        # No accepted prompt survived: the recovery caller's prompt is the one
        # to run and is accepted. A stale/idle --steer is equivalent to an
        # ordinary prompt here, so it simply owns the recovered invocation.
        next_prompt_count = _next_prompt_count(m)
        if next_prompt_count is None:
            decision["action"] = "busy"
            return True
        m["pending_prompt"] = prompt
        m["last_prompt"] = _truncate(prompt, 500)
        m["prompt_count"] = next_prompt_count
    m["active_runner"] = True
    m["runner_gen"] = gen
    m["runner_reservation"] = {
        "gen": gen,
        "owner_pid": caller_pid,
        "owner_start_ticks": proc_start_ticks(caller_pid),
        "state": "reserved",
        "reserved_at": now,
        "mode": take_mode,
    }
    decision["action"] = "spawn"
    decision["mode"] = take_mode
    decision["gen"] = gen
    return True


def _resolve_session_mode(m: Meta) -> str | None:
    """Return the native-session mode derived from the locked agent state.

    The mode (``new`` or ``continue``) is resolved under the per-agent metadata
    lock from the current agent state, never from a stale pre-lock observation.
    A recorded underlying session that can no longer be discovered fails closed
    (``None``), and otherwise the mode is ``continue`` when a native session is
    recorded or discoverable and ``new`` otherwise.

    Args:
        m: Agent metadata under the lock.

    Returns:
        ``"new"``, ``"continue"``, or ``None`` when the session is gone.
    """
    recorded = _persisted_native_session_id(m)
    # Always rediscover under the lock: external session availability is the
    # authority, and a stale discovery before the lock must never authorize a
    # second ``new`` session.
    discovered = discover_session_id(m.get("id", "")) or None
    if recorded is not None and discovered is None:
        # A recorded underlying session that can no longer be found must fail
        # closed rather than silently starting a fresh one.
        return None
    return "continue" if (recorded or discovered) is not None else "new"


def _decide_invocation(
    m: Meta,
    decision: dict[str, object],
    *,
    prompt: str,
    steer: bool,
) -> None:
    """Apply one linearizable prompt/steer transition atomically.

    Runs under the per-agent metadata lock (via ``update_meta``). Mutates ``m``
    in place and records the caller's next action in ``decision``. The full
    decision order is documented on ``_dispatch_invocation``.

    The native-session mode (``new`` or ``continue``) is derived here, under the
    lock, from the current agent state rather than from a stale pre-lock
    observation.  This closes the observe→lock TOCTOU: a caller that initially
    sees no underlying session but loses the race to another invocation
    continues the session that invocation established instead of reserving a
    second ``new`` session from stale information.

    Args:
        m: Agent metadata under the lock.
        decision: Caller-owned mapping filled with the resulting action.
        prompt: Instruction to run or steer.
        steer: Whether this is a steer rather than an ordinary prompt.
    """
    if _persisted_lifecycle_state(m) is None:
        decision["action"] = "busy"
        return
    if _active_runner_flag(m) is None:
        decision["action"] = "busy"
        return
    if _unresolved_child_state(m) != "gone":
        # An earlier unrecorded invocation could not be positively proven
        # converged (still live, or inspection ambiguous); a later
        # prompt/steer stays blocked and the marker persists until exact
        # convergence is proven, even across terminal stop/kill metadata.
        decision["action"] = "busy"
        return
    m["unresolved_invocation"] = None
    if m.get("intent") in STOP_REASONS:
        # A durably accepted stop/kill obligation for this invocation must
        # never be overwritten by a later prompt/steer: otherwise the dying
        # invocation is finalized as a steer and _drain_next resurrects the
        # agent with replacement work. Reject as busy until finalization
        # clears the stop-like intent.
        decision["action"] = "busy"
        return
    mode = _resolve_session_mode(m)
    if mode is None:
        decision["action"] = "error_session_gone"
        return
    _apply_locked_transition(m, decision, prompt=prompt, steer=steer, mode=mode)


def _reserve_fresh_runner(
    m: Meta,
    decision: dict[str, object],
    *,
    prompt: str,
    mode: str,
    now: float,
) -> None:
    """Reserve one fresh runner generation, failing closed on corrupt history."""
    caller_pid = os.getpid()
    current_gen = _runner_generation(m.get("runner_gen", 0), minimum=0)
    next_prompt_count = _next_prompt_count(m)
    if current_gen is None or next_prompt_count is None:
        decision["action"] = "busy"
        return
    gen = current_gen + 1
    _begin_invocation(m, prompt, now, prompt_count=next_prompt_count)
    m["active_runner"] = True
    m["runner_gen"] = gen
    m["runner_reservation"] = {
        "gen": gen,
        "owner_pid": caller_pid,
        "owner_start_ticks": proc_start_ticks(caller_pid),
        "state": "reserved",
        "reserved_at": now,
        "mode": mode,
    }
    decision["action"] = "spawn"
    decision["mode"] = mode
    decision["gen"] = gen


def _apply_locked_transition(
    m: Meta,
    decision: dict[str, object],
    *,
    prompt: str,
    steer: bool,
    mode: str,
) -> None:
    """Apply the linearizable prompt/steer transition under the metadata lock.

    Assumes the native-session ``mode`` has already been resolved under the lock
    (see ``_resolve_session_mode``).  Mutates ``m`` in place and records the
    caller's next action in ``decision``.

    Args:
        m: Agent metadata under the lock.
        decision: Caller-owned mapping filled with the resulting action.
        prompt: Instruction to run or steer.
        steer: Whether this is a steer rather than an ordinary prompt.
        mode: Resolved native-session mode (``new`` or ``continue``).
    """
    _require_persisted_lifecycle_state(m)
    pending = _pending_prompt(m)
    now = time.time()
    caller_pid = os.getpid()
    live_agent = is_alive(m)
    # Consumption authority is the durable flag, not raw process liveness:
    # once an exiting runner has relinquished it (``active_runner`` false,
    # reservation dropped), that runner will never consume another prompt, so
    # it must not be reused even while its process is still dying.
    live_runner = _active_runner_flag(m) is True and runner_alive(m)
    in_flight = reservation_in_flight(m)

    if live_agent:
        if steer:
            _queue_steer(m, prompt, now)
            m["intent"] = "steer"
            decision["action"] = "reuse"
            decision["interrupt"] = True
        else:
            decision["action"] = "busy"
        return

    if live_runner:
        if steer:
            _queue_steer(m, prompt, now)
            decision["action"] = "reuse"
            decision["interrupt"] = False
        elif pending is not None:
            # An invocation is already accepted and awaiting this live runner;
            # a second ordinary prompt must never overwrite it.  It is
            # explicitly busy so exactly one prompt owns the runner.
            decision["action"] = "busy"
        else:
            m["pending_prompt"] = prompt
            m["state"] = "running"
            m["last_activity_at"] = now
            decision["action"] = "reuse"
            decision["interrupt"] = False
        return

    if in_flight:
        if steer:
            _queue_steer(m, prompt, now)
            decision["action"] = "reuse"
            decision["interrupt"] = False
        elif not owned_by_me(m, caller_pid):
            decision["action"] = "busy"
        else:
            m["pending_prompt"] = prompt
            decision["action"] = "reuse"
        return

    # A stale reserved runner (spawner or runner died before claiming its exact
    # identity) or a claimed runner that died before consuming its accepted
    # prompt is recovered here: the accepted pending prompt is preserved and
    # exactly one replacement runner is started under a fresh generation.
    if _recover_stale_reservation(m, decision, prompt=prompt, steer=steer):
        return

    # Nothing is genuinely in flight and no stale reservation to recover: own
    # this transition (fresh start) and reserve exactly one runner.
    _reserve_fresh_runner(
        m,
        decision,
        prompt=prompt,
        mode=mode,
        now=now,
    )


def _dispatch_invocation(args: argparse.Namespace, prompt: str) -> int:
    """Submit a prompt or steer as one atomic, linearizable protocol step.

    Under the per-agent metadata lock the caller decides, in order:

    * an invocation is genuinely executing (live agent process) — an ordinary
      prompt is rejected (busy); a steer is queued and interrupts it;
    * only a runner with durable consumption authority exists (between
      invocations) — the prompt is queued for that runner and no second
      runner is spawned; a runner whose process lingers while its
      consumption authority was already relinquished is never reused;
    * a runner is reserved but not yet claimed — an ordinary prompt is rejected
      (busy) so it cannot overwrite the pending prompt or spawn a competing
      runner; a steer is queued deterministically;
    * nothing is in flight (idle, finished, or a stale reservation) — the
      caller reserves exactly one runner generation and spawns the single
      runner authorized to execute this invocation.  A stale reservation
      (spawner dead, runner never claimed) is taken over the same way, so
      recovery never leaves the agent permanently running nor double-executes.

    Only the process that holds the exact reservation may become the active
    runner: the spawned runner claims its generation before doing any work,
    and a second runner (whether from a race or a takeover that already
    produced a replacement) bails instead of executing.

    Args:
        args: Parsed command arguments.
        prompt: Instruction to run or steer.

    Returns:
        A process exit code.
    """
    aid = args.id or args.agent_id
    steer = bool(args.steer)

    meta = read_meta(aid)
    if meta is None:
        _err(f"{PROG}: unknown agent: {aid}")
        return EXIT_NOT_FOUND

    _test_sync("sc_observe")

    decision: dict[str, object] = {}
    update_meta(
        aid,
        lambda m: _decide_invocation(m, decision, prompt=prompt, steer=steer),
    )
    _test_sync("sc_decide")

    action = decision.get("action")
    if action == "error_session_gone":
        _err(f"{PROG}: cannot continue agent {aid}: its underlying session is not available")
        return EXIT_ERROR
    if action == "busy":
        _err(f"{PROG}: agent {aid} is still running; use --steer to redirect it")
        return EXIT_ERROR
    if action == "spawn":
        spawn_runner(aid, cast("str", decision["mode"]), gen=cast("int", decision["gen"]))
        if decision.get("recover_busy"):
            # A stale reserved runner was recovered (the accepted pending prompt
            # is preserved and a replacement runner was started), but this
            # caller's own prompt is explicitly rejected rather than silently
            # accepted behind a success code.
            _err(f"{PROG}: agent {aid} is recovering a reserved prompt; this prompt was rejected")
            return EXIT_ERROR
    elif action == "reuse" and decision.get("interrupt"):
        _interrupt_steer_if_needed(aid)

    if args.detach:
        if args.json:
            _out(json.dumps({"id": aid, "state": "running", "detached": True}))
        else:
            _out(
                "Started agent " + aid + " in the background. Observe it with "
                f"`{PROG} log {aid} --follow`."
            )
        sys.stdout.flush()
        return EXIT_OK
    return _follow_attached(aid)


def _begin_invocation(meta: Meta, prompt: str, now: float, *, prompt_count: int) -> None:
    """Mark an agent as starting a new invocation.

    ``active_runner`` is deliberately left untouched: whether a live runner
    will pick the prompt up (or a replacement must be spawned) is decided by
    the caller after checking the runner's exact liveness.

    Args:
        meta: Agent metadata.
        prompt: Instruction to run.
        now: Invocation timestamp.
        prompt_count: Already validated next durable prompt count.
    """
    meta["state"] = "running"
    meta["started_at"] = now
    meta["last_activity_at"] = now
    meta["finished_at"] = None
    meta["exit_code"] = None
    meta["exit_signal"] = None
    meta["intent"] = None
    meta["stop_reason"] = None
    meta["pid"] = None
    meta["pgid"] = None
    meta["start_time"] = None
    meta["pending_prompt"] = prompt
    meta["last_prompt"] = _truncate(prompt, 500)
    meta["prompt_count"] = prompt_count


def cmd_list(args: argparse.Namespace) -> int:
    """List Lubko-managed agents.

    Args:
        args: Parsed command arguments.

    Returns:
        A process exit code.
    """
    entries = _list_entries(args)
    if args.json:
        _out(json.dumps({"agents": list(starmap(_entry_json, entries))}))
        return EXIT_OK
    if not entries:
        _out("(no agents)")
        return EXIT_OK
    _print_agent_table(entries)
    return EXIT_OK


def _agent_ids() -> list[str]:
    """Return the sorted agent IDs stored locally.

    Returns:
        The sorted agent IDs.
    """
    with contextlib.suppress(OSError):
        return sorted(entry.name for entry in agents_dir().iterdir() if entry.is_dir())
    return []


def _matches_filters(args: argparse.Namespace, state: str) -> bool:
    """Return whether an agent state passes the list filters.

    Args:
        args: Parsed command arguments.
        state: Agent state.

    Returns:
        ``True`` when the state is not filtered out.
    """
    if args.running and state != "running":
        return False
    if args.finished and state not in TERMINAL_STATES:
        return False
    return not (
        (args.succeeded and state != "succeeded")
        or (args.failed and state != "failed")
        or (args.stopped and state != "stopped")
        or (args.killed and state != "killed")
    )


def _list_entries(args: argparse.Namespace) -> list[tuple[str, str, Meta]]:
    """Build the filtered, sorted list of (aid, state, meta) entries.

    Args:
        args: Parsed command arguments.

    Returns:
        The entries, newest first, limited when requested.
    """
    entries: list[tuple[str, str, Meta]] = []
    for aid in _agent_ids():
        meta = read_meta(aid)
        if meta is None:
            meta = {"id": aid}
        state = derive_state(meta)
        if not _matches_filters(args, state):
            continue
        entries.append((aid, state, meta))
    entries.sort(key=lambda entry: _list_summary(entry[2])[0] or 0, reverse=True)
    if args.limit and args.limit > 0:
        entries = entries[: args.limit]
    return entries


def _list_summary(
    meta: Meta,
) -> tuple[int | float | None, int | None, str | None, str | None, list[str]]:
    """Validate persisted metadata used by the multi-agent list surface.

    Returns:
        Canonical summary values plus malformed field names.
    """
    errors: list[str] = []

    created_at: int | float | None = None
    raw_created_at = meta.get("created_at")
    if raw_created_at is not None:
        if (
            not isinstance(raw_created_at, (int, float))
            or isinstance(raw_created_at, bool)
            or not math.isfinite(raw_created_at)
        ):
            errors.append("created_at")
        else:
            created_at = raw_created_at

    prompt_count: int | None = None
    if "prompt_count" in meta:
        raw_prompt_count = meta["prompt_count"]
        if type(raw_prompt_count) is not int or raw_prompt_count < 0:
            errors.append("prompt_count")
        else:
            prompt_count = raw_prompt_count

    cwd: str | None = None
    raw_cwd = meta.get("cwd")
    if raw_cwd is not None:
        if type(raw_cwd) is not str:
            errors.append("cwd")
        else:
            cwd = raw_cwd

    title: str | None = None
    raw_title = meta.get("title")
    if raw_title is not None:
        if type(raw_title) is not str:
            errors.append("title")
        else:
            title = raw_title

    return created_at, prompt_count, cwd, title, errors


def _entry_json(aid: str, state: str, meta: Meta) -> Meta:
    """Build the JSON mapping for one agent list entry.

    Returns:
        The sanitized JSON-safe list entry.
    """
    created_at, prompt_count, cwd, title, errors = _list_summary(meta)
    entry: Meta = {
        "id": aid,
        "state": state,
        "prompts": prompt_count,
        "cwd": cwd,
        "title": title,
        "created_at": created_at,
        "last_activity_at": meta.get("last_activity_at"),
        "finished_at": meta.get("finished_at"),
    }
    if errors:
        entry["metadata_errors"] = errors
    return entry


def _print_agent_table(entries: list[tuple[str, str, Meta]]) -> None:
    """Print the agent list table.

    Args:
        entries: The (aid, state, meta) entries to print.
    """
    rows = []
    for aid, state, meta in entries:
        created_at, prompt_count, cwd, title, errors = _list_summary(meta)
        error_fields = set(errors)
        rows.append((
            aid,
            state,
            "<invalid>" if "prompt_count" in error_fields else str(prompt_count or 0),
            "<invalid>" if "created_at" in error_fields else fmt_age(created_at),
            "<invalid>" if "cwd" in error_fields else _truncate(cwd or "", 24),
            (
                "<invalid>"
                if "title" in error_fields
                else _truncate((title or "").replace("\n", " "), 40)
            ),
        ))
    widths = [max(len(row[i]) for row in rows) for i in range(6)]
    labels = ("ID", "STATE", "P", "AGE", "CWD", "TITLE")
    for i, label in enumerate(labels):
        widths[i] = max(widths[i], len(label))
    _out("  ".join(label.ljust(widths[i]) for i, label in enumerate(labels)))
    for row in rows:
        _out("  ".join(row[i].ljust(widths[i]) for i in range(6)))


def cmd_status(args: argparse.Namespace) -> int:
    """Show detailed status of one agent.

    Args:
        args: Parsed command arguments.

    Returns:
        A process exit code.
    """
    aid = args.id or args.agent_id
    if not aid:
        _err(f"{PROG}: status: an agent ID is required")
        return EXIT_USAGE
    # Reconcile before reporting: a status observation must converge durable
    # metadata instead of leaving a dead invocation recorded as running. This
    # is idempotent and PID-reuse safe (exact-identity checks inside).
    reconcile_meta(aid)
    meta = read_meta(aid)
    if meta is None:
        _err(f"{PROG}: unknown agent: {aid}")
        return EXIT_NOT_FOUND
    state = derive_state(meta)
    alive = is_alive(meta)
    if args.json:
        _out(json.dumps(_status_json(aid, meta, state, alive=alive), indent=2))
        return EXIT_OK

    log_path = agent_dir(aid) / "output.log"
    _out(f"agent:      {aid}")
    _out(f"state:      {state}")
    _out(f"alive:      {'yes' if alive else 'no'}")
    _out(f"cpu:        {fmt_cpu(_status_cpu_seconds(meta, alive=alive))}")
    _out(f"cwd:        {meta.get('cwd') or '-'}")
    _out(f"created:    {fmt_time(meta.get('created_at'))}")
    _out(f"started:    {fmt_time(meta.get('started_at'))}")
    _out(f"finished:   {fmt_time(meta.get('finished_at'))}")
    exit_code = meta.get("exit_code")
    _out(f"exit code:  {exit_code if exit_code is not None else '-'}")
    _out(f"prompts:    {meta.get('prompt_count') or 0}")
    steers, steer_error = _status_steer_queue(meta)
    if steer_error is not None:
        _out(f"steers:     {steer_error}")
    elif steers:
        _out(f"steers:     {len(steers)} queued: {_first_line(steers[0]['prompt'])}")
    _out(f"title:      {meta.get('title') or '-'}")
    _out("tail(log):")
    excerpt = log_excerpt(log_path, STATUS_TAIL_LINES)
    if excerpt:
        print_box(excerpt, max_width=FOLD_WIDTH + 6)
    return EXIT_OK


def _status_cpu_seconds(meta: Meta, *, alive: bool) -> float | None:
    """Return the agent's CPU time, gated on the exact process identity.

    CPU time is only meaningful when the recorded PID is verified to be the
    exact live agent process. A stored PID that exists but was reused by an
    unrelated process must never surface CPU time.

    Args:
        meta: Agent metadata.
        alive: Whether the exact agent process is alive.

    Returns:
        The total CPU seconds, or ``None`` when not alive.
    """
    if not alive:
        return None
    return proc_cpu_seconds(meta.get("pid"))


def _status_steer_queue(meta: Meta) -> tuple[list[Meta] | None, str | None]:
    """Return validated steer status data without normalizing corruption."""
    sequence = _steer_sequence(meta)
    if sequence is None:
        return None, "malformed persisted steer metadata"
    queue = _steer_queue(meta, sequence=sequence)
    if queue is None:
        return None, "malformed persisted steer metadata"
    return queue, None


def _status_json(aid: str, meta: Meta, state: str, *, alive: bool) -> Meta:
    """Build the JSON status mapping for an agent.

    Args:
        aid: Agent ID.
        meta: Agent metadata.
        state: Effective agent state.
        alive: Whether the agent process is alive.

    Returns:
        The JSON-safe mapping.
    """
    steers, steer_error = _status_steer_queue(meta)
    return {
        "id": aid,
        "state": state,
        "alive": alive,
        "cpu_seconds": _status_cpu_seconds(meta, alive=alive),
        "native_session_id": meta.get("native_session_id"),
        "pid": meta.get("pid"),
        "pgid": meta.get("pgid"),
        "runner_pid": meta.get("runner_pid"),
        "cwd": meta.get("cwd"),
        "title": meta.get("title"),
        "created_at": meta.get("created_at"),
        "started_at": meta.get("started_at"),
        "finished_at": meta.get("finished_at"),
        "last_activity_at": meta.get("last_activity_at"),
        "exit_code": meta.get("exit_code"),
        "exit_signal": meta.get("exit_signal"),
        "prompts": meta.get("prompt_count"),
        "steers_pending": len(steers) if steers is not None else None,
        "next_steer": _first_line(steers[0]["prompt"]) if steers else None,
        "steer_metadata_error": steer_error,
        "model": AGENT_MODEL,
        "variant": meta.get("variant"),
        "log": str(agent_dir(aid) / "output.log"),
    }


def _tail_logical_lines(path: Path, count: int) -> list[str]:
    """Return the trailing ``count`` logical lines of a log file.

    Reads backward from the end of the file so huge logs are never loaded
    wholesale when only a short tail is requested.

    Args:
        path: Log file path.
        count: Number of trailing logical lines (``<= 0`` for the whole file).

    Returns:
        The trailing logical lines, newest last, without trailing newlines.
    """
    with path.open("rb") as fh:
        fh.seek(0, os.SEEK_END)
        size = fh.tell()
        return _tail_logical_lines_stream(fh, size, count)


def _tail_logical_lines_stream(fh: BinaryIO, end: int, count: int) -> list[str]:
    """Return the trailing ``count`` logical lines up to byte offset ``end``.

    The tail is read only from the open stream's bytes in ``[0, end)``, so
    the result stays consistent with the snapshot size that captured ``end``:
    bytes appended to the file afterwards are excluded and remain for the
    follow. Reads backward so huge logs are never loaded wholesale when only
    a short tail is requested.

    Args:
        fh: Open binary stream positioned at ``end``.
        end: Captured end byte offset of the file snapshot.
        count: Number of trailing logical lines (``<= 0`` for the whole file).

    Returns:
        The trailing logical lines, newest last, without trailing newlines.
    """
    if count <= 0:
        fh.seek(0)
        data = fh.read(end)
        lines = data.decode("utf-8", errors="replace").split("\n")
        if lines and not lines[-1]:
            lines.pop()
        return lines
    block = 64 * 1024
    trailing = b""
    preceded_by_newline = True
    pos = end
    while pos > 0 and trailing.count(b"\n") < count:
        chunk = min(block, pos)
        pos -= chunk
        fh.seek(pos)
        trailing = fh.read(chunk) + trailing
    if pos > 0:
        fh.seek(pos - 1)
        preceded_by_newline = fh.read(1) == b"\n"
    lines = trailing.decode("utf-8", errors="replace").split("\n")
    if lines and not lines[-1]:
        lines.pop()
    if pos > 0 and not preceded_by_newline:
        lines = lines[1:]
    if len(lines) > count:
        lines = lines[-count:]
    return lines


def _folded_tail(path: Path, lines: int, *, strip_ansi: bool = False) -> list[str]:
    """Return the newest ``lines`` folded display lines of a log file.

    This is the single shared fold-and-tail path for every user-visible log
    view: logical lines are folded to ``FOLD_WIDTH`` characters (after
    optionally stripping ANSI escapes), and only the number of folded display
    lines limits the result. Folding is presentation only; the durable log is
    never modified.

    Args:
        path: Log file path.
        lines: Maximum displayed lines (``<= 0`` for every line).
        strip_ansi: Strip ANSI escape sequences from each logical line.

    Returns:
        The displayed lines, newest last.
    """
    try:
        with path.open("rb") as fh:
            fh.seek(0, os.SEEK_END)
            size = fh.tell()
            return _folded_tail_stream(fh, size, lines, strip_ansi=strip_ansi)
    except OSError:
        return []


def _folded_tail_stream(
    fh: BinaryIO,
    end: int,
    lines: int,
    *,
    strip_ansi: bool = False,
) -> list[str]:
    """Return the newest ``lines`` folded display lines up to byte ``end``.

    Logical lines are read only from the open stream's bytes in ``[0, end)``
    and folded to ``FOLD_WIDTH`` characters, so the display never extends past
    the snapshot size that captured ``end``: bytes appended afterwards stay
    out of the snapshot and remain for the follow.

    Args:
        fh: Open binary stream positioned at ``end``.
        end: Captured end byte offset of the file snapshot.
        lines: Maximum displayed lines (``<= 0`` for every line).
        strip_ansi: Strip ANSI escape sequences from each logical line.

    Returns:
        The displayed lines, newest last.
    """
    try:
        logical = _tail_logical_lines_stream(fh, end, lines)
    except OSError:
        return []
    folded: list[str] = []
    for logical_line in logical:
        folded.extend(fold_line(_ANSI_RE.sub("", logical_line) if strip_ansi else logical_line))
    if lines > 0 and len(folded) > lines:
        folded = folded[-lines:]
    return folded


def _drop_dangling_fragment(lines: list[str]) -> list[str]:
    r"""Remove a trailing incomplete escape fragment from the last display line.

    A log may end in the middle of an ANSI CSI/OSC sequence (for example
    ``\x1b[3``). Such a fragment is not ordinary text and is never shown;
    when it is the only content of the final line that line is dropped too.

    Args:
        lines: Display lines to normalize.

    Returns:
        The display lines without any trailing dangling fragment.
    """
    if not lines:
        return lines
    stripped, pending = _strip_ansi_keep_tail(lines[-1])
    if not pending:
        return lines
    if stripped:
        lines[-1] = stripped
    else:
        lines.pop()
    return lines


def tail_lines(path: Path, n: int) -> list[str]:
    """Return the last ``n`` normalized displayed lines of a log file.

    Each logical line has ANSI escape sequences stripped and is folded to
    ``FOLD_WIDTH`` characters; only the newest ``n`` displayed lines are kept,
    and there is no character limit. An incomplete trailing escape fragment is
    never shown. The durable log is never modified.

    Args:
        path: Log file path.
        n: Number of displayed lines.

    Returns:
        The displayed lines, newest last.
    """
    return _drop_dangling_fragment(_folded_tail(path, n, strip_ansi=True))


def _file_ends_with_newline_stream(fh: BinaryIO, end: int) -> bool:
    r"""Return whether the byte just before ``end`` is a newline.

    Args:
        fh: Open binary stream.
        end: Captured end byte offset of the file snapshot.

    Returns:
        ``True`` when ``end > 0`` and byte ``end - 1`` is ``\n``.
    """
    if end <= 0:
        return False
    fh.seek(end - 1)
    return fh.read(1) == b"\n"


@dataclass(frozen=True, slots=True)
class LogSnapshot:
    """One consistent normalized log snapshot plus the follow handoff state.

    Attributes:
        display: Normalized display bytes, covering only ``[0, offset)``.
        offset: Raw byte offset to continue following from (the captured EOF).
        pending: Trailing incomplete escape fragment held back from the
            display so a sequence split across the snapshot/follow boundary is
            still stripped once its continuation arrives.
    """

    display: bytes
    offset: int
    pending: str


def _tail_snapshot_from(fh: BinaryIO, end: int, max_lines: int) -> LogSnapshot:
    """Return a normalized snapshot (display, offset, pending fragment).

    The display and the continuation offset come from the single captured
    ``end`` byte offset: the folded, ANSI-stripped tail is read only up to
    ``end``, and ``end`` is also the returned offset, so a later append is
    excluded from the display and remains for the follow. Any trailing
    incomplete escape fragment is held back into ``pending`` instead of
    leaking into the display. The durable log is never modified.

    Args:
        fh: Open binary stream positioned at ``end``.
        end: Captured end byte offset of the file snapshot.
        max_lines: Maximum displayed lines.

    Returns:
        The normalized snapshot.
    """
    display = _folded_tail_stream(fh, end, max_lines, strip_ansi=True)
    if not display:
        return LogSnapshot(b"", end, "")
    text = "\n".join(display)
    stripped, pending = _strip_ansi_keep_tail(text)
    if _file_ends_with_newline_stream(fh, end):
        stripped += "\n"
    return LogSnapshot(stripped.encode("utf-8", errors="replace"), end, pending)


def tail_snapshot(path: Path, max_lines: int = STATUS_TAIL_LINES) -> LogSnapshot:
    """Return a normalized display snapshot with the raw offset to follow from.

    The display bytes cover at most ``max_lines`` folded display lines with
    ANSI escape sequences stripped and no dangling fragment leaked; folding
    and normalization are presentation only and never touch the durable log.
    The display and the offset come from one consistent open-file snapshot:
    the file is opened once, its EOF size is captured, and the tail is read
    only up to that captured size. Bytes appended after the capture are
    excluded from the snapshot and remain for the follow, so following resumes
    exactly where the displayed output ends.

    Args:
        path: Log file path.
        max_lines: Maximum displayed lines.

    Returns:
        The normalized snapshot.
    """
    try:
        fh = path.open("rb")
    except OSError:
        return LogSnapshot(b"", 0, "")
    with fh:
        fh.seek(0, os.SEEK_END)
        size = fh.tell()
        return _tail_snapshot_from(fh, size, max_lines)


def stream_log_until_terminal(aid: str, follow_lines: int = STATUS_TAIL_LINES) -> None:
    """Print the recent normalized folded tail, then stream output until done.

    Both the initial snapshot and every incremental chunk have ANSI CSI/SGR
    sequences stripped before presentation; the durable raw log is untouched.

    Args:
        aid: Lubko agent ID.
        follow_lines: Number of recent folded display lines to show first.
    """
    log_path = agent_dir(aid) / "output.log"
    offset, pending = _print_snapshot(log_path, follow_lines)
    normalizer = _LogNormalizer(pending=pending)
    handle: BinaryIO | None = None
    idle_since: float | None = None
    terminal_since: float | None = None

    while True:
        if handle is None:
            handle = _open_log(log_path, offset)
            if handle is None:
                if _terminal_or_unknown(aid):
                    return
                time.sleep(0.3)
                continue
        if _consume_log(handle, normalizer):
            idle_since = None
        stop, terminal_since = _stable_terminal(aid, terminal_since)
        if stop:
            _drain_and_stop(handle, normalizer)
            return
        if _stale_running(aid):
            if idle_since is None:
                idle_since = time.time()
            elif time.time() - idle_since > IDLE_BREAK_SECONDS:
                _drain_and_stop(handle, normalizer)
                return
        time.sleep(0.4)


def _stable_terminal(aid: str, terminal_since: float | None) -> tuple[bool, float | None]:
    """Track whether an agent has been terminal for a stable period.

    A stable terminal is required because the runner passes through a brief
    terminal state between queued steers; following must not stop on that
    transient blip.

    Args:
        aid: Lubko agent ID.
        terminal_since: Time the terminal state was first observed, or ``None``.

    Returns:
        A ``(stop, terminal_since)`` pair where ``stop`` is ``True`` only once
        the terminal state has persisted for at least ``STABLE_TERMINAL_SECONDS``.
    """
    if _terminal_or_unknown(aid):
        if terminal_since is None:
            return False, time.time()
        if time.time() - terminal_since >= STABLE_TERMINAL_SECONDS:
            return True, terminal_since
        return False, terminal_since
    return False, None


def _print_snapshot(path: Path, follow_lines: int) -> tuple[int, str]:
    """Print the recent normalized snapshot and return the follow handoff state.

    Args:
        path: Log file path.
        follow_lines: Number of folded display lines to show.

    Returns:
        A ``(offset, pending)`` pair: the raw byte offset to continue
        following from, and any trailing incomplete escape fragment that the
        follow normalizer must keep to complete stripping at the boundary.
    """
    if not path.is_file():
        return 0, ""
    snapshot = tail_snapshot(path, follow_lines)
    if snapshot.display:
        sys.stdout.buffer.write(snapshot.display)
        sys.stdout.buffer.flush()
    return snapshot.offset, snapshot.pending


def _open_log(log_path: Path, offset: int) -> BinaryIO | None:
    """Open the agent log at ``offset``, or ``None`` when unavailable.

    Args:
        log_path: Log file path.
        offset: Byte offset to begin reading from.

    Returns:
        The open binary stream, or ``None`` when the file cannot be opened.
    """
    try:
        handle = log_path.open("rb")
    except OSError:
        return None
    handle.seek(offset)
    return handle


def _strip_ansi_keep_tail(text: str) -> tuple[str, str]:
    r"""Strip complete ANSI CSI/OSC sequences, returning any trailing partial one.

    A trailing fragment that could still become a complete CSI or OSC sequence
    (for example ``\x1b[3``) is returned separately so a sequence split across
    two reads is stripped once the rest arrives, without ever delaying
    ordinary text. A CSI prefix holds one ``\x1b`` and an OSC prefix holds at
    most two (its ``\x1b]`` start and the ``\x1b`` of a split ``\x1b\\``
    terminator), so only the last two ``\x1b`` positions are considered.

    Args:
        text: Text to normalize.

    Returns:
        A ``(stripped, pending)`` pair where ``pending`` is a maximal trailing
        fragment that may still form a complete ANSI control sequence.
    """
    stripped = _ANSI_RE.sub("", text)
    positions: list[int] = []
    for index, char in enumerate(stripped):
        if char == "\x1b":
            positions.append(index)
    for esc in positions[-2:]:
        tail = stripped[esc:]
        if _ANSI_CSI_PREFIX_RE.fullmatch(tail) or _ANSI_OSC_PREFIX_RE.fullmatch(tail):
            return stripped[:esc], tail
    return stripped, ""


class _LogNormalizer:
    """Incrementally strip ANSI CSI/SGR sequences from log bytes.

    Decodes incrementally (UTF-8 with replacement for invalid bytes) so a
    multi-byte character split across reads is preserved undamaged, and
    carries any trailing partial escape sequence across reads so a sequence
    split at a read boundary is still removed. Presentation only: the durable
    log file is never modified.
    """

    def __init__(self, pending: str = "") -> None:
        self._decoder: codecs.IncrementalDecoder = codecs.getincrementaldecoder("utf-8")(
            errors="replace"
        )
        self._pending = pending

    def write(self, data: bytes) -> None:
        """Normalize and write one chunk of log bytes to stdout.

        Args:
            data: Raw log bytes.
        """
        text = self._decoder.decode(data)
        if not text and not self._pending:
            return
        text = self._pending + text
        stripped, self._pending = _strip_ansi_keep_tail(text)
        if stripped:
            sys.stdout.buffer.write(stripped.encode("utf-8"))
            sys.stdout.buffer.flush()

    def close(self) -> None:
        """Flush any buffered decoder state and partial sequence tail."""
        text = self._decoder.decode(b"", final=True)
        text = self._pending + text
        self._pending = ""
        stripped, _pending = _strip_ansi_keep_tail(text)
        if stripped:
            sys.stdout.buffer.write(stripped.encode("utf-8"))
            sys.stdout.buffer.flush()


def _consume_log(handle: BinaryIO, normalizer: _LogNormalizer) -> bool:
    """Write any newly available log bytes to stdout, normalized.

    Args:
        handle: Open log stream.
        normalizer: Incremental ANSI-stripping normalizer for the stream.

    Returns:
        ``True`` when new bytes were written.
    """
    data = handle.read()
    if not data:
        return False
    normalizer.write(data)
    return True


def _drain_and_stop(handle: BinaryIO, normalizer: _LogNormalizer) -> None:
    """Write any remaining log bytes (normalized) and stop streaming.

    Args:
        handle: Open log stream.
        normalizer: Incremental ANSI-stripping normalizer for the stream.
    """
    data = handle.read()
    if data:
        normalizer.write(data)
    normalizer.close()


def _terminal_or_unknown(aid: str) -> bool:
    """Return whether the agent is terminal, unknown, or deleted.

    Before deciding, the durable record is reconciled: a runner/model process
    that disappeared mid-invocation must converge ``meta.json`` to an explicit
    terminal state instead of being observed as a stale ``unknown`` that leaves
    ``state=running / active_runner=true`` behind. Reconciliation rewrites only
    on an actual change, so follow polling ticks stay fsync-free when healthy.

    Args:
        aid: Lubko agent ID.

    Returns:
        ``True`` when streaming should stop.
    """
    meta = read_meta(aid)
    if meta is None:
        return True
    # Reconcile first so the decision below never returns a stale unknown for
    # a provably dead invocation without also converging the durable record.
    reconcile_meta(aid)
    meta = read_meta(aid)
    if meta is None:
        return True
    state = derive_state(meta)
    return state in TERMINAL_STATES or state == "unknown"


def _stale_running(aid: str) -> bool:
    """Return whether the agent records a running process that is dead.

    Args:
        aid: Lubko agent ID.

    Returns:
        ``True`` when the recorded process has stopped while state is running.
    """
    meta = read_meta(aid)
    if meta is None:
        return False
    return bool(meta.get("state") == "running" and meta.get("pid") and not is_alive(meta))


def cmd_log(args: argparse.Namespace) -> int:
    """Show agent output.

    Args:
        args: Parsed command arguments.

    Returns:
        A process exit code.
    """
    aid = args.agent_id
    meta = read_meta(aid)
    if meta is None:
        _err(f"{PROG}: unknown agent: {aid}")
        return EXIT_NOT_FOUND
    log_path = agent_dir(aid) / "output.log"
    if not log_path.is_file():
        if args.follow:
            # wait for the first output to appear or a terminal state
            if not _wait_for_first_output(aid, log_path) or not log_path.is_file():
                _err("(no output yet)")
                return EXIT_OK
        else:
            _err("(no output yet)")
            return EXIT_OK
    if args.follow:
        stream_log_until_terminal(aid, follow_lines=args.lines)
    else:
        for line in tail_lines(log_path, args.lines):
            _out(line)
    return EXIT_OK


def _can_produce_output(aid: str) -> bool:
    """Return whether the exact agent lifecycle can still write output.

    The agent is genuinely capable of producing output only while its exact
    recorded process is alive; in the brief running-before-pid window only the
    exact recorded runner can still start an invocation. Anything else —
    missing metadata, idle/terminal/unknown state, a recorded pid that is not
    alive, or a runner that is gone — means no further output can arrive, so
    waiting must stop promptly rather than poll forever.

    Args:
        aid: Lubko agent ID.

    Returns:
        ``True`` only while the exact recorded process (or, before the pid is
        recorded, the exact runner) is alive and the state is ``running``.
    """
    meta = read_meta(aid)
    if meta is None:
        return False
    if derive_state(meta) != "running":
        return False
    pid = meta.get("pid")
    if pid:
        return is_alive(meta)
    return runner_alive(meta)


def _wait_for_first_output(aid: str, log_path: Path) -> bool:
    """Wait for an agent's first log output while its lifecycle can still produce it.

    An agent marked running may not have created its output log yet when
    ``log --follow`` starts. There is no wall-clock cutoff: this polls until
    the log appears or the exact lifecycle proves it can no longer write
    output, so following a live agent never returns prematurely and a dead or
    idle one is never followed forever.

    Args:
        aid: Lubko agent ID.
        log_path: The agent's output log path.

    Returns:
        ``True`` when the log exists or the agent can no longer produce output.
    """
    while True:
        if log_path.is_file():
            return True
        if not _can_produce_output(aid):
            return True
        time.sleep(0.2)


def cmd_wait(args: argparse.Namespace) -> int:
    """Wait until an agent finishes.

    Args:
        args: Parsed command arguments.

    Returns:
        A process exit code.
    """
    aid = args.agent_id
    meta = read_meta(aid)
    if meta is None:
        _err(f"{PROG}: unknown agent: {aid}")
        return EXIT_NOT_FOUND
    timeout = args.timeout
    deadline = time.time() + timeout if timeout else None

    while True:
        meta = read_meta(aid)
        if meta is None:
            _err(f"{PROG}: unknown agent: {aid}")
            return EXIT_NOT_FOUND
        if is_alive(meta):
            if deadline and time.time() >= deadline:
                _err(f"{PROG}: wait: agent {aid} still running after {timeout}s")
                return EXIT_TIMEOUT
            time.sleep(1)
            continue
        # Process is gone; give the runner a moment to finalize state.
        for _ in range(20):
            current = read_meta(aid)
            if current is not None and current.get("state") != "running":
                meta = current
                break
            time.sleep(0.25)
        else:
            # The runner never finalized: converge the durable record instead
            # of returning while metadata still claims a running invocation.
            reconcile_meta(aid)
            meta = read_meta(aid) or meta
        break

    return exit_code_for(meta)


def _finish_stop_like(
    aid: str,
    identity: tuple[object, ...],
    terminal: tuple[int, int, str, str],
    success_msg: str,
) -> int:
    """Terminalize a targeted invocation, guarding against an A -> B race.

    The metadata is marked terminal under the lock only when the currently
    recorded invocation still matches the exact targeted identity; a newer
    live invocation recorded meanwhile is never overwritten or untracked.

    Args:
        aid: Lubko agent ID.
        identity: Exact invocation identity targeted.
        terminal: Exit code, exit signal, terminal state, and stop reason.
        success_msg: Message printed on success.

    Returns:
        ``EXIT_OK`` on success, ``EXIT_ERROR`` when identity changed.
    """
    exit_code, exit_signal, state, stop_reason = terminal
    if not _update_meta_if_same_invocation(
        aid, identity, lambda m: _mark_terminal(m, exit_code, exit_signal, state, stop_reason)
    ):
        _err(
            f"{PROG}: agent {aid} was restarted during the operation; "
            "newer invocation left running untouched"
        )
        return EXIT_ERROR
    _out(success_msg)
    return EXIT_OK


def _cancel_runner_work(aid: str, intent: str, meta: Meta) -> bool:
    """Durably record the stop-like cancellation of runner-owned work.

    When no invocation process group is live but a runner reservation, a
    proven-live runner, or an accepted ``pending_prompt`` can still start
    work, this establishes the stop-like decision in the same locked
    transaction that observed that ownership: the pending prompt and steer
    queue are dropped, the durable stop reason deactivates the runner, and any
    unclaimed reservation is dropped so a later claim fails closed.  The
    terminal record is written only after the exact observed runner identity
    is converged (see ``_converge_observed_runner``), so success is never
    reported while the pre-existing execution authority is still alive.

    The mutation is guarded by the exact observed ownership snapshot
    (invocation identity, pending prompt, and reservation generation), so a
    concurrently accepted newer invocation is never cancelled by a stale
    observation.

    Args:
        aid: Lubko agent ID.
        intent: The stop-like intent (``stop`` or ``kill``).
        meta: The pre-lock metadata snapshot that justified the cancellation.

    Returns:
        ``True`` only when the cancellation was applied under the lock.
    """
    identity = _invocation_identity(meta)
    try:
        expected_pending = _pending_prompt(meta)
    except MalformedPendingPromptMetadataError:
        return False
    res = meta.get("runner_reservation")
    expected_gen = res.get("gen") if isinstance(res, dict) else None
    applied = False

    def cancel(m: Meta) -> None:
        nonlocal applied
        cur_res = m.get("runner_reservation")
        cur_gen = cur_res.get("gen") if isinstance(cur_res, dict) else None
        try:
            current_pending = _pending_prompt(m)
        except MalformedPendingPromptMetadataError:
            return
        if (
            _invocation_identity(m) != identity
            or current_pending != expected_pending
            or cur_gen != expected_gen
        ):
            return
        _begin_stop_like(m, intent)
        m["stop_reason"] = intent
        _set_active_runner(m, value=False)
        applied = True

    update_meta(aid, cancel)
    return applied


def _persisted_runner_identity(meta: Meta) -> tuple[int, int, str] | None:
    """Return strict recorded-runner process and marker authority."""
    pid = _process_identity_int(meta.get("runner_pid"), minimum=1)
    ticks = _process_identity_int(meta.get("runner_start_time"), minimum=0)
    aid = _persisted_agent_id(meta.get("id"))
    if pid is None or ticks is None or aid is None:
        return None
    return pid, ticks, aid


def _runner_identity_state(pid: int, ticks: object, aid: str) -> str:
    """Classify the exact observed runner identity's liveness evidence.

    Transient ``/proc`` read failures must never be mistaken for death, so
    this distinguishes three states instead of a boolean: positively alive,
    positively gone (process exited, or provably replaced by a different
    start-time identity such as after PID reuse), and unprovable (the process
    is alive but its start ticks or environment marker cannot be read).
    Unprovable keeps convergence fail-closed: only positive evidence counts.

    Args:
        pid: The observed runner process ID.
        ticks: The observed runner start time in clock ticks.
        aid: Exact agent ID whose environment marker must match.

    Returns:
        ``"alive"``, ``"gone"``, or ``"unprovable"``.
    """
    if not pid_alive(pid):
        return "gone"
    if _is_zombie(pid):
        # A defunct process has no execution authority left.
        return "gone"
    current = proc_start_ticks(pid)
    if current is None:
        return "unprovable"
    if current != ticks:
        # Provably a different start identity occupies the PID now.
        return "gone"
    try:
        environ = Path(f"/proc/{pid}/environ").read_bytes()
    except OSError:
        return "unprovable"
    marker = f"LUBKO_AGENT_ID={aid}".encode()
    return "alive" if marker in environ.split(b"\0") else "unprovable"


def _converge_observed_runner(observed: Meta, mode: str) -> bool:
    """Converge the exact observed runner identity before terminalizing.

    The runner recorded in ``observed`` is signalled by its exact identity
    (start ticks plus per-agent marker, like deletion's convergence), never by
    a broad match.  A runner recorded later in metadata (a post-cancellation
    re-reservation by an explicit new prompt) is never signalled; once the
    observed identity is *positively* proven gone (exited, or provably
    replaced by a different start identity), the pre-existing execution
    authority it carried is converged regardless of what metadata names now.
    Identity that is merely unprovable (unreadable ``/proc`` data) is never
    counted as death: convergence keeps retrying within its bounded window
    and fails closed instead.

    Args:
        observed: The metadata snapshot taken when the cancellation was
            decided.
        mode: ``stop`` for graceful-then-forced termination of the runner,
            ``kill`` for an immediate forced kill.

    Returns:
        ``True`` only when the observed runner identity is provably gone.
    """
    raw_pid = observed.get("runner_pid")
    if raw_pid is None:
        # No runner was ever recorded (reserved pre-spawn window); nothing to
        # converge beyond dropping the reservation.
        return True
    identity = _persisted_runner_identity(observed)
    if identity is None:
        # Present malformed durable identity is ambiguous authority, not
        # absence and never signalling authority.
        return False
    pid, ticks, marker_aid = identity
    grace_signal = signal.SIGTERM if mode == "stop" else signal.SIGKILL
    signal_identity_checked(pid, ticks, grace_signal, marker_aid=marker_aid)

    def positively_gone() -> bool:
        return _runner_identity_state(pid, ticks, marker_aid) == "gone"

    deadline = time.time() + (STOP_WAIT_SECONDS if mode == "stop" else KILL_WAIT_SECONDS)
    while time.time() < deadline:
        if positively_gone():
            return True
        time.sleep(0.05)
    if positively_gone():
        return True
    if mode == "stop":
        # Grace period expired: escalate exactly like the invocation-group
        # path so no managed runner survives a successful stop.
        signal_identity_checked(pid, ticks, signal.SIGKILL, marker_aid=marker_aid)
        deadline = time.time() + KILL_WAIT_SECONDS
        while time.time() < deadline:
            if positively_gone():
                return True
            time.sleep(0.05)
    return False


def _no_invocation_owned(meta: Meta) -> bool:
    """Return whether nothing beyond a live invocation could still start work.

    Args:
        meta: Agent metadata.

    Returns:
        ``True`` when no proven-live runner, in-flight reservation, or
        accepted pending prompt remains.
    """
    try:
        pending = _pending_prompt(meta)
    except MalformedPendingPromptMetadataError:
        return False
    return not runner_alive(meta) and not reservation_in_flight(meta) and pending is None


def cmd_stop(args: argparse.Namespace) -> int:
    """Gracefully stop a running agent.

    Sends ``SIGTERM`` to the exact recorded process group, then — while any
    member of that exact group remains — ``SIGKILL`` after the grace period,
    mirroring the worker's cancellation contract so an agent run can never
    leave abandoned OpenCode children behind. Terminalization is guarded by
    the exact recorded invocation identity: if the runner records a newer
    invocation while the old one is being stopped, the newer record is never
    overwritten and the command reports failure instead of false success.

    Even with no live invocation process group, a reserved-but-unclaimed
    runner, a proven-live runner between invocations, or an accepted pending
    prompt could still start work; stop cancels exactly that owned work under
    the per-agent metadata lock instead of reporting false quiescence
    (issue #185).

    Args:
        args: Parsed command arguments.

    Returns:
        A process exit code.
    """
    aid = args.agent_id
    meta = read_meta(aid)
    if meta is None:
        _err(f"{PROG}: unknown agent: {aid}")
        return EXIT_NOT_FOUND
    while True:
        if is_alive(meta) or group_alive(meta):
            return _signal_live_invocation(aid, meta, "stop")
        if _no_invocation_owned(meta):
            _out(f"{PROG}: agent {aid} is already stopped (state {derive_state(meta)})")
            return EXIT_OK
        if _cancel_runner_work(aid, "stop", meta):
            return _finish_cancelled_runner_work(aid, meta, "stop")
        # Ownership changed concurrently (a newer invocation was recorded);
        # re-read and retry against the fresh state.
        newer = read_meta(aid)
        if newer is None:
            _out(f"{PROG}: agent {aid} is already stopped")
            return EXIT_OK
        meta = newer


def cmd_kill(args: argparse.Namespace) -> int:
    """Forcefully terminate a running agent.

    The exact recorded process group is signalled and confirmed gone, so
    ``SIGKILL`` to the leader alone can never leave group members behind.
    Terminalization is guarded by the exact recorded invocation identity: a
    newer invocation recorded mid-kill is never overwritten or falsely
    reported as killed.

    Like stop, kill also converges reserved or queued runner work when no
    invocation process group is live, instead of falsely reporting the agent
    as dead (issue #185).

    Args:
        args: Parsed command arguments.

    Returns:
        A process exit code.
    """
    aid = args.agent_id
    meta = read_meta(aid)
    if meta is None:
        _err(f"{PROG}: unknown agent: {aid}")
        return EXIT_NOT_FOUND
    while True:
        if is_alive(meta) or group_alive(meta):
            return _signal_live_invocation(aid, meta, "kill")
        if _no_invocation_owned(meta):
            _out(f"{PROG}: agent {aid} is already dead (state {derive_state(meta)})")
            return EXIT_OK
        if _cancel_runner_work(aid, "kill", meta):
            return _finish_cancelled_runner_work(aid, meta, "kill")
        newer = read_meta(aid)
        if newer is None:
            _out(f"{PROG}: agent {aid} is already dead")
            return EXIT_OK
        meta = newer


def _finish_cancelled_runner_work(aid: str, observed: Meta, mode: str) -> int:
    """Converge the observed runner and terminalize the cancelled work.

    The stop-like decision is already durably recorded (no new work can
    claim); success additionally requires the exact observed runner identity
    to be provably gone.  Only then is the terminal record written under the
    lock.  If convergence fails, the command fails closed with a coherent,
    retryable record (intent and reason kept) instead of reporting false
    success.

    Args:
        aid: Lubko agent ID.
        observed: The metadata snapshot taken when cancellation was decided.
        mode: ``stop`` or ``kill``.

    Returns:
        A process exit code.
    """
    if not _converge_observed_runner(observed, mode):
        _err(f"{PROG}: agent {aid} did not converge; its recorded runner is still alive")
        return EXIT_ERROR
    state = "stopped" if mode == "stop" else "killed"
    if not _update_meta_if_same_invocation(
        aid,
        _invocation_identity(observed),
        lambda m: _mark_terminal(m, None, None, state, mode),
    ):
        # A newer invocation was recorded during convergence (a later explicit
        # prompt re-reserved the agent); it must not be terminalized here.
        verb = "stop" if mode == "stop" else "kill"
        _err(f"{PROG}: agent {aid} changed during {verb}; newer invocation left running untouched")
        return EXIT_ERROR
    verb = "stopped" if mode == "stop" else "killed"
    _out(f"{verb} agent {aid} (cancelled reserved runner work)")
    return EXIT_OK


def _signal_live_invocation(aid: str, meta: Meta, mode: str) -> int:
    """Signal, converge, and terminalize one live recorded invocation.

    Args:
        aid: Lubko agent ID.
        meta: Metadata whose recorded invocation identity is targeted.
        mode: ``stop`` for graceful-then-forced termination, ``kill`` for an
            immediate forced kill.

    Returns:
        A process exit code.
    """
    identity = _invocation_identity(meta)
    if not _update_meta_if_same_invocation(aid, identity, lambda m: _begin_stop_like(m, mode)):
        verb = "stop" if mode == "stop" else "kill"
        _err(f"{PROG}: agent {aid} changed during {verb}; newer invocation left running untouched")
        return EXIT_ERROR
    grace_signal = signal.SIGKILL if mode == "kill" else signal.SIGTERM
    send_signal_group(meta, grace_signal)
    wait_seconds = KILL_WAIT_SECONDS if mode == "kill" else STOP_WAIT_SECONDS
    if wait_group_dead(meta, wait_seconds):
        return _finish_stop_like(
            aid,
            identity,
            (-grace_signal, grace_signal, "killed" if mode == "kill" else "stopped", mode),
            f"{'killed' if mode == 'kill' else 'stopped'} agent {aid}",
        )
    if mode == "stop":
        # The exact group still has live members; escalate so no child is
        # abandoned, exactly like the worker's cancel grace period.
        if group_alive(meta):
            send_signal_group(meta, signal.SIGKILL)
        if wait_group_dead(meta, KILL_WAIT_SECONDS):
            return _finish_stop_like(
                aid,
                identity,
                (-signal.SIGKILL, signal.SIGKILL, "stopped", "stop"),
                f"stopped agent {aid} (force-killed group members)",
            )
        _update_meta_if_same_invocation(aid, identity, lambda m: m.update(intent=None))
        _err(f"{PROG}: agent {aid} did not stop within {STOP_WAIT_SECONDS:.0f}s; use 'kill'")
        return EXIT_ERROR
    _update_meta_if_same_invocation(aid, identity, lambda m: m.update(intent=None))
    _err(f"{PROG}: agent {aid} could not be killed")
    return EXIT_ERROR


def _begin_delete(aid: str, *, force: bool) -> Meta | None:
    """Durably record deletion intent under the metadata lock.

    The tombstone is written in the same locked transaction that snapshots
    every exact process identity, so it is serialized against the runner's
    claim/startup: any runner claiming after this point observes the tombstone
    and bails instead of recreating state.  The snapshot is what deletion then
    converges on.

    Args:
        aid: Lubko agent ID.
        force: Whether forced kill semantics were requested (records the
            ``kill`` intent exactly like ``cmd_kill`` does).

    Returns:
        The exact identity snapshot taken under the lock, or ``None`` when the
        agent state vanished before the tombstone could be written.
    """
    snapshot: dict[str, Meta] = {}

    def fn(m: Meta) -> None:
        if _delete_pending_flag(m) is None:
            return
        m["delete_pending"] = True
        if force:
            m["intent"] = "kill"
        m["last_activity_at"] = time.time()
        res = m.get("runner_reservation")
        marker = m.get("unresolved_invocation")
        snapshot["meta"] = {
            "id": aid,
            "pid": m.get("pid"),
            "pgid": m.get("pgid"),
            "start_time": m.get("start_time"),
            "invocation_id": m.get("invocation_id"),
            "runner_pid": m.get("runner_pid"),
            "runner_start_time": m.get("runner_start_time"),
            "active_runner": m.get("active_runner"),
            "runner_reservation": dict(res) if isinstance(res, dict) else None,
            "unresolved_invocation": dict(marker) if isinstance(marker, dict) else None,
            "delete_pending": True,
        }

    update_meta(aid, fn)
    return snapshot.get("meta")


def _delete_converged(cur: Meta | None) -> bool:
    """Return whether no runner or invocation of the agent can still execute.

    Args:
        cur: Freshly read metadata, or ``None`` when the state is gone.

    Returns:
        ``True`` only when the exact runner, the invocation group, and any
        reserved-but-unclaimed runner are all provably gone.
    """
    if cur is None:
        return True
    if _delete_pending_flag(cur) is not True:
        return False
    if _active_runner_flag(cur) is None:
        return False
    raw_runner_pid = cur.get("runner_pid")
    if raw_runner_pid is not None and (
        _process_identity_int(raw_runner_pid, minimum=1) is None
        or _process_identity_int(cur.get("runner_start_time"), minimum=0) is None
    ):
        # Present malformed runner identity is unresolved durable authority.
        # Liveness helpers intentionally return false for malformed identity,
        # but deletion must not reinterpret that as positive convergence.
        return False
    if _unresolved_child_state(cur) != "gone":
        # An exact unrecorded invocation was never positively proven gone
        # (still live, or evidence ambiguous): deletion must not remove
        # state while it may still execute.
        return False
    return not runner_alive(cur) and not group_alive(cur) and not reservation_in_flight(cur)


def _signal_unresolved_child(meta: Meta) -> None:
    """Signal an unresolved child through exact-identity-safe pinned logic.

    Signals only when the persisted marker is well-formed; malformed or
    ambiguous state is left untouched so convergence keeps failing closed.
    Never falls back to a numeric PID/PGID signal.

    Args:
        meta: Metadata carrying the ``unresolved_invocation`` marker.
    """
    rec = meta.get("unresolved_invocation")
    if (
        not isinstance(rec, dict)
        or not isinstance(rec.get("pid"), int)
        or isinstance(rec.get("pid"), bool)
        or rec["pid"] <= 0
    ):
        return
    send_signal_group(
        {
            "id": meta.get("id"),
            "pid": rec["pid"],
            "pgid": rec["pid"],
            "start_time": rec.get("start_time"),
            "invocation_id": rec.get("invocation_id"),
        },
        signal.SIGKILL,
    )


def _abort_delete(aid: str) -> None:
    """Clear only a canonical deletion tombstone after failed convergence."""

    def clear(m: Meta) -> None:
        if _delete_pending_flag(m) is True:
            m["delete_pending"] = False

    update_meta(aid, clear)


def _remove_deleted_state(aid: str) -> bool:
    """Remove the agent directory and verify it stays gone.

    Args:
        aid: Lubko agent ID.

    Returns:
        ``True`` only when the directory is provably absent afterwards.
    """
    shutil.rmtree(agent_dir(aid), ignore_errors=True)
    return not agent_dir(aid).exists()


def _signal_delete_execution(cur: Meta) -> bool:
    """Signal exact recorded execution identities during forced deletion.

    Returns:
        ``True`` when every present identity is canonical and was signalled.
    """
    raw_runner_pid = cur.get("runner_pid")
    if raw_runner_pid is not None:
        identity = _persisted_runner_identity(cur)
        if identity is None:
            return False
        runner_pid, runner_ticks, marker_aid = identity
        signal_identity_checked(
            runner_pid,
            runner_ticks,
            signal.SIGKILL,
            marker_aid=marker_aid,
        )
    if group_alive(cur):
        send_signal_group(cur, signal.SIGKILL)
    _signal_unresolved_child(cur)
    return True


def _converge_for_delete(aid: str, *, force: bool, deadline: float) -> bool:
    """Converge the exact runner and invocation before state removal.

    Args:
        aid: Lubko agent ID.
        force: Whether live processes may be signalled.
        deadline: Absolute time by which convergence must be proven.

    Returns:
        ``True`` only when no runner or invocation can still execute.
    """
    while True:
        cur = read_meta(aid)
        if cur is not None:
            if _delete_pending_flag(cur) is not True:
                return False
            if force:
                if not _signal_delete_execution(cur):
                    return False
            elif not _delete_converged(cur):
                # Something became live between the decision and the
                # tombstone; non-forced deletion must not kill it.
                return False
        if _delete_converged(cur):
            return True
        if time.time() >= deadline:
            return False
        time.sleep(0.05)


def cmd_delete(args: argparse.Namespace) -> int:
    """Delete an agent's local state and logs.

    Deletion establishes lifecycle ownership with a durable tombstone written
    under the metadata lock (so a reserved runner can never claim afterwards),
    then converges both the exact managed runner identity and the exact
    invocation process group before removing any state.  If convergence cannot
    be proven the deletion fails closed, keeps the agent state intact for a
    retry, and never reports success.

    Args:
        args: Parsed command arguments.

    Returns:
        A process exit code.
    """
    aid = args.agent_id
    meta = read_meta(aid)
    if meta is None:
        _err(f"{PROG}: unknown agent: {aid}")
        return EXIT_NOT_FOUND
    live = is_alive(meta) or group_alive(meta) or runner_alive(meta) or reservation_in_flight(meta)
    if live and not args.force:
        _err(f"{PROG}: agent {aid} is running; stop it first or use --force")
        return EXIT_ERROR
    if _begin_delete(aid, force=args.force) is None:
        _err(f"{PROG}: agent {aid} could not establish deletion authority")
        return EXIT_ERROR
    if not _converge_for_delete(aid, force=args.force, deadline=time.time() + KILL_WAIT_SECONDS):
        # Fail closed: convergence was not proven, so keep the retryable state.
        _abort_delete(aid)
        if args.force:
            _err(f"{PROG}: agent {aid} could not be deleted; runner or invocation did not converge")
        else:
            _err(f"{PROG}: agent {aid} started running during delete; use --force")
        return EXIT_ERROR
    if _remove_deleted_state(aid):
        _out(f"deleted agent {aid}")
        return EXIT_OK
    _abort_delete(aid)
    _err(f"{PROG}: agent {aid} directory could not be removed")
    return EXIT_ERROR


def _retention_clean_live(meta: Meta) -> bool:
    """Return whether an enumerated retention candidate became live again.

    Args:
        meta: Freshly read metadata of the candidate.

    Returns:
        ``True`` when the agent can no longer be safely removed.
    """
    return (
        derive_state(meta) not in TERMINAL_STATES
        or is_alive(meta)
        or group_alive(meta)
        or runner_alive(meta)
        or reservation_in_flight(meta)
    )


def _retention_remove(aid: str, deadline: float) -> str:
    """Remove one terminal agent's state through the safe delete machinery.

    Reuses the exact tombstone/convergence/removal path of ``delete`` so the
    metadata lock serializes this against any runner claim or prompt that
    starts after candidate enumeration.  A candidate that becomes live is
    skipped and never signalled; convergence failure keeps the retryable state
    and never reports a successful removal.

    Args:
        aid: Lubko agent ID.
        deadline: Absolute time by which deletion must converge.

    Returns:
        One of ``"removed"``, ``"skipped"``, and ``"failed"``.
    """
    meta = read_meta(aid)
    if meta is None:
        return "skipped"
    if _retention_clean_live(meta):
        return "skipped"
    if _begin_delete(aid, force=False) is None:
        return "skipped"
    if not _converge_for_delete(aid, force=False, deadline=deadline):
        # Fail closed: it became live between the tombstone and convergence.
        _abort_delete(aid)
        return "skipped"
    if not _remove_deleted_state(aid):
        _abort_delete(aid)
        return "failed"
    return "removed"


def cmd_clean(args: argparse.Namespace) -> int:
    """Garbage-collect old finished agents.

    Dry-run mode is strictly observational and never mutates any state.
    Actual removal goes through the same lifecycle-safe delete machinery as
    ``delete``: a candidate that turned promptable/live after enumeration is
    skipped, its state is never raw-deleted, and no false removal is reported.

    Args:
        args: Parsed command arguments.

    Returns:
        A process exit code.
    """
    days = _retention_days(args.days)
    if days < 0:
        _err(f"{PROG}: clean: invalid retention days: {days}")
        return EXIT_USAGE
    candidates = _clean_candidates(days)
    removed = 0
    failed = False
    deadline = time.time() + KILL_WAIT_SECONDS
    for aid in candidates:
        if args.dry_run:
            _out(f"would remove agent {aid}")
            removed += 1
            continue
        outcome = _retention_remove(aid, deadline)
        if outcome == "removed":
            _out(f"removed agent {aid}")
            removed += 1
        elif outcome == "skipped":
            _out(f"skipped agent {aid}: became live after enumeration")
        else:
            failed = True
            _err(f"{PROG}: agent {aid} could not be removed")

    if not candidates:
        _out("(nothing to clean)")
    else:
        _out(f"({removed} agent(s))")
    if failed:
        return EXIT_ERROR
    return EXIT_OK


def _retention_days(explicit: int | None) -> int:
    """Resolve the retention period in days.

    Args:
        explicit: Explicit retention days, or ``None``.

    Returns:
        The effective retention days.
    """
    if explicit is not None:
        return explicit
    try:
        return int(os.environ.get("LUBKO_AGENT_RETENTION_DAYS", str(DEFAULT_RETENTION_DAYS)))
    except ValueError:
        return DEFAULT_RETENTION_DAYS


def _clean_candidates(days: int) -> list[str]:
    """Return terminal agents finished before the retention cutoff.

    Args:
        days: Retention period in days.

    Returns:
        The agent IDs that may be removed.
    """
    cutoff = time.time() - days * SECONDS_PER_DAY
    candidates = []
    for aid in _agent_ids():
        meta = read_meta(aid)
        if meta is None:
            continue
        if derive_state(meta) not in TERMINAL_STATES:
            continue
        finished = _persisted_timestamp(meta.get("finished_at"))
        if finished is not None and finished < cutoff:
            candidates.append(aid)
    return candidates


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _ArgumentSpec:
    """Specification for one command line argument."""

    flags: tuple[str, ...]
    kwargs: dict[str, object]


@dataclass(frozen=True, slots=True)
class _SubcommandSpec:
    """Specification for one subcommand."""

    name: str
    help: str
    func: Callable[[argparse.Namespace], int]
    arguments: tuple[_ArgumentSpec, ...]


def _arg(*flags: str, **kwargs: object) -> _ArgumentSpec:
    """Build an argument specification.

    Args:
        *flags: Argument flags and names.
        **kwargs: Argument keyword options.

    Returns:
        The argument specification.
    """
    return _ArgumentSpec(flags=flags, kwargs=kwargs)


SUBCOMMANDS: Final = (
    _SubcommandSpec(
        name="new",
        help="create an idle managed agent session with a caller-supplied ID",
        func=cmd_new,
        arguments=(
            _arg(
                "--id",
                metavar="ID",
                default=None,
                help="base-16 agent ID chosen by the caller (required)",
            ),
            _arg(
                "--cwd",
                metavar="DIR",
                default=None,
                help="working directory for the agent (default: current directory)",
            ),
            _arg("--title", metavar="TEXT", default=None, help="short display title"),
            _arg("--json", action="store_true", help="machine-readable output"),
        ),
    ),
    _SubcommandSpec(
        name="list",
        help="list Lubko-managed agents",
        func=cmd_list,
        arguments=(
            _arg("--running", action="store_true", help="only running agents"),
            _arg("--finished", action="store_true", help="only finished agents"),
            _arg("--succeeded", action="store_true", help="only succeeded agents"),
            _arg("--failed", action="store_true", help="only failed agents"),
            _arg("--stopped", action="store_true", help="only stopped agents"),
            _arg("--killed", action="store_true", help="only killed agents"),
            _arg(
                "--limit",
                type=int,
                default=None,
                metavar="N",
                help="maximum number of agents to show",
            ),
            _arg("--json", action="store_true", help="machine-readable output"),
        ),
    ),
    _SubcommandSpec(
        name="status",
        help="show detailed status of one agent",
        func=cmd_status,
        arguments=(
            _arg("--id", metavar="ID", default=None, help="agent ID (preferred)"),
            _arg("agent_id", nargs="?", metavar="ID", help="agent ID (positional alias)"),
            _arg("--json", action="store_true", help="machine-readable output"),
        ),
    ),
    _SubcommandSpec(
        name="prompt",
        help="start or continue an agent invocation, following it by default",
        func=cmd_prompt,
        arguments=(
            _arg(
                "--id",
                metavar="ID",
                default=None,
                help="agent ID (required; chosen by the caller)",
            ),
            _arg(
                "--steer",
                action="store_true",
                help="while the agent is running: interrupt the current run and redirect it",
            ),
            _arg(
                "--detach",
                action="store_true",
                help="start/queue the invocation and return immediately without following",
            ),
            _arg("--prompt", metavar="TEXT", default=None, help=argparse.SUPPRESS),
            _arg("prompt_text", metavar="PROMPT", help="instruction (positional)"),
            _arg("--json", action="store_true", help="machine-readable output"),
        ),
    ),
    _SubcommandSpec(
        name="log",
        help="show agent output",
        func=cmd_log,
        arguments=(
            _arg("agent_id", metavar="ID"),
            _arg(
                "--lines",
                type=int,
                default=50,
                metavar="N",
                help="number of recent lines (0 for all, default 50)",
            ),
            _arg("--follow", action="store_true", help="stream new output until the agent exits"),
        ),
    ),
    _SubcommandSpec(
        name="wait",
        help="wait until an agent finishes",
        func=cmd_wait,
        arguments=(
            _arg("agent_id", metavar="ID"),
            _arg(
                "--timeout",
                type=int,
                default=None,
                metavar="SEC",
                required=True,
                help="give up after SEC seconds without killing the agent",
            ),
        ),
    ),
    _SubcommandSpec(
        name="stop",
        help="gracefully stop a running agent",
        func=cmd_stop,
        arguments=(_arg("agent_id", metavar="ID"),),
    ),
    _SubcommandSpec(
        name="kill",
        help="forcefully terminate a running agent",
        func=cmd_kill,
        arguments=(_arg("agent_id", metavar="ID"),),
    ),
    _SubcommandSpec(
        name="delete",
        help="delete an agent's local state and logs",
        func=cmd_delete,
        arguments=(
            _arg("agent_id", metavar="ID"),
            _arg("--force", action="store_true", help="kill a running agent before deleting it"),
        ),
    ),
    _SubcommandSpec(
        name="clean",
        help="garbage-collect old finished agents",
        func=cmd_clean,
        arguments=(
            _arg(
                "--days",
                type=int,
                default=None,
                metavar="N",
                help="retention period in days (default 14)",
            ),
            _arg("--dry-run", action="store_true", help="only list what would be removed"),
        ),
    ),
)


def build_parser() -> argparse.ArgumentParser:
    """Build the ``lubko-agent`` command line parser.

    Returns:
        The configured parser.
    """
    parser = argparse.ArgumentParser(
        prog=PROG,
        description=(
            "Manage long-running Lubko agent sessions.  The orchestrator uses "
            "Lubko agent IDs only; the underlying agent implementation is hidden."
        ),
    )
    sub = parser.add_subparsers(dest="command", metavar="COMMAND")
    for spec in SUBCOMMANDS:
        subparser = sub.add_parser(spec.name, help=spec.help)
        for argument in spec.arguments:
            subparser.add_argument(*argument.flags, **cast("Any", argument.kwargs))
        subparser.set_defaults(func=spec.func)
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the ``lubko-agent`` command line interface.

    Args:
        argv: Command line arguments, or ``None`` to use ``sys.argv``.

    Returns:
        A process exit code.
    """
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)
    argv = list(argv) if argv is not None else sys.argv[1:]

    # Hidden internal entry point used by the background runner.
    if argv and argv[0] == "_runner":
        if len(argv) != RUNNER_ARGV_LENGTH:
            return EXIT_USAGE
        runner(argv[1], argv[2])
        return EXIT_OK

    parser = build_parser()
    args = parser.parse_args(argv)
    if not getattr(args, "command", None):
        parser.print_help()
        return EXIT_USAGE
    return cast("int", args.func(args))


if __name__ == "__main__":
    sys.exit(main())
