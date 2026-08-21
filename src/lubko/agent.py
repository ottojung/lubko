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
import fcntl
import json
import os
import re
import shutil
import signal
import sqlite3
import subprocess
import sys
import textwrap
import time
from dataclasses import dataclass
from itertools import starmap
from pathlib import Path
from typing import TYPE_CHECKING, Any, BinaryIO, Final, cast

from lubko.worker import group_has_members

if TYPE_CHECKING:
    from collections.abc import Callable

# Agent metadata: a JSON-serializable mapping with heterogeneous values.
Meta = dict[str, Any]

# Implementation details (hidden from the user-facing interface).
DEFAULT_MODEL: Final = "opencode-go/ox-alpha-free"
DEFAULT_VARIANT: Final = "low"
OPENCODE_TITLE_PREFIX: Final = "lubko-"  # native session title prefix used for discovery
TERMINAL_STATES: Final = ("succeeded", "failed", "stopped", "killed")
STOP_REASONS: Final = frozenset({"stop", "kill"})
PROG: Final = "lubko-agent"
HEX_DIGITS: Final = frozenset("0123456789abcdef")

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
    tmp = directory / "meta.json.tmp"
    path = directory / "meta.json"
    with tmp.open("w", encoding="utf-8") as fh:
        json.dump(meta, fh, indent=2, sort_keys=True)
        fh.write("\n")
    tmp.replace(path)


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
        "runner_pid": None,
        "runner_start_time": None,
        "started_at": None,
        "finished_at": None,
        "exit_code": None,
        "exit_signal": None,
        "intent": None,
        "stop_reason": None,
        "active_runner": False,
        "runner_gen": 0,
        "runner_reservation": None,
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
    marker = f"LUBKO_AGENT_ID={aid}".encode()
    try:
        environ = Path(f"/proc/{pid}/environ").read_bytes()
    except OSError:
        return False
    return marker in environ.split(b"\0")


def is_alive(meta: Meta) -> bool:
    """Return whether the recorded process is really our agent process.

    A PID alone is not trusted: the process start time (ticks) and a
    per-agent environment marker must both match, so a reused or recycled
    PID can never be mistaken for our agent.

    Args:
        meta: Agent metadata.

    Returns:
        ``True`` only when a live process matches every recorded identity.
    """
    pid = meta.get("pid")
    if not pid:
        return False
    if proc_start_ticks(pid) != meta.get("start_time"):
        return False
    if not env_has_marker(pid, meta.get("id", "")):
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def send_signal_group(meta: Meta, sig: int) -> None:
    """Deliver a signal to the agent's process group (session leader).

    Args:
        meta: Agent metadata.
        sig: Signal to deliver.
    """
    pid = meta.get("pid")
    if not pid:
        return
    with contextlib.suppress(ProcessLookupError):
        os.killpg(pid, sig)  # the agent was launched as its own session leader


def group_alive(meta: Meta) -> bool:
    """Return whether any live process remains in the agent's process group.

    Args:
        meta: Agent metadata.

    Returns:
        ``True`` when the recorded process group still has live members.
    """
    pgid = meta.get("pgid")
    return bool(pgid and group_has_members(int(pgid)))


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

    The runner PID is anchored by its start time and the per-agent
    environment marker, exactly like the agent process itself, so a recycled
    PID can never be mistaken for a live runner.

    Args:
        meta: Agent metadata.

    Returns:
        ``True`` only when a live process matches every recorded runner
        identity field.
    """
    pid = meta.get("runner_pid")
    if not pid:
        return False
    if proc_start_ticks(int(pid)) != meta.get("runner_start_time"):
        return False
    if not env_has_marker(int(pid), meta.get("id", "")):
        return False
    try:
        os.kill(int(pid), 0)
    except OSError:
        return False
    return True


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
        try:
            environ = (entry / "environ").read_bytes()
        except OSError:
            continue
        fields = environ.split(b"\0")
        if agent_marker in fields and gen_marker in fields:
            return True
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

    A PID alone is not trusted: the process must be live (not a zombie), its
    start time (ticks) must be readable, and it must match the recorded owner
    identity.  Unavailable ticks or a zombie owner fail closed, so a reused PID
    or a defunct owner can never justify the reservation.

    Args:
        owner: Recorded owner process ID, or ``None``.
        owner_ticks: Recorded owner start time in clock ticks, or ``None``.

    Returns:
        ``True`` only when the live process is the exact recorded owner.
    """
    if not isinstance(owner, int) or not pid_alive(owner):
        return False
    if _is_zombie(owner):
        return False
    current = proc_start_ticks(owner)
    if current is None:
        return False
    return current == owner_ticks


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
    if not meta.get("active_runner"):
        return False
    if runner_alive(meta):
        return True
    res = meta.get("runner_reservation")
    if not isinstance(res, dict) or res.get("state") != "reserved":
        return False
    if _owner_alive(res.get("owner_pid"), res.get("owner_start_ticks")):
        return True
    return _runner_marker_alive(meta.get("id", "") or "", int(res.get("gen") or 0))


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
    if not isinstance(res, dict) or res.get("owner_pid") != caller_pid:
        return False
    recorded = res.get("owner_start_ticks")
    current = proc_start_ticks(caller_pid)
    return recorded is not None and current is not None and current == recorded


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
    if not meta.get("active_runner"):
        return True
    if runner_alive(meta):
        return True
    res = meta.get("runner_reservation")
    # A ``reserved`` (not yet claimed) reservation is explicitly recoverable by
    # another caller, so it justifies ``active_runner``; a ``claimed``
    # reservation whose runner is no longer provably alive is stuck and must
    # never justify a persistent ``active_runner``.
    return isinstance(res, dict) and res.get("state") == "reserved"


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


def derive_state(meta: Meta | None) -> str:
    """Return the live state, verifying process liveness rather than trusting metadata.

    Args:
        meta: Agent metadata, or ``None``.

    Returns:
        The effective agent state.
    """
    if not meta:
        return "unknown"
    state = meta.get("state")
    if state != "running":
        return state or "idle"
    pid = meta.get("pid")
    if pid and is_alive(meta):
        return "running"
    if not pid:
        # Launched but the runner has not recorded the PID yet.
        launched = meta.get("started_at") or meta.get("created_at") or 0
        return "running" if time.time() - launched < PID_START_WINDOW_SECONDS else "unknown"
    if meta.get("finished_at"):
        return str(state)  # runner finalized it
    return "unknown"


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


def build_agent_command(meta: Meta, prompt: str, *, is_continue: bool) -> list[str] | None:
    """Return the argv used to launch the underlying agent for this invocation.

    Args:
        meta: Agent metadata.
        prompt: Instruction to run.
        is_continue: Whether to continue the existing underlying session.

    Returns:
        The command argv, or ``None`` when continuation is impossible.
    """
    env_cmd = os.environ.get("LUBKO_AGENT_CMD")
    if env_cmd:
        return ["/bin/sh", "-c", env_cmd]
    model = DEFAULT_MODEL
    variant = meta.get("variant") or DEFAULT_VARIANT
    cwd = meta.get("cwd") or str(Path.cwd())
    if is_continue:
        session_id = meta.get("native_session_id") or discover_session_id(meta.get("id", ""))
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
    if meta.get("pending_prompt") == prompt:
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
    directory = agent_dir(aid)
    directory.mkdir(parents=True, exist_ok=True)
    log_path = directory / "output.log"
    cwd = meta.get("cwd") or str(Path.cwd())
    env = dict(os.environ)
    env["LUBKO_AGENT_ID"] = aid
    env["NO_COLOR"] = "1"
    ctx = _RunnerContext(aid=aid, log_path=log_path, cwd=cwd, env=env)
    try:
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
        prompt = meta.get("pending_prompt")
        if not prompt:
            if _reclaim_prompt(ctx.aid):
                continue
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
        if m.get("pending_prompt") or (m.get("steer_queue") or []):
            if (m.get("steer_queue") or []) and not m.get("pending_prompt"):
                _pop_into_pending(m, time.time())
            m["active_runner"] = True
            holder["busy"] = True
            return
        _set_active_runner(m, value=False)

    update_meta(aid, apply)
    return holder["busy"]


def _abort_runner(aid: str) -> None:
    """Clean up an agent after an unexpected runner failure.

    The exact recorded invocation process group is killed and the agent is
    finalized, so an abnormal runner exit can never leave the invocation
    running untracked with ``active_runner`` stuck true.

    Args:
        aid: Lubko agent ID.
    """
    meta = read_meta(aid)
    if meta is None or meta.get("state") != "running":
        return
    pid = meta.get("pid")
    if pid:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(int(pid), signal.SIGKILL)
    update_meta(aid, _finalize_abort())


@dataclass(frozen=True, slots=True)
class _RunnerContext:
    """Shared state for one runner invocation stream."""

    aid: str
    log_path: Path
    cwd: str
    env: dict[str, str]


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
    update_meta(aid, lambda m: _clear_pending(m, prompt))
    try:
        log = ctx.log_path.open("ab")
    except OSError:
        return None  # agent directory no longer exists
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
            update_meta(aid, lambda m: _finalize_terminal(m, 127, None, "failed", error))
            update_meta(aid, lambda m: _set_active_runner(m, value=False))
            return None

        start = proc_start_ticks(proc.pid)
        update_meta(aid, _record_running(proc, start))

        try:
            rc = _wait_for_invocation_exit(proc, aid, is_continue=is_continue)
        except BaseException:
            # Abnormal runner exit: never leave the invocation process group
            # running untracked, and never leave the agent stuck "running".
            with contextlib.suppress(ProcessLookupError):
                os.killpg(proc.pid, signal.SIGKILL)
            with contextlib.suppress(OSError):
                proc.wait()
            update_meta(aid, _finalize_abort())
            raise
        update_meta(aid, _finalize_after(rc))

    return _drain_next(aid)


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
    if not is_continue and not os.environ.get("LUBKO_AGENT_CMD"):
        deadline = time.time() + SESSION_DISCOVER_TIMEOUT_SECONDS
        while time.time() < deadline and proc.poll() is None:
            sid = discover_session_id(aid)
            if sid:
                update_meta(aid, _set_native_session(sid))
                break
            time.sleep(SESSION_DISCOVER_POLL_SECONDS)
    return proc.wait()


def _record_running(proc: subprocess.Popen[bytes], start: int | None) -> Callable[[Meta], None]:
    """Return a metadata mutation that records a running agent process.

    Args:
        proc: The spawned agent process.
        start: The process start time in clock ticks.

    Returns:
        The metadata mutation.
    """

    def record(m: Meta) -> None:
        m["pid"] = proc.pid
        m["pgid"] = proc.pid
        m["start_time"] = start
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
        if m.get("stop_reason") in STOP_REASONS:
            _set_active_runner(m, value=False)
            return
        if m.get("pending_prompt"):
            # A new invocation was queued while this one was running; run it
            # rather than going idle, so a second runner is never needed.
            m["active_runner"] = True
            holder["prompt"] = m["pending_prompt"]
            return
        if not (m.get("steer_queue") or []):
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


def _queue_steer(meta: Meta, prompt: str, now: float) -> None:
    """Append a steer instruction to the agent's FIFO queue.

    Args:
        meta: Agent metadata.
        prompt: Steer instruction.
        now: Queue timestamp.
    """
    queue = meta.get("steer_queue") or []
    seq = (meta.get("steer_seq") or 0) + 1
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
    queue = meta.get("steer_queue") or []
    if not queue:
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
    meta["prompt_count"] = int(meta.get("prompt_count") or 0) + 1
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
    """
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
    """Recover a stale reserved runner, preserving the accepted pending prompt.

    Returns ``True`` only when ``m`` holds a reserved runner whose spawner or
    runner died before claiming its exact identity (nothing genuinely in
    flight). The caller re-owns the reservation under a fresh generation so the
    stale old generation is invalidated, and starts exactly one replacement
    runner.

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
    res = m.get("runner_reservation")
    if not (isinstance(res, dict) and res.get("state") == "reserved"):
        return False
    if reservation_in_flight(m):
        return False
    now = time.time()
    caller_pid = os.getpid()
    gen = int(m.get("runner_gen") or 0) + 1
    take_mode = res.get("mode") or "new"
    accepted = bool(m.get("pending_prompt"))
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
        m["pending_prompt"] = prompt
        m["last_prompt"] = _truncate(prompt, 500)
        m["prompt_count"] = int(m.get("prompt_count") or 0) + 1
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
    recorded = m.get("native_session_id")
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
    mode = _resolve_session_mode(m)
    if mode is None:
        decision["action"] = "error_session_gone"
        return
    _apply_locked_transition(m, decision, prompt=prompt, steer=steer, mode=mode)


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
    now = time.time()
    caller_pid = os.getpid()
    live_agent = is_alive(m)
    live_runner = runner_alive(m)
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
        elif m.get("pending_prompt"):
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
    # identity) is recovered here: the accepted pending prompt is preserved and
    # exactly one replacement runner is started under a fresh generation.
    if _recover_stale_reservation(m, decision, prompt=prompt, steer=steer):
        return

    # Nothing is genuinely in flight and no stale reservation to recover: own
    # this transition (fresh start) and reserve exactly one runner. A steer
    # that reaches here (idle, finished, or a stale reservation) is exactly
    # equivalent to an ordinary prompt, so it sets the pending prompt and
    # becomes the single reserved invocation.
    gen = int(m.get("runner_gen") or 0) + 1
    _begin_invocation(m, prompt, now)
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


def _dispatch_invocation(args: argparse.Namespace, prompt: str) -> int:
    """Submit a prompt or steer as one atomic, linearizable protocol step.

    Under the per-agent metadata lock the caller decides, in order:

    * an invocation is genuinely executing (live agent process) — an ordinary
      prompt is rejected (busy); a steer is queued and interrupts it;
    * only a proven-live runner exists (between invocations) — the prompt is
      queued for that runner and no second runner is spawned;
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
        spawn_runner(aid, str(decision["mode"]), gen=cast("int", decision["gen"]))
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


def _begin_invocation(meta: Meta, prompt: str, now: float) -> None:
    """Mark an agent as starting a new invocation.

    ``active_runner`` is deliberately left untouched: whether a live runner
    will pick the prompt up (or a replacement must be spawned) is decided by
    the caller after checking the runner's exact liveness.

    Args:
        meta: Agent metadata.
        prompt: Instruction to run.
        now: Invocation timestamp.
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
    meta["prompt_count"] = int(meta.get("prompt_count") or 0) + 1


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
    entries.sort(key=lambda entry: entry[2].get("created_at") or 0, reverse=True)
    if args.limit and args.limit > 0:
        entries = entries[: args.limit]
    return entries


def _entry_json(aid: str, state: str, meta: Meta) -> Meta:
    """Build the JSON mapping for one agent list entry.

    Args:
        aid: Agent ID.
        state: Agent state.
        meta: Agent metadata.

    Returns:
        The JSON-safe mapping.
    """
    return {
        "id": aid,
        "state": state,
        "prompts": meta.get("prompt_count"),
        "cwd": meta.get("cwd"),
        "title": meta.get("title"),
        "created_at": meta.get("created_at"),
        "last_activity_at": meta.get("last_activity_at"),
        "finished_at": meta.get("finished_at"),
    }


def _print_agent_table(entries: list[tuple[str, str, Meta]]) -> None:
    """Print the agent list table.

    Args:
        entries: The (aid, state, meta) entries to print.
    """
    rows = []
    for aid, state, meta in entries:
        cwd = _truncate(meta.get("cwd") or "", 24)
        title = _truncate((meta.get("title") or "").replace("\n", " "), 40)
        rows.append((
            aid,
            state,
            str(meta.get("prompt_count") or 0),
            fmt_age(meta.get("created_at")),
            cwd,
            title,
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
    steers = meta.get("steer_queue") or []
    if steers:
        _out(f"steers:     {len(steers)} queued: {_first_line(steers[0].get('prompt') or '')}")
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
        "steers_pending": len(meta.get("steer_queue") or []),
        "next_steer": (
            _first_line((meta.get("steer_queue") or [{}])[0].get("prompt") or "")
            if meta.get("steer_queue")
            else None
        ),
        "model": DEFAULT_MODEL,
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

    Args:
        aid: Lubko agent ID.

    Returns:
        ``True`` when streaming should stop.
    """
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
        break

    return exit_code_for(meta)


def cmd_stop(args: argparse.Namespace) -> int:
    """Gracefully stop a running agent.

    Sends ``SIGTERM`` to the exact recorded process group, then — while any
    member of that exact group remains — ``SIGKILL`` after the grace period,
    mirroring the worker's cancellation contract so an agent run can never
    leave abandoned OpenCode children behind.

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
    if not is_alive(meta) and not group_alive(meta):
        _out(f"{PROG}: agent {aid} is already stopped (state {derive_state(meta)})")
        return EXIT_OK
    update_meta(aid, lambda m: _begin_stop_like(m, "stop"))
    send_signal_group(meta, signal.SIGTERM)
    if wait_group_dead(meta, STOP_WAIT_SECONDS):
        update_meta(
            aid,
            lambda m: _mark_terminal(m, -signal.SIGTERM, signal.SIGTERM, "stopped", "stop"),
        )
        _out(f"stopped agent {aid}")
        return EXIT_OK
    # The exact group still has live members; escalate so no child is
    # abandoned, exactly like the worker's cancel grace period.
    if group_alive(meta):
        send_signal_group(meta, signal.SIGKILL)
    if wait_group_dead(meta, KILL_WAIT_SECONDS):
        update_meta(
            aid,
            lambda m: _mark_terminal(m, -signal.SIGKILL, signal.SIGKILL, "stopped", "stop"),
        )
        _out(f"stopped agent {aid} (force-killed group members)")
        return EXIT_OK
    update_meta(aid, lambda m: m.update(intent=None))
    _err(f"{PROG}: agent {aid} did not stop within {STOP_WAIT_SECONDS:.0f}s; use 'kill'")
    return EXIT_ERROR


def cmd_kill(args: argparse.Namespace) -> int:
    """Forcefully terminate a running agent.

    The exact recorded process group is signalled and confirmed gone, so
    ``SIGKILL`` to the leader alone can never leave group members behind.

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
    if not is_alive(meta) and not group_alive(meta):
        _out(f"{PROG}: agent {aid} is already dead (state {derive_state(meta)})")
        return EXIT_OK
    update_meta(aid, lambda m: _begin_stop_like(m, "kill"))
    send_signal_group(meta, signal.SIGKILL)
    if wait_group_dead(meta, KILL_WAIT_SECONDS):
        update_meta(
            aid,
            lambda m: _mark_terminal(m, -signal.SIGKILL, signal.SIGKILL, "killed", "kill"),
        )
        _out(f"killed agent {aid}")
        return EXIT_OK
    update_meta(aid, lambda m: m.update(intent=None))
    _err(f"{PROG}: agent {aid} could not be killed")
    return EXIT_ERROR


def cmd_delete(args: argparse.Namespace) -> int:
    """Delete an agent's local state and logs.

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
    if is_alive(meta) or group_alive(meta):
        if not args.force:
            _err(f"{PROG}: agent {aid} is running; stop it first or use --force")
            return EXIT_ERROR
        update_meta(aid, lambda m: m.update(intent="kill", last_activity_at=time.time()))
        send_signal_group(meta, signal.SIGKILL)
        wait_group_dead(meta, KILL_WAIT_SECONDS)
    shutil.rmtree(agent_dir(aid), ignore_errors=True)
    _out(f"deleted agent {aid}")
    return EXIT_OK


def cmd_clean(args: argparse.Namespace) -> int:
    """Garbage-collect old finished agents.

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
    for aid in candidates:
        if args.dry_run:
            _out(f"would remove agent {aid}")
        else:
            shutil.rmtree(agent_dir(aid), ignore_errors=True)
            _out(f"removed agent {aid}")

    if not candidates:
        _out("(nothing to clean)")
    else:
        _out(f"({len(candidates)} agent(s))")
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
        finished = meta.get("finished_at")
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
