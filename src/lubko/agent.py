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
import contextlib
import fcntl
import json
import os
import re
import secrets
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

if TYPE_CHECKING:
    from collections.abc import Callable

# Agent metadata: a JSON-serializable mapping with heterogeneous values.
Meta = dict[str, Any]

# Implementation details (hidden from the user-facing interface).
DEFAULT_MODEL: Final = "opencode-go/deepseek-v4-flash"
DEFAULT_VARIANT: Final = "high"
OPENCODE_TITLE_PREFIX: Final = "lubko-"  # native session title prefix used for discovery
TERMINAL_STATES: Final = ("succeeded", "failed", "stopped", "killed")
STOP_REASONS: Final = frozenset({"stop", "kill"})
PROG: Final = "lubko-agent"

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
SESSION_CONTINUE_TIMEOUT_SECONDS: Final = 10
SESSION_CONTINUE_POLL_SECONDS: Final = 0.5
STOP_WAIT_SECONDS: Final = 10.0
KILL_WAIT_SECONDS: Final = 5.0
IDLE_BREAK_SECONDS: Final = 5
STATUS_TAIL_LINES: Final = 50
STATUS_TAIL_CHARS: Final = 2000
RESULT_TAIL_LINES: Final = 50
RESULT_TAIL_CHARS: Final = 2000
DEFAULT_RETENTION_DAYS: Final = 14
RUNNER_ARGV_LENGTH: Final = 3

_ANSI_CSI_RE = re.compile(r"\x1b\[[0-9;?]*[ -/]*[@-~]")

_LAST_ASSISTANT_SQL: Final = """
SELECT p.data
FROM part p
JOIN message m ON p.message_id = m.id
WHERE p.session_id = ?
  AND json_extract(m.data, '$.role') = 'assistant'
  AND json_extract(p.data, '$.type') = 'text'
ORDER BY p.time_created DESC, p.id DESC
LIMIT 1
"""


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


def last_file() -> Path:
    """Return the path of the most-recently-used agent marker.

    Returns:
        The ``last.txt`` path.
    """
    return state_root() / "last.txt"


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


def new_agent_id() -> str:
    """Return a fresh, collision-free Lubko agent ID.

    Returns:
        A new 8-hex-character agent ID.
    """
    existing: set[str] = set()
    with contextlib.suppress(OSError):
        existing = {entry.name for entry in agents_dir().iterdir() if entry.is_dir()}
    while True:
        aid = secrets.token_hex(4)
        if aid not in existing:
            return aid


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


def base_meta(aid: str, cwd: str, prompt: str, title: str | None, *, is_continue: bool) -> Meta:
    """Build the initial metadata mapping for a new agent.

    Args:
        aid: New Lubko agent ID.
        cwd: Working directory for the agent.
        prompt: Initial instruction.
        title: Optional display title.
        is_continue: Whether this is a continuation invocation.

    Returns:
        The initial metadata mapping.
    """
    now = time.time()
    meta: Meta = {
        "id": aid,
        "created_at": now,
        "last_activity_at": now,
        "state": "running",
        "cwd": cwd,
        "title": title or _first_line(prompt),
        "model": os.environ.get("LUBKO_MODEL", DEFAULT_MODEL),
        "variant": os.environ.get("LUBKO_VARIANT", DEFAULT_VARIANT),
        "native_session_id": None,
        "pid": None,
        "pgid": None,
        "start_time": None,
        "runner_pid": None,
        "started_at": now,
        "finished_at": None,
        "exit_code": None,
        "exit_signal": None,
        "intent": None,
        "stop_reason": None,
        "active_runner": True,
        "steer_queue": [],
        "steer_seq": 0,
        "prompt_count": 0,
        "agent_version": 2,
    }
    if is_continue:
        meta["pending_prompt"] = prompt
    else:
        meta["initial_prompt"] = prompt
    return meta


def mark_last(aid: str) -> None:
    """Record ``aid`` as the most recently used agent.

    Args:
        aid: Lubko agent ID to record.
    """
    root = state_root()
    root.mkdir(parents=True, exist_ok=True)
    tmp = root / "last.txt.tmp"
    path = last_file()
    try:
        tmp.write_text(aid + "\n", encoding="utf-8")
        tmp.replace(path)
    except OSError:
        pass


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


def _env_has_marker(pid: int, aid: str) -> bool:
    marker = f"LUBKO_AGENT_ID={aid}".encode()
    try:
        environ = Path(f"/proc/{pid}/environ").read_bytes()
    except OSError:
        return False
    return marker in environ


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
    if not _env_has_marker(pid, meta.get("id", "")):
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


def wait_dead(meta: Meta, timeout: float) -> bool:
    """Wait until the recorded agent process is gone.

    Args:
        meta: Agent metadata.
        timeout: Maximum seconds to wait.

    Returns:
        ``True`` when the process is gone within the timeout.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        if not is_alive(meta):
            return True
        time.sleep(0.2)
    return not is_alive(meta)


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


def log_excerpt(
    path: Path,
    max_lines: int = STATUS_TAIL_LINES,
    max_chars: int = STATUS_TAIL_CHARS,
) -> list[str]:
    """Return a short plain-text tail of a log file for display.

    Shows at most ``max_lines`` lines within the newest ``max_chars`` bytes,
    with ANSI escape sequences stripped.

    Args:
        path: Log file path.
        max_lines: Maximum number of lines.
        max_chars: Maximum bytes to consider.

    Returns:
        The plain-text tail lines.
    """
    return [_ANSI_CSI_RE.sub("", line) for line in tail_lines(path, max_lines, max_chars)]


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


def last_assistant_text(session_id: str) -> str | None:
    """Extract the final assistant text message from the session transcript.

    Args:
        session_id: Underlying session ID.

    Returns:
        The final assistant text, or ``None`` when unavailable.
    """
    db = opencode_db_path()
    if not db:
        return None
    try:
        conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    except sqlite3.Error:
        return None
    try:
        row = conn.execute(_LAST_ASSISTANT_SQL, (session_id,)).fetchone()
    except sqlite3.Error:
        return None
    finally:
        conn.close()
    if not row:
        return None
    try:
        text = json.loads(row[0]).get("text")
    except (ValueError, TypeError):
        return None
    return text if isinstance(text, str) else None


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
    model = meta.get("model") or DEFAULT_MODEL
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


def runner(aid: str, mode: str) -> None:
    """Detached monitor: run the current invocation, then any queued steers.

    Runs one invocation, records its result, then — if steer instructions
    are queued — immediately runs them in FIFO order until the queue drains.

    Args:
        aid: Lubko agent ID.
        mode: Invocation mode (``new`` or ``continue``).
    """
    meta = read_meta(aid)
    if meta is None:
        return
    directory = agent_dir(aid)
    directory.mkdir(parents=True, exist_ok=True)
    log_path = directory / "output.log"
    cwd = meta.get("cwd") or str(Path.cwd())
    env = dict(os.environ)
    env["LUBKO_AGENT_ID"] = aid
    ctx = _RunnerContext(aid=aid, log_path=log_path, cwd=cwd, env=env)
    is_continue = mode == "continue"
    first = True

    while True:
        meta = read_meta(aid)
        if meta is None:
            return
        if first:
            prompt = meta.get("pending_prompt") if is_continue else meta.get("initial_prompt")
            first = False
        else:
            prompt = meta.get("pending_prompt")
            is_continue = True
        if not prompt:
            update_meta(aid, lambda m: m.update(active_runner=False))
            return
        next_prompt = _run_invocation(ctx, prompt, is_continue=is_continue)
        if next_prompt is None:
            return


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
        update_meta(aid, lambda m: m.update(active_runner=False))
        return None
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
            update_meta(aid, lambda m: m.update(active_runner=False))
            return None

        start = proc_start_ticks(proc.pid)
        update_meta(aid, _record_running(proc, start))

        if not is_continue and not os.environ.get("LUBKO_AGENT_CMD"):
            deadline = time.time() + SESSION_DISCOVER_TIMEOUT_SECONDS
            while time.time() < deadline and proc.poll() is None:
                sid = discover_session_id(aid)
                if sid:
                    update_meta(aid, _set_native_session(sid))
                    break
                time.sleep(SESSION_DISCOVER_POLL_SECONDS)

        rc = proc.wait()
        update_meta(aid, _finalize_after(rc))

    return _drain_next(aid)


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
            m["active_runner"] = False
            return
        if not (m.get("steer_queue") or []):
            m["active_runner"] = False
            return
        item = _pop_into_pending(m, time.time())
        m["active_runner"] = True
        holder["prompt"] = item.get("prompt") if item else None

    update_meta(aid, drain)
    return holder["prompt"]


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
    meta["active_runner"] = False


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


def spawn_runner(aid: str, mode: str) -> None:
    """Detach a background runner monitor for an agent.

    Args:
        aid: Lubko agent ID.
        mode: Invocation mode (``new`` or ``continue``).
    """
    script = Path(__file__).resolve()
    subprocess.Popen(
        [sys.executable, str(script), "_runner", aid, mode],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        close_fds=True,
    )


def cmd_new(args: argparse.Namespace) -> int:
    """Create a new agent session.

    Args:
        args: Parsed command arguments.

    Returns:
        A process exit code.
    """
    prompt = args.prompt or args.prompt_text
    if not prompt:
        _err(f"{PROG}: new: a prompt is required")
        return EXIT_USAGE
    cwd = str(Path(args.cwd or Path.cwd()).resolve())
    if not Path(cwd).is_dir():
        _err(f"{PROG}: new: working directory does not exist: {cwd}")
        return EXIT_ERROR

    aid = new_agent_id()
    meta = base_meta(aid, cwd, prompt, args.title, is_continue=False)
    meta["prompt_count"] = 1
    agent_dir(aid).mkdir(parents=True, exist_ok=True)
    write_meta(aid, meta)
    mark_last(aid)
    spawn_runner(aid, "new")

    if args.json:
        _out(
            json.dumps({
                "id": aid,
                "state": "running",
                "cwd": cwd,
                "created_at": meta["created_at"],
            })
        )
    else:
        _out(f"Created agent with id {aid}. Check progress with `{PROG} status {aid}`.")
    sys.stdout.flush()

    if args.sync:
        stream_log_until_terminal(aid)
        return exit_code_for(read_meta(aid))
    return EXIT_OK


def cmd_prompt(args: argparse.Namespace) -> int:
    """Send another instruction to an agent.

    Args:
        args: Parsed command arguments.

    Returns:
        A process exit code.
    """
    aid = args.agent_id
    prompt = args.prompt or args.prompt_text
    if not prompt:
        _err(f"{PROG}: prompt: a prompt is required")
        return EXIT_USAGE
    meta = read_meta(aid)
    if meta is None:
        _err(f"{PROG}: unknown agent: {aid}")
        return EXIT_NOT_FOUND
    state = derive_state(meta)
    if state == "running":
        if args.steer:
            return _steer_busy(args, prompt)
        _err(f"{PROG}: agent {aid} is still running; use --steer to redirect it")
        return EXIT_ERROR
    return _start_continuation(args, meta, prompt)


def _start_continuation(args: argparse.Namespace, meta: Meta, prompt: str) -> int:
    """Start a continuation invocation of an agent.

    Args:
        args: Parsed command arguments.
        meta: Agent metadata.
        prompt: Continuation instruction.

    Returns:
        A process exit code.
    """
    aid = args.agent_id

    # The underlying session must be available to continue.
    session_id = meta.get("native_session_id") or discover_session_id(aid)
    if not session_id:
        session_id = _wait_for_session(aid)
    if not session_id:
        _err(f"{PROG}: cannot continue agent {aid}: its underlying session is not available")
        return EXIT_ERROR

    now = time.time()
    update_meta(aid, lambda m: _begin_invocation(m, prompt, now))
    mark_last(aid)
    spawn_runner(aid, "continue")

    if args.json:
        _out(json.dumps({"id": aid, "state": "running"}))
    else:
        _out(f"Started agent with id {aid}. Check progress with `{PROG} status {aid}`.")
    sys.stdout.flush()

    if args.sync:
        stream_log_until_terminal(aid)
        return exit_code_for(read_meta(aid))
    return EXIT_OK


def _wait_for_session(aid: str) -> str | None:
    """Poll briefly for the underlying session of an agent.

    Args:
        aid: Lubko agent ID.

    Returns:
        The underlying session ID, or ``None`` when unavailable.
    """
    deadline = time.time() + SESSION_CONTINUE_TIMEOUT_SECONDS
    while time.time() < deadline:
        session_id = discover_session_id(aid)
        if session_id:
            return session_id
        time.sleep(SESSION_CONTINUE_POLL_SECONDS)
    return discover_session_id(aid)


def _steer_busy(args: argparse.Namespace, prompt: str) -> int:
    """Redirect a busy agent: queue the instruction and interrupt the run.

    Fire-and-forget: returns immediately.  The running runner picks up the
    queued instruction as soon as the interrupted invocation has exited.

    Args:
        args: Parsed command arguments.
        prompt: Steer instruction.

    Returns:
        A process exit code.
    """
    aid = args.agent_id
    now = time.time()
    spawn_needed = {"yes": False}

    def apply(m: Meta) -> None:
        _queue_steer(m, prompt, now)
        alive = is_alive(m)
        had_runner = bool(m.get("active_runner"))
        m["active_runner"] = True
        if alive:
            m["intent"] = "steer"
        elif not had_runner:
            _pop_into_pending(m, now)
            spawn_needed["yes"] = True

    update_meta(aid, apply)
    current = read_meta(aid)
    if current is not None and current.get("intent") == "steer" and is_alive(current):
        send_signal_group(current, signal.SIGTERM)
    if spawn_needed["yes"]:
        spawn_runner(aid, "continue")
    mark_last(aid)

    if args.json:
        _out(json.dumps({"id": aid, "state": "running", "steer": True}))
    else:
        _out(aid)
        sys.stdout.flush()
        detail = "starting now" if spawn_needed["yes"] else "interrupting current run"
        _err(f"{PROG}: steer queued; {detail}")
    return EXIT_OK


def _begin_invocation(meta: Meta, prompt: str, now: float) -> None:
    """Mark an agent as starting a new invocation.

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
    meta["active_runner"] = True
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
    aid = args.agent_id
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
    excerpt = log_excerpt(log_path, STATUS_TAIL_LINES, STATUS_TAIL_CHARS)
    if excerpt:
        print_box(excerpt)
    return EXIT_OK


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
        "model": meta.get("model"),
        "variant": meta.get("variant"),
        "log": str(agent_dir(aid) / "output.log"),
    }


def _read_tail_window(path: Path, max_chars: int) -> tuple[bytes, int, bytes] | None:
    """Read the newest bytes of a file plus the byte before the window.

    Args:
        path: File path.
        max_chars: Maximum bytes to read (``<= 0`` for the whole file).

    Returns:
        A ``(data, start, prev)`` tuple, or ``None`` when the file is empty.
    """
    with path.open("rb") as fh:
        fh.seek(0, os.SEEK_END)
        size = fh.tell()
        if size == 0:
            return None
        window = size if max_chars <= 0 else min(size, max_chars)
        start = size - window
        fh.seek(start)
        data = fh.read(window)
        prev = b"\n"
        if start > 0:
            fh.seek(start - 1)
            prev = fh.read(1)
        return data, start, prev


def tail_lines(path: Path, n: int, max_chars: int = 0) -> list[str]:
    """Return the last ``n`` lines of a file (all lines when ``n <= 0``).

    ``max_chars > 0`` limits the tail to the newest ``max_chars`` bytes; a
    mid-line fragment at the start is dropped when fuller lines follow.

    Args:
        path: File path.
        n: Number of trailing lines.
        max_chars: Maximum bytes to consider.

    Returns:
        The trailing lines.
    """
    try:
        window = _read_tail_window(path, max_chars)
    except OSError:
        return []
    if window is None:
        return []
    data, start, prev = window
    lines = data.decode("utf-8", errors="replace").split("\n")
    if start > 0 and prev != b"\n" and any(lines[1:]):
        lines = lines[1:]
    while lines and not lines[-1]:
        lines.pop()
    if n > 0:
        lines = lines[-n:]
    return lines


def tail_snapshot(path: Path, max_lines: int = 50, max_chars: int = 2000) -> tuple[bytes, int]:
    """Return (newest output bytes, byte offset to continue following from).

    The bytes cover at most ``max_lines`` lines within the newest
    ``max_chars`` bytes.  The offset is the end of that snapshot, so following
    from it neither reprints the shown output nor skips newly appended output.

    Args:
        path: File path.
        max_lines: Maximum number of lines.
        max_chars: Maximum bytes to consider.

    Returns:
        A ``(bytes, offset)`` tuple.
    """
    try:
        window = _read_tail_window(path, max_chars)
    except OSError:
        return b"", 0
    if window is None:
        return b"", 0
    data, start, _prev = window
    size = start + len(data)
    lines = data.split(b"\n")
    if max_lines > 0 and len(lines) > max_lines:
        keep_lines = lines[-(max_lines + 1) :]
        keep = b"\n".join(keep_lines)
        if keep.startswith(b"\n"):
            keep = keep[1:]
    else:
        keep = data
    return keep, size


def stream_log_until_terminal(
    aid: str,
    follow_lines: int = STATUS_TAIL_LINES,
    max_chars: int = STATUS_TAIL_CHARS,
) -> None:
    """Print the recent tail, then stream new output until the agent is done.

    Args:
        aid: Lubko agent ID.
        follow_lines: Number of recent lines to show first.
        max_chars: Maximum bytes of the initial snapshot.
    """
    log_path = agent_dir(aid) / "output.log"
    offset = _print_snapshot(log_path, follow_lines, max_chars)
    handle: BinaryIO | None = None
    idle_since: float | None = None

    while True:
        if handle is None:
            handle = _open_log(log_path, offset)
            if handle is None:
                if _terminal_or_unknown(aid):
                    return
                time.sleep(0.3)
                continue
        if _consume_log(handle):
            idle_since = None
        if _terminal_or_unknown(aid):
            _drain_and_stop(handle)
            return
        if _stale_running(aid):
            if idle_since is None:
                idle_since = time.time()
            elif time.time() - idle_since > IDLE_BREAK_SECONDS:
                _drain_and_stop(handle)
                return
        time.sleep(0.4)


def _print_snapshot(path: Path, follow_lines: int, max_chars: int) -> int:
    """Print the recent tail snapshot and return its end offset.

    Args:
        path: Log file path.
        follow_lines: Number of recent lines to show.
        max_chars: Maximum bytes to consider.

    Returns:
        The byte offset to continue following from.
    """
    if not path.is_file():
        return 0
    kept, offset = tail_snapshot(path, follow_lines, max_chars)
    if kept:
        sys.stdout.buffer.write(kept)
        sys.stdout.buffer.flush()
    return offset


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


def _consume_log(handle: BinaryIO) -> bool:
    """Write any newly available log bytes to stdout.

    Args:
        handle: Open log stream.

    Returns:
        ``True`` when new bytes were written.
    """
    data = handle.read()
    if not data:
        return False
    sys.stdout.buffer.write(data)
    sys.stdout.buffer.flush()
    return True


def _drain_and_stop(handle: BinaryIO) -> None:
    """Write any remaining log bytes and stop streaming.

    Args:
        handle: Open log stream.
    """
    data = handle.read()
    if data:
        sys.stdout.buffer.write(data)
        sys.stdout.buffer.flush()


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
            # wait for the first output to appear
            time.sleep(0.5)
            if not log_path.is_file():
                _err("(no output yet)")
                return EXIT_OK
        else:
            _err("(no output yet)")
            return EXIT_OK
    max_chars = 0 if args.lines <= 0 else STATUS_TAIL_CHARS
    if args.follow:
        stream_log_until_terminal(aid, follow_lines=args.lines, max_chars=max_chars)
    else:
        for line in tail_lines(log_path, args.lines, max_chars):
            _out(line)
    return EXIT_OK


def cmd_result(args: argparse.Namespace) -> int:
    """Show the agent's final result.

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
    state = derive_state(meta)
    if state == "running":
        _err(f"{PROG}: agent {aid} is still running; no final result yet")
        return EXIT_NOT_FOUND
    if state == "unknown":
        _err(f"{PROG}: agent {aid} is in an unknown state; no reliable result")
        return EXIT_NOT_FOUND

    session_id = meta.get("native_session_id") or discover_session_id(aid)
    text = last_assistant_text(session_id) if session_id else None
    if text is not None:
        if args.json:
            _out(
                json.dumps({
                    "id": aid,
                    "state": state,
                    "exit_code": meta.get("exit_code"),
                    "result": text,
                })
            )
        else:
            _out(text)
        return EXIT_OK

    # Fallback: last lines of the captured output.
    log_path = agent_dir(aid) / "output.log"
    lines = tail_lines(log_path, RESULT_TAIL_LINES, RESULT_TAIL_CHARS)
    if args.json:
        _out(
            json.dumps({
                "id": aid,
                "state": state,
                "exit_code": meta.get("exit_code"),
                "result": "\n".join(lines) if lines else None,
            })
        )
    elif lines:
        _out("(no structured result; showing the tail of agent output)")
        for line in lines:
            _out(line)
    else:
        _out("(no result available)")
    return EXIT_OK


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
    if not is_alive(meta):
        _out(f"{PROG}: agent {aid} is already stopped (state {derive_state(meta)})")
        return EXIT_OK
    update_meta(aid, lambda m: _begin_stop_like(m, "stop"))
    send_signal_group(meta, signal.SIGTERM)
    if wait_dead(meta, STOP_WAIT_SECONDS):
        update_meta(
            aid,
            lambda m: _mark_terminal(m, -signal.SIGTERM, signal.SIGTERM, "stopped", "stop"),
        )
        _out(f"stopped agent {aid}")
        return EXIT_OK
    update_meta(aid, lambda m: m.update(intent=None))
    _err(f"{PROG}: agent {aid} did not stop within {STOP_WAIT_SECONDS:.0f}s; use 'kill'")
    return EXIT_ERROR


def cmd_kill(args: argparse.Namespace) -> int:
    """Forcefully terminate a running agent.

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
    if not is_alive(meta):
        _out(f"{PROG}: agent {aid} is already dead (state {derive_state(meta)})")
        return EXIT_OK
    update_meta(aid, lambda m: _begin_stop_like(m, "kill"))
    send_signal_group(meta, signal.SIGKILL)
    if wait_dead(meta, KILL_WAIT_SECONDS):
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
    if is_alive(meta):
        if not args.force:
            _err(f"{PROG}: agent {aid} is running; stop it first or use --force")
            return EXIT_ERROR
        update_meta(aid, lambda m: m.update(intent="kill", last_activity_at=time.time()))
        send_signal_group(meta, signal.SIGKILL)
        wait_dead(meta, KILL_WAIT_SECONDS)
    shutil.rmtree(agent_dir(aid), ignore_errors=True)
    _forget_last(aid)
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
            _forget_last(aid)
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


def cmd_last(_args: argparse.Namespace) -> int:
    """Print the most recently used Lubko agent ID.

    Args:
        _args: Parsed command arguments (unused).

    Returns:
        A process exit code.
    """
    try:
        aid = last_file().read_text(encoding="utf-8").strip()
    except OSError:
        aid = ""
    if aid and agent_dir(aid).is_dir():
        _out(aid)
        return EXIT_OK
    _err(f"{PROG}: no previous agent")
    return EXIT_NOT_FOUND


def _forget_last(aid: str) -> None:
    """Clear the recorded last agent when it matches ``aid``.

    Args:
        aid: Lubko agent ID.
    """
    try:
        current = last_file().read_text(encoding="utf-8").strip()
    except OSError:
        return
    if current == aid:
        with contextlib.suppress(OSError):
            last_file().unlink()


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
        help="create a new agent session",
        func=cmd_new,
        arguments=(
            _arg(
                "--cwd",
                metavar="DIR",
                default=None,
                help="working directory for the agent (default: current directory)",
            ),
            _arg("--prompt", metavar="TEXT", default=None, help="initial instruction"),
            _arg(
                "prompt_text", nargs="?", metavar="PROMPT", help="initial instruction (positional)"
            ),
            _arg("--title", metavar="TEXT", default=None, help="short display title"),
            _arg("--json", action="store_true", help="machine-readable output"),
            _arg("--sync", action="store_true", help=argparse.SUPPRESS),
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
            _arg("agent_id", metavar="ID"),
            _arg("--json", action="store_true", help="machine-readable output"),
        ),
    ),
    _SubcommandSpec(
        name="prompt",
        help="send another instruction to an agent",
        func=cmd_prompt,
        arguments=(
            _arg("agent_id", metavar="ID"),
            _arg(
                "--steer",
                action="store_true",
                help="send while busy: interrupt the current run and redirect it",
            ),
            _arg("--prompt", metavar="TEXT", default=None, help="instruction to send"),
            _arg("prompt_text", nargs="?", metavar="PROMPT", help="instruction (positional)"),
            _arg("--json", action="store_true", help="machine-readable output"),
            _arg("--sync", action="store_true", help=argparse.SUPPRESS),
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
        name="result",
        help="show the agent's final result",
        func=cmd_result,
        arguments=(
            _arg("agent_id", metavar="ID"),
            _arg("--json", action="store_true", help="machine-readable output"),
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
    _SubcommandSpec(
        name="last",
        help="print the most recently used agent ID",
        func=cmd_last,
        arguments=(),
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
