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

# Implementation details (hidden from the user-facing interface).
DEFAULT_MODEL = "opencode-go/deepseek-v4-flash"
DEFAULT_VARIANT = "high"
OPENCODE_TITLE_PREFIX = "lubko-"  # native session title prefix used for discovery
TERMINAL_STATES = ("succeeded", "failed", "stopped", "killed")
PROG = "my-lubko-agent"

# Exit codes.
EXIT_OK = 0
EXIT_ERROR = 1
EXIT_USAGE = 2
EXIT_NOT_FOUND = 3
EXIT_TIMEOUT = 124


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

def _home() -> str:
    return os.path.expanduser("~")


def state_root() -> str:
    base = os.environ.get("XDG_STATE_HOME") or os.path.join(_home(), ".local", "state")
    return os.path.join(base, "lubko")


def agents_dir() -> str:
    return os.path.join(state_root(), "agents")


def agent_dir(aid: str) -> str:
    return os.path.join(agents_dir(), aid)


def last_file() -> str:
    return os.path.join(state_root(), "last.txt")


def opencode_db_path() -> str:
    """Path to the underlying agent's local session database, if present."""
    base = os.environ.get("XDG_DATA_HOME") or os.path.join(_home(), ".local", "share")
    db = os.path.join(base, "opencode", "opencode.db")
    return db if os.path.isfile(db) else ""


# ---------------------------------------------------------------------------
# Metadata
# ---------------------------------------------------------------------------

def new_agent_id() -> str:
    existing = set()
    try:
        existing = {d for d in os.listdir(agents_dir()) if os.path.isdir(os.path.join(agents_dir(), d))}
    except OSError:
        pass
    while True:
        aid = secrets.token_hex(4)
        if aid not in existing:
            return aid


def read_meta(aid: str) -> dict | None:
    path = os.path.join(agent_dir(aid), "meta.json")
    try:
        with open(path, encoding="utf-8") as fh:
            return json.load(fh)
    except FileNotFoundError:
        return None
    except (OSError, ValueError):
        return None


def write_meta(aid: str, meta: dict) -> None:
    """Atomically replace an agent's metadata file."""
    directory = agent_dir(aid)
    os.makedirs(directory, exist_ok=True)
    tmp = os.path.join(directory, "meta.json.tmp")
    path = os.path.join(directory, "meta.json")
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(meta, fh, indent=2, sort_keys=True)
        fh.write("\n")
    os.replace(tmp, path)


def update_meta(aid: str, fn) -> None:
    """Apply ``fn(meta)`` to an agent's metadata under an exclusive lock.

    If the agent has been deleted, this is a no-op: a late background runner
    must never resurrect a deleted agent's directory.
    """
    directory = agent_dir(aid)
    if not os.path.isdir(directory):
        return
    lock_path = os.path.join(directory, ".lock")
    try:
        lock = open(lock_path, "w", encoding="utf-8")
    except OSError:
        return
    with lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            meta = read_meta(aid)
            if meta is None:
                return
            fn(meta)
            write_meta(aid, meta)
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def base_meta(aid: str, cwd: str, prompt: str, title: str | None, is_continue: bool) -> dict:
    now = time.time()
    meta: dict = {
        "id": aid,
        "created_at": now,
        "last_activity_at": now,
        "state": "running",
        "cwd": cwd,
        "title": title if title else _first_line(prompt),
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
    root = state_root()
    os.makedirs(root, exist_ok=True)
    tmp = os.path.join(root, "last.txt.tmp")
    path = last_file()
    try:
        with open(tmp, "w", encoding="utf-8") as fh:
            fh.write(aid + "\n")
        os.replace(tmp, path)
    except OSError:
        pass


# ---------------------------------------------------------------------------
# Process identity
# ---------------------------------------------------------------------------

def proc_start_ticks(pid: int) -> int | None:
    """Return the process start time in clock ticks (unique per boot)."""
    try:
        with open(f"/proc/{pid}/stat", encoding="utf-8") as fh:
            stat = fh.read()
        rest = stat[stat.rfind(")") + 1 :].split()
        return int(rest[19])
    except Exception:
        return None


def _env_has_marker(pid: int, aid: str) -> bool:
    marker = f"LUBKO_AGENT_ID={aid}".encode()
    try:
        with open(f"/proc/{pid}/environ", "rb") as fh:
            return marker in fh.read()
    except OSError:
        return False


def is_alive(meta: dict) -> bool:
    """True only if the recorded process is really our agent process.

    A PID alone is not trusted: the process start time (ticks) and a
    per-agent environment marker must both match, so a reused or recycled
    PID can never be mistaken for our agent.
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


def send_signal_group(meta: dict, sig: int) -> None:
    """Deliver a signal to the agent's process group (session leader)."""
    pid = meta.get("pid")
    if not pid:
        return
    try:
        os.killpg(pid, sig)  # the agent was launched as its own session leader
    except ProcessLookupError:
        pass


def wait_dead(meta: dict, timeout: float) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if not is_alive(meta):
            return True
        time.sleep(0.2)
    return not is_alive(meta)


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

def derive_state(meta: dict | None) -> str:
    """Live state: verify process liveness rather than trusting metadata."""
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
        if time.time() - launched < 60:
            return "running"
        return "unknown"
    if meta.get("finished_at"):
        return state  # runner finalized it
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


def fmt_time(epoch) -> str:
    if epoch is None:
        return "-"
    try:
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(epoch))
    except (ValueError, OSError):
        return "-"


def fmt_age(epoch) -> str:
    if epoch is None:
        return "-"
    seconds = max(0, int(time.time() - epoch))
    if seconds < 60:
        return f"{seconds}s"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes}m"
    hours = minutes // 60
    if hours < 24:
        return f"{hours}h{minutes % 60}m"
    return f"{hours // 24}d{hours % 24}h"


_ANSI_CSI_RE = re.compile(r"\x1b\[[0-9;?]*[ -/]*[@-~]")


def log_excerpt(path: str, max_lines: int = 50, max_chars: int = 2000) -> list[str]:
    """Return a short plain-text tail of a log file for display.

    Shows at most ``max_lines`` lines within the newest ``max_chars`` bytes,
    with ANSI escape sequences stripped.
    """
    return [_ANSI_CSI_RE.sub("", line) for line in tail_lines(path, max_lines, max_chars)]


def print_box(lines: list[str], max_width: int = 80) -> None:
    """Render an ASCII box around a list of lines, folding to ``max_width``.

    Long lines are wrapped (word-wise, hard-breaking overlong tokens) so the
    completed box is never wider than ``max_width`` characters.
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
    print(bar)
    for line in folded:
        print("| " + line.ljust(width - 1) + "|")
    print(bar)


# ---------------------------------------------------------------------------
# Underlying agent integration (internal)
# ---------------------------------------------------------------------------

def discover_session_id(aid: str) -> str | None:
    """Find the underlying session id for a Lubko agent, if discoverable."""
    db = opencode_db_path()
    if not db:
        return None
    try:
        conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
        try:
            row = conn.execute(
                "SELECT id FROM session WHERE title=? ORDER BY time_created DESC LIMIT 1",
                (OPENCODE_TITLE_PREFIX + aid,),
            ).fetchone()
        finally:
            conn.close()
        return row[0] if row else None
    except Exception:
        return None


def last_assistant_text(session_id: str) -> str | None:
    """Extract the final assistant text message from the session transcript."""
    db = opencode_db_path()
    if not db:
        return None
    try:
        conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
        try:
            row = conn.execute(
                """
                SELECT p.data
                FROM part p
                JOIN message m ON p.message_id = m.id
                WHERE p.session_id = ?
                  AND json_extract(m.data, '$.role') = 'assistant'
                  AND json_extract(p.data, '$.type') = 'text'
                ORDER BY p.time_created DESC, p.id DESC
                LIMIT 1
                """,
                (session_id,),
            ).fetchone()
        finally:
            conn.close()
        if not row:
            return None
        return json.loads(row[0]).get("text")
    except Exception:
        return None


def build_agent_command(meta: dict, prompt: str, is_continue: bool) -> list[str] | None:
    """Return the argv used to launch the underlying agent for this invocation."""
    env_cmd = os.environ.get("LUBKO_AGENT_CMD")
    if env_cmd:
        return ["/bin/sh", "-c", env_cmd]
    model = meta.get("model") or DEFAULT_MODEL
    variant = meta.get("variant") or DEFAULT_VARIANT
    cwd = meta.get("cwd") or os.getcwd()
    if is_continue:
        session_id = meta.get("native_session_id") or discover_session_id(meta.get("id", ""))
        if not session_id:
            return None
        return [
            "opencode", "run", "--auto",
            "--session", session_id,
            "--model", model,
            "--variant", variant,
            "--thinking",
            "--dir", cwd,
            prompt,
        ]
    return [
        "opencode", "run", "--auto",
        "--title", OPENCODE_TITLE_PREFIX + meta.get("id", ""),
        "--model", model,
        "--variant", variant,
        "--thinking",
        "--dir", cwd,
        prompt,
    ]


# ---------------------------------------------------------------------------
# Runner (background monitor for one agent invocation)
# ---------------------------------------------------------------------------

def runner(aid: str, mode: str) -> None:
    """Detached monitor: run the current invocation, then any queued steers.

    Runs one invocation, records its result, then — if steer instructions
    are queued — immediately runs them in FIFO order until the queue drains.
    """
    meta = read_meta(aid)
    if meta is None:
        return
    directory = agent_dir(aid)
    os.makedirs(directory, exist_ok=True)
    log_path = os.path.join(directory, "output.log")
    cwd = meta.get("cwd") or os.getcwd()
    env = dict(os.environ)
    env["LUBKO_AGENT_ID"] = aid
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

        env["LUBKO_PROMPT"] = prompt
        cmd = build_agent_command(meta, prompt, is_continue)
        if cmd is None:
            update_meta(aid, lambda m: _finalize_terminal(m, None, None, "failed",
                          "cannot continue: underlying session not available"))
            update_meta(aid, lambda m: m.update(active_runner=False))
            return

        try:
            log = open(log_path, "ab")
        except OSError:
            return  # agent directory no longer exists
        with log:
            try:
                proc = subprocess.Popen(
                    cmd,
                    stdin=subprocess.DEVNULL,
                    stdout=log,
                    stderr=log,
                    cwd=cwd,
                    start_new_session=True,
                    close_fds=True,
                    env=env,
                )
            except OSError as exc:
                log.write(f"LUBKO RUNNER: failed to start agent: {exc}\n".encode("utf-8", "replace"))
                update_meta(aid, lambda m: _finalize_terminal(m, 127, None, "failed", str(exc)))
                update_meta(aid, lambda m: m.update(active_runner=False))
                return

            start = proc_start_ticks(proc.pid)

            def _record_running(m: dict) -> None:
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

            update_meta(aid, _record_running)

            if not is_continue and not os.environ.get("LUBKO_AGENT_CMD"):
                deadline = time.time() + 60
                while time.time() < deadline and proc.poll() is None:
                    sid = discover_session_id(aid)
                    if sid:
                        update_meta(aid, lambda m: m.update(native_session_id=sid))
                        break
                    time.sleep(1)

            rc = proc.wait()

            def _finalize(m: dict) -> None:
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

            update_meta(aid, _finalize)

        next_holder: dict = {"prompt": None}

        def _drain(m: dict) -> None:
            if m.get("stop_reason") in ("stop", "kill"):
                m["active_runner"] = False
                return
            if not (m.get("steer_queue") or []):
                m["active_runner"] = False
                return
            item = _pop_into_pending(m, time.time())
            m["active_runner"] = True
            next_holder["prompt"] = item.get("prompt") if item else None

        update_meta(aid, _drain)
        if next_holder["prompt"] is None:
            return


def _finalize_terminal(meta: dict, exit_code, exit_signal, state: str, note: str | None) -> None:
    meta["state"] = state
    meta["exit_code"] = exit_code
    meta["exit_signal"] = exit_signal
    meta["finished_at"] = time.time()
    meta["last_activity_at"] = time.time()
    meta["intent"] = None
    if note:
        meta["error"] = note


def _queue_steer(meta: dict, prompt: str, now: float) -> None:
    """Append a steer instruction to the agent's FIFO queue."""
    queue = meta.get("steer_queue") or []
    seq = (meta.get("steer_seq") or 0) + 1
    queue.append({"seq": seq, "prompt": prompt, "queued_at": now})
    meta["steer_queue"] = queue
    meta["steer_seq"] = seq
    meta["last_activity_at"] = now


def _pop_into_pending(meta: dict, now: float) -> dict | None:
    """Move the head of the steer queue into a runnable pending invocation."""
    queue = meta.get("steer_queue") or []
    if not queue:
        return None
    item = queue.pop(0)
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


def _begin_stop_like(meta: dict, intent: str) -> None:
    """Prepare an agent for stop/kill: drop pending steers, mark intent."""
    meta["intent"] = intent
    meta["last_activity_at"] = time.time()
    meta["steer_queue"] = []
    meta["pending_prompt"] = None


def _mark_terminal(meta: dict, exit_code, exit_signal, state: str, stop_reason: str) -> None:
    _finalize_terminal(meta, exit_code, exit_signal, state, None)
    meta["stop_reason"] = stop_reason
    meta["active_runner"] = False


# ---------------------------------------------------------------------------
# Exit status mapping
# ---------------------------------------------------------------------------

def exit_code_for(meta: dict | None) -> int:
    state = derive_state(meta) if meta else "unknown"
    if state == "succeeded":
        return EXIT_OK
    if state == "failed":
        code = meta.get("exit_code") if meta else None
        return code if isinstance(code, int) and code > 0 else 1
    return 1  # stopped, killed, unknown, idle


# ---------------------------------------------------------------------------
# Subcommands
# ---------------------------------------------------------------------------

def spawn_runner(aid: str, mode: str) -> None:
    script = os.path.abspath(__file__)
    subprocess.Popen(
        [sys.executable, script, "_runner", aid, mode],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        close_fds=True,
    )


def cmd_new(args: argparse.Namespace) -> int:
    prompt = args.prompt or args.prompt_text
    if not prompt:
        print(f"{PROG}: new: a prompt is required", file=sys.stderr)
        return EXIT_USAGE
    cwd = os.path.abspath(args.cwd or os.getcwd())
    if not os.path.isdir(cwd):
        print(f"{PROG}: new: working directory does not exist: {cwd}", file=sys.stderr)
        return EXIT_ERROR

    aid = new_agent_id()
    meta = base_meta(aid, cwd, prompt, args.title, is_continue=False)
    meta["prompt_count"] = 1
    os.makedirs(agent_dir(aid), exist_ok=True)
    write_meta(aid, meta)
    mark_last(aid)
    spawn_runner(aid, "new")

    if args.json:
        print(json.dumps({"id": aid, "state": "running", "cwd": cwd,
                          "created_at": meta["created_at"]}))
    else:
        print(f"Created agent with id {aid}. Check progress with `{PROG} status {aid}`.")
    sys.stdout.flush()

    if args.sync:
        stream_log_until_terminal(aid)
        return exit_code_for(read_meta(aid))
    return EXIT_OK


def cmd_prompt(args: argparse.Namespace) -> int:
    aid = args.agent_id
    prompt = args.prompt or args.prompt_text
    if not prompt:
        print(f"{PROG}: prompt: a prompt is required", file=sys.stderr)
        return EXIT_USAGE
    meta = read_meta(aid)
    if meta is None:
        print(f"{PROG}: unknown agent: {aid}", file=sys.stderr)
        return EXIT_NOT_FOUND
    state = derive_state(meta)
    if state == "running":
        if args.steer:
            return _steer_busy(args, meta, prompt)
        print(f"{PROG}: agent {aid} is still running; use --steer to redirect it", file=sys.stderr)
        return EXIT_ERROR
    return _start_continuation(args, meta, prompt)


def _start_continuation(args: argparse.Namespace, meta: dict, prompt: str) -> int:
    aid = args.agent_id

    # The underlying session must be available to continue.
    if meta.get("native_session_id") or discover_session_id(aid):
        pass
    else:
        deadline = time.time() + 10
        session_id = None
        while time.time() < deadline:
            session_id = discover_session_id(aid)
            if session_id:
                break
            time.sleep(0.5)
        if not session_id:
            print(
                f"{PROG}: cannot continue agent {aid}: its underlying session is not available",
                file=sys.stderr,
            )
            return EXIT_ERROR

    now = time.time()
    update_meta(
        aid,
        lambda m: _begin_invocation(m, prompt, now),
    )
    mark_last(aid)
    spawn_runner(aid, "continue")

    if args.json:
        print(json.dumps({"id": aid, "state": "running"}))
    else:
        print(f"Started agent with id {aid}. Check progress with `{PROG} status {aid}`.")
    sys.stdout.flush()

    if args.sync:
        stream_log_until_terminal(aid)
        return exit_code_for(read_meta(aid))
    return EXIT_OK


def _steer_busy(args: argparse.Namespace, meta: dict, prompt: str) -> int:
    """Redirect a busy agent: queue the instruction and interrupt the run.

    Fire-and-forget: returns immediately.  The running runner picks up the
    queued instruction as soon as the interrupted invocation has exited.
    """
    aid = args.agent_id
    now = time.time()
    spawn_needed = {"yes": False}

    def _apply(m: dict) -> None:
        _queue_steer(m, prompt, now)
        alive = is_alive(m)
        had_runner = bool(m.get("active_runner"))
        m["active_runner"] = True
        if alive:
            m["intent"] = "steer"
        elif not had_runner:
            _pop_into_pending(m, now)
            spawn_needed["yes"] = True

    update_meta(aid, _apply)
    meta = read_meta(aid)
    if meta is not None and meta.get("intent") == "steer" and is_alive(meta):
        send_signal_group(meta, signal.SIGTERM)
    if spawn_needed["yes"]:
        spawn_runner(aid, "continue")
    mark_last(aid)

    if args.json:
        print(json.dumps({"id": aid, "state": "running", "steer": True}))
    else:
        print(aid)
        sys.stdout.flush()
        print(
            f"{PROG}: steer queued; {'starting now' if spawn_needed['yes'] else 'interrupting current run'}",
            file=sys.stderr,
        )
    return EXIT_OK


def _begin_invocation(meta: dict, prompt: str, now: float) -> None:
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
    try:
        ids = sorted(
            d for d in os.listdir(agents_dir())
            if os.path.isdir(os.path.join(agents_dir(), d))
        )
    except OSError:
        ids = []

    entries = []
    for aid in ids:
        meta = read_meta(aid)
        state = derive_state(meta)
        if args.running and state != "running":
            continue
        if args.finished and state not in TERMINAL_STATES:
            continue
        if args.succeeded and state != "succeeded":
            continue
        if args.failed and state != "failed":
            continue
        if args.stopped and state != "stopped":
            continue
        if args.killed and state != "killed":
            continue
        if meta is None:
            meta = {"id": aid}
        entries.append((aid, state, meta))

    entries.sort(key=lambda e: e[2].get("created_at") or 0, reverse=True)
    if args.limit and args.limit > 0:
        entries = entries[: args.limit]

    if args.json:
        out = []
        for aid, state, meta in entries:
            out.append(
                {
                    "id": aid,
                    "state": state,
                    "prompts": meta.get("prompt_count"),
                    "cwd": meta.get("cwd"),
                    "title": meta.get("title"),
                    "created_at": meta.get("created_at"),
                    "last_activity_at": meta.get("last_activity_at"),
                    "finished_at": meta.get("finished_at"),
                }
            )
        print(json.dumps({"agents": out}))
        return EXIT_OK

    if not entries:
        print("(no agents)")
        return EXIT_OK

    rows = []
    for aid, state, meta in entries:
        cwd = _truncate(meta.get("cwd") or "", 24)
        title = _truncate((meta.get("title") or "").replace("\n", " "), 40)
        rows.append(
            (aid, state, str(meta.get("prompt_count") or 0),
             fmt_age(meta.get("created_at")), cwd, title)
        )
    widths = [max(len(r[i]) for r in rows) for i in range(6)]
    widths[0] = max(widths[0], len("ID"))
    widths[1] = max(widths[1], len("STATE"))
    widths[2] = max(widths[2], len("P"))
    widths[3] = max(widths[3], len("AGE"))
    widths[4] = max(widths[4], len("CWD"))
    widths[5] = max(widths[5], len("TITLE"))
    header = "  ".join(
        label.ljust(widths[i]) for i, label in enumerate(("ID", "STATE", "P", "AGE", "CWD", "TITLE"))
    )
    print(header)
    for row in rows:
        print("  ".join(row[i].ljust(widths[i]) for i in range(6)))
    return EXIT_OK


def cmd_status(args: argparse.Namespace) -> int:
    aid = args.agent_id
    meta = read_meta(aid)
    if meta is None:
        print(f"{PROG}: unknown agent: {aid}", file=sys.stderr)
        return EXIT_NOT_FOUND
    state = derive_state(meta)
    alive = is_alive(meta)
    if args.json:
        print(json.dumps(_status_json(aid, meta, state, alive), indent=2))
        return EXIT_OK

    pid = meta.get("pid")
    pgid = meta.get("pgid")
    pid_text = f"{pid} (pgid {pgid}, alive)" if pid and alive else (f"{pid} (dead)" if pid else "(starting)")
    runner_pid = meta.get("runner_pid") or "-"
    sid = meta.get("native_session_id")
    if not sid:
        sid = discover_session_id(aid)
    if not sid:
        sid = "(unknown)"
    log_path = os.path.join(agent_dir(aid), "output.log")
    print(f"agent:      {aid}")
    print(f"state:      {state}")
    print(f"alive:      {'yes' if alive else 'no'}")
    print(f"cwd:        {meta.get('cwd') or '-'}")
    print(f"created:    {fmt_time(meta.get('created_at'))}")
    print(f"started:    {fmt_time(meta.get('started_at'))}")
    print(f"finished:   {fmt_time(meta.get('finished_at'))}")
    print(f"exit code:  {meta.get('exit_code') if meta.get('exit_code') is not None else '-'}")
    print(f"prompts:    {meta.get('prompt_count') or 0}")
    steers = meta.get("steer_queue") or []
    if steers:
        print(f"steers:     {len(steers)} queued: {_first_line(steers[0].get('prompt') or '')}")
    print(f"title:      {meta.get('title') or '-'}")
    print(f"tail(log):")
    excerpt = log_excerpt(log_path, 50, 2000)
    if excerpt:
        print_box(excerpt)
    return EXIT_OK


def _status_json(aid: str, meta: dict, state: str, alive: bool) -> dict:
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
        "next_steer": _first_line((meta.get("steer_queue") or [{}])[0].get("prompt") or "")
                      if meta.get("steer_queue") else None,
        "model": meta.get("model"),
        "variant": meta.get("variant"),
        "log": os.path.join(agent_dir(aid), "output.log"),
    }


def tail_lines(path: str, n: int, max_chars: int = 0) -> list[str]:
    """Return the last ``n`` lines of a file (all lines when ``n <= 0``).

    ``max_chars > 0`` limits the tail to the newest ``max_chars`` bytes; a
    mid-line fragment at the start is dropped when fuller lines follow.
    """
    try:
        fh = open(path, "rb")
    except OSError:
        return []
    with fh:
        fh.seek(0, os.SEEK_END)
        size = fh.tell()
        if size == 0:
            return []
        window = size if max_chars <= 0 else min(size, max_chars)
        start = size - window
        fh.seek(start)
        data = fh.read(window)
        prev = b"\n"
        if start > 0:
            fh.seek(start - 1)
            prev = fh.read(1)
    lines = data.decode("utf-8", errors="replace").split("\n")
    if start > 0 and prev != b"\n" and any(lines[1:]):
        lines = lines[1:]
    while lines and lines[-1] == "":
        lines.pop()
    if n > 0:
        lines = lines[-n:]
    return lines


def tail_snapshot(path: str, max_lines: int = 50, max_chars: int = 2000) -> tuple[bytes, int]:
    """Return (newest output bytes, byte offset to continue following from).

    The bytes cover at most ``max_lines`` lines within the newest
    ``max_chars`` bytes.  The offset is the end of that snapshot, so following
    from it neither reprints the shown output nor skips newly appended output.
    """
    try:
        fh = open(path, "rb")
    except OSError:
        return b"", 0
    with fh:
        fh.seek(0, os.SEEK_END)
        size = fh.tell()
        if size == 0:
            return b"", 0
        window = size if max_chars <= 0 else min(size, max_chars)
        start = size - window
        fh.seek(start)
        data = fh.read(window)
    lines = data.split(b"\n")
    if max_lines > 0 and len(lines) > max_lines:
        keep_lines = lines[-(max_lines + 1):]
        keep = b"\n".join(keep_lines)
        if keep.startswith(b"\n"):
            keep = keep[1:]
    else:
        keep = data
    return keep, size


def stream_log_until_terminal(aid: str, follow_lines: int = 50, max_chars: int = 2000) -> None:
    """Print the recent tail, then stream new output until the agent is done."""
    log_path = os.path.join(agent_dir(aid), "output.log")
    if os.path.isfile(log_path):
        kept, offset = tail_snapshot(log_path, follow_lines, max_chars)
        if kept:
            sys.stdout.buffer.write(kept)
            sys.stdout.buffer.flush()
    else:
        offset = 0

    handle = None
    idle_since = None
    while True:
        if handle is None:
            try:
                handle = open(log_path, "rb")
            except OSError:
                meta = read_meta(aid)
                if meta is None or derive_state(meta) in TERMINAL_STATES or derive_state(meta) == "unknown":
                    return
                time.sleep(0.3)
                continue
            handle.seek(offset)
        data = handle.read()
        if data:
            sys.stdout.buffer.write(data)
            sys.stdout.buffer.flush()
            idle_since = None
        meta = read_meta(aid)
        if meta is None:
            break
        state = derive_state(meta)
        if state in TERMINAL_STATES or state == "unknown":
            data = handle.read()
            if data:
                sys.stdout.buffer.write(data)
                sys.stdout.buffer.flush()
            break
        if meta.get("state") == "running" and meta.get("pid") and not is_alive(meta):
            if idle_since is None:
                idle_since = time.time()
            elif time.time() - idle_since > 5:
                data = handle.read()
                if data:
                    sys.stdout.buffer.write(data)
                    sys.stdout.buffer.flush()
                break
        time.sleep(0.4)


def cmd_log(args: argparse.Namespace) -> int:
    aid = args.agent_id
    meta = read_meta(aid)
    if meta is None:
        print(f"{PROG}: unknown agent: {aid}", file=sys.stderr)
        return EXIT_NOT_FOUND
    log_path = os.path.join(agent_dir(aid), "output.log")
    if not os.path.isfile(log_path):
        if args.follow:
            # wait for the first output to appear
            time.sleep(0.5)
            if not os.path.isfile(log_path):
                print("(no output yet)", file=sys.stderr)
                return EXIT_OK
        else:
            print("(no output yet)", file=sys.stderr)
            return EXIT_OK
    if args.follow:
        stream_log_until_terminal(aid, follow_lines=args.lines,
                                  max_chars=(0 if args.lines <= 0 else 2000))
    else:
        for line in tail_lines(log_path, args.lines, (0 if args.lines <= 0 else 2000)):
            print(line)
    return EXIT_OK


def cmd_result(args: argparse.Namespace) -> int:
    aid = args.agent_id
    meta = read_meta(aid)
    if meta is None:
        print(f"{PROG}: unknown agent: {aid}", file=sys.stderr)
        return EXIT_NOT_FOUND
    state = derive_state(meta)
    if state == "running":
        print(f"{PROG}: agent {aid} is still running; no final result yet", file=sys.stderr)
        return EXIT_NOT_FOUND
    if state == "unknown":
        print(f"{PROG}: agent {aid} is in an unknown state; no reliable result", file=sys.stderr)
        return EXIT_NOT_FOUND

    session_id = meta.get("native_session_id") or discover_session_id(aid)
    text = last_assistant_text(session_id) if session_id else None
    if text is not None:
        if args.json:
            print(json.dumps({"id": aid, "state": state, "exit_code": meta.get("exit_code"),
                              "result": text}))
        else:
            print(text)
        return EXIT_OK

    # Fallback: last lines of the captured output.
    log_path = os.path.join(agent_dir(aid), "output.log")
    lines = tail_lines(log_path, 50, 2000)
    if args.json:
        print(json.dumps({"id": aid, "state": state, "exit_code": meta.get("exit_code"),
                          "result": "\n".join(lines) if lines else None}))
    else:
        if lines:
            print("(no structured result; showing the tail of agent output)")
            for line in lines:
                print(line)
        else:
            print("(no result available)")
    return EXIT_OK


def cmd_wait(args: argparse.Namespace) -> int:
    aid = args.agent_id
    meta = read_meta(aid)
    if meta is None:
        print(f"{PROG}: unknown agent: {aid}", file=sys.stderr)
        return EXIT_NOT_FOUND
    timeout = args.timeout
    deadline = time.time() + timeout if timeout else None

    while True:
        meta = read_meta(aid)
        if meta is None:
            print(f"{PROG}: unknown agent: {aid}", file=sys.stderr)
            return EXIT_NOT_FOUND
        if is_alive(meta):
            if deadline and time.time() >= deadline:
                print(f"{PROG}: wait: agent {aid} still running after {timeout}s", file=sys.stderr)
                return EXIT_TIMEOUT
            time.sleep(1)
            continue
        # Process is gone; give the runner a moment to finalize state.
        for _ in range(20):
            m = read_meta(aid)
            if m is not None and m.get("state") != "running":
                meta = m
                break
            time.sleep(0.25)
        break

    return exit_code_for(meta)


def cmd_stop(args: argparse.Namespace) -> int:
    aid = args.agent_id
    meta = read_meta(aid)
    if meta is None:
        print(f"{PROG}: unknown agent: {aid}", file=sys.stderr)
        return EXIT_NOT_FOUND
    if not is_alive(meta):
        print(f"{PROG}: agent {aid} is already stopped (state {derive_state(meta)})")
        return EXIT_OK
    update_meta(aid, lambda m: _begin_stop_like(m, "stop"))
    send_signal_group(meta, signal.SIGTERM)
    if wait_dead(meta, 10.0):
        update_meta(aid, lambda m: _mark_terminal(m, -signal.SIGTERM, signal.SIGTERM, "stopped", "stop"))
        print(f"stopped agent {aid}")
        return EXIT_OK
    update_meta(aid, lambda m: m.update(intent=None))
    print(f"{PROG}: agent {aid} did not stop within 10s; use 'kill'", file=sys.stderr)
    return EXIT_ERROR


def cmd_kill(args: argparse.Namespace) -> int:
    aid = args.agent_id
    meta = read_meta(aid)
    if meta is None:
        print(f"{PROG}: unknown agent: {aid}", file=sys.stderr)
        return EXIT_NOT_FOUND
    if not is_alive(meta):
        print(f"{PROG}: agent {aid} is already dead (state {derive_state(meta)})")
        return EXIT_OK
    update_meta(aid, lambda m: _begin_stop_like(m, "kill"))
    send_signal_group(meta, signal.SIGKILL)
    if wait_dead(meta, 5.0):
        update_meta(aid, lambda m: _mark_terminal(m, -signal.SIGKILL, signal.SIGKILL, "killed", "kill"))
        print(f"killed agent {aid}")
        return EXIT_OK
    update_meta(aid, lambda m: m.update(intent=None))
    print(f"{PROG}: agent {aid} could not be killed", file=sys.stderr)
    return EXIT_ERROR


def cmd_delete(args: argparse.Namespace) -> int:
    aid = args.agent_id
    meta = read_meta(aid)
    if meta is None:
        print(f"{PROG}: unknown agent: {aid}", file=sys.stderr)
        return EXIT_NOT_FOUND
    if is_alive(meta):
        if not args.force:
            print(f"{PROG}: agent {aid} is running; stop it first or use --force", file=sys.stderr)
            return EXIT_ERROR
        update_meta(aid, lambda m: m.update(intent="kill", last_activity_at=time.time()))
        send_signal_group(meta, signal.SIGKILL)
        wait_dead(meta, 5.0)
    shutil.rmtree(agent_dir(aid), ignore_errors=True)
    _forget_last(aid)
    print(f"deleted agent {aid}")
    return EXIT_OK


def cmd_clean(args: argparse.Namespace) -> int:
    days = args.days
    if days is None:
        try:
            days = int(os.environ.get("LUBKO_AGENT_RETENTION_DAYS", "14"))
        except ValueError:
            days = 14
    if days < 0:
        print(f"{PROG}: clean: invalid retention days: {days}", file=sys.stderr)
        return EXIT_USAGE
    cutoff = time.time() - days * 86400
    try:
        ids = sorted(
            d for d in os.listdir(agents_dir())
            if os.path.isdir(os.path.join(agents_dir(), d))
        )
    except OSError:
        ids = []

    candidates = []
    for aid in ids:
        meta = read_meta(aid)
        if meta is None:
            continue
        if derive_state(meta) in TERMINAL_STATES:
            finished = meta.get("finished_at")
            if finished is not None and finished < cutoff:
                candidates.append(aid)

    for aid in candidates:
        if args.dry_run:
            print(f"would remove agent {aid}")
        else:
            shutil.rmtree(agent_dir(aid), ignore_errors=True)
            _forget_last(aid)
            print(f"removed agent {aid}")

    if not candidates:
        print("(nothing to clean)")
    else:
        print(f"({len(candidates)} agent(s))")
    return EXIT_OK


def cmd_last(args: argparse.Namespace) -> int:
    try:
        with open(last_file(), encoding="utf-8") as fh:
            aid = fh.read().strip()
    except OSError:
        aid = ""
    if aid and os.path.isdir(agent_dir(aid)):
        print(aid)
        return EXIT_OK
    print(f"{PROG}: no previous agent", file=sys.stderr)
    return EXIT_NOT_FOUND


def _forget_last(aid: str) -> None:
    try:
        with open(last_file(), encoding="utf-8") as fh:
            current = fh.read().strip()
        if current == aid:
            os.remove(last_file())
    except OSError:
        pass


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=PROG,
        description=(
            "Manage long-running Lubko agent sessions.  The orchestrator uses "
            "Lubko agent IDs only; the underlying agent implementation is hidden."
        ),
    )
    sub = parser.add_subparsers(dest="command", metavar="COMMAND")

    p_new = sub.add_parser("new", help="create a new agent session")
    p_new.add_argument("--cwd", metavar="DIR", default=None,
                       help="working directory for the agent (default: current directory)")
    p_new.add_argument("--prompt", metavar="TEXT", default=None, help="initial instruction")
    p_new.add_argument("prompt_text", nargs="?", metavar="PROMPT", help="initial instruction (positional)")
    p_new.add_argument("--title", metavar="TEXT", default=None, help="short display title")
    p_new.add_argument("--json", action="store_true", help="machine-readable output")
    p_new.add_argument("--sync", action="store_true", help=argparse.SUPPRESS)
    p_new.set_defaults(func=cmd_new)

    p_list = sub.add_parser("list", help="list Lubko-managed agents")
    p_list.add_argument("--running", action="store_true", help="only running agents")
    p_list.add_argument("--finished", action="store_true", help="only finished agents")
    p_list.add_argument("--succeeded", action="store_true", help="only succeeded agents")
    p_list.add_argument("--failed", action="store_true", help="only failed agents")
    p_list.add_argument("--stopped", action="store_true", help="only stopped agents")
    p_list.add_argument("--killed", action="store_true", help="only killed agents")
    p_list.add_argument("--limit", type=int, default=None, metavar="N",
                        help="maximum number of agents to show")
    p_list.add_argument("--json", action="store_true", help="machine-readable output")
    p_list.set_defaults(func=cmd_list)

    p_status = sub.add_parser("status", help="show detailed status of one agent")
    p_status.add_argument("agent_id", metavar="ID")
    p_status.add_argument("--json", action="store_true", help="machine-readable output")
    p_status.set_defaults(func=cmd_status)

    p_prompt = sub.add_parser("prompt", help="send another instruction to an agent")
    p_prompt.add_argument("agent_id", metavar="ID")
    p_prompt.add_argument("--steer", action="store_true",
                          help="send while busy: interrupt the current run and redirect it")
    p_prompt.add_argument("--prompt", metavar="TEXT", default=None, help="instruction to send")
    p_prompt.add_argument("prompt_text", nargs="?", metavar="PROMPT", help="instruction (positional)")
    p_prompt.add_argument("--json", action="store_true", help="machine-readable output")
    p_prompt.add_argument("--sync", action="store_true", help=argparse.SUPPRESS)
    p_prompt.set_defaults(func=cmd_prompt)

    p_log = sub.add_parser("log", help="show agent output")
    p_log.add_argument("agent_id", metavar="ID")
    p_log.add_argument("--lines", type=int, default=50, metavar="N",
                       help="number of recent lines (0 for all, default 50)")
    p_log.add_argument("--follow", action="store_true", help="stream new output until the agent exits")
    p_log.set_defaults(func=cmd_log)

    p_result = sub.add_parser("result", help="show the agent's final result")
    p_result.add_argument("agent_id", metavar="ID")
    p_result.add_argument("--json", action="store_true", help="machine-readable output")
    p_result.set_defaults(func=cmd_result)

    p_wait = sub.add_parser("wait", help="wait until an agent finishes")
    p_wait.add_argument("agent_id", metavar="ID")
    p_wait.add_argument("--timeout", type=int, default=None, metavar="SEC", required=True,
                        help="give up after SEC seconds without killing the agent")
    p_wait.set_defaults(func=cmd_wait)

    p_stop = sub.add_parser("stop", help="gracefully stop a running agent")
    p_stop.add_argument("agent_id", metavar="ID")
    p_stop.set_defaults(func=cmd_stop)

    p_kill = sub.add_parser("kill", help="forcefully terminate a running agent")
    p_kill.add_argument("agent_id", metavar="ID")
    p_kill.set_defaults(func=cmd_kill)

    p_delete = sub.add_parser("delete", help="delete an agent's local state and logs")
    p_delete.add_argument("agent_id", metavar="ID")
    p_delete.add_argument("--force", action="store_true",
                          help="kill a running agent before deleting it")
    p_delete.set_defaults(func=cmd_delete)

    p_clean = sub.add_parser("clean", help="garbage-collect old finished agents")
    p_clean.add_argument("--days", type=int, default=None, metavar="N",
                         help="retention period in days (default 14)")
    p_clean.add_argument("--dry-run", action="store_true", help="only list what would be removed")
    p_clean.set_defaults(func=cmd_clean)

    p_last = sub.add_parser("last", help="print the most recently used agent ID")
    p_last.set_defaults(func=cmd_last)

    return parser


def main(argv: list[str] | None = None) -> int:
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)
    argv = list(argv) if argv is not None else sys.argv[1:]

    # Hidden internal entry point used by the background runner.
    if argv and argv[0] == "_runner":
        if len(argv) != 3:
            return EXIT_USAGE
        runner(argv[1], argv[2])
        return EXIT_OK

    parser = build_parser()
    args = parser.parse_args(argv)
    if not getattr(args, "command", None):
        parser.print_help()
        return EXIT_USAGE
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
