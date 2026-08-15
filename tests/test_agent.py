"""Tests for the Lubko agent management CLI."""

import contextlib
import json
import os
import shutil
import signal
import subprocess
import time
from collections.abc import Callable
from pathlib import Path
from typing import Final

import pytest

from lubko import agent
from tests import _process_guard as guard

SLEEP_BIN: Final = shutil.which("sleep") or "/bin/sleep"
MARKER_AID: Final = "a1b2c3d4"
FAILURE_EXIT_CODE: Final = 7
STEER_PROMPT_TOTAL: Final = 2
REQUIRED_COMMANDS: Final = frozenset({
    "new",
    "list",
    "status",
    "prompt",
    "log",
    "result",
    "wait",
    "stop",
    "kill",
    "delete",
    "clean",
    "last",
})


def spawn_marked_process(aid: str) -> subprocess.Popen[bytes]:
    """Spawn a long-lived process carrying the agent marker in its environment.

    The process is registered with the shared process guard so teardown owns
    and deterministically stops it even if an assertion fails mid-test.

    Args:
        aid: Agent ID to place in the process environment.

    Returns:
        The spawned process.
    """
    env = dict(os.environ)
    env["LUBKO_AGENT_ID"] = aid
    proc = subprocess.Popen(
        [SLEEP_BIN, "300"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        close_fds=True,
        env=env,
    )
    guard.register(proc)
    return proc


def kill_proc(proc: subprocess.Popen[bytes]) -> None:
    """Force-kill a spawned process and reap it.

    Args:
        proc: The spawned process.
    """
    if proc.poll() is None:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(proc.pid, signal.SIGKILL)
    proc.wait(timeout=5)
    guard.unregister(proc)


def meta_for_process(aid: str, proc: subprocess.Popen[bytes], cwd: str) -> agent.Meta:
    """Build running metadata pointing at a spawned process.

    Args:
        aid: Agent ID.
        proc: The spawned process.
        cwd: Working directory to record.

    Returns:
        Running metadata for the process.
    """
    meta = agent.base_meta(aid, cwd, "initial prompt", None, is_continue=False)
    meta["pid"] = proc.pid
    meta["pgid"] = proc.pid
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        start_time = agent.proc_start_ticks(proc.pid)
        if start_time is not None:
            meta["start_time"] = start_time
            if agent.is_alive(meta):
                break
        time.sleep(0.01)
    return meta


def make_agent(
    state_dir: Path,
    aid: str,
    *,
    state_value: str = "running",
    **overrides: object,
) -> agent.Meta:
    """Create and persist an agent with the given state.

    Args:
        state_dir: Agent state root.
        aid: Agent ID.
        state_value: Recorded state field.
        overrides: Extra metadata fields.

    Returns:
        The persisted metadata.
    """
    meta = agent.base_meta(aid, str(state_dir), "initial prompt", None, is_continue=False)
    meta["state"] = state_value
    meta.update(overrides)
    agent.write_meta(aid, meta)
    return meta


def fake_agent_command(_meta: agent.Meta, _prompt: str, *, is_continue: bool) -> list[str]:
    """Return a command that echoes the prompt to stdout for tests.

    Args:
        _meta: Agent metadata (unused).
        _prompt: Instruction (unused).
        is_continue: Whether this is a continuation (unused).

    Returns:
        A shell command echoing ``$LUBKO_PROMPT``.
    """
    del is_continue
    return ["sh", "-c", "printf '%s\\n' \"$LUBKO_PROMPT\""]


@pytest.fixture(autouse=True)
def state_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point the agent state directory at a temporary location.

    Returns:
        The temporary state root.
    """
    state = tmp_path / "state"
    state.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("XDG_STATE_HOME", str(state))
    return state


def test_state_root_uses_xdg_state_home(state_dir: Path) -> None:
    """The state root follows XDG_STATE_HOME."""
    assert agent.state_root() == state_dir / "lubko"
    assert agent.agents_dir() == state_dir / "lubko" / "agents"


def test_new_agent_id_avoids_collisions(monkeypatch: pytest.MonkeyPatch) -> None:
    """A colliding generated ID is skipped."""
    (agent.agents_dir() / "abcdef01").mkdir(parents=True)
    values = iter(["abcdef01", "12345678"])
    monkeypatch.setattr("lubko.agent.secrets.token_hex", lambda _n: next(values))
    assert agent.new_agent_id() == "12345678"


def test_write_and_read_meta_roundtrip() -> None:
    """Metadata survives a write/read round trip."""
    meta = agent.base_meta("abc12345", "/workspace", "hello", None, is_continue=False)
    agent.write_meta("abc12345", meta)
    assert agent.read_meta("abc12345") == meta
    assert agent.read_meta("missing") is None


def test_update_meta_applies_under_lock(state_dir: Path) -> None:
    """update_meta applies the mutation to persisted metadata."""
    make_agent(state_dir, "abc12345", state_value="running")
    agent.update_meta("abc12345", lambda m: m.update(state="succeeded"))
    meta = agent.read_meta("abc12345")
    assert meta is not None
    assert meta["state"] == "succeeded"


def test_update_meta_deleted_agent_is_noop() -> None:
    """Updating a deleted agent never resurrects it."""
    agent.update_meta("missing", lambda m: m.update(state="succeeded"))
    assert agent.read_meta("missing") is None


def test_cmd_last_prints_recent_agent(state_dir: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """The last command prints the most recently used agent ID."""
    make_agent(state_dir, "abc12345", state_value="succeeded")
    agent.mark_last("abc12345")
    assert agent.main(["last"]) == agent.EXIT_OK
    assert capsys.readouterr().out.strip() == "abc12345"


def test_cmd_last_no_previous(capsys: pytest.CaptureFixture[str]) -> None:
    """The last command reports when no previous agent exists."""
    assert agent.main(["last"]) == agent.EXIT_NOT_FOUND
    assert "no previous agent" in capsys.readouterr().err


def test_derive_state_returns_terminal_state(state_dir: Path) -> None:
    """A non-running recorded state is returned directly."""
    meta = make_agent(state_dir, "abc12345", state_value="succeeded")
    assert agent.derive_state(meta) == "succeeded"


def test_derive_state_running_before_pid_recorded(state_dir: Path) -> None:
    """A freshly launched agent without a PID is reported running."""
    meta = make_agent(state_dir, "abc12345", state_value="running")
    assert agent.derive_state(meta) == "running"


def test_derive_state_unknown_for_stale_unrecorded(state_dir: Path) -> None:
    """A launched-but-unrecorded agent becomes unknown after the start window."""
    meta = make_agent(
        state_dir,
        "abc12345",
        state_value="running",
        started_at=time.time() - 120,
    )
    assert agent.derive_state(meta) == "unknown"


def test_is_alive_matches_marked_process() -> None:
    """A live process with matching identity is reported alive."""
    proc = spawn_marked_process(MARKER_AID)
    try:
        meta = meta_for_process(MARKER_AID, proc, "/workspace")
        assert agent.is_alive(meta)
        assert agent.derive_state(meta) == "running"
    finally:
        kill_proc(proc)


def test_is_alive_rejects_wrong_start_time() -> None:
    """A mismatched start time (PID reuse) is never trusted."""
    proc = spawn_marked_process(MARKER_AID)
    try:
        meta = meta_for_process(MARKER_AID, proc, "/workspace")
        meta["start_time"] = (meta["start_time"] or 0) + 1
        assert not agent.is_alive(meta)
    finally:
        kill_proc(proc)


def test_fmt_time_handles_unknown() -> None:
    """fmt_time renders unknown epochs as a dash."""
    assert agent.fmt_time(None) == "-"


def test_fmt_age_compacts() -> None:
    """fmt_age renders compact human-readable ages."""
    now = time.time()
    assert agent.fmt_age(None) == "-"
    assert agent.fmt_age(now - 30) == "30s"
    assert agent.fmt_age(now - 90) == "1m"
    assert agent.fmt_age(now - 3600) == "1h0m"
    assert agent.fmt_age(now - 90000) == "1d1h"


def test_tail_lines_returns_last_n(state_dir: Path) -> None:
    """tail_lines returns the newest lines of a file."""
    log = state_dir / "out.log"
    log.write_text("a\nb\nc\n")
    assert agent.tail_lines(log, 2) == ["b", "c"]
    assert agent.tail_lines(log, 0) == ["a", "b", "c"]
    assert agent.tail_lines(log, 1, max_chars=4) == ["c"]


def test_tail_lines_drops_mid_line_fragment(state_dir: Path) -> None:
    """A mid-line fragment at the window start is dropped."""
    log = state_dir / "out.log"
    log.write_text("a\nb\nc\n")
    assert agent.tail_lines(log, 0, max_chars=3) == ["c"]


def test_log_excerpt_strips_ansi(state_dir: Path) -> None:
    """Log excerpts strip ANSI escape sequences."""
    log = state_dir / "out.log"
    log.write_text("\x1b[31mred\x1b[0m\nplain\n")
    assert agent.log_excerpt(log, 5, 2000) == ["red", "plain"]


def test_cmd_new_creates_agent(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The new command creates an agent with stable state and the last marker."""
    monkeypatch.setattr(agent, "spawn_runner", lambda _aid, _mode: None)
    cwd = str(state_dir)
    code = agent.main(["new", "--cwd", cwd, "--prompt", "do work", "--json"])
    assert code == agent.EXIT_OK
    out = json.loads(capsys.readouterr().out)
    aid = out["id"]
    meta = agent.read_meta(aid)
    assert meta is not None
    assert meta["cwd"] == cwd
    assert meta["initial_prompt"] == "do work"
    assert meta["prompt_count"] == 1
    assert agent.last_file().read_text().strip() == aid


def test_cmd_new_requires_prompt(capsys: pytest.CaptureFixture[str]) -> None:
    """The new command requires a prompt."""
    assert agent.main(["new"]) == agent.EXIT_USAGE
    assert "prompt is required" in capsys.readouterr().err


def test_cmd_new_rejects_missing_cwd(capsys: pytest.CaptureFixture[str]) -> None:
    """The new command rejects a missing working directory."""
    code = agent.main(["new", "--cwd", "/nonexistent", "--prompt", "hi"])
    assert code == agent.EXIT_ERROR
    assert "does not exist" in capsys.readouterr().err


def test_cmd_list_json_and_filters(
    state_dir: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The list command reports agents with their live states."""
    make_agent(state_dir, "aaaaaaaa", state_value="succeeded")
    make_agent(state_dir, "bbbbbbbb", state_value="running")
    assert agent.main(["list", "--json"]) == agent.EXIT_OK
    data = json.loads(capsys.readouterr().out)
    states = {item["id"]: item["state"] for item in data["agents"]}
    assert states == {"aaaaaaaa": "succeeded", "bbbbbbbb": "running"}
    assert agent.main(["list", "--running"]) == agent.EXIT_OK
    assert "bbbbbbbb" in capsys.readouterr().out
    assert "aaaaaaaa" not in capsys.readouterr().out


def test_cmd_status_json(state_dir: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """The status command reports detailed agent state as JSON."""
    make_agent(state_dir, "aaaaaaaa", state_value="succeeded", exit_code=0)
    assert agent.main(["status", "aaaaaaaa", "--json"]) == agent.EXIT_OK
    data = json.loads(capsys.readouterr().out)
    assert data["id"] == "aaaaaaaa"
    assert data["state"] == "succeeded"
    assert data["exit_code"] == 0


def test_cmd_status_unknown_agent(capsys: pytest.CaptureFixture[str]) -> None:
    """Status on an unknown agent returns not-found."""
    assert agent.main(["status", "deadbeef"]) == agent.EXIT_NOT_FOUND
    assert "unknown agent" in capsys.readouterr().err


def test_cmd_prompt_continues_terminal_agent(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Prompting a finished agent starts a continuation invocation."""
    make_agent(state_dir, "aaaaaaaa", state_value="succeeded", native_session_id="sess-1")
    spawned: list[str] = []
    monkeypatch.setattr(agent, "spawn_runner", lambda _aid, mode: spawned.append(mode))
    code = agent.main(["prompt", "aaaaaaaa", "--prompt", "more"])
    assert code == agent.EXIT_OK
    assert spawned == ["continue"]
    meta = agent.read_meta("aaaaaaaa")
    assert meta is not None
    assert meta["state"] == "running"
    assert meta["pending_prompt"] == "more"


def test_cmd_prompt_refuses_without_session(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Prompting refuses when the underlying session is unavailable."""
    make_agent(state_dir, "aaaaaaaa", state_value="succeeded")
    monkeypatch.setattr(agent, "_wait_for_session", lambda _aid: None)
    code = agent.main(["prompt", "aaaaaaaa", "--prompt", "more"])
    assert code == agent.EXIT_ERROR
    assert "session is not available" in capsys.readouterr().err


def test_cmd_prompt_steer_queues_for_busy_agent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Steering a busy agent queues the instruction and interrupts the run."""
    proc = spawn_marked_process("aaaaaaaa")
    try:
        agent.write_meta("aaaaaaaa", meta_for_process("aaaaaaaa", proc, "/workspace"))
        spawned: list[str] = []
        monkeypatch.setattr(agent, "spawn_runner", lambda _aid, mode: spawned.append(mode))
        code = agent.main(["prompt", "aaaaaaaa", "--steer", "--prompt", "redirect"])
        assert code == agent.EXIT_OK
        assert spawned == []
        current = agent.read_meta("aaaaaaaa")
        assert current is not None
        assert current["steer_queue"][0]["prompt"] == "redirect"
        assert current["intent"] == "steer"
    finally:
        kill_proc(proc)


def test_cmd_delete_terminal_agent(state_dir: Path) -> None:
    """Deleting a terminal agent removes its state and the last marker."""
    make_agent(state_dir, "aaaaaaaa", state_value="succeeded")
    agent.mark_last("aaaaaaaa")
    assert agent.main(["delete", "aaaaaaaa"]) == agent.EXIT_OK
    assert not (agent.agents_dir() / "aaaaaaaa").exists()
    assert not agent.last_file().exists()


def test_cmd_delete_refuses_running_without_force() -> None:
    """Deleting a running agent without --force is refused."""
    proc = spawn_marked_process("aaaaaaaa")
    try:
        agent.write_meta("aaaaaaaa", meta_for_process("aaaaaaaa", proc, "/workspace"))
        assert agent.main(["delete", "aaaaaaaa"]) == agent.EXIT_ERROR
        assert (agent.agents_dir() / "aaaaaaaa").exists()
    finally:
        kill_proc(proc)


def test_cmd_clean_removes_old_terminal_agents(
    state_dir: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Clean removes only old finished agents."""
    make_agent(
        state_dir,
        "aaaaaaaa",
        state_value="succeeded",
        finished_at=time.time() - 1000,
    )
    make_agent(state_dir, "bbbbbbbb", state_value="succeeded", finished_at=time.time() + 60)
    make_agent(state_dir, "cccccccc", state_value="running")
    assert agent.main(["clean", "--days", "0", "--dry-run"]) == agent.EXIT_OK
    dry_run = capsys.readouterr().out
    assert "aaaaaaaa" in dry_run
    assert "bbbbbbbb" not in dry_run
    assert "cccccccc" not in dry_run
    assert agent.main(["clean", "--days", "0"]) == agent.EXIT_OK
    assert not (agent.agents_dir() / "aaaaaaaa").exists()
    assert (agent.agents_dir() / "bbbbbbbb").exists()
    assert (agent.agents_dir() / "cccccccc").exists()


def test_cmd_wait_returns_exit_code_for_terminal(state_dir: Path) -> None:
    """Waiting on a terminal agent returns its exit code."""
    make_agent(state_dir, "aaaaaaaa", state_value="failed", exit_code=FAILURE_EXIT_CODE)
    assert agent.main(["wait", "aaaaaaaa", "--timeout", "5"]) == FAILURE_EXIT_CODE


def test_cmd_wait_timeout_for_running() -> None:
    """Waiting on a running agent times out without killing it."""
    proc = spawn_marked_process("aaaaaaaa")
    try:
        agent.write_meta("aaaaaaaa", meta_for_process("aaaaaaaa", proc, "/workspace"))
        assert agent.main(["wait", "aaaaaaaa", "--timeout", "1"]) == agent.EXIT_TIMEOUT
    finally:
        kill_proc(proc)


def test_runner_runs_invocation_and_records_result(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The runner executes one invocation and records a success result."""
    make_agent(state_dir, "aaaaaaaa", state_value="running", prompt_count=1)
    monkeypatch.setattr(agent, "build_agent_command", fake_agent_command)
    agent.runner("aaaaaaaa", "new")
    result = agent.read_meta("aaaaaaaa")
    assert result is not None
    assert result["state"] == "succeeded"
    assert result["exit_code"] == 0
    assert result["active_runner"] is False
    log = (agent.agents_dir() / "aaaaaaaa" / "output.log").read_text()
    assert "initial prompt" in log


def test_runner_drains_queued_steers(state_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The runner drains queued steers in FIFO order."""
    meta = make_agent(state_dir, "aaaaaaaa", state_value="running", prompt_count=1)
    meta["steer_queue"] = [{"seq": 1, "prompt": "second", "queued_at": time.time()}]
    meta["steer_seq"] = 1
    agent.write_meta("aaaaaaaa", meta)
    monkeypatch.setattr(agent, "build_agent_command", fake_agent_command)
    agent.runner("aaaaaaaa", "new")
    result = agent.read_meta("aaaaaaaa")
    assert result is not None
    assert result["state"] == "succeeded"
    assert result["steer_queue"] == []
    assert result["prompt_count"] == STEER_PROMPT_TOTAL
    log = (agent.agents_dir() / "aaaaaaaa" / "output.log").read_text()
    assert "initial prompt" in log
    assert "second" in log


def test_runner_records_failure(state_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The runner records a non-zero invocation result."""
    make_agent(state_dir, "aaaaaaaa", state_value="running")

    def failing_command(_meta: agent.Meta, _prompt: str, *, is_continue: bool) -> list[str]:
        del is_continue
        return ["sh", "-c", "exit 7"]

    monkeypatch.setattr(agent, "build_agent_command", failing_command)
    agent.runner("aaaaaaaa", "new")
    result = agent.read_meta("aaaaaaaa")
    assert result is not None
    assert result["state"] == "failed"
    assert result["exit_code"] == FAILURE_EXIT_CODE


def test_runner_fails_without_continuation_session(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A continuation without an underlying session fails cleanly."""
    meta = make_agent(state_dir, "aaaaaaaa", state_value="running")
    meta["pending_prompt"] = "continue this"
    agent.write_meta("aaaaaaaa", meta)
    monkeypatch.setattr(agent, "discover_session_id", lambda _aid: None)
    agent.runner("aaaaaaaa", "continue")
    result = agent.read_meta("aaaaaaaa")
    assert result is not None
    assert result["state"] == "failed"
    assert "session not available" in result["error"]


def test_build_agent_command_honors_override(monkeypatch: pytest.MonkeyPatch) -> None:
    """LUBKO_AGENT_CMD overrides the underlying agent command."""
    monkeypatch.setenv("LUBKO_AGENT_CMD", "opencode run --auto")
    meta = agent.base_meta("aaaaaaaa", "/workspace", "hi", None, is_continue=False)
    assert agent.build_agent_command(meta, "hi", is_continue=False) == [
        "/bin/sh",
        "-c",
        "opencode run --auto",
    ]


def test_exit_code_for_maps_states() -> None:
    """Exit codes follow the agent state."""
    assert agent.exit_code_for(None) == 1
    succeeded = agent.base_meta("a", "/", "p", None, is_continue=False)
    succeeded["state"] = "succeeded"
    assert agent.exit_code_for(succeeded) == 0
    failed = agent.base_meta("b", "/", "p", None, is_continue=False)
    failed["state"] = "failed"
    failed["exit_code"] = FAILURE_EXIT_CODE
    assert agent.exit_code_for(failed) == FAILURE_EXIT_CODE
    failed["exit_code"] = None
    assert agent.exit_code_for(failed) == 1


def test_subcommands_present() -> None:
    """Every documented management command is exposed."""
    names = {spec.name for spec in agent.SUBCOMMANDS}
    assert names == REQUIRED_COMMANDS


def test_main_without_command_returns_usage() -> None:
    """Invoking the CLI without a command prints usage."""
    assert agent.main([]) == agent.EXIT_USAGE


def spawn_marked_term_ignoring(aid: str) -> subprocess.Popen[bytes]:
    """Spawn a marked process whose group ignores ``SIGTERM``.

    The session-leader shell traps ``TERM`` while its ``sleep`` child keeps
    the group alive, so graceful ``stop`` must escalate to ``SIGKILL`` to end
    the whole exact group.

    Args:
        aid: Agent ID to place in the process environment.

    Returns:
        The spawned process.
    """
    env = dict(os.environ)
    env["LUBKO_AGENT_ID"] = aid
    proc = subprocess.Popen(
        ["/bin/sh", "-c", "trap '' TERM; while :; do sleep 1; done"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        close_fds=True,
        env=env,
    )
    guard.register(proc)
    return proc


def wait_until(predicate: Callable[[], bool], timeout: float = 10.0) -> None:
    """Poll until a predicate holds, raising if the deadline expires.

    Args:
        predicate: Condition to satisfy.
        timeout: Maximum seconds to wait.

    Raises:
        AssertionError: If the condition is not met within the timeout.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    msg = "condition not met within timeout"
    raise AssertionError(msg)


def test_is_alive_requires_exact_marker_entry() -> None:
    """An ID that is a prefix of another marker is never an exact match."""
    proc = spawn_marked_process(MARKER_AID)
    try:
        exact = meta_for_process(MARKER_AID, proc, "/workspace")
        assert agent.is_alive(exact)
        prefix = meta_for_process(MARKER_AID + "5", proc, "/workspace")
        prefix["start_time"] = agent.proc_start_ticks(proc.pid)
        assert not agent.is_alive(prefix)
    finally:
        kill_proc(proc)


def test_spawn_runner_sets_exact_agent_marker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """spawn_runner overwrites a stale marker with the exact agent ID."""
    monkeypatch.setenv("LUBKO_AGENT_ID", "stale-value")
    captured: list[dict[str, str]] = []

    def recording_popen(_argv: object, **kwargs: object) -> None:
        env = kwargs.get("env")
        assert isinstance(env, dict)
        captured.append({str(key): str(value) for key, value in env.items()})

    monkeypatch.setattr(subprocess, "Popen", recording_popen)
    agent.spawn_runner("aaaaaaaa", "new")

    assert len(captured) == 1
    assert captured[0]["LUBKO_AGENT_ID"] == "aaaaaaaa"


def test_runner_does_not_drop_concurrently_queued_prompt(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A prompt queued between read and claim is never lost.

    The runner clears ``pending_prompt`` under the metadata lock only when it
    exactly matches the prompt being claimed; a continuation that landed in
    the read-to-claim window must survive for the next invocation.
    """
    make_agent(state_dir, "aaaaaaaa", state_value="running", prompt_count=1)
    agent.update_meta("aaaaaaaa", lambda m: m.update(pending_prompt="first"))

    def racing_command(_meta: agent.Meta, _prompt: str, *, is_continue: bool) -> list[str]:
        del is_continue
        agent.update_meta("aaaaaaaa", lambda m: m.update(pending_prompt="second"))
        return ["sh", "-c", "printf '%s\\n' \"$LUBKO_PROMPT\""]

    monkeypatch.setattr(agent, "build_agent_command", racing_command)
    agent.runner("aaaaaaaa", "continue")
    result = agent.read_meta("aaaaaaaa")
    assert result is not None
    assert result["state"] == "succeeded"
    log = (agent.agents_dir() / "aaaaaaaa" / "output.log").read_text()
    assert "first" in log
    assert "second" in log


def test_runner_runs_prompt_queued_during_invocation(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A continuation queued while an invocation runs is executed next."""
    make_agent(state_dir, "aaaaaaaa", state_value="running", prompt_count=1)
    queued = {"done": False}

    def queuing_command(_meta: agent.Meta, _prompt: str, *, is_continue: bool) -> list[str]:
        del is_continue
        if not queued["done"]:
            queued["done"] = True
            agent.update_meta("aaaaaaaa", lambda m: m.update(pending_prompt="second"))
        return ["sh", "-c", "printf '%s\\n' \"$LUBKO_PROMPT\""]

    monkeypatch.setattr(agent, "build_agent_command", queuing_command)
    agent.runner("aaaaaaaa", "new")
    result = agent.read_meta("aaaaaaaa")
    assert result is not None
    assert result["state"] == "succeeded"
    log = (agent.agents_dir() / "aaaaaaaa" / "output.log").read_text()
    assert "initial prompt" in log
    assert "second" in log


def test_runner_reclaims_queued_steer_on_start(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A replacement runner executes a steer queued before it started."""
    meta = make_agent(state_dir, "aaaaaaaa", state_value="running", prompt_count=1)
    meta["steer_queue"] = [{"seq": 1, "prompt": "queued", "queued_at": time.time()}]
    meta["steer_seq"] = 1
    agent.write_meta("aaaaaaaa", meta)
    monkeypatch.setattr(agent, "build_agent_command", fake_agent_command)
    agent.runner("aaaaaaaa", "continue")
    result = agent.read_meta("aaaaaaaa")
    assert result is not None
    assert result["state"] == "succeeded"
    assert result["steer_queue"] == []
    assert result["active_runner"] is False
    log = (agent.agents_dir() / "aaaaaaaa" / "output.log").read_text()
    assert "queued" in log


def test_runner_abnormal_exit_kills_invocation_group(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unexpected runner failure never orphans the invocation group."""
    make_agent(state_dir, "aaaaaaaa", state_value="running", prompt_count=1)
    monkeypatch.setattr(agent, "build_agent_command", lambda *_a, **_k: [SLEEP_BIN, "300"])

    def boom(_proc: subprocess.Popen[bytes], _aid: str, *, is_continue: bool) -> int:
        del is_continue
        msg = "simulated runner failure"
        raise RuntimeError(msg)

    monkeypatch.setattr(agent, "_wait_for_invocation_exit", boom)
    with pytest.raises(RuntimeError, match="simulated"):
        agent.runner("aaaaaaaa", "new")

    result = agent.read_meta("aaaaaaaa")
    assert result is not None
    assert result["state"] == "failed"
    assert result["active_runner"] is False
    assert "aborted unexpectedly" in result["error"]
    assert not agent.group_alive(result)


def test_prompt_skips_duplicate_runner_when_live_runner_exists(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A prompt does not spawn a second runner while one is live."""
    runner_proc = spawn_marked_process("aaaaaaaa")
    try:
        meta = make_agent(
            state_dir,
            "aaaaaaaa",
            state_value="succeeded",
            native_session_id="sess-1",
        )
        meta["active_runner"] = True
        meta["runner_pid"] = runner_proc.pid
        meta["runner_start_time"] = agent.proc_start_ticks(runner_proc.pid)
        agent.write_meta("aaaaaaaa", meta)
        spawned: list[str] = []
        monkeypatch.setattr(agent, "spawn_runner", lambda _aid, mode: spawned.append(mode))
        code = agent.main(["prompt", "aaaaaaaa", "--prompt", "more"])
        assert code == agent.EXIT_OK
        assert spawned == []
        current = agent.read_meta("aaaaaaaa")
        assert current is not None
        assert current["pending_prompt"] == "more"
    finally:
        kill_proc(runner_proc)


def test_cmd_stop_escalates_to_sigkill_when_group_survives(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A TERM-ignoring group is force-killed so no member is abandoned."""
    proc = spawn_marked_term_ignoring("aaaaaaaa")
    try:
        monkeypatch.setattr(agent, "STOP_WAIT_SECONDS", 0.3)
        monkeypatch.setattr(agent, "KILL_WAIT_SECONDS", 2.0)
        agent.write_meta("aaaaaaaa", meta_for_process("aaaaaaaa", proc, str(state_dir)))
        code = agent.main(["stop", "aaaaaaaa"])
        assert code == agent.EXIT_OK
        assert "stopped" in capsys.readouterr().out
        meta = agent.read_meta("aaaaaaaa")
        assert meta is not None
        assert meta["state"] == "stopped"
        assert meta["exit_signal"] == signal.SIGKILL
        wait_until(lambda: proc.poll() is not None)
    finally:
        kill_proc(proc)


def test_cmd_kill_confirms_whole_group_gone(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Kill signals the exact group and waits for every member to leave."""
    proc = spawn_marked_term_ignoring("aaaaaaaa")
    try:
        monkeypatch.setattr(agent, "KILL_WAIT_SECONDS", 2.0)
        agent.write_meta("aaaaaaaa", meta_for_process("aaaaaaaa", proc, str(state_dir)))
        code = agent.main(["kill", "aaaaaaaa"])
        assert code == agent.EXIT_OK
        assert "killed" in capsys.readouterr().out
        meta = agent.read_meta("aaaaaaaa")
        assert meta is not None
        assert meta["state"] == "killed"
        assert meta["exit_signal"] == signal.SIGKILL
        wait_until(lambda: proc.poll() is not None)
        wait_until(lambda: not agent.group_alive(meta))
    finally:
        kill_proc(proc)


def test_cmd_delete_force_kills_live_group(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Force-deleting a running agent kills its whole exact group first."""
    proc = spawn_marked_term_ignoring("aaaaaaaa")
    try:
        monkeypatch.setattr(agent, "KILL_WAIT_SECONDS", 2.0)
        agent.write_meta("aaaaaaaa", meta_for_process("aaaaaaaa", proc, str(state_dir)))
        code = agent.main(["delete", "aaaaaaaa", "--force"])
        assert code == agent.EXIT_OK
        assert "deleted" in capsys.readouterr().out
        assert not (agent.agents_dir() / "aaaaaaaa").exists()
        wait_until(lambda: proc.poll() is not None)
    finally:
        kill_proc(proc)


def test_runner_alive_matches_exact_identity(state_dir: Path) -> None:
    """A live runner with matching start time and marker is reported alive."""
    proc = spawn_marked_process("aaaaaaaa")
    try:
        meta = make_agent(state_dir, "aaaaaaaa", state_value="running")
        meta["active_runner"] = True
        meta["runner_pid"] = proc.pid
        meta["runner_start_time"] = agent.proc_start_ticks(proc.pid)
        assert agent.runner_alive(meta)
    finally:
        kill_proc(proc)


def test_runner_alive_rejects_recycled_start_time(state_dir: Path) -> None:
    """A runner whose recorded start time does not match is never alive."""
    proc = spawn_marked_process("aaaaaaaa")
    try:
        meta = make_agent(state_dir, "aaaaaaaa", state_value="running")
        meta["active_runner"] = True
        meta["runner_pid"] = proc.pid
        meta["runner_start_time"] = agent.proc_start_ticks(proc.pid)
        assert agent.runner_alive(meta)
        meta["runner_start_time"] = (meta["runner_start_time"] or 0) + 1
        assert not agent.runner_alive(meta)
    finally:
        kill_proc(proc)


def test_runner_alive_requires_exact_marker(state_dir: Path) -> None:
    """A process carrying another agent's marker is never our runner."""
    proc = spawn_marked_process("bbbbbbbb")
    try:
        meta = make_agent(state_dir, "aaaaaaaa", state_value="running")
        meta["active_runner"] = True
        meta["runner_pid"] = proc.pid
        meta["runner_start_time"] = agent.proc_start_ticks(proc.pid)
        assert not agent.runner_alive(meta)
    finally:
        kill_proc(proc)
