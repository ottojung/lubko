"""Tests for the Lubko agent management CLI."""

import contextlib
import functools
import json
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any, BinaryIO, Final, cast

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
    "wait",
    "stop",
    "kill",
    "delete",
    "clean",
})
REMOVED_COMMANDS: Final = frozenset({"last", "result"})


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
    meta = agent.idle_meta(aid, cwd, None)
    meta["state"] = "running"
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
    meta = agent.idle_meta(aid, str(state_dir), None)
    meta["state"] = state_value
    meta["pending_prompt"] = "initial prompt"
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


def test_normalize_agent_id_lowercases_hex() -> None:
    """A caller-supplied base-16 ID is normalized to lowercase hex digits."""
    assert agent.normalize_agent_id("A13F09C2") == "a13f09c2"
    assert agent.normalize_agent_id("  a13f09c2  ") == "a13f09c2"


def test_normalize_agent_id_rejects_malformed() -> None:
    """Malformed agent IDs are rejected clearly."""
    assert agent.normalize_agent_id(None) is None
    assert agent.normalize_agent_id("") is None
    assert agent.normalize_agent_id("not-hex!") is None
    assert agent.normalize_agent_id("../etc/passwd") is None
    assert agent.normalize_agent_id("a13f09c2z") is None


def test_write_and_read_meta_roundtrip() -> None:
    """Metadata survives a write/read round trip."""
    meta = agent.idle_meta("abc12345", "/workspace", None)
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


def test_derive_state_returns_terminal_state(state_dir: Path) -> None:
    """A non-running recorded state is returned directly."""
    meta = make_agent(state_dir, "abc12345", state_value="succeeded")
    assert agent.derive_state(meta) == "succeeded"


def test_derive_state_returns_idle_for_never_prompted(state_dir: Path) -> None:
    """A freshly created, never-prompted agent is idle, not running."""
    meta = agent.idle_meta("abc12345", str(state_dir), None)
    agent.write_meta("abc12345", meta)
    assert agent.derive_state(agent.read_meta("abc12345")) == "idle"


def test_idle_meta_is_the_single_current_schema() -> None:
    """Fresh idle metadata uses the one current agent metadata version."""
    meta = agent.idle_meta("abc12345", "/workspace", None)
    assert meta["agent_version"] == agent.AGENT_META_VERSION
    assert "initial_prompt" not in meta
    assert "pending_prompt" not in meta


def test_legacy_base_meta_machinery_is_removed() -> None:
    """The obsolete base_meta builder no longer exists."""
    assert not hasattr(agent, "base_meta")


def test_derive_state_running_before_pid_recorded(state_dir: Path) -> None:
    """A freshly launched agent without a PID is reported running."""
    meta = make_agent(state_dir, "abc12345", state_value="running")
    assert agent.derive_state(meta) == "running"


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
    assert agent.tail_lines(log, 1) == ["c"]


def test_log_excerpt_strips_ansi(state_dir: Path) -> None:
    """Log excerpts strip ANSI escape sequences."""
    log = state_dir / "out.log"
    log.write_text("\x1b[31mred\x1b[0m\nplain\n")
    assert agent.log_excerpt(log, 5) == ["red", "plain"]


def test_fold_line_wraps_at_width() -> None:
    """fold_line hard-wraps logical lines at the fold width."""
    assert agent.fold_line("") == [""]
    assert agent.fold_line("x" * agent.FOLD_WIDTH) == ["x" * agent.FOLD_WIDTH]
    assert agent.fold_line("x" * (agent.FOLD_WIDTH + 1)) == [
        "x" * agent.FOLD_WIDTH,
        "x",
    ]
    assert agent.fold_line("x" * (2 * agent.FOLD_WIDTH)) == [
        "x" * agent.FOLD_WIDTH,
        "x" * agent.FOLD_WIDTH,
    ]


def test_tail_lines_folds_long_lines_without_char_cap(state_dir: Path) -> None:
    """Long logical lines fold to display lines; only line count limits."""
    log = state_dir / "out.log"
    logical = [f"{i:02d}-" + "x" * agent.FOLD_WIDTH for i in range(30)]
    log.write_text("\n".join(logical) + "\n")
    expected_all = [piece for line in logical for piece in agent.fold_line(line)]
    assert agent.tail_lines(log, 0) == expected_all
    assert agent.tail_lines(log, 60) == expected_all
    assert agent.tail_lines(log, 4) == agent.fold_line(logical[-2]) + agent.fold_line(logical[-1])
    assert len(agent.tail_lines(log, 1)) == 1


def test_tail_lines_drops_partial_line_at_backward_block_boundary(
    state_dir: Path,
) -> None:
    """A partial first line at the 64KiB backward-read boundary is not complete."""
    log = state_dir / "out.log"
    block = 64 * 1024
    long_line = "A" * (block + 10)
    log.write_text(f"{long_line}\nl1\nl2\nl3\nl4\n")
    assert log.stat().st_size > block
    assert agent.tail_lines(log, 5) == ["l1", "l2", "l3", "l4"]


def test_tail_lines_drops_partial_prefix_when_trailing_blank(
    state_dir: Path,
) -> None:
    """A partial 64KiB-boundary prefix is dropped even when trailing lines are blank."""
    log = state_dir / "out.log"
    block = 64 * 1024
    long_line = "A" * (block + 10)
    log.write_text(f"{long_line}\n\n\n\n\n")
    assert log.stat().st_size > block
    assert agent.tail_lines(log, 5) == ["", "", "", ""]


def test_tail_snapshot_folds_and_returns_raw_offset(state_dir: Path) -> None:
    """The follow snapshot shows folded display bytes and a raw byte offset."""
    log = state_dir / "out.log"
    logical = [f"{i:02d}" + "y" * agent.FOLD_WIDTH for i in range(10)]
    log.write_text("\n".join(logical) + "\n")
    snapshot = agent.tail_snapshot(log, 3)
    assert snapshot.offset == log.stat().st_size
    assert not snapshot.pending
    expected = agent.tail_lines(log, 3)
    assert len(expected) == 3
    assert snapshot.display == ("\n".join(expected) + "\n").encode()
    all_snapshot = agent.tail_snapshot(log, 100)
    assert all_snapshot.offset == log.stat().st_size
    assert all_snapshot.display == ("\n".join(agent.tail_lines(log, 0)) + "\n").encode()


def test_tail_snapshot_trailing_newline_reflects_file(state_dir: Path) -> None:
    """The snapshot keeps a trailing newline only when the log has one."""
    log = state_dir / "out.log"
    log.write_text("line1\nline2")
    snapshot = agent.tail_snapshot(log, 5)
    assert snapshot.display == b"line1\nline2"
    assert snapshot.offset == log.stat().st_size
    log.write_text("line1\nline2\n")
    snapshot_newline = agent.tail_snapshot(log, 5)
    assert snapshot_newline.display == b"line1\nline2\n"


def test_tail_snapshot_race_appends_after_size_capture(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The public snapshot excludes bytes appended after its size capture."""
    log = state_dir / "out.log"
    initial = b"first\nsecond\n"
    log.write_text(initial.decode())
    snapshot_from = cast(
        "Callable[[BinaryIO, int, int], agent.LogSnapshot]",
        agent.__dict__["_tail_snapshot_from"],
    )

    def wrapped(fh: BinaryIO, end: int, max_lines: int) -> agent.LogSnapshot:
        with log.open("ab") as writer:
            writer.write(b"third\n")
        return snapshot_from(fh, end, max_lines)

    monkeypatch.setattr(agent, "_tail_snapshot_from", wrapped)
    snapshot = agent.tail_snapshot(log, 10)
    assert snapshot.offset == len(initial)
    assert snapshot.display == initial
    assert b"third" not in snapshot.display
    assert log.read_bytes().endswith(b"third\n")


def test_log_excerpt_status_tail_is_line_count_based(state_dir: Path) -> None:
    """The status tail folds long lines and is limited only by line count."""
    log = state_dir / "out.log"
    logical = [f"{i:02d}" + "z" * agent.FOLD_WIDTH for i in range(60)]
    log.write_text("\n".join(logical) + "\n")
    excerpt = agent.log_excerpt(log, agent.STATUS_TAIL_LINES)
    assert len(excerpt) == agent.STATUS_TAIL_LINES
    assert excerpt == agent.tail_lines(log, agent.STATUS_TAIL_LINES)
    assert excerpt[-1] == "zz"


def test_cmd_log_lines_is_authoritative_with_long_lines(
    state_dir: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """--lines N returns exactly N folded display lines with no char cap."""
    make_agent(state_dir, "aaaaaaaa", state_value="succeeded", exit_code=0)
    log_path = agent.agents_dir() / "aaaaaaaa" / "output.log"
    logical = [f"{i:02d}" + "x" * agent.FOLD_WIDTH for i in range(40)]
    log_path.write_text("\n".join(logical) + "\n")
    assert agent.main(["log", "aaaaaaaa", "--lines", "10"]) == agent.EXIT_OK
    lines = capsys.readouterr().out.splitlines()
    assert len(lines) == 10
    assert lines == agent.tail_lines(log_path, 10)
    assert agent.main(["log", "aaaaaaaa", "--lines", "0"]) == agent.EXIT_OK
    assert len(capsys.readouterr().out.splitlines()) == len(agent.tail_lines(log_path, 0))
    assert agent.main(["log", "aaaaaaaa"]) == agent.EXIT_OK
    assert len(capsys.readouterr().out.splitlines()) == agent.STATUS_TAIL_LINES


def test_cmd_log_lines_100_is_authoritative_over_old_byte_cap(
    state_dir: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """--lines 100 returns 100 folded lines even when output exceeds a byte cap."""
    make_agent(state_dir, "aaaaaaaa", state_value="succeeded", exit_code=0)
    log_path = agent.agents_dir() / "aaaaaaaa" / "output.log"
    log_path.write_text(("l" * 200 + "\n") * 100)
    assert agent.main(["log", "aaaaaaaa", "--lines", "100"]) == agent.EXIT_OK
    lines = capsys.readouterr().out.splitlines()
    assert len(lines) == 100
    assert all(len(line) <= agent.FOLD_WIDTH for line in lines)
    assert lines[-1] == "l" * 40


def test_cmd_log_without_follow_returns_no_output(
    state_dir: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Log without --follow reports no output immediately."""
    make_agent(state_dir, "aaaaaaaa", state_value="running")
    assert agent.main(["log", "aaaaaaaa"]) == agent.EXIT_OK
    assert "(no output yet)" in capsys.readouterr().err


def test_cmd_log_follow_returns_promptly_for_terminal_without_output(
    state_dir: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Log --follow stops waiting once the agent is terminal with no output."""
    make_agent(state_dir, "aaaaaaaa", state_value="succeeded", exit_code=0)
    assert agent.main(["log", "aaaaaaaa", "--follow"]) == agent.EXIT_OK
    assert "(no output yet)" in capsys.readouterr().err


def test_cmd_log_follow_live_pid_waits_past_old_timeout(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Log --follow keeps waiting for a live recorded pid past the old 30s cutoff."""
    proc = spawn_marked_process("aaaaaaaa")
    try:
        agent.write_meta("aaaaaaaa", meta_for_process("aaaaaaaa", proc, "/workspace"))
        log_path = agent.agents_dir() / "aaaaaaaa" / "output.log"
        counter = {"sleep": 0}
        streamed: list[tuple[str, int]] = []
        monkeypatch.setattr(
            agent,
            "stream_log_until_terminal",
            lambda aid, follow_lines: streamed.append((aid, follow_lines)),
        )

        def fake_sleep(_seconds: float) -> None:
            counter["sleep"] += 1
            if counter["sleep"] == 5:
                log_path.write_text("first output\n")

        def fake_monotonic() -> float:
            return 10_000.0 + 10.0 * counter["sleep"]

        monkeypatch.setattr(time, "sleep", fake_sleep)
        monkeypatch.setattr(time, "monotonic", fake_monotonic)
        assert agent.main(["log", "aaaaaaaa", "--follow", "--lines", "3"]) == agent.EXIT_OK
        assert counter["sleep"] == 5
        assert streamed == [("aaaaaaaa", 3)]
        assert "(no output yet)" not in capsys.readouterr().err
    finally:
        kill_proc(proc)


def test_cmd_log_follow_live_runner_pre_pid_waits_past_old_timeout(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Log --follow keeps waiting for a live pre-pid runner past the old cutoff."""
    proc = spawn_marked_process("aaaaaaaa")
    try:
        meta = agent.idle_meta("aaaaaaaa", "/workspace", None)
        meta["state"] = "running"
        meta["started_at"] = time.time()
        meta["pid"] = None
        meta["runner_pid"] = proc.pid
        meta["runner_start_time"] = agent.proc_start_ticks(proc.pid)
        agent.write_meta("aaaaaaaa", meta)
        log_path = agent.agents_dir() / "aaaaaaaa" / "output.log"
        counter = {"sleep": 0}
        streamed: list[tuple[str, int]] = []
        monkeypatch.setattr(
            agent,
            "stream_log_until_terminal",
            lambda aid, follow_lines: streamed.append((aid, follow_lines)),
        )

        def fake_sleep(_seconds: float) -> None:
            counter["sleep"] += 1
            if counter["sleep"] == 5:
                log_path.write_text("first output\n")

        def fake_monotonic() -> float:
            return 10_000.0 + 10.0 * counter["sleep"]

        monkeypatch.setattr(time, "sleep", fake_sleep)
        monkeypatch.setattr(time, "monotonic", fake_monotonic)
        assert agent.main(["log", "aaaaaaaa", "--follow", "--lines", "3"]) == agent.EXIT_OK
        assert counter["sleep"] == 5
        assert streamed == [("aaaaaaaa", 3)]
        assert "(no output yet)" not in capsys.readouterr().err
    finally:
        kill_proc(proc)


@pytest.mark.parametrize("state_value", ["idle", "succeeded", "failed", "stopped", "killed"])
def test_cmd_log_follow_idle_or_terminal_returns_promptly(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    state_value: str,
) -> None:
    """Log --follow returns promptly with no output for idle and terminal agents."""
    make_agent(state_dir, "aaaaaaaa", state_value=state_value)
    monkeypatch.setattr(
        time,
        "sleep",
        lambda _seconds: pytest.fail("should not sleep for idle/terminal"),
    )
    assert agent.main(["log", "aaaaaaaa", "--follow"]) == agent.EXIT_OK
    assert "(no output yet)" in capsys.readouterr().err


def test_cmd_log_follow_stale_pid_returns_promptly(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Log --follow returns promptly when the recorded pid is provably dead."""
    make_agent(
        state_dir,
        "aaaaaaaa",
        state_value="running",
        pid=2**30,
        start_time=1,
        finished_at=time.time(),
    )
    monkeypatch.setattr(
        time,
        "sleep",
        lambda _seconds: pytest.fail("should not sleep for a stale pid"),
    )
    assert agent.main(["log", "aaaaaaaa", "--follow"]) == agent.EXIT_OK
    assert "(no output yet)" in capsys.readouterr().err


def test_cmd_log_follow_dead_runner_returns_promptly(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Log --follow returns promptly when the pre-pid runner is dead."""
    make_agent(
        state_dir,
        "aaaaaaaa",
        state_value="running",
        pid=None,
        started_at=time.time(),
        runner_pid=2**30,
        runner_start_time=1,
    )
    monkeypatch.setattr(
        time,
        "sleep",
        lambda _seconds: pytest.fail("should not sleep for a dead runner"),
    )
    assert agent.main(["log", "aaaaaaaa", "--follow"]) == agent.EXIT_OK
    assert "(no output yet)" in capsys.readouterr().err


def test_cmd_log_follow_waits_for_first_output(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Log --follow waits for first output instead of returning after a short sleep."""
    proc = spawn_marked_process("aaaaaaaa")
    try:
        agent.write_meta("aaaaaaaa", meta_for_process("aaaaaaaa", proc, "/workspace"))
        called: list[tuple[str, int]] = []
        monkeypatch.setattr(
            agent,
            "stream_log_until_terminal",
            lambda aid, follow_lines: called.append((aid, follow_lines)),
        )
        log_path = agent.agents_dir() / "aaaaaaaa" / "output.log"

        def write_log_after_delay() -> None:
            time.sleep(0.6)
            log_path.write_text("first output\n")

        writer = threading.Thread(target=write_log_after_delay, daemon=True)
        writer.start()
        started = time.monotonic()
        code = agent.main(["log", "aaaaaaaa", "--follow", "--lines", "3"])
        elapsed = time.monotonic() - started
        assert code == agent.EXIT_OK
        assert called == [("aaaaaaaa", 3)]
        assert "(no output yet)" not in capsys.readouterr().err
        assert elapsed >= 0.5
        writer.join(timeout=5)
    finally:
        kill_proc(proc)


def test_cmd_log_follow_snapshots_folded_tail_then_stops(
    state_dir: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Log --follow prints a folded snapshot and stops for a terminal agent."""
    make_agent(state_dir, "aaaaaaaa", state_value="succeeded", exit_code=0)
    log_path = agent.agents_dir() / "aaaaaaaa" / "output.log"
    log_path.write_text("hello\n" + "x" * (agent.FOLD_WIDTH + 10) + "\n")
    assert agent.main(["log", "aaaaaaaa", "--follow", "--lines", "5"]) == agent.EXIT_OK
    out = capsys.readouterr().out
    assert "hello" in out
    assert "x" * agent.FOLD_WIDTH in out
    assert "x" * (agent.FOLD_WIDTH + 10) not in out


def test_tail_lines_strips_ansi(state_dir: Path) -> None:
    """tail_lines strips ANSI CSI/SGR sequences from displayed lines."""
    log = state_dir / "out.log"
    log.write_text("\x1b[31;1mbold red\x1b[0m\n\x1b[2m\x1b[36mcyan dim\x1b[0m\nplain\n")
    assert agent.tail_lines(log, 0) == ["bold red", "cyan dim", "plain"]
    assert agent.tail_lines(log, 2) == ["cyan dim", "plain"]


def test_tail_snapshot_strips_ansi(state_dir: Path) -> None:
    """The follow snapshot strips ANSI CSI/SGR sequences from displayed bytes."""
    log = state_dir / "out.log"
    log.write_text("\x1b[32mgreen\x1b[0m\nplain\n")
    snapshot = agent.tail_snapshot(log, 5)
    assert snapshot.offset == log.stat().st_size
    assert snapshot.display == b"green\nplain\n"


def test_tail_snapshot_holds_dangling_fragment_for_follow(state_dir: Path) -> None:
    """A snapshot ending mid-CSI holds the fragment back instead of leaking it."""
    log = state_dir / "out.log"
    log.write_bytes(b"intro\n\x1b[3")
    snapshot = agent.tail_snapshot(log, 5)
    assert snapshot.offset == log.stat().st_size
    assert b"\x1b" not in snapshot.display
    assert snapshot.display == b"intro\n"
    assert snapshot.pending == "\x1b[3"


def test_snapshot_follow_handoff_continues_boundary_sequence(
    state_dir: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The follow normalizer completes stripping of a snapshot-boundary sequence."""
    log = state_dir / "out.log"
    log.write_bytes(b"intro\n\x1b[3")
    snapshot = agent.tail_snapshot(log, 5)
    assert snapshot.pending == "\x1b[3"
    with log.open("ab") as writer:
        writer.write(b"1mrest\x1b[0m done\n")
    normalizer = cast(
        "type[Any]",
        agent.__dict__["_LogNormalizer"],
    )(pending=snapshot.pending)
    with log.open("rb") as fh:
        fh.seek(snapshot.offset)
        normalizer.write(fh.read())
    normalizer.close()
    out = capsys.readouterr().out
    assert "\x1b" not in out
    assert "rest" in out
    assert "done" in out


def test_cmd_log_drops_dangling_fragment_without_follow(
    state_dir: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Log without --follow never shows an incomplete trailing escape fragment."""
    make_agent(state_dir, "aaaaaaaa", state_value="succeeded", exit_code=0)
    log_path = agent.agents_dir() / "aaaaaaaa" / "output.log"
    log_path.write_bytes(b"clean\n\x1b[3")
    assert agent.main(["log", "aaaaaaaa"]) == agent.EXIT_OK
    out = capsys.readouterr().out
    assert "\x1b" not in out
    assert "clean" in out


def test_cmd_log_follow_normalizes_growing_boundary_output(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Growing --follow output is normalized, including the snapshot-boundary split."""
    proc = spawn_marked_process("aaaaaaaa")
    try:
        agent.write_meta("aaaaaaaa", meta_for_process("aaaaaaaa", proc, "/workspace"))
        log_path = agent.agents_dir() / "aaaaaaaa" / "output.log"
        log_path.write_bytes(b"start\n\x1b[3")

        def append_and_finish() -> None:
            time.sleep(0.5)
            with log_path.open("ab") as writer:
                writer.write(b"1mgreen text\x1b[0m\nend\n")
            agent.update_meta(
                "aaaaaaaa",
                lambda m: m.update(state="succeeded", finished_at=time.time()),
            )

        writer = threading.Thread(target=append_and_finish, daemon=True)
        writer.start()
        code = agent.main(["log", "aaaaaaaa", "--follow", "--lines", "5"])
        assert code == agent.EXIT_OK
        out = capsys.readouterr().out
        assert "\x1b" not in out
        assert "start" in out
        assert "green text" in out
        assert "end" in out
        writer.join(timeout=5)
    finally:
        kill_proc(proc)


def test_cmd_log_preserves_durable_raw_log(
    state_dir: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The durable output.log retains raw control bytes while display is normalized."""
    make_agent(state_dir, "aaaaaaaa", state_value="succeeded", exit_code=0)
    log_path = agent.agents_dir() / "aaaaaaaa" / "output.log"
    raw = b"\x1b[31mred\x1b[0m\nplain\n"
    log_path.write_bytes(raw)
    assert agent.main(["log", "aaaaaaaa"]) == agent.EXIT_OK
    out = capsys.readouterr().out
    assert "\x1b" not in out
    assert "red" in out
    assert log_path.read_bytes() == raw


def test_cmd_log_strips_ansi_without_follow(
    state_dir: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Log without --follow presents colored output stripped of escape sequences."""
    make_agent(state_dir, "aaaaaaaa", state_value="succeeded", exit_code=0)
    log_path = agent.agents_dir() / "aaaaaaaa" / "output.log"
    log_path.write_text("\x1b[31mred line\x1b[0m\n\x1b[1m\x1b[38;5;123mbold\x1b[0m\nplain\n")
    assert agent.main(["log", "aaaaaaaa"]) == agent.EXIT_OK
    out = capsys.readouterr().out
    assert "\x1b" not in out
    assert "red line" in out
    assert "bold" in out
    assert "plain" in out


def test_cmd_log_follow_snapshot_strips_ansi(
    state_dir: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Log --follow presents the colored snapshot stripped of escape sequences."""
    make_agent(state_dir, "aaaaaaaa", state_value="succeeded", exit_code=0)
    log_path = agent.agents_dir() / "aaaaaaaa" / "output.log"
    log_path.write_text("\x1b[34mblue\x1b[0m\n\x1b[4munderline\x1b[0m\n")
    assert agent.main(["log", "aaaaaaaa", "--follow", "--lines", "5"]) == agent.EXIT_OK
    out = capsys.readouterr().out
    assert "\x1b" not in out
    assert "blue" in out
    assert "underline" in out


def test_log_normalizer_strips_ansi_across_chunk_boundary(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A CSI sequence split across reads is still fully stripped."""
    normalizer = cast(
        "type[Any]",
        agent.__dict__["_LogNormalizer"],
    )()
    normalizer.write(b"\x1b[3")
    normalizer.write(b"1mgreen\x1b[0")
    normalizer.write(b"m end\n")
    normalizer.close()
    out = capsys.readouterr().out
    assert "\x1b" not in out
    assert out == "green end\n"


def test_log_normalizer_strips_osc_st_split_across_chunks(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """An OSC ST terminator split across reads is still fully stripped."""
    normalizer = cast(
        "type[Any]",
        agent.__dict__["_LogNormalizer"],
    )()
    normalizer.write(b"a\x1b]8;;url\x1b")
    normalizer.write(b"\\done\x1b]8;;\x1b\\end\n")
    normalizer.close()
    out = capsys.readouterr().out
    assert "\x1b" not in out
    assert out == "adoneend\n"


def test_log_normalizer_preserves_utf8_across_chunk_boundary(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Multi-byte UTF-8 text split across reads is preserved undamaged."""
    normalizer = cast(
        "type[Any]",
        agent.__dict__["_LogNormalizer"],
    )()
    normalizer.write(b"caf\xc3\xa9 \xe2")
    normalizer.write(b"\x98\x95 tail")
    normalizer.write(b"\x1b[33myellow\x1b[0m done\n")
    normalizer.close()
    out = capsys.readouterr().out
    assert "café ☕ tail" in out
    assert "\x1b" not in out
    assert "yellow done" in out


def test_cmd_log_preserves_utf8_without_follow(
    state_dir: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Log without --follow preserves ordinary UTF-8 text."""
    make_agent(state_dir, "aaaaaaaa", state_value="succeeded", exit_code=0)
    log_path = agent.agents_dir() / "aaaaaaaa" / "output.log"
    log_path.write_text("\x1b[32mnaïve café ☕\x1b[0m\n")
    assert agent.main(["log", "aaaaaaaa"]) == agent.EXIT_OK
    out = capsys.readouterr().out
    assert "\x1b" not in out
    assert "naïve café ☕" in out


def test_cmd_log_strips_osc_sequences(
    state_dir: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Log strips common OSC sequences, including OSC 8 hyperlinks."""
    make_agent(state_dir, "aaaaaaaa", state_value="succeeded", exit_code=0)
    log_path = agent.agents_dir() / "aaaaaaaa" / "output.log"
    log_path.write_text(
        "\x1b]8;;https://example.com\x07link\x1b]8;;\x07\n"
        "\x1b]8;;https://example.org\x1b\\styled\x1b]8;;\x1b\\\n"
        "\x1b]0;title set\x07after\n"
    )
    assert agent.main(["log", "aaaaaaaa"]) == agent.EXIT_OK
    out = capsys.readouterr().out
    assert "\x1b" not in out
    assert "link" in out
    assert "styled" in out
    assert "after" in out


def test_cmd_log_strips_full_csi_parameter_bytes(
    state_dir: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Log strips CSI sequences with colon/less/equal/greater parameter bytes."""
    make_agent(state_dir, "aaaaaaaa", state_value="succeeded", exit_code=0)
    log_path = agent.agents_dir() / "aaaaaaaa" / "output.log"
    log_path.write_text(
        "\x1b[38:2:255:0:0mcolon params\x1b[0m\n"
        "\x1b[?25lhide cursor\x1b[?25h\n"
        "\x1b[1;2:3mcombined\x1b[0m\n"
    )
    assert agent.main(["log", "aaaaaaaa"]) == agent.EXIT_OK
    out = capsys.readouterr().out
    assert "\x1b" not in out
    assert "colon params" in out
    assert "hide cursor" in out
    assert "combined" in out


def test_cmd_log_does_not_overstrip_unterminated_osc(
    state_dir: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """An unterminated OSC mid-log is treated as text, not over-stripped."""
    make_agent(state_dir, "aaaaaaaa", state_value="succeeded", exit_code=0)
    log_path = agent.agents_dir() / "aaaaaaaa" / "output.log"
    log_path.write_text("first\nnote \x1b]8;;url without terminator\nlast\n")
    assert agent.main(["log", "aaaaaaaa"]) == agent.EXIT_OK
    out = capsys.readouterr().out
    assert "first" in out
    assert "note \x1b]8;;url without terminator" in out
    assert "last" in out


def test_cmd_prompt_attached_normalizes_colored_output(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Attached prompt strips ANSI from streamed output and still extracts the result."""
    agent.write_meta("aaaaaaaa", agent.idle_meta("aaaaaaaa", str(state_dir), None))

    def colored_command(
        _meta: agent.Meta,
        _prompt: str,
        *,
        is_continue: bool,
    ) -> list[str]:
        del is_continue
        return ["sh", "-c", "printf '\\x1b[36m%s\\x1b[0m\\n' \"$LUBKO_PROMPT\""]

    monkeypatch.setattr(agent, "build_agent_command", colored_command)
    runner_threads: list[threading.Thread] = []

    def spawn_in_thread(_aid: str, mode: str) -> None:
        thread = threading.Thread(target=agent.runner, args=(_aid, mode), daemon=True)
        thread.start()
        runner_threads.append(thread)

    monkeypatch.setattr(agent, "spawn_runner", spawn_in_thread)
    code = agent.main(["prompt", "--id", "aaaaaaaa", "do work"])
    assert code == agent.EXIT_OK
    output = capsys.readouterr().out
    assert "\x1b" not in output
    assert "do work" in output
    meta = agent.read_meta("aaaaaaaa")
    assert meta is not None
    assert meta["state"] == "succeeded"
    assert meta["exit_code"] == 0
    for thread in runner_threads:
        thread.join(timeout=10)


def test_cmd_status_tail_shows_folded_long_lines(
    state_dir: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Status tail folds long lines and is not truncated at a byte cap."""
    make_agent(state_dir, "aaaaaaaa", state_value="succeeded", exit_code=0)
    log_path = agent.agents_dir() / "aaaaaaaa" / "output.log"
    logical = [f"{i:02d}" + "w" * agent.FOLD_WIDTH for i in range(40)]
    log_path.write_text("\n".join(logical) + "\n")
    assert agent.main(["status", "aaaaaaaa"]) == agent.EXIT_OK
    out = capsys.readouterr().out
    rows = [line for line in out.splitlines() if line.startswith("| ")]
    assert len(rows) == agent.STATUS_TAIL_LINES
    assert all(len(line) <= agent.FOLD_WIDTH + 6 for line in rows)


def test_proc_cpu_seconds_reads_linux_process_data() -> None:
    """proc_cpu_seconds reads total CPU time from Linux /proc data."""
    cpu = agent.proc_cpu_seconds(os.getpid())
    assert isinstance(cpu, float)
    assert cpu >= 0.0
    assert agent.proc_cpu_seconds(None) is None
    assert agent.proc_cpu_seconds(2**30) is None


def test_fmt_cpu_compacts() -> None:
    """fmt_cpu renders compact CPU time strings."""
    assert agent.fmt_cpu(None) == "-"
    assert agent.fmt_cpu(0.0) == "0.0s"
    assert agent.fmt_cpu(12.34) == "12.3s"
    assert agent.fmt_cpu(61.0) == "1m1s"
    assert agent.fmt_cpu(3600.0) == "1h0m"


def test_cmd_status_reports_cpu_usage(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Status reports total CPU time for a live agent process."""
    proc = spawn_marked_process("aaaaaaaa")
    try:
        agent.write_meta("aaaaaaaa", meta_for_process("aaaaaaaa", proc, "/workspace"))
        assert agent.main(["status", "--id", "aaaaaaaa", "--json"]) == agent.EXIT_OK
        data = json.loads(capsys.readouterr().out)
        assert "cpu_seconds" in data
        assert isinstance(data["cpu_seconds"], (int, float))
        assert data["cpu_seconds"] >= 0
        assert agent.main(["status", "--id", "aaaaaaaa"]) == agent.EXIT_OK
        assert "cpu:" in capsys.readouterr().out
    finally:
        kill_proc(proc)


def test_cmd_status_cpu_is_unknown_without_live_process(
    state_dir: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Status reports no CPU time when no live process exists."""
    make_agent(state_dir, "aaaaaaaa", state_value="succeeded", exit_code=0)
    assert agent.main(["status", "aaaaaaaa", "--json"]) == agent.EXIT_OK
    data = json.loads(capsys.readouterr().out)
    assert data["cpu_seconds"] is None
    assert agent.main(["status", "aaaaaaaa"]) == agent.EXIT_OK
    assert "cpu:        -" in capsys.readouterr().out


def test_cmd_status_hides_cpu_for_reused_pid(
    state_dir: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Status hides CPU time when a stored PID belongs to another process."""
    make_agent(state_dir, "aaaaaaaa", state_value="running", pid=os.getpid())
    meta = agent.read_meta("aaaaaaaa")
    assert meta is not None
    cpu = agent.proc_cpu_seconds(os.getpid())
    assert cpu is not None
    assert cpu > 0
    assert agent.is_alive(meta) is False
    assert agent.main(["status", "aaaaaaaa", "--json"]) == agent.EXIT_OK
    data = json.loads(capsys.readouterr().out)
    assert data["cpu_seconds"] is None
    assert agent.main(["status", "aaaaaaaa"]) == agent.EXIT_OK
    assert "cpu:        -" in capsys.readouterr().out


def test_cmd_new_creates_idle_agent_with_supplied_id(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """New creates only an idle record with the caller-supplied ID."""
    spawned: list[str] = []
    monkeypatch.setattr(agent, "spawn_runner", lambda _aid, mode: spawned.append(mode))
    cwd = str(state_dir)
    code = agent.main(["new", "--id", "A13F09C2", "--cwd", cwd, "--json"])
    assert code == agent.EXIT_OK
    out = json.loads(capsys.readouterr().out)
    assert out["id"] == "a13f09c2"
    assert out["state"] == "idle"
    meta = agent.read_meta("a13f09c2")
    assert meta is not None
    assert meta["state"] == "idle"
    assert meta["cwd"] == cwd
    assert meta["prompt_count"] == 0
    assert "initial_prompt" not in meta
    assert "pending_prompt" not in meta
    assert spawned == []


def test_cmd_new_requires_id(capsys: pytest.CaptureFixture[str]) -> None:
    """New without --id is rejected."""
    assert agent.main(["new"]) == agent.EXIT_USAGE
    assert "--id" in capsys.readouterr().err
    assert agent.agents_dir().exists() is False or list(agent.agents_dir().iterdir()) == []


def test_cmd_new_rejects_malformed_id(capsys: pytest.CaptureFixture[str], tmp_path: Path) -> None:
    """New rejects a malformed caller-supplied ID."""
    code = agent.main(["new", "--id", "zzzz", "--cwd", str(tmp_path)])
    assert code == agent.EXIT_USAGE
    assert "base-16" in capsys.readouterr().err


def test_cmd_new_rejects_collision(state_dir: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """New rejects an ID that already exists rather than reusing it."""
    make_agent(state_dir, "a13f09c2", state_value="idle")
    code = agent.main(["new", "--id", "a13f09c2", "--cwd", str(state_dir)])
    assert code == agent.EXIT_ERROR
    assert "already exists" in capsys.readouterr().err


def test_cmd_new_rejects_missing_cwd(capsys: pytest.CaptureFixture[str]) -> None:
    """New rejects a missing working directory."""
    code = agent.main(["new", "--id", "a13f09c2", "--cwd", "/nonexistent"])
    assert code == agent.EXIT_ERROR
    assert "does not exist" in capsys.readouterr().err


def test_cmd_new_rejects_prompt_and_sync_options() -> None:
    """New accepts no --prompt, positional prompt, --sync, or --detach."""
    for argv in (
        ["new", "--id", "a13f09c2", "--prompt", "hi"],
        ["new", "--id", "a13f09c2", "hi"],
        ["new", "--id", "a13f09c2", "--sync"],
        ["new", "--id", "a13f09c2", "--detach"],
    ):
        with pytest.raises(SystemExit):
            agent.main(argv)


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


def test_cmd_status_accepts_id_flag(state_dir: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """Status --id <ID> is a supported form."""
    make_agent(state_dir, "aaaaaaaa", state_value="succeeded", exit_code=0)
    assert agent.main(["status", "--id", "aaaaaaaa", "--json"]) == agent.EXIT_OK
    data = json.loads(capsys.readouterr().out)
    assert data["id"] == "aaaaaaaa"


def test_cmd_status_unknown_agent(capsys: pytest.CaptureFixture[str]) -> None:
    """Status on an unknown agent returns not-found."""
    assert agent.main(["status", "deadbeef"]) == agent.EXIT_NOT_FOUND
    assert "unknown agent" in capsys.readouterr().err


def test_cmd_prompt_first_prompt_creates_native_session(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The first prompt of a fresh agent starts a new native session.

    New agents are idle; the first prompt creates the underlying session.
    """
    agent.write_meta("aaaaaaaa", agent.idle_meta("aaaaaaaa", str(state_dir), None))
    spawned: list[str] = []
    monkeypatch.setattr(agent, "spawn_runner", lambda _aid, mode: spawned.append(mode))
    code = agent.main(["prompt", "--id", "aaaaaaaa", "--detach", "do work"])
    assert code == agent.EXIT_OK
    assert spawned == ["new"]
    meta = agent.read_meta("aaaaaaaa")
    assert meta is not None
    assert meta["state"] == "running"
    assert meta["pending_prompt"] == "do work"


def test_cmd_prompt_continue_uses_existing_native_session(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Later prompts continue the exact native session."""
    make_agent(
        state_dir,
        "aaaaaaaa",
        state_value="succeeded",
        native_session_id="sess-1",
        prompt_count=1,
    )
    monkeypatch.setattr(agent, "discover_session_id", lambda _aid: "sess-1")
    spawned: list[str] = []
    monkeypatch.setattr(agent, "spawn_runner", lambda _aid, mode: spawned.append(mode))
    code = agent.main(["prompt", "--id", "aaaaaaaa", "--detach", "more"])
    assert code == agent.EXIT_OK
    assert spawned == ["continue"]
    meta = agent.read_meta("aaaaaaaa")
    assert meta is not None
    assert meta["pending_prompt"] == "more"


def test_cmd_prompt_refuses_when_recorded_session_disappeared(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Prompting is refused only when a recorded native session has disappeared."""
    make_agent(
        state_dir,
        "aaaaaaaa",
        state_value="succeeded",
        native_session_id="sess-1",
        prompt_count=1,
    )
    monkeypatch.setattr(agent, "discover_session_id", lambda _aid: None)
    code = agent.main(["prompt", "--id", "aaaaaaaa", "--detach", "more"])
    assert code == agent.EXIT_ERROR
    assert "session is not available" in capsys.readouterr().err


def test_prompt_retries_failed_first_attempt_as_new_session(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed first prompt (no session ever created) may be retried as new."""
    agent.write_meta("aaaaaaaa", agent.idle_meta("aaaaaaaa", str(state_dir), None))
    runner_threads: list[threading.Thread] = []

    def spawn_in_thread(_aid: str, mode: str) -> None:
        thread = threading.Thread(target=agent.runner, args=(_aid, mode), daemon=True)
        thread.start()
        runner_threads.append(thread)

    monkeypatch.setattr(agent, "spawn_runner", spawn_in_thread)

    def failing_command(_meta: agent.Meta, _prompt: str, *, is_continue: bool) -> list[str]:
        del is_continue
        return ["/nonexistent/lubko-binary"]

    monkeypatch.setattr(agent, "build_agent_command", failing_command)
    first_code = agent.main(["prompt", "--id", "aaaaaaaa", "first task"])
    for thread in runner_threads:
        thread.join(timeout=10)
    assert first_code != agent.EXIT_OK
    meta = agent.read_meta("aaaaaaaa")
    assert meta is not None
    assert meta["state"] == "failed"
    assert meta["native_session_id"] is None

    spawned: list[str] = []
    monkeypatch.setattr(agent, "build_agent_command", fake_agent_command)
    monkeypatch.setattr(agent, "spawn_runner", lambda _aid, mode: spawned.append(mode))
    retry_code = agent.main(["prompt", "--id", "aaaaaaaa", "--detach", "retry task"])
    assert retry_code == agent.EXIT_OK
    assert spawned == ["new"]
    meta = agent.read_meta("aaaaaaaa")
    assert meta is not None
    assert meta["state"] == "running"
    assert meta["pending_prompt"] == "retry task"


def test_prompt_continue_with_agent_cmd_override_without_session(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """LUBKO_AGENT_CMD continuation needs no native session and is never refused."""
    make_agent(state_dir, "aaaaaaaa", state_value="succeeded", prompt_count=1)
    monkeypatch.setenv("LUBKO_AGENT_CMD", "opencode run --auto")
    spawned: list[str] = []
    monkeypatch.setattr(agent, "spawn_runner", lambda _aid, mode: spawned.append(mode))
    code = agent.main(["prompt", "--id", "aaaaaaaa", "--detach", "more"])
    assert code == agent.EXIT_OK
    assert spawned == ["new"]
    meta = agent.read_meta("aaaaaaaa")
    assert meta is not None
    assert meta["pending_prompt"] == "more"


def test_cmd_prompt_attached_follows_and_propagates_exit_code(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Attached prompt streams the invocation and propagates its exit status."""
    agent.write_meta("aaaaaaaa", agent.idle_meta("aaaaaaaa", str(state_dir), None))
    monkeypatch.setattr(agent, "build_agent_command", fake_agent_command)
    runner_threads: list[threading.Thread] = []

    def spawn_in_thread(_aid: str, mode: str) -> None:
        thread = threading.Thread(target=agent.runner, args=(_aid, mode), daemon=True)
        thread.start()
        runner_threads.append(thread)

    monkeypatch.setattr(agent, "spawn_runner", spawn_in_thread)
    code = agent.main(["prompt", "--id", "aaaaaaaa", "do work"])
    assert code == agent.EXIT_OK
    output = capsys.readouterr().out
    assert "do work" in output
    meta = agent.read_meta("aaaaaaaa")
    assert meta is not None
    assert meta["state"] == "succeeded"
    for thread in runner_threads:
        thread.join(timeout=10)


def test_cmd_prompt_steer_is_plain_prompt_when_idle(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """--steer on an idle agent is exactly equivalent to an ordinary prompt."""
    agent.write_meta("aaaaaaaa", agent.idle_meta("aaaaaaaa", str(state_dir), None))
    spawned: list[str] = []
    monkeypatch.setattr(agent, "spawn_runner", lambda _aid, mode: spawned.append(mode))
    code = agent.main(["prompt", "--id", "aaaaaaaa", "--steer", "--detach", "task"])
    assert code == agent.EXIT_OK
    assert spawned == ["new"]
    meta = agent.read_meta("aaaaaaaa")
    assert meta is not None
    assert meta["pending_prompt"] == "task"
    assert not meta["steer_queue"]


def test_cmd_prompt_steer_is_plain_prompt_when_finished(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """--steer on a finished agent is exactly equivalent to an ordinary prompt."""
    make_agent(
        state_dir,
        "aaaaaaaa",
        state_value="succeeded",
        native_session_id="sess-1",
        prompt_count=1,
    )
    monkeypatch.setattr(agent, "discover_session_id", lambda _aid: "sess-1")
    spawned: list[str] = []
    monkeypatch.setattr(agent, "spawn_runner", lambda _aid, mode: spawned.append(mode))
    code = agent.main(["prompt", "--id", "aaaaaaaa", "--steer", "--detach", "task"])
    assert code == agent.EXIT_OK
    assert spawned == ["continue"]
    meta = agent.read_meta("aaaaaaaa")
    assert meta is not None
    assert meta["pending_prompt"] == "task"
    assert not meta["steer_queue"]


def test_cmd_prompt_refuses_without_steer_when_busy(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Prompting a busy agent without --steer is refused."""
    proc = spawn_marked_process("aaaaaaaa")
    try:
        agent.write_meta("aaaaaaaa", meta_for_process("aaaaaaaa", proc, "/workspace"))
        monkeypatch.setattr(agent, "spawn_runner", lambda _aid, _mode: None)
        code = agent.main(["prompt", "--id", "aaaaaaaa", "task"])
        assert code == agent.EXIT_ERROR
        assert "--steer" in capsys.readouterr().err
    finally:
        kill_proc(proc)


def test_cmd_prompt_steer_queues_for_busy_agent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Steering a busy agent queues the instruction and interrupts the run."""
    proc = spawn_marked_process("aaaaaaaa")
    try:
        agent.write_meta("aaaaaaaa", meta_for_process("aaaaaaaa", proc, "/workspace"))
        spawned: list[str] = []
        monkeypatch.setattr(agent, "spawn_runner", lambda _aid, mode: spawned.append(mode))
        code = agent.main(["prompt", "--id", "aaaaaaaa", "--steer", "--detach", "redirect"])
        assert code == agent.EXIT_OK
        assert spawned == []
        current = agent.read_meta("aaaaaaaa")
        assert current is not None
        assert current["steer_queue"][0]["prompt"] == "redirect"
        assert current["intent"] == "steer"
    finally:
        kill_proc(proc)


def test_cmd_delete_terminal_agent(state_dir: Path) -> None:
    """Deleting a terminal agent removes its state."""
    make_agent(state_dir, "aaaaaaaa", state_value="succeeded")
    assert agent.main(["delete", "aaaaaaaa"]) == agent.EXIT_OK
    assert not (agent.agents_dir() / "aaaaaaaa").exists()


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
    meta = agent.idle_meta("aaaaaaaa", "/workspace", None)
    assert agent.build_agent_command(meta, "hi", is_continue=False) == [
        "/bin/sh",
        "-c",
        "opencode run --auto",
    ]


def test_build_agent_command_override_used_for_continuation_without_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """LUBKO_AGENT_CMD continuation needs no recorded native session."""
    monkeypatch.setenv("LUBKO_AGENT_CMD", "opencode run --auto")
    meta = agent.idle_meta("aaaaaaaa", "/workspace", None)
    meta["native_session_id"] = None
    cmd = agent.build_agent_command(meta, "hi", is_continue=True)
    assert cmd == ["/bin/sh", "-c", "opencode run --auto"]


def test_build_agent_command_creates_session_on_first_prompt() -> None:
    """The first invocation creates a native session with the agent title."""
    meta = agent.idle_meta("aaaaaaaa", "/workspace", None)
    cmd = agent.build_agent_command(meta, "first task", is_continue=False)
    assert cmd is not None
    assert "--title" in cmd
    assert "lubko-aaaaaaaa" in cmd


def test_exit_code_for_maps_states() -> None:
    """Exit codes follow the agent state."""
    assert agent.exit_code_for(None) == 1
    succeeded = agent.idle_meta("a", "/", None)
    succeeded["state"] = "succeeded"
    assert agent.exit_code_for(succeeded) == 0
    failed = agent.idle_meta("b", "/", None)
    failed["state"] = "failed"
    failed["exit_code"] = FAILURE_EXIT_CODE
    assert agent.exit_code_for(failed) == FAILURE_EXIT_CODE
    failed["exit_code"] = None
    assert agent.exit_code_for(failed) == 1


def test_subcommands_present() -> None:
    """Every documented management command is exposed; removed ones are gone."""
    names = {spec.name for spec in agent.SUBCOMMANDS}
    assert names == REQUIRED_COMMANDS
    assert REMOVED_COMMANDS.isdisjoint(names)


def test_removed_subcommands_are_not_parsed() -> None:
    """Last and result no longer exist as subcommands."""
    for argv in (["last"], ["result", "aaaaaaaa"]):
        with pytest.raises(SystemExit):
            agent.main(argv)


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
    """A prompt queued between read and claim is never lost."""
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
    """A prompt reuses a live runner without spawning a second one."""
    runner_proc = spawn_marked_process("aaaaaaaa")
    try:
        meta = make_agent(
            state_dir,
            "aaaaaaaa",
            state_value="succeeded",
            native_session_id="sess-1",
            prompt_count=1,
        )
        meta["active_runner"] = True
        meta["runner_pid"] = runner_proc.pid
        meta["runner_start_time"] = agent.proc_start_ticks(runner_proc.pid)
        meta["pending_prompt"] = None
        agent.write_meta("aaaaaaaa", meta)
        monkeypatch.setattr(agent, "discover_session_id", lambda _aid: "sess-1")
        spawned: list[str] = []
        monkeypatch.setattr(agent, "spawn_runner", lambda _aid, mode: spawned.append(mode))
        code = agent.main(["prompt", "--id", "aaaaaaaa", "--detach", "more"])
        assert code == agent.EXIT_OK
        assert spawned == []
        current = agent.read_meta("aaaaaaaa")
        assert current is not None
        assert current["pending_prompt"] == "more"
    finally:
        kill_proc(runner_proc)


def test_live_runner_two_ordinary_prompts_exactly_one_accepted(
    state_dir: Path,
    tmp_path: Path,
) -> None:
    """Two ordinary prompts against a live between-invocation runner.

    Both callers observe the old terminal, no-pending state, then both reach
    the locked decision window.  Exactly one prompt is accepted and reused by
    the live runner (no second runner spawned, no overwrite); the loser is
    explicitly busy.
    """
    aid = "22222222"
    runner_proc = spawn_marked_process(aid)
    try:
        meta = make_agent(
            state_dir,
            aid,
            state_value="succeeded",
            prompt_count=1,
        )
        meta["active_runner"] = True
        meta["runner_pid"] = runner_proc.pid
        meta["runner_start_time"] = agent.proc_start_ticks(runner_proc.pid)
        meta["pending_prompt"] = None
        agent.write_meta(aid, meta)
        sync = tmp_path / "sync"
        p1 = _run_cli(
            ["prompt", "--id", aid, "--detach", "first"], sync_dir=sync, LUBKO_AGENT_CMD="sleep 300"
        )
        p2 = _run_cli(
            ["prompt", "--id", aid, "--detach", "second"],
            sync_dir=sync,
            LUBKO_AGENT_CMD="sleep 300",
        )
        try:
            _wait_sync_reached(sync, "sc_observe", 2)
            _release_sync(sync, "sc_observe")
            _wait_sync_reached(sync, "sc_decide", 2)
            _release_sync(sync, "sc_decide")
            rc1 = p1.wait(timeout=30)
            rc2 = p2.wait(timeout=30)
            current = agent.read_meta(aid)
            assert current is not None
            # No runner was spawned: the single live runner is reused, so the
            # generation and reservation are untouched and no second identity
            # exists.  Exactly one prompt owns the runner.
            assert current["runner_gen"] == 0
            assert current["runner_reservation"] is None
            assert current["runner_pid"] == runner_proc.pid
            assert current["active_runner"] is True
            assert current["pending_prompt"] in {"first", "second"}
            assert current["prompt_count"] == 1
            # The loser is explicitly busy; exactly one call succeeded.
            assert sorted([rc1, rc2]) == [agent.EXIT_OK, agent.EXIT_ERROR]
        finally:
            _teardown_runners(aid, sync)
    finally:
        kill_proc(runner_proc)


def test_reservation_owner_exact_process_safe_against_pid_reuse(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A reused PID with different start ticks never justifies a reservation.

    The original spawner dies and its PID is reused by an unrelated process.
    Because the reservation records the owner's start ticks, the live reused
    PID does not justify the reservation, so deterministic recovery is allowed
    and produces exactly one replacement runner.
    """
    aid = "11111111"
    reused = spawn_marked_process(aid)
    try:
        real_ticks = agent.proc_start_ticks(reused.pid)
        meta = agent.idle_meta(aid, str(state_dir), None)
        meta["state"] = "running"
        meta["active_runner"] = True
        meta["started_at"] = time.time()
        meta["runner_gen"] = 1
        # The spawner died; its PID was reused by an unrelated process whose
        # start ticks differ from the recorded owner identity.
        meta["runner_reservation"] = {
            "gen": 1,
            "owner_pid": reused.pid,
            "owner_start_ticks": (real_ticks or 0) + 1,
            "state": "reserved",
            "reserved_at": time.time(),
            "mode": "new",
        }
        agent.write_meta(aid, meta)
        # A live reused PID with mismatched start ticks must NOT justify the
        # reservation.
        assert agent.reservation_in_flight(meta) is False
        assert agent.is_genuinely_running(meta) is False
        # Recovery is allowed and produces exactly one replacement runner.
        spawned: list[str] = []
        monkeypatch.setattr(agent, "spawn_runner", lambda _aid, mode: spawned.append(mode))
        rc = agent.main(["prompt", "--id", aid, "--detach", "recover"])
        assert rc == agent.EXIT_OK
        assert spawned == ["new"]
        after = agent.read_meta(aid)
        assert after is not None
        assert after["runner_gen"] == 2
        assert isinstance(after["runner_reservation"], dict)
        assert after["runner_reservation"]["gen"] == 2
        # The replacement reservation is owned by this (alive, matching) process,
        # so it is genuinely in flight.
        assert agent.reservation_in_flight(after) is True
    finally:
        kill_proc(reused)


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


# ---------------------------------------------------------------------------
# Linearizable prompt/runner reservation (issue #77)
# ---------------------------------------------------------------------------


def _cli_env(sync_dir: Path | None, **extra: str) -> dict[str, str]:
    """Build an environment for a CLI subprocess under test.

    Args:
        sync_dir: Synchronization directory for deterministic tests, or
            ``None`` to disable the test hook.
        **extra: Extra environment variables.

    Returns:
        The subprocess environment.
    """
    env = dict(os.environ)
    if sync_dir is not None:
        env["LUBKO_TEST_SYNC"] = str(sync_dir)
    env.update(extra)
    return env


def _run_cli(
    args: list[str], sync_dir: Path | None = None, **extra: str
) -> subprocess.Popen[bytes]:
    """Launch ``lubko-agent`` as a real subprocess for a deterministic test.

    Args:
        args: CLI arguments (without the program name).
        sync_dir: Synchronization directory, or ``None`` to disable the hook.
        **extra: Extra environment variables for the subprocess.

    Returns:
        The launched subprocess.
    """
    env = _cli_env(sync_dir, **extra)
    return subprocess.Popen(
        [sys.executable, "-m", "lubko.agent", *args],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )


def _release_sync(sync_dir: Path, step: str) -> None:
    """Release every process paused at a named synchronization point.

    Args:
        sync_dir: Synchronization directory.
        step: Named synchronization point.
    """
    (sync_dir / f"{step}.release").touch()


def _wait_sync_reached(sync_dir: Path, step: str, count: int, timeout: float = 30.0) -> None:
    """Wait until ``count`` processes have reached a named sync point.

    Args:
        sync_dir: Synchronization directory.
        step: Named synchronization point.
        count: Number of reaching processes expected.
        timeout: Maximum seconds to wait.

    Raises:
        AssertionError: If the expected count is not reached in time.
    """
    deadline = time.monotonic() + timeout
    reached = 0
    while time.monotonic() < deadline:
        reached = len(list(sync_dir.glob(f"{step}.*.reached")))
        if reached >= count:
            return
        time.sleep(0.005)
    msg = f"sync point {step!r} reached by only {reached}/{count} processes"
    raise AssertionError(msg)


def _runner_claimed(aid: str) -> bool:
    """Return whether the agent's runner has claimed and recorded its PID.

    Args:
        aid: Exact agent ID.

    Returns:
        ``True`` once ``runner_pid`` is present in meta.
    """
    meta = agent.read_meta(aid)
    return bool(meta and meta.get("runner_pid"))


def _runner_dead(pid: int) -> bool:
    """Return whether an exact runner PID is no longer alive.

    Args:
        pid: The exact runner process ID.

    Returns:
        ``True`` once the process has exited.
    """
    return not agent.pid_alive(pid)


def _reached_pids(sync_dir: Path, step: str) -> list[int]:
    """Return PIDs that reached a named sync point, recovered from markers.

    The sync hook names each reached marker ``{step}.{pid}.reached``, so the
    exact process identity that reached the point is recovered from the
    marker file alone, without any ``/proc`` scan by agent ID or marker.

    Args:
        sync_dir: Synchronization directory.
        step: Named synchronization point.

    Returns:
        The reaching process IDs.
    """
    pids: list[int] = []
    for path in sync_dir.glob(f"{step}.*.reached"):
        stem = path.name[len(f"{step}.") : -len(".reached")]
        with contextlib.suppress(ValueError):
            pids.append(int(stem))
    return pids


def _kill_runner(pid: int) -> None:
    """Kill a runner process and its whole process group deterministically.

    The PID is an exact identity the test itself created (a runner this test
    spawned, or a PID a runner registered in its sync marker), never a process
    discovered by agent ID or environment marker.

    Args:
        pid: The runner process ID (a session leader).
    """
    with contextlib.suppress(ProcessLookupError, PermissionError):
        os.killpg(pid, signal.SIGKILL)
    with contextlib.suppress(ProcessLookupError, PermissionError):
        os.kill(pid, signal.SIGKILL)


def _teardown_runners(aid: str, sync_dir: Path | None = None) -> None:
    """Stop exactly the runner(s) this test created, by exact identity only.

    No ``/proc`` scan by agent ID, marker, command text, or process name is
    performed. Two exact sources are consulted:

    * the runner PID the protocol registered in meta, verified against the
      exact ``runner_start_time`` so a reused PID naming an unrelated process
      is never signalled; and
    * the exact PID a runner wrote into this test's unique ``runner_preclaim``
      reached marker, used only for a runner killed before it could claim.

    Unknown leftovers from older failed runs are observations only and are
    never signalled.

    Args:
        aid: Exact agent ID whose registered runner to stop.
        sync_dir: Synchronization directory, or ``None`` when no sync hook was
            used (only the meta-registered runner is then considered).
    """
    meta = agent.read_meta(aid)
    if meta is not None:
        pid = meta.get("runner_pid")
        if pid:
            pid = int(pid)
            expected = meta.get("runner_start_time")
            if expected is None or agent.proc_start_ticks(pid) == int(expected):
                _kill_runner(pid)
    if sync_dir is not None:
        for reached in _reached_pids(sync_dir, "runner_preclaim"):
            _kill_runner(reached)


def _assert_exact_runner(pid: int, aid: str, gen: int) -> None:
    """Assert an exact PID is the live runner for an exact generation.

    Only the single known ``/proc/<pid>`` entry is read (never a scan): it must
    advertise the exact agent and generation markers, proving the runner this
    test reserved is the one actually executing and that no second runner
    exists for this generation.

    Args:
        pid: The exact runner process ID.
        aid: Exact agent ID that must be proven.
        gen: Runner reservation generation that must be proven.
    """
    environ = Path(f"/proc/{pid}/environ").read_bytes()
    fields = environ.split(b"\0")
    assert f"LUBKO_AGENT_ID={aid}".encode() in fields
    assert f"LUBKO_RUNNER_GEN={gen}".encode() in fields


def spawn_stale_marked_process(aid: str, gen: int) -> subprocess.Popen[bytes]:
    """Spawn a long-lived process carrying both the agent and generation marker.

    Used to simulate an old-generation runner that is still alive but will
    never claim the current reservation.

    Args:
        aid: Agent ID to place in the process environment.
        gen: Runner generation marker to advertise.

    Returns:
        The spawned process.
    """
    env = dict(os.environ)
    env["LUBKO_AGENT_ID"] = aid
    env["LUBKO_RUNNER_GEN"] = str(gen)
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


def test_linearizable_two_concurrent_prompts_exactly_one_runner(
    state_dir: Path,
    tmp_path: Path,
) -> None:
    """Two concurrent ordinary prompts reserve exactly one runner.

    Both callers observe the idle agent, then both reach the runner-decision
    window under the lock. The first reserves one runner generation and the
    second observes the reserved invocation and is rejected (busy) instead of
    overwriting the pending prompt or spawning a second runner.
    """
    aid = "bbbbbbbb"
    agent.write_meta(aid, agent.idle_meta(aid, str(state_dir), None))
    sync = tmp_path / "sync"
    p1 = _run_cli(
        ["prompt", "--id", aid, "--detach", "first"], sync_dir=sync, LUBKO_AGENT_CMD="sleep 300"
    )
    p2 = _run_cli(
        ["prompt", "--id", aid, "--detach", "second"], sync_dir=sync, LUBKO_AGENT_CMD="sleep 300"
    )
    try:
        _wait_sync_reached(sync, "sc_observe", 2)
        _release_sync(sync, "sc_observe")
        _wait_sync_reached(sync, "sc_decide", 2)
        _release_sync(sync, "sc_decide")
        _wait_sync_reached(sync, "runner_preclaim", 1)
        _release_sync(sync, "runner_preclaim")
        rc1 = p1.wait(timeout=30)
        rc2 = p2.wait(timeout=30)
        wait_until(functools.partial(_runner_claimed, aid), timeout=10)
        meta = agent.read_meta(aid)
        assert meta is not None
        # Exactly one runner generation owns the agent.
        assert meta["runner_gen"] == 1
        runner_pid = int(meta["runner_pid"])
        assert agent.pid_alive(runner_pid)
        _assert_exact_runner(runner_pid, aid, 1)
        # No prompt was silently overwritten or executed twice.
        assert meta["prompt_count"] == 1
        assert meta["last_prompt"] in {"first", "second"}
        # The losing ordinary prompt gets the documented busy outcome.
        assert sorted([rc1, rc2]) == [agent.EXIT_OK, agent.EXIT_ERROR]
    finally:
        _teardown_runners(aid, sync)


def test_linearizable_prompt_and_steer_exactly_one_runner(
    state_dir: Path,
    tmp_path: Path,
) -> None:
    """A concurrent prompt plus steer reserve exactly one runner.

    The steer is enqueued deterministically and never spawns a competing
    runner; the single reserved runner owns the invocation stream.
    """
    aid = "cccccccc"
    agent.write_meta(aid, agent.idle_meta(aid, str(state_dir), None))
    sync = tmp_path / "sync"
    p_prompt = _run_cli(
        ["prompt", "--id", aid, "--detach", "primary"], sync_dir=sync, LUBKO_AGENT_CMD="sleep 300"
    )
    p_steer = _run_cli(
        ["prompt", "--id", aid, "--steer", "--detach", "redirect"],
        sync_dir=sync,
        LUBKO_AGENT_CMD="sleep 300",
    )
    try:
        _wait_sync_reached(sync, "sc_observe", 2)
        _release_sync(sync, "sc_observe")
        _wait_sync_reached(sync, "sc_decide", 2)
        _release_sync(sync, "sc_decide")
        _wait_sync_reached(sync, "runner_preclaim", 1)
        _release_sync(sync, "runner_preclaim")
        p_prompt.wait(timeout=30)
        p_steer.wait(timeout=30)
        wait_until(functools.partial(_runner_claimed, aid), timeout=10)
        meta = agent.read_meta(aid)
        assert meta is not None
        assert meta["runner_gen"] == 1
        runner_pid = int(meta["runner_pid"])
        assert agent.pid_alive(runner_pid)
        _assert_exact_runner(runner_pid, aid, 1)
        assert meta["prompt_count"] == 1
        # Exactly one runner; the steer instruction is deterministically
        # recorded (as the pending prompt or queued) and never a second runner.
        # ``pending_prompt`` is consumed by the runner once it starts, so the
        # durable ``last_prompt`` proves which prompt was reserved.
        steer_recorded = meta["last_prompt"] == "redirect" or any(
            q["prompt"] == "redirect" for q in meta["steer_queue"]
        )
        assert steer_recorded
    finally:
        _teardown_runners(aid, sync)


def test_linearizable_two_concurrent_steers_exactly_one_runner(
    state_dir: Path,
    tmp_path: Path,
) -> None:
    """Two concurrent steers on an idle agent reserve exactly one runner.

    One steer becomes the pending invocation and the other is enqueued in
    FIFO order; no competing runner is spawned.
    """
    aid = "dddddddd"
    agent.write_meta(aid, agent.idle_meta(aid, str(state_dir), None))
    sync = tmp_path / "sync"
    s1 = _run_cli(
        ["prompt", "--id", aid, "--steer", "--detach", "steer-one"],
        sync_dir=sync,
        LUBKO_AGENT_CMD="sleep 300",
    )
    s2 = _run_cli(
        ["prompt", "--id", aid, "--steer", "--detach", "steer-two"],
        sync_dir=sync,
        LUBKO_AGENT_CMD="sleep 300",
    )
    try:
        _wait_sync_reached(sync, "sc_observe", 2)
        _release_sync(sync, "sc_observe")
        _wait_sync_reached(sync, "sc_decide", 2)
        _release_sync(sync, "sc_decide")
        _wait_sync_reached(sync, "runner_preclaim", 1)
        _release_sync(sync, "runner_preclaim")
        s1.wait(timeout=30)
        s2.wait(timeout=30)
        wait_until(functools.partial(_runner_claimed, aid), timeout=10)
        meta = agent.read_meta(aid)
        assert meta is not None
        assert meta["runner_gen"] == 1
        runner_pid = int(meta["runner_pid"])
        assert agent.pid_alive(runner_pid)
        _assert_exact_runner(runner_pid, aid, 1)
        assert meta["prompt_count"] == 1
        prompts = [meta["last_prompt"]] + [q["prompt"] for q in meta["steer_queue"]]
        assert sorted(prompts) == ["steer-one", "steer-two"]
    finally:
        _teardown_runners(aid, sync)


def test_reserved_runner_death_before_pid_recovery_single_replacement(
    state_dir: Path,
    tmp_path: Path,
) -> None:
    """Killing the reserved runner before PID registration recovers once.

    The first reserved runner is killed before it records its identity. A
    later caller proves the reservation stale and takes over with exactly one
    replacement runner; a concurrent second caller is rejected rather than
    spawning a second replacement.
    """
    aid = "eeeeeeee"
    agent.write_meta(aid, agent.idle_meta(aid, str(state_dir), None))
    sync = tmp_path / "sync"
    first = _run_cli(
        ["prompt", "--id", aid, "--detach", "doomed"], sync_dir=sync, LUBKO_AGENT_CMD="sleep 300"
    )
    try:
        _wait_sync_reached(sync, "sc_observe", 1)
        _release_sync(sync, "sc_observe")
        _wait_sync_reached(sync, "sc_decide", 1)
        _release_sync(sync, "sc_decide")
        _wait_sync_reached(sync, "runner_preclaim", 1)
        # The exact reserved runner PID is recovered from its own sync marker,
        # not by scanning /proc for the agent.
        doomed = _reached_pids(sync, "runner_preclaim")
        assert doomed, "reserved runner must exist before PID registration"
        _kill_runner(doomed[0])
        _release_sync(sync, "runner_preclaim")
        first.wait(timeout=30)
        # Reservation is now stale: trigger recovery with two concurrent callers.
        r1 = _run_cli(
            ["prompt", "--id", aid, "--detach", "recover-one"], LUBKO_AGENT_CMD="sleep 300"
        )
        r2 = _run_cli(
            ["prompt", "--id", aid, "--detach", "recover-two"], LUBKO_AGENT_CMD="sleep 300"
        )
        rc1 = r1.wait(timeout=30)
        rc2 = r2.wait(timeout=30)
        wait_until(functools.partial(_runner_claimed, aid), timeout=10)
        meta = agent.read_meta(aid)
        assert meta is not None
        # Exactly one replacement runner, one generation past the stale one.
        assert meta["runner_gen"] == 2
        runner_pid = int(meta["runner_pid"])
        assert agent.pid_alive(runner_pid)
        _assert_exact_runner(runner_pid, aid, 2)
        # The doomed gen-1 runner is gone; only the gen-2 replacement exists.
        assert not agent.pid_alive(doomed[0])
        # At most one replacement: exactly one caller succeeded.
        assert sorted([rc1, rc2]) == [agent.EXIT_OK, agent.EXIT_ERROR]
    finally:
        _teardown_runners(aid)


def test_linearizable_stress_no_runner_accumulation(
    state_dir: Path,
    tmp_path: Path,
) -> None:
    """Repeated concurrent prompts never accumulate runner processes.

    Every iteration reserves exactly one runner and the losing prompt is
    rejected; after each iteration no runner process for that agent remains.
    """
    for i in range(8):
        aid = f"{i:08x}"
        agent.write_meta(aid, agent.idle_meta(aid, str(state_dir), None))
        sync = tmp_path / f"sync{i}"
        a = _run_cli(
            ["prompt", "--id", aid, "--detach", "A"], sync_dir=sync, LUBKO_AGENT_CMD="sleep 300"
        )
        b = _run_cli(
            ["prompt", "--id", aid, "--detach", "B"], sync_dir=sync, LUBKO_AGENT_CMD="sleep 300"
        )
        try:
            _wait_sync_reached(sync, "sc_observe", 2)
            _release_sync(sync, "sc_observe")
            _wait_sync_reached(sync, "sc_decide", 2)
            _release_sync(sync, "sc_decide")
            _wait_sync_reached(sync, "runner_preclaim", 1)
            _release_sync(sync, "runner_preclaim")
            a.wait(timeout=30)
            b.wait(timeout=30)
            wait_until(functools.partial(_runner_claimed, aid), timeout=10)
            meta = agent.read_meta(aid)
            assert meta is not None
            assert meta["runner_gen"] == 1, f"iteration {i}: unexpected generation"
            runner_pid = int(meta["runner_pid"])
            assert agent.pid_alive(runner_pid), f"iteration {i}: runner not alive"
            _assert_exact_runner(runner_pid, aid, 1)
            assert meta["prompt_count"] == 1
            _kill_runner(runner_pid)
            wait_until(functools.partial(_runner_dead, runner_pid), timeout=10)
        finally:
            _teardown_runners(aid, sync)


def test_stale_old_generation_marker_does_not_block_recovery(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An old-generation live marker must not justify a newer reservation.

    A live process advertising an older runner generation for the same agent
    proves nothing about the current reservation; recovery of the stale newer
    reservation must proceed and must not be blocked by that process.
    """
    aid = "ffffffff"
    stale = spawn_stale_marked_process(aid, gen=1)
    try:
        meta = agent.idle_meta(aid, str(state_dir), None)
        meta["state"] = "running"
        meta["active_runner"] = True
        meta["started_at"] = time.time()
        meta["runner_gen"] = 2
        meta["runner_reservation"] = {
            "gen": 2,
            "owner_pid": 999999,
            "state": "reserved",
            "reserved_at": time.time(),
            "mode": "new",
        }
        agent.write_meta(aid, meta)
        # The gen-1 marker must not justify the gen-2 reservation.
        assert agent.reservation_in_flight(meta) is False
        assert agent.is_genuinely_running(meta) is False
        # Recovery proceeds and is not blocked by the stale process.
        spawned: list[str] = []
        monkeypatch.setattr(agent, "spawn_runner", lambda _aid, mode: spawned.append(mode))
        rc = agent.main(["prompt", "--id", aid, "--detach", "recover"])
        assert rc == agent.EXIT_OK
        assert spawned == ["new"]
        after = agent.read_meta(aid)
        assert after is not None
        assert after["runner_gen"] == 3
        assert isinstance(after["runner_reservation"], dict)
        assert after["runner_reservation"]["gen"] == 3
        # The stale gen-1 process is still alive: recovery was not blocked.
        assert stale.poll() is None
    finally:
        kill_proc(stale)


def test_active_runner_requires_live_identity_or_reservation(tmp_path: Path) -> None:
    """``active_runner`` is only justified by a live identity or reservation.

    A stale state that leaves ``active_runner`` true without either a proven
    live runner or an explicit recoverable reservation is never justified.
    """
    aid = "aaaaaaaa"
    live = spawn_marked_process(aid)
    try:
        justified = agent.idle_meta(aid, str(tmp_path), None)
        justified["state"] = "running"
        justified["active_runner"] = True
        justified["runner_pid"] = live.pid
        justified["runner_start_time"] = agent.proc_start_ticks(live.pid)
        assert agent.active_runner_justified(justified) is True

        no_identity = agent.idle_meta(aid, str(tmp_path), None)
        no_identity["active_runner"] = True
        assert agent.active_runner_justified(no_identity) is False

        stale_reservation = agent.idle_meta(aid, str(tmp_path), None)
        stale_reservation["active_runner"] = True
        stale_reservation["runner_reservation"] = {
            "gen": 1,
            "owner_pid": 999999,
            "state": "reserved",
            "reserved_at": 0.0,
            "mode": "new",
        }
        # A reserved reservation is explicitly recoverable, so it justifies
        # ``active_runner`` even though its spawner is already dead.
        assert agent.active_runner_justified(stale_reservation) is True

        claimed_dead = agent.idle_meta(aid, str(tmp_path), None)
        claimed_dead["active_runner"] = True
        claimed_dead["runner_reservation"] = {
            "gen": 1,
            "owner_pid": 999999,
            "state": "claimed",
            "reserved_at": 0.0,
            "mode": "new",
        }
        # A claimed reservation whose runner is no longer provably alive is
        # stuck and must never justify ``active_runner``.
        assert agent.active_runner_justified(claimed_dead) is False

        live_reservation = agent.idle_meta(aid, str(tmp_path), None)
        live_reservation["active_runner"] = True
        live_reservation["runner_reservation"] = {
            "gen": 1,
            "owner_pid": os.getpid(),
            "state": "reserved",
            "reserved_at": 0.0,
            "mode": "new",
        }
        assert agent.active_runner_justified(live_reservation) is True

        inactive = agent.idle_meta(aid, str(tmp_path), None)
        inactive["active_runner"] = False
        assert agent.active_runner_justified(inactive) is True
    finally:
        kill_proc(live)
