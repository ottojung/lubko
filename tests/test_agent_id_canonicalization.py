"""General regression coverage for issue #777: case-insensitive agent IDs.

Proves that:
- Mixed/upper/lower spellings resolve to the same canonical agent on every
  ID-taking subcommand.
- The parser rejects positional IDs and accepts ``--id`` uniformly.
- Malformed IDs are still rejected fail-closed.
"""

from __future__ import annotations

import argparse
from typing import TYPE_CHECKING

import pytest

from lubko import agent

if TYPE_CHECKING:
    from pathlib import Path

# ---------------------------------------------------------------------------
# normalize_agent_id canonicalization
# ---------------------------------------------------------------------------

_CANONICAL = "a11ce"
_VARIANTS = ("a11ce", "A11CE", "a11Ce", "A11cE", "a11CE")


def _idle_meta(aid: str = _CANONICAL) -> agent.Meta:
    return {"id": aid, "state": "idle", "cwd": "/test", "created_at": 0.0}


def _running_meta(aid: str = _CANONICAL) -> agent.Meta:
    return {
        "id": aid,
        "state": "running",
        "pending_prompt": None,
        "runner_reservation": None,
    }


def test_normalize_lowercase_passthrough() -> None:
    """Lowercase hex passes through unchanged."""
    assert agent.normalize_agent_id("a1b2c3") == "a1b2c3"


def test_normalize_uppercase_is_lowercased() -> None:
    """Uppercase hex is canonicalized to lowercase."""
    assert agent.normalize_agent_id("A1B2C3") == "a1b2c3"


def test_normalize_mixed_case_is_lowercased() -> None:
    """Mixed-case hex is canonicalized to lowercase."""
    assert agent.normalize_agent_id("aBcD1234") == "abcd1234"


def test_normalize_strips_surrounding_whitespace() -> None:
    """Surrounding whitespace is stripped before canonicalization."""
    assert agent.normalize_agent_id("  a1b2  ") == "a1b2"


def test_normalize_empty_string_returns_none() -> None:
    """Empty string is malformed."""
    assert agent.normalize_agent_id("") is None


def test_normalize_none_returns_none() -> None:
    """None input is malformed."""
    assert agent.normalize_agent_id(None) is None


def test_normalize_non_hex_returns_none() -> None:
    """Non-hex characters are rejected."""
    assert agent.normalize_agent_id("xyz") is None


def test_normalize_non_string_returns_none() -> None:
    """Non-string input is rejected."""
    assert agent.normalize_agent_id(123) is None  # type: ignore[arg-type]


def test_normalize_bool_returns_none() -> None:
    """Boolean input is rejected."""
    val: object = True
    assert agent.normalize_agent_id(val) is None  # type: ignore[arg-type]


def test_normalize_all_uppercase_hex_is_lowercased() -> None:
    """All-uppercase hex is canonicalized."""
    assert agent.normalize_agent_id("DEADBEEF") == "deadbeef"


def test_normalize_all_lowercase_hex_passthrough() -> None:
    """All-lowercase hex passes through."""
    assert agent.normalize_agent_id("deadbeef") == "deadbeef"


# ---------------------------------------------------------------------------
# Case-insensitive agent resolution across all ID-taking commands
# ---------------------------------------------------------------------------


def test_new_canonicalizes_id(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Creating with uppercase stores the lowercase canonical form."""
    monkeypatch.setattr(agent, "agents_dir", lambda: tmp_path)
    args = argparse.Namespace(id="A11CE", cwd=str(tmp_path), title=None, json=False)
    code = agent.cmd_new(args)
    assert code == agent.EXIT_OK
    stored = agent.read_meta("a11ce")
    assert stored is not None
    assert stored["id"] == "a11ce"


def test_status_resolves_uppercase(monkeypatch: pytest.MonkeyPatch) -> None:
    """Status resolves mixed-case spellings to the canonical lowercase ID."""
    meta = _idle_meta(_CANONICAL)
    resolved_ids: list[str] = []

    def fake_read_meta(aid: str) -> agent.Meta | None:
        resolved_ids.append(aid)
        return meta if aid == _CANONICAL else None

    monkeypatch.setattr(agent, "read_meta", fake_read_meta)
    monkeypatch.setattr(agent, "reconcile_meta", lambda _aid: None)
    monkeypatch.setattr(agent, "derive_state", lambda _m: "idle")
    monkeypatch.setattr(agent, "is_alive", lambda _m: False)
    monkeypatch.setattr(agent, "log_excerpt", lambda _p, _n: [])

    for variant in _VARIANTS:
        resolved_ids.clear()
        args = argparse.Namespace(id=variant, json=False)
        code = agent.cmd_status(args)
        assert code == agent.EXIT_OK, f"status failed for variant {variant!r}"
        assert resolved_ids == [_CANONICAL], (
            f"status resolved to {resolved_ids!r} for input {variant!r}"
        )


def test_log_resolves_uppercase(monkeypatch: pytest.MonkeyPatch) -> None:
    """Log resolves mixed-case spellings to the canonical lowercase ID."""
    meta = _idle_meta(_CANONICAL)
    resolved_ids: list[str] = []

    def fake_read_meta(aid: str) -> agent.Meta | None:
        resolved_ids.append(aid)
        return meta if aid == _CANONICAL else None

    monkeypatch.setattr(agent, "read_meta", fake_read_meta)

    for variant in _VARIANTS:
        resolved_ids.clear()
        args = argparse.Namespace(id=variant, lines=10, follow=False)
        agent.cmd_log(args)
        assert resolved_ids == [_CANONICAL], (
            f"log resolved to {resolved_ids!r} for input {variant!r}"
        )


def test_stop_resolves_uppercase(monkeypatch: pytest.MonkeyPatch) -> None:
    """Stop resolves mixed-case spellings to the canonical lowercase ID."""
    meta = _running_meta(_CANONICAL)
    resolved_ids: list[str] = []

    def fake_read_meta(aid: str) -> agent.Meta | None:
        resolved_ids.append(aid)
        return meta if aid == _CANONICAL else None

    monkeypatch.setattr(agent, "read_meta", fake_read_meta)
    monkeypatch.setattr(agent, "is_alive", lambda _m: False)
    monkeypatch.setattr(agent, "group_alive", lambda _m: False)

    for variant in _VARIANTS:
        resolved_ids.clear()
        args = argparse.Namespace(id=variant)
        code = agent.cmd_stop(args)
        assert code in {agent.EXIT_OK, agent.EXIT_ERROR}
        assert resolved_ids == [_CANONICAL], (
            f"stop resolved to {resolved_ids!r} for input {variant!r}"
        )


def test_kill_resolves_uppercase(monkeypatch: pytest.MonkeyPatch) -> None:
    """Kill resolves mixed-case spellings to the canonical lowercase ID."""
    meta = _running_meta(_CANONICAL)
    resolved_ids: list[str] = []

    def fake_read_meta(aid: str) -> agent.Meta | None:
        resolved_ids.append(aid)
        return meta if aid == _CANONICAL else None

    monkeypatch.setattr(agent, "read_meta", fake_read_meta)
    monkeypatch.setattr(agent, "is_alive", lambda _m: False)
    monkeypatch.setattr(agent, "group_alive", lambda _m: False)

    for variant in _VARIANTS:
        resolved_ids.clear()
        args = argparse.Namespace(id=variant)
        code = agent.cmd_kill(args)
        assert code in {agent.EXIT_OK, agent.EXIT_ERROR}
        assert resolved_ids == [_CANONICAL], (
            f"kill resolved to {resolved_ids!r} for input {variant!r}"
        )


def test_delete_resolves_uppercase(monkeypatch: pytest.MonkeyPatch) -> None:
    """Delete resolves mixed-case spellings to the canonical lowercase ID."""
    meta = _idle_meta(_CANONICAL)
    resolved_ids: list[str] = []

    def fake_read_meta(aid: str) -> agent.Meta | None:
        resolved_ids.append(aid)
        return meta if aid == _CANONICAL else None

    monkeypatch.setattr(agent, "read_meta", fake_read_meta)
    monkeypatch.setattr(agent, "is_alive", lambda _m: False)
    monkeypatch.setattr(agent, "group_alive", lambda _m: False)
    monkeypatch.setattr(agent, "runner_alive", lambda _m: False)
    monkeypatch.setattr(agent, "reservation_in_flight", lambda _m: False)

    for variant in _VARIANTS:
        resolved_ids.clear()
        args = argparse.Namespace(id=variant, force=False)
        code = agent.cmd_delete(args)
        assert code in {agent.EXIT_OK, agent.EXIT_ERROR}
        assert resolved_ids == [_CANONICAL], (
            f"delete resolved to {resolved_ids!r} for input {variant!r}"
        )


# ---------------------------------------------------------------------------
# Exit code semantics: EXIT_USAGE vs EXIT_NOT_FOUND
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("bad", ["", "xyz", "NOT-HEX", "12 34"])
def test_malformed_id_returns_exit_usage(bad: str) -> None:
    """A non-hex ID triggers EXIT_USAGE (not EXIT_NOT_FOUND)."""
    args = argparse.Namespace(id=bad, json=False)
    code = agent.cmd_status(args)
    assert code == agent.EXIT_USAGE, f"Expected EXIT_USAGE for {bad!r}, got {code}"


def test_unknown_agent_returns_exit_not_found() -> None:
    """A valid hex ID that does not exist triggers EXIT_NOT_FOUND."""
    args = argparse.Namespace(id="deadbeef", json=False)
    code = agent.cmd_status(args)
    assert code == agent.EXIT_NOT_FOUND


def test_stop_malformed_returns_exit_usage() -> None:
    """Stop rejects malformed IDs with EXIT_USAGE."""
    args = argparse.Namespace(id="not-hex")
    assert agent.cmd_stop(args) == agent.EXIT_USAGE


def test_stop_unknown_returns_exit_not_found() -> None:
    """Stop returns EXIT_NOT_FOUND for unknown valid hex IDs."""
    args = argparse.Namespace(id="deadbeef")
    assert agent.cmd_stop(args) == agent.EXIT_NOT_FOUND


def test_kill_malformed_returns_exit_usage() -> None:
    """Kill rejects malformed IDs with EXIT_USAGE."""
    args = argparse.Namespace(id="")
    assert agent.cmd_kill(args) == agent.EXIT_USAGE


def test_kill_unknown_returns_exit_not_found() -> None:
    """Kill returns EXIT_NOT_FOUND for unknown valid hex IDs."""
    args = argparse.Namespace(id="deadbeef")
    assert agent.cmd_kill(args) == agent.EXIT_NOT_FOUND


def test_delete_malformed_returns_exit_usage() -> None:
    """Delete rejects malformed IDs with EXIT_USAGE."""
    args = argparse.Namespace(id="zzz", force=False)
    assert agent.cmd_delete(args) == agent.EXIT_USAGE


def test_delete_unknown_returns_exit_not_found() -> None:
    """Delete returns EXIT_NOT_FOUND for unknown valid hex IDs."""
    args = argparse.Namespace(id="deadbeef", force=False)
    assert agent.cmd_delete(args) == agent.EXIT_NOT_FOUND


# ---------------------------------------------------------------------------
# Parser shape: positional IDs rejected, --id accepted
# ---------------------------------------------------------------------------


def _build() -> argparse.ArgumentParser:
    return agent.build_parser()


def _parse(command: str, argv: list[str]) -> argparse.Namespace:
    return _build().parse_args([command, *argv])


@pytest.mark.parametrize(
    "command",
    ["status", "prompt", "log", "wait", "stop", "kill", "delete"],
)
def test_id_option_accepted(command: str) -> None:
    """--id is accepted for every ID-taking command."""
    extra: list[str] = []
    if command == "prompt":
        extra = ["hello"]
    elif command == "wait":
        extra = ["--timeout", "1"]
    elif command == "log":
        extra = ["--lines", "1"]
    args = _parse(command, ["--id", "abc123", *extra])
    assert getattr(args, "id", None) == "abc123"


def test_new_accepts_id() -> None:
    """New accepts --id."""
    args = _parse("new", ["--id", "abc123"])
    assert args.id == "abc123"


@pytest.mark.parametrize("command", ["status", "log", "stop", "kill", "delete"])
def test_positional_id_rejected(command: str) -> None:
    """A bare positional argument is not interpreted as an agent ID."""
    with pytest.raises(SystemExit):
        _build().parse_args([command, "abc123"])


def test_prompt_positional_is_prompt_text() -> None:
    """The positional in prompt is prompt text, not an agent ID."""
    args = _parse("prompt", ["--id", "abc123", "do something"])
    assert args.prompt_text == "do something"
    assert args.id == "abc123"


def test_wait_requires_timeout() -> None:
    """Wait requires --timeout."""
    with pytest.raises(SystemExit):
        _build().parse_args(["wait", "--id", "abc123"])


def test_delete_accepts_force() -> None:
    """Delete accepts --force."""
    args = _parse("delete", ["--id", "abc123", "--force"])
    assert args.force is True


@pytest.mark.parametrize("command", ["list", "clean"])
def test_no_id_option(command: str) -> None:
    """List and clean do not take --id."""
    args = _parse(command, [])
    assert not hasattr(args, "id")
