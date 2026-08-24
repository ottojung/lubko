"""CLI parsing and path-shape invariants that need no processes or services."""

from pathlib import Path

import pytest

from lubko.cli import CliError, cli_commit_dir, is_valid_commit_name, validate_commit_name
from lubko.config import (
    CONFIG_HOME_ENV,
    DATABASE_CONFIG_ENV,
    WORKER_CONFIG_ENV,
    database_config_path,
    worker_config_path,
)
from lubko.deployctl import (
    checkout_failure_exit_code,
    parse_request,
    request_type,
)
from lubko.state import state_root
from lubko.supervisor import build_parser as build_supervisor_parser

COMMIT = "a" * 40


def test_commit_name_is_exactly_40_hex() -> None:
    """Check that commit name is exactly 40 hex holds."""
    assert is_valid_commit_name(COMMIT)
    assert is_valid_commit_name("a1F0" * 10)
    for bad in ("", "a" * 39, "a" * 41, "g" * 40, COMMIT + "/../etc", "../" + COMMIT):
        assert not is_valid_commit_name(bad), bad
    with pytest.raises(CliError):
        validate_commit_name("short")


def test_commit_directory_shape(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Check that commit directory shape holds."""
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
    assert cli_commit_dir(COMMIT) == tmp_path / "lubko" / "cli" / COMMIT


def test_state_root_follows_xdg(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Check that state root follows xdg holds."""
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
    assert state_root() == tmp_path / "lubko"
    monkeypatch.delenv("XDG_STATE_HOME")
    monkeypatch.setenv("HOME", str(tmp_path))
    assert state_root() == tmp_path / ".local" / "state" / "lubko"


def test_explicit_config_env_overrides_xdg(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Check that explicit config env overrides xdg holds."""
    monkeypatch.setenv(DATABASE_CONFIG_ENV, str(tmp_path / "db.conf"))
    monkeypatch.setenv(WORKER_CONFIG_ENV, str(tmp_path / "worker.conf"))
    monkeypatch.setenv(CONFIG_HOME_ENV, "/elsewhere")
    assert database_config_path() == tmp_path / "db.conf"
    assert worker_config_path() == tmp_path / "worker.conf"


def test_deployctl_request_parsing() -> None:
    """Check that deployctl request parsing holds."""
    request = parse_request('{"type": "status", "x": 1}')
    assert request_type(request) == "status"
    assert not request_type({})
    assert not request_type({"type": 3})


def test_failed_checkout_exits_nonzero_other_rejections_do_not() -> None:
    """Check that failed checkout exits nonzero other rejections do not holds."""
    failed: dict[str, object] = {"ok": False}
    succeeded: dict[str, object] = {"ok": True}
    assert checkout_failure_exit_code("checkout", failed) != 0
    assert checkout_failure_exit_code("checkout", succeeded) == 0
    assert checkout_failure_exit_code("confirm", failed) == 0


def test_deployctl_rejects_non_object_and_bad_json() -> None:
    """Check that deployctl rejects non object and bad json holds."""
    with pytest.raises(Exception, match="not valid JSON"):
        parse_request("{oops")
    with pytest.raises(Exception, match="JSON object"):
        parse_request("[1]")


def test_supervisor_parser_defaults() -> None:
    """Check that supervisor parser defaults holds."""
    args = build_supervisor_parser().parse_args([])
    assert args.status is False
    assert args.uv is None
    status = build_supervisor_parser().parse_args(["--status"])
    assert status.status is True
