"""Fail-closed recovery consumer-probe authority tests."""

from pathlib import Path
from types import SimpleNamespace

import psycopg
import pytest

from lubko import lifecycle


def options() -> lifecycle.DeployOptions:
    """Build deterministic recovery options.

    Returns:
        Recovery deployment options for the test.
    """
    return lifecycle.DeployOptions(
        repo=Path.cwd(),
        uv_path="uv",
        bootstrap=False,
        stop_grace_seconds=0.1,
        postgres_timeout_seconds=0.1,
        lock_timeout_seconds=0.1,
        validation_timeout_seconds=1.0,
        git_timeout_seconds=1.0,
        cli_timeout_seconds=1.0,
        probe_timeout_seconds=0.1,
    )


def test_consumer_probe_config_failure_is_unknown(monkeypatch: pytest.MonkeyPatch) -> None:
    """Configuration failure must not prove that no consumer exists."""
    message = "config unavailable"

    def fail_config() -> object:
        raise OSError(message)

    monkeypatch.setattr(lifecycle, "load_database_config", fail_config)
    assert lifecycle._queue_has_consumer(".", 0.01) is None


def test_consumer_probe_connect_failure_is_unknown(monkeypatch: pytest.MonkeyPatch) -> None:
    """Connection failure must not prove that no consumer exists."""
    database = SimpleNamespace(conninfo=lambda: "postgresql://unused")
    message = "database unavailable"

    def load_config() -> object:
        return database

    def fail_connect(*_args: object, **_kwargs: object) -> object:
        raise OSError(message)

    monkeypatch.setattr(lifecycle, "load_database_config", load_config)
    monkeypatch.setattr(psycopg, "connect", fail_connect)
    assert lifecycle._queue_has_consumer(".", 0.01) is None


def test_consumer_probe_insert_failure_is_unknown(monkeypatch: pytest.MonkeyPatch) -> None:
    """Probe insertion failure must remain an unknown authority result."""
    database = SimpleNamespace(conninfo=lambda: "postgresql://unused")
    connection = SimpleNamespace(autocommit=False, close=lambda: None)

    def load_config() -> object:
        return database

    def connect(*_args: object, **_kwargs: object) -> object:
        return connection

    monkeypatch.setattr(lifecycle, "load_database_config", load_config)
    monkeypatch.setattr(psycopg, "connect", connect)
    monkeypatch.setattr(lifecycle, "_insert_probe_job", lambda _conn, _cwd: None)
    assert lifecycle._queue_has_consumer(".", 0.01) is None
    assert connection.autocommit is True


def test_recover_preflight_rejects_unknown_consumer_probe(monkeypatch: pytest.MonkeyPatch) -> None:
    """Recovery may start only after a probe explicitly proves absence."""
    monkeypatch.setattr(lifecycle, "read_meta", lambda: None)
    monkeypatch.setattr(lifecycle, "require_clean_checkout", lambda _repo, _timeout: True)
    monkeypatch.setattr(lifecycle, "git_commit", lambda _repo, _timeout: "a" * 40)
    monkeypatch.setattr(lifecycle, "check_postgres", lambda _timeout: True)
    monkeypatch.setattr(lifecycle, "_queue_has_consumer", lambda _cwd, _timeout: None)

    with pytest.raises(lifecycle._AdoptionError):
        lifecycle._recover_preflight(options())
