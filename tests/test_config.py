"""Tests for loading the Lubko database configuration file."""

from pathlib import Path
from typing import Final

import psycopg
import pytest

from lubko.config import (
    DatabaseConfig,
    database_config_path,
    load_database_config,
    parse_database_config,
)

TEST_HOST: Final = "db.example.com"
TEST_PORT: Final = 5432
TEST_DATABASE: Final = "postgres"
TEST_USER: Final = "lubko_worker"
TEST_PASSWORD: Final = "secret-value"  # ruff: ignore[hardcoded-password-string] - test credential

REQUIRED_SETTINGS: Final = f"""\
host = {TEST_HOST}
port = {TEST_PORT}
dbname = {TEST_DATABASE}
user = {TEST_USER}
password = {TEST_PASSWORD}
"""


def private_file(tmp_path: Path, text: str = REQUIRED_SETTINGS) -> Path:
    """Write a private (mode 0600) configuration file for tests.

    Args:
        tmp_path: Temporary directory to write into.
        text: Configuration file text.

    Returns:
        The written file path.
    """
    path = tmp_path / "database.conf"
    path.write_text(text + "\n")
    path.chmod(0o600)
    return path


def test_parse_database_config_ignores_blanks_and_comments() -> None:
    """Blank lines and comments are ignored when parsing."""
    text = f"""\
# Lubko database settings

  host = {TEST_HOST}
port={TEST_PORT}
"""

    settings = parse_database_config(text)

    assert settings == {"host": TEST_HOST, "port": str(TEST_PORT)}


def test_parse_database_config_rejects_invalid_line() -> None:
    """A line without an assignment raises ValueError."""
    with pytest.raises(ValueError, match="invalid database configuration line"):
        parse_database_config("just some text")


def test_load_database_config_roundtrip(tmp_path: Path) -> None:
    """A private file loads into a complete DatabaseConfig."""
    config = load_database_config(private_file(tmp_path))

    assert config.host == TEST_HOST
    assert config.port == TEST_PORT
    assert config.dbname == TEST_DATABASE
    assert config.user == TEST_USER
    assert config.password == TEST_PASSWORD


def test_load_database_config_requires_all_settings(tmp_path: Path) -> None:
    """Missing required settings are reported by name."""
    path = private_file(tmp_path, f"host = {TEST_HOST}\n")

    with pytest.raises(ValueError, match="missing required settings"):
        load_database_config(path)


def test_load_database_config_rejects_non_integer_port(tmp_path: Path) -> None:
    """A non-integer port is reported."""
    path = private_file(
        tmp_path,
        f"""\
host = {TEST_HOST}
port = postgres
dbname = {TEST_DATABASE}
user = {TEST_USER}
password = {TEST_PASSWORD}
""",
    )

    with pytest.raises(ValueError, match="'port' must be an integer"):
        load_database_config(path)


def test_load_database_config_rejects_group_readable_file(tmp_path: Path) -> None:
    """A file readable by the group is refused."""
    path = private_file(tmp_path)
    path.chmod(0o640)

    with pytest.raises(PermissionError, match="group or others"):
        load_database_config(path)


def test_load_database_config_rejects_world_readable_file(tmp_path: Path) -> None:
    """A file readable by other users is refused."""
    path = private_file(tmp_path)
    path.chmod(0o644)

    with pytest.raises(PermissionError, match="group or others"):
        load_database_config(path)


def test_load_database_config_missing_file(tmp_path: Path) -> None:
    """A missing file raises FileNotFoundError."""
    with pytest.raises(FileNotFoundError):
        load_database_config(tmp_path / "does-not-exist.conf")


def test_conninfo_roundtrips_special_characters() -> None:
    """Conninfo escapes values with whitespace, quotes, and backslashes."""
    quoted_value = "p w='d'\\x;"
    config = DatabaseConfig(
        host=TEST_HOST,
        port=TEST_PORT,
        dbname=TEST_DATABASE,
        user=TEST_USER,
        password=quoted_value,
    )

    parsed = psycopg.conninfo.conninfo_to_dict(config.conninfo())

    assert parsed["host"] == TEST_HOST
    assert parsed["port"] == str(TEST_PORT)
    assert parsed["dbname"] == TEST_DATABASE
    assert parsed["user"] == TEST_USER
    assert parsed["password"] == quoted_value


def test_database_config_path_uses_explicit_override(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """LUBKO_DATABASE_CONFIG overrides the default path."""
    target = tmp_path / "custom.conf"
    monkeypatch.setenv("LUBKO_DATABASE_CONFIG", str(target))

    assert database_config_path() == target


def test_database_config_path_uses_config_home(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """XDG_CONFIG_HOME determines the default path."""
    monkeypatch.delenv("LUBKO_DATABASE_CONFIG", raising=False)
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))

    assert database_config_path() == tmp_path / "lubko" / "database.conf"


def test_database_config_path_falls_back_to_home(monkeypatch: pytest.MonkeyPatch) -> None:
    """Without XDG_CONFIG_HOME the path falls back to the home directory."""
    monkeypatch.delenv("LUBKO_DATABASE_CONFIG", raising=False)
    monkeypatch.delenv("XDG_CONFIG_HOME", raising=False)

    assert database_config_path() == Path("~/.config/lubko/database.conf").expanduser()
