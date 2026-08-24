"""Tests for loading the Lubko database and worker configuration files."""

from pathlib import Path
from typing import Final

import psycopg
import pytest

from lubko import config
from lubko.config import (
    WORKER_CONFIG_ENV,
    DatabaseConfig,
    database_config_path,
    load_database_config,
    load_worker_server,
    parse_database_config,
    worker_config_path,
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


def worker_private_file(tmp_path: Path, text: str = "server = alpha-server\n") -> Path:
    """Write a private (mode 0600) worker configuration file for tests.

    Args:
        tmp_path: Temporary directory to write into.
        text: Configuration file text.

    Returns:
        The written file path.
    """
    path = tmp_path / "worker.conf"
    path.write_text(text)
    path.chmod(0o600)
    return path


def test_worker_config_path_uses_config_home(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """XDG_CONFIG_HOME determines the default worker configuration path."""
    monkeypatch.delenv("LUBKO_WORKER_CONFIG", raising=False)
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))

    assert worker_config_path() == tmp_path / "lubko" / "worker.conf"


def test_worker_config_path_falls_back_to_home(monkeypatch: pytest.MonkeyPatch) -> None:
    """Without XDG_CONFIG_HOME the path falls back to the home directory."""
    monkeypatch.delenv("LUBKO_WORKER_CONFIG", raising=False)
    monkeypatch.delenv("XDG_CONFIG_HOME", raising=False)

    assert worker_config_path() == Path("~/.config/lubko/worker.conf").expanduser()


def test_worker_config_path_explicit_override(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """LUBKO_WORKER_CONFIG overrides the default path, path selection only."""
    target = tmp_path / "private.conf"
    monkeypatch.setenv("LUBKO_WORKER_CONFIG", str(target))
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "other"))

    assert worker_config_path() == target


def test_load_worker_server_reads_explicit_override(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The default load resolves LUBKO_WORKER_CONFIG when it is set."""
    target = worker_private_file(tmp_path, "server = override-server\n")
    monkeypatch.setenv("LUBKO_WORKER_CONFIG", str(target))

    assert load_worker_server() == "override-server"


def test_worker_config_env_never_carries_the_server_identity() -> None:
    """The override constant is a path selector, distinct from any value env."""
    assert WORKER_CONFIG_ENV == "LUBKO_WORKER_CONFIG"
    identity_envs = [name for name in vars(config) if name.endswith("_ENV")]
    for name in identity_envs:
        assert "SERVER" not in getattr(config, name)


def test_load_worker_server_roundtrip(tmp_path: Path) -> None:
    """A private file with a non-empty server setting loads that identity."""
    assert load_worker_server(worker_private_file(tmp_path)) == "alpha-server"


def test_load_worker_server_rejects_empty_setting(tmp_path: Path) -> None:
    """An empty or whitespace-only server setting fails closed."""
    for text in ("server =\n", "server =   \n"):
        with pytest.raises(ValueError, match="non-empty 'server' setting"):
            load_worker_server(worker_private_file(tmp_path, text))


def test_load_worker_server_requires_server_setting(tmp_path: Path) -> None:
    """A file without a server setting fails closed."""
    with pytest.raises(ValueError, match="non-empty 'server' setting"):
        load_worker_server(worker_private_file(tmp_path, "host = db.example.com\n"))


def test_load_worker_server_rejects_malformed_file(tmp_path: Path) -> None:
    """A malformed line anywhere in the file fails closed."""
    with pytest.raises(ValueError, match="invalid database configuration line"):
        load_worker_server(worker_private_file(tmp_path, "server = alpha\nnot a setting\n"))


@pytest.mark.parametrize("mode", [0o640, 0o604, 0o666])
def test_load_worker_server_rejects_group_or_world_accessible_file(
    tmp_path: Path,
    mode: int,
) -> None:
    """A group- or world-accessible file is refused."""
    path = worker_private_file(tmp_path)
    path.chmod(mode)

    with pytest.raises(PermissionError, match="must not be readable by group or others"):
        load_worker_server(path)


def test_load_worker_server_missing_file(tmp_path: Path) -> None:
    """A missing configuration file fails closed."""
    missing = tmp_path / "absent" / "worker.conf"

    with pytest.raises(FileNotFoundError):
        load_worker_server(missing)


def test_load_worker_server_reads_isolated_default() -> None:
    """The default path resolves the isolated per-test worker configuration."""
    assert load_worker_server() == "alpha-server"
