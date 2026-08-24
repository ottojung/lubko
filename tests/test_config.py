"""Configuration file parsing and private-file permission invariants."""

import stat
from pathlib import Path

import pytest

from lubko.config import (
    DatabaseConfig,
    load_database_config,
    load_worker_server,
    parse_database_config,
)

PRIVATE = 0o600
PUBLIC = 0o644


def write_config(path: Path, text: str, mode: int) -> Path:
    """Write ``text`` to ``path`` with permission bits ``mode``.

    Returns:
        The written path.
    """
    path.write_text(text, encoding="utf-8")
    Path(path).chmod(mode)
    return path


def test_parse_settings() -> None:
    """Settings split at the first ``=``; comments, blanks, and junk lines handled."""
    text = "\n# comment\nhost=db\nport = 5432\nempty=\nbad line\n"
    with pytest.raises(ValueError, match="invalid database configuration"):
        parse_database_config(text)
    settings = parse_database_config("host= db.example\nport=5432\n# c\n\n")
    assert settings == {"host": "db.example", "port": "5432"}


def test_load_database_config(tmp_path: Path) -> None:
    """Private config files load into validated connection settings."""
    path = write_config(
        tmp_path / "db.conf",
        "host=h\nport=5433\ndbname=d\nuser=u\npassword=p\n",
        PRIVATE,
    )
    value = "p"
    config = load_database_config(path)
    assert config == DatabaseConfig(host="h", port=5433, dbname="d", user="u", password=value)


def test_missing_required_key_and_bad_port(tmp_path: Path) -> None:
    """Missing settings and non-integer ports fail with clear errors."""
    path = write_config(
        tmp_path / "db.conf", "host=h\nport=x\ndbname=d\nuser=u\npassword=p\n", PRIVATE
    )
    with pytest.raises(ValueError, match="port"):
        load_database_config(path)
    path.write_text("port=1\n", encoding="utf-8")
    Path(path).chmod(PRIVATE)
    with pytest.raises(ValueError, match="missing required settings"):
        load_database_config(path)


def test_group_readable_file_is_rejected(tmp_path: Path) -> None:
    """Config files readable by group or others are refused."""
    path = write_config(
        tmp_path / "db.conf",
        "host=h\nport=1\ndbname=d\nuser=u\npassword=p\n",
        PUBLIC,
    )
    with pytest.raises(PermissionError):
        load_database_config(path)


def test_missing_file(tmp_path: Path) -> None:
    """Absent configuration files raise ``FileNotFoundError`` naming the path."""
    with pytest.raises(FileNotFoundError):
        load_database_config(tmp_path / "absent.conf")


def test_conninfo_quoting() -> None:
    """Conninfo values quote backslashes and single quotes safely."""
    value = "it's a \\ test"
    config = DatabaseConfig(host="h", port=1, dbname="d", user="u", password=value)
    assert config.conninfo() == (
        "host='h' port=1 dbname='d' user='u' password='it\\'s a \\\\ test'"
    )


def test_worker_server_requires_non_empty_value(tmp_path: Path) -> None:
    """The worker server identity must be non-empty."""
    path = write_config(tmp_path / "worker.conf", "server=alpha\n", PRIVATE)
    assert load_worker_server(path) == "alpha"
    write_config(tmp_path / "empty.conf", "server=\n", PRIVATE)
    with pytest.raises(ValueError, match="server"):
        load_worker_server(tmp_path / "empty.conf")


def test_worker_server_enforces_private_mode(tmp_path: Path) -> None:
    """Worker config files enforce the same private-mode rule."""
    path = write_config(tmp_path / "worker.conf", "server=alpha\n", PUBLIC)
    assert stat.S_IMODE(path.stat().st_mode) & 0o077
    with pytest.raises(PermissionError):
        load_worker_server(path)
