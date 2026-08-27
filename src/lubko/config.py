"""Loading of the Lubko PostgreSQL connection settings from a restricted file."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Final

from lubko.protocol_versioning import (
    DEFAULT_VERSION_RANGE,
    ProtocolVersionError,
    ProtocolVersionRange,
)

DATABASE_CONFIG_ENV: Final = "LUBKO_DATABASE_CONFIG"
CONFIG_HOME_ENV: Final = "XDG_CONFIG_HOME"
CONFIG_HOME_FALLBACK: Final = ".config"
CONFIG_RELATIVE_PATH: Final = Path("lubko/database.conf")
WORKER_CONFIG_ENV: Final = "LUBKO_WORKER_CONFIG"
WORKER_CONFIG_RELATIVE_PATH: Final = Path("lubko/worker.conf")
REQUIRED_KEYS: Final = ("host", "port", "dbname", "user", "password")
PRIVATE_MODE_MASK: Final = 0o077


@dataclass(frozen=True, slots=True)
class DatabaseConfig:
    """PostgreSQL connection settings for the Lubko worker."""

    host: str
    port: int
    dbname: str
    user: str
    password: str

    def conninfo(self) -> str:
        """Build a libpq connection string from these settings.

        Returns:
            A libpq ``conninfo`` string suitable for ``psycopg.connect``.
        """
        return " ".join((
            f"host={_quote_conninfo_value(self.host)}",
            f"port={self.port}",
            f"dbname={_quote_conninfo_value(self.dbname)}",
            f"user={_quote_conninfo_value(self.user)}",
            f"password={_quote_conninfo_value(self.password)}",
        ))


def _quote_conninfo_value(value: str) -> str:
    """Quote a value for a libpq ``conninfo`` string.

    Args:
        value: Raw connection setting value.

    Returns:
        The value wrapped in single quotes with escapes applied.
    """
    escaped = value.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def database_config_path() -> Path:
    """Return the path of the worker database configuration file.

    The path is ``$LUBKO_DATABASE_CONFIG`` when set, otherwise
    ``$XDG_CONFIG_HOME/lubko/database.conf`` with a fallback of
    ``~/.config/lubko/database.conf``.

    Returns:
        The database configuration file path.
    """
    explicit = os.environ.get(DATABASE_CONFIG_ENV)
    if explicit:
        return Path(explicit)
    base = os.environ.get(CONFIG_HOME_ENV) or str(Path.home() / CONFIG_HOME_FALLBACK)
    return Path(base) / CONFIG_RELATIVE_PATH


def parse_database_config(text: str) -> dict[str, str]:
    """Parse ``key=value`` settings from configuration text.

    Blank lines and lines starting with ``#`` are ignored. Settings are split
    at the first ``=`` and surrounding whitespace is stripped.

    Args:
        text: Raw configuration file text.

    Returns:
        The parsed settings by key.

    Raises:
        ValueError: If a line is not a valid ``key=value`` assignment.
    """
    settings: dict[str, str] = {}
    for line in text.splitlines():
        content = line.strip()
        if not content or content.startswith("#"):
            continue
        key, separator, value = content.partition("=")
        if not separator or not key.strip():
            msg = f"invalid database configuration line: {content!r}"
            raise ValueError(msg)
        settings[key.strip()] = value.strip()
    return settings


def load_database_config(path: Path | None = None) -> DatabaseConfig:
    """Load the worker database configuration from a restricted file.

    The file must not be accessible by the group or by other users. Error
    messages never contain credential values.

    Args:
        path: Configuration path, or ``None`` to use :func:`database_config_path`.

    Returns:
        The parsed database configuration.

    Raises:
        FileNotFoundError: If the configuration file does not exist.
        PermissionError: If the file is accessible by group or others.
        ValueError: If a required setting is missing or invalid.
    """
    config_path = path if path is not None else database_config_path()
    try:
        file_stat = config_path.stat()
    except FileNotFoundError as exc:
        msg = f"database configuration file not found: {config_path}"
        raise FileNotFoundError(msg) from exc
    if file_stat.st_mode & PRIVATE_MODE_MASK:
        msg = f"database configuration file must not be readable by group or others: {config_path}"
        raise PermissionError(msg)
    values = parse_database_config(config_path.read_text(encoding="utf-8"))
    missing = [key for key in REQUIRED_KEYS if key not in values]
    if missing:
        msg = "database configuration file is missing required settings: " + ", ".join(missing)
        raise ValueError(msg)
    try:
        port = int(values["port"])
    except ValueError:
        msg = "database configuration setting 'port' must be an integer"
        raise ValueError(msg) from None
    return DatabaseConfig(
        host=values["host"],
        port=port,
        dbname=values["dbname"],
        user=values["user"],
        password=values["password"],
    )


def worker_config_path() -> Path:
    """Return the path of the worker configuration file.

    The path is ``$LUBKO_WORKER_CONFIG`` when set, otherwise
    ``$XDG_CONFIG_HOME/lubko/worker.conf`` with a fallback of
    ``~/.config/lubko/worker.conf``, following the same per-user convention
    as the database configuration file. The override selects the file path
    only; the execution-server identity itself always lives inside the file.

    Returns:
        The worker configuration file path.
    """
    explicit = os.environ.get(WORKER_CONFIG_ENV)
    if explicit:
        return Path(explicit)
    base = os.environ.get(CONFIG_HOME_ENV) or str(Path.home() / CONFIG_HOME_FALLBACK)
    return Path(base) / WORKER_CONFIG_RELATIVE_PATH


def load_worker_server(path: Path | None = None) -> str:
    """Load the execution-server identity from the restricted worker config.

    The file follows the same permission rules as the database configuration:
    it must not be accessible by the group or by other users. The ``server``
    setting is required and must be a non-empty string; every protocol-v4
    claim, mutation, publication, recovery, and GC pass of the worker is
    scoped to exactly this identity, so an absent or invalid value fails
    closed and the daemon refuses to start.

    Args:
        path: Configuration path, or ``None`` to use :func:`worker_config_path`.

    Returns:
        The non-empty configured server identity.

    Raises:
        FileNotFoundError: If the configuration file does not exist.
        PermissionError: If the file is accessible by group or others.
        ValueError: If the ``server`` setting is missing or empty.
    """
    config_path = path if path is not None else worker_config_path()
    try:
        file_stat = config_path.stat()
    except FileNotFoundError as exc:
        msg = f"worker configuration file not found: {config_path}"
        raise FileNotFoundError(msg) from exc
    if file_stat.st_mode & PRIVATE_MODE_MASK:
        msg = f"worker configuration file must not be readable by group or others: {config_path}"
        raise PermissionError(msg)
    values = parse_database_config(config_path.read_text(encoding="utf-8"))
    server = values.get("server", "")
    if not server:
        msg = (
            "worker configuration file is missing a non-empty 'server' setting; "
            "every daemon owns exactly one execution-server identity"
        )
        raise ValueError(msg)
    return server


def load_worker_protocol_range(path: Path | None = None) -> ProtocolVersionRange:
    """Load the supported protocol version window from the worker config.

    The window is optional. When the ``protocol_min_version`` /
    ``protocol_max_version`` keys are absent the daemon defaults to the current
    single-version window (:data:`DEFAULT_VERSION_RANGE`). When present, both
    bounds must be integers and form a valid bounded window; an invalid value
    fails closed so a misconfigured daemon cannot start with a window it cannot
    actually serve. The execution-server protocol window is never environmental:
    it is read from the restricted worker configuration file.

    Args:
        path: Configuration path, or ``None`` to use :func:`worker_config_path`.

    Returns:
        The supported protocol version window.

    Raises:
        FileNotFoundError: If the configuration file does not exist.
        PermissionError: If the file is accessible by group or others.
        ValueError: If a present window is missing, non-integer, or invalid.
    """
    config_path = path if path is not None else worker_config_path()
    try:
        file_stat = config_path.stat()
    except FileNotFoundError as exc:
        msg = f"worker configuration file not found: {config_path}"
        raise FileNotFoundError(msg) from exc
    if file_stat.st_mode & PRIVATE_MODE_MASK:
        msg = f"worker configuration file must not be readable by group or others: {config_path}"
        raise PermissionError(msg)
    values = parse_database_config(config_path.read_text(encoding="utf-8"))
    min_raw = values.get("protocol_min_version")
    max_raw = values.get("protocol_max_version")
    if min_raw is None and max_raw is None:
        return DEFAULT_VERSION_RANGE
    if min_raw is None or max_raw is None:
        msg = (
            "worker configuration must set both protocol_min_version and "
            "protocol_max_version, or neither"
        )
        raise ValueError(msg)
    try:
        parsed_min = int(min_raw)
        parsed_max = int(max_raw)
    except (TypeError, ValueError):
        msg = "worker configuration protocol_min_version/protocol_max_version must be integers"
        raise ValueError(msg) from None
    try:
        return ProtocolVersionRange(min=parsed_min, max=parsed_max)
    except ProtocolVersionError as exc:
        msg = f"invalid worker protocol window in {config_path}: {exc}"
        raise ValueError(msg) from None
