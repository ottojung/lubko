"""In-memory emulation of the authority rows of ``lubko.jobs`` for tests.

The fake implements exactly the statement shapes issued by
:mod:`lubko.lifecycle_authority` against an in-memory payload map, plus an
``unreachable`` toggle surfacing ``psycopg.OperationalError``. Statement
routing keys on distinctive SQL prefixes; anything else raises
``AssertionError`` so newly added statements fail loudly in tests instead of
silently passing against an unprepared double.
"""

from __future__ import annotations

import json
from contextlib import contextmanager
from typing import TYPE_CHECKING

import psycopg

if TYPE_CHECKING:
    from collections.abc import Iterator


class FakeAuthorityCursor:
    """Record statements and emulate authority-row reads and writes."""

    def __init__(self, table: FakeAuthorityConnection) -> None:
        self._table = table
        self.statements: list[str] = table.statements
        self._result: list[tuple[object, ...]] = []
        self.rowcount = 0

    def execute(self, sql: str, params: dict[str, object] | None = None) -> None:
        """Emulate one authority statement.

        Args:
            sql: SQL text issued by the authority module.
            params: Bound parameters.

        Raises:
            psycopg.OperationalError: When the table is unreachable.
            AssertionError: For statements outside the authority shapes.
        """
        if self._table.unreachable:
            msg = "database is unreachable"
            raise psycopg.OperationalError(msg)
        self.statements.append(sql)
        if self._table.empty_reads:
            self._result = []
            self.rowcount = 0
            return
        arguments = params or {}
        if sql.startswith("INSERT INTO lubko.jobs"):
            row_id = str(arguments["id"])
            self._table.rows.setdefault(row_id, str(arguments["payload"]))
            self.rowcount = 0
            return
        if sql.startswith("SELECT payload FROM lubko.jobs"):
            row_id = str(arguments["id"])
            payload = self._table.rows.get(row_id)
            self._result = [(payload,)] if payload is not None else []
            return
        if sql.startswith("UPDATE lubko.jobs"):
            self.rowcount = self._apply_take(arguments)
            return
        msg = f"unexpected authority statement: {sql!r}"
        raise AssertionError(msg)

    def _apply_take(self, arguments: dict[str, object]) -> int:
        """Apply a fencing-epoch compare-and-swap update.

        Args:
            arguments: Bound ``id``/``payload``/``server``/``expected_epoch``.

        Returns:
            ``1`` when the guarded update committed, ``0`` on a lost race.
        """
        row_id = str(arguments["id"])
        stored = self._table.rows.get(row_id)
        current = _decode_take_row(stored)
        if current is None:
            return 0
        if current.get("type") != "lifecycle_authority":
            return 0
        if current.get("server") != arguments["server"]:
            return 0
        if str(current.get("epoch")) != str(arguments["expected_epoch"]):
            return 0
        self._table.rows[row_id] = str(arguments["payload"])
        return 1

    def fetchone(self) -> tuple[object, ...] | None:
        """Return the next result row, if any.

        Returns:
            The single-column row, or ``None`` when absent.
        """
        if not self._result:
            return None
        return self._result.pop(0)

    def fetchall(self) -> list[tuple[object, ...]]:
        """Return all remaining result rows.

        Returns:
            The remaining rows.
        """
        remaining = list(self._result)
        self._result.clear()
        return remaining


def _decode_take_row(stored: str | None) -> dict[str, object] | None:
    """Decode a stored row for compare-and-swap validation.

    Args:
        stored: Raw stored payload text, or ``None`` when absent.

    Returns:
        The decoded mapping, or ``None`` when absent or undecodable.
    """
    if stored is None:
        return None
    try:
        current = json.loads(stored)
    except ValueError:
        return None
    return current if isinstance(current, dict) else None


class FakeAuthorityConnection:
    """In-memory ``lubko.jobs`` authority rows behind a DB-API surface."""

    def __init__(self) -> None:
        self.rows: dict[str, str] = {}
        self.statements: list[str] = []
        self.unreachable = False
        self.empty_reads = False

    @staticmethod
    @contextmanager
    def transaction() -> Iterator[None]:
        """Yield a no-op transaction boundary.

        Yields:
            ``None`` while the transaction is open.
        """
        yield

    @contextmanager
    def cursor(self, **_kwargs: object) -> Iterator[FakeAuthorityCursor]:
        """Yield a recording authority cursor.

        Yields:
            A fake cursor emulating the authority statements.
        """
        yield FakeAuthorityCursor(self)

    def close(self) -> None:
        """Discard the fake connection."""
