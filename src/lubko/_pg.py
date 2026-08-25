"""Lazy loading of the compiled PostgreSQL driver.

Importing ``psycopg`` executes its compiled binary layer, which costs roughly
0.3 s of interpreter start-up. Most Lubko modules only *define* SQL text,
connection plumbing, or pure helpers and never touch the driver until a real
connection is attempted.

Import the driver through this module::

    from lubko._pg import psycopg, tuple_row

At type-check time ``psycopg`` is the real driver module, so all attribute
uses are fully checked; at runtime it is a lazily-executing stub whose first
attribute access performs the real load.
"""

from __future__ import annotations

import importlib.util
import sys
from typing import TYPE_CHECKING, Any

__all__ = ["psycopg", "tuple_row"]

if TYPE_CHECKING:
    import psycopg
    from psycopg.rows import tuple_row

else:
    _LAZY_MODULE = "psycopg"

    def defer_psycopg_loading() -> None:
        """Register a deferred-execution stub for ``psycopg``."""
        if _LAZY_MODULE in sys.modules:
            return
        spec = importlib.util.find_spec(_LAZY_MODULE)
        if spec is None or spec.loader is None:
            return
        spec.loader = importlib.util.LazyLoader(spec.loader)
        module = importlib.util.module_from_spec(spec)
        sys.modules[_LAZY_MODULE] = module
        spec.loader.exec_module(module)

    defer_psycopg_loading()
    psycopg = sys.modules[_LAZY_MODULE]

    def _tuple_row(_cursor: object) -> type[tuple[Any, ...]]:
        """Runtime stand-in for ``psycopg.rows.tuple_row``.

        The real factory returns exactly the built-in ``tuple`` type (the C
        code fast-paths on it), so this equivalent needs no driver import.

        Returns:
            The built-in ``tuple`` type used as the row maker.
        """
        return tuple

    tuple_row = _tuple_row
