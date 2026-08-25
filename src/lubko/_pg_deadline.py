"""The supervisor's deadline-capable live database connection class.

This module owns the one piece of the established-operation deadline design
that must touch the compiled psycopg driver at class-definition time (the
:class:`DeadlineConnection` subclass). It is imported lazily by
:mod:`lubko.worker` through a module-level ``__getattr__`` so that importing
the worker never pays the driver load cost; by the time it resolves, a real
database connection is being established anyway.

Only the supervisor's own live connection is narrowed to this class; all
shared transport helpers stay generic over the plain connection type. A hung
established connection (silent TCP black hole) could otherwise block the
single supervisor loop indefinitely inside a libpq operation before the
outage state was ever reached, letting owned process groups outlive their
database leases. ``connect_timeout`` bounds only establishment and the
server-side ``statement_timeout`` cannot guarantee the client notices a black
hole, so the bound is application-owned.
"""

from __future__ import annotations

import logging
from contextlib import suppress
from typing import TYPE_CHECKING, Any, Final, override

import psycopg

from lubko.worker import wait_with_deadline

if TYPE_CHECKING:
    from psycopg.abc import RV, PQGen

LOGGER: Final = logging.getLogger(__name__)


class DeadlineConnection(psycopg.Connection[tuple[Any, ...]]):
    """A supervisor connection whose operations obey a hard client deadline.

    Every cursor execution drives its libpq generator through :meth:`wait`,
    which enforces the absolute monotonic ``operation_deadline`` currently
    installed by the supervisor for this turn. On breach the underlying libpq
    connection is finished (marked closed/broken) before the deadline error
    propagates, so no later operation can reuse a hung socket and connectivity
    classification always succeeds.

    The deadline is deliberately *not* a fixed per-operation timeout: the
    supervisor derives it from the earliest active job's lease-safety instant
    (capped by the configured ``db_operation_timeout_seconds``), so even an
    operation that starts late in a lease cycle cannot outlive the margin.
    """

    #: Absolute monotonic deadline for the current operation, installed by the
    #: supervisor before each database turn. The presence of this class
    #: attribute is the deadline capability marker checked by
    #: :func:`lubko.worker.install_operation_deadline`.
    operation_deadline: float = 0.0

    @override
    def wait(self, gen: PQGen[RV], interval: float = 0.1) -> RV:
        """Drive ``gen`` under the currently installed operation deadline.

        Args:
            gen: The nonblocking libpq generator to drive.
            interval: Unused compatibility parameter from psycopg's interface;
                waiting is bounded solely by the absolute deadline.

        Returns:
            Whatever the generator returns on completion.

        Raises:
            TimeoutError: The hard client deadline passed; the libpq
                connection is failed closed first.
        """
        try:
            return wait_with_deadline(gen, self.pgconn.socket, self.operation_deadline)
        except TimeoutError:
            # The only timeout source inside ``wait_with_deadline`` is the
            # application-owned hard client deadline; failing the connection
            # closed on it is fail-safe by construction.
            LOGGER.exception("database operation breached its client deadline")
            with suppress(Exception):
                self.pgconn.finish()
            raise
