"""Shared pytest fixtures enforcing deterministic process teardown.

The container runs under a real reaping PID 1 (tini), so this harness never
installs a reaper or calls ``waitpid(-1)``: after every test, any still-live
test-created process group is stopped by exact identity and the test fails
loudly if it leaked a process, and at the end of the session every tracked
group is asserted to be gone.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pytest

from tests import _process_guard as guard

if TYPE_CHECKING:
    from collections.abc import Iterator

LOGGER = logging.getLogger(__name__)


@pytest.fixture(scope="session", autouse=True)
def _session_process_teardown() -> Iterator[None]:
    """Assert no test-created process survives the whole session.

    Yields:
        Nothing while the suite runs.
    """
    yield
    stopped = guard.teardown_tracked()
    guard.assert_no_live_tracked()
    if stopped:
        LOGGER.debug("session teardown stopped %d leaked process(es)", stopped)


@pytest.fixture(autouse=True)
def _process_teardown() -> Iterator[None]:
    """Own and deterministically stop every process a test creates.

    Yields:
        Nothing while one test runs.
    """
    yield
    stopped = guard.teardown_tracked()
    if stopped:
        LOGGER.debug("test teardown stopped %d leaked process(es)", stopped)
