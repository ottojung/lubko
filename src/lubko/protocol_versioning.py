"""Application-side protocol version policy.

This build speaks exactly the current Lubko payload protocol. PostgreSQL treats
``payload`` as opaque text and never participates in versioning. A future real
protocol change should introduce only the compatibility machinery that change
actually needs; no speculative successor version is represented here.
"""

from __future__ import annotations

from enum import Enum
from typing import Final

CURRENT_PROTOCOL_VERSION: Final = 4


class JobVersionDisposition(Enum):
    """How a daemon treats a pending job's protocol version."""

    CLAIMABLE = "claimable"
    FAIL_CLOSED = "fail_closed"


def version_supported(version: int) -> bool:
    """Return whether this build can parse and execute ``version``."""
    return version == CURRENT_PROTOCOL_VERSION


def unsupported_version_diagnostic(version: int) -> str | None:
    """Return a diagnostic when ``version`` is not supported by this build."""
    if version_supported(version):
        return None
    if version < CURRENT_PROTOCOL_VERSION:
        return (
            f"protocol version {version} is below the current supported version "
            f"{CURRENT_PROTOCOL_VERSION}; the job targets a retired generation "
            "and must not be executed"
        )
    return (
        f"protocol version {version} is above the current supported version "
        f"{CURRENT_PROTOCOL_VERSION}; the job targets a newer generation than "
        "this daemon understands and must not be executed"
    )


def classify_job_version(version: int) -> JobVersionDisposition:
    """Return whether this build may claim and execute ``version``."""
    if version_supported(version):
        return JobVersionDisposition.CLAIMABLE
    return JobVersionDisposition.FAIL_CLOSED


def reaper_disposition(version: int) -> JobVersionDisposition:
    """Decide whether this daemon may terminalize an unsupported pending job.

    Retired versions below the current version may be failed closed. Versions
    above the current version stay pending because an older daemon cannot prove
    that no newer daemon in the fleet can serve them.

    Returns:
        ``FAIL_CLOSED`` for retired lower versions; otherwise ``CLAIMABLE``.
    """
    if version < CURRENT_PROTOCOL_VERSION:
        return JobVersionDisposition.FAIL_CLOSED
    return JobVersionDisposition.CLAIMABLE


def claim_version_predicate() -> tuple[str, dict[str, int]]:
    """Return the SQL predicate restricting claims to the current protocol."""
    fragment = "AND ((payload::jsonb)->>'v')::int = %(protocol_version)s\n"
    return fragment, {"protocol_version": CURRENT_PROTOCOL_VERSION}
