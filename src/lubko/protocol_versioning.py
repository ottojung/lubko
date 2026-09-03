"""Bounded mixed-version protocol upgrade mechanism.

The Lubko transport table ``lubko.jobs`` keeps exactly two columns forever (see
``docs/protocol.md``): ``id`` and an opaque ``payload`` text that holds a JSON
object. Every evolving job datum lives inside ``payload``, and protocol evolution
is versioned by the top-level integer ``v``. This module is the reusable,
version-agnostic machinery that lets a fleet of servers run *across a bounded
window of mutually compatible protocol versions* during a staggered upgrade
without destroying in-flight work or immutable output history, and that makes a
single daemon fail closed on any version it cannot understand.

Core model
----------

* A **version window** is an inclusive ``[min, max]`` integer range. It is
  **bounded**: ``max - min <= MAX_VERSION_SPAN`` (default ``1``). Bounding the
  window keeps the compatibility surface — parsers, builders, and the SQL shape
  constraint — finite and reviewable. There is deliberately no unbounded
  backwards-compatibility ladder.
* Every version inside one window is **mutually compatible**: the two payload
  kinds (``command`` and ``output_chunk``) and all required fields are
  identical, and evolution between window versions is strictly additive (new
  optional fields may appear). A breaking change is not admitted inside a
  window; it starts a new generation and is handled by draining the old version
  out of the window before raising ``min``.
* A daemon claims and executes a job only when the job's ``v`` lies inside its
  configured window. A job at a version outside the daemon's window is **never**
  executed by that daemon — that is the fail-closed guarantee for one server.
* Submissions negotiate the highest version common to the client and the target
  server's window, so a mixed-version fleet naturally converges new work onto
  the newest version while older in-flight jobs keep running on daemons that
  still advertise the older version.

The physical schema never changes between compatible window versions, so an
upgrade is non-destructive: no ``truncate``, no data migration, no new column.
The full procedure and rationale live in ``docs/protocol_upgrades.md``.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Final

CURRENT_PROTOCOL_VERSION: Final = 4

#: Versions this build of the code can parse and execute. A daemon's window is a
#: contiguous subset of this set; raising the window upper bound for a new
#: compatible version means adding that version here and in the worker's
#: supported range. Bound the set: see :data:`MAX_VERSION_SPAN`.
#:
#: ``5`` is included as a representative shape-compatible successor to ``4``: the
#: two versions share the identical payload shape (the same two kinds and required
#: fields, differing only additively), so the same parser and builder serve both.
#: A ``[4, 5]`` daemon window is therefore genuinely executable by this build, which
#: is what makes a staggered, non-destructive ``[4, 5]`` rollout actually runnable.
SUPPORTED_PROTOCOL_VERSIONS: Final = frozenset({CURRENT_PROTOCOL_VERSION, 5})

#: Hard cap on the width of a supported version window. A wider window would
#: force every daemon to carry compatibility code for arbitrarily many past
#: generations, so the mechanism refuses to configure one.
MAX_VERSION_SPAN: Final = 1


class JobVersionDisposition(Enum):
    """How a daemon must treat a pending job's protocol version."""

    CLAIMABLE = "claimable"
    FAIL_CLOSED = "fail_closed"


class ProtocolVersionError(ValueError):
    """Raised when a version window or version value violates the binding."""


class VersionNegotiationError(ValueError):
    """Raised when client and server share no mutually supported version."""


@dataclass(frozen=True, slots=True)
class ProtocolVersionRange:
    """An inclusive ``[min, max]`` window of mutually compatible versions.

    The window is bounded (see :data:`MAX_VERSION_SPAN`) and must contain only
    non-negative protocol versions. Construct it from the lowest version still
    being drained (``min``) and the newest version this daemon can write/execute
    (``max``).
    """

    min: int
    max: int

    def __post_init__(self) -> None:
        """Validate the window is non-empty, positive, and within the span bound.

        Raises:
            ProtocolVersionError: If the window is malformed or too wide.
        """
        if self.min < 1:
            msg = f"version window min {self.min} must be >= 1"
            raise ProtocolVersionError(msg)
        if self.max < self.min:
            msg = f"version window max {self.max} precedes min {self.min}"
            raise ProtocolVersionError(msg)
        if self.max - self.min > MAX_VERSION_SPAN:
            msg = (
                f"version window [{self.min}, {self.max}] spans "
                f"{self.max - self.min} but the bounded mixed-version mechanism "
                f"allows at most {MAX_VERSION_SPAN}; drain the oldest version "
                f"before widening the window"
            )
            raise ProtocolVersionError(msg)

    def contains(self, version: int) -> bool:
        """Return whether ``version`` lies inside the window.

        Args:
            version: A protocol version integer.

        Returns:
            ``True`` if ``min <= version <= max``.
        """
        return self.min <= version <= self.max

    def span(self) -> int:
        """Return the width of the window (``max - min``)."""
        return self.max - self.min


#: The default daemon window: exactly the current version, with no backwards
#: compatibility. A fresh install and a fully upgraded, converged fleet use this.
DEFAULT_VERSION_RANGE: Final = ProtocolVersionRange(
    min=CURRENT_PROTOCOL_VERSION, max=CURRENT_PROTOCOL_VERSION
)


def negotiate_version(
    *,
    client_min: int,
    client_max: int,
    server_range: ProtocolVersionRange,
) -> int:
    """Pick the highest protocol version common to client and server.

    A submitting client proposes the range of versions it can speak; the target
    server advertises its supported window. The negotiated version is the highest
    value in both ranges, so a mixed fleet converges new submissions onto the
    newest version everyone understands.

    Args:
        client_min: Lowest protocol version the client can speak.
        client_max: Highest protocol version the client can speak.
        server_range: The target daemon's supported window.

    Returns:
        The highest mutually supported protocol version.

    Raises:
        ProtocolVersionError: If the client range is malformed (non-positive or
            inverted).
        VersionNegotiationError: If the two ranges do not overlap.
    """
    if client_min < 1 or client_max < client_min:
        msg = f"client protocol range [{client_min}, {client_max}] is malformed"
        raise ProtocolVersionError(msg)
    overlap_min = max(client_min, server_range.min)
    overlap_max = min(client_max, server_range.max)
    if overlap_max < overlap_min:
        msg = (
            f"client supports versions [{client_min}, {client_max}] but the "
            f"server supports [{server_range.min}, {server_range.max}]; no "
            f"mutually supported protocol version exists"
        )
        raise VersionNegotiationError(msg)
    return overlap_max


def negotiate_submission_version(server_range: ProtocolVersionRange) -> int:
    """Pick the protocol version a client should stamp when submitting a job.

    A client speaks every version in its build's
    :data:`SUPPORTED_PROTOCOL_VERSIONS`; it stamps the highest version it shares
    with the target server's supported window, so new work converges onto the
    newest supported generation while older in-flight jobs keep running on daemons
    that still advertise the older version. This is the operational submission
    path: callers that write jobs to ``lubko.jobs`` use the returned version.

    Args:
        server_range: The target daemon's supported version window.

    Returns:
        The negotiated protocol version to stamp on the new job payload.
    """
    client_range = ProtocolVersionRange(
        min=min(SUPPORTED_PROTOCOL_VERSIONS),
        max=max(SUPPORTED_PROTOCOL_VERSIONS),
    )
    return negotiate_version(
        client_min=client_range.min,
        client_max=client_range.max,
        server_range=server_range,
    )


def version_supported(version: int, supported: ProtocolVersionRange) -> bool:
    """Return whether ``version`` lies inside ``supported``.

    Args:
        version: A protocol version integer.
        supported: The daemon's supported window.

    Returns:
        ``True`` if the daemon can parse and execute ``version``.
    """
    return supported.contains(version)


def unsupported_version_diagnostic(version: int, supported: ProtocolVersionRange) -> str | None:
    """Return a fail-closed diagnostic for an unsupported version, else ``None``.

    A version below the window names an older generation that has been retired
    from the fleet; a version above the window names a newer generation this
    daemon cannot parse. Both are unsafe to execute, so the daemon must fail the
    job closed rather than run or silently ignore it.

    Args:
        version: A protocol version integer.
        supported: The daemon's supported window.

    Returns:
        A human-readable diagnostic when ``version`` is outside the window, or
        ``None`` when the version is supported.
    """
    if supported.contains(version):
        return None
    if version < supported.min:
        return (
            f"protocol version {version} is below the minimum supported "
            f"{supported.min}; the job was submitted for an older, retired "
            f"generation and must not be executed"
        )
    return (
        f"protocol version {version} is above the maximum supported "
        f"{supported.max}; the job targets a newer generation than this daemon "
        f"understands and must not be executed"
    )


def classify_job_version(version: int, supported: ProtocolVersionRange) -> JobVersionDisposition:
    """Classify a pending job's version against this daemon's window.

    Args:
        version: The pending job's protocol version.
        supported: This daemon's supported window.

    Returns:
        :attr:`JobVersionDisposition.CLAIMABLE` when the daemon should claim and
        run the job, or :attr:`JobVersionDisposition.FAIL_CLOSED` when the
        version is outside the window and the job must be rejected.
    """
    if supported.contains(version):
        return JobVersionDisposition.CLAIMABLE
    return JobVersionDisposition.FAIL_CLOSED


#: The highest protocol version this particular build can parse and execute.
#: This is deliberately local build metadata, never proof of what every daemon
#: in a staggered fleet can serve. In particular, an older binary cannot infer
#: that a version above this ceiling is globally unsupported.
_MAX_SUPPORTED_VERSION: Final = max(SUPPORTED_PROTOCOL_VERSIONS)


def reaper_disposition(version: int, supported: ProtocolVersionRange) -> JobVersionDisposition:
    """Decide whether the reaper may fail closed a pending job.

    A single daemon cannot see the whole fleet's version capabilities, so the
    reaper must be conservative about newer generations. A compile-time ceiling
    such as :data:`_MAX_SUPPORTED_VERSION` only describes this binary; during a
    staggered binary upgrade another daemon may already run a newer build.

    * A version below the window's ``min`` is treated as a retired generation
      under the deployment contract and may be failed closed.
    * A version above the window's ``max`` is left pending. Future-version
      terminalization requires explicit fleet-wide authority; local build
      knowledge is insufficient to destroy the row.

    Args:
        version: The pending job's protocol version.
        supported: This daemon's supported window.

    Returns:
        :attr:`JobVersionDisposition.FAIL_CLOSED` for retired lower versions,
        otherwise :attr:`JobVersionDisposition.CLAIMABLE`.
    """
    if version < supported.min:
        return JobVersionDisposition.FAIL_CLOSED
    return JobVersionDisposition.CLAIMABLE


def claim_version_predicate(
    supported: ProtocolVersionRange,
) -> tuple[str, dict[str, int]]:
    """Build the SQL fragment and params gating a claim to the window.

    The claim query selects only ``command`` rows whose ``server`` matches and
    whose ``v`` lies inside the daemon's supported window, so a daemon never
    locks a row it cannot parse or execute. Jobs outside the window are left for
    daemons that support them (or for the fail-closed reaper described in
    ``docs/protocol_upgrades.md``).

    Args:
        supported: This daemon's supported window.

    Returns:
        A ``(fragment, params)`` pair where ``fragment`` is a SQL ``AND`` clause
        (including its leading newline) and ``params`` holds the bound
        ``min_version``/``max_version`` integers.
    """
    fragment = "AND ((payload::jsonb)->>'v')::int BETWEEN %(min_version)s AND %(max_version)s\n"
    return fragment, {"min_version": supported.min, "max_version": supported.max}
