"""Token-scoped readiness proof for supervised deployment candidates.

The supervised checkout protocol requires more than an alive candidate
process: a candidate that stays alive but never reaches the queue-processing
boundary must be rejected and rolled back before the operator confirmation
timeout (issue #30). The candidate ``lubko-worker`` therefore writes an atomic,
token-scoped marker only after it has connected to PostgreSQL and passed the
canonical schema invariant verification; ``lubko-deploy-ctl checkout`` waits
for the exact process identity together with that matching marker after the
gate is released.

The marker is keyed by ``LUBKO_LIFECYCLE_TOKEN`` (the fresh per-candidate
deployment token) so a stale marker from an earlier candidate can never satisfy
a later one. The marker write is best-effort on the worker side; a worker that
cannot write it simply fails closed on the controller side, which rolls back.

Rollback compatibility is staged: the marker is only *required* when the
candidate commit itself implements this readiness protocol (its checked-out
source tree carries this module). Legacy candidates that predate the protocol
fall back to the previous post-release liveness check, so rolling back to
older known-good versions remains possible, and rollback itself never requires
the marker from a restored worker.
"""

from __future__ import annotations

import json
import os
import time
from contextlib import suppress
from pathlib import Path
from typing import Any, Final

from lubko.state import worker_state_dir

#: Environment variable carrying the per-deployment lifecycle token that scopes
#: every readiness marker. Defined here (rather than in :mod:`lubko.lifecycle`)
#: so :mod:`lubko.worker` can import it without a dependency cycle.
LIFECYCLE_MARKER_VAR: Final = "LUBKO_LIFECYCLE_TOKEN"

READINESS_MARKER_VERSION: Final = 1
MARKER_FILENAME_PREFIX: Final = "ready-"
MARKER_FILENAME_SUFFIX: Final = ".json"


def readiness_marker_path(token: str) -> Path:
    """Return the readiness marker path keyed by one deployment token.

    Args:
        token: Exact lifecycle token of the candidate deployment.

    Returns:
        The per-user state path of the candidate's readiness marker.
    """
    return worker_state_dir() / f"{MARKER_FILENAME_PREFIX}{token}{MARKER_FILENAME_SUFFIX}"


def write_readiness_marker(token: str) -> None:
    """Atomically write the readiness marker for the current worker process.

    The marker is written to a temporary sibling and atomically replaced so a
    controller poll never observes a partially written marker.

    Args:
        token: Exact lifecycle token of this deployment.
    """
    marker: dict[str, object] = {
        "v": READINESS_MARKER_VERSION,
        "token": token,
        "pid": os.getpid(),
        "written_at": time.time(),
    }
    path = readiness_marker_path(token)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(marker, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def readiness_proven(token: str) -> bool:
    """Return whether a matching readiness marker exists for the exact token.

    The marker path is keyed by the token and its content must name the exact
    token, so a stale marker written by any other candidate can never satisfy
    the proof.

    Args:
        token: Exact lifecycle token of the candidate deployment.

    Returns:
        ``True`` only when an intact marker for exactly this token exists.
    """
    marker = _read_marker(readiness_marker_path(token))
    return marker is not None and marker.get("token") == token


def remove_readiness_marker(token: str) -> None:
    """Best-effort remove the readiness marker of one deployment token.

    Args:
        token: Exact lifecycle token of the deployment.
    """
    with suppress(OSError):
        readiness_marker_path(token).unlink()


def candidate_supports_readiness(repo: Path) -> bool:
    """Return whether the checked-out candidate implements the readiness proof.

    The candidate commit's checked-out source tree carries this module exactly
    when it implements the token-scoped readiness protocol. Legacy candidates
    that predate the protocol do not, and are verified with the older
    post-release liveness check so rolling back to older known-good versions
    stays possible.

    Args:
        repo: Deployment checkout currently at the candidate commit.

    Returns:
        ``True`` when the candidate is expected to write a readiness marker.
    """
    return (Path(repo) / "src" / "lubko" / "readiness.py").is_file()


def _read_marker(path: Path) -> dict[str, Any] | None:
    """Decode and validate a readiness marker file.

    Args:
        path: Marker path to read.

    Returns:
        The decoded marker mapping, or ``None`` when missing or malformed.
    """
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError:
        return None
    try:
        marker = json.loads(raw)
    except ValueError:
        return None
    if not isinstance(marker, dict):
        return None
    return marker
