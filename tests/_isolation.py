"""Test-suite isolation helpers.

The whole validation suite must be safe to run from the same Unix user and
container as the live Lubko worker. These helpers make that guarantee the
default rather than an opt-in:

- the conftest autouse fixtures point every XDG-backed Lubko state root at a
  pytest-owned temporary directory for every test, before any lifecycle path
  can be resolved;
- ``assert_test_owned_state_root`` is the fail-closed guard used by destructive
  test helpers (metadata writes, recorded-identity signalling): it refuses to
  touch state unless the currently-resolved state root is under the current
  test's pytest-owned temporary directory;
- an ambient "production-like" sentinel state tree and live process are
  created once per session so the regressions can prove byte-for-byte that the
  suite never mutates ambient state and never signals an ambient process.

Sentinel incarnation safety
---------------------------

``ambient_sentinel_alive`` verifies three independent properties of the
sentinel process — PID liveness, start-time-in-clock-ticks match, and the
lifecycle-token environment marker — so a reused PID after a crash can never
be mistaken for the original sentinel incarnation.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Final

from lubko.state import state_root
from tests import _process_guard as guard

if TYPE_CHECKING:
    from collections.abc import Generator

LOGGER = logging.getLogger(__name__)

STATE_HOME_ENV: Final = "XDG_STATE_HOME"

# Root of the pytest session temporary area (``tmp_path_factory.getbasetemp``),
# recorded by the conftest session fixture so test-side guards can verify
# ownership without re-deriving pytest internals.
TEST_BASETEMP: Path | None = None

# The ambient "production-like" Lubko state root created once per session. It
# deliberately lives outside every per-test XDG root so a test can only ever
# touch it by escaping the isolation, which the regressions must prove never
# happens.
AMBIENT_STATE_ROOT: Path | None = None

# Exact PID of the ambient live sentinel worker process (a real session/group
# leader carrying a lifecycle token), or ``None`` before the session fixture
# creates it.
AMBIENT_SENTINEL_PID: int | None = None

# Start time in clock ticks of the ambient sentinel, captured at spawn so
# ``ambient_sentinel_alive`` can verify the same process incarnation survived
# and not merely a PID-reused look-alike.
AMBIENT_SENTINEL_START_TICKS: int | None = None

# The lifecycle token the sentinel carries in its environment, used as the
# third incarnation-proof property.
AMBIENT_SENTINEL_TOKEN: str | None = None

# /proc/stat field layout constants (same as lifecycle.py).
_STAT_MIN_FIELDS: Final = 20
_STAT_STARTTIME_FIELD_INDEX: Final = 19


@dataclass(slots=True)
class RuntimeState:
    """Mutable runtime state that requires attribute assignment.

    Only ``CURRENT_TEST_TMP`` needs per-test mutation; all other module-level
    state (``TEST_BASETEMP``, ambient sentinel fields) is set once per session
    and never reassigned inside a generator, so bare module names suffice.
    """

    current_test_tmp: Path | None = None


RUNTIME = RuntimeState()


def proc_start_ticks(pid: int) -> int | None:
    """Return a process start time in clock ticks, or ``None``.

    The start time is unique per process on a given boot and survives PID
    reuse, making it the reliable incarnation anchor.

    Args:
        pid: Process ID to inspect.

    Returns:
        The start time in clock ticks, or ``None`` when unreadable.
    """
    try:
        stat = (Path("/proc") / str(pid) / "stat").read_bytes()
    except OSError:
        return None
    close_paren = stat.rfind(b")")
    if close_paren == -1:
        return None
    fields = stat[close_paren + 2 :].split()
    if len(fields) < _STAT_MIN_FIELDS:
        return None
    try:
        return int(fields[_STAT_STARTTIME_FIELD_INDEX])
    except ValueError:
        return None


def _proc_has_token(pid: int, token: str) -> bool:
    """Return whether a process environment carries the lifecycle token.

    The ``/proc/<pid>/environ`` file contains NUL-separated
    ``KEY=VALUE`` entries.  This function checks for an exact match of
    ``LUBKO_LIFECYCLE_TOKEN=<token>`` among those entries so a substring
    match against a different variable name (e.g. ``LUBKO_LIFECYCLE_TOKEN_OLD``)
    is never accepted.

    Args:
        pid: Process ID to inspect.
        token: Expected lifecycle token string.

    Returns:
        ``True`` when the exact token entry is present.
    """
    expected = f"LUBKO_LIFECYCLE_TOKEN={token}".encode()
    try:
        environ = (Path("/proc") / str(pid) / "environ").read_bytes()
    except OSError:
        return False
    return expected in environ.split(b"\0")


def ambient_sentinel_alive() -> bool:
    """Return whether the ambient sentinel is still the original incarnation.

    Three independent properties are verified so a reused PID after a crash
    can never be mistaken for the sentinel:

    1. PID is alive (``kill(pid, 0)`` succeeds).
    2. Start time in clock ticks matches the value captured at spawn.
    3. The lifecycle-token environment marker is present.

    Once ``AMBIENT_SENTINEL_PID`` is set (sentinel was created), all three
    identity fields — PID, start ticks, and token — must be non-``None``.
    A missing tick or token value is treated as a failed incarnation check,
    not as a skipped property.

    Returns:
        ``True`` when the sentinel is confirmed alive as the original
        incarnation, or when no sentinel was ever created.
    """
    pid = AMBIENT_SENTINEL_PID
    if pid is None:
        return True
    # All three identity fields must be present once PID is set.
    expected_ticks = AMBIENT_SENTINEL_START_TICKS
    expected_token = AMBIENT_SENTINEL_TOKEN
    if expected_ticks is None or expected_token is None:
        return False
    # Property 1: PID liveness.
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    # Property 2: start-time incarnation anchor.
    actual_ticks = proc_start_ticks(pid)
    if actual_ticks is None or actual_ticks != expected_ticks:
        return False
    # Property 3: lifecycle-token environment marker (exact NUL-separated match).
    return _proc_has_token(pid, expected_token)


def assert_test_owned_state_root() -> Path:
    """Fail closed unless the resolved state root is current-test-owned.

    Destructive lifecycle helpers call this before reading metadata or
    signalling a recorded identity. Without the guard, a helper that forgot to
    opt in to isolation would resolve ``$XDG_STATE_HOME/lubko`` (or fall back
    to ``~/.local/state/lubko``) and could mutate or signal the live user's
    control plane. With the guard, such a helper aborts loudly instead.

    The check is deliberately strict: the resolved ``$XDG_STATE_HOME`` must be
    inside the *current test's* pytest-owned temporary directory. A sibling
    pytest-owned directory is not acceptable, so an ambient production-like
    tree created under the session basetemp is never mistaken for test-owned
    state.

    Returns:
        The verified test-owned XDG state home path.

    Raises:
        AssertionError: If the state root is missing, resolves outside the
            current test's temporary directory, or isolation was never
            established.
    """
    if RUNTIME.current_test_tmp is None:
        msg = "test state isolation was not established before a destructive lifecycle op"
        raise AssertionError(msg)
    raw = os.environ.get(STATE_HOME_ENV)
    if not raw:
        msg = (
            f"{STATE_HOME_ENV} is unset; state would resolve to the live user state root "
            f"({state_root()}) instead of test-owned state"
        )
        raise AssertionError(msg)
    resolved = Path(raw).resolve()
    test_tmp = RUNTIME.current_test_tmp.resolve()
    if resolved == test_tmp:
        return resolved
    if test_tmp not in resolved.parents:
        msg = (
            f"{STATE_HOME_ENV}={resolved} is not under the current test's pytest-owned "
            f"temporary directory {test_tmp}; refusing to touch non-test lifecycle state"
        )
        raise AssertionError(msg)
    return resolved


def ambient_state_root() -> Path:
    """Return the ambient sentinel Lubko state root.

    Returns:
        The ``.../lubko`` root of the session's ambient production-like tree.

    Raises:
        AssertionError: If the ambient tree was never created.
    """
    if AMBIENT_STATE_ROOT is None:
        msg = "the ambient production-like state tree was not created"
        raise AssertionError(msg)
    return AMBIENT_STATE_ROOT


def snapshot_tree(root: Path) -> dict[str, tuple[str, str]]:
    """Snapshot a directory tree for byte-for-byte comparison.

    Symlinks are recorded by their target; regular files by their content;
    directories by a sorted listing of their entries. The relative path of
    every entry is the key so two snapshots of the same tree compare directly.

    Args:
        root: Directory to snapshot.

    Returns:
        A mapping of relative path to ``(kind, value)`` where ``kind`` is
        ``"file"``, ``"dir"``, or ``"symlink"``.
    """
    snapshot: dict[str, tuple[str, str]] = {}

    def visit(path: Path) -> None:
        relative = str(path.relative_to(root))
        if path.is_symlink():
            snapshot[relative] = ("symlink", str(path.readlink()))
            return
        if path.is_dir():
            entries = sorted(entry.name for entry in path.iterdir())
            snapshot[relative] = ("dir", "\n".join(entries))
            for entry in sorted(path.iterdir(), key=lambda item: item.name):
                visit(entry)
            return
        snapshot[relative] = (
            "file",
            path.read_text(encoding="utf-8", errors="surrogateescape"),
        )

    for entry in sorted(root.iterdir(), key=lambda item: item.name):
        visit(entry)
    return snapshot


def teardown_generator() -> Generator[dict[int, int], None, None]:
    """Yield the process-incarnation snapshot, then deterministically clean up.

    This is the exact teardown body shared by ``_process_teardown`` and by
    regression tests that drive the fixture logic directly.  The generator
    yields the ``before_incidences`` snapshot to the caller (the test body);
    when the caller finishes — whether by normal return, ``Exception``, or
    ``BaseException`` — the ``finally`` block tears down every tracked
    process, asserts no persistent leaks, and clears ``RUNTIME.current_test_tmp``.

    Error propagation:

    - If the body raises and cleanup also raises, the original body exception
      is re-raised with the cleanup exception chained as context.
    - If the body succeeds but cleanup raises, the cleanup exception
      propagates normally.
    - If only the body raises, the original body exception propagates.

    ``RUNTIME.current_test_tmp`` is cleared unconditionally in all paths.

    Yields:
        The pre-test incarnation snapshot (PID → start-time ticks).
    """
    before_incidences = guard.snapshot_incarnations()
    body_error: BaseException | None = None
    try:
        yield before_incidences
    except BaseException as exc:
        body_error = exc
        raise
    finally:
        cleanup_error = _run_teardown_cleanup(before_incidences)
        if body_error is not None and cleanup_error is not None:
            raise body_error from cleanup_error
        if body_error is None and cleanup_error is not None:
            raise cleanup_error


def _run_teardown_cleanup(before_incidences: dict[int, int]) -> BaseException | None:
    """Run teardown cleanup and return any exception, always clearing CURRENT_TEST_TMP.

    Args:
        before_incidences: The pre-test incarnation snapshot.

    Returns:
        The cleanup exception, or ``None`` if cleanup succeeded.
    """
    cleanup_error: BaseException | None = None
    try:
        _execute_teardown(before_incidences)
    except BaseException as exc:  # ruff: ignore[blind-except]
        cleanup_error = exc
    finally:
        RUNTIME.current_test_tmp = None
    return cleanup_error


def _execute_teardown(before_incidences: dict[int, int]) -> None:
    """Execute the teardown steps: stop tracked processes and assert no leaks.

    Args:
        before_incidences: The pre-test incarnation snapshot.
    """
    stopped = guard.teardown_tracked()
    allowed = {os.getpid()}
    owned: set[Path] = set()
    if RUNTIME.current_test_tmp is not None:
        owned.add(RUNTIME.current_test_tmp)
    guard.assert_no_persistent_leaks(before_incidences, allowed=allowed, owned_paths=owned)
    if stopped:
        LOGGER.debug("test teardown stopped %d leaked process(es)", stopped)
