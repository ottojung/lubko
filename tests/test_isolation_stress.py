"""Repeated/stress coverage for suite isolation under an ambient live worker.

This module extends the per-test isolation regressions in
``tests/test_isolation.py`` with the broader #105 stress matrix:

- every interruption mode of a running nested pytest session (success,
  assertion failure, cancellation-by-SIGINT, SIGTERM interruption, enforced
  timeout, and abrupt SIGKILL termination), proving containment: an
  independent exact-identity owner (``tests._nested_owner``, a child
  subreaper outside the killable pytest process) synchronously owns and reaps
  every recorded descendant, so no test-owned process is ever reparented
  under container PID 1;
- repeated back-to-back nested validation-suite runs while this session's
  real ambient sentinel worker stays alive, each proving byte-for-byte that
  ambient state is untouched.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from contextlib import contextmanager
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Final, cast

import pytest

from tests import _isolation as isolation
from tests import _nested_owner
from tests import _process_guard as guard

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

REPO_ROOT: Final = Path(__file__).resolve().parent.parent

# Nested test module used for interruption-mode scenarios. It registers a
# real session-leader process with the shared guard — registration itself
# appends the exact identity (PID plus start ticks) to the owner marker via
# the ``LUBKO_TEST_OWNER_MARKER`` environment variable, so no manual marker
# write exists — and behaves according to ``NESTED_MODE``:
# - ``success``: owns, stops, and unregisters its own leader cleanly;
# - ``failure``: leaks the leader on purpose, and the ``finally`` block
#   (the same teardown contract our conftest enforces) stops it exactly;
# - ``sleep``: blocks until the scenario interrupts the nested pytest.
NESTED_MODULE: Final = f'''
"""Nested interruption-mode subject: register a leader, behave on cue."""
import os
import signal
import subprocess
import sys
import time

sys.path.insert(0, {str(REPO_ROOT)!r})

from tests import _process_guard as guard  # noqa: E402


def test_subject() -> None:
    """Register a session leader (auto-recorded), then behave on cue."""
    mode = os.environ["NESTED_MODE"]

    proc = subprocess.Popen(
        ["/bin/sleep", "300"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        close_fds=True,
    )
    guard.register(proc)
    try:
        if mode == "success":
            os.killpg(proc.pid, signal.SIGTERM)
            assert proc.wait(timeout=10) == -signal.SIGTERM
            guard.unregister(proc)
            return
        if mode == "failure":
            raise AssertionError("deliberate nested failure")
        if mode == "two":
            second = subprocess.Popen(
                ["/bin/sleep", "300"],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
                close_fds=True,
            )
            guard.register(second)
        time.sleep(300)
    finally:
        guard.teardown_tracked(fail_on_leak=False)
'''


def _ambient_digest() -> dict[str, tuple[str, str]]:
    """Return the current byte-for-byte snapshot of the ambient state tree."""
    return isolation.snapshot_tree(isolation.ambient_state_root())


def _read_result(path: Path) -> dict[str, object]:
    """Read and validate an owner result JSON file.

    Args:
        path: Result file written by the independent owner.

    Returns:
        The parsed result mapping.
    """
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def _read_pidfile(path: Path) -> tuple[int, int] | None:
    """Return the recorded nested pytest ``(pid, ticks)``, or ``None``.

    Args:
        path: Pidfile written by the independent owner.

    Returns:
        The exact identity, or ``None`` when absent or invalid.
    """
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(payload, dict):
        return None
    pid = payload.get("pid")
    ticks = payload.get("ticks")
    if not isinstance(pid, int) or not isinstance(ticks, int) or ticks <= 0:
        return None
    return pid, ticks


def _wait_gone(pid: int, timeout: float) -> bool:
    """Wait until a PID has no live non-zombie /proc entry.

    Args:
        pid: PID to await.
        timeout: Maximum seconds to wait.

    Returns:
        ``True`` when the process is gone (or a reaped/zombie husk).
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        state = _proc_state(pid)
        if state is None or state == "Z":
            return True
        time.sleep(0.05)
    return False


def _proc_state(pid: int) -> str | None:
    """Return the process state letter of ``pid``, or ``None`` when gone.

    Args:
        pid: Process to inspect.

    Returns:
        The single-letter state, or ``None``.
    """
    try:
        stat = (Path("/proc") / str(pid) / "stat").read_bytes()
    except OSError:
        return None
    close = stat.rfind(b")")
    if close == -1:
        return None
    fields = stat[close + 2 :].split()
    if not fields:
        return None
    return fields[0].decode()


def _stop_nested_pytest(scenario: _Scenario) -> None:
    """Terminate the recorded nested pytest by its verified exact identity.

    Only the nested pytest is signalled; the subreaper owner stays alive so
    it can synchronously contain and reap the abandoned descendants.  The
    recorded ticks must still match immediately before every signal; an
    unresolved or reused identity is never signalled.

    Args:
        scenario: The scenario whose pidfile records the nested identity.
    """
    identity = _read_pidfile(scenario.pidfile)
    if identity is None:
        return
    pid, ticks = identity
    if not guard.signal_identity_checked(pid, ticks, signal.SIGTERM):
        return
    if not _wait_gone(pid, 10.0):
        guard.signal_identity_checked(pid, ticks, signal.SIGKILL)
        _wait_gone(pid, 10.0)


def _wait_truly_absent(pid: int, timeout: float) -> bool:
    """Wait until ``pid`` has no ``/proc`` entry at all.

    Unlike ``_wait_gone``, a zombie is NOT treated as gone: for the
    owner-retirement prerequisite a not-yet-reaped descendant can still be
    reparented to PID 1 if the subreaper owner dies first.

    Args:
        pid: PID to await.
        timeout: Maximum seconds to wait.

    Returns:
        ``True`` only when the ``/proc`` entry is fully absent.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if _proc_state(pid) is None:
            return True
        time.sleep(0.05)
    return False


def _valid_identity(entry: object) -> tuple[int, int] | None:
    """Return a validated ``(pid, ticks)`` pair from a raw marker entry.

    Args:
        entry: One raw marker entry.

    Returns:
        The validated pair, or ``None`` when malformed or out of range.
    """
    if not isinstance(entry, dict):
        return None
    pid = entry.get("pid")
    ticks = entry.get("ticks")
    if not isinstance(pid, int) or not isinstance(ticks, int) or ticks <= 0:
        return None
    return pid, ticks


def _retire_recorded_identity(pid: int, ticks: int) -> list[str]:
    """Retire one positively-identified recorded descendant.

    The identity must currently match; TERM then KILL are delivered by exact
    group, and the descendant must become truly absent (not merely zombie)
    before the prerequisite counts as satisfied.

    Args:
        pid: Exact PID of the recorded descendant.
        ticks: Recorded start ticks (already validated current-matching).

    Returns:
        Problems encountered; empty on positive proof of absence.
    """
    for sig in (signal.SIGTERM, signal.SIGKILL):
        if _wait_truly_absent(pid, 5.0):
            return []
        guard.signal_identity_checked(pid, ticks, sig)
    if _wait_truly_absent(pid, 5.0):
        return []
    state = _proc_state(pid)
    if state == "Z":
        return [
            (
                f"descendant pid {pid} is a zombie; not reaped, so retiring "
                "the subreaper owner could reparent it to PID 1"
            )
        ]
    return [f"descendant pid {pid} did not retire"]


def _retire_marker_descendants(marker: Path) -> list[str]:
    """Positively prove every marker-recorded descendant is contained.

    Precise prerequisite for retiring the subreaper owner:

    - an already-truly-absent recorded identity is safe;
    - a live exact identity may be TERM/KILLed but must become truly absent;
    - a live mismatched/unverifiable identity blocks owner signalling;
    - a zombie/not-reaped descendant blocks owner signalling unless it
      becomes truly absent while the owner remains alive;
    - missing/malformed markers or invalid entries also block.

    Args:
        marker: Marker file in append-only JSONL form (one
            ``{"pid": ..., "ticks": ...}`` object per line).

    Returns:
        Human-readable problems; empty only when every recorded descendant
        is positively proven gone (truly absent).
    """
    entries, unproven = _read_marker_entries(marker)
    if unproven:
        return ["marker coverage unproven (missing/malformed/torn); cannot retire owner"]

    problems: list[str] = []
    for entry in entries:
        identity = _valid_identity(entry)
        if identity is None:
            problems.append(f"marker entry {entry!r} has missing/invalid identity")
            continue
        pid, ticks = identity
        state = _proc_state(pid)
        if state is None:
            # Already truly absent: safe.
            continue
        if guard.proc_start_ticks(pid) != ticks:
            problems.append(f"descendant pid {pid} identity unresolved/reused; never signalled")
            continue
        problems.extend(_retire_recorded_identity(pid, ticks))
    return problems


@dataclass
class OwnedRun:
    """Handle for a converging owned nested run."""

    owner: subprocess.Popen[bytes]
    scenario: _Scenario


@contextmanager
def owned_run(
    scenario: _Scenario,
    start: Callable[[], subprocess.Popen[bytes]],
) -> Iterator[OwnedRun]:
    """Own an independent-owner nested run across every outer outcome.

    On normal completion, outer-test failure, or timeout, the finalizer
    converges synchronously:

    1. terminate the recorded nested pytest by exact PID+start-ticks while
       keeping the subreaper owner alive to contain/reap descendants;
    2. wait bounded for the owner to exit;
    3. only if the owner is wedged, retire every marker-recorded descendant
       fail-closed first, then retire the exact registered owner identity,
       and raise loudly — chained with the original outer exception.

    Cleanup never masks the original outer-test error unless cleanup itself
    could not converge, in which case both are reported.  The registry
    forgets the owner only once it is proven terminal/reaped.

    Args:
        scenario: Scenario artifacts (pidfile/marker/result paths).
        start: Callable starting the owner process.

    Yields:
        The owned-run handle.
    """
    owner = start()
    guard.register(owner)
    handle = OwnedRun(owner=owner, scenario=scenario)
    try:
        yield handle
    finally:
        _converge_owner(handle)


def _retire_wedged_owner(handle: OwnedRun, problems: list[str]) -> None:
    """Retire a wedged owner fail-closed after its recorded descendants.

    Descendant marker coverage is a POSITIVE prerequisite to owner
    retirement: a missing/malformed marker, any invalid/missing-tick or
    unresolved/reused descendant identity, or any descendant not proven gone
    blocks ALL owner signalling — killing the subreaper owner before its
    adopted session-leader descendants could strand them under container
    PID 1.  Only once every recorded descendant is proven gone is the owner
    signalled under its registration-time PID+start-ticks identity,
    re-verified immediately before every TERM/KILL; an unresolved or reused
    owner identity is never signalled and is reported instead.

    Args:
        handle: The owned-run handle whose owner wedged.
        problems: Accumulating cleanup-problem descriptions.
    """
    owner = handle.owner
    problems.extend(_retire_marker_descendants(handle.scenario.marker))
    if problems:
        # Descendant containment is unresolved: the subreaper owner must
        # stay alive and untouched so nothing can leak under PID 1.
        problems.append("owner left unsignalled: descendant containment unresolved")
        return
    tracked = guard.TRACKED.get(owner.pid)
    ticks = tracked.start_ticks if tracked is not None else None
    for sig in (signal.SIGTERM, signal.SIGKILL):
        if _wait_gone(owner.pid, 5.0):
            return
        if not guard.signal_identity_checked(owner.pid, ticks, sig):
            problems.append(f"owner pid {owner.pid} identity unresolved; occupant not signalled")
            return
    _wait_gone(owner.pid, 5.0)
    try:
        owner.wait(timeout=10.0)
    except subprocess.TimeoutExpired:
        problems.append(f"owner pid {owner.pid} could not be reaped")


def _converge_owner(handle: OwnedRun) -> None:
    """Synchronously converge one owned run without masking outer errors.

    Args:
        handle: The owned-run handle.

    Raises:
        AssertionError: When cleanup itself cannot converge; chained with
            the original outer exception where one is in flight.
    """
    owner = handle.owner
    scenario = handle.scenario
    outer = sys.exc_info()[1]
    problems: list[str] = []

    if owner.poll() is None:
        # Stop only the nested pytest; keep the subreaper owner alive so
        # adopted descendants are contained and reaped by it.
        _stop_nested_pytest(scenario)
        try:
            owner.wait(timeout=30.0)
        except subprocess.TimeoutExpired:
            # Wedged: retire marker-recorded descendants first — killing the
            # owner before them could strand adopted session leaders.
            _retire_wedged_owner(handle, problems)

    if owner.poll() is None:
        # Never unregister a live process; convergence failed loudly below.
        problems.append(f"owner pid {owner.pid} still live after cleanup")
    else:
        guard.unregister(owner)
        if owner.returncode != 0:
            problems.append(f"owner exited {owner.returncode}")
        result_path = scenario.result
        if not result_path.exists():
            problems.append("owner exited without writing a containment result")
        else:
            payload = _read_result(result_path)
            if payload["contained"] is not True:
                problems.append("owner reported containment failure")
            if payload["observed_ppid_1"] is not False:
                problems.append("a test-owned process was observed under PID 1")
            if payload["identity_mismatch"] is not False:
                problems.append("owner observed a stale marker identity")

    if problems:
        msg = "independent-owner cleanup did not converge: " + "; ".join(problems)
        raise AssertionError(msg) from outer


class _Scenario:
    """One owned nested-run scenario."""

    def __init__(self, workspace: Path, name: str) -> None:
        """Create the scenario artifacts.

        Args:
            workspace: Scratch directory for artifacts.
            name: Unique scenario name.
        """
        self.module = workspace / f"nested_{name}.py"
        self.marker = workspace / f"nested_{name}.marker.json"
        self.result = workspace / f"nested_{name}.result.json"
        self.pidfile = workspace / f"nested_{name}.pid"
        self.log = workspace / f"nested_{name}.log"

    def _start_owner(
        self,
        pytest_args: list[str],
        deadline: float,
        env_overrides: dict[str, str] | None = None,
    ) -> subprocess.Popen[bytes]:
        """Start the independent owner around a nested pytest command.

        Args:
            pytest_args: Arguments after ``python -m pytest``.
            deadline: Owner-enforced wall-clock limit for the nested run.
            env_overrides: Additional environment for the nested run.

        Returns:
            The independent owner process.
        """
        env = dict(os.environ)
        env["PYTEST_ADDOPTS"] = "-p no:cacheprovider"
        # Canonical automatic recording: every successful guard registration
        # inside the nested run appends its exact identity to this marker.
        env[guard.OWNER_MARKER_ENV] = str(self.marker)
        if env_overrides:
            env.update(env_overrides)
        return subprocess.Popen(
            [
                sys.executable,
                "-m",
                "tests._nested_owner",
                "--marker",
                str(self.marker),
                "--result",
                str(self.result),
                "--pidfile",
                str(self.pidfile),
                "--deadline",
                str(deadline),
                "--",
                sys.executable,
                "-m",
                "pytest",
                *pytest_args,
            ],
            cwd=REPO_ROOT,
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=self.log.open("ab"),
            stderr=subprocess.STDOUT,
        )

    def run(self, mode: str, deadline: float = 120.0) -> subprocess.Popen[bytes]:
        """Start the independently-owned nested interruption-mode subject.

        Args:
            mode: The ``NESTED_MODE`` behaviour of the nested subject.
            deadline: Owner-enforced wall-clock limit for the nested run.

        Returns:
            The independent owner process.
        """
        self.module.write_text(NESTED_MODULE, encoding="utf-8")
        return self._start_owner(
            [str(self.module), "-q", "--no-header", "-p", "no:cacheprovider"],
            deadline=deadline,
            env_overrides={"NESTED_MODE": mode},
        )

    def run_module(
        self,
        module: Path,
        deadline: float = 120.0,
        env_overrides: dict[str, str] | None = None,
    ) -> subprocess.Popen[bytes]:
        """Start an owned nested pytest run of an arbitrary generated module.

        Public harness entry point so same-package regressions can drive
        custom nested subjects without reaching into private helpers.  The
        owner-marker environment variable is set exactly as for every other
        scenario, so guard registrations inside the module are recorded
        automatically.

        Args:
            module: Generated test module to run.
            deadline: Owner-enforced wall-clock limit for the nested run.
            env_overrides: Additional environment for the nested run.

        Returns:
            The independent owner process.
        """
        return self._start_owner(
            [str(module), "-q", "--no-header", "-p", "no:cacheprovider"],
            deadline=deadline,
            env_overrides=env_overrides,
        )

    def run_isolation_module(self, deadline: float = 300.0) -> subprocess.Popen[bytes]:
        """Start an owned nested run of the isolation regression module.

        Args:
            deadline: Owner-enforced wall-clock limit for the nested run.

        Returns:
            The independent owner process.
        """
        del self.module
        return self._start_owner(
            ["tests/test_isolation.py", "-q", "--no-header", "-p", "no:cacheprovider"],
            deadline=deadline,
        )

    def run_suite_selection(
        self,
        pytest_args: list[str],
        deadline: float = 3600.0,
    ) -> subprocess.Popen[bytes]:
        """Start an owned nested run of a canonical suite selection.

        Args:
            pytest_args: Canonical selection arguments after ``pytest``.
            deadline: Owner-enforced wall-clock limit for the nested run.

        Returns:
            The independent owner process.
        """
        del self.module
        return self._start_owner([*pytest_args], deadline=deadline)


def _wait_file(path: Path, timeout: float = 60.0) -> None:
    """Wait until a file exists.

    Args:
        path: File to await.
        timeout: Maximum seconds to wait.

    Raises:
        AssertionError: If the file never appears.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            return
        time.sleep(0.05)
    msg = f"expected file never appeared: {path}"
    raise AssertionError(msg)


def _read_marker_entries(marker: Path) -> tuple[list[dict[str, object]], bool]:
    """Parse the append-only JSONL owner marker from the test side.

    Mirrors the independent owner's fail-closed semantics: a missing file,
    an unparseable line, a torn final line without a terminating newline,
    or a non-object line is unproven coverage.

    Args:
        marker: Marker path to parse.

    Returns:
        The valid entries and whether coverage is unproven.
    """
    try:
        raw = marker.read_text(encoding="utf-8")
    except OSError:
        return [], True
    if raw and not raw.endswith("\n"):
        return [], True
    entries: list[dict[str, object]] = []
    for line in raw.splitlines():
        if not line.strip():
            continue
        try:
            entry = json.loads(line)
        except ValueError:
            return [], True
        if not isinstance(entry, dict):
            return [], True
        entries.append(entry)
    return entries, False


def _write_marker_entries(marker: Path, entries: list[dict[str, int]]) -> None:
    """Write identities in the append-only JSONL marker format.

    Args:
        marker: Marker path to write.
        entries: The ``{"pid": ..., "ticks": ...}`` identities.
    """
    lines = "".join(json.dumps(entry, sort_keys=True) + "\n" for entry in entries)
    marker.write_text(lines, encoding="utf-8")


def _assert_containment(result_path: Path) -> dict[str, object]:
    """Assert the owner reports full PID-1-free containment.

    Args:
        result_path: Result file of the finished owner.

    Returns:
        The parsed result mapping.
    """
    result = _read_result(result_path)
    assert result["subreaper"] is True
    assert result["contained"] is True
    assert result["observed_ppid_1"] is False
    assert result["identity_mismatch"] is False
    return result


@pytest.mark.parametrize(
    ("mode", "deadline"),
    [("success", 120.0), ("failure", 120.0)],
)
def test_managed_modes_retire_their_own_process_exactly(
    tmp_path: Path,
    mode: str,
    deadline: float,
) -> None:
    """Success and assertion-failure paths clean up within the nested process."""
    before = _ambient_digest()
    scenario = _Scenario(tmp_path, mode)
    with owned_run(scenario, lambda: scenario.run(mode, deadline=deadline)) as handle:
        _wait_file(scenario.pidfile)
        assert handle.owner.wait(timeout=180) == 0
    result = _assert_containment(scenario.result)
    assert result["returncode"] == (0 if mode == "success" else 1)
    if mode == "failure":
        log = scenario.log.read_text(errors="replace")
        assert "deliberate nested failure" in log
    assert isolation.ambient_sentinel_alive()
    assert _ambient_digest() == before


@pytest.mark.parametrize(
    ("sig", "name"),
    [(signal.SIGINT, "sigint"), (signal.SIGTERM, "sigterm"), (signal.SIGKILL, "sigkill")],
)
def test_interrupted_nested_run_is_contained_never_reparented_to_pid_1(
    tmp_path: Path,
    sig: signal.Signals,
    name: str,
) -> None:
    """SIGINT/SIGTERM/abrupt SIGKILL still contain descendants under the owner."""
    before = _ambient_digest()
    scenario = _Scenario(tmp_path, name)
    with owned_run(scenario, lambda: scenario.run("sleep")) as handle:
        _wait_file(scenario.pidfile)
        identity = _read_pidfile(scenario.pidfile)
        assert identity is not None
        nested_pid = identity[0]
        deadline = time.monotonic() + 60.0
        entries, unproven = _read_marker_entries(scenario.marker)
        while (unproven or not entries) and time.monotonic() < deadline:
            if handle.owner.poll() is not None:
                break
            time.sleep(0.05)
            entries, unproven = _read_marker_entries(scenario.marker)
        assert entries, scenario.log.read_text(errors="replace")
        assert not unproven
        # Interrupt only the nested pytest, by exact PID; the independent
        # owner survives and must synchronously reap the abandoned leader.
        os.kill(nested_pid, sig)
        while not scenario.result.exists():
            if handle.owner.poll() is not None:
                break
            time.sleep(0.05)
        assert handle.owner.wait(timeout=60) == 0
    result = _assert_containment(scenario.result)
    if sig is not signal.SIGINT:
        # SIGTERM/SIGKILL give the nested interpreter no chance to run its
        # own teardown, so the owner must have observed and reaped a live
        # survivor.  SIGINT raises KeyboardInterrupt inside the nested run,
        # whose finally-block teardown stops the leader itself.
        assert result["survivor_seen"] is True
    assert isolation.ambient_sentinel_alive()
    assert _ambient_digest() == before


def test_owner_enforced_timeout_kills_and_contains(tmp_path: Path) -> None:
    """A nested run exceeding its deadline is killed and fully contained."""
    before = _ambient_digest()
    scenario = _Scenario(tmp_path, "timeout")
    with owned_run(scenario, lambda: scenario.run("sleep", deadline=3.0)) as handle:
        _wait_file(scenario.pidfile)
        deadline = time.monotonic() + 30.0
        entries, unproven = _read_marker_entries(scenario.marker)
        while (unproven or not entries) and time.monotonic() < deadline:
            if handle.owner.poll() is not None:
                break
            time.sleep(0.05)
            entries, unproven = _read_marker_entries(scenario.marker)
        assert entries
        assert not unproven
        assert handle.owner.wait(timeout=120) == 0
    result = _assert_containment(scenario.result)
    assert result["timed_out"] is True
    assert result["survivor_seen"] is True
    assert isolation.ambient_sentinel_alive()
    assert _ambient_digest() == before


TWO_REGISTRATIONS_MODULE: Final = """
import os, signal, subprocess, sys, time

sys.path.insert(0, "__REPO_ROOT__")

from tests import _process_guard as guard


def test_two_registrations() -> None:
    '''Register two session leaders, then tear them down cleanly.'''
    first = subprocess.Popen(
        ["/bin/sleep", "300"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        close_fds=True,
    )
    second = subprocess.Popen(
        ["/bin/sleep", "300"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        close_fds=True,
    )
    guard.register(first)
    guard.register(second)
    try:
        if os.environ.get("NESTED_MODE") == "sleep":
            time.sleep(300)
    finally:
        guard.teardown_tracked(fail_on_leak=False)
""".replace("__REPO_ROOT__", str(REPO_ROOT))


def test_auto_recorded_marker_contains_both_identities(tmp_path: Path) -> None:
    """Nested registrations are recorded automatically, exactly.

    A nested pytest run registers two distinct session-leader processes;
    after the run, the owner marker must contain both exact ``{pid, ticks}``
    identities without any manual marker write by the nested subject.

    Args:
        tmp_path: Pytest temporary directory.
    """
    module = tmp_path / "auto_record.py"
    module.write_text(TWO_REGISTRATIONS_MODULE, encoding="utf-8")
    scenario = _Scenario(tmp_path, "autorecord")
    with owned_run(scenario, partial(scenario.run_module, module)) as handle:
        assert handle.owner.wait(timeout=120) == 0
    entries, unproven = _read_marker_entries(scenario.marker)
    assert not unproven
    identities: list[tuple[int, int]] = []
    for entry in entries:
        pid = entry.get("pid")
        ticks = entry.get("ticks")
        assert isinstance(pid, int)
        assert isinstance(ticks, int)
        identities.append((pid, ticks))
    pids = sorted(pid for pid, _ticks in identities)
    assert len(pids) == 2
    for pid, ticks in identities:
        assert ticks > 0
        # Both identities were torn down by the nested run's own teardown.
        assert _proc_state(pid) is None


def test_abrupt_termination_still_leaves_recorded_identities(tmp_path: Path) -> None:
    """Identities recorded at registration survive abrupt nested death.

    The nested run registers two leaders and is then SIGKILLed before any
    cleanup; the owner must still find both exact identities in the marker
    and contain them (this is exercised through converge on exit).

    Args:
        tmp_path: Pytest temporary directory.
    """
    before = _ambient_digest()
    scenario = _Scenario(tmp_path, "abrupt-record")
    with owned_run(scenario, lambda: scenario.run("two")) as handle:
        deadline = time.monotonic() + 60.0
        entries, unproven = _read_marker_entries(scenario.marker)
        while (unproven or len(entries) < 2) and time.monotonic() < deadline:
            if handle.owner.poll() is not None:
                break
            time.sleep(0.05)
            entries, unproven = _read_marker_entries(scenario.marker)
        assert not unproven
        assert len(entries) == 2
        recorded: list[int] = []
        for entry in entries:
            pid = entry.get("pid")
            assert isinstance(pid, int)
            recorded.append(pid)
        # Abruptly kill only the nested pytest; owner must contain both.
        identity = _read_pidfile(scenario.pidfile)
        assert identity is not None
        os.kill(identity[0], signal.SIGKILL)
        assert handle.owner.wait(timeout=60) == 0
    payload = _assert_containment(scenario.result)
    assert payload["survivor_seen"] is True
    for pid in recorded:
        wait_gone_deadline = time.monotonic() + 10.0
        while _proc_state(pid) is not None and time.monotonic() < wait_gone_deadline:
            time.sleep(0.05)
        assert _proc_state(pid) is None or _proc_state(pid) == "Z"
    assert isolation.ambient_sentinel_alive()
    assert _ambient_digest() == before


def test_forced_outer_failure_converges_without_pid_1_orphans(tmp_path: Path) -> None:
    """An outer-test failure mid-run converges: every identity ends gone.

    The finalizer must interrupt the recorded nested pytest by its verified
    exact identity, keep the subreaper owner alive to contain/reap the
    abandoned session-leader descendant, and prove that owner, nested
    pytest, and marker-recorded descendant are all gone afterwards — none
    ever observed under PID 1.

    Args:
        tmp_path: Pytest temporary directory.
    """
    before = _ambient_digest()
    scenario = _Scenario(tmp_path, "outerfail")

    handles: list[OwnedRun] = []

    def failing_body() -> None:
        with owned_run(scenario, partial(scenario.run, "sleep")) as handle:
            handles.append(handle)
            _wait_file(scenario.pidfile)
            deadline = time.monotonic() + 60.0
            entries, _unproven = _read_marker_entries(scenario.marker)
            while not entries and time.monotonic() < deadline:
                if handle.owner.poll() is not None:
                    break
                time.sleep(0.05)
                entries, _unproven = _read_marker_entries(scenario.marker)
            assert entries, "nested registration was never auto-recorded"
            failure = RuntimeError("boom")
            raise failure

    with pytest.raises(RuntimeError, match="boom"):
        failing_body()

    handle = handles[0]

    # The original outer exception propagated unmasked, and cleanup still
    # converged: everything recorded is terminal and containment held.
    assert handle.owner.poll() is not None
    nested = _read_pidfile(scenario.pidfile)
    assert nested is not None
    entries, unproven = _read_marker_entries(scenario.marker)
    assert not unproven
    assert entries
    leader_pid = entries[0].get("pid")
    assert isinstance(leader_pid, int)
    for pid in (nested[0], leader_pid):
        assert _proc_state(pid) is None or _proc_state(pid) == "Z"
    payload = _read_result(scenario.result)
    assert payload["contained"] is True
    assert payload["observed_ppid_1"] is False
    assert isolation.ambient_sentinel_alive()
    assert _ambient_digest() == before


NONLEADER_MODULE: Final = """
import os, signal, subprocess, sys, time

sys.path.insert(0, "__REPO_ROOT__")

from tests import _process_guard as guard


def test_nonleader_survivor() -> None:
    '''Register a non-group-leader sleeper plus an unregistered sibling.

    Both share the nested pytest's process group.  Containment of the
    registered survivor must therefore use an exact-PID signal: a shared
    group signal would kill the unregistered sibling (and the owner).
    '''
    survivor = subprocess.Popen(
        ["/bin/sleep", "300"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        close_fds=True,
    )
    sibling = subprocess.Popen(
        ["/bin/sleep", "300"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        close_fds=True,
    )
    assert os.getpgid(survivor.pid) != survivor.pid
    guard.register(survivor)
    with open(os.environ["NESTED_SIDECAR"], "w") as handle:
        handle.write(str(survivor.pid) + " " + str(sibling.pid) + "\\n")
    time.sleep(300)
""".replace("__REPO_ROOT__", str(REPO_ROOT))


def test_nonleader_survivor_contained_exactly_without_shared_group_signal(
    tmp_path: Path,
) -> None:
    """A non-leader survivor is killed by exact PID; siblings stay alive.

    After abrupt nested-pytest death the owner must contain the registered
    non-group-leader descendant by revalidated exact-PID signalling — never
    by signalling its shared group, which still contains the unregistered
    innocent sibling (and would reach the owner itself).

    Args:
        tmp_path: Pytest temporary directory.
    """
    before = _ambient_digest()
    scenario = _Scenario(tmp_path, "nonleader")
    sidecar = tmp_path / "nonleader.sidecar"
    module = tmp_path / "nonleader.py"
    module.write_text(NONLEADER_MODULE, encoding="utf-8")
    with owned_run(
        scenario,
        partial(
            scenario.run_module,
            module,
            env_overrides={"NESTED_SIDECAR": str(sidecar)},
        ),
    ) as handle:
        deadline = time.monotonic() + 60.0
        while not sidecar.exists() and time.monotonic() < deadline:
            if handle.owner.poll() is not None:
                break
            time.sleep(0.05)
        assert sidecar.exists(), scenario.log.read_text(errors="replace")
        entries, unproven = _read_marker_entries(scenario.marker)
        while (unproven or not entries) and time.monotonic() < deadline:
            if handle.owner.poll() is not None:
                break
            time.sleep(0.05)
            entries, unproven = _read_marker_entries(scenario.marker)
        assert entries
        assert not unproven
        # Abruptly kill only the nested pytest.
        identity = _read_pidfile(scenario.pidfile)
        assert identity is not None
        os.kill(identity[0], signal.SIGKILL)
        assert handle.owner.wait(timeout=60) == 0

    survivor_pid_text, sibling_pid_text = sidecar.read_text().split()
    survivor_pid = int(survivor_pid_text)
    sibling_pid = int(sibling_pid_text)
    # After nested-pytest death BOTH processes are adopted descendants of
    # the subreaper; the lifetime rule requires the owner to contain every
    # one of them before returning — registered or not.
    wait_gone_deadline = time.monotonic() + 30.0
    while time.monotonic() < wait_gone_deadline and (
        _proc_state(survivor_pid) is not None or _proc_state(sibling_pid) is not None
    ):
        time.sleep(0.05)
    assert _proc_state(survivor_pid) in {None, "Z"}
    assert _proc_state(sibling_pid) in {None, "Z"}
    payload = _assert_containment(scenario.result)
    assert payload["survivor_seen"] is True
    assert isolation.ambient_sentinel_alive()
    assert _ambient_digest() == before


def test_repeated_nested_isolation_runs_leave_ambient_untouched(
    tmp_path: Path,
) -> None:
    """Two back-to-back nested isolation-module runs prove repeatable safety.

    Each nested run executes the full per-test isolation regression set under
    an independent owner; between and after both runs this session's ambient
    sentinel stays alive and its production-like state tree remains
    byte-for-byte identical.
    """
    assert isolation.ambient_sentinel_alive()
    digest_before = _ambient_digest()
    log = tmp_path / "repeated.log"
    for iteration in range(2):
        scenario = _Scenario(tmp_path, f"repeat{iteration}")
        with owned_run(scenario, partial(scenario.run_isolation_module)) as handle:
            assert handle.owner.wait(timeout=300) == 0, log.read_text(errors="replace")[-2000:]
        assert isolation.ambient_sentinel_alive()
        assert _ambient_digest() == digest_before


def test_owner_refuses_missing_marker_ticks_and_never_signals(
    tmp_path: Path,
) -> None:
    """A marker entry without valid ticks is unresolved containment failure.

    The independent owner must not signal an identity whose recorded start
    ticks are missing: an innocent live occupant of that PID stays untouched
    and the owner reports unresolved containment (nonzero exit).

    Args:
        tmp_path: Pytest temporary directory.
    """
    marker = tmp_path / "missing-ticks.marker.json"
    result = tmp_path / "missing-ticks.result.json"
    pidfile = tmp_path / "missing-ticks.pid"
    innocent = subprocess.Popen(
        ["/bin/sleep", "60"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        # Deliberately omit "ticks" from the marker record.
        marker.write_text(
            json.dumps({"pid": innocent.pid}, sort_keys=True) + "\n", encoding="utf-8"
        )
        done = subprocess.run(
            [
                sys.executable,
                "-m",
                "tests._nested_owner",
                "--marker",
                str(marker),
                "--result",
                str(result),
                "--pidfile",
                str(pidfile),
                "--deadline",
                "30",
                "--",
                sys.executable,
                "-c",
                "pass",
            ],
            cwd=REPO_ROOT,
            env=dict(os.environ),
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )
        assert done.returncode == 1
        payload = _read_result(result)
        assert payload["unresolved_ticks"] is True
        assert payload["contained"] is False
        assert payload["observed_ppid_1"] is False
        assert innocent.poll() is None
    finally:
        if innocent.poll() is None:
            innocent.kill()
            innocent.wait(timeout=10)


def test_owner_fails_closed_when_subreaper_setup_fails(tmp_path: Path) -> None:
    """Without proven subreaper ownership nothing is spawned and we fail.

    Args:
        tmp_path: Pytest temporary directory.
    """
    result = tmp_path / "subreaper.result.json"
    spawned = tmp_path / "spawned.marker"
    rc = _nested_owner.main(
        [
            "--marker",
            str(tmp_path / "subreaper.marker.json"),
            "--result",
            str(result),
            "--pidfile",
            str(tmp_path / "subreaper.pid"),
            "--deadline",
            "10",
            "--",
            sys.executable,
            "-c",
            f"Path({str(spawned)!r}).write_text('spawned')",
        ],
        become_subreaper=lambda: False,
    )
    assert rc == 1
    payload = _read_result(result)
    assert payload["subreaper"] is False
    assert payload["contained"] is False
    assert not spawned.exists()


@pytest.mark.parametrize(
    ("marker_body", "reason"),
    [
        ("not json at all\n", "malformed"),
        ('{"pid": 123', "torn-partial-line"),
        ('"just a string"\n', "non-object-line"),
    ],
)
def test_owner_fails_closed_on_unproven_marker_coverage(
    tmp_path: Path,
    marker_body: str,
    reason: str,
) -> None:
    """Malformed/torn/non-object marker coverage fails closed.

    The owner itself initializes empty marker storage before spawning, so a
    missing file cannot occur in a legitimate run; corruption introduced
    after initialization must be caught instead.

    Args:
        tmp_path: Pytest temporary directory.
        marker_body: Raw corrupt marker content.
        reason: Short label asserted to keep cases distinct.
    """
    assert reason
    result = tmp_path / "coverage.result.json"
    marker = tmp_path / "coverage.marker.json"
    marker.write_text(marker_body, encoding="utf-8")
    # Run the owner in an independent subprocess: the outer pytest process
    # must never acquire PR_SET_CHILD_SUBREAPER itself.
    done = subprocess.run(
        [
            sys.executable,
            "-m",
            "tests._nested_owner",
            "--marker",
            str(marker),
            "--result",
            str(result),
            "--pidfile",
            str(tmp_path / "coverage.pid"),
            "--deadline",
            "30",
            "--",
            sys.executable,
            "-c",
            "pass",
        ],
        cwd=REPO_ROOT,
        env=dict(os.environ),
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert done.returncode == 1
    payload = _read_result(result)
    assert payload["contained"] is False


def test_owner_fails_closed_when_child_removes_marker(tmp_path: Path) -> None:
    """A nested child that unlinks the owner marker fails containment.

    Public-path regression: the nested subject removes the marker file the
    owner initialized, so at containment time coverage is missing even
    though storage was validly initialized.  The owner must report
    unproven coverage, exit nonzero, and never claim success.

    Args:
        tmp_path: Pytest temporary directory.
    """
    result = tmp_path / "removed-marker.result.json"
    marker = tmp_path / "removed-marker.marker.json"
    # Independent subprocess: the outer pytest must not become a subreaper.
    done = subprocess.run(
        [
            sys.executable,
            "-m",
            "tests._nested_owner",
            "--marker",
            str(marker),
            "--result",
            str(result),
            "--pidfile",
            str(tmp_path / "removed-marker.pid"),
            "--deadline",
            "30",
            "--",
            sys.executable,
            "-c",
            f"import os; os.unlink({str(marker)!r})",
        ],
        cwd=REPO_ROOT,
        env=dict(os.environ),
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert done.returncode == 1
    payload = _read_result(result)
    assert payload["coverage_unproven"] is True
    assert payload["contained"] is False


def _wait_for_child_of(nested_pid: int | None, timeout: float) -> int | None:
    """Wait until a live direct child of ``nested_pid`` appears.

    Args:
        nested_pid: Parent PID from the owner pidfile, if known yet.
        timeout: Maximum seconds to wait.

    Returns:
        The discovered child PID, or ``None`` on timeout.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if nested_pid is not None:
            for entry in Path("/proc").iterdir():
                if not entry.name.isdigit():
                    continue
                try:
                    stat = (entry / "stat").read_bytes()
                except OSError:
                    continue
                close = stat.rfind(b")")
                if close == -1:
                    continue
                fields = stat[close + 2 :].split()
                if len(fields) >= 4 and int(fields[1]) == nested_pid:
                    return int(entry.name)
        time.sleep(0.05)
    return None


def _start_torn_coverage_owner(
    module: Path,
    result: Path,
    marker: Path,
    pidfile: Path,
    log: Path,
) -> subprocess.Popen[bytes]:
    """Start the independent owner around the torn-coverage subject.

    Args:
        module: Generated subject module.
        result: Owner result path.
        marker: Owner marker path.
        pidfile: Owner pidfile path.
        log: Log file for combined output.

    Returns:
        The owner process.
    """
    env = dict(os.environ)
    # Canonical automatic recording: registrations append to this marker;
    # the generated subject then deliberately deletes it to prove that torn
    # coverage never orphans a live descendant.
    env[guard.OWNER_MARKER_ENV] = str(marker)
    return subprocess.Popen(
        [
            sys.executable,
            "-m",
            "tests._nested_owner",
            "--marker",
            str(marker),
            "--result",
            str(result),
            "--pidfile",
            str(pidfile),
            "--deadline",
            "60",
            "--",
            sys.executable,
            "-m",
            "pytest",
            str(module),
            "-q",
            "--no-header",
            "-p",
            "no:cacheprovider",
        ],
        cwd=REPO_ROOT,
        env=env,
        stdin=subprocess.DEVNULL,
        stdout=log.open("ab"),
        stderr=subprocess.STDOUT,
    )


def test_owner_stays_alive_and_contains_despite_torn_coverage(tmp_path: Path) -> None:
    """Torn/deleted coverage never orphans a live registered descendant.

    The nested subject registers a session-leader sleeper (auto-recorded),
    then deletes the owner marker, then blocks.  After an abrupt SIGKILL of
    the nested pytest the owner must still discover the adopted descendant,
    contain it by its exact identity, and only then exit — with unproven
    coverage reported and no PID-1 transition ever observed.  Executed as
    an independent subprocess so the outer pytest never becomes a
    subreaper.

    Args:
        tmp_path: Pytest temporary directory.
    """
    before = _ambient_digest()
    module = tmp_path / "torn_coverage_subject.py"
    module.write_text(
        f'''
import os
import subprocess
import sys
import time

sys.path.insert(0, {str(REPO_ROOT)!r})

from tests import _process_guard as guard


def test_register_then_delete_marker() -> None:
    """Register a leader, delete the marker, and block forever."""
    proc = subprocess.Popen(
        ["/bin/sleep", "300"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        close_fds=True,
    )
    guard.register(proc)
    os.unlink(os.environ[{guard.OWNER_MARKER_ENV!r}])
    time.sleep(300)
''',
        encoding="utf-8",
    )
    result = tmp_path / "torn.result.json"
    marker = tmp_path / "torn.marker.json"
    pidfile = tmp_path / "torn.pid"
    log = tmp_path / "torn.log"
    owner = _start_torn_coverage_owner(module, result, marker, pidfile, log)
    try:
        deadline = time.monotonic() + 60.0
        nested_pid: int | None = None
        leader_pid: int | None = None
        while time.monotonic() < deadline:
            if nested_pid is None and pidfile.exists():
                payload = json.loads(pidfile.read_text(encoding="utf-8"))
                candidate = payload.get("pid")
                if isinstance(candidate, int):
                    nested_pid = candidate
            leader_pid = _wait_for_child_of(nested_pid, timeout=1.0)
            if leader_pid is not None:
                break
        assert leader_pid is not None, "registered descendant never appeared"
        # Abruptly kill only the nested pytest; coverage was already torn.
        assert nested_pid is not None
        os.kill(nested_pid, signal.SIGKILL)
        assert owner.wait(timeout=120) == 1
        payload = _read_result(result)
        assert payload["coverage_unproven"] is True
        assert payload["contained"] is False
        assert payload["observed_ppid_1"] is False
        assert payload["survivor_seen"] is True
        wait_gone_deadline = time.monotonic() + 10.0
        while _proc_state(leader_pid) is not None and time.monotonic() < wait_gone_deadline:
            time.sleep(0.05)
        assert _proc_state(leader_pid) in {None, "Z"}
    finally:
        if owner.poll() is None:
            owner.kill()
            owner.wait(timeout=10)
    assert isolation.ambient_sentinel_alive()
    assert _ambient_digest() == before


def test_retire_marker_descendants_accepts_already_absent(tmp_path: Path) -> None:
    """An already truly-absent recorded identity is positively safe.

    Args:
        tmp_path: Pytest temporary directory.
    """
    gone = subprocess.Popen(
        [sys.executable, "-c", "pass"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    ticks = guard.proc_start_ticks(gone.pid)
    assert ticks is not None
    gone.kill()
    gone.wait(timeout=10)
    # Fully reaped: /proc entry is truly absent while its old ticks remain
    # recorded in the marker.
    marker = tmp_path / "absent.marker.json"
    _write_marker_entries(marker, [{"pid": gone.pid, "ticks": ticks}])
    problems = _retire_marker_descendants(marker)
    assert problems == []


def test_retire_marker_descendants_blocks_on_zombie(tmp_path: Path) -> None:
    """A not-yet-reaped zombie descendant blocks owner retirement proof.

    Args:
        tmp_path: Pytest temporary directory.
    """
    zombie = subprocess.Popen(
        [sys.executable, "-c", "pass"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    deadline = time.monotonic() + 30.0
    while zombie.poll() is None and time.monotonic() < deadline:
        time.sleep(0.02)
    # poll() reaped it already on some platforms; force the zombie window
    # deterministically via fork so the child stays unreaped.
    if _proc_state(zombie.pid) is None:
        pid = os.fork()
        if pid == 0:  # pragma: no cover - child side
            os._exit(0)
        deadline = time.monotonic() + 30.0
        while _proc_state(pid) != "Z" and time.monotonic() < deadline:
            time.sleep(0.02)
        ticks = guard.proc_start_ticks(pid)
        assert ticks is not None
        try:
            marker = tmp_path / "zombie.marker.json"
            _write_marker_entries(marker, [{"pid": pid, "ticks": ticks}])
            problems = _retire_marker_descendants(marker)
            assert any("zombie" in problem for problem in problems)
        finally:
            os.waitpid(pid, 0)
    else:
        ticks = guard.proc_start_ticks(zombie.pid)
        assert ticks is not None
        marker = tmp_path / "zombie.marker.json"
        _write_marker_entries(marker, [{"pid": zombie.pid, "ticks": ticks}])
        problems = _retire_marker_descendants(marker)
        assert problems == []
        zombie.wait(timeout=10)


# Canonical selection for in-suite repository stress: the whole repository
# test suite except the stress harness itself, which is excluded because a
# nested session spawning further nested sessions would recurse unboundedly.
CANONICAL_SUITE_ARGS: Final = (
    "tests",
    "--ignore=tests/test_isolation_stress.py",
    "-q",
    "--no-header",
    "-p",
    "no:cacheprovider",
)


def test_wedged_owner_not_signalled_while_descendant_unresolved(
    tmp_path: Path,
) -> None:
    """An unresolved live descendant blocks ALL owner signalling.

    Deterministic wedge: a duck-typed owner never exits, and its marker
    records a live descendant whose start ticks do not match
    (unresolved/reused).  The finalizer must refuse to signal both the
    unresolved descendant and the owner itself, and must raise loudly
    instead of silently passing.

    Args:
        tmp_path: Pytest temporary directory.
    """
    before = _ambient_digest()
    innocent_owner = subprocess.Popen(
        ["/bin/sleep", "60"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    innocent_descendant = subprocess.Popen(
        ["/bin/sleep", "60"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    try:
        descendant_ticks = guard.proc_start_ticks(innocent_descendant.pid)
        assert descendant_ticks is not None
        _run_wedge_scenario(
            _Scenario(tmp_path, "wedge"),
            innocent_owner,
            innocent_descendant,
            descendant_ticks + 1,
        )
        assert _proc_state(innocent_owner.pid) is not None
        assert _proc_state(innocent_descendant.pid) is not None
    finally:
        for proc in (innocent_owner, innocent_descendant):
            if proc.poll() is None:
                os.killpg(proc.pid, signal.SIGKILL)
                proc.wait(timeout=10)
    assert isolation.ambient_sentinel_alive()
    assert _ambient_digest() == before


def _run_wedge_scenario(
    scenario: _Scenario,
    innocent_owner: subprocess.Popen[bytes],
    innocent_descendant: subprocess.Popen[bytes],
    recorded_descendant_ticks: int,
) -> None:
    """Drive wedged-owner convergence with an unresolved live descendant.

    Args:
        scenario: Scenario artifacts (marker/result paths).
        innocent_owner: Live process whose PID stands in for the wedged owner.
        innocent_descendant: Live unresolved descendant recorded in the marker.
        recorded_descendant_ticks: Deliberately wrong ticks for the marker.
    """
    _write_marker_entries(
        scenario.marker,
        [{"pid": innocent_descendant.pid, "ticks": recorded_descendant_ticks}],
    )
    handles: list[OwnedRun] = []

    class WedgedOwner:
        """Duck-typed Popen stand-in that never exits."""

        pid = innocent_owner.pid

        @staticmethod
        def poll() -> int | None:
            return None

        @staticmethod
        def wait(timeout: float | None = None) -> int:
            raise subprocess.TimeoutExpired(cmd="wedged-owner", timeout=timeout or 0.0)

    def start() -> subprocess.Popen[bytes]:
        return cast("subprocess.Popen[bytes]", WedgedOwner())

    with (
        pytest.raises(AssertionError, match="did not converge"),
        owned_run(scenario, start) as handle,
    ):
        handles.append(handle)

    assert handles[0].owner.poll() is None
    # Neither the unresolved descendant nor the owner-pid occupant was hit.
    assert _proc_state(innocent_descendant.pid) is not None
    assert _proc_state(innocent_owner.pid) is not None
    # The registry entry stays because the owner was never proven terminal;
    # forget it here so the test leaves no registry residue behind.
    guard.unregister(handles[0].owner)


def test_repeated_repository_suite_under_owner_preserves_ambient(
    tmp_path: Path,
) -> None:
    """Two nested runs of the canonical repository suite keep ambient intact.

    Each run executes the entire suite (minus this stress harness) under the
    independent exact-identity owner.  After each run the ambient sentinel
    worker must still be alive and the ambient production-like state tree
    must be byte-for-byte identical to before the suite ever started.

    Args:
        tmp_path: Pytest temporary directory for owner artifacts.
    """
    assert isolation.ambient_sentinel_alive()
    digest_before = _ambient_digest()
    for iteration in range(2):
        scenario = _Scenario(tmp_path, f"suite{iteration}")
        with owned_run(
            scenario,
            partial(scenario.run_suite_selection, list(CANONICAL_SUITE_ARGS)),
        ) as handle:
            assert handle.owner.wait(timeout=3600) == 0
        assert isolation.ambient_sentinel_alive()
        assert _ambient_digest() == digest_before
