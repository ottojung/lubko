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
from typing import TYPE_CHECKING, Final

import pytest

from tests import _isolation as isolation
from tests import _process_guard as guard

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

REPO_ROOT: Final = Path(__file__).resolve().parent.parent

# Nested test module used for interruption-mode scenarios. It registers a
# real session-leader process with the shared guard, records its exact
# identity (PID plus start ticks) in the owner's marker file, and behaves
# according to ``NESTED_MODE``:
# - ``success``: owns, stops, and unregisters its own leader cleanly;
# - ``failure``: leaks the leader on purpose, and the ``finally`` block
#   (the same teardown contract our conftest enforces) stops it exactly;
# - ``sleep``: blocks until the scenario interrupts the nested pytest.
NESTED_MODULE: Final = f'''
"""Nested interruption-mode subject: register a leader, behave on cue."""
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, {str(REPO_ROOT)!r})

from tests import _process_guard as guard  # noqa: E402


def test_subject() -> None:
    """Register a session leader, then behave according to NESTED_MODE."""
    mode = os.environ["NESTED_MODE"]
    marker_path = os.environ["NESTED_MARKER"]

    proc = subprocess.Popen(
        ["/bin/sleep", "300"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        close_fds=True,
    )
    guard.register(proc)
    Path(marker_path).write_text(
        json.dumps([{{"pid": proc.pid, "ticks": guard.proc_start_ticks(proc.pid)}}]),
        encoding="utf-8",
    )
    try:
        if mode == "success":
            os.killpg(proc.pid, signal.SIGTERM)
            assert proc.wait(timeout=10) == -signal.SIGTERM
            guard.unregister(proc)
            return
        if mode == "failure":
            raise AssertionError("deliberate nested failure")
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


def _retire_marker_descendants(marker: Path) -> list[str]:
    """Fail-closed retire every marker-recorded descendant identity.

    Each recorded identity is re-verified against current start ticks
    immediately before every signal; unresolved or reused identities are
    never signalled and are reported instead.  Used only on the forced wedge
    path, before the owner itself may be retired.

    Args:
        marker: Marker file with ``[{"pid": ..., "ticks": ...}]`` entries.

    Returns:
        Human-readable problems for identities that could not be retired.
    """
    problems: list[str] = []
    try:
        payload = json.loads(marker.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return problems
    if not isinstance(payload, list):
        return problems
    for entry in payload:
        if not isinstance(entry, dict):
            continue
        pid = entry.get("pid")
        ticks = entry.get("ticks")
        if not isinstance(pid, int) or not isinstance(ticks, int) or ticks <= 0:
            continue
        for sig in (signal.SIGTERM, signal.SIGKILL):
            if _wait_gone(pid, 5.0):
                break
            guard.signal_identity_checked(pid, ticks, sig)
        if _wait_gone(pid, 5.0):
            continue
        if guard.proc_start_ticks(pid) != ticks:
            problems.append(f"descendant pid {pid} identity unresolved; not signalled")
        else:
            problems.append(f"descendant pid {pid} did not retire")
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

    Descendants are retired first: killing the subreaper owner before them
    could strand adopted session-leader grandchildren under container PID 1.
    The owner is signalled only under its registration-time PID+start-ticks
    identity, re-verified immediately before every TERM/KILL; an unresolved
    or reused identity is never signalled and is reported instead.

    Args:
        handle: The owned-run handle whose owner wedged.
        problems: Accumulating cleanup-problem descriptions.
    """
    owner = handle.owner
    problems.extend(_retire_marker_descendants(handle.scenario.marker))
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
            env_overrides={"NESTED_MODE": mode, "NESTED_MARKER": str(self.marker)},
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
        while not scenario.marker.exists() and time.monotonic() < deadline:
            time.sleep(0.05)
        assert scenario.marker.exists(), scenario.log.read_text(errors="replace")
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
        while not scenario.marker.exists() and time.monotonic() < deadline:
            time.sleep(0.05)
        assert scenario.marker.exists()
        assert handle.owner.wait(timeout=120) == 0
    result = _assert_containment(scenario.result)
    assert result["timed_out"] is True
    assert result["survivor_seen"] is True
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
            while not scenario.marker.exists() and time.monotonic() < deadline:
                time.sleep(0.05)
            assert scenario.marker.exists()
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
    entries = json.loads(scenario.marker.read_text(encoding="utf-8"))
    assert isinstance(entries, list)
    assert entries
    leader = entries[0]
    assert isinstance(leader, dict)
    for pid in (nested[0], int(leader["pid"])):
        assert _proc_state(pid) is None or _proc_state(pid) == "Z"
    payload = _read_result(scenario.result)
    assert payload["contained"] is True
    assert payload["observed_ppid_1"] is False
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
        # Deliberately omit "ticks" from the marker entry.
        marker.write_text(json.dumps([{"pid": innocent.pid}]), encoding="utf-8")
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
