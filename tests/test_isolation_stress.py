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
from pathlib import Path
from typing import Final

import pytest

from tests import _isolation as isolation

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

    def run(self, mode: str, deadline: float = 120.0) -> subprocess.Popen[bytes]:
        """Start the independently-owned nested pytest run.

        Args:
            mode: The ``NESTED_MODE`` behaviour of the nested subject.
            deadline: Owner-enforced wall-clock limit for the nested run.

        Returns:
            The independent owner process.
        """
        self.module.write_text(NESTED_MODULE, encoding="utf-8")
        env = dict(os.environ)
        env["NESTED_MODE"] = mode
        env["NESTED_MARKER"] = str(self.marker)
        env["PYTEST_ADDOPTS"] = "-p no:cacheprovider"
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
                str(self.module),
                "-q",
                "--no-header",
                "-p",
                "no:cacheprovider",
            ],
            cwd=REPO_ROOT,
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=self.log.open("ab"),
            stderr=subprocess.STDOUT,
        )


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
    owner = scenario.run(mode, deadline=deadline)
    _wait_file(scenario.pidfile)
    assert owner.wait(timeout=180) == 0
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
    owner = scenario.run("sleep")
    _wait_file(scenario.pidfile)
    nested_pid = int(scenario.pidfile.read_text().strip())
    deadline = time.monotonic() + 60.0
    while not scenario.marker.exists() and time.monotonic() < deadline:
        time.sleep(0.05)
    assert scenario.marker.exists(), scenario.log.read_text(errors="replace")
    # Interrupt only the nested pytest, by exact PID; the independent owner
    # survives and must synchronously reap the abandoned leader.
    os.kill(nested_pid, sig)
    while not scenario.result.exists():
        assert owner.poll() is None or scenario.result.exists()
        if owner.poll() is not None and not scenario.result.exists():
            break
        time.sleep(0.05)
    assert owner.wait(timeout=60) == 0
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
    owner = scenario.run("sleep", deadline=3.0)
    _wait_file(scenario.pidfile)
    deadline = time.monotonic() + 30.0
    while not scenario.marker.exists() and time.monotonic() < deadline:
        time.sleep(0.05)
    assert scenario.marker.exists()
    assert owner.wait(timeout=120) == 0
    result = _assert_containment(scenario.result)
    assert result["timed_out"] is True
    assert result["survivor_seen"] is True
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
        owner = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "tests._nested_owner",
                "--marker",
                str(scenario.marker),
                "--result",
                str(scenario.result),
                "--pidfile",
                str(scenario.pidfile),
                "--deadline",
                "300",
                "--",
                sys.executable,
                "-m",
                "pytest",
                "tests/test_isolation.py",
                "-q",
                "--no-header",
                "-p",
                "no:cacheprovider",
            ],
            cwd=REPO_ROOT,
            env=dict(os.environ),
            stdin=subprocess.DEVNULL,
            stdout=log.open("ab"),
            stderr=subprocess.STDOUT,
        )
        assert owner.wait(timeout=300) == 0, log.read_text(errors="replace")[-2000:]
        assert _read_result(scenario.result)["contained"] is True
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
        marker = tmp_path / f"suite-{iteration}.marker.json"
        result = tmp_path / f"suite-{iteration}.result.json"
        pidfile = tmp_path / f"suite-{iteration}.pid"
        owner = subprocess.Popen(
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
                "3600",
                "--",
                sys.executable,
                "-m",
                "pytest",
                *CANONICAL_SUITE_ARGS,
            ],
            cwd=REPO_ROOT,
            env=dict(os.environ),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            assert owner.wait(timeout=3600) == 0, result.read_text()
            contained = _read_result(result)
            assert contained["contained"] is True
            assert contained["observed_ppid_1"] is False
        finally:
            for path in (marker, result, pidfile):
                path.unlink(missing_ok=True)
        assert isolation.ambient_sentinel_alive()
        assert _ambient_digest() == digest_before
