"""Deterministic invariants for the durable pre-spawn recovery obligation.

A supervisor death after a successful ``Popen`` but before the durable
child/meta publication must never allow a replacement supervisor to start a
second maintained queue consumer while the first spawn may still be live.
The pre-spawn obligation is written crash-durably *before* ``Popen``, is
upgraded with the exact child identity right after it, and blocks every new
spawn until the first spawn's fate is positively resolved — never by
process-name matching or broad numeric PID/PGID signalling.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from typing import TYPE_CHECKING, cast

import pytest

from lubko import cli, lifecycle, supervise, supervisor
from lubko.supervise import SpawningObligation, proc_start_ticks, read_state

if TYPE_CHECKING:
    from pathlib import Path

COMMIT = "a" * 40


@pytest.fixture(autouse=True)
def state_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Use an isolated durable state root for each test.

    Returns:
        The supervisor state directory.
    """
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state"))
    supervise.state_path().parent.mkdir(parents=True, exist_ok=True)
    return supervise.state_path().parent


@pytest.fixture(autouse=True)
def default_owned_group_recovery(monkeypatch: pytest.MonkeyPatch) -> None:
    """Stub owned-group recovery as successful by default.

    Every test that expects authority resolution must not depend on the
    developer machine's database configuration; tests exercising the
    fail-closed recovery behavior override this stub explicitly.
    """
    monkeypatch.setattr(supervisor, "recover_owned_groups", lambda _token: None)


def _obligation(
    *,
    pid: int | None = None,
    ticks: int | None = None,
    boot_id: str | None = None,
    creator_pid: int = 999_999,
) -> SpawningObligation:
    """Build a pre-spawn obligation as a dead predecessor would have left it.

    Returns:
        The obligation record.
    """
    return SpawningObligation(
        token=os.urandom(8).hex(),
        commit=COMMIT,
        creator_pid=creator_pid,
        creator_start_time_ticks=1,
        pid=pid,
        start_time_ticks=ticks,
        created_at=0.0,
        boot_id=boot_id if boot_id is not None else supervise.current_boot_id(),
    )


def _write_state_with_spawning(spawning: SpawningObligation | None) -> None:
    """Persist a minimal run-mode state carrying only ``spawning``."""
    supervise.write_state(
        supervise.SupervisorState(
            schema_version=supervise.SCHEMA_VERSION,
            applied_generation=0,
            mode=supervise.MODE_RUN,
            commit=COMMIT,
            child=None,
            unresolved_child=None,
            ownership_hold_malformed=False,
            unresolved_hold_malformed=False,
            spawning=spawning,
            spawning_hold_malformed=False,
            intent=supervise.INTENT_RUN,
            restart_count=0,
            next_attempt_at=None,
            last_exit=None,
            last_spawn_at=None,
            ready=False,
            next_readiness_at=None,
            boot_id=None,
        )
    )


def _blocking_child() -> subprocess.Popen[bytes]:
    """Spawn a real child that stays alive until killed.

    Returns:
        The live child of this test process.
    """
    return subprocess.Popen(
        [sys.executable, "-c", "import sys; sys.stdin.read()"],
        stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


# ---------------------------------------------------------------------------
# Durable shape
# ---------------------------------------------------------------------------


def test_obligation_roundtrip_is_exact() -> None:
    """The serialized obligation preserves every field exactly."""
    obligation = _obligation(pid=4242, ticks=777)
    restored = SpawningObligation.from_dict(obligation.to_dict())
    assert restored == obligation


@pytest.mark.parametrize("raw", [7, "nope", {"token": 5}, {"commit": COMMIT}])
def test_present_malformed_obligation_is_durable_hold(
    raw: object,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A corrupt obligation survives its own corruption and blocks spawns."""
    supervise.state_path().write_text(
        json.dumps({"schema_version": supervise.SCHEMA_VERSION, "spawning": raw}),
        encoding="utf-8",
    )
    state = supervise.read_state()
    assert state.spawning_hold_malformed is True
    supervise.write_state(state)
    assert supervise.read_state().spawning_hold_malformed is True

    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    monkeypatch.setattr(
        daemon,
        "_spawn_worker",
        lambda _commit: pytest.fail("a malformed pre-spawn obligation authorized a spawn"),
    )
    daemon._ensure_worker(COMMIT)
    assert daemon._message is not None
    assert "malformed" in daemon._message


# ---------------------------------------------------------------------------
# Recovery after a crash at the dangerous boundary
# ---------------------------------------------------------------------------


def _patch_recovery(
    monkeypatch: pytest.MonkeyPatch,
    *,
    calls: list[str] | None = None,
    error: Exception | None = None,
) -> list[tuple[str, str]]:
    """Patch owned-group recovery inside the supervisor module.

    Args:
        monkeypatch: The pytest monkeypatch fixture.
        calls: Optional ordered event log appended to during resolution.
        error: When given, the recovery stub raises ``OwnedGroupRecoveryError``.

    Returns:
        The list of ``(event, token)`` tuples recorded so far.
    """
    observed: list[tuple[str, str]] = []
    log = calls if calls is not None else []

    def fake_recover(token: str) -> None:
        observed.append(("recover", token))
        log.append("recover")
        if error is not None:
            raise error

    monkeypatch.setattr(supervisor, "recover_owned_groups", fake_recover)
    return observed


def test_pid_less_obligation_from_dead_creator_resolves(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A crash between the pre-Popen write and the identity upgrade resolves."""
    obligation = _obligation()
    _write_state_with_spawning(obligation)
    observed = _patch_recovery(monkeypatch)

    daemon = supervisor.SupervisorDaemon(supervisor.Settings())

    assert daemon._resolve_spawning_obligation() is True
    assert read_state().spawning is None, "the resolved obligation was durably cleared"
    assert observed == [("recover", obligation.token)], (
        "owned command groups were recovered under the exact worker incarnation"
    )


def test_owned_group_recovery_failure_keeps_the_pid_less_hold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed owned-group recovery keeps the hold and forbids any spawn."""
    obligation = _obligation()
    _write_state_with_spawning(obligation)
    events: list[str] = []
    _patch_recovery(monkeypatch, calls=events, error=supervisor.OwnedGroupRecoveryError("db down"))

    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    spawn_attempts: list[str] = []

    def record_spawn(commit: str) -> None:
        spawn_attempts.append(commit)

    monkeypatch.setattr(daemon, "_spawn_worker", record_spawn)

    daemon._ensure_worker(COMMIT)
    assert spawn_attempts == [], "a blocked obligation authorized a spawn"
    assert read_state().spawning is not None, "the blocking obligation survived"
    assert read_state().child is None, "no replacement worker was started"
    assert daemon._message is not None
    assert "holding" in daemon._message
    assert events == ["recover"], "recovery ran before any respawn decision"

    # A later successful recovery positively converges the groups and opens
    # the gate again.
    _patch_recovery(monkeypatch)
    assert daemon._resolve_spawning_obligation() is True
    assert read_state().spawning is None


def test_previous_boot_obligation_resolves_without_evidence() -> None:
    """No spawn can survive its host's reboot, so the obligation clears."""
    _write_state_with_spawning(_obligation(boot_id="00000000-0000-0000-0000-000000000000"))

    daemon = supervisor.SupervisorDaemon(supervisor.Settings())

    assert daemon._resolve_spawning_obligation() is True
    assert read_state().spawning is None


def test_live_first_spawn_blocks_every_new_spawn(monkeypatch: pytest.MonkeyPatch) -> None:
    """Crash after Popen with a live first spawn: a second spawn is forbidden.

    The obligation carries the exact identity (PID plus start-time ticks) of
    a genuinely live process — the dangerous boundary state a successor
    supervisor reads after the spawning daemon died. No spawn may be
    authorized until that exact instance is positively gone.
    """
    proc = _blocking_child()
    try:
        ticks = proc_start_ticks(proc.pid)
        assert ticks is not None
        _write_state_with_spawning(_obligation(pid=proc.pid, ticks=ticks))

        daemon = supervisor.SupervisorDaemon(supervisor.Settings(stop_grace_seconds=0.05))
        monkeypatch.setattr(
            daemon,
            "_spawn_worker",
            lambda _commit: pytest.fail("a possibly-live first spawn authorized a second"),
        )
        daemon._ensure_worker(COMMIT)

        assert daemon._message is not None
        assert "still live" in daemon._message
        assert read_state().child is None, "no replacement worker was started"
        assert read_state().spawning is not None, "the blocking obligation survived"

        # The first spawn's fate is now positively resolved (killed and
        # reaped): only then does the gate open again.
        proc.kill()
        proc.wait()

        assert daemon._resolve_spawning_obligation() is True
        assert read_state().spawning is None
    finally:
        proc.poll()


def test_recycled_identity_never_extends_the_obligation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A PID whose start ticks differ from the record proves the instance gone."""
    _patch_recovery(monkeypatch)
    proc = _blocking_child()
    try:
        _write_state_with_spawning(_obligation(pid=proc.pid, ticks=1))

        daemon = supervisor.SupervisorDaemon(supervisor.Settings())

        assert daemon._resolve_spawning_obligation() is True
        assert read_state().spawning is None
    finally:
        proc.kill()
        proc.wait()


# ---------------------------------------------------------------------------
# Owned-group recovery before dropping pre-publication authority
# ---------------------------------------------------------------------------


def test_recover_unpublished_spawn_failure_keeps_token_authority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Failed group recovery after converging an unpublished spawn blocks.

    The worker itself is positively reaped, but its already-launched command
    groups cannot be recovered: the durable token-bearing obligation must
    survive (upgraded to the exact observed identity) so no replacement can
    spawn, and a later successful recovery must reopen the gate.
    """
    obligation = _obligation()
    _write_state_with_spawning(obligation)
    events: list[str] = []
    _patch_recovery(monkeypatch, calls=events, error=supervisor.OwnedGroupRecoveryError("db down"))

    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    daemon.proc = cast("subprocess.Popen[bytes]", FakeProc(pid=4711))

    assert (
        daemon._recover_unpublished_spawn(
            cast("subprocess.Popen[bytes]", FakeProc(pid=4711)), obligation, 555
        )
        is None
    )

    held = read_state().spawning
    assert held is not None, "the token-bearing blocking authority survived"
    assert held.token == obligation.token
    assert held.pid == 4711, "the hold carries the exact observed identity for a precise retry"
    assert held.start_time_ticks == 555
    assert read_state().child is None

    # Later recovery success releases the hold and permits the retry.
    def recovering_recover(token: str) -> None:
        assert read_state().spawning is not None, (
            "recovery ran while the blocking authority was already cleared"
        )
        observed.append(("recover", token))

    observed: list[tuple[str, str]] = []
    monkeypatch.setattr(supervisor, "recover_owned_groups", recovering_recover)

    assert daemon._resolve_spawning_obligation() is True
    assert read_state().spawning is None
    assert observed == [("recover", obligation.token)]


def test_identified_dead_worker_group_recovery_failure_keeps_the_hold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A resolved dead identified worker still owes exact group recovery.

    The worker PID is provably gone (recycled identity), but its command
    groups are independent process groups: failed recovery must keep the
    obligation durable, and later success must recover under the exact token
    before the authority is cleared.
    """
    obligation = _obligation(pid=os.getpid(), ticks=1)
    _write_state_with_spawning(obligation)
    events: list[str] = []
    _patch_recovery(monkeypatch, calls=events, error=supervisor.OwnedGroupRecoveryError("db down"))

    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    monkeypatch.setattr(
        daemon,
        "_spawn_worker",
        lambda _commit: pytest.fail("unrecovered owned groups authorized a replacement"),
    )
    daemon._ensure_worker(COMMIT)

    held = read_state().spawning
    assert held is not None, "the blocking obligation survived"
    assert held.token == obligation.token
    assert read_state().child is None
    assert daemon._message is not None
    assert events == ["recover"], "recovery ran before any respawn decision"

    observed = _patch_recovery(monkeypatch)
    assert daemon._resolve_spawning_obligation() is True
    assert read_state().spawning is None
    assert observed == [("recover", obligation.token)]


def test_identified_converged_worker_group_recovery_failure_keeps_the_holds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Converging a live identified worker is not enough to drop its token.

    The successor terminates and positively reaps the exact recorded instance,
    yet failed owned-group recovery must keep both the pre-spawn obligation
    and the unresolved hold durable; a later successful recovery clears them.
    """
    monkeypatch.setattr(time, "sleep", lambda _seconds: None)
    obligation = _obligation()
    proc = _blocking_child()
    try:
        ticks = proc_start_ticks(proc.pid)
        assert ticks is not None
        _write_state_with_spawning(
            _obligation(pid=proc.pid, ticks=ticks, boot_id=obligation.boot_id)
        )
        stored = read_state().spawning
        assert stored is not None

        daemon = supervisor.SupervisorDaemon(supervisor.Settings())
        daemon.proc = proc

        events: list[str] = []
        _patch_recovery(
            monkeypatch, calls=events, error=supervisor.OwnedGroupRecoveryError("db down")
        )
        assert daemon._resolve_spawning_obligation() is False
        assert proc.poll() is not None, "the worker instance was positively reaped"
        held = read_state().spawning
        assert held is not None, "token authority survived"
        assert held.token == stored.token
        assert read_state().unresolved_child is not None, "the exact-identity hold survived"

        observed = _patch_recovery(monkeypatch)
        assert daemon._resolve_spawning_obligation() is True
        assert read_state().spawning is None
        assert read_state().unresolved_child is None
        assert [token for _, token in observed] == [stored.token]
    finally:
        proc.poll()


class UnconvergableProc:
    """``Popen`` stand-in whose exit can never be positively proven."""

    def __init__(self, pid: int) -> None:
        """Record the fake PID.

        Args:
            pid: The PID the fake spawn reports.
        """
        self.pid = pid

    @staticmethod
    def poll() -> int | None:
        """Report the child as still running.

        Returns:
            ``None``: the fake child never exits.
        """
        return None

    @staticmethod
    def terminate() -> None:
        """Nothing to terminate."""

    @staticmethod
    def kill() -> None:
        """Nothing to kill."""

    @staticmethod
    def wait(timeout: float | None = None) -> int:
        """Never report an exit.

        Args:
            timeout: The elapsed timeout.

        Raises:
            subprocess.TimeoutExpired: Always, simulating an unexitable child.
        """
        raise subprocess.TimeoutExpired(cmd="worker", timeout=timeout or 0.0)


def test_transferred_unresolved_hold_clears_only_after_group_recovery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A token-bearing hold from failed identity publication owes recovery.

    When the unpublished spawn's worker cannot be converged in-line, its token
    authority transfers to an unresolved-child hold. That hold may only be
    cleared once the exact worker instance is converged *and* its owned command
    groups are recovered under the same token; failure keeps the hold durable,
    later success releases it.
    """
    obligation = _obligation()
    _write_state_with_spawning(obligation)

    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    proc = cast("subprocess.Popen[bytes]", UnconvergableProc(pid=4711))
    daemon.proc = proc

    assert daemon._recover_unpublished_spawn(proc, obligation, 555) is None
    transferred = read_state().unresolved_child
    assert transferred is not None, "the token authority transferred to a durable hold"
    assert transferred.token == obligation.token
    assert read_state().spawning is None

    events: list[str] = []
    _patch_recovery(monkeypatch, calls=events, error=supervisor.OwnedGroupRecoveryError("db down"))
    monkeypatch.setattr(
        daemon,
        "_spawn_worker",
        lambda _commit: pytest.fail("an unresolved hold authorized a replacement"),
    )
    daemon._ensure_worker(COMMIT)

    assert read_state().unresolved_child is not None, "the blocking hold survived"
    assert read_state().child is None
    assert daemon._message is not None
    assert events == ["recover"], "group recovery ran before any respawn decision"

    observed = _patch_recovery(monkeypatch)
    assert daemon._resolve_unresolved_child() is True
    assert read_state().unresolved_child is None
    assert observed == [("recover", obligation.token)]


def test_live_identified_first_spawn_converges_by_exact_pinned_signal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A live identified first spawn is converged by pinned single-PID signals.

    The successor takes over the blocking authority as an unresolved-child
    hold, terminates the exact recorded instance (start-ticks guarded), and
    only then clears every blocker. The child is this test process's own
    direct ``Popen`` child assigned to the daemon, so the kernel-proven
    ownership path reaps it positively.
    """
    monkeypatch.setattr(time, "sleep", lambda _seconds: None)
    _patch_recovery(monkeypatch)
    proc = _blocking_child()
    try:
        ticks = proc_start_ticks(proc.pid)
        assert ticks is not None
        _write_state_with_spawning(_obligation(pid=proc.pid, ticks=ticks))

        daemon = supervisor.SupervisorDaemon(supervisor.Settings())
        daemon.proc = proc

        assert daemon._resolve_spawning_obligation() is True
        assert read_state().spawning is None
        assert read_state().unresolved_child is None
        assert proc.poll() is not None, "the first spawn was positively reaped"
    finally:
        proc.poll()


# ---------------------------------------------------------------------------
# Normal spawn path writes and clears the obligation
# ---------------------------------------------------------------------------


class FakeProc:
    """Minimal ``Popen`` stand-in whose handle is an already-exited child."""

    def __init__(self, pid: int) -> None:
        """Record the fake PID.

        Args:
            pid: The PID the fake spawn reports.
        """
        self.pid = pid

    @staticmethod
    def poll() -> int:
        """Report the child as exited.

        Returns:
            The fake exit code.
        """
        return 0

    @staticmethod
    def wait(timeout: float | None = None) -> int:
        """Report an immediate exit.

        Args:
            timeout: Ignored.

        Returns:
            The fake exit code.
        """
        del timeout
        return 0

    @staticmethod
    def terminate() -> None:
        """Nothing to terminate."""

    @staticmethod
    def kill() -> None:
        """Nothing to kill."""


def test_normal_spawn_writes_obligation_before_popen_and_clears_afterwards(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Every successful or failed normal spawn keeps the protocol coherent.

    While the fake ``Popen`` executes — the exact instant the real crash
    window opens — the obligation must already be durably on disk without a
    child identity. Afterwards, an ordinary (already-exited) spawn failure
    must clear the obligation again so a bounded retry remains possible.
    """
    observed_during_popen: list[supervise.SpawningObligation | None] = []

    def fake_popen(*_args: object, **_kwargs: object) -> FakeProc:
        observed_during_popen.append(read_state().spawning)
        return FakeProc(pid=4711)

    monkeypatch.setattr(cli, "runtime_is_usable", lambda _commit: True)
    monkeypatch.setattr(cli, "cli_entry_executable", lambda _commit, _name: "/bin/true")
    monkeypatch.setattr(cli, "cli_commit_dir", lambda _commit: tmp_path)
    monkeypatch.setattr(lifecycle, "worker_env", lambda _token: {})
    monkeypatch.setattr("lubko.supervisor.subprocess.Popen", fake_popen, raising=False)
    monkeypatch.setattr(supervisor, "_pdeathsig_supported", lambda: True)

    daemon = supervisor.SupervisorDaemon(
        supervisor.Settings(identity_timeout_seconds=0.01, stop_grace_seconds=0.05)
    )

    assert daemon._spawn_worker(COMMIT) is None, "the fake child had already exited"

    assert len(observed_during_popen) == 1
    during = observed_during_popen[0]
    assert during is not None, "the obligation was durable before Popen"
    assert during.commit == COMMIT
    assert during.pid is None, "the child identity cannot exist before Popen"
    assert read_state().spawning is None, "the failed spawn cleared the obligation"


def test_failed_runtime_check_never_leaves_an_obligation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A refused spawn (missing runtime) never happens after the Popen gate."""
    monkeypatch.setattr(cli, "runtime_is_usable", lambda _commit: False)

    daemon = supervisor.SupervisorDaemon(supervisor.Settings())

    assert daemon._spawn_worker(COMMIT) is None
    assert read_state().spawning is None


def test_child_side_pdeathsig_failure_never_execs_worker_code(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed child-side parent-death install blocks exec and keeps authority safe.

    Forcing the in-child ``prctl`` to fail must make ``Popen`` itself fail
    (the child raises before exec, so worker user code can never run
    unguarded), and the durable spawn authority must be cleared again so a
    later retry can be authorized — safe because positively nothing was
    spawned. The sentinel proves the real fork/exec path ran without ever
    reaching the worker executable.
    """
    sentinel = tmp_path / "sentinel"
    worker = tmp_path / "worker"
    worker.write_text(f"#!/bin/sh\ntouch {sentinel}\n", encoding="utf-8")
    worker.chmod(0o755)
    monkeypatch.setattr(cli, "runtime_is_usable", lambda _commit: True)
    monkeypatch.setattr(cli, "cli_entry_executable", lambda _commit, _name: str(worker))
    monkeypatch.setattr(cli, "cli_commit_dir", lambda _commit: tmp_path)
    monkeypatch.setattr(lifecycle, "worker_env", lambda _token: {})
    # An invalid prctl option makes the real in-child prctl call fail with
    # EINVAL deterministically; the forked child inherits this patched value.
    monkeypatch.setattr(supervisor, "PR_SET_PDEATHSIG", -1)
    monkeypatch.setattr(supervisor, "_pdeathsig_supported", lambda: True)

    daemon = supervisor.SupervisorDaemon(supervisor.Settings())

    assert daemon._spawn_worker(COMMIT) is None
    assert read_state().spawning is None, "durable authority was safely cleared"
    assert not sentinel.exists(), "worker user code ran despite the failed install"
