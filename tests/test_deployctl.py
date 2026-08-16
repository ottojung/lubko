"""Focused tests for the supervised deployment controller.

The coherence tests exercise the global-CLI guarantee with real two-commit git
repositories: a provisional candidate never moves the ``current`` CLI pointer,
confirmation moves it only after durable ``confirmed`` state, and every
rollback/failure path preserves the prior confirmed CLI version.
"""

from __future__ import annotations

import json
import os
import re
import secrets
import shutil
import signal
import subprocess
import sys
import threading
import time
from contextlib import suppress
from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING, Final
from uuid import uuid4

import psycopg
import pytest

from lubko import cli, lifecycle, supervise, worker
from lubko import deployctl as dc
from lubko.lifecycle import (
    SCHEMA_VERSION,
    STATE_RUNNING,
    ValidationReport,
    WorkerMeta,
    read_meta,
    worker_alive,
    write_meta,
)
from lubko.state import rollback_state_path, state_root
from lubko.worker import group_has_members
from tests import _isolation as isolation
from tests import _process_guard as guard
from tests.test_cli import fake_uv_sync, make_repo

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from typing import Any

    from lubko.lifecycle import ProcessIdentity
    from tests import _pg

SLEEP_BIN: Final = shutil.which("sleep") or "/bin/sleep"
CHALLENGE_RE: Final = re.compile(r"[0-9a-f]{7}")
LINGER_SOURCE: Final = (
    "import signal,time\nsignal.signal(signal.SIGTERM, signal.SIG_IGN)\ntime.sleep(300)\n"
)
RETIRING_MARKER: Final = "retiring-worker"
CANDIDATE_MARKER: Final = "candidate-worker"


def worker_meta(commit: str, *, pid: int = 100, repo: str = "/workspace/Lubko") -> WorkerMeta:
    """Build deterministic maintained-worker metadata for controller tests.

    Args:
        commit: Exact commit represented by the worker.
        pid: Synthetic process identity.
        repo: Repository recorded in the metadata.

    Returns:
        Worker metadata suitable for rollback-state tests.
    """
    return WorkerMeta(
        schema_version=SCHEMA_VERSION,
        state=STATE_RUNNING,
        pid=pid,
        pgid=pid,
        sid=pid,
        start_time_ticks=pid * 10,
        token=f"token-{pid}",
        repo=repo,
        git_commit=commit,
        worker_id="test-worker",
        log_path="/workspace/worker.log",
        started_at=1.0,
        stopped_at=None,
    )


def pending_state(
    *,
    repo: str = "/workspace/Lubko",
    old: str = "1" * 40,
    new: str = "2" * 40,
    previous_retiring: bool = False,
    generation: int = 1,
) -> dc.RollbackState:
    """Return a live pending deployment state.

    Args:
        repo: Repository recorded in the state.
        old: Previous confirmed commit.
        new: Proposed candidate commit.
        previous_retiring: Whether the previous worker's retirement has begun.
        generation: Monotonic mission generation.

    Returns:
        A pending rollback state with distinct old/new commits.
    """
    return dc.RollbackState(
        schema_version=dc.ROLLBACK_SCHEMA_VERSION,
        generation=generation,
        status=dc.STATUS_PENDING,
        commit=new,
        previous_commit=old,
        challenge_hash=None,
        deadline=time.time() + 60,
        repo=repo,
        uv_path="uv",
        stop_grace_seconds=1.0,
        git_timeout_seconds=5.0,
        previous_retiring=previous_retiring,
        previous_meta=worker_meta(old, pid=100, repo=repo),
        new_meta=worker_meta(new, pid=200, repo=repo),
    )


def make_options(repo: Path) -> dc.Options:
    """Build controller options for a test repository.

    Args:
        repo: Repository to deploy.

    Returns:
        Runtime options for the controller.
    """
    return dc.Options(
        repo=repo,
        uv_path="uv",
        confirm_window_seconds=120,
        stop_grace_seconds=1.0,
        postgres_timeout_seconds=5,
        lock_timeout_seconds=5,
        validation_timeout_seconds=5,
        git_timeout_seconds=5,
        cli_timeout_seconds=60,
    )


class _NoopDeployLock:
    """Minimal deployment-lock context that never blocks or fails."""

    def __enter__(self) -> None:
        return None

    def __exit__(self, *_args: object) -> None:
        return None


def _supervisor_child_state(commit: str, generation: int) -> supervise.SupervisorState:
    """Build supervisor durable state running ``commit`` under ``generation``.

    Args:
        commit: Exact commit the daemon runs.
        generation: Generation the daemon applied.

    Returns:
        A run-mode supervisor state owning a live candidate child.
    """
    return replace(
        supervise.fresh_state(),
        mode=supervise.MODE_RUN,
        commit=commit,
        applied_generation=generation,
        child=supervise.WorkerChild(
            pid=4242,
            pgid=4242,
            sid=4242,
            start_time_ticks=42_424_242,
            token=f"token-{generation}",
            worker_id="test-supervised-worker",
            spawned_at=1.0,
        ),
        intent=supervise.INTENT_RUN,
        ready=True,
    )


def run_launcher(path: Path) -> str:
    """Run a stable launcher and return its trimmed stdout.

    Args:
        path: Launcher path.

    Returns:
        The launcher's standard output.
    """
    proc = subprocess.run([str(path)], capture_output=True, text=True, check=True)
    return proc.stdout.strip()


def spawn_real_process(token: str) -> subprocess.Popen[bytes]:
    """Spawn a real long-lived session-leader process owned by the guard.

    Args:
        token: Lifecycle token placed in the process environment.

    Returns:
        The spawned process, registered for deterministic teardown.
    """
    env = dict(os.environ)
    env[lifecycle.LIFECYCLE_MARKER_VAR] = token
    proc = subprocess.Popen(
        [SLEEP_BIN, "300"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        close_fds=True,
        env=env,
    )
    guard.register(proc)
    return proc


def spawn_lingering_previous() -> subprocess.Popen[bytes]:
    """Spawn a real process that lingers through SIGTERM, simulating shutdown.

    The process ignores SIGTERM so it stays alive through the graceful-stop
    grace period: a previous worker that is in the middle of retiring but not
    yet gone. It is registered with the process guard for deterministic
    teardown.

    Returns:
        The lingering process.
    """
    env = dict(os.environ)
    env[lifecycle.LIFECYCLE_MARKER_VAR] = RETIRING_MARKER
    proc = subprocess.Popen(
        [sys.executable, "-c", LINGER_SOURCE],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        close_fds=True,
        env=env,
    )
    guard.register(proc)
    return proc


def wait_identity(proc: subprocess.Popen[bytes]) -> ProcessIdentity:
    """Wait for a real spawned process to establish its own session.

    Args:
        proc: The spawned process.

    Returns:
        The established exact identity.
    """
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        identity = lifecycle.process_identity(proc.pid)
        if identity is not None and identity.pgid == proc.pid and identity.sid == proc.pid:
            return identity
        time.sleep(0.01)
    identity = lifecycle.process_identity(proc.pid)
    assert identity is not None
    return identity


def real_meta(proc: subprocess.Popen[bytes], repo: Path, commit: str, token: str) -> WorkerMeta:
    """Build running metadata from a real process identity.

    Args:
        proc: The real spawned process.
        repo: Repository path to record.
        commit: Exact commit the process represents.
        token: Lifecycle token carried by the process.

    Returns:
        Running metadata anchored on the real PID and start time.
    """
    identity = wait_identity(proc)
    return WorkerMeta(
        schema_version=SCHEMA_VERSION,
        state=STATE_RUNNING,
        pid=identity.pid,
        pgid=identity.pgid,
        sid=identity.sid,
        start_time_ticks=identity.start_time_ticks,
        token=token,
        repo=str(repo),
        git_commit=commit,
        worker_id="test-worker",
        log_path=str(lifecycle.worker_log_path()),
        started_at=time.time(),
        stopped_at=None,
    )


def kill_proc(proc: subprocess.Popen[bytes]) -> None:
    """Force-kill a real process by its dedicated group and reap it.

    Args:
        proc: The owned session-leader process.
    """
    if proc.poll() is None:
        with suppress(ProcessLookupError):
            os.killpg(proc.pid, signal.SIGKILL)
    proc.wait(timeout=5)
    guard.unregister(proc)


def kill_many(procs: list[subprocess.Popen[bytes]]) -> None:
    """Force-kill and reap every owned session-leader process.

    Args:
        procs: The owned processes.
    """
    for proc in procs:
        kill_proc(proc)


def patch_fresh_worker_spawn(
    monkeypatch: pytest.MonkeyPatch,
    spawned: list[subprocess.Popen[bytes]],
) -> None:
    """Route fresh previous-worker spawns to a real owned process.

    ``_restart_previous`` spawns the restored worker through
    ``lifecycle.spawn_worker``; here its command is replaced with a real sleep
    process that is registered with the guard, and the PostgreSQL liveness
    check is satisfied without a real database.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
        spawned: List receiving every fresh worker spawned.
    """
    monkeypatch.setattr(lifecycle, "_worker_command", lambda _uv: [SLEEP_BIN, "300"])
    original_spawn = lifecycle.spawn_worker

    def tracking_spawn(
        repo: Path,
        uv_path: str,
        log_path: Path,
        env: dict[str, str],
    ) -> subprocess.Popen[bytes]:
        proc = original_spawn(repo, uv_path, log_path, env)
        guard.register(proc)
        spawned.append(proc)
        return proc

    monkeypatch.setattr(dc, "spawn_worker", tracking_spawn)
    monkeypatch.setattr(dc, "check_postgres", lambda _timeout: True)


@pytest.fixture(autouse=True)
def state_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Point all controller state at a temporary location.

    Args:
        tmp_path: Pytest temporary path.
        monkeypatch: Pytest monkeypatch fixture.
    """
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state"))


@pytest.fixture(autouse=True)
def _ambient_production_intact() -> Iterator[None]:
    """Assert every test leaves the ambient production-like state untouched.

    The ambient tree stands in for the live user state tree and the ambient
    live worker for the real maintained worker. A deployment test that escaped
    the XDG isolation would mutate the tree or signal the sentinel; both are
    caught here after every test in this module.

    Yields:
        Nothing while one test runs.
    """
    tree = isolation.ambient_state_root()
    before = isolation.snapshot_tree(tree)
    yield
    after = isolation.snapshot_tree(tree)
    assert after == before, "test mutated the ambient production-like state tree"
    assert isolation.ambient_sentinel_alive(), "test signalled the ambient live worker"


@pytest.fixture
def coherent_environment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[Path, str, str, Path]:
    """Prepare a two-commit repo with stable launchers and CLI roots.

    Returns:
        ``(repo, first, second, bin_dir)`` with ``current`` pointing at first.
    """
    repo, first, second = make_repo(tmp_path / "repo")
    monkeypatch.setattr(cli, "_sync_venv", fake_uv_sync)
    cli.build_cli_root(repo, first, "uv", 60.0)
    cli.build_cli_root(repo, second, "uv", 60.0)
    bin_dir = tmp_path / "bin"
    cli.install_launchers(bin_dir)
    cli.set_current(first)
    return repo, first, second, bin_dir


def patch_rollback_dependencies(monkeypatch: pytest.MonkeyPatch) -> None:
    """Stub the heavy rollback subprocess and process operations.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
    """
    monkeypatch.setattr(dc, "stop_worker", lambda _meta, _grace: True)
    monkeypatch.setattr(dc, "_restart_previous", lambda state: state.previous_meta)


def test_rollback_state_round_trip() -> None:
    """Persisted rollback state round-trips without losing process identity."""
    state = replace(pending_state(), previous_retiring=True)

    dc._write_state(state)

    assert dc._read_state() == state


def test_rollback_state_generation_round_trip() -> None:
    """The monotonic mission generation survives serialization identically."""
    state = replace(pending_state(), generation=7)

    dc._write_state(state)

    parsed = dc._read_state()
    assert parsed is not None
    assert parsed == state
    assert parsed.to_dict()["generation"] == 7


def test_rollback_state_rejects_missing_generation() -> None:
    """A legacy mission without a generation fails closed, never inventing zero."""
    data = pending_state(generation=3).to_dict()
    del data["generation"]

    with pytest.raises(dc.DeployCtlError, match="malformed"):
        dc.RollbackState.from_dict(data)


def test_read_state_rejects_legacy_mission_without_generation() -> None:
    """A legacy state file lacking generation is untrustworthy and rejected."""
    data = pending_state(generation=3).to_dict()
    del data["generation"]
    rollback_state_path().parent.mkdir(parents=True, exist_ok=True)
    rollback_state_path().write_text(
        json.dumps(data, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(dc.DeployCtlError, match="malformed"):
        dc._read_state()


def test_next_mission_generation_is_strictly_greater(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A new mission generation outranks every durable recorded generation."""
    existing = replace(pending_state(), generation=4)
    monkeypatch.setattr(dc, "_read_state", lambda: existing)
    supervise.write_desired(
        supervise.SupervisorDesired(
            schema_version=supervise.SCHEMA_VERSION,
            generation=9,
            commit="2" * 40,
            repo="",
            uv_path="uv",
            worker_id=None,
            requested_at=1.0,
        )
    )
    supervise.write_state(replace(supervise.fresh_state(), applied_generation=12))

    assert dc.next_mission_generation() == 13


def test_next_mission_generation_defaults_to_one(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With no recorded generations anywhere, the first mission is generation one."""
    monkeypatch.setattr(dc, "_read_state", lambda: None)
    monkeypatch.setattr(supervise, "read_desired", lambda: None)
    monkeypatch.setattr(supervise, "read_state", supervise.fresh_state)

    assert dc.next_mission_generation() == 1


def test_first_confirmation_returns_7_hex_challenge(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The first confirmation returns a 7-hex challenge but stores only its digest."""
    state = pending_state()
    options = make_options(tmp_path / "repo")
    written: list[dc.RollbackState] = []
    monkeypatch.setattr(dc, "_read_state", lambda: state)
    monkeypatch.setattr(dc, "worker_alive", lambda _meta: True)
    monkeypatch.setattr(dc, "_write_state", written.append)

    response = dc._confirm_locked({"type": "confirm", "commit": state.commit}, options)

    challenge = response["challenge"]
    assert isinstance(challenge, str)
    assert CHALLENGE_RE.fullmatch(challenge) is not None
    assert len(challenge) == 7
    assert challenge == challenge.lower()
    assert written[-1].challenge_hash == dc._challenge_digest(challenge)
    assert challenge not in written[-1].to_dict().values()


def test_second_confirmation_writes_meta_before_terminal_state(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Successful confirmation records candidate metadata before terminal state."""
    state = pending_state()
    options = make_options(tmp_path / "repo")
    challenge = "3fa91c0"
    challenged = replace(state, challenge_hash=dc._challenge_digest(challenge))
    events: list[str] = []
    written: list[dc.RollbackState] = []

    monkeypatch.setattr(dc, "_read_state", lambda: challenged)
    monkeypatch.setattr(dc, "worker_alive", lambda _meta: True)
    monkeypatch.setattr(cli, "build_cli_root", lambda *_args, **_kwargs: Path())
    monkeypatch.setattr(cli, "set_current", lambda _commit: None)
    monkeypatch.setattr(cli, "gc_cli_roots", lambda _keep: None)

    def record_meta(_meta: WorkerMeta) -> None:
        events.append("meta")

    def record_state(value: dc.RollbackState) -> None:
        events.append("state")
        written.append(value)

    monkeypatch.setattr(dc, "write_meta", record_meta)
    monkeypatch.setattr(dc, "_write_state", record_state)
    monkeypatch.setattr(dc, "append_deploy_log", lambda _message: None)

    response = dc._confirm_locked(
        {
            "type": "confirm",
            "commit": state.commit,
            "challenge": challenge[::-1],
        },
        options,
    )

    assert response["confirmed"] is True
    assert events == ["meta", "state"]
    assert written[-1].status == dc.STATUS_CONFIRMED


def test_wrong_challenge_rolls_back(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """An incorrect second factor immediately invokes rollback."""
    state = pending_state()
    options = make_options(tmp_path / "repo")
    challenged = replace(state, challenge_hash=dc._challenge_digest("3fa91c0"))
    rollbacks: list[dc.RollbackState] = []
    monkeypatch.setattr(dc, "_read_state", lambda: challenged)
    monkeypatch.setattr(dc, "worker_alive", lambda _meta: True)

    def rollback(value: dc.RollbackState) -> bool:
        rollbacks.append(value)
        return True

    monkeypatch.setattr(dc, "_rollback_locked", rollback)

    with pytest.raises(dc.DeployCtlError, match="incorrect"):
        dc._confirm_locked(
            {
                "type": "confirm",
                "commit": state.commit,
                "challenge": "1111111",
            },
            options,
        )

    assert rollbacks == [challenged]


def test_wrong_length_challenge_rolls_back(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A second factor of the wrong length never confirms the candidate."""
    state = pending_state()
    options = make_options(tmp_path / "repo")
    challenged = replace(state, challenge_hash=dc._challenge_digest("3fa91c0"))
    rollbacks: list[dc.RollbackState] = []
    monkeypatch.setattr(dc, "_read_state", lambda: challenged)
    monkeypatch.setattr(dc, "worker_alive", lambda _meta: True)

    def rollback(value: dc.RollbackState) -> bool:
        rollbacks.append(value)
        return True

    monkeypatch.setattr(dc, "_rollback_locked", rollback)

    for answer in ("3fa91c", "3fa91c00"):
        with pytest.raises(dc.DeployCtlError, match="malformed"):
            dc._confirm_locked(
                {
                    "type": "confirm",
                    "commit": state.commit,
                    "challenge": answer,
                },
                options,
            )

    assert rollbacks == [challenged, challenged]


def test_non_hex_challenge_rolls_back(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A non-hexadecimal second factor never confirms the candidate."""
    state = pending_state()
    options = make_options(tmp_path / "repo")
    challenged = replace(state, challenge_hash=dc._challenge_digest("3fa91c0"))
    rollbacks: list[dc.RollbackState] = []
    monkeypatch.setattr(dc, "_read_state", lambda: challenged)
    monkeypatch.setattr(dc, "worker_alive", lambda _meta: True)

    def rollback(value: dc.RollbackState) -> bool:
        rollbacks.append(value)
        return True

    monkeypatch.setattr(dc, "_rollback_locked", rollback)

    for answer in ("zzzzzzz", "3FA91C0"):
        with pytest.raises(dc.DeployCtlError, match="malformed"):
            dc._confirm_locked(
                {
                    "type": "confirm",
                    "commit": state.commit,
                    "challenge": answer,
                },
                options,
            )

    assert rollbacks == [challenged, challenged]


def test_watchdog_rollback_condition_uses_deadline_or_candidate_death(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Status lazily rolls back a dead pending candidate under the same lock."""
    state = pending_state()
    options = make_options(tmp_path / "repo")
    rolled_back = replace(state, status=dc.STATUS_ROLLED_BACK)
    states = iter((state, rolled_back))
    calls: list[dc.RollbackState] = []

    class FakeLock:
        """Minimal deployment-lock context for the status test."""

        def __enter__(self) -> None:
            return None

        def __exit__(self, *_args: object) -> None:
            return None

    monkeypatch.setattr(dc, "deploy_lock", lambda _timeout: FakeLock())
    monkeypatch.setattr(dc, "_read_state", lambda: next(states))
    monkeypatch.setattr(dc, "worker_alive", lambda _meta: False)
    monkeypatch.setattr(dc, "read_meta", lambda: state.previous_meta)

    def rollback(value: dc.RollbackState) -> bool:
        calls.append(value)
        return True

    monkeypatch.setattr(dc, "_rollback_locked", rollback)

    result = dc._handle_status(options)

    assert calls == [state]
    assert result["phase"] == "idle"
    assert result["last_outcome"] == dc.STATUS_ROLLED_BACK


def test_status_keeps_live_supervised_pending_mission(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A status query never rolls back a healthy supervisor-owned pending mission.

    The candidate identity is the never-alive supervisor placeholder, so the
    supervisor's durable child state is the only genuine liveness signal: the
    status path must use ``_mission_candidate_alive`` and report the pending
    phase instead of rolling the live deployment back.
    """
    base = pending_state()
    state = replace(base, new_meta=dc._placeholder_meta(base.commit, base.repo))
    options = make_options(tmp_path / "repo")
    rollbacks: list[dc.RollbackState] = []
    current: list[dc.RollbackState] = [state]

    monkeypatch.setattr(supervise, "supervisor_running", lambda: True)
    monkeypatch.setattr(
        supervise,
        "read_state",
        lambda: _supervisor_child_state(state.commit, state.generation),
    )
    monkeypatch.setattr(dc, "_read_state", lambda: current[0])
    monkeypatch.setattr(dc, "read_meta", lambda: state.previous_meta)
    monkeypatch.setattr(dc, "_reconcile_cli", lambda _state: None)
    monkeypatch.setattr(dc, "deploy_lock", lambda _timeout: _NoopDeployLock())

    def rollback(value: dc.RollbackState) -> bool:
        rollbacks.append(value)
        return True

    monkeypatch.setattr(dc, "_rollback_locked", rollback)

    result = dc._handle_status(options)

    assert rollbacks == []
    assert result["phase"] == "await-confirmation"
    assert result["proposed_commit"] == state.commit
    assert result["previous_commit"] == state.previous_commit
    assert result["deadline"] == state.deadline

    current[0] = replace(state, challenge_hash=dc._challenge_digest("3fa91c0"))

    result = dc._handle_status(options)

    assert rollbacks == []
    assert result["phase"] == "await-reversal"
    assert result["proposed_commit"] == state.commit
    assert result["previous_commit"] == state.previous_commit


def test_status_rolls_back_supervised_mission_when_candidate_gone(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A status query rolls back a supervised candidate the daemon no longer tracks."""
    base = pending_state()
    state = replace(base, new_meta=dc._placeholder_meta(base.commit, base.repo))
    rolled_back = replace(state, status=dc.STATUS_ROLLED_BACK)
    options = make_options(tmp_path / "repo")
    rollbacks: list[dc.RollbackState] = []
    states = iter((state, rolled_back))

    monkeypatch.setattr(supervise, "supervisor_running", lambda: True)
    monkeypatch.setattr(supervise, "read_state", supervise.fresh_state)
    monkeypatch.setattr(dc, "_read_state", lambda: next(states))
    monkeypatch.setattr(dc, "read_meta", lambda: state.previous_meta)
    monkeypatch.setattr(dc, "_reconcile_cli", lambda _state: None)
    monkeypatch.setattr(dc, "deploy_lock", lambda _timeout: _NoopDeployLock())

    def rollback(value: dc.RollbackState) -> bool:
        rollbacks.append(value)
        return True

    monkeypatch.setattr(dc, "_rollback_locked", rollback)

    result = dc._handle_status(options)

    assert rollbacks == [state]
    assert result["phase"] == "idle"
    assert result["last_outcome"] == dc.STATUS_ROLLED_BACK


def test_status_rolls_back_supervised_mission_after_deadline(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A status query rolls back a supervised mission whose window lapsed.

    Even a still-active candidate cannot keep a mission alive past its
    confirmation deadline: the deadline semantics alone must require rollback.
    """
    base = pending_state()
    state = replace(
        base,
        new_meta=dc._placeholder_meta(base.commit, base.repo),
        deadline=time.time() - 1,
    )
    rolled_back = replace(state, status=dc.STATUS_ROLLED_BACK)
    options = make_options(tmp_path / "repo")
    rollbacks: list[dc.RollbackState] = []
    states = iter((state, rolled_back))

    monkeypatch.setattr(supervise, "supervisor_running", lambda: True)
    monkeypatch.setattr(
        supervise,
        "read_state",
        lambda: _supervisor_child_state(state.commit, state.generation),
    )
    monkeypatch.setattr(dc, "_read_state", lambda: next(states))
    monkeypatch.setattr(dc, "read_meta", lambda: state.previous_meta)
    monkeypatch.setattr(dc, "_reconcile_cli", lambda _state: None)
    monkeypatch.setattr(dc, "deploy_lock", lambda _timeout: _NoopDeployLock())

    def rollback(value: dc.RollbackState) -> bool:
        rollbacks.append(value)
        return True

    monkeypatch.setattr(dc, "_rollback_locked", rollback)

    result = dc._handle_status(options)

    assert rollbacks == [state]
    assert result["phase"] == "idle"
    assert result["last_outcome"] == dc.STATUS_ROLLED_BACK


def test_status_keeps_live_legacy_pending_mission(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Without a supervisor, a live recorded candidate survives status."""
    state = pending_state()
    options = make_options(tmp_path / "repo")
    rollbacks: list[dc.RollbackState] = []

    monkeypatch.setattr(supervise, "supervisor_running", lambda: False)
    monkeypatch.setattr(dc, "_read_state", lambda: state)
    monkeypatch.setattr(dc, "worker_alive", lambda _meta: True)
    monkeypatch.setattr(dc, "read_meta", lambda: state.previous_meta)
    monkeypatch.setattr(dc, "_reconcile_cli", lambda _state: None)
    monkeypatch.setattr(dc, "deploy_lock", lambda _timeout: _NoopDeployLock())

    def rollback(value: dc.RollbackState) -> bool:
        rollbacks.append(value)
        return True

    monkeypatch.setattr(dc, "_rollback_locked", rollback)

    result = dc._handle_status(options)

    assert rollbacks == []
    assert result["phase"] == "await-confirmation"
    assert result["proposed_commit"] == state.commit
    assert result["previous_commit"] == state.previous_commit


def test_watchdog_child_drops_inherited_file_descriptors(tmp_path: Path) -> None:
    """A forked watchdog drops inherited descriptors without closing the parent's copy."""
    fd = os.open(tmp_path / "held", os.O_CREAT | os.O_RDWR)
    pid = os.fork()
    if pid == 0:
        os.closerange(3, int(os.sysconf("SC_OPEN_MAX")))
        try:
            os.fstat(fd)
        except OSError:
            os._exit(0)
        os._exit(1)

    _, status = os.waitpid(pid, 0)
    assert os.waitstatus_to_exitcode(status) == 0
    os.fstat(fd)
    os.close(fd)


def test_confirmation_switches_clis_only_after_confirmed(
    coherent_environment: tuple[Path, str, str, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The global CLIs move to the candidate only at terminal confirmation."""
    repo, first, second, bin_dir = coherent_environment
    state = pending_state(repo=str(repo), old=first, new=second)
    dc._write_state(state)
    monkeypatch.setattr(dc, "worker_alive", lambda _meta: True)
    options = make_options(repo)

    assert cli.current_commit() == first
    assert run_launcher(bin_dir / "lubko-agent") == f"lubko-agent@{first}"

    first_response = dc._confirm_locked({"type": "confirm", "commit": second}, options)
    assert cli.current_commit() == first
    assert run_launcher(bin_dir / "lubko-agent") == f"lubko-agent@{first}"

    challenge = first_response["challenge"]
    assert isinstance(challenge, str)
    second_response = dc._confirm_locked(
        {
            "type": "confirm",
            "commit": second,
            "challenge": challenge[::-1],
        },
        options,
    )

    assert second_response["confirmed"] is True
    assert cli.current_commit() == second
    assert run_launcher(bin_dir / "lubko-agent") == f"lubko-agent@{second}"
    assert run_launcher(bin_dir / "lubko-deploy-ctl") == f"lubko-deploy-ctl@{second}"
    final_state = dc._read_state()
    assert final_state is not None
    assert final_state.status == dc.STATUS_CONFIRMED
    final_meta = read_meta()
    assert final_meta is not None
    assert final_meta.git_commit == second


def test_wrong_challenge_rollback_preserves_previous_cli(
    coherent_environment: tuple[Path, str, str, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed confirmation rolls back without moving the CLI pointer."""
    repo, first, second, bin_dir = coherent_environment
    state = pending_state(repo=str(repo), old=first, new=second)
    challenged = replace(state, challenge_hash=dc._challenge_digest("3fa91c0"))
    dc._write_state(challenged)
    monkeypatch.setattr(dc, "worker_alive", lambda _meta: True)
    options = make_options(repo)
    patch_rollback_dependencies(monkeypatch)

    with pytest.raises(dc.DeployCtlError, match="incorrect"):
        dc._confirm_locked(
            {
                "type": "confirm",
                "commit": second,
                "challenge": "1111111",
            },
            options,
        )

    assert cli.current_commit() == first
    assert run_launcher(bin_dir / "lubko-agent") == f"lubko-agent@{first}"
    assert not cli.cli_commit_dir(second).exists()
    rolled_back = dc._read_state()
    assert rolled_back is not None
    assert rolled_back.status == dc.STATUS_ROLLED_BACK


def test_rollback_on_candidate_failure_preserves_previous_cli(
    coherent_environment: tuple[Path, str, str, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A watchdog-style rollback restores the previous CLI without stranding."""
    repo, first, second, bin_dir = coherent_environment
    state = pending_state(repo=str(repo), old=first, new=second)
    monkeypatch.setattr(dc, "worker_alive", lambda _meta: False)
    patch_rollback_dependencies(monkeypatch)

    assert dc._rollback_locked(state) is True

    assert cli.current_commit() == first
    assert run_launcher(bin_dir / "lubko-agent") == f"lubko-agent@{first}"
    assert not cli.cli_commit_dir(second).exists()
    restored_meta = read_meta()
    assert restored_meta is not None
    assert restored_meta.git_commit == first


def test_provisional_candidate_build_does_not_switch_clis(
    coherent_environment: tuple[Path, str, str, Path],
) -> None:
    """Building the candidate CLI environment never touches the pointer."""
    repo, first, second, bin_dir = coherent_environment
    cli.build_cli_root(repo, second, "uv", 60.0)

    assert cli.current_commit() == first
    assert run_launcher(bin_dir / "lubko-agent") == f"lubko-agent@{first}"


def test_confirmation_activation_failure_keeps_previous_root(
    coherent_environment: tuple[Path, str, str, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed CLI switch during confirmation never breaks the prior CLI."""
    repo, first, second, bin_dir = coherent_environment
    state = pending_state(repo=str(repo), old=first, new=second)
    challenged = replace(state, challenge_hash=dc._challenge_digest("3fa91c0"))
    dc._write_state(challenged)
    write_meta(worker_meta(first, pid=100, repo=str(repo)))
    monkeypatch.setattr(dc, "worker_alive", lambda _meta: True)
    options = make_options(repo)

    original_set_current = cli.set_current
    attempts: list[str] = []

    def flaky_set_current(commit: str) -> None:
        attempts.append(commit)
        if len(attempts) == 1:
            msg = "switch boom"
            raise cli.CliError(msg)
        original_set_current(commit)

    monkeypatch.setattr(cli, "set_current", flaky_set_current)

    response = dc._confirm_locked(
        {
            "type": "confirm",
            "commit": second,
            "challenge": "3fa91c0"[::-1],
        },
        options,
    )

    assert response["confirmed"] is True
    assert cli.current_commit() == first
    assert cli.cli_commit_dir(first).is_dir()
    assert cli.cli_commit_dir(second).is_dir()
    assert run_launcher(bin_dir / "lubko-agent") == f"lubko-agent@{first}"

    dc._handle_status(options)

    assert cli.current_commit() == second
    assert run_launcher(bin_dir / "lubko-agent") == f"lubko-agent@{second}"


def test_checkout_aborts_and_restores_when_candidate_cli_build_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed candidate CLI build aborts checkout and restores the checkout."""
    repo, first, second = make_repo(tmp_path / "repo")
    monkeypatch.setattr(cli, "_sync_venv", fake_uv_sync)
    write_meta(worker_meta(first, pid=100, repo=str(repo)))
    monkeypatch.setattr(dc, "worker_alive", lambda _meta: True)
    monkeypatch.setattr(
        dc,
        "run_validation",
        lambda _repo, _uv, _timeout: ValidationReport(ok=True, detail=""),
    )

    def broken_build(_repo: Path, _commit: str, _uv: str, _timeout: float) -> Path:
        msg = "build boom"
        raise cli.CliError(msg)

    monkeypatch.setattr(cli, "build_cli_root", broken_build)
    options = make_options(repo)

    with pytest.raises(dc.DeployCtlError, match="candidate CLI environment could not be built"):
        dc._deploy_locked(options, second)

    assert cli.git_commit(repo, 5.0) == first
    assert cli.current_commit() is None
    preserved_meta = read_meta()
    assert preserved_meta is not None
    assert preserved_meta.git_commit == first


def test_status_reconciles_stale_confirmed_pointer(
    coherent_environment: tuple[Path, str, str, Path],
) -> None:
    """A crash after confirmed state but before the CLI switch is repaired."""
    repo, first, second, bin_dir = coherent_environment
    confirmed = replace(
        pending_state(repo=str(repo), old=first, new=second),
        status=dc.STATUS_CONFIRMED,
    )
    dc._write_state(confirmed)
    write_meta(worker_meta(second, pid=200, repo=str(repo)))

    assert cli.current_commit() == first
    assert run_launcher(bin_dir / "lubko-agent") == f"lubko-agent@{first}"

    result = dc._handle_status(make_options(repo))

    assert result["phase"] == "idle"
    assert cli.current_commit() == second
    assert run_launcher(bin_dir / "lubko-agent") == f"lubko-agent@{second}"
    repaired = dc._read_state()
    assert repaired is not None
    assert repaired.status == dc.STATUS_CONFIRMED


def test_status_reconciles_plain_deploy_crash(
    coherent_environment: tuple[Path, str, str, Path],
) -> None:
    """A plain-deploy crash (meta ahead of the CLI pointer) is repaired."""
    repo, first, second, bin_dir = coherent_environment
    write_meta(worker_meta(second, pid=200, repo=str(repo)))

    assert cli.current_commit() == first
    dc._handle_status(make_options(repo))

    assert cli.current_commit() == second
    assert run_launcher(bin_dir / "lubko-agent") == f"lubko-agent@{second}"


def test_status_never_reconciles_to_a_pending_candidate(
    coherent_environment: tuple[Path, str, str, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """While a mission is pending the pointer stays on the previous commit."""
    repo, first, second, bin_dir = coherent_environment
    state = pending_state(repo=str(repo), old=first, new=second)
    dc._write_state(state)
    write_meta(worker_meta(first, pid=100, repo=str(repo)))
    monkeypatch.setattr(dc, "worker_alive", lambda _meta: True)
    cli.set_current(second)

    dc._handle_status(make_options(repo))

    assert cli.current_commit() == first
    assert run_launcher(bin_dir / "lubko-agent") == f"lubko-agent@{first}"


def test_rollback_keeps_previous_root_and_drops_candidate_root(
    coherent_environment: tuple[Path, str, str, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Rollback retains the previous environment and removes the candidate's."""
    repo, first, second, bin_dir = coherent_environment
    state = pending_state(repo=str(repo), old=first, new=second)
    monkeypatch.setattr(dc, "worker_alive", lambda _meta: False)
    patch_rollback_dependencies(monkeypatch)

    assert dc._rollback_locked(state) is True

    assert cli.current_commit() == first
    assert cli.cli_commit_dir(first).is_dir()
    assert not cli.cli_commit_dir(second).exists()
    assert run_launcher(bin_dir / "lubko-agent") == f"lubko-agent@{first}"


def test_rollback_without_staged_candidate_root_succeeds(
    coherent_environment: tuple[Path, str, str, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An old watchdog that never staged a candidate root still rolls back."""
    repo, first, second, bin_dir = coherent_environment
    cli.remove_cli_root(second)
    state = pending_state(repo=str(repo), old=first, new=second)
    monkeypatch.setattr(dc, "worker_alive", lambda _meta: False)
    patch_rollback_dependencies(monkeypatch)

    assert dc._rollback_locked(state) is True

    assert cli.current_commit() == first
    assert run_launcher(bin_dir / "lubko-agent") == f"lubko-agent@{first}"
    rolled_back = dc._read_state()
    assert rolled_back is not None
    assert rolled_back.status == dc.STATUS_ROLLED_BACK


def test_rollback_restores_pointer_off_a_provisional_candidate(
    coherent_environment: tuple[Path, str, str, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Rollback pulls the pointer back even if it sits on the candidate."""
    repo, first, second, bin_dir = coherent_environment
    state = pending_state(repo=str(repo), old=first, new=second)
    monkeypatch.setattr(dc, "worker_alive", lambda _meta: False)
    patch_rollback_dependencies(monkeypatch)
    cli.set_current(second)

    assert dc._rollback_locked(state) is True

    assert cli.current_commit() == first
    assert run_launcher(bin_dir / "lubko-agent") == f"lubko-agent@{first}"


def test_restart_previous_reuses_healthy_previous_before_retirement(
    tmp_path: Path,
) -> None:
    """A never-retired alive previous worker is reused under its exact identity."""
    proc = spawn_real_process(RETIRING_MARKER)
    try:
        previous = real_meta(proc, tmp_path, "1" * 40, RETIRING_MARKER)
        assert worker_alive(previous)
        state = replace(
            pending_state(repo=str(tmp_path)),
            previous_meta=previous,
        )

        restored = dc._restart_previous(state)

        assert restored == previous
        assert restored.pid == previous.pid
        assert restored.start_time_ticks == previous.start_time_ticks
        assert proc.poll() is None
    finally:
        kill_proc(proc)


def test_restart_previous_rejects_lingering_previous_after_retirement_begins(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Rollback never accepts a momentarily-alive retiring previous worker."""
    repo, first, _second = make_repo(tmp_path / "repo")
    lifecycle.worker_state_dir().mkdir(parents=True, exist_ok=True)
    lingering = spawn_lingering_previous()
    spawned: list[subprocess.Popen[bytes]] = []
    try:
        previous = real_meta(lingering, repo, first, RETIRING_MARKER)
        assert worker_alive(previous)
        state = replace(
            pending_state(repo=str(repo), old=first, previous_retiring=True),
            previous_meta=previous,
            stop_grace_seconds=0.3,
        )
        patch_fresh_worker_spawn(monkeypatch, spawned)

        restored = dc._restart_previous(state)

        assert restored is not None
        assert restored.pid != previous.pid
        assert restored.git_commit == first
        assert worker_alive(restored)
        assert lingering.poll() is not None
        assert not worker_alive(previous)
    finally:
        with suppress(Exception):
            lingering.wait(timeout=5)
        kill_many(spawned)


def test_deploy_locked_persists_retirement_marker_before_stopping_previous(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The durable retirement marker is written before the previous worker stops."""
    repo, first, second = make_repo(tmp_path / "repo")
    previous = worker_meta(first, pid=100, repo=str(repo))
    written: list[dc.RollbackState] = []
    stopped: list[WorkerMeta] = []
    gated_proc: subprocess.Popen[bytes] | None = None

    monkeypatch.setattr(dc, "_cleanup_pending_locked", lambda: None)
    monkeypatch.setattr(dc, "read_meta", lambda: previous)
    monkeypatch.setattr(dc, "worker_alive", lambda _meta: True)
    monkeypatch.setattr(dc, "_require_exact_commit", lambda *_args: None)
    monkeypatch.setattr(dc, "_require_clean_checkout", lambda *_args: None)

    def checkout_ok(_repo: Path, _commit: str, _timeout: float, *, force: bool) -> bool:
        assert not force
        return True

    monkeypatch.setattr(dc, "_checkout", checkout_ok)
    monkeypatch.setattr(
        dc,
        "run_validation",
        lambda _repo, _uv, _timeout: ValidationReport(ok=True, detail=""),
    )
    monkeypatch.setattr(cli, "build_cli_root", lambda *_args, **_kwargs: Path())
    monkeypatch.setattr(dc, "check_postgres", lambda _timeout: True)
    monkeypatch.setattr(dc, "_fork_watchdog", lambda _timeout: None)
    monkeypatch.setattr(dc, "_write_state", written.append)

    def record_stop(meta: WorkerMeta, _grace: float) -> bool:
        stopped.append(meta)
        return True

    monkeypatch.setattr(dc, "stop_worker", record_stop)
    monkeypatch.setattr(dc, "_release_gate", lambda _writer: None)
    monkeypatch.setattr(dc, "_close_gate", lambda _writer: None)
    monkeypatch.setattr(dc, "_wait_for_released_worker", lambda _meta: True)

    def fake_gated(_options: dc.Options, _commit: str) -> dc.GatedWorker:
        nonlocal gated_proc
        gated_proc = subprocess.Popen(
            [SLEEP_BIN, "300"],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
            close_fds=True,
        )
        guard.register(gated_proc)
        return dc.GatedWorker(
            proc=gated_proc,
            gate_writer=0,
            meta=worker_meta(second, pid=200, repo=str(repo)),
        )

    monkeypatch.setattr(dc, "_spawn_gated_candidate", fake_gated)

    try:
        result = dc._deploy_locked(make_options(repo), second)

        assert len(written) == 3
        assert written[0].previous_retiring is False
        assert written[1].previous_retiring is True
        assert written[2].previous_retiring is True
        assert stopped == [previous]
        assert result.previous_retiring is True
    finally:
        if gated_proc is not None:
            kill_proc(gated_proc)


def test_watchdog_rollback_bad_candidate_restores_maintained_worker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A watchdog rollback leaves previous commit, a live worker, and rolled_back."""
    repo, first, second = make_repo(tmp_path / "repo")
    monkeypatch.setattr(cli, "_sync_venv", fake_uv_sync)
    cli.build_cli_root(repo, first, "uv", 60.0)
    cli.build_cli_root(repo, second, "uv", 60.0)
    bin_dir = tmp_path / "bin"
    cli.install_launchers(bin_dir)
    cli.set_current(first)

    previous_proc = spawn_lingering_previous()
    candidate_proc = spawn_real_process(CANDIDATE_MARKER)
    spawned: list[subprocess.Popen[bytes]] = []
    try:
        previous_meta = real_meta(previous_proc, repo, first, RETIRING_MARKER)
        candidate_meta = real_meta(candidate_proc, repo, second, CANDIDATE_MARKER)
        kill_proc(candidate_proc)
        assert not worker_alive(candidate_meta)
        state = replace(
            pending_state(repo=str(repo), old=first, new=second, previous_retiring=True),
            previous_meta=previous_meta,
            new_meta=candidate_meta,
            stop_grace_seconds=0.3,
            deadline=time.time() - 1,
        )
        dc._write_state(state)
        patch_fresh_worker_spawn(monkeypatch, spawned)

        dc._watchdog_main(lock_timeout_seconds=1.0)

        rolled = dc._read_state()
        assert rolled is not None
        assert rolled.status == dc.STATUS_ROLLED_BACK
        assert cli.git_commit(repo, 5.0) == first
        meta = read_meta()
        assert meta is not None
        assert meta.git_commit == first
        assert meta.pid == spawned[0].pid
        assert worker_alive(meta)
        assert previous_proc.poll() is not None
        assert cli.current_commit() == first
        assert run_launcher(bin_dir / "lubko-agent") == f"lubko-agent@{first}"
        assert not cli.cli_commit_dir(second).exists()
    finally:
        with suppress(Exception):
            previous_proc.wait(timeout=5)
        with suppress(Exception):
            candidate_proc.wait(timeout=5)
        kill_many(spawned)


def write_database_config(tmp_path: Path, cluster: _pg.PgCluster) -> Path:
    """Write a private database configuration file for the cluster.

    Args:
        tmp_path: Temporary directory for the configuration file.
        cluster: The running PostgreSQL cluster.

    Returns:
        The configuration file path.
    """
    conf = tmp_path / "database.conf"
    conf.write_text(
        f"host={cluster.socket_dir}\n"
        f"port={cluster.port}\n"
        "dbname=postgres\n"
        "user=postgres\n"
        "password=local-trust\n",
        encoding="utf-8",
    )
    conf.chmod(0o600)
    return conf


def insert_running_job(conninfo: str, cwd: str, command: str) -> object:
    """Insert a protocol v2 running command job and return its id.

    Args:
        conninfo: PostgreSQL connection string.
        cwd: Working directory for the job.
        command: Shell command to run.

    Returns:
        The job identifier.
    """
    payload = json.dumps({
        "v": 2,
        "type": "command",
        "request": {"cwd": cwd, "command": command},
        "state": {"status": "running"},
    })
    with psycopg.connect(conninfo) as conn:
        row = conn.execute(
            "INSERT INTO lubko.jobs (payload) VALUES (%s) RETURNING id",
            (payload,),
        ).fetchone()
    assert row is not None
    return row[0]


def read_job_status(conninfo: str, job_id: object) -> str:
    """Read the current status of one job row.

    Args:
        conninfo: PostgreSQL connection string.
        job_id: The job identifier.

    Returns:
        The job status.
    """
    with psycopg.connect(conninfo) as conn:
        row = conn.execute(
            "SELECT (payload::jsonb)->'state'->>'status' FROM lubko.jobs WHERE id = %s",
            (job_id,),
        ).fetchone()
    assert row is not None
    return str(row[0])


def test_read_pipe_line_reads_one_line() -> None:
    """The parent reads exactly one newline-terminated helper response line."""
    reader, writer = os.pipe()
    try:
        os.write(writer, b'{"type": "checkout", "ok": true}\n')
        assert dc._read_pipe_line(reader) == '{"type": "checkout", "ok": true}'
    finally:
        os.close(reader)
        os.close(writer)


def test_read_pipe_line_returns_empty_on_eof() -> None:
    """A helper that dies before writing yields an empty read deterministically."""
    reader, writer = os.pipe()
    os.close(writer)
    try:
        assert not dc._read_pipe_line(reader)
    finally:
        os.close(reader)


def test_queue_checkout_parent_reports_helper_response(monkeypatch: pytest.MonkeyPatch) -> None:
    """The queue-checkout parent reports the helper response and returns."""
    expected = {"type": "checkout", "ok": True, "phase": "pending"}

    def fake_helper(_options: dc.Options, _commit: str, _job_id: object, writer: int) -> None:
        del _options, _commit, _job_id
        os.write(writer, (json.dumps(expected, sort_keys=True) + "\n").encode())
        os._exit(0)

    monkeypatch.setattr(dc, "_run_helper", fake_helper)
    response = dc._queue_checkout(make_options(Path("/repo")), "2" * 40, uuid4())

    assert response == expected
    assert response["ok"] is True


def test_queue_checkout_raises_when_helper_dies_silently(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A helper that dies before writing reports a deterministic parent error."""
    monkeypatch.setattr(dc, "_run_helper", lambda *_args: os._exit(0))

    with pytest.raises(dc.DeployCtlError, match="exited before reporting"):
        dc._queue_checkout(make_options(Path("/repo")), "2" * 40, uuid4())


def test_queue_checkout_parent_does_not_wait_for_helper_finish(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The parent exits as soon as the response is delivered, never the handoff."""
    released = threading.Event()

    def fake_helper(_options: dc.Options, _commit: str, _job_id: object, writer: int) -> None:
        os.write(writer, b'{"type": "checkout", "ok": true}\n')
        released.wait(timeout=10)
        os._exit(0)

    monkeypatch.setattr(dc, "_run_helper", fake_helper)

    start = time.monotonic()
    dc._queue_checkout(make_options(Path("/repo")), "2" * 40, uuid4())
    elapsed = time.monotonic() - start
    released.set()

    assert elapsed < 2.0


def test_handle_checkout_uses_queue_path_when_queue_owned(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A queue-owned checkout dispatches to the helper path with the captured id."""
    job_id = uuid4()
    captured: list[object] = []
    monkeypatch.setattr(dc, "_current_queue_job_id", lambda: (job_id, False))

    def fake_queue(_options: dc.Options, _commit: str, received: object) -> dict[str, object]:
        captured.append(received)
        return {"type": "checkout", "ok": True, "phase": "pending"}

    monkeypatch.setattr(dc, "_queue_checkout", fake_queue)

    response = dc._handle_checkout(make_options(tmp_path), {"type": "checkout", "commit": "2" * 40})

    assert response["ok"] is True
    assert captured == [job_id]


def test_handle_checkout_rejects_cancelled_queue_owner(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A cancelled queue owner aborts before any handoff work."""
    monkeypatch.setattr(dc, "_current_queue_job_id", lambda: (uuid4(), True))

    with pytest.raises(dc.DeployCtlError, match="cancelled"):
        dc._handle_checkout(make_options(tmp_path), {"type": "checkout", "commit": "2" * 40})


def test_handle_checkout_manual_path_stays_synchronous(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A manual checkout retains the synchronous locked safe path."""
    repo, first, second = make_repo(tmp_path / "repo")
    monkeypatch.setattr(dc, "_current_queue_job_id", lambda: (None, False))
    monkeypatch.setattr(dc, "_reconcile_cli", lambda _state: None)
    state = pending_state(repo=str(repo), old=first, new=second)
    acquired: list[bool] = []

    class FakeLock:
        """Minimal deployment-lock context for the manual path."""

        def __enter__(self) -> None:
            acquired.append(True)

        def __exit__(self, *_args: object) -> None:
            return None

    monkeypatch.setattr(dc, "deploy_lock", lambda _timeout: FakeLock())
    monkeypatch.setattr(dc, "_deploy_locked", lambda _options, _commit: state)

    response = dc._handle_checkout(make_options(repo), {"type": "checkout", "commit": second})

    assert acquired == [True]
    assert response["ok"] is True
    assert response["commit"] == second


def test_prepare_locked_is_reversible_and_leaves_previous_running(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Preparation never stops the previous worker and stays reversible."""
    repo, first, second = make_repo(tmp_path / "repo")
    previous = worker_meta(first, pid=100, repo=str(repo))
    written: list[dc.RollbackState] = []
    stopped: list[WorkerMeta] = []
    gated_proc: subprocess.Popen[bytes] | None = None

    monkeypatch.setattr(dc, "_cleanup_pending_locked", lambda: None)
    monkeypatch.setattr(dc, "read_meta", lambda: previous)
    monkeypatch.setattr(dc, "worker_alive", lambda _meta: True)
    monkeypatch.setattr(dc, "_require_exact_commit", lambda *_args: None)
    monkeypatch.setattr(dc, "_require_clean_checkout", lambda *_args: None)
    monkeypatch.setattr(dc, "_checkout", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(
        dc,
        "run_validation",
        lambda _repo, _uv, _timeout: ValidationReport(ok=True, detail=""),
    )
    monkeypatch.setattr(cli, "build_cli_root", lambda *_args, **_kwargs: Path())
    monkeypatch.setattr(dc, "check_postgres", lambda _timeout: True)
    monkeypatch.setattr(dc, "_write_state", written.append)
    monkeypatch.setattr(dc, "_fork_watchdog", lambda _timeout: None)

    def record_stop(meta: WorkerMeta, _grace: float) -> bool:
        stopped.append(meta)
        return True

    monkeypatch.setattr(dc, "stop_worker", record_stop)

    def fake_gated(_options: dc.Options, _commit: str) -> dc.GatedWorker:
        nonlocal gated_proc
        gated_proc = subprocess.Popen(
            [SLEEP_BIN, "300"],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
            close_fds=True,
        )
        guard.register(gated_proc)
        return dc.GatedWorker(
            proc=gated_proc,
            gate_writer=0,
            meta=worker_meta(second, pid=200, repo=str(repo)),
        )

    monkeypatch.setattr(dc, "_spawn_gated_candidate", fake_gated)

    try:
        state, gated = dc._prepare_locked(make_options(repo), second, supervised=False)

        assert gated is not None
        assert state.previous_retiring is False
        assert state.previous_meta == previous
        assert state.new_meta == gated.meta
        assert len(written) == 1
        assert written[0].previous_retiring is False
        assert stopped == []
    finally:
        if gated_proc is not None:
            kill_proc(gated_proc)


def test_prepare_locked_restores_previous_on_validation_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed candidate validation restores the previous checkout."""
    repo, first, second = make_repo(tmp_path / "repo")
    previous = worker_meta(first, pid=100, repo=str(repo))
    checkouts: list[tuple[str, bool]] = []
    monkeypatch.setattr(dc, "_cleanup_pending_locked", lambda: None)
    monkeypatch.setattr(dc, "read_meta", lambda: previous)
    monkeypatch.setattr(dc, "worker_alive", lambda _meta: True)
    monkeypatch.setattr(dc, "_require_exact_commit", lambda *_args: None)
    monkeypatch.setattr(dc, "_require_clean_checkout", lambda *_args: None)

    def checkout(_repo: Path, commit: str, _timeout: float, *, force: bool) -> bool:
        checkouts.append((commit, force))
        return True

    monkeypatch.setattr(dc, "_checkout", checkout)
    monkeypatch.setattr(
        dc,
        "run_validation",
        lambda _repo, _uv, _timeout: ValidationReport(ok=False, detail="boom"),
    )
    monkeypatch.setattr(cli, "remove_cli_root", lambda _commit: None)

    with pytest.raises(dc.DeployCtlError, match="validation failed"):
        dc._prepare_locked(make_options(repo), second, supervised=False)

    assert checkouts == [(second, False), (first, True)]


def test_prepare_locked_rolls_back_when_watchdog_fork_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A watchdog fork failure undoes the prepared mission completely."""
    repo, first, second = make_repo(tmp_path / "repo")
    previous = worker_meta(first, pid=100, repo=str(repo))
    written: list[dc.RollbackState] = []
    rolled_back: list[dc.RollbackState] = []
    closed: list[int] = []
    gated_proc: subprocess.Popen[bytes] | None = None

    monkeypatch.setattr(dc, "_cleanup_pending_locked", lambda: None)
    monkeypatch.setattr(dc, "read_meta", lambda: previous)
    monkeypatch.setattr(dc, "worker_alive", lambda _meta: True)
    monkeypatch.setattr(dc, "_require_exact_commit", lambda *_args: None)
    monkeypatch.setattr(dc, "_require_clean_checkout", lambda *_args: None)
    monkeypatch.setattr(dc, "_checkout", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(
        dc,
        "run_validation",
        lambda _repo, _uv, _timeout: ValidationReport(ok=True, detail=""),
    )
    monkeypatch.setattr(cli, "build_cli_root", lambda *_args, **_kwargs: Path())
    monkeypatch.setattr(dc, "check_postgres", lambda _timeout: True)
    monkeypatch.setattr(dc, "_write_state", written.append)

    def failing_fork_watchdog(_timeout: float) -> None:
        msg = "fork boom"
        raise dc.DeployCtlError(msg)

    monkeypatch.setattr(dc, "_fork_watchdog", failing_fork_watchdog)

    def record_close(fd: int) -> None:
        closed.append(fd)

    monkeypatch.setattr(dc, "_close_gate", record_close)

    def record_rollback(state: dc.RollbackState) -> bool:
        rolled_back.append(state)
        return True

    monkeypatch.setattr(dc, "_rollback_locked", record_rollback)

    def fake_gated(_options: dc.Options, _commit: str) -> dc.GatedWorker:
        nonlocal gated_proc
        gated_proc = subprocess.Popen(
            [SLEEP_BIN, "300"],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
            close_fds=True,
        )
        guard.register(gated_proc)
        return dc.GatedWorker(
            proc=gated_proc,
            gate_writer=7,
            meta=worker_meta(second, pid=200, repo=str(repo)),
        )

    monkeypatch.setattr(dc, "_spawn_gated_candidate", fake_gated)

    try:
        with pytest.raises(dc.DeployCtlError, match="fork boom"):
            dc._prepare_locked(make_options(repo), second, supervised=False)

        assert len(written) == 1
        assert written[0].previous_retiring is False
        assert closed == [7]
        assert len(rolled_back) == 1
        assert rolled_back[0].previous_retiring is False
    finally:
        if gated_proc is not None:
            kill_proc(gated_proc)


def test_abort_mission_closes_gate_and_restores_without_stopping_previous(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Aborting a mission never crosses into the destructive boundary."""
    repo, first, second = make_repo(tmp_path / "repo")
    state = pending_state(repo=str(repo), old=first, new=second)
    closed: list[int] = []
    restored: list[tuple[str, bool]] = []

    def record_close(fd: int) -> None:
        closed.append(fd)

    monkeypatch.setattr(dc, "_close_gate", record_close)

    def record_checkout(_repo: Path, commit: str, _timeout: float, *, force: bool) -> bool:
        restored.append((commit, force))
        return True

    monkeypatch.setattr(dc, "_checkout", record_checkout)

    dummy = subprocess.Popen(
        [SLEEP_BIN, "300"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        close_fds=True,
    )
    guard.register(dummy)
    try:
        dc._abort_mission(dc.GatedWorker(proc=dummy, gate_writer=9, meta=state.new_meta), state)
    finally:
        kill_proc(dummy)

    assert closed == [9]
    assert restored == [(first, True)]


def test_wait_for_durable_success_polls_until_succeeded(
    jobs_db: str,
    pg_cluster: _pg.PgCluster,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The helper waits for the exact row to become durably succeeded."""
    conf = write_database_config(tmp_path, pg_cluster)
    monkeypatch.setenv("LUBKO_DATABASE_CONFIG", str(conf))
    job_id = insert_running_job(jobs_db, str(tmp_path), "echo hi")

    def finalize() -> None:
        time.sleep(0.3)
        with psycopg.connect(jobs_db) as conn:
            conn.execute(
                "UPDATE lubko.jobs\n"
                "SET payload = jsonb_set(\n"
                "    jsonb_set(payload::jsonb, '{state,status}', to_jsonb('succeeded'::text)),\n"
                "    '{state,finished_at}', to_jsonb('2026-01-01T00:00:00.000000Z'::text)\n"
                ")::text\n"
                "WHERE id = %s",
                (job_id,),
            )

    thread = threading.Thread(target=finalize)
    thread.start()
    try:
        dc._wait_for_durable_success(job_id, time.time() + 10)
    finally:
        thread.join(timeout=10)

    assert read_job_status(jobs_db, job_id) == "succeeded"


def test_wait_for_durable_success_rejects_cancelled(
    jobs_db: str,
    pg_cluster: _pg.PgCluster,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A row that reaches cancelled before success aborts the helper."""
    conf = write_database_config(tmp_path, pg_cluster)
    monkeypatch.setenv("LUBKO_DATABASE_CONFIG", str(conf))
    job_id = insert_running_job(jobs_db, str(tmp_path), "echo hi")
    with psycopg.connect(jobs_db) as conn:
        conn.execute(
            "UPDATE lubko.jobs\n"
            "SET payload = jsonb_set("
            "payload::jsonb, '{state,status}', to_jsonb('cancelled'::text))::text\n"
            "WHERE id = %s",
            (job_id,),
        )

    with pytest.raises(dc.DeployCtlError, match="cancelled before durable success"):
        dc._wait_for_durable_success(job_id, time.time() + 5)


def test_wait_for_durable_success_rejects_failed(
    jobs_db: str,
    pg_cluster: _pg.PgCluster,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A row that reaches failed before success aborts the helper."""
    conf = write_database_config(tmp_path, pg_cluster)
    monkeypatch.setenv("LUBKO_DATABASE_CONFIG", str(conf))
    job_id = insert_running_job(jobs_db, str(tmp_path), "echo hi")
    with psycopg.connect(jobs_db) as conn:
        conn.execute(
            "UPDATE lubko.jobs\n"
            "SET payload = jsonb_set("
            "payload::jsonb, '{state,status}', to_jsonb('failed'::text))::text\n"
            "WHERE id = %s",
            (job_id,),
        )

    with pytest.raises(dc.DeployCtlError, match="failed before durable success"):
        dc._wait_for_durable_success(job_id, time.time() + 5)


def test_wait_for_durable_success_rejects_deleted(
    jobs_db: str,
    pg_cluster: _pg.PgCluster,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A deleted row aborts the helper before any destructive work."""
    conf = write_database_config(tmp_path, pg_cluster)
    monkeypatch.setenv("LUBKO_DATABASE_CONFIG", str(conf))
    job_id = insert_running_job(jobs_db, str(tmp_path), "echo hi")
    with psycopg.connect(jobs_db) as conn:
        conn.execute("DELETE FROM lubko.jobs WHERE id = %s", (job_id,))

    with pytest.raises(dc.DeployCtlError, match="deleted before durable success"):
        dc._wait_for_durable_success(job_id, time.time() + 5)


def test_wait_for_durable_success_rejects_deadline(
    jobs_db: str,
    pg_cluster: _pg.PgCluster,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A row that never reaches success before the deadline aborts the helper."""
    conf = write_database_config(tmp_path, pg_cluster)
    monkeypatch.setenv("LUBKO_DATABASE_CONFIG", str(conf))
    job_id = insert_running_job(jobs_db, str(tmp_path), "echo hi")

    with pytest.raises(dc.DeployCtlError, match="before the deadline"):
        dc._wait_for_durable_success(job_id, time.time() + 0.2)


def test_queue_detection_treats_missing_injection_as_manual(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Without an injected root job UUID the invocation is a manual checkout."""
    monkeypatch.delenv(worker.JOB_ID_ENV, raising=False)
    assert dc._current_queue_job_id() == (None, False)


def test_queue_detection_uses_injected_job_id_without_process_pgid(
    jobs_db: str,
    pg_cluster: _pg.PgCluster,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Queue detection relies on the injected UUID, never on process_pgid timing.

    The row is forced to carry a process group that can never match the current
    process group (the BLOCKER Q8 race: ``_persist_process`` has not committed
    yet), yet the exact injected root job UUID still selects the queue path.
    """
    conf = write_database_config(tmp_path, pg_cluster)
    monkeypatch.setenv("LUBKO_DATABASE_CONFIG", str(conf))
    job_id = insert_running_job(jobs_db, str(tmp_path), "echo hi")
    with psycopg.connect(jobs_db) as conn:
        conn.execute(
            "UPDATE lubko.jobs\n"
            "SET payload = jsonb_set("
            "payload::jsonb, '{state,process_pgid}', to_jsonb(2147483647::int))::text\n"
            "WHERE id = %s",
            (job_id,),
        )
    monkeypatch.setenv(worker.JOB_ID_ENV, str(job_id))

    assert dc._current_queue_job_id() == (job_id, False)


def test_queue_detection_reports_cancellation_marker(
    jobs_db: str,
    pg_cluster: _pg.PgCluster,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A cancellation marker on the injected row is reported as cancelled."""
    conf = write_database_config(tmp_path, pg_cluster)
    monkeypatch.setenv("LUBKO_DATABASE_CONFIG", str(conf))
    job_id = insert_running_job(jobs_db, str(tmp_path), "echo hi")
    with psycopg.connect(jobs_db) as conn:
        conn.execute(
            "UPDATE lubko.jobs\n"
            "SET payload = jsonb_set("
            "payload::jsonb, '{state,cancel_requested_at}', "
            "to_jsonb('2026-01-01T00:00:00.000000Z'::text))::text\n"
            "WHERE id = %s",
            (job_id,),
        )
    monkeypatch.setenv(worker.JOB_ID_ENV, str(job_id))

    assert dc._current_queue_job_id() == (job_id, True)


def test_queue_detection_rejects_deleted_injected_row(
    jobs_db: str,
    pg_cluster: _pg.PgCluster,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A deleted injected row fails closed instead of taking the manual path."""
    conf = write_database_config(tmp_path, pg_cluster)
    monkeypatch.setenv("LUBKO_DATABASE_CONFIG", str(conf))
    job_id = insert_running_job(jobs_db, str(tmp_path), "echo hi")
    with psycopg.connect(jobs_db) as conn:
        conn.execute("DELETE FROM lubko.jobs WHERE id = %s", (job_id,))
    monkeypatch.setenv(worker.JOB_ID_ENV, str(job_id))

    with pytest.raises(dc.DeployCtlError, match="does not exist"):
        dc._current_queue_job_id()


def _controller_main_args(request: str, repo: Path) -> list[str]:
    """Build the argv for one in-process controller invocation.

    Args:
        request: JSON request object.
        repo: Deployment checkout.

    Returns:
        The controller argv with a resolved ``uv`` executable.
    """
    uv = shutil.which("uv")
    assert uv is not None
    return [request, "--repo", str(repo), "--uv", uv]


def test_queue_checkout_success_response_exits_ok(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A genuine candidate response keeps the checkout job exit code at zero."""
    expected = {"type": "checkout", "ok": True, "phase": "pending"}

    def fake_helper(_options: dc.Options, _commit: str, _job_id: object, writer: int) -> None:
        del _options, _commit, _job_id
        os.write(writer, (json.dumps(expected, sort_keys=True) + "\n").encode())
        os._exit(0)

    monkeypatch.setattr(dc, "_current_queue_job_id", lambda: (uuid4(), False))
    monkeypatch.setattr(dc, "_run_helper", fake_helper)
    commit = "2" * 40
    code = dc.main(
        _controller_main_args(json.dumps({"type": "checkout", "commit": commit}), tmp_path / "repo")
    )

    assert code == dc.EXIT_OK


def test_queue_checkout_error_response_exits_failed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A reported helper error fails the checkout job rather than faking success."""
    error = {"ok": False, "error": "candidate validation failed"}

    def fake_helper(_options: dc.Options, _commit: str, _job_id: object, writer: int) -> None:
        del _options, _commit, _job_id
        os.write(writer, (json.dumps(error, sort_keys=True) + "\n").encode())
        os._exit(0)

    monkeypatch.setattr(dc, "_current_queue_job_id", lambda: (uuid4(), False))
    monkeypatch.setattr(dc, "_run_helper", fake_helper)
    commit = "2" * 40
    code = dc.main(
        _controller_main_args(json.dumps({"type": "checkout", "commit": commit}), tmp_path / "repo")
    )

    assert code == dc.EXIT_ERROR


def test_queue_checkout_helper_death_exits_failed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A helper that dies silently fails the checkout job, never faking success."""
    monkeypatch.setattr(dc, "_current_queue_job_id", lambda: (uuid4(), False))
    monkeypatch.setattr(dc, "_run_helper", lambda *_args: os._exit(0))
    commit = "2" * 40
    code = dc.main(
        _controller_main_args(json.dumps({"type": "checkout", "commit": commit}), tmp_path / "repo")
    )

    assert code == dc.EXIT_ERROR


def test_confirm_rejection_keeps_zero_exit_code(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A confirm rejection still returns zero so its structured error is delivered."""
    monkeypatch.setattr(
        dc, "_handle_confirm", lambda _options, _request: {"ok": False, "error": "no"}
    )
    code = dc.main(
        _controller_main_args(json.dumps({"type": "confirm", "commit": "2" * 40}), tmp_path)
    )
    assert code == dc.EXIT_OK


def test_helper_uses_fresh_durable_success_deadline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The durable-success wait deadline is computed after preparation.

    A preparation that outlived the original confirmation window must not
    expire the handoff wait before it even starts (the review nonblocker).
    """
    repo, first, second = make_repo(tmp_path / "repo")
    stale = replace(
        pending_state(repo=str(repo), old=first, new=second),
        deadline=time.time() - 1000.0,
    )
    gated_proc = subprocess.Popen(
        [SLEEP_BIN, "300"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        close_fds=True,
    )
    guard.register(gated_proc)
    gated = dc.GatedWorker(proc=gated_proc, gate_writer=-1, meta=stale.new_meta)
    captured: dict[str, float] = {}
    monkeypatch.setattr(dc, "_prepare_locked", lambda _options, _commit, **_kwargs: (stale, gated))
    monkeypatch.setattr(dc, "_send_helper_response", lambda _writer, _response: None)
    monkeypatch.setattr(dc, "_complete_handoff", lambda _options, _state, _gated: stale)

    def capture_deadline(_job_id: object, deadline: float) -> None:
        captured["deadline"] = deadline

    monkeypatch.setattr(dc, "_wait_for_durable_success", capture_deadline)
    reader, writer = os.pipe()
    try:
        os.close(reader)
        dc._helper_locked(make_options(repo), second, uuid4(), writer)
    finally:
        os.close(writer)
        kill_proc(gated_proc)

    assert captured["deadline"] != stale.deadline
    assert captured["deadline"] > time.time()


# ---------------------------------------------------------------------------
# End-to-end process tests
# ---------------------------------------------------------------------------

REPO_ROOT: Final = Path(__file__).resolve().parent.parent
WORKER_TIMINGS: Final = {
    "LUBKO_POLL_INTERVAL_SECONDS": "0.05",
    "LUBKO_PROCESS_POLL_INTERVAL_SECONDS": "0.02",
    "LUBKO_CANCEL_GRACE_SECONDS": "0.5",
    "LUBKO_LEASE_DURATION_SECONDS": "2.0",
    "LUBKO_LEASE_REFRESH_INTERVAL_SECONDS": "0.15",
    "LUBKO_LEASE_RECOVERY_INTERVAL_SECONDS": "0.2",
    "LUBKO_LEASE_SAFETY_MARGIN_SECONDS": "0.3",
    "LUBKO_OUTPUT_PUBLICATION_INTERVAL_SECONDS": "0.1",
    "LUBKO_CLAIM_BATCH_LIMIT": "16",
}


def wait_until(predicate: Callable[[], bool], timeout: float = 60.0) -> None:
    """Poll until a predicate holds, raising if the deadline expires.

    Args:
        predicate: Condition to satisfy.
        timeout: Maximum seconds to wait.

    Raises:
        AssertionError: If the condition is not met within the timeout.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    msg = "condition not met within timeout"
    raise AssertionError(msg)


def make_fake_uv(
    tmp_path: Path,
    python: str,
    *,
    bad_candidate_commit: str | None = None,
    fail_validation: bool = False,
) -> Path:
    """Write a stub ``uv`` that validates instantly and runs the real worker.

    Every ``uv``-based validation step exits zero, while ``uv run
    lubko-worker`` (used to spawn candidates and restored workers) executes the
    real worker from the current project. When ``bad_candidate_commit`` is
    given, the candidate commit fails to start so the handoff cannot complete.
    When ``fail_validation`` is given, every validation step fails so a queue
    checkout reports an error to its owner.

    Args:
        tmp_path: Temporary directory for the script.
        python: Python interpreter that runs the real worker.
        bad_candidate_commit: Exact commit whose worker must fail to start.
        fail_validation: Whether the validation steps must fail.

    Returns:
        The fake ``uv`` executable path.
    """
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(parents=True, exist_ok=True)
    script = bin_dir / "uv"
    lines = ["#!/bin/sh", 'if [ "$1" = "run" ] && [ "$2" = "lubko-worker" ]; then']
    if bad_candidate_commit is not None:
        lines.extend([
            f'    if [ "$(git -C . rev-parse HEAD)" = "{bad_candidate_commit}" ]; then',
            "        exit 7",
            "    fi",
        ])
    lines.extend([f"    exec {python} -m lubko.worker", "fi"])
    if fail_validation:
        lines.append("exit 9")
    else:
        lines.append("exit 0")
    script.write_text("\n".join(lines) + "\n", encoding="utf-8")
    script.chmod(0o755)
    return script


def worker_env_with(token: str) -> dict[str, str]:
    """Build the maintained-worker environment with fast timings.

    Args:
        token: Lifecycle token for the worker.

    Returns:
        The worker environment.
    """
    env = lifecycle.worker_env(token)
    env["LUBKO_WORKER_ID"] = "e2e-worker"
    env.update(WORKER_TIMINGS)
    return env


def spawn_maintained_worker(
    env: dict[str, str],
    argv: list[str] | None = None,
) -> subprocess.Popen[bytes]:
    """Spawn a real maintained worker process registered with the guard.

    Args:
        env: Worker environment.
        argv: Worker command, or the canonical ``lubko.worker`` invocation.

    Returns:
        The spawned worker process.
    """
    command = argv or [sys.executable, "-m", "lubko.worker"]
    proc = subprocess.Popen(
        command,
        cwd=REPO_ROOT,
        env=env,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        close_fds=True,
    )
    guard.register(proc)
    return proc


def worker_without_persist_command(tmp_path: Path) -> list[str]:
    """Return the argv of a worker whose ``process_pgid`` is never persisted.

    The worker runs through a wrapper that disables ``_persist_process``, so
    every claimed job keeps no ``process_pgid`` in PostgreSQL: queue detection
    can only work through the injected root job UUID, never the database.

    Args:
        tmp_path: Temporary directory for the wrapper script.

    Returns:
        The worker argv running through the no-persist wrapper.
    """
    wrapper = tmp_path / "worker_no_persist.py"
    wrapper.write_text(
        "from lubko import worker\n"
        "worker._persist_process = lambda _conn, _job_id, _pid, _pgid: None\n"
        "worker.main()\n",
        encoding="utf-8",
    )
    return [sys.executable, str(wrapper)]


def insert_pending_job(conninfo: str, cwd: str, command: str) -> object:
    """Insert a protocol v2 pending command job.

    Args:
        conninfo: PostgreSQL connection string.
        cwd: Working directory for the job.
        command: Shell command to run.

    Returns:
        The job identifier.
    """
    payload = json.dumps({
        "v": 2,
        "type": "command",
        "request": {"cwd": cwd, "command": command},
        "state": {"status": "pending"},
    })
    with psycopg.connect(conninfo) as conn:
        row = conn.execute(
            "INSERT INTO lubko.jobs (payload) VALUES (%s) RETURNING id",
            (payload,),
        ).fetchone()
    assert row is not None
    return row[0]


def insert_pending_args_job(conninfo: str, cwd: str, args: list[str]) -> object:
    """Insert a protocol v2 pending argv-style command job.

    Args:
        conninfo: PostgreSQL connection string.
        cwd: Working directory for the job.
        args: Executable arguments.

    Returns:
        The job identifier.
    """
    payload = json.dumps({
        "v": 2,
        "type": "command",
        "request": {"cwd": cwd, "args": args},
        "state": {"status": "pending"},
    })
    with psycopg.connect(conninfo) as conn:
        row = conn.execute(
            "INSERT INTO lubko.jobs (payload) VALUES (%s) RETURNING id",
            (payload,),
        ).fetchone()
    assert row is not None
    return row[0]


def read_job(conninfo: str, job_id: object) -> dict[str, Any]:
    """Read one job payload as a decoded mapping.

    Args:
        conninfo: PostgreSQL connection string.
        job_id: The job identifier.

    Returns:
        The decoded job payload.
    """
    with psycopg.connect(conninfo) as conn:
        row = conn.execute(
            "SELECT payload FROM lubko.jobs WHERE id = %s",
            (job_id,),
        ).fetchone()
    assert row is not None
    parsed = json.loads(str(row[0]))
    assert isinstance(parsed, dict)
    return parsed


def deployctl_args(repo: Path, fake_uv: Path, request: str) -> list[str]:
    """Build the argv of one supervised controller queue job.

    Args:
        repo: Deployment checkout.
        fake_uv: Stub ``uv`` executable.
        request: JSON request object.

    Returns:
        The controller argv.
    """
    return [
        sys.executable,
        "-m",
        "lubko.deployctl",
        request,
        "--repo",
        str(repo),
        "--uv",
        str(fake_uv),
        "--confirm-window-seconds",
        "120",
        "--grace-seconds",
        "1.0",
        "--db-timeout",
        "5",
        "--lock-timeout",
        "15",
        "--validation-timeout",
        "30",
        "--git-timeout",
        "10",
        "--cli-timeout",
        "60",
    ]


def kill_recorded_workers() -> None:
    """Force-kill every worker recorded in lifecycle metadata or rollback state.

    The candidate/restored workers are spawned by the controller subprocess and
    therefore never appear in the process guard registry, so they are stopped
    by exact recorded identity here. This is deliberately destructive and
    therefore fails closed: the recorded identities are only read from state
    the current test owns, never from the live user state tree.
    """
    isolation.assert_test_owned_state_root()
    recorded: list[WorkerMeta] = []
    meta = read_meta()
    if meta is not None:
        recorded.append(meta)
    state = dc._read_state()
    if state is not None:
        recorded.append(state.new_meta)
    for recorded_worker in recorded:
        pgid = recorded_worker.pgid or recorded_worker.pid
        if pgid is None:
            continue
        if group_has_members(pgid):
            with suppress(ProcessLookupError):
                os.killpg(pgid, signal.SIGKILL)
            deadline = time.monotonic() + 5.0
            while time.monotonic() < deadline and group_has_members(pgid):
                time.sleep(0.02)


def any_lubko_processes() -> bool:
    """Return whether any live controller/helper/worker process still exists.

    Matches only the module invocation forms used by the deployment flow
    (``lubko.deployctl`` for controller/helper/watchdog, ``lubko.worker`` for
    maintained workers) so the host orchestrator's own ``lubko-worker`` daemon
    is never mistaken for a leak.

    Returns:
        ``True`` when a stray controller/helper/worker process still exists.
    """
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            cmdline = (entry / "cmdline").read_bytes()
        except OSError:
            continue
        if b"lubko.deployctl" in cmdline or b"lubko.worker" in cmdline:
            return True
    return False


def assert_no_lubko_leaks() -> None:
    """Assert every controller/helper/worker process has fully exited."""
    wait_until(lambda: not any_lubko_processes(), timeout=20.0)


def _prepare_e2e(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    pg_cluster: _pg.PgCluster,
    *,
    bad_candidate: bool = False,
    worker_command: list[str] | None = None,
) -> tuple[Path, str, str, Path, subprocess.Popen[bytes], WorkerMeta, Path]:
    """Build the shared end-to-end deployment environment.

    Args:
        tmp_path: Temporary directory for the test.
        monkeypatch: Pytest monkeypatch fixture.
        pg_cluster: The running cluster.
        bad_candidate: Whether the candidate worker must fail to start.
        worker_command: Worker argv, or the canonical ``lubko.worker``.

    Returns:
        The repo, commits, database config, old worker process, old worker
        metadata, and fake ``uv``.
    """
    isolation.assert_test_owned_state_root()
    conf = write_database_config(tmp_path, pg_cluster)
    monkeypatch.setenv("LUBKO_DATABASE_CONFIG", str(conf))
    monkeypatch.setattr(cli, "_sync_venv", fake_uv_sync)
    repo, first, second = make_repo(tmp_path / "repo")
    cli.build_cli_root(repo, first, "uv", 60.0)
    cli.build_cli_root(repo, second, "uv", 60.0)
    token = secrets.token_hex(16)
    env = worker_env_with(token)
    old = spawn_maintained_worker(env, worker_command)
    old_meta = real_meta(old, repo, first, token)
    write_meta(old_meta)
    bad_commit = second if bad_candidate else None
    fake_uv = make_fake_uv(tmp_path, sys.executable, bad_candidate_commit=bad_commit)
    return repo, first, second, conf, old, old_meta, fake_uv


def _prepare_e2e_parts(
    prepared: tuple[Path, str, str, Path, subprocess.Popen[bytes], WorkerMeta, Path],
) -> tuple[Path, str, WorkerMeta, Path]:
    """Select the parts most end-to-end tests need from a prepared environment.

    Args:
        prepared: Result of :func:`_prepare_e2e`.

    Returns:
        The repo, candidate commit, old worker metadata, and fake ``uv``.
    """
    return prepared[0], prepared[2], prepared[5], prepared[6]


def _preparing_mission() -> bool:
    """Return whether a pending mission is being prepared (not yet retiring)."""
    state = dc._read_state()
    return state is not None and state.status == dc.STATUS_PENDING and not state.previous_retiring


def _live_handoff_done() -> bool:
    """Return whether the destructive handoff completed with a live deadline."""
    state = dc._read_state()
    return state is not None and state.previous_retiring and state.deadline > time.time()


def _rolled_back_state() -> bool:
    """Return whether the deployment reached terminal rolled_back."""
    state = dc._read_state()
    return state is not None and state.status == dc.STATUS_ROLLED_BACK


def _wait_for_checkout_success_and_old_death(
    jobs_db: str, checkout_id: object, old_meta: WorkerMeta
) -> None:
    """Wait for durable checkout success and prove the old worker dies after.

    The control job must reach durable ``succeeded`` before the old worker is
    stopped; this is the exit-143 regression. Returns once both are observed.

    Args:
        jobs_db: Connection string.
        checkout_id: The checkout queue job.
        old_meta: The old worker metadata.
    """
    dead_at: float | None = None
    succeeded_at: float | None = None
    deadline = time.monotonic() + 90.0
    while time.monotonic() < deadline:
        status = read_job(jobs_db, checkout_id)["state"]["status"]
        if succeeded_at is None and status == "succeeded":
            succeeded_at = time.monotonic()
        if dead_at is None and not worker_alive(old_meta):
            dead_at = time.monotonic()
        if succeeded_at is not None and dead_at is not None:
            break
        time.sleep(0.02)
    assert succeeded_at is not None, "checkout row never reached succeeded"
    assert dead_at is not None, "old worker never died"
    assert dead_at >= succeeded_at, "old worker died before durable checkout success"


def _run_confirmation_handshake(jobs_db: str, repo: Path, fake_uv: Path, commit: str) -> None:
    """Run the two-request confirmation handshake through the replacement worker.

    Args:
        jobs_db: Connection string.
        repo: Deployment checkout.
        fake_uv: Stub ``uv`` executable.
        commit: Exact proposed commit.
    """
    confirm1_id = insert_pending_args_job(
        jobs_db,
        str(repo),
        deployctl_args(repo, fake_uv, json.dumps({"type": "confirm", "commit": commit})),
    )
    wait_until(
        lambda: read_job(jobs_db, confirm1_id)["state"]["status"] == "succeeded",
        timeout=60.0,
    )
    confirm1 = json.loads(str(read_job(jobs_db, confirm1_id)["result"]["stdout"]))
    challenge = confirm1["challenge"]
    assert isinstance(challenge, str)
    assert CHALLENGE_RE.fullmatch(challenge) is not None
    assert len(challenge) == 7

    confirm2_id = insert_pending_args_job(
        jobs_db,
        str(repo),
        deployctl_args(
            repo,
            fake_uv,
            json.dumps({"type": "confirm", "commit": commit, "challenge": challenge[::-1]}),
        ),
    )
    wait_until(
        lambda: read_job(jobs_db, confirm2_id)["state"]["status"] == "succeeded",
        timeout=60.0,
    )
    confirm2 = json.loads(str(read_job(jobs_db, confirm2_id)["result"]["stdout"]))
    assert confirm2["confirmed"] is True


def test_end_to_end_queue_checkout_survives_old_worker_shutdown(
    jobs_db: str,
    pg_cluster: _pg.PgCluster,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A queue checkout completes even though the old worker shuts down.

    The control job exits zero and reaches durable ``succeeded`` before the old
    worker is stopped (the production exit-143 regression), the replacement
    worker consumes both confirmation jobs, and unrelated active jobs are
    terminated/reaped by the old worker's shutdown as before.
    """
    prepared = _prepare_e2e(tmp_path, monkeypatch, pg_cluster)
    repo, second, old_meta, fake_uv = _prepare_e2e_parts(prepared)
    try:
        unrelated = insert_pending_job(jobs_db, str(repo), "sleep 30")
        checkout_id = insert_pending_args_job(
            jobs_db,
            str(repo),
            deployctl_args(repo, fake_uv, json.dumps({"type": "checkout", "commit": second})),
        )
        wait_until(
            lambda: read_job(jobs_db, unrelated)["state"]["status"] == "running",
            timeout=30.0,
        )
        _wait_for_checkout_success_and_old_death(jobs_db, checkout_id, old_meta)

        checkout = read_job(jobs_db, checkout_id)
        assert checkout["state"]["status"] == "succeeded"
        result = checkout["result"]
        assert isinstance(result, dict)
        assert result["exit_code"] == 0
        response = json.loads(str(result["stdout"]))
        assert response["ok"] is True
        assert response["commit"] == second

        wait_until(
            lambda: read_job(jobs_db, unrelated)["state"]["status"] in {"cancelled", "failed"},
            timeout=30.0,
        )
        unrelated_payload = read_job(jobs_db, unrelated)
        assert unrelated_payload["state"]["status"] == "cancelled"
        pgid = unrelated_payload["state"].get("process_pgid")
        assert pgid is not None
        assert not group_has_members(int(str(pgid)))

        wait_until(_live_handoff_done, timeout=30.0)
        _run_confirmation_handshake(jobs_db, repo, fake_uv, second)

        final_state = dc._read_state()
        assert final_state is not None
        assert final_state.status == dc.STATUS_CONFIRMED
        meta = read_meta()
        assert meta is not None
        assert meta.git_commit == second
        assert cli.current_commit() == second
        assert not group_has_members(old_meta.pgid or old_meta.pid or 0)
    finally:
        kill_recorded_workers()
        assert_no_lubko_leaks()


def test_end_to_end_bad_candidate_rolls_back(
    jobs_db: str,
    pg_cluster: _pg.PgCluster,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A candidate that dies at release rolls the deployment back."""
    repo, first, second, _conf, _old, old_meta, fake_uv = _prepare_e2e(
        tmp_path, monkeypatch, pg_cluster, bad_candidate=True
    )
    try:
        checkout_id = insert_pending_args_job(
            jobs_db,
            str(repo),
            deployctl_args(repo, fake_uv, json.dumps({"type": "checkout", "commit": second})),
        )
        wait_until(
            lambda: read_job(jobs_db, checkout_id)["state"]["status"] == "succeeded",
            timeout=60.0,
        )
        wait_until(_rolled_back_state, timeout=60.0)

        rolled = dc._read_state()
        assert rolled is not None
        assert rolled.status == dc.STATUS_ROLLED_BACK
        assert cli.git_commit(repo, 10.0) == first
        meta = read_meta()
        assert meta is not None
        assert meta.git_commit == first
        assert worker_alive(meta)
        assert not worker_alive(old_meta)
        assert not group_has_members(old_meta.pgid or old_meta.pid or 0)
    finally:
        kill_recorded_workers()
        assert_no_lubko_leaks()


def test_end_to_end_cancelled_checkout_leaves_previous_worker_running(
    jobs_db: str,
    pg_cluster: _pg.PgCluster,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A cancelled initiating row aborts without ever stopping the old worker."""
    repo, first, second, _conf, _old, old_meta, fake_uv = _prepare_e2e(
        tmp_path, monkeypatch, pg_cluster
    )
    try:
        checkout_id = insert_pending_args_job(
            jobs_db,
            str(repo),
            deployctl_args(repo, fake_uv, json.dumps({"type": "checkout", "commit": second})),
        )
        wait_until(_preparing_mission, timeout=60.0)
        with psycopg.connect(jobs_db) as conn:
            conn.execute(
                "UPDATE lubko.jobs\n"
                "SET payload = jsonb_set("
                "payload::jsonb, "
                "'{state,cancel_requested_at}', "
                "to_jsonb('2026-01-01T00:00:00.000000Z'::text))::text\n"
                "WHERE id = %s",
                (checkout_id,),
            )

        wait_until(
            lambda: read_job(jobs_db, checkout_id)["state"]["status"] == "cancelled",
            timeout=30.0,
        )
        wait_until(_rolled_back_state, timeout=60.0)

        rolled = dc._read_state()
        assert rolled is not None
        assert rolled.status == dc.STATUS_ROLLED_BACK
        assert cli.git_commit(repo, 10.0) == first
        assert worker_alive(old_meta)
        meta = read_meta()
        assert meta is not None
        assert meta.git_commit == first
        assert meta.pid == old_meta.pid
        assert not group_has_members(rolled.new_meta.pgid or rolled.new_meta.pid or 0)
    finally:
        kill_recorded_workers()
        assert_no_lubko_leaks()


def test_end_to_end_queue_checkout_withholds_process_pgid(
    jobs_db: str,
    pg_cluster: _pg.PgCluster,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Queue detection never depends on process_pgid persistence timing.

    The owning worker disables ``_persist_process`` entirely, so no checkout
    row ever carries a ``process_pgid``; the control job must still recognize
    its own queue row through the exact injected ``LUBKO_JOB_ID`` (the BLOCKER
    Q8 race). The row reaches durable ``succeeded`` with exit code zero before
    the old worker dies and the replacement worker confirms both confirmation
    jobs, exactly like the production queue handoff.
    """
    repo, second, old_meta, fake_uv = _prepare_e2e_parts(
        _prepare_e2e(
            tmp_path,
            monkeypatch,
            pg_cluster,
            worker_command=worker_without_persist_command(tmp_path),
        )
    )
    try:
        checkout_id = insert_pending_args_job(
            jobs_db,
            str(repo),
            deployctl_args(repo, fake_uv, json.dumps({"type": "checkout", "commit": second})),
        )
        _wait_for_checkout_success_and_old_death(jobs_db, checkout_id, old_meta)

        checkout = read_job(jobs_db, checkout_id)
        assert checkout["state"]["status"] == "succeeded"
        assert "process_pgid" not in (checkout["state"] or {})
        result = checkout["result"]
        assert isinstance(result, dict)
        assert result["exit_code"] == 0
        response = json.loads(str(result["stdout"]))
        assert response["ok"] is True
        assert response["commit"] == second

        wait_until(_live_handoff_done, timeout=30.0)
        _run_confirmation_handshake(jobs_db, repo, fake_uv, second)

        final_state = dc._read_state()
        assert final_state is not None
        assert final_state.status == dc.STATUS_CONFIRMED
        meta = read_meta()
        assert meta is not None
        assert meta.git_commit == second
        assert cli.current_commit() == second
        assert not group_has_members(old_meta.pgid or old_meta.pid or 0)
    finally:
        kill_recorded_workers()
        assert_no_lubko_leaks()


def test_end_to_end_queue_checkout_error_leaves_failed_row(
    jobs_db: str,
    pg_cluster: _pg.PgCluster,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed queue checkout is durably failed, never falsely succeeded.

    Candidate validation fails, so the handoff helper reports an error, the
    controller parent exits non-zero, and the owning worker records the row as
    ``failed``: a helper-death/error outcome can never leave a successful row.
    The previous worker stays running and the previous checkout is restored.
    """
    repo, first, second, _conf, _old, old_meta, _uv = _prepare_e2e(
        tmp_path, monkeypatch, pg_cluster
    )
    fake_uv = make_fake_uv(tmp_path, sys.executable, fail_validation=True)
    try:
        checkout_id = insert_pending_args_job(
            jobs_db,
            str(repo),
            deployctl_args(repo, fake_uv, json.dumps({"type": "checkout", "commit": second})),
        )
        wait_until(
            lambda: read_job(jobs_db, checkout_id)["state"]["status"] == "failed",
            timeout=60.0,
        )

        checkout = read_job(jobs_db, checkout_id)
        result = checkout["result"]
        assert isinstance(result, dict)
        assert result["exit_code"] != 0
        response = json.loads(str(result["stdout"]))
        assert response["ok"] is False
        assert cli.git_commit(repo, 10.0) == first
        assert worker_alive(old_meta)
        meta = read_meta()
        assert meta is not None
        assert meta.git_commit == first
        assert meta.pid == old_meta.pid
        state = dc._read_state()
        assert state is None or state.status in {dc.STATUS_CONFIRMED, dc.STATUS_ROLLED_BACK}
    finally:
        kill_recorded_workers()
        assert_no_lubko_leaks()


def read_proc_environ(pid: int) -> dict[str, str]:
    """Read the exact environment of a live process from ``/proc``.

    Args:
        pid: Process to inspect.

    Returns:
        The process environment as a mapping.
    """
    raw = (Path("/proc") / str(pid) / "environ").read_bytes()
    entries = (entry for entry in raw.split(b"\0") if b"=" in entry)
    return {
        str(entry.split(b"=", 1)[0], "utf-8"): str(entry.split(b"=", 1)[1], "utf-8")
        for entry in entries
    }


def test_end_to_end_full_deployment_stays_hermetic(
    jobs_db: str,
    pg_cluster: _pg.PgCluster,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A full supervised deployment never escapes the test-owned state root.

    Runs the real queue-driven deployment and confirmation handshake while a
    live sentinel worker with ambient production-like state remains present.
    The maintained worker subprocess must observe the same isolated
    ``XDG_STATE_HOME`` as the test process, every lifecycle file the
    deployment creates must land under the test root, the ambient tree must
    stay byte-for-byte unchanged, and the sentinel process must never be
    signalled.
    """
    isolation.assert_test_owned_state_root()
    prepared = _prepare_e2e(tmp_path, monkeypatch, pg_cluster)
    repo, second, old_meta, fake_uv = _prepare_e2e_parts(prepared)
    ambient_before = isolation.snapshot_tree(isolation.ambient_state_root())
    expected_state_home = os.environ["XDG_STATE_HOME"]
    assert old_meta.pid is not None
    worker_environ = read_proc_environ(old_meta.pid)
    assert worker_environ.get("XDG_STATE_HOME") == expected_state_home
    try:
        checkout_id = insert_pending_args_job(
            jobs_db,
            str(repo),
            deployctl_args(repo, fake_uv, json.dumps({"type": "checkout", "commit": second})),
        )
        _wait_for_checkout_success_and_old_death(jobs_db, checkout_id, old_meta)
        wait_until(_live_handoff_done, timeout=30.0)
        _run_confirmation_handshake(jobs_db, repo, fake_uv, second)

        final_state = dc._read_state()
        assert final_state is not None
        assert final_state.status == dc.STATUS_CONFIRMED
        meta = read_meta()
        assert meta is not None
        assert meta.git_commit == second
        assert cli.current_commit() == second
    finally:
        kill_recorded_workers()
        assert_no_lubko_leaks()

    assert isolation.snapshot_tree(isolation.ambient_state_root()) == ambient_before
    assert isolation.ambient_sentinel_alive()
    test_tmp = isolation.CURRENT_TEST_TMP
    assert test_tmp is not None
    assert state_root().is_relative_to(test_tmp)
    resolved_home = Path(os.environ["XDG_STATE_HOME"]).resolve()
    assert resolved_home.is_relative_to(test_tmp)
