"""Focused tests for the supervised deployment controller.

The coherence tests exercise the global-CLI guarantee with real two-commit git
repositories: a provisional candidate never moves the ``current`` CLI pointer,
confirmation moves it only after durable ``confirmed`` state, and every
rollback/failure path preserves the prior confirmed CLI version.
"""

from __future__ import annotations

import os
import shutil
import signal
import subprocess
import sys
import time
from contextlib import suppress
from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING, Final

import pytest

from lubko import cli, lifecycle
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
from tests import _process_guard as guard
from tests.test_cli import fake_uv_sync, make_repo

if TYPE_CHECKING:
    from lubko.lifecycle import ProcessIdentity

SLEEP_BIN: Final = shutil.which("sleep") or "/bin/sleep"
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
) -> dc.RollbackState:
    """Return a live pending deployment state.

    Args:
        repo: Repository recorded in the state.
        old: Previous confirmed commit.
        new: Proposed candidate commit.
        previous_retiring: Whether the previous worker's retirement has begun.

    Returns:
        A pending rollback state with distinct old/new commits.
    """
    return dc.RollbackState(
        schema_version=dc.ROLLBACK_SCHEMA_VERSION,
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


def test_first_confirmation_persists_only_challenge_digest(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The first confirmation returns a challenge but stores only its digest."""
    state = pending_state()
    options = make_options(tmp_path / "repo")
    written: list[dc.RollbackState] = []
    monkeypatch.setattr(dc, "_read_state", lambda: state)
    monkeypatch.setattr(dc, "worker_alive", lambda _meta: True)
    monkeypatch.setattr(dc, "_write_state", written.append)

    response = dc._confirm_locked({"type": "confirm", "commit": state.commit}, options)

    challenge = response["challenge"]
    assert isinstance(challenge, str)
    assert challenge
    assert written[-1].challenge_hash == dc._challenge_digest(challenge)
    assert challenge not in written[-1].to_dict().values()


def test_second_confirmation_writes_meta_before_terminal_state(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Successful confirmation records candidate metadata before terminal state."""
    state = pending_state()
    options = make_options(tmp_path / "repo")
    challenge = "challenge-value"
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
    challenged = replace(state, challenge_hash=dc._challenge_digest("expected"))
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
                "challenge": "wrong",
            },
            options,
        )

    assert rollbacks == [challenged]


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
    challenged = replace(state, challenge_hash=dc._challenge_digest("expected"))
    dc._write_state(challenged)
    monkeypatch.setattr(dc, "worker_alive", lambda _meta: True)
    options = make_options(repo)
    patch_rollback_dependencies(monkeypatch)

    with pytest.raises(dc.DeployCtlError, match="incorrect"):
        dc._confirm_locked(
            {
                "type": "confirm",
                "commit": second,
                "challenge": "wrong-answer",
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
    challenged = replace(state, challenge_hash=dc._challenge_digest("challenge"))
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
            "challenge": "challenge"[::-1],
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
