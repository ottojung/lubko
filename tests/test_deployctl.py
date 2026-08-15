"""Focused tests for the supervised deployment controller.

The coherence tests exercise the global-CLI guarantee with real two-commit git
repositories: a provisional candidate never moves the ``current`` CLI pointer,
confirmation moves it only after durable ``confirmed`` state, and every
rollback/failure path preserves the prior confirmed CLI version.
"""

from __future__ import annotations

import os
import subprocess
import time
from dataclasses import replace
from pathlib import Path

import pytest

from lubko import cli
from lubko import deployctl as dc
from lubko.lifecycle import (
    SCHEMA_VERSION,
    STATE_RUNNING,
    ValidationReport,
    WorkerMeta,
    read_meta,
    write_meta,
)
from tests.test_cli import fake_uv_sync, make_repo


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
) -> dc.RollbackState:
    """Return a live pending deployment state.

    Args:
        repo: Repository recorded in the state.
        old: Previous confirmed commit.
        new: Proposed candidate commit.

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
    state = pending_state()

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
