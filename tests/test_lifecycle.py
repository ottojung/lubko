"""Tests for Lubko worker lifecycle management."""

import argparse
import json
import os
import shutil
import signal
import subprocess
import sys
import time
from collections.abc import Callable, Iterator
from contextlib import suppress
from dataclasses import replace
from pathlib import Path
from typing import Final
from unittest import mock
from uuid import uuid4

import psycopg
import pytest
from psycopg.rows import tuple_row

from lubko import cli, lifecycle, supervise, toolchain
from lubko import deployctl as dc
from lubko.config import DatabaseConfig, load_database_config
from lubko.lifecycle import (
    EXIT_ERROR,
    EXIT_OK,
    ProcessIdentity,
    ValidationReport,
    WorkerMeta,
)
from lubko.state import rollback_state_path
from lubko.worker import JOB_ID_ENV, delete_job_and_chunks, request_cancel
from tests import _pg
from tests import _process_guard as guard
from tests.test_cli import fake_uv_sync, make_repo

MARKER: Final = "test-marker"
STALE_MARKER: Final = "stale"
OTHER_MARKER: Final = "other-marker"
SHORT_MARKER: Final = "tok"
GIT_SHA: Final = "a" * 40
GIT_SHA_LENGTH: Final = 40
SLEEP_BIN: Final = shutil.which("sleep") or "/bin/sleep"
REPO_ROOT: Final = Path(__file__).resolve().parent.parent
BASELINE_MIGRATION: Final = REPO_ROOT / "migrations" / "0001_two_column_protocol.sql"
TEST_PASSWORD: Final = "secret-value"  # ruff: ignore[hardcoded-password-string] - test credential


def spawn_controlled(marker: str = MARKER) -> subprocess.Popen[bytes]:
    """Spawn a controlled long-lived session-leader process.

    The process is registered with the shared process guard so teardown owns
    and deterministically stops it even if an assertion fails mid-test.

    Args:
        marker: Lifecycle token to place in the process environment.

    Returns:
        The spawned process.
    """
    env = dict(os.environ)
    env[lifecycle.LIFECYCLE_MARKER_VAR] = marker
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


def identity_of(proc: subprocess.Popen[bytes]) -> ProcessIdentity:
    """Wait for a spawned process to establish its own session.

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


def meta_for_process(proc: subprocess.Popen[bytes], repo: Path) -> WorkerMeta:
    """Build running metadata for a controlled process.

    Args:
        proc: The controlled process.
        repo: Repository path to record.

    Returns:
        Running metadata for the process.
    """
    identity = identity_of(proc)
    return WorkerMeta(
        schema_version=1,
        state=lifecycle.STATE_RUNNING,
        pid=identity.pid,
        pgid=identity.pgid,
        sid=identity.sid,
        start_time_ticks=identity.start_time_ticks,
        token=MARKER,
        repo=str(repo),
        git_commit=GIT_SHA,
        worker_id="test-worker",
        log_path=str(lifecycle.worker_log_path()),
        started_at=time.time(),
        stopped_at=None,
    )


def kill_proc(proc: subprocess.Popen[bytes]) -> None:
    """Force-kill a controlled process and reap it.

    Args:
        proc: The controlled process.
    """
    if proc.poll() is None:
        with suppress(ProcessLookupError):
            os.killpg(proc.pid, signal.SIGKILL)
    proc.wait(timeout=5)
    guard.unregister(proc)


def _run_lifecycle_launcher(path: Path) -> str:
    """Run a launcher script and return its trimmed stdout.

    Args:
        path: Launcher path.

    Returns:
        The launcher's standard output.
    """
    proc = subprocess.run([str(path)], capture_output=True, text=True, check=True)
    return proc.stdout.strip()


def kill_many(procs: list[subprocess.Popen[bytes]]) -> None:
    """Force-kill a list of controlled processes.

    Args:
        procs: The controlled processes.
    """
    for proc in procs:
        kill_proc(proc)


def wait_until(predicate: Callable[[], bool], timeout: float = 10.0) -> None:
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
        time.sleep(0.01)
    msg = "condition not met within timeout"
    raise AssertionError(msg)


def make_options(repo: Path, *, bootstrap: bool) -> lifecycle.DeployOptions:
    """Build deployment options for tests.

    ``direct_spawn`` is enabled explicitly: these unit tests exercise the
    narrow legacy direct-spawn mechanism that a normal maintained install no
    longer falls back to silently.

    Args:
        repo: Repository path to deploy.
        bootstrap: Whether to allow the unmanaged bootstrap case.

    Returns:
        Deployment options.
    """
    return lifecycle.DeployOptions(
        repo=repo,
        uv_path="uv",
        bootstrap=bootstrap,
        direct_spawn=True,
        stop_grace_seconds=0.5,
        postgres_timeout_seconds=1.0,
        lock_timeout_seconds=1.0,
        validation_timeout_seconds=5.0,
        git_timeout_seconds=5.0,
        cli_timeout_seconds=5.0,
    )


def patch_deploy(
    monkeypatch: pytest.MonkeyPatch,
    *,
    validation_ok: bool = True,
    postgres_ok: bool = True,
) -> list[subprocess.Popen[bytes]]:
    """Patch deploy dependencies and return spawned worker handles.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
        validation_ok: Whether validation should pass.
        postgres_ok: Whether the PostgreSQL check should pass.

    Returns:
        The worker processes spawned during deployment.
    """

    def fake_validation(
        _repo: Path,
        _uv: str,
        _timeout: float,
    ) -> ValidationReport:
        return ValidationReport(ok=validation_ok, detail="boom")

    def fake_postgres(_timeout: float) -> bool:
        return postgres_ok

    def fake_commit(_repo: Path, _timeout: float) -> str:
        return GIT_SHA

    monkeypatch.setattr(lifecycle, "run_validation", fake_validation)
    monkeypatch.setattr(lifecycle, "check_postgres", fake_postgres)
    monkeypatch.setattr(lifecycle, "git_commit", fake_commit)
    monkeypatch.setattr(lifecycle, "require_clean_checkout", lambda _repo, _timeout: True)

    def fake_cli_build(
        _repo: Path,
        commit: str,
        _uv: str,
        _timeout: float,
    ) -> Path:
        cli.cli_commit_dir(commit).mkdir(parents=True, exist_ok=True)
        return cli.cli_commit_dir(commit)

    monkeypatch.setattr(cli, "build_cli_root", fake_cli_build)

    spawned: list[subprocess.Popen[bytes]] = []

    def fake_worker_command(_uv_path: str) -> list[str]:
        return [SLEEP_BIN, "300"]

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

    monkeypatch.setattr(lifecycle, "_worker_command", fake_worker_command)
    monkeypatch.setattr(lifecycle, "spawn_worker", tracking_spawn)
    return spawned


def write_fake_uv(directory: Path, *, fail_on: str | None = None) -> str:
    """Write a controllable fake ``uv`` executable.

    Args:
        directory: Directory to write the script into.
        fail_on: When present, any invocation whose joined args contain this
            substring exits with code 7.

    Returns:
        The path of the fake ``uv`` executable.
    """
    lines = ["#!/bin/sh"]
    if fail_on is None:
        lines.append("exit 0")
    else:
        lines.extend((
            'case "$*" in',
            f"  *{fail_on}*) exit 7 ;;",
            "  *) exit 0 ;;",
            "esac",
        ))
    script = directory / "fake-uv"
    script.write_text("\n".join(lines) + "\n")
    script.chmod(0o755)
    return str(script)


def write_uv_executable(directory: Path, *, name: str = "uv") -> str:
    """Write a trivial executable named for resolution tests.

    Args:
        directory: Directory to write the script into.
        name: Executable file name.

    Returns:
        The absolute path of the executable.
    """
    script = directory / name
    script.write_text("#!/bin/sh\nexit 0\n")
    script.chmod(0o755)
    return str(script)


def make_deploy_args(tmp_path: Path, *, uv: str | None) -> argparse.Namespace:
    """Build CLI deploy arguments for resolution tests.

    Args:
        tmp_path: Repository path to record.
        uv: The ``--uv`` value.

    Returns:
        A namespace matching the deploy subparser.
    """
    return argparse.Namespace(
        repo=tmp_path,
        uv=uv,
        bootstrap=False,
        grace_seconds=0.5,
        db_timeout=1.0,
        lock_timeout=1.0,
        validation_timeout=5.0,
        git_timeout=5.0,
        cli_timeout=5.0,
    )


def capture_deploy_uv(
    monkeypatch: pytest.MonkeyPatch,
) -> list[str]:
    """Replace lifecycle.deploy with a resolver capture.

    Args:
        monkeypatch: Pytest monkeypatch fixture.

    Returns:
        A list filled with the uv path passed to deploy.
    """
    captured: list[str] = []

    def recording_deploy(options: lifecycle.DeployOptions) -> int:
        captured.append(options.uv_path)
        return EXIT_OK

    monkeypatch.setattr(lifecycle, "deploy", recording_deploy)
    return captured


@pytest.fixture(autouse=True)
def state_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point the lifecycle state directory at a temporary location.

    Returns:
        The temporary lifecycle state directory.
    """
    state = tmp_path / "state"
    monkeypatch.setenv("XDG_STATE_HOME", str(state))
    return state


def test_status_reports_unmanaged(capsys: pytest.CaptureFixture[str]) -> None:
    """With no metadata, status reports the unmanaged bootstrap case."""
    assert lifecycle.status_cmd() == EXIT_OK
    out = capsys.readouterr().out
    assert "state: unmanaged" in out
    assert "legacy" in out


def test_status_reports_running_worker(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """With live matching metadata, status reports the running worker."""
    proc = spawn_controlled()
    try:
        lifecycle.write_meta(meta_for_process(proc, tmp_path))
        assert lifecycle.status_cmd() == EXIT_OK
        out = capsys.readouterr().out
        assert "state: running" in out
        assert str(proc.pid) in out
    finally:
        kill_proc(proc)


def test_deploy_unmanaged_refuses_without_bootstrap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Deploying over an unmanaged worker is refused unless acknowledged."""
    spawned = patch_deploy(monkeypatch)
    try:
        code = lifecycle.deploy(make_options(tmp_path, bootstrap=False))
        assert code == EXIT_ERROR
        assert not spawned
        assert lifecycle.read_meta() is None
        assert "unmanaged" in capsys.readouterr().err
    finally:
        kill_many(spawned)


def test_deploy_unmanaged_with_bootstrap_starts_worker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With the bootstrap flag, the first maintained worker is started."""
    spawned = patch_deploy(monkeypatch)
    try:
        code = lifecycle.deploy(make_options(tmp_path, bootstrap=True))
        assert code == EXIT_OK
        assert len(spawned) == 1
        meta = lifecycle.read_meta()
        assert meta is not None
        assert meta.state == lifecycle.STATE_RUNNING
        assert meta.pid == spawned[0].pid
    finally:
        kill_many(spawned)


def test_deploy_failed_validation_preserves_worker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failing validation leaves the previous worker untouched."""
    old = spawn_controlled()
    try:
        lifecycle.write_meta(meta_for_process(old, tmp_path))
        spawned = patch_deploy(monkeypatch, validation_ok=False)
        code = lifecycle.deploy(make_options(tmp_path, bootstrap=False))
        assert code == EXIT_ERROR
        assert not spawned
        assert old.poll() is None
        current = lifecycle.read_meta()
        assert current is not None
        assert current.pid == old.pid
    finally:
        kill_proc(old)


def test_deploy_validates_before_replacing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Validation runs before any worker process is spawned."""
    old = spawn_controlled()
    spawned: list[subprocess.Popen[bytes]] = []
    try:
        lifecycle.write_meta(meta_for_process(old, tmp_path))
        order: list[str] = []

        def recording_validation(
            _repo: Path,
            _uv: str,
            _timeout: float,
        ) -> ValidationReport:
            order.append("validation")
            return ValidationReport(ok=True, detail="")

        monkeypatch.setattr(lifecycle, "run_validation", recording_validation)
        monkeypatch.setattr(lifecycle, "check_postgres", lambda _timeout: True)
        monkeypatch.setattr(lifecycle, "git_commit", lambda _repo, _timeout: GIT_SHA)
        monkeypatch.setattr(lifecycle, "require_clean_checkout", lambda _repo, _timeout: True)
        monkeypatch.setattr(cli, "build_cli_root", lambda *_args, **_kwargs: Path())
        original_spawn = lifecycle.spawn_worker

        def recording_spawn(
            repo: Path,
            uv_path: str,
            log_path: Path,
            env: dict[str, str],
        ) -> subprocess.Popen[bytes]:
            order.append("spawn")
            proc = original_spawn(repo, uv_path, log_path, env)
            guard.register(proc)
            spawned.append(proc)
            return proc

        monkeypatch.setattr(lifecycle, "_worker_command", lambda _uv: [SLEEP_BIN, "300"])
        monkeypatch.setattr(lifecycle, "spawn_worker", recording_spawn)

        code = lifecycle.deploy(make_options(tmp_path, bootstrap=False))
        assert code == EXIT_OK
        assert order == ["validation", "spawn"]
    finally:
        kill_proc(old)
        kill_many(spawned)


def test_deploy_refuses_dirty_checkout(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A dirty checkout is refused so the worker and CLIs run the same commit."""
    repo, _first, _second = make_repo(tmp_path / "repo")
    (repo / "marker.txt").write_text("uncommitted change\n", encoding="utf-8")
    real_require_clean = lifecycle.require_clean_checkout
    spawned = patch_deploy(monkeypatch)
    monkeypatch.setattr(lifecycle, "require_clean_checkout", real_require_clean)
    try:
        code = lifecycle.deploy(make_options(repo, bootstrap=True))
        assert code == EXIT_ERROR
        assert not spawned
        assert lifecycle.read_meta() is None
        assert "dirty" in capsys.readouterr().err
    finally:
        kill_many(spawned)


def test_bootstrap_deploy_activates_coherent_cli(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A bootstrap deploy leaves the maintained CLIs coherent with the worker."""
    repo, _first, second = make_repo(tmp_path / "repo")
    monkeypatch.setattr(
        lifecycle,
        "run_validation",
        lambda _repo, _uv, _timeout: ValidationReport(ok=True, detail=""),
    )
    monkeypatch.setattr(lifecycle, "check_postgres", lambda _timeout: True)
    monkeypatch.setattr(lifecycle, "require_clean_checkout", lambda _repo, _timeout: True)
    monkeypatch.setattr(lifecycle, "git_commit", lambda _repo, _timeout: second)
    monkeypatch.setattr(cli, "_sync_venv", fake_uv_sync)
    spawned: list[subprocess.Popen[bytes]] = []
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

    monkeypatch.setattr(lifecycle, "_worker_command", lambda _uv: [SLEEP_BIN, "300"])
    monkeypatch.setattr(lifecycle, "spawn_worker", tracking_spawn)
    try:
        code = lifecycle.deploy(make_options(repo, bootstrap=True))
        assert code == EXIT_OK
        assert len(spawned) == 1
        meta = lifecycle.read_meta()
        assert meta is not None
        assert meta.git_commit == second
        assert cli.current_commit() == second
        assert cli.cli_entry_executable(second, "lubko-deploy-ctl") is not None
    finally:
        kill_many(spawned)


def test_deploy_activation_failure_preserves_coherent_cli(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A failed CLI switch leaves the prior CLI usable and is not silent."""
    repo, first, second = make_repo(tmp_path / "repo")
    monkeypatch.setattr(cli, "_sync_venv", fake_uv_sync)
    bin_dir = tmp_path / "bin"
    cli.build_cli_root(repo, first, "uv", 60.0)
    cli.install_launchers(bin_dir)
    cli.set_current(first)

    monkeypatch.setattr(lifecycle, "require_clean_checkout", lambda _repo, _timeout: True)
    monkeypatch.setattr(
        lifecycle,
        "run_validation",
        lambda _repo, _uv, _timeout: ValidationReport(ok=True, detail=""),
    )
    monkeypatch.setattr(lifecycle, "check_postgres", lambda _timeout: True)
    monkeypatch.setattr(lifecycle, "git_commit", lambda _repo, _timeout: second)
    spawned: list[subprocess.Popen[bytes]] = []
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

    monkeypatch.setattr(lifecycle, "_worker_command", lambda _uv: [SLEEP_BIN, "300"])
    monkeypatch.setattr(lifecycle, "spawn_worker", tracking_spawn)

    def broken_set_current(_commit: str) -> None:
        msg = "switch boom"
        raise cli.CliError(msg)

    monkeypatch.setattr(cli, "set_current", broken_set_current)
    try:
        code = lifecycle.deploy(make_options(repo, bootstrap=True))
        assert code == EXIT_ERROR
        meta = lifecycle.read_meta()
        assert meta is not None
        assert meta.git_commit == second
        assert cli.current_commit() == first
        assert cli.cli_commit_dir(first).is_dir()
        assert _run_lifecycle_launcher(bin_dir / "lubko-agent") == f"lubko-agent@{first}"
        err = capsys.readouterr().err
        assert "activation failed" in err
        assert "previous CLI commit remains active" in err
    finally:
        kill_many(spawned)


def test_deploy_verification_failure_preserves_worker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A replacement that fails verification is rolled back."""
    old = spawn_controlled()
    try:
        lifecycle.write_meta(meta_for_process(old, tmp_path))
        spawned = patch_deploy(monkeypatch, postgres_ok=False)
        code = lifecycle.deploy(make_options(tmp_path, bootstrap=False))
        assert code == EXIT_ERROR
        assert len(spawned) == 1
        assert spawned[0].poll() is not None
        assert old.poll() is None
        current = lifecycle.read_meta()
        assert current is not None
        assert current.pid == old.pid
    finally:
        kill_proc(old)
        kill_many(spawned)


def test_deploy_success_replaces_worker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A successful deploy records metadata and reports the git commit."""
    old = spawn_controlled()
    try:
        lifecycle.write_meta(meta_for_process(old, tmp_path))
        spawned = patch_deploy(monkeypatch)
        code = lifecycle.deploy(make_options(tmp_path, bootstrap=False))
        assert code == EXIT_OK
        assert len(spawned) == 1
        wait_until(lambda: old.poll() is not None)
        assert spawned[0].poll() is None
        meta = lifecycle.read_meta()
        assert meta is not None
        assert meta.state == lifecycle.STATE_RUNNING
        assert meta.pid == spawned[0].pid
        assert meta.git_commit == GIT_SHA
        assert meta.log_path
        log_p = Path(meta.log_path)
        assert log_p.parent.name == "logs"
        assert log_p.name.startswith("worker-")
        assert log_p.name.endswith(".log")
        out = capsys.readouterr().out
        assert "deployed git commit" in out
        assert GIT_SHA in out
    finally:
        kill_proc(old)
        kill_many(spawned)


def test_deploy_refuses_without_supervisor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A normal deploy without the external supervisor refuses loudly.

    The external supervisor is the single authority that owns the maintained
    worker; a maintained install must never silently fall back to direct
    spawning merely because the supervisor is absent.
    """
    spawned = patch_deploy(monkeypatch)
    lifecycle.write_meta(
        WorkerMeta(
            schema_version=1,
            state=lifecycle.STATE_STOPPED,
            pid=999_999,
            pgid=999_999,
            sid=999_999,
            start_time_ticks=1,
            token=STALE_MARKER,
            repo=str(tmp_path),
            git_commit=GIT_SHA,
            worker_id="old",
            log_path=str(tmp_path / "old.log"),
            started_at=1.0,
            stopped_at=1.0,
        )
    )
    options = lifecycle.DeployOptions(
        repo=tmp_path,
        uv_path="uv",
        bootstrap=False,
        direct_spawn=False,
        stop_grace_seconds=0.5,
        postgres_timeout_seconds=1.0,
        lock_timeout_seconds=1.0,
        validation_timeout_seconds=5.0,
        git_timeout_seconds=5.0,
        cli_timeout_seconds=5.0,
    )
    try:
        code = lifecycle.deploy(options)
        assert code == EXIT_ERROR
        assert not spawned
        current = lifecycle.read_meta()
        assert current is not None
        assert current.state == lifecycle.STATE_STOPPED
        assert current.pid == 999_999
        err = capsys.readouterr().err
        assert "external supervisor is running" in err
    finally:
        kill_many(spawned)


def test_deploy_replaces_stale_worker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Stale metadata pointing at a dead worker is replaced cleanly."""
    stale = WorkerMeta(
        schema_version=1,
        state=lifecycle.STATE_RUNNING,
        pid=999_999,
        pgid=999_999,
        sid=999_999,
        start_time_ticks=1,
        token=STALE_MARKER,
        repo=str(tmp_path),
        git_commit="old",
        worker_id="old",
        log_path=str(tmp_path / "old.log"),
        started_at=1.0,
        stopped_at=None,
    )
    lifecycle.write_meta(stale)
    spawned = patch_deploy(monkeypatch)
    try:
        code = lifecycle.deploy(make_options(tmp_path, bootstrap=False))
        assert code == EXIT_OK
        assert len(spawned) == 1
        meta = lifecycle.read_meta()
        assert meta is not None
        assert meta.pid == spawned[0].pid
    finally:
        kill_many(spawned)


# ---------------------------------------------------------------------------
# Queue-invoked deploy (#68)
# ---------------------------------------------------------------------------


def dead_worker_meta(commit: str, repo: Path) -> WorkerMeta:
    """Build dead (non-live) maintained-worker metadata.

    Args:
        commit: Exact commit the metadata claims.
        repo: Repository path recorded in the metadata.

    Returns:
        Metadata whose process is never alive.
    """
    return WorkerMeta(
        schema_version=1,
        state=lifecycle.STATE_STOPPED,
        pid=999_999,
        pgid=999_999,
        sid=999_999,
        start_time_ticks=1,
        token=STALE_MARKER,
        repo=str(repo),
        git_commit=commit,
        worker_id="old",
        log_path="",
        started_at=1.0,
        stopped_at=1.0,
    )


def test_deploy_queue_owned_routes_to_detached_helper(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A deploy running inside a Lubko job routes to the detached helper."""
    job_id = uuid4()
    captured: list[object] = []
    monkeypatch.setattr(lifecycle, "_current_queue_job", lambda: (job_id, False))

    def fake_queue(_options: lifecycle.DeployOptions, received: object) -> int:
        captured.append(received)
        return EXIT_OK

    monkeypatch.setattr(lifecycle, "_queue_deploy", fake_queue)
    assert lifecycle.deploy(make_options(tmp_path, bootstrap=False)) == EXIT_OK
    assert captured == [job_id]


def test_deploy_queue_owned_cancelled_refuses(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A cancelled deploy queue owner refuses before any helper work."""
    monkeypatch.setattr(lifecycle, "_current_queue_job", lambda: (uuid4(), True))
    assert lifecycle.deploy(make_options(tmp_path, bootstrap=False)) == EXIT_ERROR
    assert "cancelled" in capsys.readouterr().err


def test_deploy_queue_owned_helper_error_returns_failed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A queue-deploy helper error fails the deploy, never the manual path."""
    monkeypatch.setattr(lifecycle, "_current_queue_job", lambda: (uuid4(), False))

    def failing_queue(_options: lifecycle.DeployOptions, _job_id: object) -> int:
        msg = "deployment handoff helper exited before reporting an outcome"
        raise lifecycle.DeployAbortedError(msg)

    monkeypatch.setattr(lifecycle, "_queue_deploy", failing_queue)
    assert lifecycle.deploy(make_options(tmp_path, bootstrap=False)) == EXIT_ERROR
    assert "exited before reporting" in capsys.readouterr().err


def test_deploy_queue_owned_detection_failure_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A queue-ownership validation failure fails closed, never the manual path."""
    monkeypatch.setenv(JOB_ID_ENV, str(uuid4()))

    def fail_detection() -> tuple[object | None, bool]:
        msg = "cannot validate the injected queue job"
        raise lifecycle.DeployAbortedError(msg)

    monkeypatch.setattr(lifecycle, "_current_queue_job", fail_detection)
    assert lifecycle.deploy(make_options(tmp_path, bootstrap=False)) == EXIT_ERROR
    assert "cannot validate" in capsys.readouterr().err


def test_deploy_without_job_injection_keeps_manual_locked_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A manual deploy retains the synchronous locked safe path."""
    monkeypatch.setattr(lifecycle, "_current_queue_job", lambda: (None, False))
    acquired: list[bool] = []

    class FakeLock:
        """Minimal deployment-lock context for the manual path."""

        def __enter__(self) -> None:
            acquired.append(True)

        def __exit__(self, *_args: object) -> None:
            return None

    monkeypatch.setattr(lifecycle, "deploy_lock", lambda _timeout: FakeLock())
    monkeypatch.setattr(lifecycle, "_deploy_locked", lambda _options: EXIT_OK)
    assert lifecycle.deploy(make_options(tmp_path, bootstrap=False)) == EXIT_OK
    assert acquired == [True]


def test_queue_deploy_parent_reports_prepared_response(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The queue-deploy parent reports the prepared response and exits ok."""
    response = {"ok": True, "type": "deploy", "commit": GIT_SHA, "phase": "requested"}

    def fake_helper(_options: lifecycle.DeployOptions, _job_id: object, writer: int) -> None:
        os.write(writer, (json.dumps(response, sort_keys=True) + "\n").encode())
        os._exit(0)

    monkeypatch.setattr(lifecycle, "_run_deploy_helper", fake_helper)
    code = lifecycle._queue_deploy(make_options(tmp_path, bootstrap=False), uuid4())
    assert code == EXIT_OK
    assert GIT_SHA in capsys.readouterr().out


def test_queue_deploy_parent_fails_when_helper_dies_silently(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A helper that dies before reporting fails the deploy job."""
    monkeypatch.setattr(lifecycle, "_run_deploy_helper", lambda *_args: os._exit(0))
    with pytest.raises(lifecycle.DeployAbortedError, match="exited before reporting"):
        lifecycle._queue_deploy(make_options(tmp_path, bootstrap=False), uuid4())


def test_queue_deploy_parent_fails_on_helper_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A reported helper error fails the deploy job rather than faking success."""
    error = {"ok": False, "error": "candidate validation failed"}

    def fake_helper(_options: lifecycle.DeployOptions, _job_id: object, writer: int) -> None:
        os.write(writer, (json.dumps(error, sort_keys=True) + "\n").encode())
        os._exit(0)

    monkeypatch.setattr(lifecycle, "_run_deploy_helper", fake_helper)
    with pytest.raises(lifecycle.DeployAbortedError, match="candidate validation failed"):
        lifecycle._queue_deploy(make_options(tmp_path, bootstrap=False), uuid4())


def test_deploy_helper_locked_prepares_reports_waits_then_handoffs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The helper prepares, reports, waits for durable success, then handoffs.

    This is the ordering that protects the initiating deploy job: the response
    is delivered before the destructive handoff, and the handoff runs only
    after the row is durably terminal.
    """
    repo, first, second = make_repo(tmp_path / "repo")
    previous = dead_worker_meta(first, repo)
    order: list[object] = []
    monkeypatch.setattr(lifecycle, "read_meta", lambda: previous)
    monkeypatch.setattr(supervise, "supervisor_running", lambda: True)
    monkeypatch.setattr(lifecycle, "_validate_and_prepare", lambda _options: second)
    monkeypatch.setattr(
        dc, "send_helper_response", lambda _writer, _response: order.append("response")
    )
    monkeypatch.setattr(
        dc, "wait_for_durable_success", lambda _job_id, _deadline: order.append("durable")
    )

    def record_handoff(
        _options: lifecycle.DeployOptions,
        commit: str,
        _previous: WorkerMeta,
        _state: str,
    ) -> int:
        order.append(("handoff", commit))
        return EXIT_OK

    monkeypatch.setattr(lifecycle, "_complete_deploy_handoff", record_handoff)
    reader, writer = os.pipe()
    try:
        os.close(reader)
        lifecycle._deploy_helper_locked(make_options(repo, bootstrap=False), uuid4(), writer)
    finally:
        os.close(writer)
    assert order == ["response", "durable", ("handoff", second)]


def test_deploy_helper_locked_aborts_before_handoff_when_not_durable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A row that never becomes durably succeeded aborts before the handoff.

    Nothing destructive happens: the previous worker is left running, the
    provisional CLI root is removed, and the deployment never silently falls
    back to the manual synchronous path.
    """
    repo, first, second = make_repo(tmp_path / "repo")
    previous = dead_worker_meta(first, repo)
    removed: list[str] = []
    handoffs: list[object] = []
    monkeypatch.setattr(lifecycle, "read_meta", lambda: previous)
    monkeypatch.setattr(supervise, "supervisor_running", lambda: True)
    monkeypatch.setattr(lifecycle, "_validate_and_prepare", lambda _options: second)
    monkeypatch.setattr(dc, "send_helper_response", lambda _writer, _response: None)

    def fail_wait(_job_id: object, _deadline: float) -> None:
        msg = "checkout queue job reached cancelled before durable success"
        raise dc.DeployCtlError(msg)

    monkeypatch.setattr(dc, "wait_for_durable_success", fail_wait)
    monkeypatch.setattr(cli, "remove_cli_root", removed.append)
    monkeypatch.setattr(
        lifecycle, "_complete_deploy_handoff", lambda *_args, **_kwargs: handoffs.append(1)
    )
    reader, writer = os.pipe()
    try:
        os.close(reader)
        lifecycle._deploy_helper_locked(make_options(repo, bootstrap=False), uuid4(), writer)
    finally:
        os.close(writer)
    assert removed == [second]
    assert handoffs == []


def test_deploy_helper_locked_reports_unmanaged_without_bootstrap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unmanaged baseline refuses a queue deploy without the bootstrap flag."""
    errors: list[str] = []
    monkeypatch.setattr(lifecycle, "read_meta", lambda: None)
    monkeypatch.setattr(dc, "send_helper_error", lambda _writer, message: errors.append(message))
    reader, writer = os.pipe()
    try:
        os.close(reader)
        lifecycle._deploy_helper_locked(make_options(tmp_path, bootstrap=False), uuid4(), writer)
    finally:
        os.close(writer)
    assert errors
    assert "unmanaged" in errors[0]


def test_deploy_helper_locked_reports_validation_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A validation failure is reported so the row is durably failed."""
    repo, first, _second = make_repo(tmp_path / "repo")
    errors: list[str] = []

    def fail_validate(_options: lifecycle.DeployOptions) -> str:
        msg = "validation failed; the current worker is left untouched"
        raise lifecycle.DeployAbortedError(msg)

    monkeypatch.setattr(lifecycle, "read_meta", lambda: dead_worker_meta(first, repo))
    monkeypatch.setattr(supervise, "supervisor_running", lambda: True)
    monkeypatch.setattr(lifecycle, "_validate_and_prepare", fail_validate)
    monkeypatch.setattr(dc, "send_helper_error", lambda _writer, message: errors.append(message))
    reader, writer = os.pipe()
    try:
        os.close(reader)
        lifecycle._deploy_helper_locked(make_options(repo, bootstrap=False), uuid4(), writer)
    finally:
        os.close(writer)
    assert errors
    assert "validation failed" in errors[0]


def test_restore_after_handoff_failure_rolls_back_when_supervisor_holds(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A post-durable handoff failure settles back to the previous commit.

    When the supervisor never proved the candidate, the detached helper asks
    the supervisor to restore the previous confirmed commit at a strictly newer
    generation so the queue keeps exactly one known-good consumer.
    """
    repo, first, second = make_repo(tmp_path / "repo")
    monkeypatch.setattr(supervise, "supervisor_running", lambda: True)
    monkeypatch.setattr(supervise, "read_status", lambda: None)
    requested: list[tuple[str, str, str]] = []

    def request_run(
        commit: str,
        *,
        repo: str,
        uv_path: str,
        worker_id: str | None,
        restart: bool = False,
    ) -> int:
        del worker_id, restart
        requested.append((commit, repo, uv_path))
        return 42

    monkeypatch.setattr(supervise, "request_run", request_run)
    monkeypatch.setattr(supervise, "wait_for_generation", lambda _generation, _timeout: True)
    monkeypatch.setattr(supervise, "wait_until_ready", lambda _generation, _timeout: True)
    lifecycle._restore_after_handoff_failure(
        make_options(repo, bootstrap=False), second, dead_worker_meta(first, repo)
    )
    assert requested
    assert requested[0][0] == first


def test_restore_after_handoff_failure_keeps_fully_converged_candidate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A fully converged candidate (live, ready, CLIs selected) is never rolled back.

    The handoff error may come from a post-apply step (a transient readiness
    check) after the maintained CLIs already selected the candidate; the
    deployment is genuinely live and coherent, so the helper must not disturb
    the sole consumer.
    """
    repo, first, second = make_repo(tmp_path / "repo")
    cli.set_current(second)
    monkeypatch.setattr(supervise, "supervisor_running", lambda: True)
    monkeypatch.setattr(
        supervise,
        "read_status",
        lambda: supervise.SupervisorStatus(
            schema_version=supervise.SCHEMA_VERSION,
            supervisor_pid=1,
            supervisor_start_time_ticks=1,
            started_at=0.0,
            applied_generation=2,
            mode=supervise.MODE_RUN,
            commit=second,
            child=supervise.WorkerChild(
                pid=123,
                pgid=123,
                sid=123,
                start_time_ticks=1,
                token=STALE_MARKER,
                worker_id="w",
                spawned_at=0.0,
            ),
            intent=supervise.INTENT_RUN,
            restart_count=0,
            next_attempt_at=None,
            last_exit=None,
            mission=None,
            db_ready=True,
            ready=True,
            message=None,
            worker_health=None,
        ),
    )
    requested: list[object] = []
    monkeypatch.setattr(supervise, "request_run", lambda *_args, **_kwargs: requested.append(1))
    lifecycle._restore_after_handoff_failure(
        make_options(repo, bootstrap=False), second, dead_worker_meta(first, repo)
    )
    assert requested == []


def test_restore_after_handoff_failure_rolls_back_when_cli_stale(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A live candidate with a stale CLI pointer is rolled back to coherence.

    The candidate worker is live and ready, but ``cli/current`` never selected
    it, so worker and CLI would diverge. The helper must settle back to the
    previous confirmed commit and reconcile the CLIs instead of leaving the
    split state behind.
    """
    repo, first, second = make_repo(tmp_path / "repo")
    cli.set_current(first)
    monkeypatch.setattr(supervise, "supervisor_running", lambda: True)
    monkeypatch.setattr(
        supervise,
        "read_status",
        lambda: supervise.SupervisorStatus(
            schema_version=supervise.SCHEMA_VERSION,
            supervisor_pid=1,
            supervisor_start_time_ticks=1,
            started_at=0.0,
            applied_generation=2,
            mode=supervise.MODE_RUN,
            commit=second,
            child=supervise.WorkerChild(
                pid=123,
                pgid=123,
                sid=123,
                start_time_ticks=1,
                token=STALE_MARKER,
                worker_id="w",
                spawned_at=0.0,
            ),
            intent=supervise.INTENT_RUN,
            restart_count=0,
            next_attempt_at=None,
            last_exit=None,
            mission=None,
            db_ready=True,
            ready=True,
            message=None,
            worker_health=None,
        ),
    )
    requested: list[tuple[str, str, str]] = []

    def request_run(
        commit: str,
        *,
        repo: str,
        uv_path: str,
        worker_id: str | None,
        restart: bool = False,
    ) -> int:
        del worker_id, restart
        requested.append((commit, repo, uv_path))
        return 42

    monkeypatch.setattr(supervise, "request_run", request_run)
    monkeypatch.setattr(supervise, "wait_for_generation", lambda _generation, _timeout: True)
    monkeypatch.setattr(supervise, "wait_until_ready", lambda _generation, _timeout: True)
    monkeypatch.setattr(cli, "runtime_is_usable", lambda _commit: True)
    lifecycle._restore_after_handoff_failure(
        make_options(repo, bootstrap=False), second, dead_worker_meta(first, repo)
    )
    assert requested
    assert requested[0][0] == first
    assert cli.current_commit() == first


def test_queue_deploy_cli_activation_failure_rolls_back_and_converges(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A CLI activation failure after the candidate is live rolls back coherently.

    The full queue helper path: the candidate handoff succeeds, every CLI
    activation retry fails, and the helper settles the supervisor back to the
    previous confirmed commit and reconciles the maintained CLIs — so the live
    worker, the supervisor desired intent, and ``cli/current`` all select the
    same previous commit with no manual ``status`` reconciliation.
    """
    repo, first, second = make_repo(tmp_path / "repo")
    previous = dead_worker_meta(first, repo)
    responses: list[dict[str, object]] = []
    requested: list[str] = []
    real_set_current = cli.set_current
    monkeypatch.setattr(lifecycle, "read_meta", lambda: previous)
    monkeypatch.setattr(supervise, "supervisor_running", lambda: True)
    monkeypatch.setattr(lifecycle, "_validate_and_prepare", lambda _options: second)
    monkeypatch.setattr(
        dc, "send_helper_response", lambda _writer, response: responses.append(response)
    )
    monkeypatch.setattr(dc, "wait_for_durable_success", lambda _job_id, _deadline: None)

    def fake_through_supervisor(_options: lifecycle.DeployOptions, commit: str) -> WorkerMeta:
        return dead_worker_meta(commit, repo)

    monkeypatch.setattr(lifecycle, "_deploy_through_supervisor", fake_through_supervisor)

    def selective_set_current(commit: str) -> None:
        if commit != first:
            msg = f"activation boom for {commit}"
            raise cli.CliError(msg)
        real_set_current(commit)

    monkeypatch.setattr(cli, "set_current", selective_set_current)
    monkeypatch.setattr(cli, "runtime_is_usable", lambda _commit: True)

    def request_run(
        commit: str,
        *,
        repo: str,
        uv_path: str,
        worker_id: str | None,
        restart: bool = False,
    ) -> int:
        del repo, uv_path, worker_id, restart
        requested.append(commit)
        return 42

    monkeypatch.setattr(supervise, "request_run", request_run)
    monkeypatch.setattr(supervise, "wait_for_generation", lambda _generation, _timeout: True)
    monkeypatch.setattr(supervise, "wait_until_ready", lambda _generation, _timeout: True)
    monkeypatch.setattr(time, "sleep", lambda _seconds: None)

    reader, writer = os.pipe()
    try:
        os.close(reader)
        lifecycle._deploy_helper_locked(make_options(repo, bootstrap=False), uuid4(), writer)
    finally:
        os.close(writer)

    assert responses
    assert responses[0]["ok"] is True
    assert requested == [first]
    assert cli.current_commit() == first


def test_deploy_helper_locked_refuses_queue_bootstrap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Queue-invoked bootstrap is refused so no second consumer ever starts.

    A queue job is executed by a live worker, so the bootstrap precondition —
    the legacy worker was stopped manually first — is inherently unsatisfied.
    Refusing makes the row durably ``failed`` instead of risking two consumers.
    """
    errors: list[str] = []
    monkeypatch.setattr(lifecycle, "read_meta", lambda: None)
    monkeypatch.setattr(dc, "send_helper_error", lambda _writer, message: errors.append(message))
    reader, writer = os.pipe()
    try:
        os.close(reader)
        lifecycle._deploy_helper_locked(make_options(tmp_path, bootstrap=True), uuid4(), writer)
    finally:
        os.close(writer)
    assert errors
    assert "bootstrap" in errors[0]


def test_close_inherited_descriptors_closes_unintended_fds(tmp_path: Path) -> None:
    """The helper closes every inherited descriptor except the kept set."""
    stray = os.open(tmp_path / "stray", os.O_CREAT | os.O_RDWR)
    kept = os.open(tmp_path / "kept", os.O_CREAT | os.O_RDWR)
    reader, writer = os.pipe()
    try:
        pid = os.fork()
        if pid == 0:
            os.close(reader)
            lifecycle._close_inherited_descriptors({0, 1, 2, kept, writer})
            try:
                os.fstat(stray)
                result = b"open"
            except OSError:
                result = b"closed"
            os.write(writer, result)
            os.close(writer)
            os._exit(0)
        os.close(writer)
        raw = os.read(reader, 64)
        os.close(reader)
        os.waitpid(pid, 0)
        assert raw == b"closed"
    finally:
        with suppress(OSError):
            os.close(stray)
        with suppress(OSError):
            os.close(kept)


def test_run_deploy_helper_fails_closed_when_setsid_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A deploy helper that cannot detach reports an error before any success.

    A failed ``setsid`` would leave the helper inside the retiring worker's job
    session, so it must fail closed instead of ever reporting a prepared
    response that would durably ``succeed`` the row.
    """
    repo, _first, _second = make_repo(tmp_path / "repo")

    def failing_setsid() -> None:
        msg = "operation not permitted"
        raise OSError(msg)

    monkeypatch.setattr(os, "setsid", failing_setsid)
    reader, writer = os.pipe()
    pid = os.fork()
    if pid == 0:
        os.close(reader)
        lifecycle._run_deploy_helper(make_options(repo, bootstrap=False), uuid4(), writer)
    os.close(writer)
    try:
        raw = os.read(reader, 65536)
    finally:
        os.close(reader)
    os.waitpid(pid, 0)
    assert raw
    response = json.loads(raw.decode())
    assert response["ok"] is False
    assert "detach" in str(response["error"])


def test_run_restart_helper_fails_closed_when_setsid_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A restart helper that cannot detach reports an error before any success."""

    def failing_setsid() -> None:
        msg = "operation not permitted"
        raise OSError(msg)

    monkeypatch.setattr(os, "setsid", failing_setsid)
    reader, writer = os.pipe()
    pid = os.fork()
    if pid == 0:
        os.close(reader)
        lifecycle._run_restart_helper(uuid4(), writer)
    os.close(writer)
    try:
        raw = os.read(reader, 65536)
    finally:
        os.close(reader)
    os.waitpid(pid, 0)
    assert raw
    response = json.loads(raw.decode())
    assert response["ok"] is False
    assert "detach" in str(response["error"])


def test_activate_maintained_cli_retries_transient_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A transient CLI activation failure is retried to convergence.

    A freshly live candidate whose ``cli/current`` switch briefly fails must not
    be left stale requiring a manual status reconciliation.
    """
    _repo, _first, second = make_repo(tmp_path / "repo")
    real_set_current = cli.set_current
    attempts = 0

    def flaky(commit: str) -> None:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            msg = f"transient switch failure {attempts}"
            raise cli.CliError(msg)
        real_set_current(commit)

    monkeypatch.setattr(cli, "set_current", flaky)
    monkeypatch.setattr(time, "sleep", lambda _seconds: None)
    assert lifecycle._activate_maintained_cli(second) is True
    assert cli.current_commit() == second


def test_activate_maintained_cli_fails_after_bounded_retries(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A persistently failing CLI activation stops after the bounded retries."""
    _repo, _first, second = make_repo(tmp_path / "repo")

    def always_fail(_commit: str) -> None:
        msg = "switch boom"
        raise cli.CliError(msg)

    monkeypatch.setattr(cli, "set_current", always_fail)
    monkeypatch.setattr(time, "sleep", lambda _seconds: None)
    assert lifecycle._activate_maintained_cli(second) is False
    assert "activation failed" in capsys.readouterr().err


def test_restart_cmd_queue_owned_routes_to_detached_helper(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A restart running inside a Lubko job routes to the detached helper."""
    job_id = uuid4()
    captured: list[object] = []
    monkeypatch.setattr(lifecycle, "_current_queue_job", lambda: (job_id, False))

    def fake_queue(received: object) -> int:
        captured.append(received)
        return EXIT_OK

    monkeypatch.setattr(lifecycle, "_queue_restart", fake_queue)
    assert lifecycle.restart_cmd(argparse.Namespace()) == EXIT_OK
    assert captured == [job_id]


def test_restart_cmd_queue_owned_cancelled_refuses(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A cancelled restart queue owner refuses before any helper work."""
    monkeypatch.setattr(lifecycle, "_current_queue_job", lambda: (uuid4(), True))
    assert lifecycle.restart_cmd(argparse.Namespace()) == EXIT_ERROR
    assert "cancelled" in capsys.readouterr().err


def test_restart_cmd_without_job_keeps_manual_sync_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A manual restart retains the synchronous supervised path."""
    called: list[bool] = []

    def record_manual() -> int:
        called.append(True)
        return EXIT_OK

    monkeypatch.setattr(lifecycle, "_current_queue_job", lambda: (None, False))
    monkeypatch.setattr(lifecycle, "_restart_manual", record_manual)
    assert lifecycle.restart_cmd(argparse.Namespace()) == EXIT_OK
    assert called == [True]


def test_queue_restart_parent_reports_prepared_response(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The queue-restart parent reports the prepared response and exits ok."""
    response = {"ok": True, "type": "restart", "commit": GIT_SHA, "phase": "requested"}

    def fake_helper(_job_id: object, writer: int) -> None:
        os.write(writer, (json.dumps(response, sort_keys=True) + "\n").encode())
        os._exit(0)

    monkeypatch.setattr(lifecycle, "_run_restart_helper", fake_helper)
    code = lifecycle._queue_restart(uuid4())
    assert code == EXIT_OK
    assert GIT_SHA in capsys.readouterr().out


def test_queue_restart_parent_fails_when_helper_dies_silently(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A restart helper that dies before reporting fails the restart job."""
    monkeypatch.setattr(lifecycle, "_run_restart_helper", lambda *_args: os._exit(0))
    with pytest.raises(lifecycle.DeployAbortedError, match="exited before reporting"):
        lifecycle._queue_restart(uuid4())


def test_restart_helper_locked_prepares_reports_waits_then_handoffs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The restart helper reports, waits for durable success, then handoffs.

    The response is delivered before the destructive process replacement, and
    the replacement runs only after the row is durably terminal.
    """
    _repo, _first, second = make_repo(tmp_path / "repo")
    order: list[object] = []
    monkeypatch.setattr(supervise, "supervisor_running", lambda: True)
    monkeypatch.setattr(
        supervise, "read_state", lambda: replace(supervise.fresh_state(), commit=second)
    )
    monkeypatch.setattr(cli, "runtime_is_usable", lambda _commit: True)
    monkeypatch.setattr(
        dc, "send_helper_response", lambda _writer, _response: order.append("response")
    )
    monkeypatch.setattr(
        dc, "wait_for_durable_success", lambda _job_id, _deadline: order.append("durable")
    )
    monkeypatch.setattr(lifecycle, "_complete_restart_handoff", lambda: order.append("handoff"))
    reader, writer = os.pipe()
    try:
        os.close(reader)
        lifecycle._restart_helper_locked(uuid4(), writer)
    finally:
        os.close(writer)
    assert order == ["response", "durable", "handoff"]


def test_restart_helper_locked_aborts_before_handoff_when_not_durable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A restart row that never becomes durably succeeded aborts before handoff."""
    _repo, _first, second = make_repo(tmp_path / "repo")
    handoffs: list[object] = []
    monkeypatch.setattr(supervise, "supervisor_running", lambda: True)
    monkeypatch.setattr(
        supervise, "read_state", lambda: replace(supervise.fresh_state(), commit=second)
    )
    monkeypatch.setattr(cli, "runtime_is_usable", lambda _commit: True)
    monkeypatch.setattr(dc, "send_helper_response", lambda _writer, _response: None)

    def fail_wait(_job_id: object, _deadline: float) -> None:
        msg = "restart queue job reached cancelled before durable success"
        raise dc.DeployCtlError(msg)

    monkeypatch.setattr(dc, "wait_for_durable_success", fail_wait)
    monkeypatch.setattr(lifecycle, "_complete_restart_handoff", lambda: handoffs.append(1))
    reader, writer = os.pipe()
    try:
        os.close(reader)
        lifecycle._restart_helper_locked(uuid4(), writer)
    finally:
        os.close(writer)
    assert handoffs == []


def test_restart_helper_locked_reports_no_runtime(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A restart with no usable sealed runtime reports an error."""
    _repo, _first, _second = make_repo(tmp_path / "repo")
    errors: list[str] = []
    monkeypatch.setattr(supervise, "supervisor_running", lambda: True)
    monkeypatch.setattr(
        supervise, "read_state", lambda: replace(supervise.fresh_state(), commit=None)
    )
    monkeypatch.setattr(dc, "send_helper_error", lambda _writer, message: errors.append(message))
    reader, writer = os.pipe()
    try:
        os.close(reader)
        lifecycle._restart_helper_locked(uuid4(), writer)
    finally:
        os.close(writer)
    assert errors
    assert "no usable sealed runtime" in errors[0]


def test_deploy_cli_has_no_stop_command(capsys: pytest.CaptureFixture[str]) -> None:
    """The deploy CLI must not offer an administrative stop subcommand."""
    parser = lifecycle.build_parser()
    with pytest.raises(SystemExit) as exc:
        parser.parse_args(["stop"])
    assert exc.value.code != 0
    assert "invalid choice" in capsys.readouterr().err


def test_restart_fails_closed_without_supervisor(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A supervised restart is refused when no supervisor is running."""
    code = lifecycle.restart_cmd(argparse.Namespace())
    assert code == EXIT_ERROR
    assert "no external supervisor" in capsys.readouterr().err


def test_restart_fails_closed_without_confirmed_commit(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A restart with no confirmed commit fails closed."""
    monkeypatch.setattr(supervise, "supervisor_running", lambda: True)
    monkeypatch.setattr(supervise, "read_state", supervise.fresh_state)
    code = lifecycle.restart_cmd(argparse.Namespace())
    assert code == EXIT_ERROR
    assert "no usable sealed runtime" in capsys.readouterr().err


def test_migrate_cmd_writes_verified_desired_and_replaces_stale_mission(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Migration replaces stale/corrupt state with a verified exact commit."""
    repo, first, _second = make_repo(tmp_path / "repo")
    monkeypatch.setattr(cli, "_sync_venv", fake_uv_sync)
    cli.build_cli_root(repo, first, "uv", 60.0)
    rollback_state_path().parent.mkdir(parents=True, exist_ok=True)
    rollback_state_path().write_text("{not json\n", encoding="utf-8")

    args = argparse.Namespace(commit=first, repo=repo, uv="uv", lock_timeout=5.0)
    code = lifecycle.migrate_cmd(args)
    assert code == EXIT_OK
    desired = supervise.read_desired()
    assert desired is not None
    assert desired.commit == first
    assert desired.generation >= 1
    assert not rollback_state_path().exists()


def test_migrate_cmd_refuses_unverified_runtime(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Migration fails closed without a verified sealed runtime."""
    monkeypatch.setattr(supervise, "supervisor_running", lambda: False)
    args = argparse.Namespace(commit="b" * 40, repo=Path.cwd(), uv="uv", lock_timeout=5.0)
    code = lifecycle.migrate_cmd(args)
    assert code == EXIT_ERROR
    assert "no verified sealed runtime" in capsys.readouterr().err


def test_stop_worker_kills_exact_group_only(
    tmp_path: Path,
) -> None:
    """Stopping signals only the recorded worker's process group."""
    tracked = spawn_controlled()
    unrelated = spawn_controlled()
    try:
        assert lifecycle.stop_worker(meta_for_process(tracked, tmp_path), 0.5)
        wait_until(lambda: tracked.poll() is not None)
        assert unrelated.poll() is None
    finally:
        kill_proc(tracked)
        kill_proc(unrelated)


def test_stop_worker_refuses_wrong_start_time(
    tmp_path: Path,
) -> None:
    """A mismatched start time (PID reuse) prevents signalling."""
    proc = spawn_controlled()
    try:
        meta = meta_for_process(proc, tmp_path)
        forged = replace(meta, start_time_ticks=(meta.start_time_ticks or 0) + 1)
        assert not lifecycle.worker_alive(forged)
        assert lifecycle.stop_worker(forged, 0.2)
        assert proc.poll() is None
    finally:
        kill_proc(proc)


def test_stop_worker_refuses_wrong_marker(
    tmp_path: Path,
) -> None:
    """A mismatched lifecycle marker prevents signalling."""
    proc = spawn_controlled()
    try:
        meta = meta_for_process(proc, tmp_path)
        forged = replace(meta, token=OTHER_MARKER)
        assert not lifecycle.worker_alive(forged)
        # The exact process is alive but carries a different lifecycle token, so
        # it is a live, unowned process: retirement is refused (reported False)
        # and the process must NOT be signalled.
        stopped = lifecycle.stop_worker(forged, 0.2)
        assert stopped is False
        assert proc.poll() is None
    finally:
        kill_proc(proc)


def test_meta_roundtrip_atomic(tmp_path: Path) -> None:
    """Metadata survives a write/read round trip without temp residue."""
    meta = WorkerMeta(
        schema_version=1,
        state=lifecycle.STATE_RUNNING,
        pid=1,
        pgid=1,
        sid=1,
        start_time_ticks=2,
        token=SHORT_MARKER,
        repo=str(tmp_path),
        git_commit="abc",
        worker_id="w",
        log_path=str(tmp_path / "w.log"),
        started_at=1.0,
        stopped_at=None,
    )
    lifecycle.write_meta(meta)
    assert lifecycle.read_meta() == meta
    state = lifecycle.worker_state_dir()
    assert not (state / "meta.json.tmp").exists()


def test_deploy_lock_serializes() -> None:
    """Two concurrent deployments cannot both hold the lock."""
    with (
        lifecycle.deploy_lock(1.0),
        pytest.raises(lifecycle.LockTimeoutError, match="timed out"),
        lifecycle.deploy_lock(0.2),
    ):
        pass


def test_run_validation_success(tmp_path: Path) -> None:
    """All validation commands pass with a healthy uv."""
    uv = write_fake_uv(tmp_path)
    report = lifecycle.run_validation(tmp_path, uv, 5.0)
    assert report.ok
    assert not report.detail


def test_run_validation_reports_first_failure(tmp_path: Path) -> None:
    """The first failing validation command is reported."""
    uv = write_fake_uv(tmp_path, fail_on="pytest")
    report = lifecycle.run_validation(tmp_path, uv, 5.0)
    assert not report.ok
    assert "pytest" in report.detail


def test_check_postgres_success(monkeypatch: pytest.MonkeyPatch) -> None:
    """check_postgres connects using the file-based database configuration."""
    config = DatabaseConfig(
        host="db.example.com",
        port=5432,
        dbname="postgres",
        user="lubko_worker",
        password=TEST_PASSWORD,
    )
    monkeypatch.setattr(lifecycle, "load_database_config", lambda: config)
    connection = mock.MagicMock()
    connection.__enter__.return_value = connection
    cursor = mock.MagicMock()
    cursor.__enter__.return_value = cursor
    cursor.fetchone.return_value = (1,)
    connection.cursor.return_value = cursor
    captured: list[tuple[tuple[str, ...], dict[str, object]]] = []

    def recording_connect(*args: str, **kwargs: object) -> object:
        captured.append((args, kwargs))
        return connection

    monkeypatch.setattr("lubko.lifecycle.psycopg.connect", recording_connect)
    assert lifecycle.check_postgres(1.0)
    assert captured[0][0][0] == config.conninfo()
    assert captured[0][1]["connect_timeout"] == 1


def test_check_postgres_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """check_postgres returns False when the connection fails."""
    config = DatabaseConfig(
        host="db.example.com",
        port=5432,
        dbname="postgres",
        user="lubko_worker",
        password=TEST_PASSWORD,
    )
    monkeypatch.setattr(lifecycle, "load_database_config", lambda: config)

    def fake_connect(*_args: object, **_kwargs: object) -> object:
        msg = "boom"
        raise psycopg.OperationalError(msg)

    monkeypatch.setattr("lubko.lifecycle.psycopg.connect", fake_connect)
    assert not lifecycle.check_postgres(1.0)


def test_check_postgres_missing_config_returns_false(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """check_postgres returns False when the configuration file is unavailable."""

    def missing() -> object:
        msg = "no config"
        raise FileNotFoundError(msg)

    monkeypatch.setattr(lifecycle, "load_database_config", missing)
    assert not lifecycle.check_postgres(1.0)


def test_worker_env_removes_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    """worker_env strips libpq, connection-string, and credential variables."""
    monkeypatch.setenv("PGHOST", "db.example.com")
    monkeypatch.setenv("PGPASSWORD", "secret-value")
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@db.example.com/db")
    monkeypatch.setenv("POSTGRES_PASSWORD", "secret-value")
    monkeypatch.setenv("LUBKO_WORKER_ID", "phoebe")
    monkeypatch.setenv("LUBKO_LIFECYCLE_TOKEN", "outer")

    env = lifecycle.worker_env(MARKER)

    assert "PGHOST" not in env
    assert "PGPASSWORD" not in env
    assert "DATABASE_URL" not in env
    assert "POSTGRES_PASSWORD" not in env
    assert env[lifecycle.LIFECYCLE_MARKER_VAR] == MARKER
    assert env["LUBKO_WORKER_ID"] == "phoebe"


def test_worker_env_keeps_unrelated_variables(monkeypatch: pytest.MonkeyPatch) -> None:
    """worker_env preserves unrelated environment variables."""
    monkeypatch.setenv("HOME", "/home/user1")
    monkeypatch.setenv("PATH", "/bin")

    env = lifecycle.worker_env(MARKER)

    assert env["HOME"] == "/home/user1"
    assert env["PATH"] == "/bin"


def test_git_commit_reads_head() -> None:
    """git_commit reads the checkout HEAD hash without mutating git."""
    repo = Path(__file__).resolve().parents[1]
    commit = lifecycle.git_commit(repo, 5.0)
    assert commit is not None
    assert len(commit) == GIT_SHA_LENGTH


def test_main_status_dispatch(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The CLI dispatches the status subcommand."""
    assert lifecycle.main(["status"]) == EXIT_OK
    assert "state: unmanaged" in capsys.readouterr().out


def test_deploy_cmd_prefers_explicit_uv(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An explicit --uv wins over PATH and recorded executables."""
    explicit = write_uv_executable(tmp_path, name="explicit-uv")
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    write_uv_executable(bin_dir)
    recorded = write_uv_executable(tmp_path, name="recorded-uv")
    toolchain.write_toolchain(recorded)
    monkeypatch.setenv("PATH", str(bin_dir))

    captured = capture_deploy_uv(monkeypatch)
    args = make_deploy_args(tmp_path, uv=explicit)
    assert lifecycle.deploy_cmd(args) == EXIT_OK
    assert captured == [explicit]


def test_deploy_cmd_uses_path_uv_before_recorded(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Uv on PATH wins over the recorded executable."""
    path_dir = tmp_path / "path"
    path_dir.mkdir()
    on_path = write_uv_executable(path_dir)
    recorded = write_uv_executable(tmp_path, name="recorded-uv")
    toolchain.write_toolchain(recorded)
    monkeypatch.setenv("PATH", str(path_dir))

    captured = capture_deploy_uv(monkeypatch)
    args = make_deploy_args(tmp_path, uv=None)
    assert lifecycle.deploy_cmd(args) == EXIT_OK
    assert captured == [on_path]


def test_deploy_cmd_uses_recorded_uv_when_not_on_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With uv off PATH, the recorded executable is used."""
    recorded = write_uv_executable(tmp_path, name="recorded-uv")
    toolchain.write_toolchain(recorded)
    monkeypatch.setenv("PATH", "/nonexistent")

    captured = capture_deploy_uv(monkeypatch)
    args = make_deploy_args(tmp_path, uv=None)
    assert lifecycle.deploy_cmd(args) == EXIT_OK
    assert captured == [recorded]


def test_deploy_cmd_rejects_broken_explicit_uv(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A broken explicit --uv is refused even though uv is on PATH."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    write_uv_executable(bin_dir)
    monkeypatch.setenv("PATH", str(bin_dir))

    args = make_deploy_args(tmp_path, uv=str(tmp_path / "missing-uv"))
    assert lifecycle.deploy_cmd(args) == EXIT_ERROR
    assert "explicit uv executable" in capsys.readouterr().err


def test_deploy_cmd_fails_without_any_uv(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Without uv on PATH or a recorded toolchain, deploy fails clearly."""
    monkeypatch.setenv("PATH", "/nonexistent")

    args = make_deploy_args(tmp_path, uv=None)
    assert lifecycle.deploy_cmd(args) == EXIT_ERROR
    assert "uv" in capsys.readouterr().err


def test_deploy_cmd_fails_on_stale_recorded_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A stale recorded path prevents deployment without uv on PATH."""
    toolchain.write_toolchain(str(tmp_path / "gone-uv"))
    monkeypatch.setenv("PATH", "/nonexistent")

    args = make_deploy_args(tmp_path, uv=None)
    assert lifecycle.deploy_cmd(args) == EXIT_ERROR
    assert "recorded" in capsys.readouterr().err


# ---------------------------------------------------------------------------
# Repair (supported recovery path for corrupted lifecycle state)
# ---------------------------------------------------------------------------

REPAIR_WORKER_ID: Final = "repair-worker"
REPAIR_TIMINGS: Final = {
    "LUBKO_POLL_INTERVAL_SECONDS": "0.05",
    "LUBKO_PROCESS_POLL_INTERVAL_SECONDS": "0.01",
    "LUBKO_CANCEL_GRACE_SECONDS": "0.5",
    "LUBKO_LEASE_DURATION_SECONDS": "2.0",
    "LUBKO_LEASE_REFRESH_INTERVAL_SECONDS": "0.15",
    "LUBKO_LEASE_RECOVERY_INTERVAL_SECONDS": "0.2",
    "LUBKO_LEASE_SAFETY_MARGIN_SECONDS": "0.3",
    "LUBKO_OUTPUT_PUBLICATION_INTERVAL_SECONDS": "0.1",
    "LUBKO_CLAIM_BATCH_LIMIT": "16",
}


def write_database_config(
    tmp_path: Path,
    cluster: _pg.PgCluster,
    *,
    name: str = "database.conf",
) -> Path:
    """Write a private database configuration file for the cluster.

    Args:
        tmp_path: Temporary directory for the configuration file.
        cluster: The running PostgreSQL cluster.
        name: Configuration file name, allowing several clusters per test.

    Returns:
        The configuration file path.
    """
    conf = tmp_path / name
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


def spawn_real_worker(
    db_conf: Path,
    *,
    worker_id: str = REPAIR_WORKER_ID,
) -> subprocess.Popen[bytes]:
    """Spawn a real queue-consuming worker registered with the process guard.

    Args:
        db_conf: Database configuration file for the worker.
        worker_id: Worker identifier the worker records on claims.

    Returns:
        The spawned worker process.
    """
    env = dict(os.environ)
    env["LUBKO_DATABASE_CONFIG"] = str(db_conf)
    env["LUBKO_WORKER_ID"] = worker_id
    env.update(REPAIR_TIMINGS)
    proc = subprocess.Popen(
        [sys.executable, "-m", "lubko.worker"],
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


def make_repair_options(repo: Path, *, probe_timeout: float = 30.0) -> lifecycle.DeployOptions:
    """Build repair options for tests.

    Args:
        repo: Repository to repair against.
        probe_timeout: Queue-probe timeout in seconds.

    Returns:
        Repair deployment options.
    """
    return lifecycle.DeployOptions(
        repo=repo,
        uv_path="uv",
        bootstrap=False,
        stop_grace_seconds=0.5,
        postgres_timeout_seconds=5.0,
        lock_timeout_seconds=1.0,
        validation_timeout_seconds=5.0,
        git_timeout_seconds=5.0,
        cli_timeout_seconds=5.0,
        probe_timeout_seconds=probe_timeout,
    )


def stale_meta(pid: int, commit: str, repo: Path) -> WorkerMeta:
    """Build the exact incident-corruption lifecycle metadata.

    Args:
        pid: Synthetic process identity.
        commit: Synthetic commit.
        repo: Repository recorded in the metadata.

    Returns:
        Corrupt ``test-worker`` metadata.
    """
    return WorkerMeta(
        schema_version=1,
        state=lifecycle.STATE_RUNNING,
        pid=pid,
        pgid=pid,
        sid=pid,
        start_time_ticks=pid * 10,
        token=f"token-{pid}",
        repo=str(repo),
        git_commit=commit,
        worker_id="test-worker",
        log_path="corrupt-worker.log",
        started_at=1.0,
        stopped_at=None,
    )


@pytest.fixture
def second_pg_cluster(tmp_path: Path) -> Iterator[_pg.PgCluster]:
    """Start a second isolated PostgreSQL cluster for two-worker tests.

    Args:
        tmp_path: Pytest temporary directory.

    Yields:
        The second running cluster.
    """
    binaries = _pg.postgres_binaries()
    if binaries is None:
        pytest.skip("PostgreSQL server binaries not available on this host")
    root = tmp_path / "pg-other"
    data_dir = root / "data"
    socket_dir = root / "sock"
    socket_dir.mkdir(parents=True)
    port = _pg.free_port()
    subprocess.run(
        [
            binaries["initdb"],
            "-D",
            str(data_dir),
            "-U",
            "postgres",
            "--auth=trust",
        ],
        check=True,
        capture_output=True,
    )
    current = _pg.PgCluster(binaries, data_dir, socket_dir, port, dict(os.environ))
    current.start()
    with psycopg.connect(current.conninfo()) as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS lubko")
        conn.execute("DROP TABLE IF EXISTS lubko.jobs CASCADE")
        conn.execute(BASELINE_MIGRATION.read_text(encoding="utf-8"))
    try:
        yield current
    finally:
        current.stop()


def test_spawned_by_recovery_worker_true_for_child_and_grandchild() -> None:
    """The ancestor walk accepts direct children and deeper descendants.

    A real grandchild mirrors the production recovery worker launched through
    ``uv run lubko-worker``: the adopted PID (``uv``) is the grandparent of the
    job command process, whose direct parent is the worker daemon child.
    """
    child = subprocess.Popen(
        [
            sys.executable,
            "-c",
            (
                "import subprocess, sys, time; "
                "p = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(300)']); "
                "print(p.pid, flush=True); "
                "time.sleep(300)"
            ),
        ],
        cwd=REPO_ROOT,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
    )
    guard.register(child)
    grandchild_pid = int(child.stdout.readline().strip()) if child.stdout is not None else 0
    try:
        assert lifecycle._spawned_by_recovery_worker(child.pid, os.getppid())
        assert lifecycle._spawned_by_recovery_worker(grandchild_pid, child.pid)
        assert lifecycle._spawned_by_recovery_worker(grandchild_pid, os.getppid())
    finally:
        with suppress(ProcessLookupError):
            os.kill(grandchild_pid, signal.SIGKILL)
        if child.poll() is None:
            child.kill()
        child.wait(timeout=5)
        guard.unregister(child)


def test_spawned_by_recovery_worker_false_for_unrelated_pid() -> None:
    """A process whose ancestor chain lacks the pid is never accepted."""
    child = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(300)"],
        cwd=REPO_ROOT,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    guard.register(child)
    try:
        assert not lifecycle._spawned_by_recovery_worker(child.pid, child.pid + 1)
        assert not lifecycle._spawned_by_recovery_worker(999_999, os.getppid())
    finally:
        if child.poll() is None:
            child.kill()
        child.wait(timeout=5)
        guard.unregister(child)


def test_spawned_by_recovery_worker_false_for_dead_process() -> None:
    """A gone process is never accepted, failing closed."""
    assert not lifecycle._spawned_by_recovery_worker(999_999, os.getppid())


def test_repair_adopts_recovery_worker(
    jobs_db: str,
    pg_cluster: _pg.PgCluster,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Repair adopts a live recovery worker after real queue verification."""
    del jobs_db
    conf = write_database_config(tmp_path, pg_cluster)
    monkeypatch.setenv("LUBKO_DATABASE_CONFIG", str(conf))
    repo, _first, second = make_repo(tmp_path / "repo")
    monkeypatch.setattr(cli, "_sync_venv", fake_uv_sync)
    cli.build_cli_root(repo, second, "uv", 60.0)
    worker = spawn_real_worker(conf)
    try:
        code = lifecycle.repair(make_repair_options(repo), worker.pid)

        assert code == EXIT_OK
        meta = lifecycle.read_meta()
        assert meta is not None
        assert meta.pid == worker.pid
        assert meta.pgid == worker.pid
        assert meta.worker_id == REPAIR_WORKER_ID
        assert meta.git_commit == second
        assert meta.repo == str(repo)
        assert lifecycle.worker_alive(meta)
        assert cli.current_commit() == second
    finally:
        kill_proc(worker)


def test_repair_rewrites_corrupt_test_worker_metadata(
    jobs_db: str,
    pg_cluster: _pg.PgCluster,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Repair replaces the incident corruption signature, never trusting it."""
    del jobs_db
    conf = write_database_config(tmp_path, pg_cluster)
    monkeypatch.setenv("LUBKO_DATABASE_CONFIG", str(conf))
    repo, _first, second = make_repo(tmp_path / "repo")
    monkeypatch.setattr(cli, "_sync_venv", fake_uv_sync)
    cli.build_cli_root(repo, second, "uv", 60.0)
    lifecycle.write_meta(stale_meta(647_485, "deadbeef", repo))
    worker = spawn_real_worker(conf)
    try:
        code = lifecycle.repair(make_repair_options(repo), worker.pid)

        assert code == EXIT_OK
        meta = lifecycle.read_meta()
        assert meta is not None
        assert meta.pid == worker.pid
        assert meta.worker_id == REPAIR_WORKER_ID
        assert meta.worker_id != "test-worker"
        assert meta.git_commit == second
        assert lifecycle.worker_alive(meta)
    finally:
        kill_proc(worker)


def test_repair_clears_stale_rollback_ready_and_toolchain_state(
    jobs_db: str,
    pg_cluster: _pg.PgCluster,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Repair removes stale test-produced state whose ownership is proven."""
    del jobs_db
    conf = write_database_config(tmp_path, pg_cluster)
    monkeypatch.setenv("LUBKO_DATABASE_CONFIG", str(conf))
    repo, _first, second = make_repo(tmp_path / "repo")
    monkeypatch.setattr(cli, "_sync_venv", fake_uv_sync)
    cli.build_cli_root(repo, second, "uv", 60.0)
    worker = spawn_real_worker(conf)
    try:
        rollback = {
            "schema_version": 1,
            "status": "confirmed",
            "commit": "2" * 40,
            "previous_commit": "3" * 40,
            "challenge_hash": "0" * 64,
            "deadline": 1.0,
            "repo": str(repo),
            "uv_path": "uv",
            "stop_grace_seconds": 5.0,
            "git_timeout_seconds": 10.0,
            "previous_retiring": True,
            "previous_meta": stale_meta(434_468, "3" * 40, repo).to_dict(),
            "new_meta": stale_meta(553_520, "2" * 40, repo).to_dict(),
        }
        rollback_path = rollback_state_path()
        rollback_path.parent.mkdir(parents=True, exist_ok=True)
        rollback_path.write_text(
            __import__("json").dumps(rollback, sort_keys=True) + "\n", encoding="utf-8"
        )
        worker_dir = lifecycle.worker_state_dir()
        worker_dir.mkdir(parents=True, exist_ok=True)
        (worker_dir / "ready-stale.json").write_text(
            '{"v": 1, "pid": 424242, "token": "stale"}\n', encoding="utf-8"
        )
        (worker_dir / "ready-garbage.json").write_text("not json\n", encoding="utf-8")
        ready_kept = worker_dir / f"ready-{worker.pid}.json"
        ready_kept.write_text(
            f'{{"v": 1, "pid": {worker.pid}, "token": "fresh"}}\n', encoding="utf-8"
        )
        toolchain.write_toolchain("/nonexistent/uv")

        code = lifecycle.repair(make_repair_options(repo), worker.pid)

        assert code == EXIT_OK
        assert not rollback_path.exists()
        assert not (worker_dir / "ready-stale.json").exists()
        assert not (worker_dir / "ready-garbage.json").exists()
        assert ready_kept.exists()
        recorded = toolchain.read_toolchain()
        assert recorded is not None
        assert recorded.uv_path == "uv"
        meta = lifecycle.read_meta()
        assert meta is not None
        assert meta.pid == worker.pid
    finally:
        kill_proc(worker)


def test_repair_refuses_a_process_that_is_not_a_worker(
    tmp_path: Path,
) -> None:
    """A live process whose command is not a Lubko worker is refused."""
    repo, _first, _second = make_repo(tmp_path / "repo")
    proc = spawn_controlled()
    try:
        code = lifecycle.repair(make_repair_options(repo, probe_timeout=1.0), proc.pid)
        assert code == EXIT_ERROR
        assert lifecycle.read_meta() is None
        assert proc.poll() is None
    finally:
        kill_proc(proc)


def test_repair_refuses_a_non_leader_process(
    tmp_path: Path,
) -> None:
    """A process that is not its own session/group leader is refused."""
    repo, _first, _second = make_repo(tmp_path / "repo")
    env = dict(os.environ)
    proc = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(300)"],
        cwd=REPO_ROOT,
        env=env,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    guard.register(proc)
    try:
        code = lifecycle.repair(make_repair_options(repo, probe_timeout=1.0), proc.pid)
        assert code == EXIT_ERROR
        assert lifecycle.read_meta() is None
    finally:
        if proc.poll() is None:
            proc.kill()
        proc.wait(timeout=5)
        guard.unregister(proc)


def test_repair_refuses_conflicting_live_maintained_worker(
    jobs_db: str,
    pg_cluster: _pg.PgCluster,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Repair never adopts a second worker over a live recorded one."""
    del jobs_db
    conf = write_database_config(tmp_path, pg_cluster)
    monkeypatch.setenv("LUBKO_DATABASE_CONFIG", str(conf))
    repo, first, _second = make_repo(tmp_path / "repo")
    recorded_worker = spawn_real_worker(conf)
    adopting_worker = spawn_real_worker(conf)
    try:
        identity = lifecycle.process_identity(recorded_worker.pid)
        assert identity is not None
        recorded_meta = WorkerMeta(
            schema_version=1,
            state=lifecycle.STATE_RUNNING,
            pid=identity.pid,
            pgid=identity.pgid,
            sid=identity.sid,
            start_time_ticks=identity.start_time_ticks,
            token=None,
            repo=str(repo),
            git_commit=first,
            worker_id=REPAIR_WORKER_ID,
            log_path=str(lifecycle.worker_log_path()),
            started_at=time.time(),
            stopped_at=None,
        )
        lifecycle.write_meta(recorded_meta)
        code = lifecycle.repair(make_repair_options(repo), adopting_worker.pid)
        assert code == EXIT_ERROR
        meta = lifecycle.read_meta()
        assert meta is not None
        assert meta.pid == recorded_worker.pid
    finally:
        kill_proc(recorded_worker)
        kill_proc(adopting_worker)


def test_repair_refuses_when_recovery_worker_cannot_consume(
    jobs_db: str,
    pg_cluster: _pg.PgCluster,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A process that never consumes the queue is never adopted."""
    del jobs_db
    conf = write_database_config(tmp_path, pg_cluster)
    monkeypatch.setenv("LUBKO_DATABASE_CONFIG", str(conf))
    repo, _first, _second = make_repo(tmp_path / "repo")
    fake_worker = tmp_path / "bin"
    fake_worker.mkdir()
    script = fake_worker / "lubko-worker"
    script.write_text("#!/bin/sh\nsleep 300\n", encoding="utf-8")
    script.chmod(0o755)
    proc = subprocess.Popen(
        [str(script)],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        close_fds=True,
    )
    guard.register(proc)
    try:
        code = lifecycle.repair(make_repair_options(repo, probe_timeout=1.0), proc.pid)
        assert code == EXIT_ERROR
        assert lifecycle.read_meta() is None
    finally:
        kill_proc(proc)


def test_repair_refuses_live_pending_rollback_mission(
    jobs_db: str,
    pg_cluster: _pg.PgCluster,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A live pending supervised mission blocks adoption."""
    del jobs_db
    conf = write_database_config(tmp_path, pg_cluster)
    monkeypatch.setenv("LUBKO_DATABASE_CONFIG", str(conf))
    repo, _first, _second = make_repo(tmp_path / "repo")
    candidate = spawn_real_worker(conf)
    adopting_worker = spawn_real_worker(conf)
    try:
        identity = lifecycle.process_identity(candidate.pid)
        assert identity is not None
        pending = {
            "schema_version": 1,
            "status": "pending",
            "commit": "2" * 40,
            "previous_commit": "3" * 40,
            "challenge_hash": None,
            "deadline": time.time() + 60,
            "repo": str(repo),
            "uv_path": "uv",
            "stop_grace_seconds": 5.0,
            "git_timeout_seconds": 10.0,
            "previous_retiring": False,
            "previous_meta": stale_meta(434_468, "3" * 40, repo).to_dict(),
            "new_meta": WorkerMeta(
                schema_version=1,
                state=lifecycle.STATE_RUNNING,
                pid=identity.pid,
                pgid=identity.pgid,
                sid=identity.sid,
                start_time_ticks=identity.start_time_ticks,
                token=None,
                repo=str(repo),
                git_commit="2" * 40,
                worker_id=REPAIR_WORKER_ID,
                log_path=str(lifecycle.worker_log_path()),
                started_at=time.time(),
                stopped_at=None,
            ).to_dict(),
        }
        rollback_path = rollback_state_path()
        rollback_path.parent.mkdir(parents=True, exist_ok=True)
        rollback_path.write_text(
            __import__("json").dumps(pending, sort_keys=True) + "\n", encoding="utf-8"
        )
        code = lifecycle.repair(make_repair_options(repo), adopting_worker.pid)
        assert code == EXIT_ERROR
        assert rollback_path.exists()
        assert lifecycle.read_meta() is None
    finally:
        kill_proc(candidate)
        kill_proc(adopting_worker)


def probe_claimed_by(
    repo: Path,
    worker_pid: int,
    *,
    expected_worker_id: str = REPAIR_WORKER_ID,
    timeout_seconds: float = 30.0,
) -> bool:
    """Return whether the exact worker PID claims a fresh probe from the queue.

    The repair-queue probe is inserted, then awaited with the same
    exact-PID-bounded proof the repair uses: the persisted ``process_pid``
    must be a descendant of ``worker_pid``. The probe is always cancelled,
    awaited terminal, and removed.

    Args:
        repo: Working directory for the probe command.
        worker_pid: Exact worker PID whose claim is being proven.
        expected_worker_id: Worker identifier the probe claim must record.
        timeout_seconds: Maximum seconds to wait for the claim.

    Returns:
        ``True`` only when the exact worker PID executed the probe.
    """
    conn = psycopg.connect(load_database_config().conninfo(), autocommit=True)
    probe_id = lifecycle._insert_probe_job(conn, str(repo))
    assert probe_id is not None
    try:
        outcome = lifecycle._wait_for_probe_claim(
            conn, probe_id, expected_worker_id, worker_pid, timeout_seconds
        )
    finally:
        request_cancel(conn, probe_id)
        lifecycle._wait_for_probe_terminal(conn, probe_id, timeout_seconds)
        delete_job_and_chunks(conn, probe_id)
        conn.close()
    return outcome


def test_repair_refuses_when_same_id_other_worker_claims_probe(
    jobs_db: str,
    pg_cluster: _pg.PgCluster,
    second_pg_cluster: _pg.PgCluster,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A same-worker-id twin that claims the probe must never be adopted.

    Two real workers run simultaneously with the identical
    ``LUBKO_WORKER_ID``. The operator supplies the PID of worker A, but the
    probe is claimed and executed by worker B (the only consumer of the repair
    queue). The claim proof is bound to the exact supplied PID through the
    persisted ``process_pid`` descendant check, so the repair must refuse to
    adopt A rather than accept B's consumption as evidence for A.
    """
    del jobs_db
    conf = write_database_config(tmp_path, pg_cluster)
    monkeypatch.setenv("LUBKO_DATABASE_CONFIG", str(conf))
    repo, _first, second = make_repo(tmp_path / "repo")
    monkeypatch.setattr(cli, "_sync_venv", fake_uv_sync)
    cli.build_cli_root(repo, second, "uv", 60.0)
    other_conf = write_database_config(tmp_path, second_pg_cluster, name="other.conf")
    supplied = spawn_real_worker(other_conf)
    claiming = spawn_real_worker(conf)
    try:
        assert probe_claimed_by(repo, claiming.pid) is True
        assert probe_claimed_by(repo, supplied.pid) is False

        code = lifecycle.repair(make_repair_options(repo), supplied.pid)

        assert code == EXIT_ERROR
        assert lifecycle.read_meta() is None
        assert supplied.poll() is None
        assert claiming.poll() is None
    finally:
        kill_proc(supplied)
        kill_proc(claiming)


def test_recover_starts_an_adoptable_session_leader(
    jobs_db: str,
    pg_cluster: _pg.PgCluster,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The supported recover command starts a dedicated adoptable leader.

    The documented recovery flow is ``lubko-deploy recover`` (starts a
    detached session/process-group-leader worker with a stable exact PID)
    followed by ``lubko-deploy repair --recovery-worker-pid <PID>``. This
    proves the flow end to end: the spawned worker is a dedicated leader and
    repair adopts its exact PID, recording its real lifecycle token.
    """
    del jobs_db
    conf = write_database_config(tmp_path, pg_cluster)
    monkeypatch.setenv("LUBKO_DATABASE_CONFIG", str(conf))
    repo, _first, second = make_repo(tmp_path / "repo")
    monkeypatch.setattr(cli, "_sync_venv", fake_uv_sync)
    cli.build_cli_root(repo, second, "uv", 60.0)
    spawned: list[subprocess.Popen[bytes]] = []
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

    monkeypatch.setattr(lifecycle, "spawn_worker", tracking_spawn)
    monkeypatch.setattr(
        lifecycle, "_worker_command", lambda _uv: [sys.executable, "-m", "lubko.worker"]
    )
    try:
        code = lifecycle.recover(make_repair_options(repo, probe_timeout=1.0))
        assert code == EXIT_OK
        assert len(spawned) == 1
        pid = spawned[0].pid
        identity = lifecycle.process_identity(pid)
        assert identity is not None
        assert identity.pgid == pid
        assert identity.sid == pid
        assert lifecycle._is_lubko_worker_process(pid)
        assert lifecycle.read_meta() is None

        code = lifecycle.repair(make_repair_options(repo), pid)
        assert code == EXIT_OK
        meta = lifecycle.read_meta()
        assert meta is not None
        assert meta.pid == pid
        assert meta.git_commit == second
        assert lifecycle.worker_alive(meta)
    finally:
        kill_many(spawned)


def test_recover_refuses_when_any_worker_consumes(
    jobs_db: str,
    pg_cluster: _pg.PgCluster,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Recover refuses to start a second consumer when the queue is occupied."""
    del jobs_db
    conf = write_database_config(tmp_path, pg_cluster)
    monkeypatch.setenv("LUBKO_DATABASE_CONFIG", str(conf))
    repo, _first, _second = make_repo(tmp_path / "repo")
    claiming = spawn_real_worker(conf)
    try:
        code = lifecycle.recover(make_repair_options(repo, probe_timeout=1.0))

        assert code == EXIT_ERROR
        assert "already consuming the queue" in capsys.readouterr().err
        assert lifecycle.read_meta() is None
        assert claiming.poll() is None
    finally:
        kill_proc(claiming)


def test_recover_refuses_when_live_maintained_worker(
    jobs_db: str,
    pg_cluster: _pg.PgCluster,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Recover refuses to start a worker while a maintained worker is live."""
    del jobs_db
    conf = write_database_config(tmp_path, pg_cluster)
    monkeypatch.setenv("LUBKO_DATABASE_CONFIG", str(conf))
    repo, _first, _second = make_repo(tmp_path / "repo")
    worker = spawn_real_worker(conf)
    try:
        identity = lifecycle.process_identity(worker.pid)
        assert identity is not None
        recorded = WorkerMeta(
            schema_version=1,
            state=lifecycle.STATE_RUNNING,
            pid=identity.pid,
            pgid=identity.pgid,
            sid=identity.sid,
            start_time_ticks=identity.start_time_ticks,
            token=None,
            repo=str(repo),
            git_commit=GIT_SHA,
            worker_id=REPAIR_WORKER_ID,
            log_path=str(lifecycle.worker_log_path()),
            started_at=time.time(),
            stopped_at=None,
        )
        lifecycle.write_meta(recorded)
        code = lifecycle.recover(make_repair_options(repo, probe_timeout=1.0))

        assert code == EXIT_ERROR
        assert "live maintained worker" in capsys.readouterr().err
        assert lifecycle.read_meta() is not None
    finally:
        kill_proc(worker)


def test_repair_refuses_a_foreground_worker(
    jobs_db: str,
    pg_cluster: _pg.PgCluster,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A foreground worker inheriting the terminal group is never adopted.

    A worker started in the foreground (for example ``uv run lubko-worker`` in
    a terminal) inherits the terminal's session and foreground process group,
    so it is not a dedicated session/group leader. Repair must refuse it
    rather than adopt an identity whose group it could never safely signal, and
    it must never signal the foreground worker or its shared group.
    """
    del jobs_db
    conf = write_database_config(tmp_path, pg_cluster)
    monkeypatch.setenv("LUBKO_DATABASE_CONFIG", str(conf))
    repo, _first, second = make_repo(tmp_path / "repo")
    monkeypatch.setattr(cli, "_sync_venv", fake_uv_sync)
    cli.build_cli_root(repo, second, "uv", 60.0)
    env = dict(os.environ)
    env["LUBKO_DATABASE_CONFIG"] = str(conf)
    env["LUBKO_WORKER_ID"] = REPAIR_WORKER_ID
    env.update(REPAIR_TIMINGS)
    proc = subprocess.Popen(
        [sys.executable, "-m", "lubko.worker"],
        cwd=REPO_ROOT,
        env=env,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    guard.register(proc)
    try:
        time.sleep(0.3)
        code = lifecycle.repair(make_repair_options(repo, probe_timeout=1.0), proc.pid)

        assert code == EXIT_ERROR
        assert lifecycle.read_meta() is None
        assert proc.poll() is None
    finally:
        if proc.poll() is None:
            proc.kill()
        proc.wait(timeout=5)
        guard.unregister(proc)


def test_process_has_token_rejects_adjacent_env_key() -> None:
    """A different env key containing the token substring is never accepted.

    ``/proc/<pid>/environ`` is NUL-separated; a naive ``marker in environ``
    byte-substring check would accept ``X_LUBKO_LIFECYCLE_TOKEN=<token>``
    because the expected ``LUBKO_LIFECYCLE_TOKEN=<token>`` appears as a
    suffix of the adjacent entry's bytes.  The function must parse NUL-
    separated entries and require exact ``KEY=VALUE`` equality.
    """
    token = uuid4().hex
    wrong_key_env = dict(os.environ)
    wrong_key_env["X_LUBKO_LIFECYCLE_TOKEN"] = token
    wrong_proc = subprocess.Popen(
        [SLEEP_BIN, "300"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        close_fds=True,
        env=wrong_key_env,
    )
    guard.register(wrong_proc)
    try:
        assert not lifecycle.process_has_token(wrong_proc.pid, token)
    finally:
        if wrong_proc.poll() is None:
            wrong_proc.kill()
        wrong_proc.wait(timeout=5)
        guard.unregister(wrong_proc)

    exact_env = dict(os.environ)
    exact_env[lifecycle.LIFECYCLE_MARKER_VAR] = token
    exact_proc = subprocess.Popen(
        [SLEEP_BIN, "300"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        close_fds=True,
        env=exact_env,
    )
    guard.register(exact_proc)
    try:
        assert lifecycle.process_has_token(exact_proc.pid, token)
    finally:
        if exact_proc.poll() is None:
            exact_proc.kill()
        exact_proc.wait(timeout=5)
        guard.unregister(exact_proc)


def test_probe_job_independent_of_sys_executable_and_runtime_path(
    jobs_db: str,
    pg_cluster: _pg.PgCluster,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Readiness probe must not depend on sys.executable or the runtime dir.

    Production proved that ``lifecycle._insert_probe_job`` uses
    ``[sys.executable, -c, sleep]`` and a still-running supervisor loses
    readiness after its immutable runtime directory is pruned.  The probe
    process must be a static binary independent of the supervisor runtime
    path.  This test both inspects the probe payload (unit) and runs the
    probe through a real worker (E2E) to prove the exact-worker
    queue-roundtrip proof, cancellation, terminal wait, cleanup, and
    process isolation are preserved.
    """
    del jobs_db
    conf = write_database_config(tmp_path, pg_cluster)
    monkeypatch.setenv("LUBKO_DATABASE_CONFIG", str(conf))
    conn = psycopg.connect(load_database_config().conninfo(), autocommit=True)
    try:
        probe_id = lifecycle._insert_probe_job(conn, str(tmp_path))
        assert probe_id is not None

        with conn.cursor(row_factory=tuple_row) as cursor:
            cursor.execute(
                "SELECT (payload::jsonb)->'request'->'process' FROM lubko.jobs WHERE id = %s",
                (probe_id,),
            )
            row = cursor.fetchone()
        assert row is not None
        process_argv = row[0]
        assert isinstance(process_argv, list)
        assert len(process_argv) >= 2

        assert sys.executable not in process_argv, (
            f"probe process must not reference sys.executable ({sys.executable!r}); "
            "the readiness probe must be independent of the supervisor runtime path"
        )

        runtime_dir = Path(sys.prefix)
        for element in process_argv:
            assert str(runtime_dir) not in str(element), (
                f"probe process element {element!r} references the runtime directory "
                f"({runtime_dir!r}); the readiness probe must be independent of the "
                "supervisor runtime path"
            )

        worker = spawn_real_worker(conf)
        try:
            try:
                outcome = lifecycle._wait_for_probe_claim(
                    conn, probe_id, REPAIR_WORKER_ID, worker.pid, 10.0
                )
                assert outcome is True, "exact worker must claim the probe proving queue-roundtrip"
            finally:
                with suppress(psycopg.Error):
                    request_cancel(conn, probe_id)
                lifecycle._wait_for_probe_terminal(conn, probe_id, 10.0)
                with suppress(psycopg.Error):
                    delete_job_and_chunks(conn, probe_id)

            with conn.cursor(row_factory=tuple_row) as cursor:
                cursor.execute(
                    "SELECT (payload::jsonb)->'state'->>'status' FROM lubko.jobs WHERE id = %s",
                    (probe_id,),
                )
                terminal_row = cursor.fetchone()
            assert terminal_row is None, "probe row must be deleted after terminal, proving cleanup"
            assert worker.poll() is None, "worker must still be alive after probe lifecycle"
        finally:
            kill_proc(worker)
    finally:
        conn.close()
