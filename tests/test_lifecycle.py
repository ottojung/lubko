"""Tests for Lubko worker lifecycle management."""

import argparse
import os
import shutil
import signal
import subprocess
import time
from collections.abc import Callable
from contextlib import suppress
from dataclasses import replace
from pathlib import Path
from typing import Final
from unittest import mock

import psycopg
import pytest

from lubko import cli, lifecycle, toolchain
from lubko.config import DatabaseConfig
from lubko.lifecycle import (
    EXIT_ERROR,
    EXIT_OK,
    ProcessIdentity,
    ValidationReport,
    WorkerMeta,
)
from tests import _process_guard as guard
from tests.test_cli import fake_uv_sync, make_repo

MARKER: Final = "test-marker"
STALE_MARKER: Final = "stale"
OTHER_MARKER: Final = "other-marker"
SHORT_MARKER: Final = "tok"
GIT_SHA: Final = "a" * 40
GIT_SHA_LENGTH: Final = 40
SLEEP_BIN: Final = shutil.which("sleep") or "/bin/sleep"
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
        assert lifecycle.worker_log_path().is_file()
        out = capsys.readouterr().out
        assert "deployed git commit" in out
        assert GIT_SHA in out
    finally:
        kill_proc(old)
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


def test_stop_cmd_stops_maintained_worker(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The stop command terminates the maintained worker by identity."""
    proc = spawn_controlled()
    try:
        lifecycle.write_meta(meta_for_process(proc, tmp_path))
        code = lifecycle.stop_cmd(0.5)
        assert code == EXIT_OK
        wait_until(lambda: proc.poll() is not None)
        meta = lifecycle.read_meta()
        assert meta is not None
        assert meta.state == lifecycle.STATE_STOPPED
        assert "stopped" in capsys.readouterr().out
    finally:
        kill_proc(proc)


def test_stop_cmd_refuses_unmanaged(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The stop command refuses to claim it can stop a legacy worker."""
    code = lifecycle.stop_cmd(0.5)
    assert code == EXIT_ERROR
    assert "unmanaged" in capsys.readouterr().err


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
        assert lifecycle.stop_worker(forged, 0.2)
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
