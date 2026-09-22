"""Stable invariants for already-deployed operation under exhausted storage.

Exhausting the host's general-purpose persistent filesystem must not prevent
an already-deployed supervisor and worker from starting, supervising, and
carrying jobs: diagnostic publication degrades silently, restart validation
is read-only, immutable startup artifacts are never rewritten outside an
explicit deployment transition, and exact-identity authority secured at
deployment time is updated in place without new allocation.
"""

from __future__ import annotations

import errno
import io
import os
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest

from lubko import cli, lifecycle, supervise, supervisor, worker
from lubko import deployctl as dc
from lubko import durable as _durable
from lubko import startup_contract as sc
from lubko import state as _state_mod
from lubko._exact_signal import proc_start_ticks
from lubko.config import DatabaseConfig
from lubko.durable import (
    DurabilityError,
    SlotError,
    encode_fixed_slot,
    is_fixed_slot,
    read_fixed_slot,
    rewrite_fixed_slot,
    write_bytes_durable,
)

if TYPE_CHECKING:
    from collections.abc import Callable


def _no_space(*_args: object, **_kwargs: object) -> None:
    """Simulate exhausted persistent storage deterministically.

    Raises:
        OSError: Always, with ``ENOSPC``.
    """
    raise OSError(errno.ENOSPC, "No space left on device")


def _supervisor_daemon() -> supervisor.SupervisorDaemon:
    """Build a supervisor daemon with default settings.

    Returns:
        A daemon with no worker child yet.
    """
    return supervisor.SupervisorDaemon(supervisor.Settings())


@pytest.mark.usefixtures("supervisor_token")
def test_status_snapshot_failure_is_dropped_without_affecting_supervision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed status write never fails, blocks, or alters a lifecycle decision."""
    daemon = _supervisor_daemon()
    daemon._message = "holding for an explicit run intent"
    daemon._next_db_check_at = float("inf")
    monkeypatch.setattr(supervisor, "read_state", lambda: supervise.SupervisorState.from_dict({}))
    monkeypatch.setattr(supervisor, "read_worker_health", lambda: None)
    monkeypatch.setattr(supervise, "read_supervisor_pid", lambda: None)
    monkeypatch.setattr(dc, "read_rollback_state", lambda: None)
    monkeypatch.setattr(supervisor, "write_status", _no_space)
    daemon._write_status()
    daemon._write_status()
    assert daemon._status_write_drops == 2
    assert daemon._message == "holding for an explicit run intent"


@pytest.mark.usefixtures("supervisor_token")
def test_status_publication_recovers_after_storage_returns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Dropped snapshots do not poison later publication."""
    daemon = _supervisor_daemon()
    daemon._next_db_check_at = float("inf")
    monkeypatch.setattr(supervisor, "read_state", lambda: supervise.SupervisorState.from_dict({}))
    monkeypatch.setattr(supervisor, "read_worker_health", lambda: None)
    monkeypatch.setattr(supervise, "read_supervisor_pid", lambda: None)
    monkeypatch.setattr(dc, "read_rollback_state", lambda: None)
    monkeypatch.setattr(supervisor, "write_status", _no_space)
    daemon._write_status()
    assert daemon._status_write_drops == 1
    published: list[supervise.SupervisorStatus] = []
    monkeypatch.setattr(supervisor, "write_status", published.append)
    daemon._write_status()
    assert len(published) == 1
    assert daemon._status_write_drops == 1


@pytest.mark.usefixtures("supervisor_token")
def test_restart_validation_writes_nothing_to_startup_artifacts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Validating an already-deployed restart performs no persistent writes."""
    monkeypatch.setattr(sc, "state_root", lambda: tmp_path)
    monkeypatch.setattr(dc, "state_root", lambda: tmp_path)
    monkeypatch.setattr(_state_mod, "state_root", lambda: tmp_path)
    monkeypatch.setattr(supervise, "state_root", lambda: tmp_path)
    for name in sc.CURRENT_CONTRACT.required_state_dirs:
        (tmp_path / name).mkdir(mode=0o700, exist_ok=True)
    bin_home = tmp_path / "bin"
    bin_home.mkdir(mode=0o700)
    sc.write_contract()
    sc.write_startup_definition()
    sc.write_startup_launcher(bin_home)
    assert sc.validate_startup_artifacts(bin_home) is None

    launcher = bin_home / sc.STARTUP_LAUNCHER_NAME
    launcher_before = (launcher.read_bytes(), launcher.stat().st_mtime_ns)
    contract_before = (sc.contract_path().read_bytes(), sc.contract_path().stat().st_mtime_ns)
    definition_path = sc.startup_definition_path()
    definition_before = (definition_path.read_bytes(), definition_path.stat().st_mtime_ns)

    writes: list[str] = []

    def _record_write(name: str) -> Callable[..., None]:
        def _recorder(*_args: object, **_kwargs: object) -> None:
            writes.append(name)

        return _recorder

    monkeypatch.setattr(sc, "write_contract", _record_write("contract"))
    monkeypatch.setattr(sc, "write_startup_definition", _record_write("definition"))
    monkeypatch.setattr(sc, "write_startup_launcher", _record_write("launcher"))

    assert sc.validate_startup_artifacts(bin_home) is None
    assert sc.assess_recorded_contract().state == "current"
    assert sc.validate_startup_launcher(bin_home)
    assert sc.validate_startup_definition().ok

    daemon = _supervisor_daemon()
    monkeypatch.setattr(lifecycle, "_resolve_bin_home", lambda: bin_home)
    daemon._converge_startup_artifacts()

    assert writes == []
    assert launcher.read_bytes() == launcher_before[0]
    assert launcher.stat().st_mtime_ns == launcher_before[1]
    assert sc.contract_path().read_bytes() == contract_before[0]
    assert sc.contract_path().stat().st_mtime_ns == contract_before[1]
    assert definition_path.read_bytes() == definition_before[0]
    assert definition_path.stat().st_mtime_ns == definition_before[1]


def test_worker_health_failure_leaves_supervision_unaffected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed worker health snapshot never escapes into lifecycle logic."""
    settings = worker.Settings(
        worker_id="w-test",
        poll_interval_seconds=0.0,
        process_poll_interval_seconds=0.0,
        cancel_grace_seconds=1.0,
        server="srv-test",
    )
    database = DatabaseConfig(host="h", port=1, dbname="d", user="u", password=str(uuid4()))
    daemon = worker.Supervisor(settings, database)
    monkeypatch.setattr(worker, "write_worker_health", _no_space)
    daemon._publish_health(force=True)
    assert daemon.active == {}
    assert not daemon._stopping


def _enospc() -> OSError:
    """Build a deterministic exhausted-storage failure.

    Returns:
        An ``ENOSPC`` error.
    """
    return OSError(errno.ENOSPC, "No space left on device")


def _is_write_mode(mode: str) -> bool:
    """Return whether an open mode would allocate new file content.

    Args:
        mode: Open mode string.

    Returns:
        ``True`` for modes that create, truncate, append, or update.
    """
    return "w" in mode or "a" in mode or "x" in mode or "+" in mode


class _ZeroAllocationFilesystem:
    """Simulate zero free blocks for registered trees at the syscall seam.

    Paths outside the registered roots delegate to the real syscalls. Inside
    a root, any mutation that would require a new persistent-filesystem
    block fails with ``ENOSPC``: creating files, directories, or directory
    entries, truncating, and extending a file past its current size. Reads,
    same-size in-place overwrites, ``fsync``, ``flock``, and removals
    succeed, exactly matching storage that was secured while space was
    still available.
    """

    def __init__(self, *roots: str) -> None:
        self._roots = tuple(Path(root).resolve() for root in roots)
        self._open_sizes: dict[int, int] = {}

    def register(self, *roots: str) -> None:
        """Constrain additional trees to zero free blocks.

        Args:
            roots: Directory trees to constrain.
        """
        self._roots += tuple(Path(root).resolve() for root in roots)

    def _under_root(self, path: object) -> bool:
        """Return whether a path lies inside a constrained tree.

        Args:
            path: Candidate path.

        Returns:
            ``True`` only for string-like paths inside a registered root.
        """
        if not isinstance(path, (str, os.PathLike)):
            return False
        absolute = Path(os.fspath(path)).resolve()
        return any(absolute == root or root in absolute.parents for root in self._roots)

    def wrap_open(self, real_open: Callable[..., int]) -> Callable[..., int]:
        """Wrap ``os.open`` to refuse creation of new files.

        Args:
            real_open: The real ``os.open``.

        Returns:
            A wrapper enforcing the zero-allocation rule.
        """

        def _open(path: object, flags: int, *args: object, **kwargs: object) -> int:
            if not isinstance(path, (str, os.PathLike)) or not self._under_root(path):
                return real_open(path, flags, *args, **kwargs)
            if bool(flags & os.O_CREAT) and not os.path.lexists(path):
                raise _enospc()
            fd = real_open(path, flags, *args, **kwargs)
            try:
                size = os.fstat(fd).st_size
            except OSError:
                return fd
            if flags & os.O_TRUNC:
                size = 0
            self._open_sizes[fd] = size
            return fd

        return _open

    def wrap_write(self, real_write: Callable[..., int]) -> Callable[..., int]:
        """Wrap an ``os.write``-shaped callable to refuse file growth.

        Args:
            real_write: The real write callable.

        Returns:
            A wrapper enforcing the zero-allocation rule.
        """

        def _write(fd: int, data: bytes | bytearray | memoryview) -> int:
            size = self._open_sizes.get(fd)
            if size is None:
                return real_write(fd, data)
            try:
                offset = os.lseek(fd, 0, os.SEEK_CUR)
            except OSError:
                return real_write(fd, data)
            if offset + len(data) > size:
                raise _enospc()
            written = real_write(fd, data)
            self._open_sizes[fd] = max(size, offset + written)
            return written

        return _write

    def wrap_close(self, real_close: Callable[..., None]) -> Callable[..., None]:
        """Wrap ``os.close`` to release tracked descriptors.

        Args:
            real_close: The real ``os.close``.

        Returns:
            A wrapper releasing tracking state.
        """

        def _close(fd: int) -> None:
            self._open_sizes.pop(fd, None)
            real_close(fd)

        return _close

    def wrap_io_open(self, real_io_open: Callable[..., object]) -> Callable[..., object]:
        """Wrap ``io.open`` to refuse new or truncated files under the roots.

        Appends and read-updates of already-secured files stay available:
        nothing on the steady-state path grows them.

        Args:
            real_io_open: The real ``io.open``.

        Returns:
            A wrapper enforcing the zero-allocation rule.
        """

        def _io_open(file: object, mode: object = "r", *args: object, **kwargs: object) -> object:
            if not isinstance(mode, str) or not self._under_root(file):
                return real_io_open(file, mode, *args, **kwargs)
            assert isinstance(file, (str, bytes, os.PathLike))
            if "x" in mode or "w" in mode:
                raise _enospc()
            if not os.path.lexists(file) and ("a" in mode or "+" in mode):
                raise _enospc()
            return real_io_open(file, mode, *args, **kwargs)

        return _io_open

    def wrap_fdopen(self, real_fdopen: Callable[..., object]) -> Callable[..., object]:
        """Wrap ``os.fdopen`` to refuse write modes on tracked files.

        Args:
            real_fdopen: The real ``os.fdopen``.

        Returns:
            A wrapper enforcing the zero-allocation rule.
        """

        def _fdopen(fd: int, mode: object = "r", *args: object, **kwargs: object) -> object:
            if fd in self._open_sizes and isinstance(mode, str) and _is_write_mode(mode):
                raise _enospc()
            return real_fdopen(fd, mode, *args, **kwargs)

        return _fdopen

    def wrap_replace(self, real_replace: Callable[..., None]) -> Callable[..., None]:
        """Wrap ``os.replace``/``os.rename`` to refuse new directory entries.

        Args:
            real_replace: The real replace callable.

        Returns:
            A wrapper enforcing the zero-allocation rule.
        """

        def _replace(src: object, dst: object, *args: object, **kwargs: object) -> None:
            if not self._under_root(dst):
                return real_replace(src, dst, *args, **kwargs)
            assert isinstance(src, (str, bytes, os.PathLike))
            assert isinstance(dst, (str, bytes, os.PathLike))
            if not os.path.lexists(src):
                return real_replace(src, dst, *args, **kwargs)
            if not os.path.lexists(dst):
                raise _enospc()
            return real_replace(src, dst, *args, **kwargs)

        return _replace

    def wrap_mkdir(self, real_mkdir: Callable[..., None]) -> Callable[..., None]:
        """Wrap ``os.mkdir`` to refuse new directories.

        Args:
            real_mkdir: The real ``os.mkdir``.

        Returns:
            A wrapper enforcing the zero-allocation rule.
        """

        def _mkdir(path: object, *args: object, **kwargs: object) -> None:
            if self._under_root(path):
                assert isinstance(path, (str, bytes, os.PathLike))
                if not os.path.lexists(path):
                    raise _enospc()
            return real_mkdir(path, *args, **kwargs)

        return _mkdir


@pytest.fixture
def zero_allocation(monkeypatch: pytest.MonkeyPatch) -> Callable[..., None]:
    """Activate the zero-free-block simulation for registered roots.

    Returns:
        A callable registering constrained roots after activation.
    """
    double = _ZeroAllocationFilesystem()
    monkeypatch.setattr(os, "open", double.wrap_open(os.open))
    monkeypatch.setattr(os, "write", double.wrap_write(os.write))
    monkeypatch.setattr(os, "close", double.wrap_close(os.close))
    monkeypatch.setattr(os, "fdopen", double.wrap_fdopen(os.fdopen))
    monkeypatch.setattr(os, "replace", double.wrap_replace(os.replace))
    monkeypatch.setattr(os, "rename", double.wrap_replace(os.rename))
    monkeypatch.setattr(os, "mkdir", double.wrap_mkdir(os.mkdir))
    monkeypatch.setattr(io, "open", double.wrap_io_open(io.open))
    monkeypatch.setattr(_durable, "_os_write", double.wrap_write(_durable._os_write))

    def _register(*roots: str) -> None:
        double.register(*roots)

    return _register


def _prepare_deployed_tree(bin_home: Path) -> None:
    """Prepare an already-deployed tree while storage is still available.

    Installs launchers, the startup contract/definition, the required state
    directories, the ownership lock file, and the secured identity slot, so a
    later restart under exhausted storage exercises only pre-secured capacity.

    Args:
        bin_home: Directory holding the installed launchers.
    """
    for name in sc.CURRENT_CONTRACT.required_state_dirs:
        (_state_mod.state_root() / name).mkdir(mode=0o700, parents=True, exist_ok=True)
    bin_home.mkdir(mode=0o700, parents=True, exist_ok=True)
    cli.install_launchers(bin_home)
    sc.write_contract()
    sc.write_startup_definition()
    sc.write_startup_launcher(bin_home)
    supervise.supervisor_lock_path().touch(mode=0o600, exist_ok=True)
    (supervise.supervisor_dir() / ".pending-request.lock").touch(exist_ok=True)
    supervise.write_supervisor_pid(os.getpid(), proc_start_ticks(os.getpid()) or 0)
    assert is_fixed_slot(supervise.supervisor_pid_path(), size=supervise.SUPERVISOR_PID_SLOT_SIZE)


def _deployed_files(root: Path) -> set[str]:
    """Snapshot every relative path under a deployed tree.

    Args:
        root: Tree to snapshot.

    Returns:
        Relative paths of all entries.
    """
    return {str(path.relative_to(root)) for path in root.rglob("*")}


@pytest.mark.usefixtures("supervisor_token")
def test_prepared_restart_needs_no_new_blocks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, zero_allocation: Callable[..., None]
) -> None:
    """A prepared restart starts, owns, and reconciles with zero new blocks."""
    state_root = _state_mod.state_root()
    bin_home = tmp_path / "bin"
    _prepare_deployed_tree(bin_home)
    launcher = bin_home / sc.STARTUP_LAUNCHER_NAME
    launcher_before = launcher.read_bytes()
    pid_path = supervise.supervisor_pid_path()
    tree_before = _deployed_files(state_root)
    zero_allocation(str(state_root), str(bin_home))

    cli.install_launchers(bin_home)
    assert sc.install_and_validate_startup_definition(bin_home) is None
    assert launcher.read_bytes() == launcher_before

    lock_fd = supervise.acquire_supervisor_lock()
    try:
        daemon = _supervisor_daemon()
        daemon._write_pidfile()
    finally:
        os.close(lock_fd)
    recorded = supervise.read_supervisor_pid()
    assert recorded is not None
    assert recorded[0] == os.getpid()
    assert pid_path.stat().st_size == supervise.SUPERVISOR_PID_SLOT_SIZE

    daemon = _supervisor_daemon()
    daemon._next_db_check_at = float("inf")
    monkeypatch.setattr(supervisor, "read_worker_health", lambda: None)
    monkeypatch.setattr(lifecycle, "_resolve_bin_home", lambda: bin_home)
    monkeypatch.setattr(daemon, "_ensure_worker", lambda _commit: None)
    monkeypatch.setattr(daemon, "_maybe_reset_backoff", lambda _state, _now: None)
    monkeypatch.setattr(daemon, "_record_mission_progress", lambda _commit: None)
    monkeypatch.setattr(daemon, "_probe_readiness", lambda _now: None)
    monkeypatch.setattr(daemon, "_complete_cold_migration", lambda: None)
    daemon.reconcile(0.0)
    assert "no explicit" in (daemon._message or "")
    daemon._write_status()
    assert daemon._status_write_drops == 1

    assert launcher.read_bytes() == launcher_before
    assert _deployed_files(state_root) == tree_before


@pytest.mark.usefixtures("supervisor_token")
def test_first_start_without_a_slot_fails_closed(
    zero_allocation: Callable[..., None],
) -> None:
    """Authority is never faked: securing capacity without space refuses loudly."""
    zero_allocation(str(_state_mod.state_root()))
    daemon = _supervisor_daemon()
    with pytest.raises(SystemExit):
        daemon._write_pidfile()
    assert supervise.read_supervisor_pid() is None


@pytest.mark.usefixtures("supervisor_token")
def test_torn_identity_slot_never_reads_as_another_daemon() -> None:
    """A partially overwritten identity record fails closed, never mismatched."""
    supervise.write_supervisor_pid(os.getpid(), proc_start_ticks(os.getpid()) or 0)
    path = supervise.supervisor_pid_path()
    raw = bytearray(path.read_bytes())
    assert len(raw) == supervise.SUPERVISOR_PID_SLOT_SIZE
    raw[10] = (raw[10] + 1) % 256
    path.write_bytes(bytes(raw))
    with pytest.raises(supervise.MalformedSupervisorIdentityError):
        supervise.read_supervisor_pid()
    daemon = _supervisor_daemon()
    with pytest.raises(SystemExit):
        daemon._write_pidfile()


def test_fixed_slot_rewrite_needs_no_new_blocks(
    tmp_path: Path, zero_allocation: Callable[..., None]
) -> None:
    """Prepared slots update in place; unprepared paths fail loudly, not silently."""
    slot = tmp_path / "authority.slot"
    assert read_fixed_slot(slot, size=256) is None
    assert not is_fixed_slot(slot, size=256)
    with pytest.raises(SlotError):
        rewrite_fixed_slot(slot, b"v1", size=256)
    write_bytes_durable(slot, encode_fixed_slot(b"v1", size=256))
    assert read_fixed_slot(slot, size=256) == b"v1"
    with pytest.raises(DurabilityError):
        encode_fixed_slot(b"x" * 256, size=256)

    zero_allocation(str(tmp_path))
    tree_before = {path.name for path in tmp_path.iterdir()}
    rewrite_fixed_slot(slot, b"v2", size=256)
    assert read_fixed_slot(slot, size=256) == b"v2"
    assert slot.stat().st_size == 256
    assert {path.name for path in tmp_path.iterdir()} == tree_before


def test_worker_startup_prefix_needs_no_new_blocks(
    zero_allocation: Callable[..., None],
) -> None:
    """Maintained-worker startup reaches the queue with zero new blocks."""
    worker_root = _state_mod.state_root() / "worker"
    worker_root.mkdir(parents=True, exist_ok=True)
    before = _deployed_files(worker_root)
    zero_allocation(str(_state_mod.state_root()))
    settings = worker.Settings(
        worker_id="w-test",
        poll_interval_seconds=0.0,
        process_poll_interval_seconds=0.0,
        cancel_grace_seconds=1.0,
        server="srv-test",
    )
    database = DatabaseConfig(host="h", port=1, dbname="d", user="u", password=str(uuid4()))
    daemon = worker.Supervisor(settings, database)
    daemon.conn = None
    daemon._publish_health(force=True)
    assert daemon.active == {}
    assert not daemon._stopping
    assert _deployed_files(worker_root) == before
