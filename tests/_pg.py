"""Shared isolated PostgreSQL cluster harness for integration tests.

These helpers spin up a real, isolated PostgreSQL cluster (detached on PATH or
in the Guix store) so row-locking and atomic ``jsonb_set`` semantics are
exercised, never mocked.  The whole test module skips when no server
installation is available.

Postmaster ownership
--------------------

The postmaster is started as a **direct child** of a tiny Python shim that
calls ``prctl(PR_SET_PDEATHSIG, SIGKILL)`` via ``ctypes`` before exec-ing
the ``postgres`` binary.  When the pytest process is killed externally
(SIGKILL, OOM, timeout runner), the kernel sends ``SIGKILL`` to the shim
(now the postmaster after exec), so the postmaster cannot outlive the test
owner.  Normal teardown terminates the shim through ``Popen.terminate`` /
``Popen.wait`` via the process guard, and the force-kill fallback covers
the narrow window where the shim exited but the postmaster has not yet
received the parent-death signal.
"""

from __future__ import annotations

import os
import shutil
import signal
import socket
import subprocess
import textwrap
import time
from contextlib import suppress
from pathlib import Path
from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    from types import ModuleType

POSTGRES_NAMES: Final = ("initdb", "postgres", "pg_ctl")

_PYTHON_SHIM_SOURCE: Final = textwrap.dedent(
    """\
    import ctypes, os, sys
    libc = ctypes.CDLL("libc.so.6")
    PR_SET_PDEATHSIG = 1
    SIGKILL = 9
    libc.prctl(PR_SET_PDEATHSIG, SIGKILL)
    os.execvp(sys.argv[1], sys.argv[1:])
    """
).replace("\n    ", "\n")


def _resolve_shim(shim_dir: Path) -> str:
    """Return the Python ``PR_SET_PDEATHSIG`` shim path.

    Writes a tiny Python script that calls ``prctl(PR_SET_PDEATHSIG,
    SIGKILL)`` via ctypes then execs its arguments.  No C compiler is
    needed.

    Args:
        shim_dir: Directory for the shim script.

    Returns:
        Absolute path of the shim.
    """
    shim_path = shim_dir / "pdeathsig-shim.py"
    shim_path.write_text(
        "#!/usr/bin/env python3\n" + _PYTHON_SHIM_SOURCE,
        encoding="utf-8",
    )
    shim_path.chmod(0o755)
    return str(shim_path)


class PgCluster:
    """A running isolated PostgreSQL server on a unix socket.

    The postmaster is a direct child of the shim process, which is itself a
    direct child of the pytest process.  ``PR_SET_PDEATHSIG`` ensures the
    postmaster cannot outlive the test owner even when pytest is externally
    killed.
    """

    def __init__(  # ruff: ignore[too-many-arguments] - cluster needs these parameters
        self,
        binaries: dict[str, str],
        data_dir: Path,
        socket_dir: Path,
        port: int,
        env: dict[str, str],
        *,
        shim_proc: subprocess.Popen[bytes] | None = None,
    ) -> None:
        self.binaries = binaries
        self.data_dir = data_dir
        self.socket_dir = socket_dir
        self.port = port
        self.env = env
        self.postmaster_pid: int | None = None
        self._shim_proc = shim_proc

    def conninfo(self) -> str:
        """Return a connection string for this cluster.

        Returns:
            A libpq connection string.
        """
        return f"host={self.socket_dir} port={self.port} dbname=postgres user=postgres"

    def start(self, guard_mod: ModuleType | None = None) -> None:
        """Start the postmaster as a direct shim child and record its PID.

        The ``guard_mod`` parameter accepts the ``tests._process_guard``
        module so the shim process is registered for deterministic teardown.
        The postmaster PID is read from the ``postmaster.pid`` file written
        by the server itself.

        Raises:
            RuntimeError: If the postmaster does not start within the timeout.
        """
        shim = _resolve_shim(self.data_dir.parent)
        log_path = self.data_dir.parent / "server.log"
        cmd = [
            shim,
            self.binaries["postgres"],
            "-D",
            str(self.data_dir),
            "-p",
            str(self.port),
            "-k",
            str(self.socket_dir),
        ]
        with log_path.open("ab") as log:
            self._shim_proc = subprocess.Popen(
                cmd,
                env=self.env,
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
                close_fds=True,
            )
        if guard_mod is not None:
            guard_mod.register(self._shim_proc)
        deadline = time.monotonic() + 15.0
        while time.monotonic() < deadline:
            pidfile = self.data_dir / "postmaster.pid"
            if pidfile.is_file():
                try:
                    self.postmaster_pid = int(pidfile.read_text(encoding="utf-8").splitlines()[0])
                    if process_live(self.postmaster_pid):
                        return
                except (OSError, ValueError, IndexError):
                    pass
            time.sleep(0.1)
        msg = f"postmaster did not start within timeout (shim pid {self._shim_proc.pid})"
        raise RuntimeError(msg)

    def stop(self) -> None:
        """Stop the shim and confirm the exact postmaster is gone.

        Normal teardown terminates the shim (which propagates to the
        postmaster); a force-kill fallback covers the window where the shim
        exited but the postmaster survived.
        """
        if self._shim_proc is not None and self._shim_proc.poll() is None:
            self._shim_proc.terminate()
            try:
                self._shim_proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self._shim_proc.kill()
                with suppress(Exception):
                    self._shim_proc.wait(timeout=5)
        self._force_kill_postmaster()
        self._assert_postmaster_gone()

    def _force_kill_postmaster(self) -> None:
        """Force-kill the recorded postmaster PID if it is still alive.

        ``pg_ctl stop`` may be interrupted or ineffective; killing the exact
        postmaster PID ensures the cluster never leaks even when the outer
        pytest invocation is externally terminated.
        """
        pid = self.postmaster_pid
        if pid is None:
            return
        if not process_live(pid):
            return
        with suppress(ProcessLookupError):
            os.kill(pid, signal.SIGKILL)

    def _assert_postmaster_gone(self) -> None:
        pid = self.postmaster_pid
        if pid is None:
            return
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline and process_live(pid):
            time.sleep(0.05)
        if process_live(pid):
            with suppress(ProcessLookupError):
                os.kill(pid, signal.SIGKILL)
            deadline = time.monotonic() + 10.0
            while time.monotonic() < deadline and process_live(pid):
                time.sleep(0.05)
        if process_live(pid):
            msg = f"postgres postmaster pid {pid} still live after cluster teardown"
            raise AssertionError(msg)


def process_live(pid: int) -> bool:
    """Return whether a process exists and is not a zombie.

    Args:
        pid: Process ID to probe.

    Returns:
        ``True`` when a running (non-zombie) process with that ID exists.
    """
    try:
        stat = (Path("/proc") / str(pid) / "stat").read_bytes()
    except OSError:
        return False
    close_paren = stat.rfind(b")")
    if close_paren == -1:
        return True
    fields = stat[close_paren + 2 :].split()
    if not fields:
        return True
    return fields[0] not in {b"Z", b"X"}


def postgres_binaries() -> dict[str, str] | None:
    """Locate a usable PostgreSQL server installation.

    Returns:
        A mapping of binary name to path, or ``None`` when unavailable.
    """
    resolved: dict[str, str] = {}
    for name in POSTGRES_NAMES:
        path = shutil.which(name)
        if path is None:
            break
        resolved[name] = path
    else:
        return resolved
    for store in Path("/gnu/store").glob("*postgresql-*/bin"):
        candidate = {name: str(store / name) for name in POSTGRES_NAMES}
        if all(Path(path).is_file() for path in candidate.values()):
            return candidate
    return None


def postgres_lib_dir(binary_dir: Path) -> str | None:
    """Return a sibling ``lib`` directory for ``LD_LIBRARY_PATH``, if any.

    Args:
        binary_dir: Directory containing the PostgreSQL binaries.

    Returns:
        The library directory, or ``None`` when the loader already finds it.
    """
    lib = binary_dir.parent / "lib"
    if lib.is_dir():
        return str(lib)
    return None


def free_port() -> int:
    """Return an ephemeral TCP port that is currently free.

    Returns:
        A port number.
    """
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])
