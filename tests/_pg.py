"""Shared isolated PostgreSQL cluster harness for integration tests.

These helpers spin up a real, isolated PostgreSQL cluster (detected on PATH or
in the Guix store) so row-locking and atomic ``jsonb_set`` semantics are
exercised, never mocked. The whole test module skips when no server
installation is available.
"""

from __future__ import annotations

import os
import shutil
import signal
import socket
import subprocess
import time
from contextlib import suppress
from pathlib import Path
from typing import Final

POSTGRES_NAMES: Final = ("initdb", "postgres", "pg_ctl")


class PgCluster:
    """A running isolated PostgreSQL server on a unix socket."""

    def __init__(
        self,
        binaries: dict[str, str],
        data_dir: Path,
        socket_dir: Path,
        port: int,
        env: dict[str, str],
    ) -> None:
        self.binaries = binaries
        self.data_dir = data_dir
        self.socket_dir = socket_dir
        self.port = port
        self.env = env
        self.postmaster_pid: int | None = None

    def conninfo(self) -> str:
        """Return a connection string for this cluster.

        Returns:
            A libpq connection string.
        """
        return f"host={self.socket_dir} port={self.port} dbname=postgres user=postgres"

    def start(self) -> None:
        """Start the postmaster and record its exact PID."""
        subprocess.run(
            [
                self.binaries["pg_ctl"],
                "-D",
                str(self.data_dir),
                "-o",
                f"-p {self.port} -k {self.socket_dir}",
                "-l",
                str(self.data_dir.parent / "server.log"),
                "start",
            ],
            env=self.env,
            check=True,
            capture_output=True,
        )
        pidfile = self.data_dir / "postmaster.pid"
        try:
            self.postmaster_pid = int(pidfile.read_text(encoding="utf-8").splitlines()[0])
        except (OSError, ValueError, IndexError):
            self.postmaster_pid = None

    def stop(self) -> None:
        """Stop the cluster and confirm the exact postmaster is gone.

        The postmaster is stopped through ``pg_ctl`` and then verified by its
        exact recorded PID; if it still lives afterwards the exact PID is
        force-killed, so a cluster teardown never leaks a postmaster.
        """
        subprocess.run(
            [
                self.binaries["pg_ctl"],
                "-D",
                str(self.data_dir),
                "-m",
                "immediate",
                "stop",
            ],
            env=self.env,
            check=False,
            capture_output=True,
        )
        self._assert_postmaster_gone()

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
