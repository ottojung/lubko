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
        self.postmaster_start_ticks: int | None = None

    def conninfo(self) -> str:
        """Return a connection string for this cluster.

        Returns:
            A libpq connection string.
        """
        return f"host={self.socket_dir} port={self.port} dbname=postgres user=postgres"

    def start(self) -> None:
        """Start the postmaster and record its exact PID and start ticks."""
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
        if self.postmaster_pid is not None:
            self.postmaster_start_ticks = proc_start_ticks(self.postmaster_pid)
        else:
            self.postmaster_start_ticks = None

    def _identity_is_current(self) -> bool:
        """Return whether the recorded postmaster identity is verifiably live.

        Authoritative exact-identity check: the recorded start ticks must be
        valid and still match the live occupant of the recorded PID, so a
        reused or stale PID can never be force-signalled during teardown.

        Returns:
            ``True`` only when the recorded identity provably matches.
        """
        pid = self.postmaster_pid
        ticks = self.postmaster_start_ticks
        if pid is None or ticks is None or ticks <= 0:
            return False
        return proc_start_ticks(pid) == ticks

    def _refuse_stale_pre_stop(self) -> None:
        """Fail closed when the recorded PID is live but its ticks mismatch.

        Any shutdown signalling targets whatever occupies the recorded
        postmaster PID, so a reused/stale identity must be refused before
        anything is signalled.  A fully absent occupant is safe to skip.

        Raises:
            AssertionError: When a live occupant does not match the recorded
                start-ticks identity.
        """
        pid = self.postmaster_pid
        if pid is None:
            return
        current = proc_start_ticks(pid)
        if current is None:
            # Occupant already gone: nothing can be signalled.
            return
        ticks = self.postmaster_start_ticks
        if ticks is None or ticks <= 0 or current != ticks:
            msg = (
                f"postgres postmaster pid {pid} identity stale/reused; "
                "refusing shutdown against an unverified occupant"
            )
            raise AssertionError(msg)

    def _signal_postmaster(self, sig: int) -> bool:
        """Signal the postmaster only under its exact verified identity.

        The recorded start ticks are re-read immediately before signalling;
        a session/group leader's dedicated group is signalled (taking its
        backends with it), a non-leader receives an exact-PID signal only.

        Args:
            sig: Signal to deliver.

        Returns:
            ``True`` when delivered; ``False`` when the identity went stale.
        """
        pid = self.postmaster_pid
        ticks = self.postmaster_start_ticks
        if pid is None or ticks is None or proc_start_ticks(pid) != ticks:
            return False
        try:
            pgid = os.getpgid(pid)
        except ProcessLookupError:
            return False
        with suppress(ProcessLookupError):
            if pgid == pid:
                os.killpg(pgid, sig)
            else:
                os.kill(pid, sig)
        return True

    def stop(self) -> None:
        """Stop the cluster and confirm the exact postmaster is gone.

        Immediate shutdown is performed by an exact PID+start-ticks signal
        at the signal point (never through ``pg_ctl``, which would signal
        whatever numeric PID occupies the postmaster pidfile), followed by
        exact escalation and reap verification.  A stale/reused or
        unverifiable identity fails closed instead of being signalled.

        Raises:
            AssertionError: When the identity is unrecorded or goes stale
                between observation and the signalling point.
        """
        self._refuse_stale_pre_stop()
        pid = self.postmaster_pid
        if pid is not None and proc_start_ticks(pid) is None:
            # Already truly gone before teardown: succeed without signalling.
            self.assert_postmaster_gone()
            return
        ticks = self.postmaster_start_ticks
        if pid is None or ticks is None:
            msg = "postmaster identity unrecorded; refusing unverified shutdown"
            raise AssertionError(msg)
        if not self._signal_postmaster(signal.SIGQUIT):
            msg = f"postmaster pid {pid} identity went stale at signal point"
            raise AssertionError(msg)
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline and process_live(pid):
            time.sleep(0.05)
        if process_live(pid) and not self._signal_postmaster(signal.SIGKILL):
            msg = f"postmaster pid {pid} identity went stale at escalation"
            raise AssertionError(msg)
        self.assert_postmaster_gone()

    def assert_postmaster_gone(self) -> None:
        pid = self.postmaster_pid
        if pid is None:
            return
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline and process_live(pid):
            time.sleep(0.05)
        if process_live(pid):
            # Exact-identity revalidation via the one canonical signalling
            # helper: a stale/reused PID must never be signalled.
            if not self._signal_postmaster(signal.SIGKILL):
                msg = (
                    f"postgres postmaster pid {pid} identity stale/unverifiable; "
                    "refusing to signal an unverified occupant"
                )
                raise AssertionError(msg)
            deadline = time.monotonic() + 10.0
            while time.monotonic() < deadline and process_live(pid):
                time.sleep(0.05)
        if process_live(pid):
            msg = f"postgres postmaster pid {pid} still live after cluster teardown"
            raise AssertionError(msg)


def proc_start_ticks(pid: int) -> int | None:
    """Return the process start time in clock ticks, or ``None`` when gone.

    Args:
        pid: Process to inspect.

    Returns:
        The start time in clock ticks, or ``None`` when unreadable/gone.
    """
    try:
        stat = (Path("/proc") / str(pid) / "stat").read_text(encoding="utf-8")
    except OSError:
        return None
    rest = stat[stat.rfind(")") + 1 :].split()
    try:
        return int(rest[19])
    except (ValueError, IndexError):
        return None


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
