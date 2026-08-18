"""Shared isolated PostgreSQL cluster harness for integration tests.

These helpers spin up a real, isolated PostgreSQL cluster (detected on PATH or
in the Guix store) so row-locking and atomic ``jsonb_set`` semantics are
exercised, never mocked. The whole test module skips when no server
installation is available.

Every cluster is owned by an exact incarnation identity: the postmaster PID
*and* its start-time-in-clock-ticks, both read from the PID file and
``/proc/<pid>/stat`` immediately after ``pg_ctl start`` returns.  Every
liveness probe and every signal delivery re-verifies the full PID+ticks pair
so a recycled PID can never be mistaken for the postmaster.

Teardown is fail-closed: ``pg_ctl stop`` is attempted first, then the exact
incarnation is re-verified and force-killed if still alive.  A failed
``start()`` that raised before recording an identity still attempts
``pg_ctl stop`` and cleans up any pidfile that was written.
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
from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    from collections.abc import Callable

POSTGRES_NAMES: Final = ("initdb", "postgres", "pg_ctl")
_STAT_STATE_FIELD: Final = 0
_STAT_MIN_FIELDS: Final = 20
_STAT_STARTTIME_FIELD_INDEX: Final = 19


def _proc_start_ticks(pid: int) -> int | None:
    """Return a process start time in clock ticks, or ``None``.

    The start time is unique per process on a given boot and survives PID
    reuse, making it the reliable incarnation anchor.

    Args:
        pid: Process ID to inspect.

    Returns:
        The start time in clock ticks, or ``None`` when unreadable.
    """
    try:
        stat = (Path("/proc") / str(pid) / "stat").read_bytes()
    except OSError:
        return None
    close_paren = stat.rfind(b")")
    if close_paren == -1:
        return None
    fields = stat[close_paren + 2 :].split()
    if len(fields) < _STAT_MIN_FIELDS:
        return None
    try:
        return int(fields[_STAT_STARTTIME_FIELD_INDEX])
    except ValueError:
        return None


def _proc_alive(pid: int) -> bool:
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


def _incarnation_alive(pid: int, ticks: int) -> bool:
    """Return whether the exact PID+ticks incarnation is live.

    Ticks are mandatory: a missing start-time tick means the incarnation
    cannot be verified, and PID-only liveness is never accepted for an
    owned postmaster.

    Args:
        pid: Process ID to probe.
        ticks: Expected start-time in clock ticks.

    Returns:
        ``True`` when the PID is alive and the incarnation matches.
    """
    if not _proc_alive(pid):
        return False
    actual = _proc_start_ticks(pid)
    return actual is not None and actual == ticks


def _read_pidfile(data_dir: Path) -> tuple[int | None, int | None]:
    """Read the postmaster PID and start-time ticks from the PID file.

    Returns:
        A ``(pid, start_ticks)`` pair.  Either may be ``None`` when
        unreadable.
    """
    pidfile = data_dir / "postmaster.pid"
    try:
        pid = int(pidfile.read_text(encoding="utf-8").splitlines()[0])
    except (OSError, ValueError, IndexError):
        return None, None
    ticks = _proc_start_ticks(pid)
    return pid, ticks


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
        self.on_start: Callable[[int, int], None] | None = None
        self.on_stop: Callable[[int, int], None] | None = None

    def conninfo(self) -> str:
        """Return a connection string for this cluster.

        Returns:
            A libpq connection string.
        """
        return f"host={self.socket_dir} port={self.port} dbname=postgres user=postgres"

    def start(self) -> None:
        """Start the postmaster and record its exact incarnation identity.

        ``pg_ctl start`` is wrapped so that any failure — including a
        ``CalledProcessError`` from ``check=True`` — still attempts
        ``pg_ctl stop`` and cleans up any pidfile that was written.  After
        startup the PID *and* start-time ticks are read from the pidfile and
        verified live; only then does the cluster become usable.

        Raises:
            AssertionError: If the postmaster could not be confirmed alive
                with an exact incarnation identity.
            subprocess.CalledProcessError: If ``pg_ctl start`` itself fails.
        """
        try:
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
        except subprocess.CalledProcessError:
            # Capture the exact incarnation BEFORE best-effort stop, which
            # may remove the pidfile.  If no identity can be captured,
            # pg_ctl stop against the verified test-owned data_dir is the
            # only cleanup authority.
            captured_pid, captured_ticks = _read_pidfile(self.data_dir)
            self._try_pg_ctl_stop()
            if captured_pid is not None and captured_ticks is not None:
                deadline = time.monotonic() + 10.0
                while time.monotonic() < deadline and _incarnation_alive(
                    captured_pid, captured_ticks
                ):
                    time.sleep(0.05)
                if _incarnation_alive(captured_pid, captured_ticks):
                    with suppress(ProcessLookupError):
                        os.kill(captured_pid, signal.SIGKILL)
                    deadline = time.monotonic() + 10.0
                    while time.monotonic() < deadline and _incarnation_alive(
                        captured_pid, captured_ticks
                    ):
                        time.sleep(0.05)
                if _incarnation_alive(captured_pid, captured_ticks):
                    msg = (
                        f"postgres postmaster pid {captured_pid} "
                        f"(ticks={captured_ticks}) still live after "
                        "pg_ctl start failure cleanup"
                    )
                    raise AssertionError(msg) from None
            raise
        self._read_identity_from_pidfile()
        if self.postmaster_pid is None or self.postmaster_start_ticks is None:
            pid = self.postmaster_pid
            ticks = self.postmaster_start_ticks
            self._try_pg_ctl_stop()
            msg = (
                f"postgres postmaster identity incomplete after pg_ctl start: "
                f"pid={pid} ticks={ticks}; both are required for incarnation proof"
            )
            raise AssertionError(msg)
        if not _incarnation_alive(self.postmaster_pid, self.postmaster_start_ticks):
            pid = self.postmaster_pid
            ticks = self.postmaster_start_ticks
            self._try_pg_ctl_stop()
            self._assert_incarnation_gone(
                f"postmaster pid {pid} (ticks={ticks}) not alive after start"
            )
            msg = f"postgres postmaster pid {pid} (ticks={ticks}) not alive after start"
            raise AssertionError(msg)
        self._notify_start()

    def _notify_start(self) -> None:
        """Invoke the ``on_start`` callback after a successful start.

        If the callback fails, the new postmaster is stopped and its exact
        incarnation is proven gone before the exception propagates.
        """
        if self.on_start is None:
            return
        pid = self.postmaster_pid
        ticks = self.postmaster_start_ticks
        assert pid is not None
        assert ticks is not None
        try:
            self.on_start(pid, ticks)
        except Exception:
            self._try_pg_ctl_stop()
            self._assert_incarnation_gone(
                f"on_start callback failed; postmaster pid {pid} (ticks={ticks})"
            )
            raise

    def _read_identity_from_pidfile(self) -> None:
        """Record the postmaster incarnation from the PID file.

        Both PID and start-time ticks are captured together so every later
        liveness probe re-verifies the full incarnation pair.
        """
        self.postmaster_pid, self.postmaster_start_ticks = _read_pidfile(self.data_dir)

    def _try_pg_ctl_stop(self) -> None:
        """Attempt ``pg_ctl stop`` as best-effort cleanup.

        This is called during error paths where the postmaster may or may
        not be running; ``check=False`` so a failure here is swallowed.
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

    def stop(self) -> None:
        """Stop the cluster and confirm the exact incarnation is gone.

        ``pg_ctl stop`` is attempted first; then the exact recorded
        incarnation (PID + start-time ticks) is re-verified.  If the
        incarnation is still alive it is force-killed by exact PID and
        asserted dead.  The identity is never trusted without a live-
        incarnation recheck, so a recycled PID cannot be signalled.

        After the incarnation is confirmed gone, ``on_stop`` is called
        (if configured) with the exact PID and ticks that were stopped,
        so the caller can unregister the incarnation.
        """
        old_pid = self.postmaster_pid
        old_ticks = self.postmaster_start_ticks
        self._try_pg_ctl_stop()
        self._assert_incarnation_gone("cluster teardown")
        if self.on_stop is not None and old_pid is not None and old_ticks is not None:
            self.on_stop(old_pid, old_ticks)

    def _assert_incarnation_gone(self, context: str) -> None:
        """Assert the exact recorded incarnation (PID + ticks) is dead.

        If the identity is not known (``start()`` failed before writing the
        PID file), no assertion is raised because there is nothing to
        verify.  The force-kill uses the exact recorded PID; no process-name
        matching or broad kill is used.  Ticks are mandatory: if the PID is
        known but ticks could not be captured, no signal is sent because
        PID-only identity is insufficient.

        Args:
            context: Human-readable description for the assertion message.

        Raises:
            AssertionError: If the incarnation is still alive after
                force-kill.
        """
        pid = self.postmaster_pid
        ticks = self.postmaster_start_ticks
        if pid is None or ticks is None:
            return
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline and _incarnation_alive(pid, ticks):
            time.sleep(0.05)
        if _incarnation_alive(pid, ticks):
            with suppress(ProcessLookupError):
                os.kill(pid, signal.SIGKILL)
            deadline = time.monotonic() + 10.0
            while time.monotonic() < deadline and _incarnation_alive(pid, ticks):
                time.sleep(0.05)
        if _incarnation_alive(pid, ticks):
            msg = f"postgres postmaster pid {pid} (ticks={ticks}) still live after {context}"
            raise AssertionError(msg)


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
