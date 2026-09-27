"""A live published worker that is not our direct child is never signalled.

Exact process identity (PID, process group, session, start time, lifecycle
token) proves *which* process a published record names. It does not prove
*whose* that process is. A worker reparented away from the supervisor — the
process that spawned it exited, a subreaper or init re-adopted it, PID
namespace behaviour — keeps every identity field while ceasing to be the
supervisor's direct child, and the retirement authority must fail closed on it
exactly as :func:`lubko.lifecycle_state.authorize_retirement` states.

The reproduction here is a real process, not a fake: a grandchild is spawned by
a short-lived intermediate that then exits, so the live process is genuinely
parented by init rather than by the test process. No sleep longer than a
convergence poll is used, and the reparented process is terminated before the
test returns.
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from contextlib import suppress
from pathlib import Path
from typing import TYPE_CHECKING, Final

import pytest

from lubko import lifecycle, supervisor
from lubko import lifecycle_authority as authority
from lubko import worker as worker_mod

if TYPE_CHECKING:
    from lubko.supervisor import SupervisorDaemon

COMMIT: Final = "c" * 40
INCARNATION: Final = "3f2a9c1d7b5e4086af10c2d4e6b8a0c1"
#: A live process is all that is required: the retirement signal is delivered
#: to the process group, and nothing else about the process is consulted.
_LIVE_PROCESS: Final = "import time\ntime.sleep(300)\n"
#: Spawn the grandchild into its own session, then exit so the grandchild is
#: reparented away from this process and can no longer be reaped with ``waitpid``.
_SPAWN_THEN_EXIT: Final = (
    "import os, subprocess, sys\n"
    f"proc = subprocess.Popen([sys.executable, '-c', {_LIVE_PROCESS!r}], start_new_session=True)\n"
    "print(proc.pid, flush=True)\n"
    "os._exit(0)\n"
)
_POLL_SECONDS: Final = 0.005
_CONVERGE_TIMEOUT_SECONDS: Final = 10.0


class AuthorityCalls:
    """The durable side effects one retirement attempt performed."""

    def __init__(self) -> None:
        """Start with no record cleared and no group recovered."""
        self.cleared: list[str] = []
        self.recovered: list[str] = []


def _parent_pid(pid: int) -> int | None:
    """Return a live process's parent PID, or ``None`` once it is gone.

    Args:
        pid: Process to inspect.

    Returns:
        The observed parent PID, or ``None`` when the process no longer exists.
    """
    try:
        fields = Path(f"/proc/{pid}/stat").read_bytes().rsplit(b")", 1)[-1].split()
    except (FileNotFoundError, ProcessLookupError):
        return None
    return int(fields[1])


def _converge(pid: int) -> None:
    """Terminate a reparented process, which can no longer be reaped.

    Args:
        pid: Process to terminate.
    """
    with suppress(ProcessLookupError):
        os.kill(pid, signal.SIGKILL)
    deadline = time.monotonic() + _CONVERGE_TIMEOUT_SECONDS
    while _parent_pid(pid) is not None and time.monotonic() < deadline:
        time.sleep(_POLL_SECONDS)


def _start_reparented() -> int:
    """Start a real live process that is provably not our direct child.

    The intermediate is spawned in its own session and reaped normally; the
    grandchild is orphaned on purpose. Every path out of this function that runs
    after the grandchild exists terminates it, so a failure here leaks no
    process.

    Returns:
        The PID of the live, reparented process.

    Raises:
        AssertionError: If the process never became a live non-child.
    """
    intermediate = subprocess.Popen(
        [sys.executable, "-c", _SPAWN_THEN_EXIT],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        env=lifecycle.worker_env(INCARNATION),
    )
    pid: int | None = None
    became_non_child = False
    try:
        pipe = intermediate.stdout
        assert pipe is not None
        pid = int(pipe.readline())
        if intermediate.stdout is not None:
            intermediate.stdout.close()
        intermediate.wait(timeout=_CONVERGE_TIMEOUT_SECONDS)
        deadline = time.monotonic() + _CONVERGE_TIMEOUT_SECONDS
        while time.monotonic() < deadline:
            ppid = _parent_pid(pid)
            is_child = ppid == os.getpid()
            if ppid is not None and not is_child and lifecycle.process_identity(pid) is not None:
                became_non_child = True
                break
            time.sleep(_POLL_SECONDS)
        if not became_non_child:
            msg = f"process {pid} never became a live process that is not our direct child"
            raise AssertionError(msg)
        return pid
    finally:
        # The grandchild is deliberately un-reapable, so every path that can
        # fail after its PID is known must terminate it here.
        if pid is not None and not became_non_child:
            _converge(pid)


def _retirement_daemon(monkeypatch: pytest.MonkeyPatch) -> tuple[SupervisorDaemon, AuthorityCalls]:
    """Build a daemon whose durable side effects are recorded, not performed.

    Args:
        monkeypatch: Patcher for the durable side effects.

    Returns:
        The daemon and the record of what it durably did.
    """
    created = supervisor.SupervisorDaemon(
        supervisor.Settings(stop_grace_seconds=0.5, identity_timeout_seconds=10.0)
    )
    calls = AuthorityCalls()

    def clear(_self: SupervisorDaemon, token: str) -> bool:
        calls.cleared.append(token)
        return True

    def recover(token: str) -> None:
        calls.recovered.append(token)

    monkeypatch.setattr(type(created), "_clear_worker_authority", clear)
    monkeypatch.setattr(supervisor, "recover_owned_groups", recover)
    monkeypatch.setattr(worker_mod, "drain_sentinel_matches", lambda _token: False)
    return created, calls


@pytest.fixture
def reparented_record() -> object:
    """Yield a real, live published-worker record that is not our child.

    The whole body after the spawn is inside the cleanup ``try``, so every
    failure path terminates the reparented process instead of leaking it.

    Yields:
        The published record naming the live reparented process.
    """
    pid = _start_reparented()
    try:
        identity = lifecycle.process_identity(pid)
        assert identity is not None
        record = authority.WorkerRecord(
            token=INCARNATION,
            commit=COMMIT,
            pid=identity.pid,
            pgid=identity.pgid,
            sid=identity.sid,
            start_time_ticks=identity.start_time_ticks,
            worker_id="reparented",
        )
        published_meta = supervisor.SupervisorDaemon._db_worker_meta(record)
        assert lifecycle.worker_alive(published_meta), (
            "the reproduction must be a live process matching every exact identity field"
        )
        yield record
    finally:
        _converge(pid)


def test_live_reparented_published_worker_is_held_not_signalled(
    reparented_record: authority.WorkerRecord, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Retiring a live non-child worker must not signal it or clear its record.

    The process really is alive and really is not a direct child, so an
    identity-only retirement kills it. The authority requires proof of
    parentage, so the daemon must hold instead.
    """
    record = reparented_record
    daemon, calls = _retirement_daemon(monkeypatch)
    assert _parent_pid(record.pid) != os.getpid()

    retired = daemon._retire_db_worker(record)

    assert lifecycle.process_identity(record.pid) is not None, (
        f"published worker pid {record.pid} was signalled and killed: a live process that is "
        "not our direct child must never be retired"
    )
    assert retired is False, "retirement of a live reparented worker must not be claimed"
    assert calls.cleared == []
    assert calls.recovered == []
    assert "not proven to be our direct child" in (daemon._message or "")


def test_dead_published_worker_still_owes_recovery_and_clearing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A dead published worker is retired, not held.

    A dead process proves nothing about parentage, so the direct-child gate must
    not block the ordinary retirement that lets a replacement start.
    """
    daemon, calls = _retirement_daemon(monkeypatch)
    absent = 2**22 - 1
    record = authority.WorkerRecord(
        token=INCARNATION,
        commit=COMMIT,
        pid=absent,
        pgid=absent,
        sid=absent,
        start_time_ticks=1,
        worker_id="dead",
    )

    assert daemon._retire_db_worker(record) is True
    assert calls.cleared == [INCARNATION]
    assert calls.recovered == [INCARNATION]
