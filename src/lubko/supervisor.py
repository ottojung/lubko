"""External process supervisor that owns and restarts the maintained Lubko worker.

``lubko-supervisor`` is the small, stable control component outside the worker
process whose job is to ensure that exactly one intended maintained Lubko
worker is running and to restore a verified last-known-good worker after an
unexpected worker exit.  It is deliberately independent of the worker and of
the Lubko job queue: it never needs a queue roundtrip to notice or repair
worker death.

The daemon is designed to be the container's long-lived main process.  The
production container currently runs ``tini-static -- sleep infinity``; the
supported startup contract replaces the ``sleep infinity`` child of Tini with
the supervisor:

    tini-static -- lubko-supervisor

On every container start Tini launches the supervisor, which reconstructs the
intended maintained worker deterministically from durable state under
``$XDG_STATE_HOME/lubko/supervisor/`` and from the existing deployment
authorities (``worker/meta.json`` and ``worker/rollback.json``).  The exact
runtime requirement (switching the container command from ``sleep infinity``
to ``lubko-supervisor``) is configured in the container image, which is outside
this repository; all repository-side pieces required by that contract are
implemented here.

Ownership model
---------------

The supervisor is the stable authority that actually starts, stops, and
restarts worker processes:

- it spawns the worker as its **direct child** from the immutable per-commit
  maintained environment (``$XDG_STATE_HOME/lubko/cli/<commit>/``), never from
  a mutable working tree, so a crash never launches arbitrary checkout
  contents and ordinary restarts never run repository validation or mutate
  Git;
- it never uses ``pkill``/``killall``/argv matching/process-name discovery:
  every stop and liveness check uses the exact recorded process identity
  (PID, process group, session, start time, lifecycle token), so a recycled
  PID can never be signalled;
- ``lubko-deploy`` and ``lubko-deploy-ctl`` change the desired commit only
  through the durable protocol in :mod:`lubko.supervise`, so planned stops,
  candidate handoffs, confirmation, and rollback never race the supervisor;
- during a supervised deployment the supervisor recognises the durable pending
  mission (``worker/rollback.json``) and holds: it never resurrects the
  intentionally retiring previous worker, and on container restart during a
  pending mission it resolves the abandoned mission before restoring a worker,
  so exactly one maintained consumer exists after every crash/deploy/restart.

Crash-loop protection is bounded and exponential; every worker exit is logged
and exposed through the machine-readable status file.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import secrets
import signal
import socket
import subprocess
import sys
import time
from contextlib import suppress
from dataclasses import dataclass, replace
from pathlib import Path
from typing import TYPE_CHECKING, Final

from lubko import cli, deployctl, lifecycle, supervise
from lubko.supervise import (
    INTENT_RUN,
    MODE_RUN,
    MODE_STOPPED,
    SCHEMA_VERSION,
    LastExit,
    SupervisorStatus,
    WorkerChild,
    proc_start_ticks,
    read_desired,
    read_state,
    read_status,
    read_supervisor_pid,
    supervisor_log_path,
    write_state,
    write_status,
    write_supervisor_pid,
)
from lubko.toolchain import UvResolutionError, resolve_uv

if TYPE_CHECKING:
    from lubko.lifecycle import ProcessIdentity, WorkerMeta

LOGGER: Final = logging.getLogger(__name__)

DEFAULT_POLL_SECONDS: Final = 1.0
DEFAULT_BACKOFF_BASE_SECONDS: Final = 2.0
DEFAULT_BACKOFF_MAX_SECONDS: Final = 120.0
DEFAULT_STABLE_WINDOW_SECONDS: Final = supervise.DEFAULT_STABLE_WINDOW_SECONDS
DEFAULT_STOP_GRACE_SECONDS: Final = 5.0
DEFAULT_IDENTITY_TIMEOUT_SECONDS: Final = 5.0
DEFAULT_POSTGRES_TIMEOUT_SECONDS: Final = 2.0
DEFAULT_REQUEST_TIMEOUT_SECONDS: Final = supervise.DEFAULT_REQUEST_TIMEOUT_SECONDS
DEFAULT_LOCK_TIMEOUT_SECONDS: Final = 5.0
DEFAULT_PROBE_TIMEOUT_SECONDS: Final = 15.0
DEFAULT_READINESS_INTERVAL_SECONDS: Final = 5.0
IDENTITY_POLL_SECONDS: Final = 0.02
DB_CHECK_INTERVAL_SECONDS: Final = 15.0
STAT_PPID_FIELD_INDEX: Final = 1
STAT_PPID_MIN_FIELDS: Final = 2


@dataclass(frozen=True, slots=True)
class Settings:
    """Runtime settings for the supervisor daemon."""

    poll_interval_seconds: float = DEFAULT_POLL_SECONDS
    backoff_base_seconds: float = DEFAULT_BACKOFF_BASE_SECONDS
    backoff_max_seconds: float = DEFAULT_BACKOFF_MAX_SECONDS
    stable_window_seconds: float = DEFAULT_STABLE_WINDOW_SECONDS
    stop_grace_seconds: float = DEFAULT_STOP_GRACE_SECONDS
    identity_timeout_seconds: float = DEFAULT_IDENTITY_TIMEOUT_SECONDS
    postgres_timeout_seconds: float = DEFAULT_POSTGRES_TIMEOUT_SECONDS
    lock_timeout_seconds: float = DEFAULT_LOCK_TIMEOUT_SECONDS
    probe_timeout_seconds: float = DEFAULT_PROBE_TIMEOUT_SECONDS
    readiness_interval_seconds: float = DEFAULT_READINESS_INTERVAL_SECONDS

    def __post_init__(self) -> None:
        """Validate the settings so a broken daemon never loops wildly.

        Raises:
            ValueError: If any value is unusable.
        """
        if self.poll_interval_seconds <= 0:
            msg = "LUBKO_SUPERVISOR_POLL_SECONDS must be positive"
            raise ValueError(msg)
        if self.backoff_base_seconds <= 0 or self.backoff_max_seconds < self.backoff_base_seconds:
            msg = "LUBKO_SUPERVISOR backoff settings are invalid"
            raise ValueError(msg)
        if self.stable_window_seconds <= 0:
            msg = "LUBKO_SUPERVISOR_STABLE_WINDOW_SECONDS must be positive"
            raise ValueError(msg)
        if self.stop_grace_seconds <= 0:
            msg = "LUBKO_SUPERVISOR_STOP_GRACE_SECONDS must be positive"
            raise ValueError(msg)
        if self.identity_timeout_seconds <= 0:
            msg = "LUBKO_SUPERVISOR_IDENTITY_TIMEOUT_SECONDS must be positive"
            raise ValueError(msg)
        if self.probe_timeout_seconds <= 0 or self.readiness_interval_seconds <= 0:
            msg = "LUBKO_SUPERVISOR readiness probe settings must be positive"
            raise ValueError(msg)

    @classmethod
    def from_environment(cls) -> Settings:
        """Load supervisor settings from environment variables.

        Returns:
            Settings derived from the process environment.
        """
        return cls(
            poll_interval_seconds=float(
                os.getenv("LUBKO_SUPERVISOR_POLL_SECONDS", str(DEFAULT_POLL_SECONDS))
            ),
            backoff_base_seconds=float(
                os.getenv(
                    "LUBKO_SUPERVISOR_BACKOFF_BASE_SECONDS",
                    str(DEFAULT_BACKOFF_BASE_SECONDS),
                )
            ),
            backoff_max_seconds=float(
                os.getenv(
                    "LUBKO_SUPERVISOR_BACKOFF_MAX_SECONDS",
                    str(DEFAULT_BACKOFF_MAX_SECONDS),
                )
            ),
            stable_window_seconds=float(
                os.getenv(
                    "LUBKO_SUPERVISOR_STABLE_WINDOW_SECONDS",
                    str(DEFAULT_STABLE_WINDOW_SECONDS),
                )
            ),
            stop_grace_seconds=float(
                os.getenv(
                    "LUBKO_SUPERVISOR_STOP_GRACE_SECONDS",
                    str(DEFAULT_STOP_GRACE_SECONDS),
                )
            ),
            identity_timeout_seconds=float(
                os.getenv(
                    "LUBKO_SUPERVISOR_IDENTITY_TIMEOUT_SECONDS",
                    str(DEFAULT_IDENTITY_TIMEOUT_SECONDS),
                )
            ),
            postgres_timeout_seconds=float(
                os.getenv(
                    "LUBKO_SUPERVISOR_POSTGRES_TIMEOUT_SECONDS",
                    str(DEFAULT_POSTGRES_TIMEOUT_SECONDS),
                )
            ),
            lock_timeout_seconds=float(
                os.getenv(
                    "LUBKO_SUPERVISOR_LOCK_TIMEOUT_SECONDS",
                    str(DEFAULT_LOCK_TIMEOUT_SECONDS),
                )
            ),
            probe_timeout_seconds=float(
                os.getenv(
                    "LUBKO_SUPERVISOR_PROBE_TIMEOUT_SECONDS",
                    str(DEFAULT_PROBE_TIMEOUT_SECONDS),
                )
            ),
            readiness_interval_seconds=float(
                os.getenv(
                    "LUBKO_SUPERVISOR_READINESS_INTERVAL_SECONDS",
                    str(DEFAULT_READINESS_INTERVAL_SECONDS),
                )
            ),
        )


def _process_ppid(pid: int) -> int | None:
    """Return the exact parent process ID of a live process.

    Args:
        pid: Process whose parent to inspect.

    Returns:
        The parent PID, or ``None`` when the process is gone or unreadable.
    """
    try:
        stat = (Path("/proc") / str(pid) / "stat").read_bytes()
    except OSError:
        return None
    close_paren = stat.rfind(b")")
    if close_paren == -1:
        return None
    fields = stat[close_paren + 2 :].split()
    if len(fields) < STAT_PPID_MIN_FIELDS:
        return None
    try:
        return int(fields[STAT_PPID_FIELD_INDEX])
    except ValueError:
        return None


def _child_to_meta(child: WorkerChild, repo: str) -> WorkerMeta:
    """Build maintained-worker metadata describing a supervisor child.

    Args:
        child: The exact worker child identity.
        repo: Maintained checkout recorded in the metadata.

    Returns:
        Running metadata for the child.
    """
    return lifecycle.WorkerMeta(
        schema_version=lifecycle.SCHEMA_VERSION,
        state=lifecycle.STATE_RUNNING,
        pid=child.pid,
        pgid=child.pgid,
        sid=child.sid,
        start_time_ticks=child.start_time_ticks,
        token=child.token,
        repo=repo,
        git_commit=None,
        worker_id=child.worker_id,
        log_path=str(lifecycle.worker_log_path()),
        started_at=child.spawned_at,
        stopped_at=None,
    )


class SupervisorDaemon:
    """One long-lived daemon that owns and restarts the maintained worker."""

    def __init__(self, settings: Settings) -> None:
        """Build a daemon with no worker child yet.

        Args:
            settings: Supervisor runtime settings.
        """
        self.settings = settings
        self.proc: subprocess.Popen[bytes] | None = None
        self._stopping = False
        self._started_at = time.time()
        self._next_db_check_at = 0.0
        self._message: str | None = None

    def run(self) -> None:
        """Run the supervisor loop until a shutdown signal arrives.

        The daemon never exits on its own: an unexpected worker exit is
        recorded and backed off, and the intended worker is restored.
        """
        LOGGER.info("lubko supervisor starting (pid %d)", os.getpid())
        self._write_pidfile()
        self._install_signal_handlers()
        self._write_status("starting")
        while not self._stopping:
            try:
                self._tick(time.monotonic())
            except Exception:
                LOGGER.exception("supervisor tick failed; continuing")
            self._write_status()
            time.sleep(self.settings.poll_interval_seconds)
        self._shutdown()

    # ------------------------------------------------------------------
    # Tick / decision
    # ------------------------------------------------------------------

    def _tick(self, now: float) -> None:
        """Run one supervision decision.

        Args:
            now: Monotonic time at the start of the turn.
        """
        desired = read_desired()
        state = read_state()
        if desired is not None and desired.generation > state.applied_generation:
            self._apply_desired(desired)
            return
        if state.child is not None and not self._child_alive(state):
            if state.intent != INTENT_RUN:
                self._clear_child(now)
            else:
                self._handle_crash(state, now)
                if self._in_backoff(now):
                    return
        else:
            self._maybe_reset_backoff(state, now)
        action, commit = self._derive_action(state)
        if action == "hold":
            self._ensure_held()
            return
        if action == "rollback":
            deployctl.resolve_abandoned_mission(self.settings.lock_timeout_seconds)
            return
        if self._in_backoff(now):
            return
        if commit is None:
            self._message = "run action without an exact commit; holding"
            return
        self._ensure_worker(commit)
        self._probe_readiness(now)

    def _derive_action(self, state: supervise.SupervisorState) -> tuple[str, str | None]:
        """Decide the intended worker state from durable authorities.

        A pending supervised mission always takes precedence over the daemon's
        own recorded mode: the candidate is the intended consumer and the
        daemon must hold.  Corrupt mission metadata fails closed into a hold so
        a worker is never started during an unknown handoff.

        Args:
            state: Current daemon state.

        Returns:
            An ``(action, commit)`` pair where ``action`` is ``run``, ``hold``,
            or ``rollback``.
        """
        try:
            rollback = deployctl.read_rollback_state()
        except deployctl.DeployCtlError:
            self._message = "corrupt supervised-deployment state; holding without a worker"
            LOGGER.exception("corrupt supervised-deployment state; holding without a worker")
            return "hold", None
        action: str
        commit: str | None
        if rollback is not None and rollback.status == deployctl.STATUS_PENDING:
            candidate_alive = lifecycle.worker_alive(rollback.new_meta)
            if candidate_alive and time.time() < rollback.deadline:
                action, commit = "hold", None
            else:
                action, commit = "rollback", None
        elif rollback is not None and rollback.status == deployctl.STATUS_CONFIRMED:
            action, commit = "run", rollback.commit
        elif rollback is not None and rollback.status == deployctl.STATUS_ROLLED_BACK:
            action, commit = "run", rollback.previous_commit
        elif state.mode == MODE_STOPPED:
            action, commit = "hold", None
        elif state.mode == MODE_RUN and state.commit is not None:
            action, commit = "run", state.commit
        else:
            meta = lifecycle.read_meta()
            if (
                meta is not None
                and meta.git_commit is not None
                and meta.state != lifecycle.STATE_STOPPED
            ):
                action, commit = "run", meta.git_commit
            else:
                action, commit = "hold", None
        return action, commit

    # ------------------------------------------------------------------
    # Explicit desired intent
    # ------------------------------------------------------------------

    def _apply_desired(self, desired: supervise.SupervisorDesired) -> None:
        """Apply one explicit desired intent from the deployment CLIs.

        Args:
            desired: The intent to apply.
        """
        if desired.mode == MODE_STOPPED:
            current = read_state()
            was_live = current.child is not None and self._child_alive(current)
            self._retire_child()
            state = replace(
                read_state(),
                applied_generation=desired.generation,
                mode=MODE_STOPPED,
                commit=None,
                intent=INTENT_RUN,
                restart_count=0,
                next_attempt_at=None,
                ready=False,
                next_readiness_at=None,
            )
            write_state(state)
            if was_live:
                meta = lifecycle.read_meta()
                if meta is not None:
                    lifecycle.write_meta(
                        replace(meta, state=lifecycle.STATE_STOPPED, stopped_at=time.time())
                    )
                lifecycle.append_deploy_log("supervisor stopped the maintained worker")
            LOGGER.info("applied supervisor stop (generation %d)", desired.generation)
            return
        if desired.commit is None:
            self._message = "refusing a run intent without an exact commit"
            LOGGER.error("refusing a run intent without an exact commit")
            return
        self._retire_child()
        state = replace(
            read_state(),
            applied_generation=desired.generation,
            mode=MODE_RUN,
            commit=desired.commit,
            intent=INTENT_RUN,
            restart_count=0,
            next_attempt_at=None,
            ready=False,
            next_readiness_at=None,
        )
        write_state(state)
        self._ensure_worker(desired.commit)
        LOGGER.info("applied supervisor run intent (generation %d)", desired.generation)

    # ------------------------------------------------------------------
    # Worker ownership
    # ------------------------------------------------------------------

    def _ensure_worker(self, commit: str) -> None:
        """Ensure exactly one worker child owned by us runs ``commit``.

        A live maintained worker recorded in metadata that is not provably our
        direct child is first stopped by its exact identity, regardless of
        which commit it runs.  If that exact stop cannot be confirmed, the
        daemon holds and backs off instead of spawning: starting a second
        consumer is never acceptable.

        Args:
            commit: Exact commit the worker must run.
        """
        state = read_state()
        if state.child is not None and self._child_alive(state) and state.commit == commit:
            return
        if state.child is not None:
            self._retire_child()
        state = read_state()
        meta = lifecycle.read_meta()
        if meta is not None and lifecycle.worker_alive(meta):
            is_our_child = (
                state.child is not None
                and meta.pid == state.child.pid
                and meta.start_time_ticks == state.child.start_time_ticks
            )
            if not is_our_child:
                LOGGER.info(
                    "adopting ownership: stopping the recorded maintained worker pid %d by exact "
                    "identity before starting our own child",
                    meta.pid,
                )
                if not lifecycle.stop_worker(meta, self.settings.stop_grace_seconds):
                    now = time.monotonic()
                    write_state(
                        replace(
                            read_state(),
                            next_attempt_at=now + self._backoff_seconds(read_state().restart_count),
                        )
                    )
                    self._message = (
                        f"could not stop the recorded maintained worker pid {meta.pid}; "
                        "holding without starting a worker"
                    )
                    LOGGER.error(
                        "could not stop the recorded maintained worker pid %d; holding",
                        meta.pid,
                    )
                    return
        child = self._spawn_worker(commit)
        now = time.monotonic()
        if child is None:
            state = replace(
                read_state(),
                mode=MODE_RUN,
                commit=commit,
                intent=INTENT_RUN,
                ready=False,
                next_readiness_at=None,
                next_attempt_at=now + self._backoff_seconds(read_state().restart_count),
            )
            write_state(state)
            LOGGER.error("could not start a worker for commit %s; backing off", commit)
            return
        state = replace(
            read_state(),
            mode=MODE_RUN,
            commit=commit,
            child=child,
            intent=INTENT_RUN,
            next_attempt_at=None,
            last_spawn_at=now,
            ready=False,
            next_readiness_at=now + self.settings.readiness_interval_seconds,
        )
        write_state(state)
        repo = _desired_repo()
        meta = replace(_child_to_meta(child, repo), git_commit=commit)
        lifecycle.write_meta(meta)
        lifecycle.append_deploy_log(
            f"supervisor started worker pid={child.pid} commit={commit} "
            f"incarnation={child.worker_id}"
        )
        LOGGER.info("started worker child pid=%d for commit %s", child.pid, commit)

    def _probe_readiness(self, now: float) -> None:
        """Prove the exact worker child consumes the queue before declaring ready.

        Readiness is not PID liveness or database connectivity: it is a real
        queue roundtrip bound to the exact supervisor child.  The probe job is
        cancelled, awaited terminal, and removed, so a failed probe never
        leaves a row or process behind and never shortens availability.

        Args:
            now: Monotonic time.
        """
        state = read_state()
        child = state.child
        if child is None or not self._child_alive(state):
            return
        if state.ready:
            return
        if state.next_readiness_at is not None and now < state.next_readiness_at:
            return
        probe_cwd = _desired_repo() or str(cli.cli_commit_dir(state.commit or ""))
        if lifecycle.verify_worker_consumes_queue(
            child.worker_id, probe_cwd, child.pid, self.settings.probe_timeout_seconds
        ):
            write_state(replace(state, ready=True, next_readiness_at=None))
            LOGGER.info("worker child pid=%d proven to consume the queue", child.pid)
            lifecycle.append_deploy_log(
                f"supervisor verified worker pid={child.pid} consumes the queue"
            )
        else:
            write_state(
                replace(
                    state,
                    ready=False,
                    next_readiness_at=now + self.settings.readiness_interval_seconds,
                )
            )
            LOGGER.warning(
                "worker child pid=%d not yet proven to consume the queue; will retry",
                child.pid,
            )

    def _retire_child(self) -> None:
        """Stop the current worker child by exact identity and forget it.

        The child is never restarted after retirement: the caller has decided
        the transition was intentional.
        """
        state = read_state()
        child = state.child
        if child is None:
            return
        if self._child_alive(state):
            meta = _child_to_meta(child, _desired_repo())
            lifecycle.stop_worker(meta, self.settings.stop_grace_seconds)
        if self.proc is not None:
            with suppress(Exception):
                self.proc.wait(timeout=self.settings.stop_grace_seconds)
            self.proc = None
        write_state(replace(state, child=None))
        LOGGER.info("retired worker child pid=%d", child.pid)

    def _clear_child(self, _now: float) -> None:
        """Forget a child that exited after an intentional retirement.

        Args:
            _now: Monotonic time (unused).
        """
        self.proc = None
        write_state(replace(read_state(), child=None))

    @staticmethod
    def _child_alive(state: supervise.SupervisorState) -> bool:
        """Return whether the recorded worker child is really ours and alive.

        A worker is only ever trusted when it is our **direct child**: the
        exact identity (PID, group, session, start time, token) must match a
        live process whose parent is this supervisor process.  After a
        supervisor restart an orphaned worker that was reparented to the
        container's PID 1 is therefore never mistaken for our child, and the
        takeover path stops it by exact identity before spawning a fresh one.

        Args:
            state: Daemon state holding the child identity.

        Returns:
            ``True`` when the exact identity matches a live direct child.
        """
        child = state.child
        if child is None:
            return False
        meta = _child_to_meta(child, _desired_repo())
        if not lifecycle.worker_alive(meta):
            return False
        return _process_ppid(child.pid) == os.getpid()

    # ------------------------------------------------------------------
    # Crash handling and backoff
    # ------------------------------------------------------------------

    def _handle_crash(self, state: supervise.SupervisorState, now: float) -> None:
        """Record an unexpected worker exit and schedule a bounded retry.

        Args:
            state: Daemon state holding the dead child.
            now: Monotonic time of the crash.
        """
        returncode = self.proc.poll() if self.proc is not None else None
        LOGGER.error(
            "maintained worker pid=%d exited unexpectedly with returncode %r; scheduling restart",
            state.child.pid if state.child is not None else None,
            returncode,
        )
        restart_count = state.restart_count + 1
        backoff = self._backoff_seconds(restart_count)
        next_state = replace(
            state,
            restart_count=restart_count,
            next_attempt_at=now + backoff,
            last_exit=LastExit(returncode=returncode, at=time.time()),
            intent=INTENT_RUN,
            child=None,
        )
        write_state(next_state)
        self.proc = None
        lifecycle.append_deploy_log(
            f"supervisor detected unexpected worker exit pid="
            f"{state.child.pid if state.child is not None else 'unknown'} "
            f"returncode={returncode} restart={restart_count}"
        )

    def _backoff_seconds(self, restart_count: int) -> float:
        """Return the exponential backoff delay for a restart count.

        Args:
            restart_count: Number of consecutive unexpected exits.

        Returns:
            A bounded exponential delay in seconds.
        """
        exponent = max(0, restart_count - 1)
        delay = self.settings.backoff_base_seconds * (2.0**exponent)
        return min(delay, self.settings.backoff_max_seconds)

    @staticmethod
    def _in_backoff(now: float) -> bool:
        """Return whether the daemon is currently backing off a restart.

        Args:
            now: Monotonic time.

        Returns:
            ``True`` while a scheduled retry is still in the future.
        """
        next_attempt = read_state().next_attempt_at
        return next_attempt is not None and now < next_attempt

    def _maybe_reset_backoff(self, state: supervise.SupervisorState, now: float) -> None:
        """Reset the restart counter once a worker has been stable.

        Args:
            state: Current daemon state.
            now: Monotonic time.
        """
        if state.restart_count == 0 and state.next_attempt_at is None:
            return
        last_spawn = state.last_spawn_at
        if last_spawn is None or now - last_spawn < self.settings.stable_window_seconds:
            return
        write_state(replace(state, restart_count=0, next_attempt_at=None, last_exit=None))
        LOGGER.info("worker is stable; resetting restart counter")

    # ------------------------------------------------------------------
    # Spawning
    # ------------------------------------------------------------------

    def _spawn_worker(self, commit: str) -> WorkerChild | None:
        """Spawn the worker for ``commit`` as a direct child from the immutable env.

        Args:
            commit: Exact commit whose immutable maintained environment runs
                the worker.

        Returns:
            The exact child identity, or ``None`` if the worker could not be
            started or did not establish a session.
        """
        executable = cli.cli_entry_executable(commit, "lubko-worker")
        if executable is None:
            LOGGER.error(
                "no maintained CLI environment for commit %s; refusing to launch an arbitrary "
                "worker",
                commit,
            )
            return None
        token = secrets.token_hex(16)
        env = lifecycle.worker_env(token)
        desired = read_desired()
        worker_id = (
            (desired.worker_id if desired is not None else None)
            or os.getenv("LUBKO_WORKER_ID")
            or socket.gethostname()
        )
        env["LUBKO_WORKER_ID"] = worker_id
        log_path = lifecycle.worker_log_path()
        log_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with log_path.open("ab") as log:
                proc = subprocess.Popen(
                    [str(executable)],
                    cwd=str(cli.cli_commit_dir(commit)),
                    stdin=subprocess.DEVNULL,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                    close_fds=True,
                    env=env,
                )
        except OSError:
            LOGGER.exception("could not start the worker for commit %s", commit)
            return None
        self.proc = proc
        identity = self._wait_for_identity(proc.pid)
        if identity is None:
            LOGGER.error("worker for commit %s exited before establishing its identity", commit)
            with suppress(Exception):
                proc.wait(timeout=self.settings.stop_grace_seconds)
            self.proc = None
            return None
        return WorkerChild(
            pid=identity.pid,
            pgid=identity.pgid,
            sid=identity.sid,
            start_time_ticks=identity.start_time_ticks,
            token=token,
            worker_id=worker_id,
            spawned_at=time.time(),
        )

    def _wait_for_identity(self, pid: int) -> ProcessIdentity | None:
        """Wait until a spawned worker establishes its own session and group.

        A process that never becomes a session/process-group leader is never
        accepted: on timeout the daemon fails rather than record an identity it
        cannot stop by exact group semantics.

        Args:
            pid: Process ID of the spawned worker.

        Returns:
            The exact leader identity, or ``None`` if the process died or never
            established its own session.
        """
        deadline = time.monotonic() + self.settings.identity_timeout_seconds
        while True:
            identity = lifecycle.process_identity(pid)
            if identity is not None and identity.pgid == pid and identity.sid == pid:
                return identity
            if self.proc is not None and self.proc.poll() is not None:
                return None
            if time.monotonic() >= deadline:
                return None
            time.sleep(IDENTITY_POLL_SECONDS)

    # ------------------------------------------------------------------
    # Holding
    # ------------------------------------------------------------------

    def _ensure_held(self) -> None:
        """Retire our worker child and hold while the intended state is idle.

        This covers an explicit stop, an unmanaged/stopped record, and a
        pending supervised mission where the candidate is the intended
        consumer.
        """
        if read_state().child is not None:
            self._retire_child()

    # ------------------------------------------------------------------
    # Signals, status, lifecycle
    # ------------------------------------------------------------------

    def _install_signal_handlers(self) -> None:
        """Install graceful-shutdown handlers for the container runtime.

        As the container's main process, the supervisor must turn Tini's
        ``SIGTERM`` into a graceful worker stop and a clean exit.
        """

        def _handle_shutdown(signum: int, _frame: object) -> None:
            del signum
            LOGGER.info("shutdown signal received; stopping the maintained worker")
            self._stopping = True

        signal.signal(signal.SIGTERM, _handle_shutdown)
        signal.signal(signal.SIGINT, _handle_shutdown)

    def _shutdown(self) -> None:
        """Stop the worker child gracefully and persist a clean state."""
        LOGGER.info("supervisor shutting down")
        if read_state().child is not None:
            self._retire_child()
        supervise.supervisor_pid_path().unlink(missing_ok=True)
        self._write_status("stopped")
        LOGGER.info("supervisor stopped")

    @staticmethod
    def _write_pidfile() -> None:
        """Record our exact identity, refusing to double-run a live daemon.

        Raises:
            SystemExit: If another live supervisor daemon is already running.
        """
        recorded = read_supervisor_pid()
        if recorded is not None and supervise.supervisor_running():
            msg = (
                f"another lubko supervisor is already running (pid {recorded[0]}); "
                "refusing to start a second one"
            )
            LOGGER.error(msg)
            raise SystemExit(1)
        write_supervisor_pid(os.getpid(), proc_start_ticks(os.getpid()) or 0)

    def _write_status(self, message: str | None = None) -> None:
        """Publish the machine-readable status snapshot.

        Args:
            message: Optional human-facing diagnostic.
        """
        state = read_state()
        now = time.monotonic()
        db_ready: bool | None = None
        if now >= self._next_db_check_at:
            try:
                db_ready = lifecycle.check_postgres(self.settings.postgres_timeout_seconds)
            except Exception:
                db_ready = None
            self._next_db_check_at = now + DB_CHECK_INTERVAL_SECONDS
        mission = None
        try:
            rollback = deployctl.read_rollback_state()
        except deployctl.DeployCtlError:
            rollback = None
            self._message = "corrupt supervised-deployment state; holding without a worker"
        if rollback is not None:
            mission = rollback.status
        write_status(
            SupervisorStatus(
                schema_version=SCHEMA_VERSION,
                supervisor_pid=os.getpid(),
                started_at=self._started_at,
                applied_generation=state.applied_generation,
                mode=state.mode,
                commit=state.commit,
                child=state.child,
                intent=state.intent,
                restart_count=state.restart_count,
                next_attempt_at=state.next_attempt_at,
                last_exit=state.last_exit,
                mission=mission,
                db_ready=db_ready,
                ready=state.ready if state.child is not None else None,
                message=self._message if message is None else message,
            )
        )


def _desired_repo() -> str:
    """Return the maintained checkout recorded in the latest desired intent.

    Returns:
        The recorded checkout path, or ``""`` when no intent exists.
    """
    desired = read_desired()
    return desired.repo if desired is not None else ""


def _status_cmd() -> int:
    """Print the machine-readable supervisor status and exit.

    Returns:
        A process exit code.
    """
    status = read_status()
    if status is None:
        sys.stdout.write("supervisor: not running\n")
        return 1
    sys.stdout.write(json.dumps(status.to_dict(), sort_keys=True, indent=2) + "\n")
    return 0


def _run_daemon(settings: Settings) -> int:
    """Run the supervisor daemon forever.

    Args:
        settings: Supervisor settings.

    Returns:
        A process exit code.
    """
    with suppress(KeyboardInterrupt):
        SupervisorDaemon(settings).run()
    return 0


def build_parser() -> argparse.ArgumentParser:
    """Build the ``lubko-supervisor`` command line parser.

    Returns:
        The configured parser.
    """
    parser = argparse.ArgumentParser(
        prog="lubko-supervisor",
        description=(
            "Externally supervise the maintained Lubko worker: own it as a direct child, "
            "restart it on unexpected exit with bounded backoff, and keep it aligned with the "
            "durable confirmed commit."
        ),
    )
    parser.add_argument(
        "--uv",
        default=None,
        help="uv executable (default: uv on PATH, then the recorded Lubko toolchain path)",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="print the machine-readable supervisor status and exit",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the ``lubko-supervisor`` command.

    Args:
        argv: Command line arguments, or ``None`` to use ``sys.argv``.

    Returns:
        A process exit code.
    """
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.status:
        return _status_cmd()
    with suppress(UvResolutionError):
        # The daemon itself never needs uv: it restores workers from immutable
        # per-commit environments. Refusing to start here would defeat crash
        # recovery on a machine where uv vanished from PATH.
        resolve_uv(args.uv)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logger = logging.getLogger("lubko.supervisor")
    with suppress(OSError):
        supervise.supervisor_dir().mkdir(parents=True, exist_ok=True)
        handler = logging.FileHandler(supervisor_log_path())
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
        logger.addHandler(handler)
    try:
        settings = Settings.from_environment()
    except ValueError:
        LOGGER.exception("invalid supervisor settings")
        return 1
    return _run_daemon(settings)


if __name__ == "__main__":
    raise SystemExit(main())
