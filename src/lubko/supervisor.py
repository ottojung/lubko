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
  through the durable protocol in :mod:`lubko.supervise`, so planned
  replacements, candidate handoffs, confirmation, and rollback never race the
  supervisor;
- a process-level advisory ``flock`` on the supervisor state's lock file is
  held for the entire daemon lifetime before anything writes durable state or
  touches worker lifecycle: two near-simultaneous ``lubko-supervisor`` starts
  resolve to exactly one owner and the loser exits fail-closed, while the
  kernel releases the lock at the owner's death (graceful, crash, or SIGKILL)
  so a later supervisor can always take ownership afterwards;
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
from lubko import worker as worker_mod
from lubko._exact_signal import open_pidfd as _open_unresolved_pidfd
from lubko._exact_signal import pidfd_send_signal as _signal_pinned_unresolved
from lubko._pg import psycopg
from lubko.config import load_database_config
from lubko.durable import remove_durable
from lubko.health import (
    interpret_worker_health,
    prune_old_incarnation_artifacts,
    publish_current_surfaces,
    read_worker_health,
    read_worker_health_by_incarnation,
    worker_health_payload,
)
from lubko.state import rollback_state_path
from lubko.supervise import (
    INTENT_RUN,
    MODE_RUN,
    SCHEMA_VERSION,
    LastExit,
    SupervisorStatus,
    UnresolvedChild,
    WorkerChild,
    acquire_supervisor_lock,
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
#: Bounded per-step timeout for preparing the migrated commit's CLI
#: environment during cold-migration completion.
COLD_MIGRATION_CLI_TIMEOUT_SECONDS: Final = cli.DEFAULT_BUILD_TIMEOUT_SECONDS
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


def _identity_is_private_session(identity: ProcessIdentity) -> bool:
    """Return whether an identity proves the process leads a private session.

    This is the exact invariant ``_wait_for_identity`` requires: the process
    must be both its own session leader and its own process group leader, so
    signalling that group can never reach any unrelated process.

    Args:
        identity: Observed live identity of a process.

    Returns:
        ``True`` when group authority over the identity is safe to grant.
    """
    return identity.pgid == identity.pid and identity.sid == identity.pid


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


def _runtime_dir(commit: str | None) -> str:
    """Return the commit-addressed sealed runtime path for metadata/probes.

    Ordinary worker identity and probe bookkeeping never depend on a mutable
    checkout: the recorded repo value and probe working directory are the
    exact per-commit runtime root, so a supervisor restart or crash probe
    stays functional even when the source checkout is deleted or modified.

    Args:
        commit: Exact commit whose runtime is used, or ``None``.

    Returns:
        The sealed runtime directory, or ``""`` when no commit is known.
    """
    return str(cli.cli_commit_dir(commit)) if commit else ""


def _child_to_meta(child: WorkerChild, repo: str) -> WorkerMeta:
    """Build maintained-worker metadata describing a supervisor child.

    Args:
        child: The exact worker child identity.
        repo: Runtime root recorded in the metadata (never the mutable checkout).

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
        log_path=str(lifecycle.worker_log_path(child.token)),
        started_at=child.spawned_at,
        stopped_at=None,
    )


class OwnedGroupRecoveryError(Exception):
    """Raised when owned command-group recovery could not be completed.

    This is a durable *blocking* obligation, not a recoverable warning: when the
    retired worker left command groups alive and they cannot be reaped because
    the database configuration is missing, the database is unreachable, or the
    recovery query fails, the supervisor must not clear the retired child or
    spawn a replacement. Callers let this propagate so the next daemon tick
    retries the same exact orphan rather than handing off sole-consumer
    authority alongside stale side-effecting process groups.
    """


def recover_owned_groups(incarnation: str) -> None:
    """Recover any command group still owned by a retired worker incarnation.

    After a worker is stopped or force-killed, any command process group it
    owned must be terminated and reaped by its exact persisted process-group
    id (``state.process_pgid``), never by process-name matching or a broad
    kill. This is a durable blocking obligation: a missing database
    configuration, an unreachable database, a recovery query failure, or a
    verified-ours group that survives the recovery pass raises
    :class:`OwnedGroupRecoveryError` so the caller preserves the retired
    child and does not spawn a replacement alongside a still-live
    side-effecting group. Only exact identities are ever signalled, and the
    recovery pass proves each target is genuinely dead before it is treated
    as reclaimed.

    Args:
        incarnation: The retired worker's lifecycle token (incarnation).

    Raises:
        OwnedGroupRecoveryError: If the recovery could not be completed, or
            if a verified-ours group survived the recovery pass.
    """
    if not incarnation:
        return
    try:
        database = load_database_config()
    except (OSError, ValueError) as exc:
        msg = f"cannot load database config to recover owned groups for {incarnation}"
        raise OwnedGroupRecoveryError(msg) from exc
    try:
        conn = psycopg.connect(
            database.conninfo(),
            connect_timeout=5,
        )
        conn.autocommit = True
    except (psycopg.Error, OSError) as exc:
        msg = f"cannot connect to recover owned groups for {incarnation}"
        raise OwnedGroupRecoveryError(msg) from exc
    try:
        result = worker_mod.recover_owned_job_groups(
            conn, incarnation, worker_mod.DEFAULT_CANCEL_GRACE_SECONDS
        )
    except psycopg.Error as exc:
        msg = f"error recovering owned groups for incarnation {incarnation}"
        raise OwnedGroupRecoveryError(msg) from exc
    finally:
        with suppress(Exception):
            conn.close()
    if result.surviving:
        msg = (
            f"owned command group(s) {result.surviving} still alive after "
            f"recovery for incarnation {incarnation}; holding without "
            "clearing authority or spawning a replacement"
        )
        raise OwnedGroupRecoveryError(msg)
    if result.unresolved:
        msg = (
            f"owned command group(s) {result.unresolved} could not be "
            f"identity-verified during recovery for incarnation {incarnation}; "
            "holding without clearing authority or spawning a replacement"
        )
        raise OwnedGroupRecoveryError(msg)
    if result.reaped:
        LOGGER.info(
            "recovered %d owned command group(s) for incarnation %s",
            len(result.reaped),
            incarnation,
        )


def normalize_cross_boot_state() -> None:
    """Neutralize monotonic-domain fields left over from a previous boot.

    ``next_attempt_at``, ``last_spawn_at``, and ``next_readiness_at`` are
    expressed in ``time.monotonic()`` coordinates, which are only meaningful
    within one boot. Durable state records the boot identifier it was written
    in; when it does not match the current boot (or cannot be proven to match,
    because the boot identity is unreadable), those values would otherwise be
    misread as deadlines in the new clock domain and could wedge the daemon
    behind a prior-boot uptime value.

    The restart counter, the last-exit record, and the recorded child identity
    stay durable: the counter keeps crash-loop history bounded, and a stale
    child is resolved by the ordinary reconciliation path, which classifies it
    dead (or reparented) by exact identity and either crash-handles or retires
    it — never duplicating a worker.
    """
    state = read_state()
    boot_id = supervise.current_boot_id()
    if state.boot_id == boot_id and boot_id is not None:
        return
    write_state(
        replace(
            state,
            boot_id=boot_id,
            next_attempt_at=None,
            last_spawn_at=None,
            next_readiness_at=None,
            ready=False,
        )
    )
    if state.boot_id is not None:
        LOGGER.info(
            "durable supervisor state predates this boot; reset monotonic "
            "backoff/readiness deadlines for commit %s",
            state.commit,
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
        self._ownership_fd: int | None = None
        self._start_time_ticks: int = 0

    def run(self) -> None:
        """Run the supervisor loop until a shutdown signal arrives.

        The process-level ownership lock is acquired before anything writes
        durable supervisor state or touches worker lifecycle, so a second
        concurrent ``lubko-supervisor`` exits fail-closed here; the lock is
        held for the whole run and released only on shutdown, and the kernel
        revokes it automatically if this process ever dies without a clean
        shutdown, so a later supervisor can always take ownership afterwards.
        The daemon never exits on its own: an unexpected worker exit is
        recorded and backed off, and the intended worker is restored.
        """
        LOGGER.info("lubko supervisor starting (pid %d)", os.getpid())
        self._acquire_ownership()
        try:
            self._write_pidfile()
            self._invalidate_stale_status()
            normalize_cross_boot_state()
            self._install_signal_handlers()
            self._write_status("starting")
            while not self._stopping:
                try:
                    self.reconcile(time.monotonic())
                except Exception:
                    LOGGER.exception("supervisor tick failed; continuing")
                self._write_status()
                time.sleep(self.settings.poll_interval_seconds)
            self._shutdown()
        finally:
            self._release_ownership()

    # ------------------------------------------------------------------
    # Tick / decision
    # ------------------------------------------------------------------

    def reconcile(self, now: float) -> None:
        """Run one deterministic supervisor reconciliation cycle.

        Derives the intended action from durable mission/desired state,
        handles crash recovery, retires stale children, and ensures the
        correct worker is running.  Called once per poll interval from
        :meth:`run`.
        """
        self._message = None
        desired = read_desired()
        state = read_state()
        if state.ownership_hold_malformed:
            # Materialize the hold so a later rewrite cannot turn authority
            # corruption into apparent worker absence. Clearing it is an
            # explicit operator repair, never an automatic recovery decision.
            write_state(state)
            self._message = (
                "the durable supervisor worker-ownership state is malformed or unreadable; "
                "holding without starting a worker until it is repaired"
            )
            LOGGER.error("%s", self._message)
            return
        action, commit = self._derive_action(state)
        if (
            desired is not None
            and action == "run"
            and commit == desired.commit
            and desired.generation > state.applied_generation
        ):
            self._apply_desired(desired)
            return
        if state.child is not None and not self._child_alive(state):
            child_meta = _child_to_meta(state.child, _runtime_dir(state.commit))
            if lifecycle.worker_alive(child_meta):
                LOGGER.info(
                    "worker pid=%d alive but reparented (not our direct child); "
                    "proceeding to exact retirement",
                    state.child.pid,
                )
            elif state.intent != INTENT_RUN:
                self._clear_child(now)
            else:
                self._handle_crash(state, now)
                if self._in_backoff(now):
                    return
        else:
            self._maybe_reset_backoff(state, now)
        if action == "hold":
            self._ensure_held()
            return
        if self._in_backoff(now):
            return
        if commit is None:
            self._message = "run action without an exact commit; holding"
            return
        self._ensure_worker(commit)
        self._record_mission_progress(commit)
        self._probe_readiness(now)
        self._complete_cold_migration()

    def _complete_cold_migration(self) -> None:
        """Converge CLI/deployctl authority onto a proven migrated commit.

        ``lubko-deploy migrate`` writes one atomically durable desired intent
        that both publishes the exact target commit and carries the
        ``migration`` flag, so no crash can separate the two. This is the only
        completion path for that flag:

        - while the migrated worker is not yet proven queue-ready for the
          exact migration generation/commit pair, nothing happens: the
          maintained CLIs stay fail-closed on the previous confirmed
          authority;
        - once readiness is proven, the sealed CLI environment is prepared,
          the maintained pointer converges to the target commit, the
          superseded terminal mission record is removed so deployctl
          authority follows the actually maintained worker, and finally the
          flag itself is cleared under the generation lock.

        The whole decision-and-mutation sequence runs under the same
        deployment lock every deployctl writer holds, so a concurrent newer
        checkout/confirm can never publish a mission between the re-read and
        the CLI/rollback mutations: it either completed before this critical
        section (and strictly outranks the migration) or afterwards (and its
        authority survives untouched). Lock order stays deployment lock
        before generation lock. On lock contention the completion is simply
        retried on a later tick.

        Every step is idempotent and ordered so a crash between steps leaves
        either the old coherent authority or retries the same convergence on
        the next tick; the flag is cleared last, so an interrupted run is
        always resumed rather than skipped.
        """
        desired = read_desired()
        if desired is None or not desired.migration:
            return
        try:
            with lifecycle.deploy_lock(DEFAULT_LOCK_TIMEOUT_SECONDS):
                self._complete_cold_migration_locked(desired)
        except lifecycle.LockTimeoutError:
            LOGGER.warning("cold-migration completion deferred: deployment lock is held")
            self._message = "cold-migration completion deferred; deployment in progress"

    def _complete_cold_migration_locked(self, desired: supervise.SupervisorDesired) -> None:
        """Perform cold-migration convergence while holding the deployment lock.

        All authority inputs are re-read inside the critical section so the
        decision is serialized against deployctl writers.

        Args:
            desired: The migration intent observed before locking; re-read
                and revalidated while locked.
        """
        current_desired = read_desired()
        if (
            current_desired is None
            or not current_desired.migration
            or current_desired.generation != desired.generation
            or current_desired.commit != desired.commit
        ):
            return
        try:
            mission = deployctl.read_rollback_state()
        except deployctl.DeployCtlError:
            mission = None
        if mission is not None and mission.generation > desired.generation:
            # A strictly newer supervised-deployment mission supersedes the
            # migration; its own confirmation path owns authority now and its
            # record must survive untouched.
            supervise.clear_migration_flag(desired.generation)
            lifecycle.append_deploy_log(
                f"cold migration to {desired.commit} superseded by newer mission "
                f"generation {mission.generation}"
            )
            return
        state = read_state()
        if (
            not state.ready
            or state.applied_generation < desired.generation
            or state.commit != desired.commit
        ):
            return
        try:
            cli.build_cli_root(
                Path(desired.repo),
                desired.commit,
                desired.uv_path,
                COLD_MIGRATION_CLI_TIMEOUT_SECONDS,
            )
        except cli.CliError as exc:
            self._message = f"cold-migration CLI environment failed: {exc}"
            LOGGER.warning("cold-migration CLI environment could not be prepared: %s", exc)
            return
        if cli.current_commit() != desired.commit:
            try:
                cli.set_current(desired.commit)
            except cli.CliError as exc:
                self._message = f"cold-migration CLI activation failed: {exc}"
                LOGGER.warning("cold-migration CLI activation failed: %s", exc)
                return
            lifecycle.append_deploy_log(f"cold migration activated CLI commit {desired.commit}")
        remove_durable(rollback_state_path())
        supervise.clear_migration_flag(desired.generation)
        lifecycle.append_deploy_log(
            f"cold migration complete: deployment authority converged to commit {desired.commit}"
        )
        LOGGER.info("cold migration converged deployment authority to commit %s", desired.commit)

    def _record_mission_progress(self, commit: str) -> None:
        """Advance the applied generation once a mission candidate is running.

        A pending deployment mission is an active intent even though it never
        travels through ``desired.json``: once its exact candidate commit is
        the live worker, the mission generation is recorded as applied so a
        waiting deployctl observes convergence and a supervisor restart
        reconstructs deterministically.

        Args:
            commit: The candidate commit that is now the maintained worker.
        """
        desired = read_desired()
        desired_gen = desired.generation if desired is not None else 0
        try:
            mission = deployctl.read_rollback_state()
        except deployctl.DeployCtlError:
            return
        if (
            mission is None
            or mission.status != deployctl.STATUS_PENDING
            or mission.commit != commit
            or mission.generation < desired_gen
        ):
            return
        current = read_state()
        if (
            current.child is not None
            and current.commit == commit
            and mission.generation > current.applied_generation
            and self._child_alive(current)
        ):
            write_state(replace(current, applied_generation=mission.generation))

    def _derive_action(self, state: supervise.SupervisorState) -> tuple[str, str | None]:
        """Decide the intended worker from durable mission/desired generations.

        Only the newest intent may choose a worker commit. A supervised mission
        and the desired run intent share one monotonic generation space, and
        precedence is purely generation-based:

        - a corrupt/unreadable mission fails closed into a hold;
        - a mission older than the desired intent is stale history and cannot
          override the desired commit, whatever its status;
        - a newer pending mission is the active candidate intent and is run;
        - a pending mission at the desired generation only runs when it selects
          the same commit, otherwise the contradiction holds;
        - a terminal mission at the desired generation or newer is an
          unsettled/incomplete settlement and holds: terminal status alone
          never chooses a commit (this removes the stale-terminal override).

        Args:
            state: Current daemon state.

        Returns:
            An ``(action, commit)`` pair where ``action`` is ``run`` or
            ``hold``.
        """
        desired = read_desired()
        desired_gen = desired.generation if desired is not None else 0
        desired_commit = desired.commit if desired is not None else None
        try:
            mission = deployctl.read_rollback_state()
        except deployctl.DeployCtlError:
            self._message = "corrupt supervised-deployment state; holding without a worker"
            LOGGER.exception("corrupt supervised-deployment state; holding without a worker")
            return "hold", None
        if mission is None:
            return self._derive_without_mission(state, desired_commit)
        return self._derive_with_mission(mission, desired_gen, desired_commit)

    @staticmethod
    def _derive_without_mission(
        state: supervise.SupervisorState,
        desired_commit: str | None,
    ) -> tuple[str, str | None]:
        """Derive the intended worker when no supervised mission exists.

        Only a live intent may choose a worker commit. Without a mission, the
        desired run intent is the sole live intent; a stale state/meta record
        alone never selects a worker (fail closed). A legacy metadata record is
        therefore never trusted to launch a worker here — the explicit
        ``lubko-deploy migrate`` command establishes the durable desired intent
        for pre-supervisor state.

        Args:
            state: Current daemon state (kept for callers' consistency).
            desired_commit: Exact commit the latest desired intent selects.

        Returns:
            An ``(action, commit)`` pair.
        """
        del state
        if desired_commit is not None:
            return "run", desired_commit
        return "hold", None

    def _derive_with_mission(
        self,
        mission: deployctl.RollbackState,
        desired_gen: int,
        desired_commit: str | None,
    ) -> tuple[str, str | None]:
        """Derive the intended worker with generation-based mission precedence.

        Args:
            mission: Current supervised-deployment mission.
            desired_gen: Generation of the latest desired intent.
            desired_commit: Commit the latest desired intent selects.

        Returns:
            An ``(action, commit)`` pair.
        """
        action: str = "hold"
        commit: str | None = None
        if mission.generation < desired_gen:
            if desired_commit is not None:
                action, commit = "run", desired_commit
        elif mission.generation > desired_gen:
            if mission.status == deployctl.STATUS_PENDING:
                action, commit = "run", mission.commit
            else:
                self._unsettled_terminal_message(mission)
        elif mission.status == deployctl.STATUS_PENDING:
            if desired_commit is not None and mission.commit == desired_commit:
                action, commit = "run", mission.commit
            else:
                self._contradiction_message(mission)
        else:
            self._unsettled_terminal_message(mission)
        return action, commit

    def _unsettled_terminal_message(self, mission: deployctl.RollbackState) -> None:
        """Record the diagnostic for an unsettled terminal mission.

        Args:
            mission: The terminal mission that never received a newer settle.
        """
        self._message = (
            "terminal supervised-deployment state at generation "
            f"{mission.generation} has no newer settled intent; holding without a worker"
        )
        LOGGER.error(
            "terminal supervised mission at generation %d is unsettled; holding",
            mission.generation,
        )

    def _contradiction_message(self, mission: deployctl.RollbackState) -> None:
        """Record the diagnostic for a same-generation mission contradiction.

        Args:
            mission: The pending mission contradicting the desired intent.
        """
        self._message = "contradictory same-generation supervised intent; holding without a worker"
        LOGGER.error(
            "pending mission at generation %d contradicts the desired intent; holding",
            mission.generation,
        )

    # ------------------------------------------------------------------
    # Explicit desired intent
    # ------------------------------------------------------------------

    def _apply_desired(self, desired: supervise.SupervisorDesired) -> None:
        """Apply one explicit run intent from the deployment CLIs.

        A ``restart`` intent force-replaces the current child with a fresh
        process from the same commit-addressed runtime. A plain run intent for
        an already-running commit is a durable settlement (confirmation or
        rollback convergence): it only records the newer generation and never
        disturbs the live worker, so settlement can never kill the very worker
        that is executing the confirmation protocol.

        Args:
            desired: The intent to apply.
        """
        if not desired.commit:
            self._message = "refusing a run intent without an exact commit"
            LOGGER.error("refusing a run intent without an exact commit")
            return
        state = read_state()
        already_running = (
            not desired.restart
            and state.commit == desired.commit
            and state.child is not None
            and self._child_alive(state)
        )
        if not already_running:
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
        if not already_running:
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
        if not self._resolve_unresolved_child():
            return
        state = read_state()
        if state.child is not None and self._child_alive(state) and state.commit == commit:
            return
        if state.child is not None and not self._retire_child():
            now = time.monotonic()
            write_state(
                replace(
                    read_state(),
                    next_attempt_at=now + self._backoff_seconds(read_state().restart_count),
                )
            )
            self._message = (
                f"could not stop recorded worker pid {state.child.pid}; "
                "holding without starting a worker"
            )
            LOGGER.error(
                "could not stop recorded worker pid %d; holding",
                state.child.pid,
            )
            return
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
                    next_backoff = now + self._backoff_seconds(read_state().restart_count)
                    write_state(replace(read_state(), next_attempt_at=next_backoff))
                    self._message = (
                        f"could not stop the recorded maintained worker pid {meta.pid}; "
                        "holding without starting a worker"
                    )
                    LOGGER.error(
                        "could not stop the recorded maintained worker pid %d; holding",
                        meta.pid,
                    )
                    return
                # The adopted worker may have left command groups alive. If it
                # proved a clean drain the groups are already gone and no
                # emergency recovery is required; otherwise recovery is a durable
                # blocking obligation — a DB/config/SQL failure must not let us
                # spawn a replacement alongside stale groups.
                if not (meta.token and worker_mod.drain_sentinel_matches(meta.token)):
                    recover_owned_groups(meta.token or "")
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
        meta = replace(_child_to_meta(child, _runtime_dir(commit)), git_commit=commit)
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

        The candidate's health snapshot is read by its exact incarnation
        (not through the stable ``health.json`` symlink, which may still
        point to the old confirmed worker).  The snapshot's PID and
        start-time ticks are cross-checked against the recorded child
        identity so a stale candidate snapshot cannot masquerade as the
        confirmed worker.

        The stable health/log symlinks are published only after the queue
        roundtrip succeeds and the identity cross-check passes — so a
        retiring old worker or a stale candidate can never move the
        stable read surface.

        Args:
            now: Monotonic time.
        """
        state = read_state()
        child = state.child
        if child is None or not self._child_alive(state):
            return
        if state.ready or (state.next_readiness_at is not None and now < state.next_readiness_at):
            return
        probe_cwd = _runtime_dir(state.commit)
        ready, reason = self._check_readiness(child, probe_cwd)
        if not ready:
            self._record_not_ready(state, now, child.pid, reason)
            return
        try:
            publish_current_surfaces(child.token)
        except OSError:
            self._record_not_ready(state, now, child.pid, "stable symlink publication failed")
            return
        write_state(replace(state, ready=True, next_readiness_at=None))
        LOGGER.info("worker child pid=%d proven to consume the queue", child.pid)
        lifecycle.append_deploy_log(
            f"supervisor verified worker pid={child.pid} consumes the queue"
        )
        prune_old_incarnation_artifacts(child.token)

    def _check_readiness(
        self,
        child: supervise.WorkerChild,
        probe_cwd: str,
    ) -> tuple[bool, str]:
        """Verify queue consumption and health identity cross-check.

        Args:
            child: The worker child identity.
            probe_cwd: Working directory for the queue probe.

        Returns:
            A ``(ready, reason)`` tuple.
        """
        if not lifecycle.verify_worker_consumes_queue(
            child.worker_id, probe_cwd, child.pid, self.settings.probe_timeout_seconds
        ):
            return False, "queue consumption not proven"
        snapshot = read_worker_health_by_incarnation(child.token)
        if snapshot is None:
            return False, f"no health snapshot for incarnation {child.token}"
        if snapshot.pid != child.pid:
            return False, f"snapshot PID {snapshot.pid} != child PID {child.pid}"
        if snapshot.start_time_ticks != child.start_time_ticks:
            return (
                False,
                f"snapshot ticks {snapshot.start_time_ticks} != child {child.start_time_ticks}",
            )
        if snapshot.worker_incarnation != child.token:
            inc, tok = snapshot.worker_incarnation, child.token
            return (False, f"snapshot incarnation {inc!r} != child token {tok!r}")
        eff = interpret_worker_health(snapshot)
        return (True, "ok") if eff.live else (False, f"worker health not live: {eff.reason}")

    def _record_not_ready(
        self,
        state: supervise.SupervisorState,
        now: float,
        child_pid: int,
        reason: str,
    ) -> None:
        """Record a not-ready probe result and schedule a retry.

        Args:
            state: Current daemon state.
            now: Monotonic time.
            child_pid: PID of the worker child.
            reason: Why readiness was not confirmed.
        """
        write_state(
            replace(
                state,
                ready=False,
                next_readiness_at=now + self.settings.readiness_interval_seconds,
            )
        )
        LOGGER.warning(
            "worker child pid=%d not ready: %s; will retry",
            child_pid,
            reason,
        )

    def _retire_child(self) -> bool:
        """Stop the current worker child by exact identity.

        ``stop_worker`` performs full identity verification (PID, start-time
        ticks, PGID, SID, lifecycle token) before signalling so a reparented
        or reused PID is never mis-signalled.

        When the stop cannot be confirmed the durable child identity is
        preserved so the next daemon tick can retry the same exact orphan
        rather than losing track of it and spawning a duplicate consumer.

        Returns:
            ``True`` when the child was successfully retired (or was already
            dead); ``False`` when the stop could not be confirmed.
        """
        state = read_state()
        child = state.child
        if child is None:
            return True
        meta = _child_to_meta(child, _runtime_dir(state.commit))
        stopped = lifecycle.stop_worker(meta, self.settings.stop_grace_seconds)
        # A live exact worker that stop_worker could not authorize or stop (e.g.
        # a wrong/absent lifecycle token, or PID reuse) must not be signalled
        # and must not be reported retired. Hold immediately: do NOT attempt
        # owned-group recovery (it is keyed by the same token we could not
        # authorize) and do NOT clear the child identity or hand off
        # sole-consumer authority. The next daemon tick retries the same exact
        # orphan rather than spawning a duplicate consumer.
        if not stopped:
            LOGGER.error(
                "could not confirm stop of worker pid %d; preserving child identity for retry",
                child.pid,
            )
            return False
        # The worker is confirmed dead. A wedged worker that was force-killed can
        # leave command process groups alive. If the worker proved a clean drain
        # the groups are already gone and no emergency recovery is required (and
        # no database round-trip that could fail). Otherwise recovery is a
        # durable blocking obligation: a DB/config/SQL failure or a surviving/
        # unresolved group raises, which preserves the retired child and prevents
        # spawning a replacement alongside stale groups.
        if not (child.token and worker_mod.drain_sentinel_matches(child.token)):
            recover_owned_groups(child.token)
        if self.proc is not None:
            with suppress(Exception):
                self.proc.wait(timeout=self.settings.stop_grace_seconds)
            self.proc = None
        write_state(replace(state, child=None))
        LOGGER.info("retired worker child pid=%d", child.pid)
        return True

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
        meta = _child_to_meta(child, _runtime_dir(state.commit))
        if not lifecycle.worker_alive(meta):
            return False
        return _process_ppid(child.pid) == os.getpid()

    # ------------------------------------------------------------------
    # Crash handling and backoff
    # ------------------------------------------------------------------

    def _handle_crash(self, state: supervise.SupervisorState, now: float) -> None:
        """Record an unexpected worker exit and schedule a bounded retry.

        Before the dead child identity is cleared, any command process group
        still owned by the exact crashed worker incarnation is recovered with
        the same blocking machinery used for planned retirement. If recovery
        cannot be completed (missing database configuration, unreachable
        database, query failure, or a surviving/unresolved group) the crash is
        failed closed: the durable child identity is preserved as the
        blocking recovery obligation, no replacement is spawned, and a later
        reconciliation tick retries the same exact incarnation.

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
        child = state.child
        if child is not None and not (
            child.token and worker_mod.drain_sentinel_matches(child.token)
        ):
            try:
                recover_owned_groups(child.token)
            except OwnedGroupRecoveryError:
                LOGGER.exception(
                    "owned-group recovery for crashed incarnation %s is incomplete; "
                    "preserving the dead child identity and withholding any replacement",
                    child.token,
                )
                self._message = (
                    f"owned-group recovery for crashed incarnation {child.token} "
                    "is incomplete; holding without a replacement worker"
                )
                write_state(
                    replace(
                        state,
                        last_exit=LastExit(returncode=returncode, at=time.time()),
                        intent=INTENT_RUN,
                        next_attempt_at=now + self._backoff_seconds(state.restart_count + 1),
                        ready=False,
                        next_readiness_at=None,
                    )
                )
                return
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
        """Reset crash backoff only after the exact worker stayed healthy.

        Elapsed time since ``last_spawn_at`` is stability evidence only while
        the same recorded child is still our live direct child. A dead child
        must never earn crash forgiveness merely by remaining absent until the
        stability window elapses; otherwise an active exponential-backoff
        deadline can be shortened and a persistent crash loop never reaches
        its configured cap.

        Args:
            state: Current daemon state.
            now: Monotonic time.
        """
        if state.restart_count == 0 and state.next_attempt_at is None:
            return
        if state.child is None or not self._child_alive(state):
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
        """Spawn the worker for ``commit`` as a direct child from the sealed runtime.

        Args:
            commit: Exact commit whose sealed per-commit runtime runs the worker.

        Returns:
            The exact child identity, or ``None`` if the worker could not be
            started or did not establish a session.
        """
        if not cli.runtime_is_usable(commit):
            self._message = (
                f"maintained runtime for commit {commit} is missing, corrupt, incomplete, "
                "or not sealed; holding without a worker"
            )
            LOGGER.error(
                "maintained runtime for commit %s is missing/corrupt/unsealed; refusing to "
                "launch a worker",
                commit,
            )
            return None
        executable = cli.cli_entry_executable(commit, "lubko-worker")
        if executable is None:
            self._message = (
                f"maintained runtime for commit {commit} is incomplete; holding without a worker"
            )
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
        try:
            proc = subprocess.Popen(
                [str(executable)],
                cwd=str(cli.cli_commit_dir(commit)),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
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
            return self._settle_unproven_spawn(commit, proc, token, worker_id)
        return WorkerChild(
            pid=identity.pid,
            pgid=identity.pgid,
            sid=identity.sid,
            start_time_ticks=identity.start_time_ticks,
            token=token,
            worker_id=worker_id,
            spawned_at=time.time(),
        )

    def _settle_unproven_spawn(
        self,
        commit: str,
        proc: subprocess.Popen[bytes],
        token: str,
        worker_id: str,
    ) -> WorkerChild | None:
        """Settle a spawn whose exact identity was never established.

        ``_wait_for_identity`` returns ``None`` for two distinct conditions:
        an already-exited child (an ordinary retryable spawn failure) and a
        child that is still alive when the identity deadline expires. A live
        child is never forgotten: while it is still this supervisor's direct
        ``Popen`` child it is converged with exact single-PID signals and its
        exit is positively proven by reaping. If convergence cannot be proven,
        ownership is durably retained so no later reconciliation (including
        after a supervisor restart) can start a second maintained worker
        alongside the unresolved first child. Group authority is only ever
        granted when the observed identity satisfies the same private
        session/group invariant ``_wait_for_identity`` requires; otherwise a
        distinct unresolved-child hold is recorded, which carries **no**
        group/session authority and can only ever be converged by exact
        single-PID signals guarded by start-time ticks.

        Args:
            commit: Exact commit whose worker spawn failed.
            proc: The direct ``Popen`` handle of the spawned child.
            token: Lifecycle token handed to the spawned child.
            worker_id: Worker identity handed to the spawned child.

        Returns:
            Always ``None``: a failed spawn never yields a usable child.
        """
        if proc.poll() is not None:
            LOGGER.error("worker for commit %s exited before establishing its identity", commit)
            self.proc = None
            return None
        LOGGER.error(
            "worker pid %d for commit %s is live without an acceptable identity; converging it",
            proc.pid,
            commit,
        )
        hold_persisted = False
        while True:
            if self._converge_direct_child(proc):
                # Converged while a crash-safe hold was already on disk:
                # clear it so an ordinary retryable failure can proceed.
                if hold_persisted:
                    write_state(replace(read_state(), unresolved_child=None))
                self.proc = None
                return None
            observed = self._await_observable_identity(proc)
            if observed is None and proc.poll() is not None:
                # The child exited during the hold: an ordinary retryable failure.
                if hold_persisted:
                    write_state(replace(read_state(), unresolved_child=None))
                self.proc = None
                return None
            if observed is not None and _identity_is_private_session(observed):
                self._record_proven_private_child(observed, token, worker_id)
                break
            if observed is None:
                hold_persisted = self._persist_unobservable_hold(
                    proc, token, already_persisted=hold_persisted
                )
                time.sleep(IDENTITY_POLL_SECONDS)
                continue
            self._record_shared_group_hold(proc, observed, token)
            break
        state = read_state()
        if state.unresolved_child is not None:
            self._message = (
                f"worker pid {proc.pid} for commit {commit} could not be converged after its "
                "identity timed out; holding without group-signallable authority or a "
                "replacement worker"
            )
            LOGGER.error("%s", self._message)
        else:
            LOGGER.info(
                "worker pid %d for commit %s proved a private session before any replacement; "
                "recorded as the maintained child",
                proc.pid,
                commit,
            )
        return None

    @staticmethod
    def _record_proven_private_child(
        observed: ProcessIdentity,
        token: str,
        worker_id: str,
    ) -> None:
        """Record a child whose observed identity proved the private invariant.

        The identity satisfies the exact private session/group invariant, so
        recording it as the maintained child grants safe group authority for
        later retirement. Any earlier authority-free hold for this same child
        is upgraded away: it must never outlive the proven-safe record.

        Args:
            observed: Observed exact identity of the live direct child.
            token: Lifecycle token handed to the spawned child.
            worker_id: Worker identity handed to the spawned child.
        """
        write_state(
            replace(
                read_state(),
                child=WorkerChild(
                    pid=observed.pid,
                    pgid=observed.pgid,
                    sid=observed.sid,
                    start_time_ticks=observed.start_time_ticks,
                    token=token,
                    worker_id=worker_id,
                    spawned_at=time.time(),
                ),
                unresolved_child=None,
            )
        )

    @staticmethod
    def _persist_unobservable_hold(
        proc: subprocess.Popen[bytes],
        token: str,
        *,
        already_persisted: bool,
    ) -> bool:
        """Persist an authority-free hold for a live but unobservable child.

        The hold is written immediately, before any further waiting, so a
        supervisor crash/restart during convergence can never forget the live
        spawned child. ``start_time_ticks`` is deliberately ``None``: nothing
        was observable to prove, so no signalling may ever be authorized from
        this record; it only blocks replacement until the PID is positively
        gone (or a safe identity appears).

        Args:
            proc: The direct ``Popen`` handle of the spawned child.
            token: Lifecycle token handed to the spawned child.
            already_persisted: Whether the hold is already on disk.

        Returns:
            ``True`` once the hold is durably recorded.
        """
        if already_persisted:
            return True
        write_state(
            replace(
                read_state(),
                unresolved_child=UnresolvedChild(
                    pid=proc.pid,
                    start_time_ticks=None,
                    token=token,
                    spawned_at=time.time(),
                ),
            )
        )
        LOGGER.error(
            "identity of live worker pid %d remains unobservable; "
            "persisting an authority-free hold before further convergence",
            proc.pid,
        )
        return True

    @staticmethod
    def _record_shared_group_hold(
        proc: subprocess.Popen[bytes],
        observed: ProcessIdentity,
        token: str,
    ) -> None:
        """Record an authority-free hold for a live child with a shared group.

        The observed identity does NOT satisfy the private session/group
        invariant. Persisting it as an ordinary child would let lifecycle code
        signal a possibly shared group, so fail closed with an authority-free
        hold instead (ticks recorded so the exact instance — never a recycled
        PID — can later be signalled by exact PID).

        Args:
            proc: The direct ``Popen`` handle of the spawned child.
            observed: Observed (unsafe) exact identity of the live child.
            token: Lifecycle token handed to the spawned child.
        """
        write_state(
            replace(
                read_state(),
                unresolved_child=UnresolvedChild(
                    pid=proc.pid,
                    start_time_ticks=observed.start_time_ticks,
                    token=token,
                    spawned_at=time.time(),
                ),
            )
        )

    def _owned_hold_child(self, hold: UnresolvedChild) -> subprocess.Popen[bytes] | None:
        """Return our direct ``Popen`` child when the kernel proves the hold is its exit.

        Ownership is proven by kernel parentage rather than bookkeeping alone:
        only a live or unreaped direct child of this very process can be
        waited for, so a successful ``waitpid(WNOHANG)`` proves the recorded
        PID is exactly our own still-unreaped child — and simultaneously reaps
        it when it has already exited — while ``ECHILD`` proves the PID does
        not belong to any unreaped child of ours. A foreign or arbitrary
        zombie is therefore never claimed, and because the kernel cannot
        recycle the numeric PID of an unreaped child, no recycled identity can
        slip through either. When start-time ticks were recorded they must
        additionally still match the live instance.

        Relying on ``self.proc`` as the wait target rests on a structural
        daemon invariant: while an unresolved hold survives on disk, no
        replacement worker can be spawned, because the only writer of a fresh
        ``self.proc`` (``_spawn_worker``) is reachable solely through
        ``_ensure_worker``, which first runs ``_resolve_unresolved_child()``
        and refuses to proceed until the durable hold has been cleared; after
        a supervisor restart ``self.proc`` is ``None``. The ``waitpid`` parentage
        proof above remains sound independently of that invariant.

        Args:
            hold: The recorded authority-free hold.

        Returns:
            The owned direct child handle, or ``None`` when the hold does not
            correspond to a still-owned, kernel-proven direct child.
        """
        proc = self.proc
        if proc is None or proc.pid != hold.pid:
            return None
        try:
            os.waitpid(proc.pid, os.WNOHANG)
        except ChildProcessError:
            # The kernel itself proves this PID is not an unreaped direct
            # child of this process: nothing owned can be converged here.
            return None
        if (
            hold.start_time_ticks is not None
            and proc_start_ticks(hold.pid) != hold.start_time_ticks
        ):
            return None
        return proc

    def _unresolved_alive(self, hold: UnresolvedChild) -> bool:
        """Return whether the exact unresolved child instance is still alive.

        Args:
            hold: The recorded authority-free hold.

        Returns:
            ``True`` only when a live process matches the recorded PID *and*
            start time ticks (when ticks were observed), so a recycled PID can
            never extend the hold. When the hold corresponds to our own
            kernel-proven direct child, an exited child — including its
            unreaped zombie state — has already been positively reaped by the
            ownership proof itself and is gone instead of looking alive
            forever.
        """
        child = self._owned_hold_child(hold)
        if child is not None:
            return child.poll() is None
        if hold.start_time_ticks is None:
            return proc_start_ticks(hold.pid) is not None
        return proc_start_ticks(hold.pid) == hold.start_time_ticks

    def _await_unresolved_exit(self, hold: UnresolvedChild) -> bool:
        """Wait until the exact unresolved child instance provably exits.

        Args:
            hold: The recorded authority-free hold.

        Returns:
            ``True`` when the exact instance is gone within the stop grace.
        """
        deadline = time.monotonic() + self.settings.stop_grace_seconds
        while self._unresolved_alive(hold):
            if time.monotonic() >= deadline:
                return False
            time.sleep(IDENTITY_POLL_SECONDS)
        return True

    def _converge_unresolved(self, hold: UnresolvedChild) -> bool:
        """Converge an unresolved direct child without any group signalling.

        The recorded PID is first pinned with a pidfd, which kernel-pins the
        exact process instance even if the numeric PID is recycled afterwards.
        Only when the pinned process still provably matches the recorded
        start-time ticks are signals delivered — through ``pidfd_send_signal``
        on that same pinned descriptor for both TERM and KILL escalation, so a
        recycled numeric identity can never be signalled at any point.

        When pinning or exact proof is unavailable, nothing is signalled and
        the hold is preserved (fail closed). When start-time ticks were never
        observable, no signal can be authorized and the previous no-signal
        behavior applies.

        Args:
            hold: The recorded authority-free hold.

        Returns:
            ``True`` when the exact instance is positively gone afterwards.
        """
        if not self._unresolved_alive(hold):
            return True
        ticks = hold.start_time_ticks
        if ticks is None:
            # Ticks were never observable: no signal can be authorized. The
            # hold resolves only when the PID itself is provably gone; a live
            # PID keeps the hold (and blocks any replacement) no matter how
            # long it takes.
            return self._await_unresolved_exit(hold)
        try:
            pidfd = _open_unresolved_pidfd(hold.pid)
        except (OSError, AttributeError):
            LOGGER.debug("unresolved worker pid %d could not be pinned", hold.pid)
            return False
        try:
            if proc_start_ticks(hold.pid) != ticks:
                LOGGER.debug(
                    "pinned process %d no longer matches its recorded start ticks",
                    hold.pid,
                )
                return False
            with suppress(OSError, AttributeError):
                _signal_pinned_unresolved(pidfd, signal.SIGTERM)
            if self._await_unresolved_exit(hold):
                return True
            with suppress(OSError, AttributeError):
                _signal_pinned_unresolved(pidfd, signal.SIGKILL)
            return self._await_unresolved_exit(hold)
        finally:
            with suppress(OSError):
                os.close(pidfd)

    def _resolve_unresolved_child(self) -> bool:
        """Resolve any durable unresolved-child hold before further decisions.

        Returns:
            ``True`` when no blocking hold remains.
        """
        state = read_state()
        if state.ownership_hold_malformed or state.unresolved_hold_malformed:
            # A present-but-malformed durable hold was found on disk. The
            # blocking obligation survives its own shape corruption: a possibly
            # live unresolved spawned child may still exist, so no replacement
            # worker can ever be authorized until the state is repaired by an
            # operator. This is deliberately not self-healing.
            now = time.monotonic()
            write_state(
                replace(read_state(), next_attempt_at=now + self.settings.poll_interval_seconds)
            )
            self._message = (
                "the durable unresolved-worker hold is malformed; failing closed without "
                "starting any worker until the supervisor state is repaired"
            )
            LOGGER.error("%s", self._message)
            return False
        hold = state.unresolved_child
        if hold is None:
            return True
        if not self._converge_unresolved(hold):
            now = time.monotonic()
            write_state(
                replace(read_state(), next_attempt_at=now + self.settings.poll_interval_seconds)
            )
            self._message = (
                f"unresolved worker pid {hold.pid} cannot be converged by exact identity; "
                "holding without starting a worker"
            )
            LOGGER.error("%s", self._message)
            return False
        write_state(replace(read_state(), unresolved_child=None))
        LOGGER.info("resolved prior unresolved worker pid=%d", hold.pid)
        return True

    def _converge_direct_child(self, proc: subprocess.Popen[bytes]) -> bool:
        """Terminate and reap our own direct ``Popen`` child exactly.

        Signals go only to the exact PID of the process this supervisor just
        spawned — never to a process group — so no other process can ever be
        signalled. The child is positively reaped before success is reported.

        Args:
            proc: The direct ``Popen`` handle of the spawned child.

        Returns:
            ``True`` when the child provably exited and was reaped.
        """
        with suppress(OSError):
            proc.terminate()
        reaped = False
        try:
            proc.wait(timeout=self.settings.stop_grace_seconds)
        except subprocess.TimeoutExpired:
            pass
        else:
            reaped = True
        if reaped:
            return True
        with suppress(OSError):
            proc.kill()
        force_reaped = True
        try:
            proc.wait(timeout=self.settings.stop_grace_seconds)
        except subprocess.TimeoutExpired:
            force_reaped = False
        if not force_reaped:
            LOGGER.error(
                "could not prove exit of live worker pid %d; failing closed",
                proc.pid,
            )
            return False
        return True

    def _await_observable_identity(self, proc: subprocess.Popen[bytes]) -> ProcessIdentity | None:
        """Observe the identity of a live direct child for a fail-closed hold.

        Args:
            proc: The direct ``Popen`` handle of the spawned child.

        Returns:
            The observed exact identity of the still-live child, or ``None``
            when the child exited (or could not be observed) meanwhile.
        """
        deadline = time.monotonic() + self.settings.stop_grace_seconds
        while True:
            identity = lifecycle.process_identity(proc.pid)
            if identity is not None:
                return identity
            if proc.poll() is not None:
                return None
            if time.monotonic() >= deadline:
                return None
            time.sleep(IDENTITY_POLL_SECONDS)

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

    def _acquire_ownership(self) -> None:
        """Fail closed if another live supervisor already owns the role.

        The process-level flock is taken before anything writes durable
        supervisor state or touches worker lifecycle, so a second concurrent
        ``lubko-supervisor`` exits here: it never reaches the pidfile write,
        the status write, a ``_tick`` decision, or a worker spawn, and it can
        never oscillate the durable generations or duplicate a consumer.  The
        descriptor stays open for the daemon's whole lifetime and is closed
        only when the daemon shuts down; because the kernel drops the flock
        when this process exits — even under SIGKILL or a crash — a later
        supervisor can always take ownership afterwards.

        Raises:
            SystemExit: If another live supervisor holds the ownership lock.
        """
        try:
            acquired = acquire_supervisor_lock()
        except OSError:
            LOGGER.exception(
                "another lubko supervisor is already running; refusing to start a second one"
            )
            raise SystemExit(1) from None
        self._ownership_fd = acquired
        LOGGER.info("supervisor ownership lock acquired (fd %d)", acquired)

    def _release_ownership(self) -> None:
        """Release the process-level ownership lock held for the lifetime."""
        if self._ownership_fd is not None:
            with suppress(Exception):
                os.close(self._ownership_fd)
            self._ownership_fd = None

    def _shutdown(self) -> None:
        """Stop the worker child gracefully and persist a clean state."""
        LOGGER.info("supervisor shutting down")
        if read_state().child is not None:
            self._retire_child()
        if not self._resolve_unresolved_child():
            # The hold survives shutdown on purpose: an unresolved spawned
            # child must never be abandoned to make room for a replacement.
            hold = read_state().unresolved_child
            LOGGER.error(
                "shutting down with an unresolved worker pid=%s still held",
                hold.pid if hold is not None else None,
            )
        remove_durable(supervise.supervisor_pid_path())
        self._write_status("stopped")
        LOGGER.info("supervisor stopped")

    @staticmethod
    def _invalidate_stale_status() -> None:
        """Remove any stale status snapshot left by a previous incarnation.

        After the ownership lock and pidfile are established, the old status
        may still carry ``ready=true`` for a dead supervisor.  Removing the
        file ensures that ``read_status()`` returns ``None`` until this
        incarnation publishes its own fresh snapshot.
        """
        with suppress(OSError):
            supervise.status_path().unlink(missing_ok=True)

    def _write_pidfile(self) -> None:
        """Record our exact identity, refusing to double-run a live daemon.

        The process-level ownership lock acquired in :meth:`run` already held
        off any second flock-aware daemon, so the read/check/write here cannot
        race a concurrent start.  The recorded-pid liveness check stays as
        defense in depth against a legacy flock-less daemon instance.

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
        self._start_time_ticks = proc_start_ticks(os.getpid()) or 0
        write_supervisor_pid(os.getpid(), self._start_time_ticks)

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
        worker_health = worker_health_payload(read_worker_health())
        write_status(
            SupervisorStatus(
                schema_version=SCHEMA_VERSION,
                supervisor_pid=os.getpid(),
                supervisor_start_time_ticks=self._start_time_ticks,
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
                worker_health=worker_health,
            )
        )


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
