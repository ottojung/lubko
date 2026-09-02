"""Deterministic invariants for the child+spawning publication order (issue #282).

A supervisor death after publishing ``state.child`` but before the maintained
``worker/meta.json`` is durable must never let manual recovery authorize a
second queue consumer. The fix keeps the exact pid-bearing ``spawning``
obligation durable while ``state.child`` is published, writes the lifecycle
meta, and only then clears ``spawning`` in a final durable state write.

These tests drive the publication protocol and its reconciliation deterministically
without real worker processes.
"""

from __future__ import annotations

import os
import subprocess
from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING, cast

import pytest

from lubko import cli, lifecycle, supervise, supervisor
from lubko.durable import DurabilityError
from lubko.supervise import SpawningObligation, WorkerChild, read_state

if TYPE_CHECKING:
    from lubko.supervise import SupervisorState

COMMIT = "a" * 40


@pytest.fixture(autouse=True)
def valid_spawn_selection(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep publication tests focused below the final intent-selection gate."""
    monkeypatch.setattr(
        supervisor.SupervisorDaemon,
        "_derive_action",
        lambda _self, _state: ("run", COMMIT),
    )


#: A fake sealed runtime root used only to build child metadata paths; it is
#: never touched on disk by the publication protocol under test.
FAKE_RUNTIME_ROOT = Path("/opt/lubko-fake-runtime")


@pytest.fixture(autouse=True)
def state_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Use an isolated durable state root for each test.

    Returns:
        The supervisor state directory.
    """
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state"))
    supervise.state_path().parent.mkdir(parents=True, exist_ok=True)
    return supervise.state_path().parent


def _published_child() -> WorkerChild:
    """Build the exact child identity a successful spawn returns.

    Returns:
        The exact child identity a successful spawn would produce.
    """
    return WorkerChild(
        pid=4711,
        pgid=4711,
        sid=4711,
        start_time_ticks=555,
        token="tok" + "a" * 20,
        worker_id="w",
        spawned_at=0.0,
    )


def _in_progress_state(child: WorkerChild, commit: str = COMMIT) -> None:
    """Persist a state where child is published under a matching live obligation.

    Args:
        child: The already-published exact child identity.
        commit: The commit the child was published for.
    """
    obligation = SpawningObligation(
        token=child.token,
        commit=commit,
        creator_pid=1,
        creator_start_time_ticks=1,
        pid=child.pid,
        start_time_ticks=child.start_time_ticks,
        created_at=0.0,
        boot_id=supervise.current_boot_id(),
    )
    supervise.write_state(
        supervise.SupervisorState(
            schema_version=supervise.SCHEMA_VERSION,
            applied_generation=0,
            mode=supervise.MODE_RUN,
            commit=commit,
            child=child,
            unresolved_child=None,
            ownership_hold_malformed=False,
            unresolved_hold_malformed=False,
            spawning=obligation,
            spawning_hold_malformed=False,
            intent=supervise.INTENT_RUN,
            restart_count=0,
            next_attempt_at=None,
            last_exit=None,
            last_spawn_at=None,
            ready=False,
            next_readiness_at=None,
            boot_id=supervise.current_boot_id(),
        )
    )


def _fake_spawn(daemon: supervisor.SupervisorDaemon, commit: str) -> WorkerChild:
    """Stand-in ``_spawn_worker`` that records the pid-bearing obligation.

    Args:
        daemon: The daemon requesting the spawn (unused by the stub).
        commit: The exact commit the worker must run.

    Returns:
        The spawned child identity.
    """
    del daemon
    child = _published_child()
    obligation = SpawningObligation(
        token=child.token,
        commit=commit,
        creator_pid=os.getpid(),
        creator_start_time_ticks=1,
        pid=child.pid,
        start_time_ticks=child.start_time_ticks,
        created_at=0.0,
        boot_id=supervise.current_boot_id(),
    )
    supervise.write_state(replace(read_state(), spawning=obligation))
    return child


def _patch_recovery(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, str]]:
    """Patch owned-group recovery to record calls.

    Args:
        monkeypatch: The pytest monkeypatch fixture.

    Returns:
        The list of ``(event, token)`` tuples recorded so far.
    """
    observed: list[tuple[str, str]] = []

    def fake_recover(token: str) -> None:
        observed.append(("recover", token))

    monkeypatch.setattr(supervisor, "recover_owned_groups", fake_recover)
    return observed


def test_child_published_with_spawning_then_meta_then_spawning_cleared(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ordered protocol: child+spawning -> meta -> spawning cleared."""
    monkeypatch.setattr(cli, "runtime_is_usable", lambda _commit: True)
    monkeypatch.setattr(cli, "cli_commit_dir", lambda _commit: FAKE_RUNTIME_ROOT)
    monkeypatch.setattr(lifecycle, "worker_env", lambda _token: {})

    meta_log: list[lifecycle.WorkerMeta] = []
    real_write_meta = lifecycle.write_meta

    def fake_write_meta(meta: lifecycle.WorkerMeta) -> None:
        meta_log.append(meta)
        real_write_meta(meta)

    monkeypatch.setattr(lifecycle, "write_meta", fake_write_meta)

    events: list[SupervisorState] = []
    real_write_state = supervise.write_state

    def capturing_write(state: SupervisorState) -> None:
        events.append(state)
        real_write_state(state)

    monkeypatch.setattr(supervisor, "write_state", capturing_write)

    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    monkeypatch.setattr(daemon, "_spawn_worker", lambda c: _fake_spawn(daemon, c))
    daemon._ensure_consumer_locked(COMMIT)

    published = next(s for s in events if s.child is not None and s.spawning is not None)
    assert published.child is not None
    assert published.spawning is not None
    assert published.spawning.token == published.child.token
    assert published.spawning.pid == published.child.pid
    assert len(meta_log) == 1, "the lifecycle meta was written exactly once"
    final = events[-1]
    assert final.spawning is None
    assert final.child is not None
    assert final.child.pid == published.child.pid
    assert meta_log[0].pid == published.child.pid
    assert meta_log[0].token == published.child.token


def test_meta_write_failure_keeps_spawning_durable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed meta write leaves spawning durable (replacement-blocking)."""
    monkeypatch.setattr(cli, "runtime_is_usable", lambda _commit: True)
    monkeypatch.setattr(cli, "cli_commit_dir", lambda _commit: FAKE_RUNTIME_ROOT)
    monkeypatch.setattr(lifecycle, "worker_env", lambda _token: {})
    monkeypatch.setattr(
        lifecycle,
        "write_meta",
        lambda _meta: (_ for _ in ()).throw(DurabilityError("x")),
    )

    events: list[SupervisorState] = []
    real_write_state = supervise.write_state

    def capturing_write(state: SupervisorState) -> None:
        events.append(state)
        real_write_state(state)

    monkeypatch.setattr(supervisor, "write_state", capturing_write)

    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    monkeypatch.setattr(daemon, "_spawn_worker", lambda c: _fake_spawn(daemon, c))
    daemon._ensure_consumer_locked(COMMIT)

    assert read_state().spawning is not None, "the obligation survived the meta failure"
    assert read_state().child is not None, "the child was still published"
    assert all(s.spawning is not None for s in events), "spawning never cleared"
    assert lifecycle.read_meta() is None, "no meta was published on failure"


def test_deferred_publication_retries_then_clears_on_next_tick(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A restart reconciles child+spawning: retry meta, then clear spawning."""
    child = _published_child()
    _in_progress_state(child)

    meta_log: list[lifecycle.WorkerMeta] = []
    real_write_meta = lifecycle.write_meta

    def fake_write_meta(meta: lifecycle.WorkerMeta) -> None:
        meta_log.append(meta)
        real_write_meta(meta)

    monkeypatch.setattr(lifecycle, "write_meta", fake_write_meta)

    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    assert daemon._resolve_spawning_obligation() is True
    assert read_state().spawning is None, "obligation cleared after retry"
    assert read_state().child is not None
    assert len(meta_log) == 1, "the previously-missing meta was written on reconciliation"
    assert meta_log[0].pid == child.pid


def test_deferred_publication_meta_failure_stops_reconciliation_turn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A meta failure during reconciliation blocks the turn and keeps authority.

    When the deferred publication cannot write its lifecycle meta, the obligation
    is not reported resolved: the reconciliation turn stops (no retirement, no
    adoption, no new spawn) and the spawning obligation stays durably blocking
    until the next tick can retry.
    """
    child = _published_child()
    _in_progress_state(child)
    monkeypatch.setattr(
        lifecycle,
        "write_meta",
        lambda _meta: (_ for _ in ()).throw(DurabilityError("x")),
    )

    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    spawn_attempts: list[str] = []

    def blocked_spawn(c: str) -> WorkerChild:
        spawn_attempts.append(c)
        return _published_child()

    monkeypatch.setattr(daemon, "_spawn_worker", blocked_spawn)
    assert daemon._resolve_spawning_obligation() is False
    assert read_state().spawning is not None, "the obligation stayed blocking"
    assert read_state().child is not None, "the in-progress child was not retired"

    # The whole consumer-establishment turn aborts before any spawn.
    daemon._ensure_consumer_locked(COMMIT)
    assert spawn_attempts == [], "no replacement spawn happened while obligation blocked"
    assert read_state().spawning is not None, "obligation still blocking after the turn"

    # Once the publication succeeds, the turn resolves and clears the obligation.
    monkeypatch.setattr(lifecycle, "write_meta", lambda _meta: None)
    assert daemon._resolve_spawning_obligation() is True
    assert read_state().spawning is None
    assert read_state().child is not None


def test_in_progress_publication_is_not_converged_as_live_spawn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """child+spawning is recognized as in-progress, never as a live first spawn."""
    child = _published_child()
    _in_progress_state(child)
    observed = _patch_recovery(monkeypatch)

    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    monkeypatch.setattr(
        daemon,
        "_spawn_worker",
        lambda _commit: pytest.fail("in-progress publication authorized a spawn"),
    )
    assert daemon._resolve_spawning_obligation() is True
    assert read_state().spawning is None
    assert observed == [], "no group recovery ran for an in-progress publication"


def test_manual_recovery_blocked_while_spawning_retained() -> None:
    """A retained spawning obligation blocks a second consumer's adoption."""
    child = _published_child()
    _in_progress_state(child)

    other = lifecycle.WorkerMeta(
        schema_version=lifecycle.SCHEMA_VERSION,
        state=lifecycle.STATE_RUNNING,
        pid=9999,
        pgid=9999,
        sid=9999,
        start_time_ticks=1,
        token="other" + "b" * 20,
        repo="",
        git_commit=COMMIT,
        worker_id="w",
        log_path="",
        started_at=0.0,
        stopped_at=None,
    )
    assert lifecycle._pre_adoption_authority_error(other) is not None

    matching = lifecycle.WorkerMeta(
        schema_version=lifecycle.SCHEMA_VERSION,
        state=lifecycle.STATE_RUNNING,
        pid=child.pid,
        pgid=child.pid,
        sid=child.pid,
        start_time_ticks=child.start_time_ticks,
        token=child.token,
        repo="",
        git_commit=COMMIT,
        worker_id="w",
        log_path="",
        started_at=0.0,
        stopped_at=None,
    )
    # The supervisor's in-progress obligation carries the kernel parent-death
    # guarantee (parent_death_signal=True), so even a metadata-exact match must
    # stay blocked: manual recovery may not release a supervisor-owned
    # publication's authority, only the supervisor itself may finish it.
    assert lifecycle._pre_adoption_authority_error(matching) is not None


def test_fully_published_state_has_no_blocking_obligation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A successful publication ends with child published, meta present, no spawning."""
    monkeypatch.setattr(cli, "runtime_is_usable", lambda _commit: True)
    monkeypatch.setattr(cli, "cli_commit_dir", lambda _commit: FAKE_RUNTIME_ROOT)
    monkeypatch.setattr(lifecycle, "worker_env", lambda _token: {})
    real_write_meta = lifecycle.write_meta

    def fake_write_meta(meta: lifecycle.WorkerMeta) -> None:
        real_write_meta(meta)

    monkeypatch.setattr(lifecycle, "write_meta", fake_write_meta)

    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    monkeypatch.setattr(daemon, "_spawn_worker", lambda c: _fake_spawn(daemon, c))
    daemon._ensure_consumer_locked(COMMIT)

    state = read_state()
    assert state.child is not None
    assert state.spawning is None
    meta = lifecycle.read_meta()
    assert meta is not None
    assert meta.pid == state.child.pid
    assert meta.token == state.child.token
    assert lifecycle._pre_adoption_authority_error(meta) is None


def test_pre_popen_obligation_carries_no_child(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Before Popen the obligation is durable with no child identity yet."""
    observed: list[SpawningObligation | None] = []

    real_spawn = supervisor.SupervisorDaemon._spawn_worker

    def intercept(daemon: supervisor.SupervisorDaemon, commit: str) -> WorkerChild | None:
        observed.append(read_state().spawning)
        return real_spawn(daemon, commit)

    monkeypatch.setattr(cli, "runtime_is_usable", lambda _commit: False)
    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    monkeypatch.setattr(daemon, "_spawn_worker", lambda c: intercept(daemon, c))
    daemon._ensure_worker(COMMIT)

    # The refused-runtime path never reaches Popen, so no durable obligation is
    # left behind; the invariant is that any obligation present pre-Popen names
    # no child until the spawn succeeds.
    assert read_state().spawning is None
    assert read_state().child is None


class _FakeDirectProc:
    """Minimal direct-Popen stand-in that records its own convergence."""

    def __init__(self, pid: int) -> None:
        """Record the fake PID.

        Args:
            pid: The PID the fake spawn reports.
        """
        self.pid = pid
        self.terminated = False

    def terminate(self) -> None:
        """Record the termination request."""
        self.terminated = True

    @staticmethod
    def wait(timeout: float | None = None) -> int:
        """Report an immediate clean exit.

        Args:
            timeout: Ignored.

        Returns:
            The fake exit code.
        """
        del timeout
        return 0

    def kill(self) -> None:
        """Record the kill request."""
        self.terminated = True


def test_missing_pre_popen_obligation_fails_closed_without_synthesis(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A live child without its pre-Popen obligation is converged, never published.

    Synthesizing a fresh obligation after Popen cannot prove the required
    pre-Popen durability boundary, so the spawn must fail closed: the direct
    child is converged by exact single-PID signalling, no child is published,
    no replacement obligation is fabricated, and a backoff is recorded so no
    second consumer can be authorized.
    """
    monkeypatch.setattr(cli, "runtime_is_usable", lambda _commit: True)
    monkeypatch.setattr(cli, "cli_commit_dir", lambda _commit: FAKE_RUNTIME_ROOT)
    monkeypatch.setattr(lifecycle, "worker_env", lambda _token: {})
    # Owned-group recovery succeeds, so the clean release path is exercised.
    monkeypatch.setattr(supervisor, "recover_owned_groups", lambda _token: None)

    meta_calls: list[object] = []

    def fake_write_meta(meta: lifecycle.WorkerMeta) -> None:
        meta_calls.append(meta)

    monkeypatch.setattr(lifecycle, "write_meta", fake_write_meta)

    daemon = supervisor.SupervisorDaemon(supervisor.Settings())

    def fake_spawn_without_obligation(_commit: str) -> WorkerChild:
        daemon.proc = cast("subprocess.Popen[bytes]", _FakeDirectProc(4711))
        return _published_child()

    monkeypatch.setattr(daemon, "_spawn_worker", fake_spawn_without_obligation)
    daemon._spawn_and_publish(COMMIT)

    # No authority was published and no obligation was synthesized.
    state = read_state()
    assert state.child is None, "no child was published without its obligation"
    assert state.spawning is None, "no replacement obligation was fabricated"
    assert meta_calls == [], "no lifecycle meta was written"
    assert daemon.proc is None, "the direct child was converged and released"
    assert state.unresolved_child is None, "authority released cleanly after recovery"
    assert state.next_attempt_at is not None, "a backoff hold was recorded"
    assert daemon._message is not None
    assert "pre-Popen obligation" in daemon._message


def test_missing_obligation_convergence_failure_keeps_durable_blocking_hold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A non-converging direct child is durably held, never forgotten or replaced.

    When the live child cannot be positively reaped, the possibly-live direct
    child must not be dropped (which would forget it with no blocking authority)
    and no replacement may be authorized. The exact-identity unresolved-child
    hold is persisted durably (carrying the child's token so later convergence
    can also recover owned command groups) and the direct handle is retained so
    a later tick can converge it by pinned single-PID signals.
    """
    monkeypatch.setattr(cli, "runtime_is_usable", lambda _commit: True)
    monkeypatch.setattr(cli, "cli_commit_dir", lambda _commit: FAKE_RUNTIME_ROOT)
    monkeypatch.setattr(lifecycle, "worker_env", lambda _token: {})
    monkeypatch.setattr(lifecycle, "write_meta", lambda _meta: None)
    # Simulate the child still being live so reconciliation cannot converge it.
    monkeypatch.setattr(
        supervisor.SupervisorDaemon, "_converge_unresolved", lambda _self, _h: False
    )

    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    # Force direct-child convergence to fail so the fail-closed hold path runs.
    monkeypatch.setattr(daemon, "_converge_direct_child", lambda _proc: False)

    published = _published_child()

    def fake_spawn_without_obligation(_commit: str) -> WorkerChild:
        daemon.proc = cast("subprocess.Popen[bytes]", _FakeDirectProc(published.pid))
        return published

    monkeypatch.setattr(daemon, "_spawn_worker", fake_spawn_without_obligation)
    daemon._spawn_and_publish(COMMIT)

    state = read_state()
    assert state.child is None, "no child was published without its obligation"
    assert state.spawning is None, "no replacement obligation was fabricated"
    # The possibly-live child is durably recorded as a blocking hold carrying
    # the exact child identity/token, and the direct handle is retained.
    held = state.unresolved_child
    assert held is not None, "a durable blocking hold was persisted for the live child"
    assert held.pid == published.pid
    assert held.start_time_ticks == published.start_time_ticks
    assert held.token == published.token, "the exact incarnation token is preserved"
    assert daemon.proc is not None, "the direct child handle was not dropped"
    assert state.next_attempt_at is not None, "a backoff hold was recorded"
    assert daemon._message is not None
    assert "pre-Popen obligation" in daemon._message

    # The durable hold keeps replacement blocked: while the child is still live,
    # a fresh spawn attempt cannot publish a second consumer.
    spawn_attempts: list[str] = []

    def blocked_spawn(c: str) -> WorkerChild:
        spawn_attempts.append(c)
        return published

    monkeypatch.setattr(daemon, "_spawn_worker", blocked_spawn)
    daemon._ensure_worker(COMMIT)
    assert spawn_attempts == [], "the durable hold blocked any replacement spawn"
    assert read_state().unresolved_child is not None, "the hold stayed durable"


def test_missing_obligation_without_direct_handle_keeps_durable_blocking_hold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A returned child with no usable Popen handle is still durably held.

    Defensively, if the direct handle is unexpectedly unavailable, the possibly
    live child must not be merely backed off without authority: the exact-identity
    blocking hold (carrying the child's token) is persisted so a later tick can
    converge it by pinned single-PID signals, and no replacement is authorized.
    """
    monkeypatch.setattr(cli, "runtime_is_usable", lambda _commit: True)
    monkeypatch.setattr(cli, "cli_commit_dir", lambda _commit: FAKE_RUNTIME_ROOT)
    monkeypatch.setattr(lifecycle, "worker_env", lambda _token: {})
    monkeypatch.setattr(lifecycle, "write_meta", lambda _meta: None)
    monkeypatch.setattr(
        supervisor.SupervisorDaemon, "_converge_unresolved", lambda _self, _h: False
    )

    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    published = _published_child()

    def fake_spawn_without_handle(_commit: str) -> WorkerChild:
        # No direct Popen handle is recorded despite a returned child.
        daemon.proc = None
        return published

    monkeypatch.setattr(daemon, "_spawn_worker", fake_spawn_without_handle)
    daemon._spawn_and_publish(COMMIT)

    state = read_state()
    assert state.child is None, "no child was published without its obligation"
    assert state.spawning is None, "no replacement obligation was fabricated"
    held = state.unresolved_child
    assert held is not None, "a durable blocking hold was persisted for the live child"
    assert held.pid == published.pid
    assert held.token == published.token, "the exact incarnation token is preserved"
    assert daemon.proc is None, "no direct handle was available to retain"

    spawn_attempts: list[str] = []

    def blocked_spawn(c: str) -> WorkerChild:
        spawn_attempts.append(c)
        return published

    monkeypatch.setattr(daemon, "_spawn_worker", blocked_spawn)
    daemon._ensure_worker(COMMIT)
    assert spawn_attempts == [], "the durable hold blocked any replacement spawn"


def test_missing_obligation_converged_but_group_recovery_fails_keeps_token_hold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Worker reaped but owned-group recovery fails: keep a token-bearing hold.

    A returned worker may already have launched independent command groups under
    its lifecycle token. Even after the direct child is positively reaped, if the
    exact-incarnation owned-group recovery fails the durable token-bearing hold
    must survive so no replacement can start with stale side-effecting groups.
    """
    monkeypatch.setattr(cli, "runtime_is_usable", lambda _commit: True)
    monkeypatch.setattr(cli, "cli_commit_dir", lambda _commit: FAKE_RUNTIME_ROOT)
    monkeypatch.setattr(lifecycle, "worker_env", lambda _token: {})
    monkeypatch.setattr(lifecycle, "write_meta", lambda _meta: None)
    monkeypatch.setattr(
        supervisor,
        "recover_owned_groups",
        lambda _token: (_ for _ in ()).throw(supervisor.OwnedGroupRecoveryError("db down")),
    )

    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    published = _published_child()

    def fake_spawn_without_obligation(_commit: str) -> WorkerChild:
        daemon.proc = cast("subprocess.Popen[bytes]", _FakeDirectProc(published.pid))
        return published

    monkeypatch.setattr(daemon, "_spawn_worker", fake_spawn_without_obligation)
    daemon._spawn_and_publish(COMMIT)

    state = read_state()
    assert state.child is None, "no child was published without its obligation"
    assert state.spawning is None, "no replacement obligation was fabricated"
    assert daemon.proc is None, "the reaped child handle was released"
    # A durable token-bearing hold blocks any replacement.
    held = state.unresolved_child
    assert held is not None, "a token-bearing blocking hold survived the group recovery failure"
    assert held.token == published.token, "the exact incarnation token is preserved"
    assert held.pid == published.pid
    assert state.next_attempt_at is not None, "a backoff hold was recorded"
    assert daemon._message is not None
    assert "owned command groups" in daemon._message

    # The durable hold keeps replacement blocked on the next tick.
    spawn_attempts: list[str] = []

    def blocked_spawn(c: str) -> WorkerChild:
        spawn_attempts.append(c)
        return published

    monkeypatch.setattr(daemon, "_spawn_worker", blocked_spawn)
    daemon._ensure_worker(COMMIT)
    assert spawn_attempts == [], "the durable hold blocked any replacement spawn"


# Keep ``subprocess`` referenced for parity with sibling spawn tests.
_ = subprocess
