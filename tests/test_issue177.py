"""Regression tests for live-child convergence on identity timeout (#177)."""

from __future__ import annotations

import json
import signal
import subprocess
from dataclasses import replace
from typing import TYPE_CHECKING

import pytest

from lubko import cli, lifecycle, supervise
from lubko.supervisor import Settings, SupervisorDaemon

if TYPE_CHECKING:
    from pathlib import Path

COMMIT = "f" * 40


class FakePopen:
    """Deterministic stand-in for the spawned worker ``Popen`` handle."""

    def __init__(self, pid: int, *, mode: str) -> None:
        """Record the fake behaviour mode.

        Args:
            pid: Fake process id.
            mode: One of ``converges``, ``exited``, or ``wedged``.
        """
        self.pid = pid
        self.mode = mode
        self.returncode: int | None = 0 if mode == "exited" else None
        self.signals: list[str] = []

    def terminate(self) -> None:
        """Record a SIGTERM; a converging child then exits."""
        self.signals.append("SIGTERM")
        if self.mode == "converges":
            self.returncode = -15

    def kill(self) -> None:
        """Record a SIGKILL; any non-wedged child then exits."""
        self.signals.append("SIGKILL")
        if self.mode != "wedged":
            self.returncode = -9

    def poll(self) -> int | None:
        """Return the exit status while the child is still alive."""
        return self.returncode

    def wait(self, timeout: float | None = None) -> int:
        """Reap the child unless it is wedged or not yet signalled.

        Args:
            timeout: How long a real ``Popen`` would wait.

        Returns:
            The exit status.

        Raises:
            subprocess.TimeoutExpired: When the child refuses to exit.
        """
        expired: float = timeout if timeout is not None else 0.0
        if self.mode == "wedged":
            raise subprocess.TimeoutExpired(cmd="worker", timeout=expired)
        if self.mode == "converges" and "SIGTERM" not in self.signals:
            raise subprocess.TimeoutExpired(cmd="worker", timeout=expired)
        assert self.returncode is not None
        return self.returncode


def _install_spawn_fixtures(
    monkeypatch: pytest.MonkeyPatch,
    daemon: SupervisorDaemon,
    fake: FakePopen,
    tmp_path: Path,
) -> None:
    """Make ``_spawn_worker`` launch ``fake`` instead of a real worker."""
    monkeypatch.setattr(daemon, "_wait_for_identity", lambda _pid: None)
    monkeypatch.setattr(cli, "runtime_is_usable", lambda _commit: True)
    monkeypatch.setattr(cli, "cli_entry_executable", lambda _commit, _name: tmp_path / "worker")
    monkeypatch.setattr(cli, "cli_commit_dir", lambda _commit: tmp_path)
    monkeypatch.setattr(subprocess, "Popen", lambda *_a, **_k: fake)


def _identity(pid: int) -> lifecycle.ProcessIdentity:
    """Build the exact identity observed for a live fake child.

    Args:
        pid: Fake process id.

    Returns:
        The exact identity with matching group, session, and start ticks.
    """
    return lifecycle.ProcessIdentity(
        pid=pid,
        pgid=pid,
        sid=pid,
        start_time_ticks=4242,
    )


def _prepare_state() -> None:
    """Persist a run-intent state for the test commit."""
    supervise.write_state(
        replace(
            supervise.fresh_state(),
            mode=supervise.MODE_RUN,
            commit=COMMIT,
            intent=supervise.INTENT_RUN,
        )
    )


def test_identity_timeout_converges_live_child_and_stays_retryable(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A live child whose identity timed out is converged, reaped, then retried."""
    _prepare_state()
    daemon = SupervisorDaemon(Settings())
    fake = FakePopen(40001, mode="converges")
    _install_spawn_fixtures(monkeypatch, daemon, fake, tmp_path)

    child = daemon._spawn_worker(COMMIT)  # ruff: ignore[private-member-access]

    assert child is None
    assert fake.signals == ["SIGTERM"]
    assert fake.poll() == -15
    assert daemon.proc is None
    assert supervise.read_state().child is None


def test_identity_timeout_already_exited_child_is_retryable(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """An already-exited child remains an ordinary retryable spawn failure."""
    _prepare_state()
    daemon = SupervisorDaemon(Settings())
    fake = FakePopen(40002, mode="exited")
    _install_spawn_fixtures(monkeypatch, daemon, fake, tmp_path)

    child = daemon._spawn_worker(COMMIT)  # ruff: ignore[private-member-access]

    assert child is None
    assert fake.signals == []
    assert daemon.proc is None
    assert supervise.read_state().child is None


def test_unprovable_live_child_fails_closed_and_blocks_replacement(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A live child that cannot be converged is durably held, never overlapped."""
    _prepare_state()
    daemon = SupervisorDaemon(Settings())
    fake = FakePopen(40003, mode="wedged")
    _install_spawn_fixtures(monkeypatch, daemon, fake, tmp_path)
    monkeypatch.setattr(lifecycle, "process_identity", lambda _pid: _identity(fake.pid))

    child = daemon._spawn_worker(COMMIT)  # ruff: ignore[private-member-access]

    assert child is None
    assert "SIGTERM" in fake.signals
    assert "SIGKILL" in fake.signals
    # Fail closed: ownership of the unresolved child is retained durably.
    assert daemon.proc is not None
    assert daemon.proc.pid == fake.pid
    recorded = supervise.read_state().child
    assert recorded is not None
    assert recorded.pid == fake.pid
    assert recorded.start_time_ticks == 4242
    assert recorded.token

    # No later reconciliation may start a second worker alongside it.
    spawns: list[str] = []

    def _counting_spawn(commit: str) -> supervise.WorkerChild | None:
        spawns.append(commit)
        return None

    monkeypatch.setattr(daemon, "_spawn_worker", _counting_spawn)
    monkeypatch.setattr(daemon, "_child_alive", lambda _state: True)
    daemon._ensure_worker(COMMIT)  # ruff: ignore[private-member-access]

    assert spawns == []


def test_fail_closed_hold_clears_once_the_child_positive_exits(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Once the held child provably dies, ordinary reconciliation resumes."""
    _prepare_state()
    daemon = SupervisorDaemon(Settings())
    fake = FakePopen(40004, mode="wedged")
    _install_spawn_fixtures(monkeypatch, daemon, fake, tmp_path)
    monkeypatch.setattr(lifecycle, "process_identity", lambda _pid: _identity(fake.pid))
    assert (
        daemon._spawn_worker(COMMIT) is None  # ruff: ignore[private-member-access]
    )
    assert supervise.read_state().child is not None

    fake.returncode = -9
    spawns: list[str] = []

    def _counting_spawn(commit: str) -> supervise.WorkerChild | None:
        spawns.append(commit)
        return None

    def _fake_stop(meta: lifecycle.WorkerMeta, grace: float, **_kwargs: object) -> bool:
        assert grace > 0
        return meta.pid == fake.pid

    monkeypatch.setattr(daemon, "_spawn_worker", _counting_spawn)
    monkeypatch.setattr(lifecycle, "stop_worker", _fake_stop)
    monkeypatch.setattr("lubko.supervisor.recover_owned_groups", lambda _token: None)
    monkeypatch.setattr(
        daemon,
        "_child_alive",
        lambda state: (
            state.child is not None and state.child.pid == fake.pid and fake.poll() is None
        ),
    )

    daemon._ensure_worker(COMMIT)  # ruff: ignore[private-member-access]

    assert spawns == [COMMIT]


def test_retire_child_converges_the_held_child_by_exact_identity(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A restart can retire the fail-closed hold through exact retirement."""
    _prepare_state()
    daemon = SupervisorDaemon(Settings())
    fake = FakePopen(40005, mode="wedged")
    _install_spawn_fixtures(monkeypatch, daemon, fake, tmp_path)
    monkeypatch.setattr(lifecycle, "process_identity", lambda _pid: _identity(fake.pid))
    assert (
        daemon._spawn_worker(COMMIT) is None  # ruff: ignore[private-member-access]
    )
    recorded = supervise.read_state().child
    assert recorded is not None

    stops: list[int] = []

    def _fake_stop(meta: lifecycle.WorkerMeta, grace: float, **_kwargs: object) -> bool:
        assert grace > 0
        assert meta.pid is not None
        stops.append(meta.pid)
        fake.returncode = -9
        return True

    monkeypatch.setattr(lifecycle, "stop_worker", _fake_stop)
    monkeypatch.setattr("lubko.supervisor.recover_owned_groups", lambda _token: None)

    assert daemon._retire_child()  # ruff: ignore[private-member-access]

    assert stops == [recorded.pid]
    assert supervise.read_state().child is None


def test_shared_group_live_child_gets_authority_free_hold(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A live child with a shared group never earns group-signallable authority."""
    _prepare_state()
    daemon = SupervisorDaemon(Settings())
    fake = FakePopen(40006, mode="wedged")
    _install_spawn_fixtures(monkeypatch, daemon, fake, tmp_path)
    shared = lifecycle.ProcessIdentity(
        pid=fake.pid,
        pgid=7777,
        sid=7777,
        start_time_ticks=4242,
    )
    monkeypatch.setattr(lifecycle, "process_identity", lambda _pid: shared)

    child = daemon._spawn_worker(COMMIT)  # ruff: ignore[private-member-access]

    assert child is None
    # No group-signallable WorkerChild identity is persisted.
    state = supervise.read_state()
    assert state.child is None
    assert state.unresolved_child is not None
    assert state.unresolved_child.pid == fake.pid
    assert state.unresolved_child.start_time_ticks == 4242

    # And no replacement starts while the unresolved hold lives.
    spawns: list[str] = []

    def _counting_spawn(commit: str) -> supervise.WorkerChild | None:
        spawns.append(commit)
        return None

    monkeypatch.setattr(daemon, "_spawn_worker", _counting_spawn)
    monkeypatch.setattr("lubko.supervisor.proc_start_ticks", lambda _pid: 4242)

    daemon._ensure_worker(COMMIT)  # ruff: ignore[private-member-access]

    assert spawns == []


def test_unresolved_hold_resolves_by_exact_pid_without_group_signals(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The hold converges by exact PID only and clears once the instance dies."""
    _prepare_state()
    daemon = SupervisorDaemon(Settings(stop_grace_seconds=0.2))
    fake = FakePopen(40007, mode="wedged")
    _install_spawn_fixtures(monkeypatch, daemon, fake, tmp_path)
    shared = lifecycle.ProcessIdentity(
        pid=fake.pid,
        pgid=7777,
        sid=7777,
        start_time_ticks=4242,
    )
    monkeypatch.setattr(lifecycle, "process_identity", lambda _pid: shared)
    assert (
        daemon._spawn_worker(COMMIT) is None  # ruff: ignore[private-member-access]
    )
    assert supervise.read_state().unresolved_child is not None

    ticks: dict[str, int | None] = {"value": 4242}
    kills: list[tuple[int, int]] = []

    def _fake_kill(pid: int, sig: int) -> None:
        kills.append((pid, sig))

    spawns: list[str] = []

    def _counting_spawn(commit: str) -> supervise.WorkerChild | None:
        spawns.append(commit)
        return None

    monkeypatch.setattr(daemon, "_spawn_worker", _counting_spawn)
    monkeypatch.setattr("lubko.supervisor.proc_start_ticks", lambda _pid: ticks["value"])
    monkeypatch.setattr("lubko.supervisor.os.kill", _fake_kill)

    # While the exact instance lives, the hold blocks any replacement.
    daemon._ensure_worker(COMMIT)  # ruff: ignore[private-member-access]
    assert spawns == []
    assert kills
    assert all(pid == fake.pid for pid, _sig in kills)
    assert all(sig in {signal.SIGTERM, signal.SIGKILL} for _pid, sig in kills)
    assert supervise.read_state().unresolved_child is not None

    # Once the exact instance is gone, ordinary reconciliation resumes.
    ticks["value"] = None
    daemon._ensure_worker(COMMIT)  # ruff: ignore[private-member-access]
    assert spawns == [COMMIT]
    assert supervise.read_state().unresolved_child is None


def test_unobservable_ticks_live_pid_keeps_hold_until_disappearance(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A never-observable live PID keeps the hold; disappearance resolves it."""
    _prepare_state()
    daemon = SupervisorDaemon(Settings(stop_grace_seconds=0.2))
    fake = FakePopen(40008, mode="wedged")
    _install_spawn_fixtures(monkeypatch, daemon, fake, tmp_path)

    # The identity is observable but shared, and its start ticks are recorded;
    # then simulate a hold whose start ticks were never observable.
    shared = lifecycle.ProcessIdentity(
        pid=fake.pid,
        pgid=7777,
        sid=7777,
        start_time_ticks=4242,
    )
    monkeypatch.setattr(lifecycle, "process_identity", lambda _pid: shared)
    assert (
        daemon._spawn_worker(COMMIT) is None  # ruff: ignore[private-member-access]
    )
    assert supervise.read_state().unresolved_child is not None
    # Downgrade to a hold with unobservable start ticks: no signal authorized.
    held = supervise.read_state()
    assert held.unresolved_child is not None
    supervise.write_state(
        replace(
            held,
            unresolved_child=supervise.UnresolvedChild(
                pid=held.unresolved_child.pid,
                start_time_ticks=None,
                token=held.unresolved_child.token,
                spawned_at=held.unresolved_child.spawned_at,
            ),
        )
    )

    spawns: list[str] = []

    def _counting_spawn(commit: str) -> supervise.WorkerChild | None:
        spawns.append(commit)
        return None

    ticks: dict[str, int | None] = {"value": 4242}
    kills: list[tuple[int, int]] = []
    monkeypatch.setattr(daemon, "_spawn_worker", _counting_spawn)
    monkeypatch.setattr("lubko.supervisor.proc_start_ticks", lambda _pid: ticks["value"])
    monkeypatch.setattr("lubko.supervisor.os.kill", lambda pid, sig: kills.append((pid, sig)))

    # While the PID lives (even though it ignores signals), nothing may spawn
    # and the durable hold must be retained.
    daemon._ensure_worker(COMMIT)  # ruff: ignore[private-member-access]
    assert spawns == []
    assert supervise.read_state().unresolved_child is not None

    # Once the PID provably disappears, ordinary reconciliation resumes.
    ticks["value"] = None
    daemon._ensure_worker(COMMIT)  # ruff: ignore[private-member-access]
    assert spawns == [COMMIT]
    assert supervise.read_state().unresolved_child is None


def test_completely_unobservable_live_child_hold_survives_supervisor_restart(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The PID-only hold is crash-safe: written pre-wait and never signalled."""

    class SupervisorCrashed(BaseException):
        """Deterministic sentinel simulating a supervisor crash mid-convergence."""

    _prepare_state()
    daemon = SupervisorDaemon(Settings(stop_grace_seconds=0.2))
    fake = FakePopen(40009, mode="wedged")
    _install_spawn_fixtures(monkeypatch, daemon, fake, tmp_path)

    # The spawned child stays alive but its /proc identity is unreadable. The
    # authority-free hold must be durably recorded before any further waiting.
    observations: list[bool] = []

    def _fake_identity(_pid: int) -> None:
        observations.append(supervise.read_state().unresolved_child is not None)

    monkeypatch.setattr(lifecycle, "process_identity", _fake_identity)
    # Single-shot identity observation per convergence round for determinism.
    monkeypatch.setattr(
        daemon,
        "_await_observable_identity",
        lambda proc: lifecycle.process_identity(proc.pid),
    )

    crashes = {"pending": True}

    def _crash_after_persist(seconds: float | None) -> None:
        del seconds
        if crashes["pending"] and supervise.read_state().unresolved_child is not None:
            # The hold just hit disk: the supervisor "crashes" here.
            crashes["pending"] = False
            raise SupervisorCrashed

    monkeypatch.setattr("lubko.supervisor.time.sleep", _crash_after_persist)

    try:
        daemon._spawn_worker(COMMIT)  # ruff: ignore[private-member-access]
    except SupervisorCrashed:
        pass
    else:
        pytest.fail("supervisor should have crashed right after persisting the hold")

    # Crash state on disk: an untouched authority-free PID-only hold.
    held = supervise.read_state().unresolved_child
    assert held is not None
    assert held.pid == fake.pid
    assert held.start_time_ticks is None
    assert supervise.read_state().child is None
    # Persist-before-further-wait: the hold was absent at the first identity
    # observation and the crash fired on the very first post-persist wait.
    assert observations == [False]

    # Fresh daemon restarts from this exact durable state: while the held PID
    # is reported live it must not spawn a replacement AND must never signal —
    # unobservable ticks authorize no signal at all.
    restarted = SupervisorDaemon(Settings(stop_grace_seconds=0.2))
    spawns: list[str] = []

    def _counting_spawn(commit: str) -> supervise.WorkerChild | None:
        spawns.append(commit)
        return None

    ticks: dict[str, int | None] = {"value": 4242}
    kills: list[tuple[int, int]] = []
    monkeypatch.setattr(restarted, "_spawn_worker", _counting_spawn)
    monkeypatch.setattr("lubko.supervisor.proc_start_ticks", lambda _pid: ticks["value"])
    monkeypatch.setattr("lubko.supervisor.os.kill", lambda pid, sig: kills.append((pid, sig)))

    restarted._ensure_worker(COMMIT)  # ruff: ignore[private-member-access]
    assert spawns == []
    assert kills == []
    assert supervise.read_state().unresolved_child is not None

    # Only once the held PID is positively gone does reconciliation resume.
    ticks["value"] = None
    restarted._ensure_worker(COMMIT)  # ruff: ignore[private-member-access]
    assert spawns == [COMMIT]
    assert supervise.read_state().unresolved_child is None


def test_malformed_durable_hold_fails_closed_against_replacement(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A present-but-malformed hold blocks replacement instead of vanishing."""
    del tmp_path
    _prepare_state()
    payload = supervise.read_state().to_dict()
    payload["unresolved_child"] = {"pid": ["not", "an", "int"]}
    supervise.state_path().parent.mkdir(parents=True, exist_ok=True)
    supervise.state_path().write_text(
        json.dumps(payload, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    # Parsing keeps the blocking obligation alive as an explicit flag.
    state = supervise.read_state()
    assert state.unresolved_hold_malformed
    assert state.unresolved_child is None

    daemon = SupervisorDaemon(Settings())
    spawns: list[str] = []

    def _counting_spawn(commit: str) -> supervise.WorkerChild | None:
        spawns.append(commit)
        return None

    monkeypatch.setattr(daemon, "_spawn_worker", _counting_spawn)

    daemon._ensure_worker(COMMIT)  # ruff: ignore[private-member-access]

    assert spawns == []
    # The flag is durable across rewrites: reconciliation never erases it.
    assert supervise.read_state().unresolved_hold_malformed
