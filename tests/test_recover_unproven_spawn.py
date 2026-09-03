"""The recover path must never drop a live unproven child it spawned."""

from __future__ import annotations

import fcntl
import json
import signal
import subprocess
from dataclasses import replace
from pathlib import Path

import pytest

from lubko import cli, lifecycle, supervise, supervisor
from lubko.durable import DurabilityError
from lubko.lifecycle import DeployOptions, ProcessIdentity

COMMIT = "c" * 40
PID = 4242
TOKEN = "T" * 32
PRIVATE = ProcessIdentity(pid=PID, pgid=PID, sid=PID, start_time_ticks=555)
NON_PRIVATE = ProcessIdentity(pid=PID, pgid=1, sid=9000, start_time_ticks=555)
PIN_BASE = 20000


def test_cleanup_ready_markers_tolerates_non_file_entries(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Malformed marker filesystem shapes cannot abort recovery cleanup."""
    monkeypatch.setattr(lifecycle, "worker_state_dir", lambda: tmp_path)
    (tmp_path / "ready-directory").mkdir()
    valid = tmp_path / "ready-valid"
    valid.write_text(json.dumps({"pid": PID}))
    stale = tmp_path / "ready-stale"
    stale.write_text(json.dumps({"pid": PID + 1}))
    malformed = tmp_path / "ready-malformed"
    malformed.write_text("not json")
    wrong_type = tmp_path / "ready-wrong-type"
    wrong_type.write_text(json.dumps({"pid": float(PID)}))

    lifecycle._cleanup_ready_markers(PID)

    assert valid.is_file()
    assert not stale.exists()
    assert not malformed.exists()
    assert not wrong_type.exists()
    assert (tmp_path / "ready-directory").is_dir()


class NumericSignalError(AssertionError):
    """Raised when a numeric kill primitive is used instead of a pidfd."""


def _numeric_signal_forbidden() -> None:
    """Fail the test: only pinned ``pidfd_send_signal`` may deliver signals.

    Raises:
        NumericSignalError: Always.
    """
    raise NumericSignalError


class FakePopen:
    """Deterministic stand-in for the spawned recovery worker."""

    def __init__(self, pid: int) -> None:
        """Start as a live child with no recorded signals.

        Args:
            pid: Fake process id.
        """
        self.pid = pid
        self.returncode: int | None = None
        self.signals: list[str] = []

    def poll(self) -> int | None:
        """Return the exit status while the child is still alive."""
        return self.returncode

    def terminate(self) -> None:
        """Numeric fallback that must never be reached."""
        del self
        _numeric_signal_forbidden()

    def kill(self) -> None:
        """Numeric fallback that must never be reached."""
        del self
        _numeric_signal_forbidden()

    def wait(self, timeout: float | None = None) -> int:
        """Reap the child once it has been signalled or reaped without one.

        Args:
            timeout: How long a real ``Popen`` would wait.

        Returns:
            The exit status.

        Raises:
            subprocess.TimeoutExpired: When the child refuses to exit yet.
        """
        if timeout is not None and "SIGTERM" not in self.signals:
            raise subprocess.TimeoutExpired(cmd="worker", timeout=timeout)
        if self.returncode is None:
            self.returncode = -15 if "SIGTERM" in self.signals else -1
        return self.returncode


def options() -> DeployOptions:
    """Return minimal recover deployment options.

    Returns:
        Deterministic deployment inputs.
    """
    return DeployOptions(
        repo=Path(),
        uv_path="uv",
        bootstrap=True,
        stop_grace_seconds=0.0,
        postgres_timeout_seconds=1.0,
        lock_timeout_seconds=1.0,
        validation_timeout_seconds=1.0,
        git_timeout_seconds=1.0,
        cli_timeout_seconds=1.0,
    )


def _write_obligation() -> None:
    """Durably record a stale recovery obligation for the forced token."""
    supervise.write_state(
        replace(
            supervise.read_state(),
            spawning=replace(
                _pid_less_obligation(),
                pid=PID,
            ),
        )
    )


def _pid_less_obligation() -> supervise.SpawningObligation:
    """Build a pid-less manual recovery obligation for the forced token.

    Returns:
        The replacement-blocking obligation.
    """
    return supervise.SpawningObligation(
        token=TOKEN,
        commit=COMMIT,
        creator_pid=999999,
        creator_start_time_ticks=1,
        pid=None,
        start_time_ticks=None,
        created_at=0.0,
        boot_id=None,
        parent_death_signal=False,
    )


def _write_pid_less_obligation() -> None:
    """Durably record a pid-less manual recovery obligation."""
    supervise.write_state(replace(supervise.read_state(), spawning=_pid_less_obligation()))


@pytest.fixture(autouse=True)
def isolated_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Use an isolated durable supervisor-state root for each test.

    Args:
        tmp_path: Pytest temporary directory.
        monkeypatch: Pytest monkeypatch fixture.

    Returns:
        The supervisor state directory.
    """
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state"))
    supervise.state_path().parent.mkdir(parents=True, exist_ok=True)
    return supervise.state_path().parent


@pytest.fixture(autouse=True)
def deterministic_token(monkeypatch: pytest.MonkeyPatch) -> str:
    """Force a known recovery-worker lifecycle token.

    Args:
        monkeypatch: Pytest monkeypatch fixture.

    Returns:
        The forced token.
    """
    monkeypatch.setattr("secrets.token_hex", lambda _n: TOKEN)
    return TOKEN


@pytest.fixture
def recover_env(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[list[FakePopen], list[tuple[str, str]]]:
    """Stub preflight, logging, spawning, and owned-group recovery.

    Owned-group recovery succeeds by default and records every
    ``(token, event)`` observation so tests can assert exact ordering.

    Args:
        monkeypatch: Pytest monkeypatch fixture.

    Returns:
        The spawned fake children and the ordered recovery observations.
    """
    monkeypatch.setattr(lifecycle, "_recover_preflight", lambda _options: COMMIT)
    monkeypatch.setattr(lifecycle, "append_deploy_log", lambda _line: None)
    spawned: list[FakePopen] = []
    events: list[tuple[str, str]] = []

    def spawn(*_args: object, **_kwargs: object) -> FakePopen:
        """Return a fresh fake live child."""
        fake = FakePopen(PID)
        spawned.append(fake)
        return fake

    def recover(token: str) -> bool:
        """Record the recovery attempt as successful.

        Args:
            token: The recovered worker token.

        Returns:
            Always ``True``.
        """
        events.append((token, "recovered"))
        return True

    monkeypatch.setattr(lifecycle, "spawn_worker", spawn)
    monkeypatch.setattr(lifecycle, "_recover_owned_groups", recover)
    monkeypatch.setattr("os.kill", lambda *_a: _numeric_signal_forbidden())
    monkeypatch.setattr("os.killpg", lambda *_a: _numeric_signal_forbidden())
    return spawned, events


def observe(monkeypatch: pytest.MonkeyPatch, observed: ProcessIdentity | None) -> None:
    """Make ``_wait_for_identity`` time out observing ``observed``.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
        observed: The last identity ``/proc`` reports for the child.
    """
    monkeypatch.setattr(lifecycle, "SESSION_ESTABLISH_TIMEOUT_SECONDS", 0.0)
    monkeypatch.setattr(lifecycle, "process_identity", lambda _pid: observed)


def install_convergence(
    monkeypatch: pytest.MonkeyPatch,
    fake: FakePopen,
    proof: ProcessIdentity | None,
) -> list[tuple[int, str]]:
    """Make pidfd pinning and pinned signalling deterministic for ``fake``.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
        fake: The fake direct child being converged.
        proof: Occupant identity seen under the pin (or ``None``).

    Returns:
        The ``(pid, signal name)`` pairs delivered through the pin.
    """
    delivered: list[tuple[int, str]] = []
    state = {"pinned": False}

    def open_pin(_pid: int) -> int:
        state["pinned"] = True
        return PIN_BASE + fake.pid

    def send(_pin: int, sig: int) -> None:
        name = signal.Signals(sig).name
        delivered.append((fake.pid, name))
        fake.signals.append(name)

    def identity(_pid: int) -> ProcessIdentity | None:
        return proof if state["pinned"] else NON_PRIVATE

    monkeypatch.setattr(lifecycle, "_open_exact_pidfd", open_pin)
    monkeypatch.setattr(lifecycle, "process_identity", identity)
    monkeypatch.setattr(lifecycle, "pidfd_send_signal", send)
    monkeypatch.setattr("os.close", lambda _fd: None)
    return delivered


def test_recover_converges_live_child_when_identity_is_none(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    recover_env: tuple[list[FakePopen], list[tuple[str, str]]],
) -> None:
    """A live child with no observable identity is converged before failing."""
    spawned, _events = recover_env
    observe(monkeypatch, None)
    order: list[str] = []

    def converge(proc: FakePopen, grace: float, anchor: ProcessIdentity | None) -> None:
        """Record direct-child convergence ahead of group recovery."""
        del proc, grace, anchor
        order.append("converged")

    def recover(token: str) -> bool:
        """Record exact owned-group recovery for the forced token.

        Args:
            token: The recovered worker token.

        Returns:
            Always ``True``.
        """
        order.append(f"recovered:{token}")
        return True

    monkeypatch.setattr(lifecycle, "_converge_unproven_spawn", converge)
    monkeypatch.setattr(lifecycle, "_recover_owned_groups", recover)

    code = lifecycle._recover_locked(options())

    assert code == lifecycle.EXIT_ERROR
    assert "converging" in capsys.readouterr().err
    assert len(spawned) == 1
    assert spawned[0].poll() is None
    assert order == ["converged", f"recovered:{TOKEN}"]
    assert supervise.read_state().spawning is None


def test_recover_converges_live_child_with_non_private_identity(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    recover_env: tuple[list[FakePopen], list[tuple[str, str]]],
) -> None:
    """A live non-private session child is exactly signalled via its pin."""
    spawned, _events = recover_env
    observe(monkeypatch, NON_PRIVATE)
    fake = FakePopen(PID)

    def spawn(*_args: object, **_kwargs: object) -> FakePopen:
        """Return the prebuilt non-private-session fake child."""
        spawned.append(fake)
        return fake

    monkeypatch.setattr(lifecycle, "spawn_worker", spawn)
    delivered = install_convergence(monkeypatch, fake, NON_PRIVATE)

    code = lifecycle._recover_locked(options())

    assert code == lifecycle.EXIT_ERROR
    assert "converging" in capsys.readouterr().err
    assert delivered == [(PID, "SIGTERM")]
    assert fake.poll() == -15


def test_recover_recovers_owned_groups_on_every_failure_exit(
    capsys: pytest.CaptureFixture[str],
    recover_env: tuple[list[FakePopen], list[tuple[str, str]]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Every post-spawn failure exit recovers the token's groups first."""
    _spawned, events = recover_env

    def converge(*_args: object) -> None:
        """Deterministically skip direct-child convergence."""

    monkeypatch.setattr(lifecycle, "_converge_unproven_spawn", converge)
    for observed in (None, NON_PRIVATE):
        observe(monkeypatch, observed)
        assert lifecycle._recover_locked(options()) == lifecycle.EXIT_ERROR
        assert "adopt it with" not in capsys.readouterr().out
    # The dead-after-private-session exit path must also recover the groups.
    observe(monkeypatch, PRIVATE)

    def spawn_dead(*_args: object, **_kwargs: object) -> FakePopen:
        """Return a child whose private session already exited."""
        dead = FakePopen(PID)
        dead.returncode = 0
        return dead

    monkeypatch.setattr(lifecycle, "spawn_worker", spawn_dead)
    assert lifecycle._recover_locked(options()) == lifecycle.EXIT_ERROR

    recovered = {token for token, event in events if event == "recovered"}
    assert recovered == {TOKEN}


def test_recover_records_durable_authority_when_group_recovery_fails(
    capsys: pytest.CaptureFixture[str],
    recover_env: tuple[list[FakePopen], list[tuple[str, str]]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Failed owned-group recovery leaves a durable blocking obligation."""
    _spawned, _events = recover_env
    observe(monkeypatch, None)
    monkeypatch.setattr(lifecycle, "_converge_unproven_spawn", lambda *_a: None)
    monkeypatch.setattr(lifecycle, "_recover_owned_groups", lambda _token: False)

    code = lifecycle._recover_locked(options())
    captured = capsys.readouterr()

    assert code == lifecycle.EXIT_ERROR
    assert "could not be recovered" in captured.err
    obligation = supervise.read_state().spawning
    assert obligation is not None
    assert obligation.token == TOKEN
    assert obligation.commit == COMMIT
    assert obligation.pid == PID
    assert json.loads(supervise.state_path().read_text())["spawning"]["token"] == TOKEN


def test_recover_refuses_to_spawn_until_stale_obligation_resolves(
    capsys: pytest.CaptureFixture[str],
    recover_env: tuple[list[FakePopen], list[tuple[str, str]]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A later recover neither races unresolved groups nor forgets them."""
    spawned, events = recover_env
    _write_obligation()
    monkeypatch.setattr(lifecycle, "_recover_owned_groups", lambda _token: False)

    def must_not_spawn(*_args: object, **_kwargs: object) -> FakePopen:
        """Fail the test when a consumer is authorized despite the hold.

        Raises:
            AssertionError: Always.
        """
        msg = "consumer started beside unresolved groups"
        raise AssertionError(msg)

    monkeypatch.setattr(lifecycle, "spawn_worker", must_not_spawn)

    assert lifecycle._recover_locked(options()) == lifecycle.EXIT_ERROR
    assert "refusing to start another consumer" in capsys.readouterr().err
    assert not spawned

    observe(monkeypatch, PRIVATE)

    def recover_ok(token: str) -> bool:
        """Record the successful stale-obligation group recovery.

        Args:
            token: The recovered worker token.

        Returns:
            Always ``True``.
        """
        events.append((token, "recovered"))
        return True

    monkeypatch.setattr(lifecycle, "_recover_owned_groups", recover_ok)

    def spawn_ok(*_args: object, **_kwargs: object) -> FakePopen:
        """Return a live adoptable fake child."""
        ok = FakePopen(PID)
        spawned.append(ok)
        return ok

    monkeypatch.setattr(lifecycle, "spawn_worker", spawn_ok)
    assert lifecycle._recover_locked(options()) == lifecycle.EXIT_OK
    assert len(spawned) == 1
    obligation = supervise.read_state().spawning
    assert obligation is not None
    assert obligation.token == TOKEN
    assert obligation.pid == PID
    assert (TOKEN, "recovered") in events


def test_recover_never_spawns_without_durable_authority(
    capsys: pytest.CaptureFixture[str],
    recover_env: tuple[list[FakePopen], list[tuple[str, str]]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A recovery worker may only start once its authority is durably held."""
    spawned, _events = recover_env

    def fail_write(_state: object) -> None:
        """Model a state write that cannot be confirmed durable.

        Raises:
            DurabilityError: Always.
        """
        raise DurabilityError

    monkeypatch.setattr(supervise, "write_state", fail_write)
    monkeypatch.setattr(
        lifecycle,
        "spawn_worker",
        lambda *_a: (_ for _ in ()).throw(AssertionError("spawned without authority")),
    )

    code = lifecycle._recover_locked(options())

    assert code == lifecycle.EXIT_ERROR
    assert "could not durably establish" in capsys.readouterr().err
    assert not spawned
    # Whatever was written before the failure still blocks every consumer.
    assert supervise.read_state().spawning is None


def test_supervisor_never_resolves_pid_less_manual_obligation_by_assumption(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A pid-less manual obligation blocks even when pdeathsig is available."""
    _write_pid_less_obligation()
    monkeypatch.setattr(supervisor, "recover_owned_groups", lambda _token: None)

    daemon = supervisor.SupervisorDaemon(supervisor.Settings())

    assert not daemon._resolve_spawning_obligation()
    assert supervise.read_state().spawning is not None


def test_recover_never_spawns_over_a_malformed_spawning_authority(
    capsys: pytest.CaptureFixture[str],
    recover_env: tuple[list[FakePopen], list[tuple[str, str]]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A malformed pre-spawn authority fails closed and survives untouched."""
    spawned, _events = recover_env
    raw = {
        "schema_version": supervise.SCHEMA_VERSION,
        "spawning": {"token": TOKEN},
    }
    supervise.state_path().write_text(json.dumps(raw), encoding="utf-8")
    monkeypatch.setattr(
        lifecycle,
        "spawn_worker",
        lambda *_a: (_ for _ in ()).throw(AssertionError("spawned over a malformed authority")),
    )

    code = lifecycle._recover_locked(options())
    captured = capsys.readouterr()

    assert code == lifecycle.EXIT_ERROR
    assert "could not be resolved" in captured.err
    assert not spawned
    state = supervise.read_state()
    assert state.spawning_hold_malformed is True
    assert json.loads(supervise.state_path().read_text()) == raw


def test_supervisor_honors_recorded_recovery_obligation(
    monkeypatch: pytest.MonkeyPatch,
    recover_env: tuple[list[FakePopen], list[tuple[str, str]]],
) -> None:
    """The maintained supervisor resolves the recorded authority before spawning."""
    del recover_env
    observe(monkeypatch, None)
    monkeypatch.setattr(lifecycle, "_converge_unproven_spawn", lambda *_a: None)
    monkeypatch.setattr(lifecycle, "_recover_owned_groups", lambda _token: False)
    assert lifecycle._recover_locked(options()) == lifecycle.EXIT_ERROR

    recovered: list[str] = []

    def succeed(token: str) -> None:
        """Record the supervisor's own exact owned-group recovery."""
        recovered.append(token)

    monkeypatch.setattr(supervisor, "recover_owned_groups", succeed)
    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    assert daemon._resolve_spawning_obligation()
    assert recovered == [TOKEN]
    assert supervise.read_state().spawning is None

    _write_obligation()

    def fail(_token: str) -> None:
        """Model a still-unrecoverable owned command group.

        Raises:
            OwnedGroupRecoveryError: Always.
        """
        msg = "unresolved"
        raise supervisor.OwnedGroupRecoveryError(msg)

    monkeypatch.setattr(supervisor, "recover_owned_groups", fail)
    assert not daemon._resolve_spawning_obligation()
    assert supervise.read_state().spawning is not None


def test_recover_does_not_report_success_for_dead_child(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    recover_env: tuple[list[FakePopen], list[tuple[str, str]]],
) -> None:
    """A child that already exits is never reported as adoptable."""
    spawned, _events = recover_env
    observe(monkeypatch, PRIVATE)

    def mark_dead(_pid: int) -> ProcessIdentity | None:
        """Report the private identity while terminating the fake child.

        Returns:
            The private identity observed before the child exited.
        """
        if spawned:
            spawned[-1].returncode = 0
        return PRIVATE

    monkeypatch.setattr(lifecycle, "process_identity", mark_dead)

    code = lifecycle._recover_locked(options())
    captured = capsys.readouterr()

    assert code == lifecycle.EXIT_ERROR
    assert "exited before it could be adopted" in captured.err
    assert "adopt it with" not in captured.out
    assert f"pid={PID}" not in captured.out


def test_recover_reports_success_for_live_private_session_child(
    capsys: pytest.CaptureFixture[str],
    recover_env: tuple[list[FakePopen], list[tuple[str, str]]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A live child that establishes its private session is reported."""
    spawned, _events = recover_env
    observe(monkeypatch, PRIVATE)

    code = lifecycle._recover_locked(options())

    assert code == lifecycle.EXIT_OK
    assert spawned[-1].poll() is None
    assert "adopt it with" in capsys.readouterr().out


def _adopted_meta() -> lifecycle.WorkerMeta:
    """Build metadata describing the adopted fake recovery worker.

    Returns:
        Maintained-worker metadata for the exact fake identity.
    """
    return lifecycle.WorkerMeta(
        schema_version=lifecycle.SCHEMA_VERSION,
        state=lifecycle.STATE_RUNNING,
        pid=PRIVATE.pid,
        pgid=PRIVATE.pgid,
        sid=PRIVATE.sid,
        start_time_ticks=PRIVATE.start_time_ticks,
        token=TOKEN,
        repo=".",
        git_commit=COMMIT,
        worker_id="worker",
        log_path="worker.log",
        started_at=0.0,
        stopped_at=None,
    )


def _stub_repair(monkeypatch: pytest.MonkeyPatch) -> None:
    """Stub every repair dependency except the durable state transition.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
    """
    monkeypatch.setattr(lifecycle, "git_commit", lambda *_a: COMMIT)
    monkeypatch.setattr(lifecycle, "_adoption_candidate", lambda *_a: (_adopted_meta(), "worker"))
    monkeypatch.setattr(lifecycle, "process_identity", lambda _pid: PRIVATE)
    monkeypatch.setattr(lifecycle, "process_has_token", lambda _pid, _token: True)
    monkeypatch.setattr(cli, "reconcile_pointer", lambda _commit: True)
    monkeypatch.setattr(cli, "gc_cli_roots", lambda _commits: None)
    monkeypatch.setattr(lifecycle, "_cleanup_ready_markers", lambda _pid: None)
    monkeypatch.setattr(lifecycle, "_reconcile_toolchain", lambda _uv: None)
    monkeypatch.setattr(lifecycle, "_verify_queue_roundtrip", lambda *_a: True)
    monkeypatch.setattr(lifecycle, "append_deploy_log", lambda _line: None)


def test_adoption_candidate_refuses_malformed_maintained_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A corrupt maintained authority cannot collapse to absence during adoption."""
    monkeypatch.setattr(lifecycle, "require_clean_checkout", lambda *_a: True)
    monkeypatch.setattr(lifecycle, "process_identity", lambda _pid: PRIVATE)
    monkeypatch.setattr(lifecycle, "_is_lubko_worker_process", lambda _pid: True)
    monkeypatch.setattr(lifecycle, "check_postgres", lambda *_a: True)

    def malformed_meta() -> lifecycle.WorkerMeta | None:
        msg = "maintained-worker metadata is not valid JSON"
        raise lifecycle.WorkerMetadataError(msg)

    monkeypatch.setattr(lifecycle, "read_meta_strict", malformed_meta)

    with pytest.raises(lifecycle._AdoptionError, match="present but untrustworthy"):
        lifecycle._adoption_candidate(options(), PID, COMMIT)


def test_repair_refuses_metadata_corruption_at_final_publication_boundary(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Corruption after candidate validation blocks the authoritative write."""
    _stub_repair(monkeypatch)

    def malformed_meta() -> lifecycle.WorkerMeta | None:
        msg = "maintained-worker metadata is malformed"
        raise lifecycle.WorkerMetadataError(msg)

    monkeypatch.setattr(lifecycle, "read_meta_strict", malformed_meta)
    monkeypatch.setattr(
        lifecycle,
        "write_meta",
        lambda _meta: pytest.fail("corrupt maintained metadata must not be overwritten"),
    )

    code = lifecycle._repair_locked(options(), PID)
    captured = capsys.readouterr()

    assert code == lifecycle.EXIT_ERROR
    assert "became untrustworthy before adoption publication" in captured.err


def test_recover_success_keeps_exact_authority_until_adoption(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    recover_env: tuple[list[FakePopen], list[tuple[str, str]]],
) -> None:
    """A successful recover leaves the live worker durably represented."""
    spawned, _events = recover_env
    monkeypatch.setattr(lifecycle, "proc_start_ticks", lambda _pid: PRIVATE.start_time_ticks)
    observe(monkeypatch, PRIVATE)

    code = lifecycle._recover_locked(options())
    captured = capsys.readouterr()

    assert code == lifecycle.EXIT_OK
    assert "adopt it with" in captured.out
    assert len(spawned) == 1
    obligation = supervise.read_state().spawning
    assert obligation is not None
    assert obligation.token == TOKEN
    assert obligation.pid == PID
    assert obligation.start_time_ticks == PRIVATE.start_time_ticks
    assert obligation.parent_death_signal is False


def test_recover_success_holds_supervisor_reconcile_while_worker_is_live(
    monkeypatch: pytest.MonkeyPatch,
    recover_env: tuple[list[FakePopen], list[tuple[str, str]]],
) -> None:
    """The retained authority stops a restarted supervisor from respawning."""
    spawned, _events = recover_env
    monkeypatch.setattr(lifecycle, "proc_start_ticks", lambda _pid: PRIVATE.start_time_ticks)
    observe(monkeypatch, PRIVATE)
    assert lifecycle._recover_locked(options()) == lifecycle.EXIT_OK

    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    monkeypatch.setattr(daemon, "_unresolved_alive", lambda _hold: True)
    monkeypatch.setattr(daemon, "_converge_unresolved", lambda _hold: False)
    monkeypatch.setattr(daemon, "_recover_spawn_owned_groups", lambda _token: False)

    assert not daemon._resolve_spawning_obligation()
    assert supervise.read_state().spawning is not None
    assert daemon._message is not None
    assert "still live" in daemon._message
    assert len(spawned) == 1


def test_repair_adopts_and_durably_clears_exact_authority(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    recover_env: tuple[list[FakePopen], list[tuple[str, str]]],
) -> None:
    """A verified adoption of the named worker releases the exact authority."""
    spawned, _events = recover_env
    monkeypatch.setattr(lifecycle, "proc_start_ticks", lambda _pid: PRIVATE.start_time_ticks)
    observe(monkeypatch, PRIVATE)
    assert lifecycle._recover_locked(options()) == lifecycle.EXIT_OK
    _stub_repair(monkeypatch)

    assert lifecycle._repair_locked(options(), PID) == lifecycle.EXIT_OK
    assert "adopted recovery worker" in capsys.readouterr().out
    assert supervise.read_state().spawning is None
    assert len(spawned) == 1


def test_repair_refuses_to_clear_a_mismatched_authority(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    recover_env: tuple[list[FakePopen], list[tuple[str, str]]],
) -> None:
    """An obligation naming a different instance stays blocking and wins."""
    _spawned, _events = recover_env
    _write_obligation()
    _stub_repair(monkeypatch)

    code = lifecycle._repair_locked(options(), PID)
    captured = capsys.readouterr()

    assert code == lifecycle.EXIT_ERROR
    assert "names another worker instance" in captured.err
    obligation = supervise.read_state().spawning
    assert obligation is not None
    assert (obligation.token, obligation.pid) == (TOKEN, PID)


def test_repair_failure_to_release_authority_reports_no_success(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    recover_env: tuple[list[FakePopen], list[tuple[str, str]]],
) -> None:
    """Adoption without a confirmed release never reports success."""
    _spawned, _events = recover_env
    monkeypatch.setattr(lifecycle, "proc_start_ticks", lambda _pid: PRIVATE.start_time_ticks)
    observe(monkeypatch, PRIVATE)
    assert lifecycle._recover_locked(options()) == lifecycle.EXIT_OK
    _stub_repair(monkeypatch)
    monkeypatch.setattr(lifecycle, "_clear_spawning_obligation", lambda: False)

    code = lifecycle._repair_locked(options(), PID)
    captured = capsys.readouterr()

    assert code == lifecycle.EXIT_ERROR
    assert "could not be released" in captured.err
    assert "adopted recovery worker" not in captured.out
    obligation = supervise.read_state().spawning
    assert obligation is not None
    assert obligation.pid == PID


def test_dead_before_repair_convergence_resolves_the_retained_authority(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    recover_env: tuple[list[FakePopen], list[tuple[str, str]]],
) -> None:
    """A worker that dies before repair is converged out of the authority."""
    spawned, events = recover_env
    ticks: dict[str, int | None] = {"value": PRIVATE.start_time_ticks}
    monkeypatch.setattr(lifecycle, "proc_start_ticks", lambda _pid: ticks["value"])
    observe(monkeypatch, PRIVATE)
    assert lifecycle._recover_locked(options()) == lifecycle.EXIT_OK
    spawned[0].returncode = 0
    ticks["value"] = None

    assert lifecycle._recover_locked(options()) == lifecycle.EXIT_OK
    assert "refusing to start another consumer" not in capsys.readouterr().err
    assert len(spawned) == 2
    assert (TOKEN, "recovered") in events
    obligation = supervise.read_state().spawning
    assert obligation is not None
    assert obligation.token == TOKEN


def test_legacy_repair_without_authority_still_succeeds(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Repair keeps working when no recovery authority was ever recorded."""
    _stub_repair(monkeypatch)

    assert lifecycle._repair_locked(options(), PID) == lifecycle.EXIT_OK
    assert "adopted recovery worker" in capsys.readouterr().out
    assert supervise.read_state().spawning is None


def test_repair_failure_when_authority_became_malformed(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    recover_env: tuple[list[FakePopen], list[tuple[str, str]]],
) -> None:
    """A concurrently malformed authority fails closed and stays durable."""
    _spawned, _events = recover_env
    monkeypatch.setattr(lifecycle, "proc_start_ticks", lambda _pid: PRIVATE.start_time_ticks)
    observe(monkeypatch, PRIVATE)
    assert lifecycle._recover_locked(options()) == lifecycle.EXIT_OK
    _stub_repair(monkeypatch)
    real_read = supervise.read_state
    reads = {"count": 0}

    def malformed() -> supervise.SupervisorState:
        """Model a concurrent writer corrupting the shared authority.

        Returns:
            The state with a malformed hold once repair reaches release.
        """
        reads["count"] += 1
        state = real_read()
        if reads["count"] > 1:
            return replace(state, spawning_hold_malformed=True)
        return state

    monkeypatch.setattr(supervise, "read_state", malformed)

    code = lifecycle._repair_locked(options(), PID)
    captured = capsys.readouterr()

    assert code == lifecycle.EXIT_ERROR
    assert "became malformed" in captured.err
    assert json.loads(supervise.state_path().read_text())["spawning"]["token"] == TOKEN


def test_repair_fails_closed_when_the_consumer_boundary_is_busy(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    recover_env: tuple[list[FakePopen], list[tuple[str, str]]],
) -> None:
    """A busy consumer-establishment boundary blocks the whole adoption."""
    _spawned, _events = recover_env
    monkeypatch.setattr(lifecycle, "proc_start_ticks", lambda _pid: PRIVATE.start_time_ticks)
    observe(monkeypatch, PRIVATE)
    assert lifecycle._recover_locked(options()) == lifecycle.EXIT_OK
    _stub_repair(monkeypatch)

    def must_not_run(**_kwargs: object) -> None:
        """Fail if any part of the transition ran without the lock.

        Raises:
            AssertionError: Always.
        """
        msg = "authority transition ran without the consumer lock"
        raise AssertionError(msg)

    monkeypatch.setattr(lifecycle, "_verify_queue_roundtrip", must_not_run)
    lock_path = supervise.supervisor_dir() / ".consumer.lock"
    with lock_path.open("a+") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        code = lifecycle._repair_locked(replace(options(), lock_timeout_seconds=0.0), PID)
        fcntl.flock(handle, fcntl.LOCK_UN)

    captured = capsys.readouterr()
    assert code == lifecycle.EXIT_ERROR
    assert "establishing a queue consumer" in captured.err
    obligation = supervise.read_state().spawning
    assert obligation is not None
    assert obligation.pid == PID


def _maintained_meta() -> lifecycle.WorkerMeta:
    """Build metadata describing a newer maintained supervisor worker.

    Returns:
        Maintained-worker metadata for an identity distinct from the
        recovery worker.
    """
    return lifecycle.WorkerMeta(
        schema_version=lifecycle.SCHEMA_VERSION,
        state=lifecycle.STATE_RUNNING,
        pid=5151,
        pgid=5151,
        sid=5151,
        start_time_ticks=777,
        token=TOKEN + "s",
        repo=".",
        git_commit=COMMIT,
        worker_id="maintained",
        log_path="maintained.log",
        started_at=0.0,
        stopped_at=None,
    )


def test_repair_never_publishes_a_candidate_that_exited_before_the_lock(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    recover_env: tuple[list[FakePopen], list[tuple[str, str]]],
) -> None:
    """A candidate proven outside the lock must be re-proved under the lock."""
    _spawned, _events = recover_env
    monkeypatch.setattr(lifecycle, "proc_start_ticks", lambda _pid: PRIVATE.start_time_ticks)
    observe(monkeypatch, PRIVATE)
    assert lifecycle._recover_locked(options()) == lifecycle.EXIT_OK

    maintained = _maintained_meta()
    observed = {"live": True}
    writes: list[lifecycle.WorkerMeta] = []
    cli_mutations: list[str] = []
    real_write_meta = lifecycle.write_meta

    def identity(_pid: int) -> ProcessIdentity | None:
        return PRIVATE if observed["live"] else None

    def publish(meta: lifecycle.WorkerMeta) -> None:
        writes.append(meta)

    real_consumer_lock = supervise.consumer_lock

    def consumer_lock(timeout_seconds: float) -> object:
        observed["live"] = False
        supervise.write_state(replace(supervise.read_state(), spawning=None))
        return real_consumer_lock(timeout_seconds=timeout_seconds)

    monkeypatch.setattr(lifecycle, "process_identity", identity)
    monkeypatch.setattr(lifecycle, "worker_alive", lambda _meta: True)
    monkeypatch.setattr(lifecycle, "write_meta", publish)
    monkeypatch.setattr(supervise, "consumer_lock", consumer_lock)

    def reconcile(_c: str) -> bool:
        cli_mutations.append("reconcile")
        return True

    def gc(_commits: tuple[str, ...]) -> None:
        cli_mutations.append("gc")

    monkeypatch.setattr(cli, "reconcile_pointer", reconcile)
    monkeypatch.setattr(cli, "gc_cli_roots", gc)
    monkeypatch.setattr(lifecycle, "_verify_queue_roundtrip", lambda *_a: True)
    monkeypatch.setattr(lifecycle, "_cleanup_ready_markers", lambda _pid: None)
    monkeypatch.setattr(lifecycle, "_reconcile_toolchain", lambda _uv: None)
    monkeypatch.setattr(lifecycle, "git_commit", lambda *_a: COMMIT)
    monkeypatch.setattr(lifecycle, "_adoption_candidate", lambda *_a: (_adopted_meta(), "worker"))
    monkeypatch.setattr(lifecycle, "append_deploy_log", lambda _line: None)

    real_write_meta(maintained)
    writes.clear()

    code = lifecycle._repair_locked(options(), PID)
    captured = capsys.readouterr()

    assert code == lifecycle.EXIT_ERROR
    assert "refusing stale metadata" in captured.err
    assert "adopted recovery worker" not in captured.out
    assert writes == []
    assert cli_mutations == []
    assert lifecycle.read_meta() == maintained


def test_repair_adopts_a_candidate_that_stays_exact_through_the_lock(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    recover_env: tuple[list[FakePopen], list[tuple[str, str]]],
) -> None:
    """A candidate still exactly live under the lock adopts successfully."""
    spawned, _events = recover_env
    monkeypatch.setattr(lifecycle, "proc_start_ticks", lambda _pid: PRIVATE.start_time_ticks)
    observe(monkeypatch, PRIVATE)
    assert lifecycle._recover_locked(options()) == lifecycle.EXIT_OK
    _stub_repair(monkeypatch)
    monkeypatch.setattr(lifecycle, "process_identity", lambda _pid: PRIVATE)

    code = lifecycle._repair_locked(options(), PID)

    captured = capsys.readouterr()
    assert code == lifecycle.EXIT_OK
    assert "adopted recovery worker" in captured.out
    published = lifecycle.read_meta()
    assert published is not None
    assert published.pid == PID
    assert published.start_time_ticks == PRIVATE.start_time_ticks
    assert supervise.read_state().spawning is None
    assert len(spawned) == 1


def test_repair_refuses_a_candidate_whose_lifecycle_token_no_longer_matches(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    recover_env: tuple[list[FakePopen], list[tuple[str, str]]],
) -> None:
    """A same-identity candidate without its exact token is never published.

    A recycled or re-exec'd process can retain its PID, start ticks, group,
    and session while no longer being the validated recovery worker, so the
    lifecycle marker itself must still be proven under the lock.
    """
    _spawned, _events = recover_env
    maintained = _maintained_meta()
    writes: list[lifecycle.WorkerMeta] = []
    cli_mutations: list[str] = []

    def reconcile(_c: str) -> bool:
        cli_mutations.append("reconcile")
        return True

    def gc(_commits: tuple[str, ...]) -> None:
        cli_mutations.append("gc")

    monkeypatch.setattr(lifecycle, "git_commit", lambda *_a: COMMIT)
    monkeypatch.setattr(lifecycle, "_adoption_candidate", lambda *_a: (_adopted_meta(), "worker"))
    monkeypatch.setattr(lifecycle, "process_identity", lambda _pid: PRIVATE)
    monkeypatch.setattr(lifecycle, "process_has_token", lambda _pid, _token: False)
    monkeypatch.setattr(lifecycle, "worker_alive", lambda _meta: True)
    real_write_meta = lifecycle.write_meta
    monkeypatch.setattr(lifecycle, "write_meta", writes.append)
    monkeypatch.setattr(cli, "reconcile_pointer", reconcile)
    monkeypatch.setattr(cli, "gc_cli_roots", gc)

    real_write_meta(maintained)
    writes.clear()

    code = lifecycle._repair_locked(options(), PID)
    captured = capsys.readouterr()

    assert code == lifecycle.EXIT_ERROR
    assert "no longer carries the exact" in captured.err
    assert "adopted recovery worker" not in captured.out
    assert writes == []
    assert cli_mutations == []
    assert lifecycle.read_meta() == maintained
