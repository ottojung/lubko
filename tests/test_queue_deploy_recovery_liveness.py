"""Regression tests for queue-deploy recovery child-liveness authority."""

from collections.abc import Iterator
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest

from lubko import cli, lifecycle, supervise


def _options() -> lifecycle.DeployOptions:
    return cast("lifecycle.DeployOptions", SimpleNamespace(repo=Path("/repo"), uv_path="uv"))


def _previous(commit: str) -> lifecycle.WorkerMeta:
    return cast("lifecycle.WorkerMeta", SimpleNamespace(git_commit=commit))


def _status(
    commit: str,
    *,
    generation: int = 0,
    child: object | None = None,
    ready: bool = True,
    holding: bool = False,
) -> SimpleNamespace:
    return SimpleNamespace(
        applied_generation=generation,
        commit=commit,
        child=child if child is not None else object(),
        ready=ready,
        holding=holding,
    )


def _desired(commit: str, generation: int) -> SimpleNamespace:
    return SimpleNamespace(commit=commit, generation=generation)


def test_restore_after_handoff_failure_rejects_stale_ready_dead_child(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A stale ready snapshot cannot suppress restoration after child death."""
    candidate = "a" * 40
    previous_commit = "b" * 40
    previous = _previous(previous_commit)
    status = _status(candidate)
    requested: list[str] = []
    desired = iter([_desired(candidate, 0), _desired(previous_commit, 7)])

    monkeypatch.setattr(supervise, "supervisor_running", lambda: True)
    monkeypatch.setattr(supervise, "read_status", lambda: status)
    monkeypatch.setattr(supervise, "read_desired_strict", lambda: next(desired))
    monkeypatch.setattr(supervise, "generation_lock", nullcontext)
    monkeypatch.setattr(supervise, "child_alive", lambda _child: False)
    monkeypatch.setattr(cli, "current_commit", lambda: candidate)

    def record_restore(commit: str, **_kwargs: object) -> int:
        requested.append(commit)
        return 7

    monkeypatch.setattr(supervise, "request_run", record_restore)
    monkeypatch.setattr(supervise, "wait_for_generation", lambda *_args: True)
    monkeypatch.setattr(supervise, "wait_until_ready", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(cli, "reconcile_pointer", lambda _commit: True)
    monkeypatch.setattr(lifecycle, "append_deploy_log", lambda _line: None)

    lifecycle._restore_after_handoff_failure(_options(), candidate, previous)

    assert requested == [previous_commit]


def test_restore_after_handoff_failure_accepts_live_ready_child(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A synchronously live ready child remains a valid convergence proof."""
    candidate = "a" * 40
    previous = _previous("b" * 40)
    status = _status(candidate, generation=7)

    def fail_restore(*_args: object, **_kwargs: object) -> int:
        raise AssertionError

    monkeypatch.setattr(supervise, "supervisor_running", lambda: True)
    monkeypatch.setattr(supervise, "read_status", lambda: status)
    monkeypatch.setattr(supervise, "read_desired_strict", lambda: _desired(candidate, 7))
    monkeypatch.setattr(supervise, "generation_lock", nullcontext)
    monkeypatch.setattr(supervise, "child_alive", lambda child: child is status.child)
    monkeypatch.setattr(cli, "current_commit", lambda: candidate)
    monkeypatch.setattr(supervise, "request_run", fail_restore)
    monkeypatch.setattr(lifecycle, "append_deploy_log", lambda _line: None)

    lifecycle._restore_after_handoff_failure(_options(), candidate, previous)


@dataclass(frozen=True, slots=True)
class _CandidateCase:
    desired_generation: int
    desired_commit: str
    applied_generation: int
    applied_commit: str
    child_alive: bool
    cli_commit: str
    expected: bool


@pytest.mark.parametrize(
    "case",
    [
        _CandidateCase(
            desired_generation=7,
            desired_commit="a" * 40,
            applied_generation=7,
            applied_commit="a" * 40,
            child_alive=True,
            cli_commit="a" * 40,
            expected=True,
        ),
        _CandidateCase(
            desired_generation=8,
            desired_commit="a" * 40,
            applied_generation=7,
            applied_commit="a" * 40,
            child_alive=True,
            cli_commit="a" * 40,
            expected=True,
        ),
        _CandidateCase(
            desired_generation=8,
            desired_commit="a" * 40,
            applied_generation=8,
            applied_commit="a" * 40,
            child_alive=True,
            cli_commit="a" * 40,
            expected=True,
        ),
        _CandidateCase(
            desired_generation=8,
            desired_commit="c" * 40,
            applied_generation=7,
            applied_commit="a" * 40,
            child_alive=True,
            cli_commit="a" * 40,
            expected=False,
        ),
        _CandidateCase(
            desired_generation=7,
            desired_commit="a" * 40,
            applied_generation=7,
            applied_commit="a" * 40,
            child_alive=False,
            cli_commit="a" * 40,
            expected=False,
        ),
        _CandidateCase(
            desired_generation=7,
            desired_commit="a" * 40,
            applied_generation=7,
            applied_commit="a" * 40,
            child_alive=True,
            cli_commit="c" * 40,
            expected=False,
        ),
    ],
    ids=[
        "exact-authority",
        "newer-same-commit-desired",
        "newer-same-commit-applied",
        "newer-different-commit-desired",
        "dead-child",
        "cli-mismatch",
    ],
)
def test_candidate_convergence_requires_current_compatible_authority(
    monkeypatch: pytest.MonkeyPatch, case: _CandidateCase
) -> None:
    """Candidate convergence follows the shared desired/applied authority policy."""
    candidate = "a" * 40
    status = _status(case.applied_commit, generation=case.applied_generation)

    monkeypatch.setattr(supervise, "generation_lock", nullcontext)
    monkeypatch.setattr(
        supervise,
        "read_desired_strict",
        lambda: _desired(case.desired_commit, case.desired_generation),
    )
    monkeypatch.setattr(supervise, "read_status", lambda: status)
    monkeypatch.setattr(supervise, "child_alive", lambda _child: case.child_alive)
    monkeypatch.setattr(cli, "current_commit", lambda: case.cli_commit)

    assert lifecycle._queue_deploy_candidate_converged(candidate) is case.expected


def test_candidate_convergence_holds_generation_lock_through_cli_decision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Desired, applied, liveness, and CLI proof share one generation lock."""
    candidate = "a" * 40
    status = _status(candidate, generation=7)
    held = False

    @contextmanager
    def generation_lock(**_kwargs: object) -> Iterator[None]:
        nonlocal held
        assert not held
        held = True
        try:
            yield
        finally:
            held = False

    def desired() -> SimpleNamespace:
        assert held
        return _desired(candidate, 7)

    def current_status() -> SimpleNamespace:
        assert held
        return status

    def child_alive(_child: object) -> bool:
        assert held
        return True

    def current_commit() -> str:
        assert held
        return candidate

    monkeypatch.setattr(supervise, "generation_lock", generation_lock)
    monkeypatch.setattr(supervise, "read_desired_strict", desired)
    monkeypatch.setattr(supervise, "read_status", current_status)
    monkeypatch.setattr(supervise, "child_alive", child_alive)
    monkeypatch.setattr(cli, "current_commit", current_commit)

    assert lifecycle._queue_deploy_candidate_converged(candidate)
    assert not held


@dataclass(frozen=True, slots=True)
class _RestoreCase:
    desired_generation: int
    desired_commit: str
    applied_generation: int
    applied_commit: str
    restored_child_alive: bool
    ready: bool
    holding: bool
    expected: bool


@pytest.mark.parametrize(
    "case",
    [
        _RestoreCase(
            desired_generation=7,
            desired_commit="b" * 40,
            applied_generation=7,
            applied_commit="b" * 40,
            restored_child_alive=True,
            ready=True,
            holding=False,
            expected=True,
        ),
        _RestoreCase(
            desired_generation=8,
            desired_commit="b" * 40,
            applied_generation=7,
            applied_commit="b" * 40,
            restored_child_alive=True,
            ready=True,
            holding=False,
            expected=True,
        ),
        _RestoreCase(
            desired_generation=8,
            desired_commit="b" * 40,
            applied_generation=8,
            applied_commit="b" * 40,
            restored_child_alive=True,
            ready=True,
            holding=False,
            expected=True,
        ),
        _RestoreCase(
            desired_generation=8,
            desired_commit="c" * 40,
            applied_generation=7,
            applied_commit="b" * 40,
            restored_child_alive=True,
            ready=True,
            holding=False,
            expected=False,
        ),
        _RestoreCase(
            desired_generation=8,
            desired_commit="c" * 40,
            applied_generation=8,
            applied_commit="c" * 40,
            restored_child_alive=True,
            ready=True,
            holding=False,
            expected=False,
        ),
        _RestoreCase(
            desired_generation=7,
            desired_commit="b" * 40,
            applied_generation=8,
            applied_commit="b" * 40,
            restored_child_alive=True,
            ready=True,
            holding=False,
            expected=False,
        ),
        _RestoreCase(
            desired_generation=7,
            desired_commit="b" * 40,
            applied_generation=6,
            applied_commit="b" * 40,
            restored_child_alive=True,
            ready=True,
            holding=False,
            expected=False,
        ),
        _RestoreCase(
            desired_generation=7,
            desired_commit="b" * 40,
            applied_generation=7,
            applied_commit="b" * 40,
            restored_child_alive=False,
            ready=True,
            holding=False,
            expected=False,
        ),
        _RestoreCase(
            desired_generation=7,
            desired_commit="b" * 40,
            applied_generation=7,
            applied_commit="b" * 40,
            restored_child_alive=True,
            ready=False,
            holding=False,
            expected=False,
        ),
        _RestoreCase(
            desired_generation=7,
            desired_commit="b" * 40,
            applied_generation=7,
            applied_commit="b" * 40,
            restored_child_alive=True,
            ready=True,
            holding=True,
            expected=False,
        ),
    ],
    ids=[
        "exact-live",
        "newer-same-commit-desired",
        "newer-same-commit-applied",
        "newer-different-desired",
        "newer-different-applied",
        "applied-ahead-of-desired",
        "older-generation",
        "dead-child",
        "not-ready",
        "holding",
    ],
)
def test_restoration_requires_current_live_queue_ready_authority(
    monkeypatch: pytest.MonkeyPatch,
    case: _RestoreCase,
) -> None:
    """Only compatible live queue-ready restore authority may reconcile the CLI."""
    candidate = "a" * 40
    previous_commit = "b" * 40
    previous = _previous(previous_commit)
    candidate_child = object()
    restored_child = object()
    observations = iter([
        _status(candidate, child=candidate_child),
        _status(
            case.applied_commit,
            generation=case.applied_generation,
            child=restored_child,
            ready=case.ready,
            holding=case.holding,
        ),
    ])
    reconciled: list[str] = []

    monkeypatch.setattr(supervise, "supervisor_running", lambda: True)
    monkeypatch.setattr(supervise, "read_status", lambda: next(observations))
    monkeypatch.setattr(
        supervise,
        "read_desired_strict",
        lambda: _desired(case.desired_commit, case.desired_generation),
    )
    monkeypatch.setattr(supervise, "generation_lock", nullcontext)
    monkeypatch.setattr(
        supervise,
        "child_alive",
        lambda child: child is restored_child and case.restored_child_alive,
    )
    monkeypatch.setattr(cli, "current_commit", lambda: candidate)
    monkeypatch.setattr(supervise, "request_run", lambda *_args, **_kwargs: 7)
    monkeypatch.setattr(supervise, "wait_for_generation", lambda *_args: True)
    monkeypatch.setattr(supervise, "wait_until_ready", lambda *_args, **_kwargs: True)

    def record_reconcile(commit: str) -> bool:
        reconciled.append(commit)
        return True

    monkeypatch.setattr(cli, "reconcile_pointer", record_reconcile)
    monkeypatch.setattr(lifecycle, "append_deploy_log", lambda _line: None)

    lifecycle._restore_after_handoff_failure(_options(), candidate, previous)

    assert reconciled == ([previous_commit] if case.expected else [])


def test_restore_holds_generation_lock_through_cli_reconciliation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Final authority proof and CLI reconciliation share one generation lock."""
    candidate = "a" * 40
    previous_commit = "b" * 40
    previous = _previous(previous_commit)
    candidate_child = object()
    restored_child = object()
    observations = iter([
        _status(candidate, child=candidate_child),
        _status(previous_commit, generation=7, child=restored_child),
    ])
    held = False

    @contextmanager
    def generation_lock(**_kwargs: object) -> Iterator[None]:
        nonlocal held
        assert not held
        held = True
        try:
            yield
        finally:
            held = False

    monkeypatch.setattr(supervise, "supervisor_running", lambda: True)
    monkeypatch.setattr(supervise, "read_status", lambda: next(observations))
    monkeypatch.setattr(supervise, "read_desired_strict", lambda: _desired(previous_commit, 7))
    monkeypatch.setattr(supervise, "child_alive", lambda child: child is restored_child)
    monkeypatch.setattr(supervise, "generation_lock", generation_lock)
    monkeypatch.setattr(cli, "current_commit", lambda: candidate)
    monkeypatch.setattr(supervise, "request_run", lambda *_args, **_kwargs: 7)
    monkeypatch.setattr(supervise, "wait_for_generation", lambda *_args: True)
    monkeypatch.setattr(supervise, "wait_until_ready", lambda *_args, **_kwargs: True)

    reconciled: list[str] = []

    def reconcile(commit: str) -> bool:
        assert held
        reconciled.append(commit)
        return True

    monkeypatch.setattr(cli, "reconcile_pointer", reconcile)
    monkeypatch.setattr(lifecycle, "append_deploy_log", lambda _line: None)

    lifecycle._restore_after_handoff_failure(_options(), candidate, previous)

    assert reconciled == [previous_commit]
    assert not held
