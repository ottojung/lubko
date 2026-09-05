"""Regression tests for queue-deploy recovery child-liveness authority."""

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


def test_restore_after_handoff_failure_rejects_stale_ready_dead_child(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A stale ready snapshot cannot suppress restoration after child death."""
    candidate = "a" * 40
    previous_commit = "b" * 40
    previous = _previous(previous_commit)
    status = _status(candidate)
    requested: list[str] = []

    monkeypatch.setattr(supervise, "supervisor_running", lambda: True)
    monkeypatch.setattr(supervise, "read_status", lambda: status)
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
    status = _status(candidate)

    def fail_restore(*_args: object, **_kwargs: object) -> int:
        raise AssertionError

    monkeypatch.setattr(supervise, "supervisor_running", lambda: True)
    monkeypatch.setattr(supervise, "read_status", lambda: status)
    monkeypatch.setattr(supervise, "child_alive", lambda child: child is status.child)
    monkeypatch.setattr(cli, "current_commit", lambda: candidate)
    monkeypatch.setattr(supervise, "request_run", fail_restore)
    monkeypatch.setattr(lifecycle, "append_deploy_log", lambda _line: None)

    lifecycle._restore_after_handoff_failure(_options(), candidate, previous)


@dataclass(frozen=True, slots=True)
class _RestoreCase:
    generation: int
    ready_commit: str
    restored_child_alive: bool
    ready: bool
    holding: bool
    expected: bool


@pytest.mark.parametrize(
    "case",
    [
        _RestoreCase(
            generation=7,
            ready_commit="b" * 40,
            restored_child_alive=True,
            ready=True,
            holding=False,
            expected=True,
        ),
        _RestoreCase(
            generation=8,
            ready_commit="b" * 40,
            restored_child_alive=True,
            ready=True,
            holding=False,
            expected=True,
        ),
        _RestoreCase(
            generation=8,
            ready_commit="c" * 40,
            restored_child_alive=True,
            ready=True,
            holding=False,
            expected=False,
        ),
        _RestoreCase(
            generation=6,
            ready_commit="b" * 40,
            restored_child_alive=True,
            ready=True,
            holding=False,
            expected=False,
        ),
        _RestoreCase(
            generation=7,
            ready_commit="b" * 40,
            restored_child_alive=False,
            ready=True,
            holding=False,
            expected=False,
        ),
        _RestoreCase(
            generation=7,
            ready_commit="b" * 40,
            restored_child_alive=True,
            ready=False,
            holding=False,
            expected=False,
        ),
        _RestoreCase(
            generation=7,
            ready_commit="b" * 40,
            restored_child_alive=True,
            ready=True,
            holding=True,
            expected=False,
        ),
    ],
    ids=[
        "exact-live",
        "newer-same-commit-live",
        "newer-different-commit",
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
            case.ready_commit,
            generation=case.generation,
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
