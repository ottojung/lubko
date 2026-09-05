"""Regression tests for queue-deploy recovery child-liveness authority."""

from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest

from lubko import cli, lifecycle, supervise


def _options() -> lifecycle.DeployOptions:
    return cast("lifecycle.DeployOptions", SimpleNamespace(repo=Path("/repo"), uv_path="uv"))


def _previous(commit: str) -> lifecycle.WorkerMeta:
    return cast("lifecycle.WorkerMeta", SimpleNamespace(git_commit=commit))


def _status(commit: str) -> SimpleNamespace:
    return SimpleNamespace(commit=commit, child=object(), ready=True)


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
    monkeypatch.setattr(supervise, "wait_until_ready", lambda *_args: True)
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
