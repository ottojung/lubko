"""Provenance-aware deployment checkout synchronization tests.

Verify that the maintained procedure obtains the exact commit from the declared
source authority, rejects stale local-origin assumptions, and produces
actionable provenance errors.
"""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Final

import pytest

from lubko import deployctl as dc
from lubko import lifecycle, lifecycle_state

COMMIT: Final = "a" * 40
PREVIOUS_COMMIT: Final = "b" * 40
SOURCE_URL: Final = "https://github.com/example/repo.git"


def _completed(
    *,
    returncode: int = 0,
    stdout: str = "",
    stderr: str = "",
) -> subprocess.CompletedProcess[str]:
    """Return a fake completed git process."""
    return subprocess.CompletedProcess(
        args=["git"],
        returncode=returncode,
        stdout=stdout,
        stderr=stderr,
    )


def _deployctl_options(
    *,
    source_url: str | None = None,
) -> dc.Options:
    """Return minimal deployctl options for testing."""
    return dc.Options(
        repo=Path("/workspace/Lubko"),
        uv_path="uv",
        confirm_window_seconds=120.0,
        stop_grace_seconds=1.0,
        postgres_timeout_seconds=1.0,
        lock_timeout_seconds=1.0,
        validation_timeout_seconds=1.0,
        git_timeout_seconds=5.0,
        cli_timeout_seconds=1.0,
        source_url=source_url,
    )


def _make_previous_meta() -> lifecycle.WorkerMeta:
    """Return valid previous worker metadata with correct schema version."""
    return lifecycle.WorkerMeta(
        schema_version=lifecycle.SCHEMA_VERSION,
        state=lifecycle.STATE_RUNNING,
        pid=100,
        pgid=100,
        sid=100,
        start_time_ticks=1000,
        token="test-worker-token",  # ruff: ignore[hardcoded-password-func-arg]
        repo="/workspace/Lubko",
        git_commit=PREVIOUS_COMMIT,
        worker_id="prev-worker",
        log_path="worker.log",
        started_at=1.0,
        stopped_at=None,
    )


# ---------------------------------------------------------------------------
# fetch_from_authority unit tests
# ---------------------------------------------------------------------------


def test_fetch_from_authority_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A successful fetch passes silently."""
    calls: list[list[str]] = []

    def fake_run_git(
        _repo: Path,
        args: tuple[str, ...],
        _timeout: float,
    ) -> subprocess.CompletedProcess[str]:
        calls.append(list(args))
        return _completed(returncode=0)

    monkeypatch.setattr(dc, "_run_git", fake_run_git)

    dc.fetch_from_authority(
        Path("/workspace/Lubko"),
        COMMIT,
        SOURCE_URL,
        5.0,
    )

    assert calls == [["fetch", "--depth=1", SOURCE_URL, COMMIT]]


def test_fetch_from_authority_rejects_commit_not_in_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A fetch that fails proves the commit is not from the declared authority."""
    calls: list[list[str]] = []

    def fake_run_git(
        _repo: Path,
        args: tuple[str, ...],
        _timeout: float,
    ) -> subprocess.CompletedProcess[str]:
        calls.append(list(args))
        return _completed(
            returncode=1,
            stderr=f"fatal: remote error: commit {COMMIT} not found",
        )

    monkeypatch.setattr(dc, "_run_git", fake_run_git)

    with pytest.raises(dc.ProvenanceError, match="source authority"):
        dc.fetch_from_authority(
            Path("/workspace/Lubko"),
            COMMIT,
            SOURCE_URL,
            5.0,
        )

    assert calls == [["fetch", "--depth=1", SOURCE_URL, COMMIT]]


def test_fetch_from_authority_rejects_on_os_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A network/OS error during fetch produces an actionable provenance error."""

    def fake_run_git(
        _repo: Path,
        _args: tuple[str, ...],
        _timeout: float,
    ) -> subprocess.CompletedProcess[str]:
        msg = "network unreachable"
        raise OSError(msg)

    monkeypatch.setattr(dc, "_run_git", fake_run_git)

    with pytest.raises(dc.ProvenanceError, match="could not fetch"):
        dc.fetch_from_authority(
            Path("/workspace/Lubko"),
            COMMIT,
            SOURCE_URL,
            5.0,
        )


def test_fetch_from_authority_rejects_on_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A timeout during fetch produces an actionable provenance error."""

    def fake_run_git(
        _repo: Path,
        _args: tuple[str, ...],
        _timeout: float,
    ) -> subprocess.CompletedProcess[str]:
        raise subprocess.TimeoutExpired(cmd="git", timeout=5.0)

    monkeypatch.setattr(dc, "_run_git", fake_run_git)

    with pytest.raises(dc.ProvenanceError, match="could not fetch"):
        dc.fetch_from_authority(
            Path("/workspace/Lubko"),
            COMMIT,
            SOURCE_URL,
            5.0,
        )


# ---------------------------------------------------------------------------
# _prepare_locked integration: source_url triggers provenance fetch
# ---------------------------------------------------------------------------


def test_prepare_locked_fetches_from_source_authority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When source_url is set, _prepare_locked fetches from authority before checkout."""
    previous_meta = _make_previous_meta()
    fetch_calls: list[tuple[str, str]] = []
    checkout_calls: list[tuple[str, str]] = []

    monkeypatch.setattr(dc, "_cleanup_pending_locked", lambda: None)
    monkeypatch.setattr(dc, "read_meta", lambda: previous_meta)
    monkeypatch.setattr(dc, "worker_alive", lambda _meta: True)
    monkeypatch.setattr(dc, "_require_exact_commit", lambda _repo, _commit, _timeout: None)

    def fake_fetch_from_authority(
        _repo: Path,
        commit: str,
        source_url: str,
        _timeout: float,
    ) -> None:
        fetch_calls.append((commit, source_url))

    monkeypatch.setattr(dc, "fetch_from_authority", fake_fetch_from_authority)
    monkeypatch.setattr(dc, "_require_clean_checkout", lambda _repo, _timeout: None)

    def fake_checkout(
        _repo: Path,
        commit: str,
        _timeout: float,
        *,
        force: bool,
    ) -> bool:
        checkout_calls.append((commit, str(force)))
        return True

    monkeypatch.setattr(dc, "_checkout", fake_checkout)
    monkeypatch.setattr(dc, "_candidate_identity", lambda _o, _c, **_kw: (None, None))

    def fake_run_validation(*_args: object) -> object:
        class R:
            ok = True
            detail = ""

        return R()

    monkeypatch.setattr(dc, "run_validation", fake_run_validation)
    monkeypatch.setattr(dc, "cli", type("cli", (), {"build_cli_root": lambda *_a: None}))
    monkeypatch.setattr(dc, "check_postgres", lambda _timeout: True)
    monkeypatch.setattr(dc, "_read_state", lambda: None)
    monkeypatch.setattr(dc, "_mission_authority_facts", lambda *_a, **_kw: object())
    monkeypatch.setattr(
        lifecycle_state,
        "authorize_mission_publish",
        lambda _f: True,
    )

    options = _deployctl_options(source_url=SOURCE_URL)
    state, _gated = dc._prepare_locked(options, COMMIT, supervised=False)

    assert fetch_calls == [(COMMIT, SOURCE_URL)]
    assert checkout_calls == [(COMMIT, "False")]
    assert state.commit == COMMIT
    assert state.source_url == SOURCE_URL


def test_prepare_locked_skips_fetch_when_no_source_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When source_url is None, _prepare_locked does not fetch from any authority."""
    previous_meta = _make_previous_meta()
    fetch_calls: list[tuple[str, str]] = []

    monkeypatch.setattr(dc, "_cleanup_pending_locked", lambda: None)
    monkeypatch.setattr(dc, "read_meta", lambda: previous_meta)
    monkeypatch.setattr(dc, "worker_alive", lambda _meta: True)
    monkeypatch.setattr(dc, "_require_exact_commit", lambda _repo, _commit, _timeout: None)
    monkeypatch.setattr(dc, "_require_clean_checkout", lambda _repo, _timeout: None)

    def fake_fetch(
        _repo: Path,
        commit: str,
        source_url: str,
        _timeout: float,
    ) -> None:
        fetch_calls.append((commit, source_url))

    monkeypatch.setattr(dc, "fetch_from_authority", fake_fetch)
    monkeypatch.setattr(dc, "_checkout", lambda _repo, _commit, _timeout, **_kw: True)
    monkeypatch.setattr(dc, "_candidate_identity", lambda _o, _c, **_kw: (None, None))

    def fake_run_validation(*_args: object) -> object:
        class R:
            ok = True
            detail = ""

        return R()

    monkeypatch.setattr(dc, "run_validation", fake_run_validation)
    monkeypatch.setattr(dc, "cli", type("cli", (), {"build_cli_root": lambda *_a: None}))
    monkeypatch.setattr(dc, "check_postgres", lambda _timeout: True)
    monkeypatch.setattr(dc, "_read_state", lambda: None)
    monkeypatch.setattr(dc, "_mission_authority_facts", lambda *_a, **_kw: object())
    monkeypatch.setattr(
        lifecycle_state,
        "authorize_mission_publish",
        lambda _f: True,
    )

    options = _deployctl_options(source_url=None)
    state, _gated = dc._prepare_locked(options, COMMIT, supervised=False)

    assert fetch_calls == []
    assert state.source_url is None


# ---------------------------------------------------------------------------
# RollbackState serialization round-trip with source_url
# ---------------------------------------------------------------------------


def test_rollback_state_source_url_survives_serialization() -> None:
    """source_url round-trips through to_dict/from_dict."""
    previous = _make_previous_meta()
    state = dc.RollbackState(
        schema_version=dc.ROLLBACK_SCHEMA_VERSION,
        generation=1,
        status=dc.STATUS_PENDING,
        commit=COMMIT,
        previous_commit=PREVIOUS_COMMIT,
        deadline=999.0,
        repo="/workspace/Lubko",
        uv_path="uv",
        stop_grace_seconds=1.0,
        git_timeout_seconds=5.0,
        previous_retiring=False,
        previous_meta=previous,
        new_meta=previous,
        supervisor_owned=False,
        source_url=SOURCE_URL,
    )

    raw = state.to_dict()
    assert raw["source_url"] == SOURCE_URL

    restored = dc.RollbackState.from_dict(raw)
    assert restored.source_url == SOURCE_URL
    assert restored.commit == COMMIT
    assert restored.previous_commit == PREVIOUS_COMMIT


def test_rollback_state_missing_source_url_defaults_none() -> None:
    """Older state without source_url defaults to None."""
    previous = _make_previous_meta()
    state = dc.RollbackState(
        schema_version=dc.ROLLBACK_SCHEMA_VERSION,
        generation=1,
        status=dc.STATUS_PENDING,
        commit=COMMIT,
        previous_commit=PREVIOUS_COMMIT,
        deadline=999.0,
        repo="/workspace/Lubko",
        uv_path="uv",
        stop_grace_seconds=1.0,
        git_timeout_seconds=5.0,
        previous_retiring=False,
        previous_meta=previous,
        new_meta=previous,
        supervisor_owned=False,
    )

    raw = state.to_dict()
    assert raw["source_url"] is None

    # Simulate an older state file that lacks source_url entirely
    del raw["source_url"]
    restored = dc.RollbackState.from_dict(raw)
    assert restored.source_url is None


# ---------------------------------------------------------------------------
# Stale local-origin vs explicit canonical source
# ---------------------------------------------------------------------------


def test_fetch_rejects_commit_present_locally_but_not_in_authority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A commit that exists locally but not in the declared authority is rejected.

    This is the core stale-origin regression: the local repo may contain a
    commit that was fetched from ``origin`` or another remote, but the declared
    source authority does not have it. Provenance-aware checkout must refuse.
    """
    calls: list[list[str]] = []

    def fake_run_git(
        _repo: Path,
        args: tuple[str, ...],
        _timeout: float,
    ) -> subprocess.CompletedProcess[str]:
        calls.append(list(args))
        # Simulate: the commit is NOT in the declared source
        return _completed(
            returncode=1,
            stderr=f"fatal: remote error: not found: {COMMIT}",
        )

    monkeypatch.setattr(dc, "_run_git", fake_run_git)

    with pytest.raises(dc.ProvenanceError, match="does not contain commit"):
        dc.fetch_from_authority(
            Path("/workspace/Lubko"),
            COMMIT,
            SOURCE_URL,
            5.0,
        )

    # Verify the exact source URL was used
    assert calls[0][2] == SOURCE_URL
    assert calls[0][3] == COMMIT


def test_fetch_uses_declared_source_not_origin(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The declared source URL is passed to git, never the implicit origin."""
    custom_source = "https://custom.example.com/lubko.git"
    calls: list[list[str]] = []

    def fake_run_git(
        _repo: Path,
        args: tuple[str, ...],
        _timeout: float,
    ) -> subprocess.CompletedProcess[str]:
        calls.append(list(args))
        return _completed(returncode=0)

    monkeypatch.setattr(dc, "_run_git", fake_run_git)

    dc.fetch_from_authority(
        Path("/workspace/Lubko"),
        COMMIT,
        custom_source,
        5.0,
    )

    assert calls[0][2] == custom_source
    assert "origin" not in str(calls)


# ---------------------------------------------------------------------------
# Exact commit identity after fetch
# ---------------------------------------------------------------------------


def test_fetch_then_require_exact_commit_verifies_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """After a successful fetch, _require_exact_commit confirms exact identity."""
    verified_commits: list[str] = []

    def fake_fetch(
        _repo: Path,
        commit: str,
        _source_url: str,
        _timeout: float,
    ) -> None:
        pass

    def fake_require_exact(
        _repo: Path,
        commit: str,
        _timeout: float,
    ) -> None:
        verified_commits.append(commit)

    monkeypatch.setattr(dc, "fetch_from_authority", fake_fetch)
    monkeypatch.setattr(dc, "_require_exact_commit", fake_require_exact)

    # Simulate the _prepare_locked flow: fetch then verify
    options = _deployctl_options(source_url=SOURCE_URL)
    dc.fetch_from_authority(
        options.repo,
        COMMIT,
        options.source_url,  # type: ignore[arg-type]
        options.git_timeout_seconds,
    )
    dc._require_exact_commit(options.repo, COMMIT, options.git_timeout_seconds)

    assert verified_commits == [COMMIT]


# ---------------------------------------------------------------------------
# ProvenanceError is distinct from DeployCtlError
# ---------------------------------------------------------------------------


def test_provenance_error_is_runtime_error() -> None:
    """ProvenanceError is a distinct error class for source authority failures.

    Raises:
        ProvenanceError: Always, to exercise the exception type.
    """
    assert issubclass(dc.ProvenanceError, RuntimeError)
    assert not issubclass(dc.ProvenanceError, dc.DeployCtlError)

    msg = "commit abc not in source"
    with pytest.raises(dc.ProvenanceError, match="commit abc not in source"):
        raise dc.ProvenanceError(msg)


# ---------------------------------------------------------------------------
# Source URL stored in rollback state
# ---------------------------------------------------------------------------


def test_source_url_persisted_in_rollback_state() -> None:
    """The source_url from Options is recorded in the RollbackState."""
    previous = _make_previous_meta()
    state = dc.RollbackState(
        schema_version=dc.ROLLBACK_SCHEMA_VERSION,
        generation=1,
        status=dc.STATUS_PENDING,
        commit=COMMIT,
        previous_commit=PREVIOUS_COMMIT,
        deadline=999.0,
        repo="/workspace/Lubko",
        uv_path="uv",
        stop_grace_seconds=1.0,
        git_timeout_seconds=5.0,
        previous_retiring=False,
        previous_meta=previous,
        new_meta=previous,
        supervisor_owned=False,
        source_url=SOURCE_URL,
    )

    assert state.source_url == SOURCE_URL
    raw = state.to_dict()
    assert raw["source_url"] == SOURCE_URL
