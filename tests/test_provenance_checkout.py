"""Provenance-aware deployment checkout regression tests.

Combines focused unit tests for type/serialization contracts with real-Git
topology tests that use temporary repositories to prove the production
deployment scenario from issue #729:

* Deployment checkout A has origin pointing to a stale mutable local clone.
* An explicit separately-addressed authority has exact target B.
* The maintained preparation path obtains/checks out exactly B from that
  authority, not from origin.
* Checkout is detached and clean.
* An authority lacking B fails clearly without switching to another tip.
* After preparation/confirmation the confirmed runtime/restart path does not
  need the mutable development checkout or network.
"""

from __future__ import annotations

import subprocess
from typing import TYPE_CHECKING, Final

import pytest

from lubko import deployctl as dc
from lubko.lifecycle import SCHEMA_VERSION, STATE_RUNNING, WorkerMeta

if TYPE_CHECKING:
    from pathlib import Path

COMMIT: Final = "a" * 40
PREVIOUS_COMMIT: Final = "b" * 40
SOURCE_URL: Final = "https://github.com/example/repo.git"
TIMEOUT: Final = 10.0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _run(cmd: list[str], *, cwd: Path) -> subprocess.CompletedProcess[str]:
    """Run a subprocess, asserting success.

    Returns:
        The completed process result.
    """
    return subprocess.run(
        cmd,
        cwd=cwd,
        capture_output=True,
        text=True,
        check=False,
    )


def _git(cwd: Path, *args: str) -> subprocess.CompletedProcess[str]:
    """Run a git command.

    Returns:
        The completed process result.
    """
    return _run(["git", *args], cwd=cwd)


def _init_repo(path: Path) -> None:
    """Initialize a git repo with an initial commit."""
    path.mkdir(parents=True, exist_ok=True)
    _git(path, "init", "--initial-branch=main")
    _git(path, "config", "user.email", "test@test.example")
    _git(path, "config", "user.name", "Test")
    _git(path, "commit", "--allow-empty", "-m", "initial")


def _make_commit(repo: Path, message: str) -> str:
    """Create a commit and return its full hash.

    Returns:
        The 40-character commit hash.
    """
    _git(repo, "config", "user.email", "test@test.example")
    _git(repo, "config", "user.name", "Test")
    _git(repo, "commit", "--allow-empty", "-m", message)
    result = _git(repo, "rev-parse", "HEAD")
    return result.stdout.strip()


def _setup_real_topology(tmp_path: Path) -> tuple[Path, Path, str]:
    """Create the production issue-729 topology with hermetic bare repos.

    The resulting graph is::

        authority.git (bare, canonical)  ←──  authority-work (pushes A, then B)
             ↑                                      ↑
        dev-clone (mutable, stale at A)            |
             ↑                                     |
        deploy-checkout (origin = dev-clone)       |

    After setup the deployment checkout's origin still points at the dev-clone
    which only has commit A.  Commit B exists exclusively in the bare
    authority and the authority-work clone.

    Returns:
        (authority_bare, deployment_checkout, target_commit_B)
    """
    # 1. Bare canonical authority (simulates GitHub / production remote).
    authority = tmp_path / "authority.git"
    authority.mkdir(parents=True, exist_ok=True)
    _git(authority, "init", "--bare")

    # 2. Authority-work clone: used solely to create and push commits into
    #    the bare authority.  Push A first so dev-clone can clone it.
    authority_work = tmp_path / "authority-work"
    _run(["git", "clone", str(authority), str(authority_work)], cwd=tmp_path)
    _make_commit(authority_work, "commit A (initial)")
    _run(["git", "push", "origin", "HEAD:main"], cwd=authority_work)

    # 3. Mutable dev-clone frozen at A.  This is the local clone whose path
    #    will become the stale origin of the deployment checkout.
    dev_clone = tmp_path / "dev-clone"
    _run(["git", "clone", str(authority), str(dev_clone)], cwd=tmp_path)

    # 4. Deployment checkout cloned from dev-clone so its origin remote
    #    points at the mutable dev-clone path, NOT the canonical authority.
    deploy_checkout = tmp_path / "deploy-checkout"
    _run(["git", "clone", str(dev_clone), str(deploy_checkout)], cwd=tmp_path)

    # 5. Create commit B and push it directly to the bare authority through
    #    authority-work.  The dev-clone is never updated — it stays at A.
    commit_b = _make_commit(authority_work, "commit B (target)")
    _run(["git", "push", "origin", "HEAD:main"], cwd=authority_work)

    return authority, deploy_checkout, commit_b


# ---------------------------------------------------------------------------
# Unit: type contracts
# ---------------------------------------------------------------------------


def test_provenance_error_is_runtime_error() -> None:
    """ProvenanceError is a distinct error class for source authority failures."""
    assert issubclass(dc.ProvenanceError, RuntimeError)
    assert not issubclass(dc.ProvenanceError, dc.DeployCtlError)


# ---------------------------------------------------------------------------
# Unit: RollbackState serialization
# ---------------------------------------------------------------------------


def _make_rollback_state(
    *,
    commit: str = COMMIT,
    source_url: str | None = SOURCE_URL,
) -> dc.RollbackState:
    """Build a minimal RollbackState for serialization tests.

    Returns:
        A rollback state instance.
    """
    previous = WorkerMeta(
        schema_version=SCHEMA_VERSION,
        state=STATE_RUNNING,
        pid=100,
        pgid=100,
        sid=100,
        start_time_ticks=1000,
        token="test-token",  # ruff: ignore[hardcoded-password-func-arg]
        repo="/workspace/Lubko",
        git_commit=PREVIOUS_COMMIT,
        worker_id="prev-worker",
        log_path="worker.log",
        started_at=1.0,
        stopped_at=None,
    )
    return dc.RollbackState(
        schema_version=dc.ROLLBACK_SCHEMA_VERSION,
        generation=1,
        status=dc.STATUS_PENDING,
        commit=commit,
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
        source_url=source_url,
    )


def test_rollback_state_source_url_survives_serialization() -> None:
    """source_url round-trips through to_dict/from_dict."""
    state = _make_rollback_state()
    raw = state.to_dict()
    assert raw["source_url"] == SOURCE_URL
    restored = dc.RollbackState.from_dict(raw)
    assert restored.source_url == SOURCE_URL
    assert restored.commit == COMMIT


def test_rollback_state_missing_source_url_defaults_none() -> None:
    """Older state without source_url defaults to None."""
    state = _make_rollback_state(source_url=None)
    raw = state.to_dict()
    assert raw["source_url"] is None
    del raw["source_url"]
    restored = dc.RollbackState.from_dict(raw)
    assert restored.source_url is None


# ---------------------------------------------------------------------------
# Real-Git: authority fetch topology
# ---------------------------------------------------------------------------


def test_fetch_from_real_authority_succeeds(tmp_path: Path) -> None:
    """Fetching a commit that exists in the real authority succeeds."""
    authority, checkout, target = _setup_real_topology(tmp_path)
    dc.fetch_from_authority(checkout, target, str(authority), TIMEOUT)
    dc._require_exact_commit(checkout, target, TIMEOUT)


def test_stale_origin_not_used_for_fetch(tmp_path: Path) -> None:
    """The declared authority URL is used, not the checkout's origin remote."""
    authority, checkout, target = _setup_real_topology(tmp_path)

    # Verify origin points to the dev-clone (stale mutable local clone)
    origin_url = _git(checkout, "remote", "get-url", "origin").stdout.strip()
    assert origin_url == str(tmp_path / "dev-clone")

    # Fetch succeeds using the explicit authority, proving origin is not consulted
    dc.fetch_from_authority(checkout, target, str(authority), TIMEOUT)
    dc._require_exact_commit(checkout, target, TIMEOUT)


# ---------------------------------------------------------------------------
# Real-Git: detached clean checkout
# ---------------------------------------------------------------------------


def test_checkout_is_detached_and_clean(tmp_path: Path) -> None:
    """After fetch + checkout, HEAD is detached at the exact commit, worktree clean."""
    authority, checkout, target = _setup_real_topology(tmp_path)
    dc.fetch_from_authority(checkout, target, str(authority), TIMEOUT)
    dc._require_exact_commit(checkout, target, TIMEOUT)

    # Detach HEAD at the target
    result = dc._checkout(checkout, target, TIMEOUT, force=False)
    assert result is True

    # HEAD points to the exact commit
    head = _git(checkout, "rev-parse", "HEAD").stdout.strip()
    assert head == target

    # Worktree is clean
    status = _git(checkout, "status", "--porcelain").stdout
    assert not status

    # HEAD is detached (not on a branch)
    symbolic = _git(checkout, "symbolic-ref", "HEAD")
    assert symbolic.returncode != 0


# ---------------------------------------------------------------------------
# Real-Git: authority lacking B fails clearly
# ---------------------------------------------------------------------------


def test_authority_without_commit_fails_without_switching_tip(tmp_path: Path) -> None:
    """Authority lacking target fails clearly; HEAD unchanged."""
    _authority, checkout, target = _setup_real_topology(tmp_path)

    # Create a separate authority that does NOT have the target commit
    wrong_authority = tmp_path / "wrong-authority.git"
    _init_repo(wrong_authority)

    # Record the current HEAD before the failed fetch
    head_before = _git(checkout, "rev-parse", "HEAD").stdout.strip()

    with pytest.raises(dc.ProvenanceError, match="does not contain commit"):
        dc.fetch_from_authority(checkout, target, str(wrong_authority), TIMEOUT)

    # HEAD was not changed by the failed fetch
    head_after = _git(checkout, "rev-parse", "HEAD").stdout.strip()
    assert head_before == head_after


# ---------------------------------------------------------------------------
# Real-Git: confirmed runtime independence
# ---------------------------------------------------------------------------


def test_confirmed_runtime_independent_of_dev_checkout(tmp_path: Path) -> None:
    """Confirmed commit is local; no mutable clone or network needed."""
    authority, checkout, target = _setup_real_topology(tmp_path)
    dc.fetch_from_authority(checkout, target, str(authority), TIMEOUT)
    dc._require_exact_commit(checkout, target, TIMEOUT)

    # Simulate confirmed state: checkout the target in detached mode
    dc._checkout(checkout, target, TIMEOUT, force=False)

    # The commit is a local object — no network or other clone needed
    verify = _git(checkout, "cat-file", "-t", target)
    assert verify.returncode == 0
    assert verify.stdout.strip() == "commit"

    # Worktree is clean after preparation
    status = _git(checkout, "status", "--porcelain").stdout
    assert not status

    # HEAD is exactly the target
    head = _git(checkout, "rev-parse", "HEAD").stdout.strip()
    assert head == target


# ---------------------------------------------------------------------------
# Real-Git: _require_exact_commit with real repos
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("commit", "pattern"),
    [
        ("d" * 40, "not present"),
        ("not-a-hash", "exact 40-character"),
    ],
    ids=["nonexistent-hash", "bad-format"],
)
def test_require_exact_commit_rejects(commit: str, pattern: str, tmp_path: Path) -> None:
    """_require_exact_commit rejects missing and malformed commits."""
    _authority, checkout, _target = _setup_real_topology(tmp_path)
    with pytest.raises(dc.DeployCtlError, match=pattern):
        dc._require_exact_commit(checkout, commit, TIMEOUT)


# ---------------------------------------------------------------------------
# Error propagation: deterministic mocks for OS/timeout mapping
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "_exc",
    [OSError("network unreachable"), subprocess.TimeoutExpired(cmd="git", timeout=TIMEOUT)],
    ids=["os-error", "timeout"],
)
def test_fetch_error_maps_to_provenance_error(
    _exc: OSError | subprocess.TimeoutExpired,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """OS and timeout errors during fetch are wrapped as ProvenanceError."""
    _authority, checkout, target = _setup_real_topology(tmp_path)

    def _boom(
        _repo: Path,
        _args: tuple[str, ...],
        _timeout: float,
    ) -> subprocess.CompletedProcess[str]:
        raise _exc

    monkeypatch.setattr(dc, "_run_git", _boom)
    with pytest.raises(dc.ProvenanceError, match="could not fetch"):
        dc.fetch_from_authority(checkout, target, "https://unused", TIMEOUT)
