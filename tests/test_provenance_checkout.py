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
import time
from pathlib import Path
from typing import Final

import pytest

from lubko import deployctl as dc
from lubko.lifecycle import SCHEMA_VERSION, STATE_RUNNING, WorkerMeta

COMMIT: Final = "a" * 40
PREVIOUS_COMMIT: Final = "b" * 40
SOURCE_URL: Final = "https://github.com/example/repo.git"
TIMEOUT: Final = 10.0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _run(cmd: list[str], *, cwd: Path) -> subprocess.CompletedProcess[str]:
    """Run a subprocess, returning the result without raising on non-zero exit.

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

        authority.git (bare, canonical)  <--  authority-work (pushes A, then B)
             |                                      |
        dev-clone (mutable, stale at A)            |
             |                                     |
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
    #    authority-work.  The dev-clone is never updated -- it stays at A.
    commit_b = _make_commit(authority_work, "commit B (target)")
    _run(["git", "push", "origin", "HEAD:main"], cwd=authority_work)

    return authority, deploy_checkout, commit_b


def _clone_checkout(tmp_path: Path, source: Path, name: str) -> Path:
    """Clone a fresh deploy-checkout from source for test isolation.

    Args:
        tmp_path: Parent directory for the clone.
        source: Path to clone from.
        name: Directory name for the clone.

    Returns:
        Path to the fresh clone.
    """
    dest = tmp_path / name
    _run(["git", "clone", str(source), str(dest)], cwd=tmp_path)
    return dest


def _worker_meta_for_rollback() -> WorkerMeta:
    """Return valid worker metadata for rollback state construction."""
    return WorkerMeta(
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


# ---------------------------------------------------------------------------
# Unit: type contracts
# ---------------------------------------------------------------------------


def test_provenance_error_is_deployctl_error_subtype() -> None:
    """ProvenanceError is a DeployCtlError subtype so existing error boundaries catch it."""
    assert issubclass(dc.ProvenanceError, dc.DeployCtlError)
    assert issubclass(dc.ProvenanceError, RuntimeError)


def test_provenance_error_caught_by_dispatch_boundary() -> None:
    """ProvenanceError is caught by the same boundary as DeployCtlError.

    Raises:
        ProvenanceError: Always, to exercise the catch boundary.
    """
    caught: list[str] = []
    try:
        msg = "authority missing commit"
        raise dc.ProvenanceError(msg)
    except dc.DeployCtlError as exc:
        caught.append(str(exc))
    assert caught == ["authority missing commit"]


# ---------------------------------------------------------------------------
# Unit: credential redaction
# ---------------------------------------------------------------------------


def test_has_userinfo_detects_https() -> None:
    """HTTPS URLs with user:password@ are detected as having userinfo."""
    assert dc._has_userinfo("https://user:pass@host/repo.git") is True
    assert dc._has_userinfo("https://host/repo.git") is False


def test_has_userinfo_detects_scp_style() -> None:
    """SCP-style user@host:path is detected as having userinfo."""
    assert dc._has_userinfo("git@github.com:user/repo.git") is True
    assert dc._has_userinfo("deploy-token@github.com:user/repo.git") is True
    assert dc._has_userinfo("github.com:user/repo.git") is False


def test_redact_source_url_strips_https_userinfo() -> None:
    """HTTPS URLs with userinfo have the credential portion removed."""
    url = "https://user:secret@github.com/org/repo.git"
    assert dc._redact_source_url(url) == "https://github.com/org/repo.git"


def test_redact_source_url_strips_scp_userinfo() -> None:
    """SCP-style URLs with userinfo have the user portion removed."""
    url = "git@github.com:user/repo.git"
    assert dc._redact_source_url(url) == "github.com:user/repo.git"


def test_redact_source_url_preserves_bare_urls() -> None:
    """URLs without userinfo are returned unchanged."""
    url = "https://github.com/org/repo.git"
    assert dc._redact_source_url(url) == url


def test_provenance_error_suppresses_stderr_for_userinfo_urls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When source_url has userinfo, stderr is not included in the exception.

    Git may normalize/encode the credential-bearing URL differently than
    the raw input, so stderr is suppressed entirely to guarantee no
    credential material leaks.
    """
    url_with_creds = "https://deploy-token:tk-abc123@github.com/org/repo.git"
    stderr_with_url = (
        f"fatal: unable to access '{url_with_creds}': The requested URL returned error: 401"
    )

    def fake_run_git(
        _repo: Path,
        _args: tuple[str, ...],
        _timeout: float,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            args=["git"],
            returncode=128,
            stdout="",
            stderr=stderr_with_url,
        )

    monkeypatch.setattr(dc, "_run_git", fake_run_git)

    with pytest.raises(dc.ProvenanceError) as exc_info:
        dc.fetch_from_authority(
            Path("/workspace/Lubko"),
            COMMIT,
            url_with_creds,
            5.0,
        )

    error_text = str(exc_info.value)
    assert "tk-abc123" not in error_text
    assert "deploy-token" not in error_text
    assert url_with_creds not in error_text
    # Stderr is suppressed entirely — no raw 401 detail either
    assert "401" not in error_text
    # The redacted host/path is still present for diagnostics
    assert "github.com/org/repo.git" in error_text


def test_provenance_error_suppresses_stderr_for_scp_userinfo(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SCP-style user@host URLs also suppress stderr to avoid credential leak."""
    scp_url = "git@github.com:org/private-repo.git"
    stderr_text = "fatal: repository 'git@github.com:org/private-repo.git' not found"

    def fake_run_git(
        _repo: Path,
        _args: tuple[str, ...],
        _timeout: float,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            args=["git"],
            returncode=128,
            stdout="",
            stderr=stderr_text,
        )

    monkeypatch.setattr(dc, "_run_git", fake_run_git)

    with pytest.raises(dc.ProvenanceError) as exc_info:
        dc.fetch_from_authority(
            Path("/workspace/Lubko"),
            COMMIT,
            scp_url,
            5.0,
        )

    error_text = str(exc_info.value)
    assert "git@" not in error_text
    assert "github.com:org/private-repo.git" in error_text


def test_provenance_error_includes_stderr_for_credential_free_urls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """URLs without userinfo retain stderr for useful diagnostics."""
    clean_url = "https://github.com/org/repo.git"
    stderr_text = "fatal: remote error: not found"

    def fake_run_git(
        _repo: Path,
        _args: tuple[str, ...],
        _timeout: float,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            args=["git"],
            returncode=128,
            stdout="",
            stderr=stderr_text,
        )

    monkeypatch.setattr(dc, "_run_git", fake_run_git)

    with pytest.raises(dc.ProvenanceError, match="remote error: not found"):
        dc.fetch_from_authority(
            Path("/workspace/Lubko"),
            COMMIT,
            clean_url,
            5.0,
        )


def test_provenance_error_clean_with_normalized_encoded_secret(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When Git normalizes/encodes the secret differently, no credential leaks.

    Git may percent-encode, hash, or otherwise transform a credential-
    bearing URL in its stderr output.  Because stderr is suppressed when
    userinfo is present, even a completely different encoded form of the
    secret cannot appear in the exception.
    """
    raw_url = "https://user:p%40ssw0rd@host/repo.git"
    # Simulate Git encoding the '@' as '%40' and double-encoding the password
    normalized_stderr = (
        "fatal: unable to access 'https://user:p%2540ssw0rd@host/repo.git': HTTP 401"
    )

    def fake_run_git(
        _repo: Path,
        _args: tuple[str, ...],
        _timeout: float,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            args=["git"],
            returncode=128,
            stdout="",
            stderr=normalized_stderr,
        )

    monkeypatch.setattr(dc, "_run_git", fake_run_git)

    with pytest.raises(dc.ProvenanceError) as exc_info:
        dc.fetch_from_authority(
            Path("/workspace/Lubko"),
            COMMIT,
            raw_url,
            5.0,
        )

    error_text = str(exc_info.value)
    assert "p%40ssw0rd" not in error_text
    assert "p%2540ssw0rd" not in error_text
    assert "ssw0rd" not in error_text
    assert "user" not in error_text
    assert "401" not in error_text


def test_source_url_rejected_for_status_request() -> None:
    """--source-url on a status request fails, not silently ignored."""
    request = dc.parse_request('{"type": "status"}')
    options = dc.Options(
        repo=Path("/workspace/Lubko"),
        uv_path="uv",
        confirm_window_seconds=120.0,
        stop_grace_seconds=1.0,
        postgres_timeout_seconds=1.0,
        lock_timeout_seconds=1.0,
        validation_timeout_seconds=1.0,
        git_timeout_seconds=5.0,
        cli_timeout_seconds=1.0,
        source_url="https://example.com/repo.git",
    )
    with pytest.raises(dc.DeployCtlError, match="--source-url is only supported for checkout"):
        dc._dispatch(options, request)


def test_source_url_rejected_for_confirm_request() -> None:
    """--source-url on a confirm request fails, not silently ignored."""
    request = dc.parse_request(f'{{"type":"confirm","commit":"{COMMIT}"}}')
    options = dc.Options(
        repo=Path("/workspace/Lubko"),
        uv_path="uv",
        confirm_window_seconds=120.0,
        stop_grace_seconds=1.0,
        postgres_timeout_seconds=1.0,
        lock_timeout_seconds=1.0,
        validation_timeout_seconds=1.0,
        git_timeout_seconds=5.0,
        cli_timeout_seconds=1.0,
        source_url="https://example.com/repo.git",
    )
    with pytest.raises(dc.DeployCtlError, match="--source-url is only supported for checkout"):
        dc._dispatch(options, request)


# ---------------------------------------------------------------------------
# Unit: RollbackState serialization
# ---------------------------------------------------------------------------


def _make_rollback_state() -> dc.RollbackState:
    """Build a minimal RollbackState for serialization tests.

    Returns:
        A rollback state instance.
    """
    return dc.RollbackState(
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
        previous_meta=_worker_meta_for_rollback(),
        new_meta=_worker_meta_for_rollback(),
        supervisor_owned=False,
    )


def test_rollback_state_does_not_persist_source_url() -> None:
    """source_url is transient preparation input and must not appear in rollback state."""
    state = _make_rollback_state()
    raw = state.to_dict()
    assert "source_url" not in raw
    assert not hasattr(state, "source_url")


def test_rollback_state_round_trip_without_source_url() -> None:
    """RollbackState round-trips through to_dict/from_dict without source_url."""
    state = _make_rollback_state()
    raw = state.to_dict()
    restored = dc.RollbackState.from_dict(raw)
    assert restored.commit == COMMIT
    assert restored.previous_commit == PREVIOUS_COMMIT


# ---------------------------------------------------------------------------
# Unit: main/dispatch boundary catches ProvenanceError
# ---------------------------------------------------------------------------


def test_main_catches_provenance_error_as_checkout_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ProvenanceError raised during checkout is caught and produces a structured error response."""
    checkout_request = f'{{"type":"checkout","commit":"{COMMIT}"}}'

    def _always_provenance(*_a: object, **_kw: object) -> None:
        msg = "source authority missing commit"
        raise dc.ProvenanceError(msg)

    monkeypatch.setattr(dc, "fetch_from_authority", _always_provenance)
    monkeypatch.setattr(dc, "_require_exact_commit", lambda *_a: None)
    monkeypatch.setattr(dc, "_require_clean_checkout", lambda *_a: None)
    monkeypatch.setattr(dc, "_checkout", lambda *_a, **_kw: True)
    monkeypatch.setattr(dc, "_cleanup_pending_locked", lambda: None)
    monkeypatch.setattr(dc, "read_meta", _worker_meta_for_rollback)
    monkeypatch.setattr(dc, "worker_alive", lambda _m: True)
    monkeypatch.setattr(dc, "_provenance_fetch", lambda _o, _c: None)
    monkeypatch.setattr(
        dc, "run_validation", lambda *_a: type("R", (), {"ok": True, "detail": ""})()
    )
    monkeypatch.setattr(dc, "check_postgres", lambda _t: True)
    monkeypatch.setattr(dc, "_read_state", lambda: None)
    monkeypatch.setattr(dc, "_candidate_identity", lambda *_a, **_kw: (None, None))
    monkeypatch.setattr(dc, "_mission_authority_facts", lambda *_a, **_kw: object())
    monkeypatch.setattr(
        __import__("lubko.lifecycle_state", fromlist=["authorize_mission_publish"]),
        "authorize_mission_publish",
        lambda _f: True,
    )

    exit_code = dc.main([checkout_request, "--repo", "/nonexistent"])
    # The exit code is EXIT_ERROR (1) for a failed checkout, proving the
    # ProvenanceError was caught by the dispatch boundary, not traceback.
    assert exit_code == dc.EXIT_ERROR


# ---------------------------------------------------------------------------
# Real-Git: shared topology base, per-test lightweight clones
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def topology_base(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, str]:
    """Module-scoped shared base topology for real-git tests.

    Created once per test module by pytest and automatically cleaned up
    when the module finishes.  Each test clones a fresh isolated checkout
    from the shared base to stay isolated while avoiding repeated full
    topology setup.

    Returns:
        (authority_bare_path, target_commit_B)
    """
    base = tmp_path_factory.mktemp("provenance-topo")
    _authority, _checkout, target = _setup_real_topology(base)
    return base, target


def _fresh_checkout(tmp_path: Path, topology_base: tuple[Path, str]) -> Path:
    """Clone a fresh checkout and repoint its origin at the stale dev-clone.

    The clone is created from the shared base ``deploy-checkout`` for
    efficient object sharing, then the ``origin`` remote is rewritten to
    point at ``dev-clone`` — the stale mutable local clone that only has
    commit A.  This preserves the production topology invariant: origin
    does NOT have the target commit B, while the explicit authority does.

    Args:
        tmp_path: Per-test temporary directory.
        topology_base: The shared ``(authority_path, target_commit)`` tuple.

    Returns:
        Path to the fresh isolated checkout.
    """
    base, _target = topology_base
    checkout = _clone_checkout(tmp_path, base / "deploy-checkout", "checkout")
    _git(checkout, "remote", "set-url", "origin", str(base / "dev-clone"))
    return checkout


# ---------------------------------------------------------------------------
# Real-Git: authority fetch topology
# ---------------------------------------------------------------------------


def test_fetch_from_real_authority_succeeds(
    tmp_path: Path, topology_base: tuple[Path, str]
) -> None:
    """Fetching a commit that exists in the real authority succeeds."""
    base, target = topology_base
    authority = base / "authority.git"
    checkout = _fresh_checkout(tmp_path, topology_base)
    dc.fetch_from_authority(checkout, target, str(authority), TIMEOUT)
    dc._require_exact_commit(checkout, target, TIMEOUT)


def test_stale_origin_not_used_for_fetch(tmp_path: Path, topology_base: tuple[Path, str]) -> None:
    """The declared authority URL is used, not the checkout's origin remote.

    Origin is the stale dev-clone (only has commit A).  Commit B exists
    exclusively in the bare authority.  ``fetch_from_authority`` with the
    authority URL obtains B, proving origin is never consulted.
    """
    base, target = topology_base
    authority = base / "authority.git"
    checkout = _fresh_checkout(tmp_path, topology_base)

    # Origin points at dev-clone which only has commit A (stale)
    origin_url = _git(checkout, "remote", "get-url", "origin").stdout.strip()
    assert origin_url == str(base / "dev-clone")

    # Commit B is absent from the stale dev-clone's object store
    cat_file = _git(base / "dev-clone", "cat-file", "-e", f"{target}^{{commit}}")
    assert cat_file.returncode != 0, "stale dev-clone must not contain target commit B"

    # Fetch succeeds using the explicit authority, proving origin is not consulted
    dc.fetch_from_authority(checkout, target, str(authority), TIMEOUT)
    dc._require_exact_commit(checkout, target, TIMEOUT)


# ---------------------------------------------------------------------------
# Real-Git: detached clean checkout
# ---------------------------------------------------------------------------


def test_checkout_is_detached_and_clean(tmp_path: Path, topology_base: tuple[Path, str]) -> None:
    """After fetch + checkout, HEAD is detached at the exact commit, worktree clean."""
    base, target = topology_base
    authority = base / "authority.git"
    checkout = _fresh_checkout(tmp_path, topology_base)
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


def test_authority_without_commit_fails_without_switching_tip(
    tmp_path: Path, topology_base: tuple[Path, str]
) -> None:
    """Authority lacking target fails clearly; HEAD unchanged."""
    _base, target = topology_base
    checkout = _fresh_checkout(tmp_path, topology_base)

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


def test_confirmed_runtime_independent_of_dev_checkout(
    tmp_path: Path, topology_base: tuple[Path, str]
) -> None:
    """After source severing the restart path performs no Git/network lookup."""
    base, target = topology_base
    authority = base / "authority.git"
    checkout = _fresh_checkout(tmp_path, topology_base)

    # Prepare: fetch B from authority, verify, detach at B.
    dc.fetch_from_authority(checkout, target, str(authority), TIMEOUT)
    dc._require_exact_commit(checkout, target, TIMEOUT)
    dc._checkout(checkout, target, TIMEOUT, force=False)

    # Prove the detached checkout remains clean and exact.
    head = _git(checkout, "rev-parse", "HEAD").stdout.strip()
    assert head == target
    assert not _git(checkout, "status", "--porcelain").stdout

    # Build a restart mission whose previous worker is not retiring and is
    # still alive -- the restart path should just return it directly without
    # touching Git or network.
    previous_meta = WorkerMeta(
        schema_version=SCHEMA_VERSION,
        state=STATE_RUNNING,
        pid=100,
        pgid=100,
        sid=100,
        start_time_ticks=1000,
        token="test-token",  # ruff: ignore[hardcoded-password-func-arg]
        repo=str(checkout),
        git_commit=target,
        worker_id="test-worker",
        log_path="worker.log",
        started_at=1.0,
        stopped_at=None,
    )
    state = dc.RollbackState(
        schema_version=dc.ROLLBACK_SCHEMA_VERSION,
        generation=1,
        status=dc.STATUS_PENDING,
        commit=target,
        previous_commit=target,
        deadline=time.time() + 60,
        repo=str(checkout),
        uv_path="uv",
        stop_grace_seconds=1.0,
        git_timeout_seconds=5.0,
        previous_retiring=False,
        previous_meta=previous_meta,
        new_meta=None,
        supervisor_owned=False,
    )

    # Mock: worker is alive under its recorded identity.
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(dc, "read_meta_strict", lambda: previous_meta)
    monkeypatch.setattr(dc, "worker_alive", lambda meta: meta == previous_meta)

    # Guard: Git/network must not be called during restart.
    def _git_forbidden(*_a: object, **_kw: object) -> object:
        msg = "restart path must not consult Git or network"
        raise AssertionError(msg)

    monkeypatch.setattr(dc, "fetch_from_authority", _git_forbidden)
    monkeypatch.setattr(dc, "_run_git", _git_forbidden)

    # Exercise the maintained restart path.
    restored = dc.restart_previous(state)

    # The restart returned the existing live worker without any Git/network.
    assert restored is not None
    assert restored.git_commit == target
    assert restored.pid == previous_meta.pid
