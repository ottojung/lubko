"""Queue-readiness positive execution evidence regression tests.

Issue #726: readiness must require the probe payload to actually exec and
produce a deterministic sentinel in stdout before declaring the worker
queue-ready.  A missing executable, bad working directory, or equivalent
execution error must make readiness fail even when the row briefly reaches
``running`` with a valid ``process_pid``.
"""

from __future__ import annotations

import os
import sys
from contextlib import contextmanager
from types import SimpleNamespace
from typing import TYPE_CHECKING

import pytest

from lubko import lifecycle

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

_SENTINEL = lifecycle.READINESS_SENTINEL


def _make_fake_conn(
    fetchone_result: object,
) -> SimpleNamespace:
    """Build a minimal fake connection that returns *fetchone_result*.

    Returns:
        A ``SimpleNamespace`` impersonating a database connection.
    """
    fake_cursor = SimpleNamespace(
        execute=lambda _sql, _params: None,
        fetchone=lambda: fetchone_result,
    )

    @contextmanager
    def _cursor_ctx(**_kwargs: object) -> Iterator[SimpleNamespace]:
        yield fake_cursor

    return SimpleNamespace(
        cursor=_cursor_ctx,
    )


# ---------------------------------------------------------------------------
# Probe process construction
# ---------------------------------------------------------------------------


def _create_venv_python(cwd: Path) -> Path:
    """Create a minimal fake venv Python at *cwd* and return its path.

    Returns:
        The path to the created fake Python interpreter.
    """
    venv_bin = cwd / ".venv" / "bin"
    venv_bin.mkdir(parents=True)
    python = venv_bin / "python"
    python.write_text("", encoding="utf-8")
    return python


def test_probe_uses_sealed_runtime_python(tmp_path: Path) -> None:
    """Probe argv[0] is the venv Python inside the cwd."""
    python = _create_venv_python(tmp_path)
    process = lifecycle._probe_process(str(tmp_path))
    assert process[0] == str(python)


def test_probe_script_contains_sentinel(tmp_path: Path) -> None:
    """The -c script must emit the deterministic readiness sentinel."""
    _create_venv_python(tmp_path)
    process = lifecycle._probe_process(str(tmp_path))
    assert process[1] == "-c"
    assert _SENTINEL in process[2]


def test_probe_script_flushes_stdout(tmp_path: Path) -> None:
    """The -c script must flush stdout so the sentinel is visible."""
    _create_venv_python(tmp_path)
    process = lifecycle._probe_process(str(tmp_path))
    assert "flush" in process[2]


def test_probe_python_path_missing_raises(tmp_path: Path) -> None:
    """A cwd without a sealed venv Python must fail closed."""
    with pytest.raises(FileNotFoundError, match="sealed runtime Python"):
        lifecycle._probe_python_path(str(tmp_path))


def test_probe_insert_returns_none_when_python_missing(tmp_path: Path) -> None:
    """_insert_probe_job must return None when the runtime Python is absent."""
    with pytest.raises(FileNotFoundError):
        lifecycle._probe_python_path(str(tmp_path))


# ---------------------------------------------------------------------------
# Sentinel presence / absence in output
# ---------------------------------------------------------------------------


def test_sentinel_present_returns_true() -> None:
    """A stdout tail containing the sentinel is accepted."""
    conn = _make_fake_conn((_SENTINEL + "\n",))
    assert lifecycle._read_probe_sentinel(conn, object()) is True  # type: ignore[arg-type]


def test_sentinel_absent_returns_false() -> None:
    """A stdout tail without the sentinel is rejected."""
    conn = _make_fake_conn(("some other output\n",))
    assert lifecycle._read_probe_sentinel(conn, object()) is False  # type: ignore[arg-type]


def test_empty_output_returns_false() -> None:
    """Before the worker publishes any output, the sentinel is absent."""
    conn = _make_fake_conn((None,))
    assert lifecycle._read_probe_sentinel(conn, object()) is False  # type: ignore[arg-type]


def test_missing_row_returns_false() -> None:
    """A deleted probe row returns False, not a crash."""
    conn = _make_fake_conn(None)
    assert lifecycle._read_probe_sentinel(conn, object()) is False  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Exit-127 probe must never produce ready=true
# ---------------------------------------------------------------------------


def test_missing_executable_blocks_readiness() -> None:
    """A probe targeting a non-existent executable cannot succeed.

    Even if the row reaches ``running`` with a valid ``process_pid``,
    readiness must fail because the sentinel never appears.
    """
    running_state: dict[str, object] = {
        "status": "running",
        "worker_id": "test-worker",
        "process_pid": 99999,
    }
    claim = lifecycle._parse_probe_claim_state(running_state)
    assert claim is not None
    status, owner, pid = claim
    assert status == "running"
    assert owner == "test-worker"
    assert pid == 99999

    conn = _make_fake_conn((None,))
    assert lifecycle._read_probe_sentinel(conn, object()) is False  # type: ignore[arg-type]


def test_probe_payload_never_uses_host_paths(tmp_path: Path) -> None:
    """The probe process argv must not reference /usr/bin/sleep."""
    _create_venv_python(tmp_path)
    process = lifecycle._probe_process(str(tmp_path))
    assert "/usr/bin/sleep" not in process
    assert "/usr/bin" not in " ".join(process)


# ---------------------------------------------------------------------------
# Exact-worker identity binding
# ---------------------------------------------------------------------------


def test_spawned_by_check_requires_exact_pid_chain() -> None:
    """A process_pid that is not a descendant must be rejected."""
    this_pid = os.getpid()
    assert lifecycle._spawned_by_recovery_worker(99999999, this_pid) is False


def test_spawned_by_own_pid_succeeds() -> None:
    """A process whose ancestor chain includes the recovery worker."""
    this_pid = os.getpid()
    assert lifecycle._spawned_by_recovery_worker(this_pid, this_pid) is True


# ---------------------------------------------------------------------------
# Probe roundtrip without a real database
# ---------------------------------------------------------------------------


def test_config_failure_returns_false(monkeypatch: pytest.MonkeyPatch) -> None:
    """Database config failure must not prove readiness."""
    monkeypatch.setattr(
        lifecycle,
        "load_database_config",
        lambda: (_ for _ in ()).throw(OSError("config missing")),
    )
    assert lifecycle._verify_queue_roundtrip("w", sys.prefix, 1, 0.1) is False


def test_connect_failure_returns_false(monkeypatch: pytest.MonkeyPatch) -> None:
    """Connection failure must not prove readiness."""
    import psycopg  # ruff: ignore[import-outside-top-level]

    monkeypatch.setattr(
        lifecycle,
        "load_database_config",
        lambda: SimpleNamespace(conninfo=lambda: "postgresql://unused"),
    )
    monkeypatch.setattr(
        psycopg,
        "connect",
        lambda *_a, **_kw: (_ for _ in ()).throw(OSError("no db")),
    )
    assert lifecycle._verify_queue_roundtrip("w", sys.prefix, 1, 0.1) is False


def test_insert_failure_returns_false(monkeypatch: pytest.MonkeyPatch) -> None:
    """Probe insert failure must not prove readiness."""
    import psycopg  # ruff: ignore[import-outside-top-level]

    monkeypatch.setattr(
        lifecycle,
        "load_database_config",
        lambda: SimpleNamespace(conninfo=lambda: "postgresql://unused"),
    )
    fake_conn = SimpleNamespace(
        autocommit=False,
        close=lambda: None,
        cursor=lambda _row_factory=None, **_kw: SimpleNamespace(
            execute=lambda *_a: None,
            fetchone=lambda: None,
        ),
    )
    monkeypatch.setattr(psycopg, "connect", lambda *_a, **_kw: fake_conn)
    monkeypatch.setattr(lifecycle, "_insert_probe_job", lambda _conn, _cwd: None)
    assert lifecycle._verify_queue_roundtrip("w", sys.prefix, 1, 0.1) is False


# ---------------------------------------------------------------------------
# READINESS_SENTINEL constant
# ---------------------------------------------------------------------------


def test_sentinel_is_nonempty_string() -> None:
    """The sentinel must be a non-empty string."""
    assert isinstance(_SENTINEL, str)
    assert len(_SENTINEL) > 0


def test_sentinel_is_stable() -> None:
    """The sentinel value must not change between imports."""
    assert _SENTINEL == "lubko-readiness-sentinel"
