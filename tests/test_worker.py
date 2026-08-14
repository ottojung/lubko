"""Tests for the Lubko worker."""

import os
import shutil
from pathlib import Path
from typing import Final
from uuid import uuid4

import pytest

from lubko.worker import (
    TRUNCATION_MARKER,
    Job,
    Settings,
    execute_job,
    resolve_shell,
    truncate_output,
)

EXECUTION_ERROR_EXIT_CODE: Final = 127
COMMAND_FAILURE_EXIT_CODE: Final = 7


def test_truncate_output_preserves_short_output() -> None:
    """Short output is returned unchanged."""
    assert truncate_output(b"hello\n", 128) == "hello\n"


def test_truncate_output_keeps_tail() -> None:
    """Oversized output keeps the newest bytes and records truncation."""
    limit = 64
    output = b"a" * 100 + b"the-end"

    result = truncate_output(output, limit)

    assert result.encode().startswith(TRUNCATION_MARKER)
    assert result.endswith("the-end")
    assert len(result.encode()) == limit


def make_settings() -> Settings:
    """Build worker settings for tests.

    Returns:
        Worker settings for tests.
    """
    return Settings(
        worker_id="test-worker",
        poll_interval_seconds=1.0,
        max_output_bytes=256 * 1024,
    )


def test_resolve_shell_finds_bash() -> None:
    """resolve_shell locates an installed bash executable."""
    assert resolve_shell() == shutil.which("bash")
    assert resolve_shell() is not None


def test_execute_job_runs_directly_without_docker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A job executes directly without any Docker executable or lookup."""
    original_which = shutil.which
    monkeypatch.setattr(
        shutil,
        "which",
        lambda name: original_which(name) if name != "docker" else None,
    )

    job = Job(id=uuid4(), cwd=str(tmp_path), command="echo direct")

    exit_code, stdout, stderr = execute_job(job, make_settings())

    assert exit_code == 0
    assert stdout.strip() == "direct"
    assert not stderr


def test_execute_job_honors_cwd(tmp_path: Path) -> None:
    """A job runs from the requested working directory."""
    job = Job(id=uuid4(), cwd=str(tmp_path), command="pwd")

    exit_code, stdout, stderr = execute_job(job, make_settings())

    assert exit_code == 0
    assert stdout.strip() == os.path.realpath(str(tmp_path))
    assert not stderr


def test_execute_job_success(tmp_path: Path) -> None:
    """A successful job reports a zero exit code and its stdout."""
    job = Job(id=uuid4(), cwd=str(tmp_path), command="echo hello world")

    exit_code, stdout, stderr = execute_job(job, make_settings())

    assert exit_code == 0
    assert stdout.strip() == "hello world"
    assert not stderr


def test_execute_job_reports_command_failure(tmp_path: Path) -> None:
    """A failing command preserves its exit code and output."""
    job = Job(id=uuid4(), cwd=str(tmp_path), command="echo oops >&2; exit 7")

    exit_code, stdout, stderr = execute_job(job, make_settings())

    assert exit_code == COMMAND_FAILURE_EXIT_CODE
    assert not stdout
    assert "oops" in stderr


def test_execute_job_reports_missing_cwd(tmp_path: Path) -> None:
    """A missing working directory produces a useful error."""
    missing = tmp_path / "missing"
    job = Job(id=uuid4(), cwd=str(missing), command="echo hi")

    exit_code, stdout, stderr = execute_job(job, make_settings())

    assert exit_code == EXECUTION_ERROR_EXIT_CODE
    assert not stdout
    assert "working directory" in stderr


def test_execute_job_reports_non_directory_cwd(tmp_path: Path) -> None:
    """A working directory that is a regular file produces a useful error."""
    target = tmp_path / "file"
    target.write_text("not a directory")
    job = Job(id=uuid4(), cwd=str(target), command="echo hi")

    exit_code, stdout, stderr = execute_job(job, make_settings())

    assert exit_code == EXECUTION_ERROR_EXIT_CODE
    assert not stdout
    assert "working directory" in stderr


def test_execute_job_reports_missing_shell(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A missing shell executable produces a useful error."""
    original_which = shutil.which
    monkeypatch.setattr(
        shutil,
        "which",
        lambda name: original_which(name) if name != "bash" else None,
    )

    job = Job(id=uuid4(), cwd=str(tmp_path), command="echo hi")

    exit_code, stdout, stderr = execute_job(job, make_settings())

    assert exit_code == EXECUTION_ERROR_EXIT_CODE
    assert not stdout
    assert "shell" in stderr
