"""Regression coverage for executable test temporary paths."""

from __future__ import annotations

import subprocess
from pathlib import Path


def test_tmp_path_supports_test_executables_outside_system_tmp(tmp_path: Path) -> None:
    """Fake executables run from the repository-local temp fixture.

    The production-like Lubko container mounts the system ``/tmp`` filesystem
    ``noexec``. This fixture deliberately lives under the repository's ignored
    pytest cache instead, so executable test doubles do not depend on ambient
    system-temp mount flags.
    """
    repo_root = Path(__file__).resolve().parents[1]
    assert tmp_path.is_relative_to(repo_root / ".pytest_cache" / "exec-tmp")

    probe = tmp_path / "probe"
    probe.write_text("#!/bin/sh\nprintf executable-temp-ok\n", encoding="utf-8")
    probe.chmod(0o755)

    completed = subprocess.run([str(probe)], capture_output=True, text=True, check=True)
    assert completed.stdout == "executable-temp-ok"
