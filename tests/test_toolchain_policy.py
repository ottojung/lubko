"""Reproducible build-tool policy without runtime patch-version coupling."""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
PYPROJECT = REPO_ROOT / "pyproject.toml"
CI = REPO_ROOT / ".github" / "workflows" / "ci.yml"
TOOLCHAIN_DOC = REPO_ROOT / "docs" / "TOOLCHAIN.md"
TOOLCHAIN_SOURCE = REPO_ROOT / "src" / "lubko" / "toolchain.py"
UV_PIN_RE = re.compile(r"uv==(\d+\.\d+\.\d+)")


def _read(path: Path) -> str:
    """Return repository text."""
    return path.read_text(encoding="utf-8")


def test_supported_python_is_single_series() -> None:
    """Project metadata supports exactly the CPython 3.12 series."""
    version = _read(PYPROJECT).split('requires-python = "', 1)[1].split('"', 1)[0]
    assert version == "==3.12.*"


def test_ci_pins_exactly_one_uv_version() -> None:
    """CI remains reproducible with one explicit reviewed uv pin."""
    text = _read(CI)
    pins = UV_PIN_RE.findall(text)
    assert len(pins) == 1
    assert 'python -m pip install uv"' not in text
    assert "python -m pip install uv " not in text


def test_runtime_has_no_exact_uv_patch_authority() -> None:
    """The CI pin is deliberately not duplicated as a runtime contract."""
    source = _read(TOOLCHAIN_SOURCE)
    assert "SUPPORTED_UV_VERSION" not in source
    assert "uv --version" not in source
    doc = _read(TOOLCHAIN_DOC)
    assert "not a production runtime protocol" in doc
