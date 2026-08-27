"""Toolchain policy invariants: one supported Python series, one pinned uv.

The supported toolchain is documented in ``docs/TOOLCHAIN.md`` and must stay
consistent with the project metadata (``pyproject.toml``) and CI
(``.github/workflows/ci.yml``). These checks fail when any one of those places
drifts, so an unsupported or untested version cannot be claimed silently.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
PYPROJECT = REPO_ROOT / "pyproject.toml"
CI = REPO_ROOT / ".github" / "workflows" / "ci.yml"
TOOLCHAIN_DOC = REPO_ROOT / "docs" / "TOOLCHAIN.md"

UV_PIN_RE = re.compile(r"uv==(\d+\.\d+\.\d+)")


def _read(path: Path) -> str:
    """Return the file text, failing clearly if it is missing."""
    return path.read_text(encoding="utf-8")


def test_supported_python_is_single_series() -> None:
    """Project metadata supports exactly the CPython 3.12 series."""
    text = _read(PYPROJECT)
    version = text.split('requires-python = "', 1)[1].split('"', 1)[0]
    assert version == "==3.12.*", version


def test_ci_pins_exactly_one_uv_version() -> None:
    """CI installs a single, explicitly pinned uv version with no drift."""
    text = _read(CI)
    assert 'python -m pip install uv"' not in text
    assert "python -m pip install uv " not in text
    pins = UV_PIN_RE.findall(text)
    assert pins == [pins[0]], f"CI must pin exactly one uv version, found: {pins}"


def test_toolchain_doc_is_consistent_source_of_truth() -> None:
    """The documented versions match the metadata and CI exactly."""
    doc = _read(TOOLCHAIN_DOC)
    uv_version = UV_PIN_RE.search(doc)
    assert uv_version is not None, "TOOLCHAIN.md must state an explicit uv==VERSION"
    pinned = uv_version.group(1)

    pyproject = _read(PYPROJECT)
    assert 'requires-python = "==3.12.*"' in pyproject

    ci = _read(CI)
    assert f'uv=={pinned}"' in ci, "CI uv pin must match docs/TOOLCHAIN.md"
    assert "python -m pip install uv" not in ci
