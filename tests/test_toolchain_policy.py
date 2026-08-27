"""Toolchain policy invariants: one supported Python series, one pinned uv.

The supported toolchain is enforced at runtime by :data:`lubko.toolchain
.SUPPORTED_UV_VERSION` and must stay consistent with the project metadata
(``pyproject.toml``), CI (``.github/workflows/ci.yml``), and the human-facing
policy (``docs/TOOLCHAIN.md``). These checks fail when any one of those places
drifts, so an unsupported or untested version cannot be claimed silently.
"""

from __future__ import annotations

import re
from pathlib import Path

from lubko import toolchain

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


def test_runtime_uv_pin_is_consistent_everywhere() -> None:
    """The runtime uv pin matches CI and the documented policy exactly."""
    pinned = toolchain.SUPPORTED_UV_VERSION
    assert re.fullmatch(r"\d+\.\d+\.\d+", pinned), pinned

    ci = _read(CI)
    assert f'uv=={pinned}"' in ci, "CI uv pin must match the runtime constant"
    assert "python -m pip install uv" not in ci

    doc = _read(TOOLCHAIN_DOC)
    assert f"uv=={pinned}" in doc, "docs/TOOLCHAIN.md must match the runtime constant"


def test_ci_pins_exactly_one_uv_version() -> None:
    """CI installs a single, explicitly pinned uv version with no drift."""
    text = _read(CI)
    assert 'python -m pip install uv"' not in text
    assert "python -m pip install uv " not in text
    pins = UV_PIN_RE.findall(text)
    assert pins == [pins[0]], f"CI must pin exactly one uv version, found: {pins}"
