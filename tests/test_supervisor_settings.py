"""Regression tests for supervisor timing settings validation."""

from dataclasses import replace
from typing import Any

import pytest

from lubko import supervisor

TIMING_FIELDS = (
    "poll_interval_seconds",
    "backoff_base_seconds",
    "backoff_max_seconds",
    "stable_window_seconds",
    "stop_grace_seconds",
    "identity_timeout_seconds",
    "postgres_timeout_seconds",
    "lock_timeout_seconds",
    "probe_timeout_seconds",
    "readiness_interval_seconds",
    "no_progress_grace_seconds",
)


def _settings_with(field_name: str, value: float) -> supervisor.Settings:
    """Build supervisor settings with one field replaced.

    Args:
        field_name: Name of the setting to replace.
        value: Replacement value.

    Returns:
        The settings instance.
    """
    overrides: dict[str, Any] = {field_name: value}
    return replace(supervisor.Settings(), **overrides)


def test_settings_reject_non_finite_timing_values() -> None:
    """Reject every non-finite value for every timing field."""
    for field_name in TIMING_FIELDS:
        for value in (float("nan"), float("inf"), float("-inf")):
            with pytest.raises(ValueError, match="must be finite"):
                _settings_with(field_name, value)


@pytest.mark.parametrize("spelling", ["nan", "inf", "-inf"])
def test_settings_from_environment_rejects_non_finite_timing(
    monkeypatch: pytest.MonkeyPatch, spelling: str
) -> None:
    """Reject non-finite spellings loaded from environment variables."""
    monkeypatch.setenv("LUBKO_SUPERVISOR_POLL_SECONDS", spelling)

    with pytest.raises(ValueError, match="must be finite"):
        supervisor.Settings.from_environment()


def test_settings_reject_non_positive_database_timeouts() -> None:
    """Reject zero and negative database timeout values."""
    for field_name in ("postgres_timeout_seconds", "lock_timeout_seconds"):
        for value in (0.0, -1.0):
            with pytest.raises(ValueError, match="database timeout settings must be positive"):
                _settings_with(field_name, value)


def test_settings_reject_non_positive_forward_progress_settings() -> None:
    """Reject a non-positive grace period and a zero probe requirement."""
    with pytest.raises(ValueError, match="NO_PROGRESS_GRACE_SECONDS must be positive"):
        _settings_with("no_progress_grace_seconds", 0.0)
    with pytest.raises(ValueError, match="NO_PROGRESS_REQUIRED_PROBES must be at least one"):
        _settings_with("no_progress_required_probes", 0)


def test_settings_require_multiple_probes_and_a_real_grace_period() -> None:
    """The sustained no-progress rule needs both a duration and corroboration."""
    settings = supervisor.Settings()

    assert settings.no_progress_grace_seconds >= 60.0
    assert settings.no_progress_required_probes >= 3


def test_settings_accept_valid_finite_timing_values() -> None:
    """Preserve valid finite settings and existing ordering constraints."""
    settings = supervisor.Settings(
        poll_interval_seconds=0.1,
        backoff_base_seconds=0.2,
        backoff_max_seconds=0.3,
        stable_window_seconds=0.4,
        stop_grace_seconds=0.5,
        identity_timeout_seconds=0.6,
        postgres_timeout_seconds=0.7,
        lock_timeout_seconds=0.8,
        probe_timeout_seconds=0.9,
        readiness_interval_seconds=1.0,
    )

    assert settings.backoff_max_seconds >= settings.backoff_base_seconds
