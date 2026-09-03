"""Regression tests for supervisor timing settings validation."""

from dataclasses import replace

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
)


@pytest.mark.parametrize("field_name", TIMING_FIELDS)
@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_settings_reject_non_finite_timing_values(field_name: str, value: float) -> None:
    """Reject every non-finite value for every timing field."""
    with pytest.raises(ValueError, match="must be finite"):
        replace(supervisor.Settings(), **{field_name: value})


@pytest.mark.parametrize("spelling", ["nan", "inf", "-inf"])
def test_settings_from_environment_rejects_non_finite_timing(
    monkeypatch: pytest.MonkeyPatch, spelling: str
) -> None:
    """Reject non-finite spellings loaded from environment variables."""
    monkeypatch.setenv("LUBKO_SUPERVISOR_POLL_SECONDS", spelling)

    with pytest.raises(ValueError, match="must be finite"):
        supervisor.Settings.from_environment()


@pytest.mark.parametrize("field_name", ["postgres_timeout_seconds", "lock_timeout_seconds"])
@pytest.mark.parametrize("value", [0.0, -1.0])
def test_settings_reject_non_positive_database_timeouts(field_name: str, value: float) -> None:
    """Reject zero and negative database timeout values."""
    with pytest.raises(ValueError, match="database timeout settings must be positive"):
        replace(supervisor.Settings(), **{field_name: value})


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
