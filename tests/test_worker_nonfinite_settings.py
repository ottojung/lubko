"""Regression coverage for non-finite worker timing configuration."""

from dataclasses import replace
from typing import Any, cast

import pytest

from lubko.worker import Settings


@pytest.fixture
def settings() -> Settings:
    """Return a valid baseline worker configuration."""
    return Settings(
        worker_id="worker",
        poll_interval_seconds=1.0,
        process_poll_interval_seconds=0.1,
        cancel_grace_seconds=5.0,
        server="server",
    )


@pytest.mark.parametrize(
    "field",
    [
        "poll_interval_seconds",
        "process_poll_interval_seconds",
        "cancel_grace_seconds",
        "lease_duration_seconds",
        "lease_refresh_interval_seconds",
        "lease_recovery_interval_seconds",
        "output_publication_interval_seconds",
        "health_publish_interval_seconds",
        "lease_safety_margin_seconds",
        "db_operation_timeout_seconds",
        "gc_retention_seconds",
        "gc_interval_seconds",
    ],
)
@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_settings_reject_nonfinite_timing(settings: Settings, field: str, value: float) -> None:
    """Reject every non-finite timing value before worker startup."""
    with pytest.raises(ValueError, match=f"{field} must be finite"):
        replace(settings, **cast("dict[str, Any]", {field: value}))


@pytest.mark.parametrize("value", ["nan", "inf", "-inf"])
def test_environment_rejects_nonfinite_timing(monkeypatch: pytest.MonkeyPatch, value: str) -> None:
    """Reject non-finite float spellings loaded from worker environment."""
    monkeypatch.setenv("LUBKO_LEASE_DURATION_SECONDS", value)
    with pytest.raises(ValueError, match="lease_duration_seconds must be finite"):
        Settings.from_environment(server="server")
