"""Supervisor state namespace token invariants.

Tests cover token validation, tokenized path resolution, environment
stripping, namespace isolation (collision-class regression), and backward
compatibility when the token is absent.
"""

from __future__ import annotations

import json
import secrets
from typing import TYPE_CHECKING

import pytest

from lubko import lifecycle, supervise
from lubko.state import (
    SUPERVISOR_STATE_TOKEN_ENV,
    SupervisorStateTokenError,
    supervisor_state_token,
    validate_supervisor_state_token,
)

if TYPE_CHECKING:
    from pathlib import Path

VALID_TOKEN = secrets.token_hex(32)


# ---------------------------------------------------------------------------
# Token validation
# ---------------------------------------------------------------------------


class TestTokenValidation:
    """Deterministic validation of the 256-bit hex token format."""

    @staticmethod
    def test_valid_token_passes() -> None:
        """A 64-character lowercase hex string is accepted."""
        assert validate_supervisor_state_token(VALID_TOKEN) == VALID_TOKEN

    @staticmethod
    def test_empty_token_rejected() -> None:
        """An empty string fails closed."""
        with pytest.raises(SupervisorStateTokenError, match="empty"):
            validate_supervisor_state_token("")

    @staticmethod
    def test_uppercase_hex_rejected() -> None:
        """Uppercase hex is not path-safe canonical; rejected."""
        upper = VALID_TOKEN.upper()
        with pytest.raises(SupervisorStateTokenError, match="invalid"):
            validate_supervisor_state_token(upper)

    @staticmethod
    def test_short_token_rejected() -> None:
        """A 63-character hex string is too short."""
        with pytest.raises(SupervisorStateTokenError, match="invalid"):
            validate_supervisor_state_token(VALID_TOKEN[:-1])

    @staticmethod
    def test_long_token_rejected() -> None:
        """A 65-character hex string is too long."""
        with pytest.raises(SupervisorStateTokenError, match="invalid"):
            validate_supervisor_state_token(VALID_TOKEN + "0")

    @staticmethod
    def test_non_hex_rejected() -> None:
        """Characters outside 0-9a-f are rejected."""
        bad = "g" + VALID_TOKEN[1:]
        with pytest.raises(SupervisorStateTokenError, match="invalid"):
            validate_supervisor_state_token(bad)

    @staticmethod
    def test_none_returned_for_absent_env(
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Missing environment variable returns None."""
        monkeypatch.delenv(SUPERVISOR_STATE_TOKEN_ENV, raising=False)
        assert supervisor_state_token() is None

    @staticmethod
    def test_none_returned_for_empty_env(
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Empty environment variable returns None."""
        monkeypatch.setenv(SUPERVISOR_STATE_TOKEN_ENV, "")
        assert supervisor_state_token() is None

    @staticmethod
    def test_valid_token_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
        """Valid token from environment is returned."""
        monkeypatch.setenv(SUPERVISOR_STATE_TOKEN_ENV, VALID_TOKEN)
        assert supervisor_state_token() == VALID_TOKEN

    @staticmethod
    def test_invalid_token_from_env_raises(
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Invalid non-empty token from environment raises."""
        monkeypatch.setenv(SUPERVISOR_STATE_TOKEN_ENV, "not-a-token")
        with pytest.raises(SupervisorStateTokenError):
            supervisor_state_token()


# ---------------------------------------------------------------------------
# Tokenized path resolution
# ---------------------------------------------------------------------------


class TestTokenizedPaths:
    """Tokenized directory and file paths are correct under all token states."""

    @staticmethod
    def test_private_dir_without_token(
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """Without a token, private_dir falls back to untokenized path."""
        monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
        monkeypatch.delenv(SUPERVISOR_STATE_TOKEN_ENV, raising=False)
        assert supervise.supervisor_private_dir() == supervise.supervisor_dir()

    @staticmethod
    def test_private_dir_with_token(
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """With a token, private_dir includes the token in the path."""
        monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
        monkeypatch.setenv(SUPERVISOR_STATE_TOKEN_ENV, VALID_TOKEN)
        expected = supervise.supervisor_dir() / VALID_TOKEN
        assert supervise.supervisor_private_dir() == expected

    @staticmethod
    def test_state_path_untokenized(
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """state_path always returns the untokenized path (CLI-readable)."""
        monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
        monkeypatch.setenv(SUPERVISOR_STATE_TOKEN_ENV, VALID_TOKEN)
        assert supervise.state_path() == supervise.supervisor_dir() / "state.json"

    @staticmethod
    def test_private_authority_path_with_token(
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """With a token, authority.json lives under the tokenized directory."""
        monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
        monkeypatch.setenv(SUPERVISOR_STATE_TOKEN_ENV, VALID_TOKEN)
        expected = supervise.supervisor_dir() / VALID_TOKEN / "authority.json"
        assert supervise.private_authority_path() == expected

    @staticmethod
    def test_private_authority_path_without_token(
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """Without a token, authority.json falls back to untokenized path."""
        monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
        monkeypatch.delenv(SUPERVISOR_STATE_TOKEN_ENV, raising=False)
        assert supervise.private_authority_path() == (supervise.supervisor_dir() / "authority.json")

    @staticmethod
    def test_desired_path_always_untokenized(
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """desired_path (request surface) is always at the untokenized path."""
        monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
        monkeypatch.setenv(SUPERVISOR_STATE_TOKEN_ENV, VALID_TOKEN)
        assert supervise.desired_path() == supervise.supervisor_dir() / "desired.json"

    @staticmethod
    def test_status_path_always_untokenized(
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """status_path (observation surface) is always at the untokenized path."""
        monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
        monkeypatch.setenv(SUPERVISOR_STATE_TOKEN_ENV, VALID_TOKEN)
        assert supervise.status_path() == supervise.supervisor_dir() / "status.json"


# ---------------------------------------------------------------------------
# Token stripping from worker environment
# ---------------------------------------------------------------------------


class TestTokenStripping:
    """The supervisor state token must never reach worker/child processes."""

    @staticmethod
    def test_token_stripped_from_worker_env(
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """worker_env() excludes LUBKO_SUPERVISOR_STATE_TOKEN."""
        monkeypatch.setenv(SUPERVISOR_STATE_TOKEN_ENV, VALID_TOKEN)
        env = lifecycle.worker_env("test-incarnation")
        assert SUPERVISOR_STATE_TOKEN_ENV not in env

    @staticmethod
    def test_token_absent_when_not_set(
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """worker_env() never injects the token even when absent."""
        monkeypatch.delenv(SUPERVISOR_STATE_TOKEN_ENV, raising=False)
        env = lifecycle.worker_env("test-incarnation")
        assert SUPERVISOR_STATE_TOKEN_ENV not in env

    @staticmethod
    def test_lifecycle_token_preserved(
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The lifecycle/incarnation token is preserved (distinct from state token)."""
        monkeypatch.setenv(SUPERVISOR_STATE_TOKEN_ENV, VALID_TOKEN)
        env = lifecycle.worker_env("my-incarnation")
        assert env["LUBKO_LIFECYCLE_TOKEN"] == "my-incarnation"  # ruff: ignore[hardcoded-password-string]

    @staticmethod
    def test_other_env_vars_preserved(
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Non-credential, non-state-token env vars are inherited."""
        monkeypatch.setenv(SUPERVISOR_STATE_TOKEN_ENV, VALID_TOKEN)
        monkeypatch.setenv("MY_CUSTOM_VAR", "hello")
        env = lifecycle.worker_env("test")
        assert env["MY_CUSTOM_VAR"] == "hello"

    @staticmethod
    def test_credential_vars_still_stripped(
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Credential variables are still stripped alongside the state token."""
        monkeypatch.setenv(SUPERVISOR_STATE_TOKEN_ENV, VALID_TOKEN)
        monkeypatch.setenv("PGPASSWORD", "secret")
        monkeypatch.setenv("DATABASE_URL", "postgres://...")
        env = lifecycle.worker_env("test")
        assert "PGPASSWORD" not in env
        assert "DATABASE_URL" not in env
        assert SUPERVISOR_STATE_TOKEN_ENV not in env


# ---------------------------------------------------------------------------
# Namespace isolation regression: accidental collision class
# ---------------------------------------------------------------------------


class TestNamespaceIsolation:
    """Tokenized namespace prevents accidental collision with live authority.

    The original accidental collision class: two supervisor instances (or a
    supervisor and a CLI tool) resolving state at the same untokenized path
    could silently overwrite each other's authority.  The token isolates
    authoritative mutable state so default resolution cannot reach live
    supervisor authority.
    """

    @staticmethod
    def test_tokenized_authority_is_isolated(
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """Authority written under a token is not reachable without the token."""
        monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))

        # Write authority with a specific token
        monkeypatch.setenv(SUPERVISOR_STATE_TOKEN_ENV, VALID_TOKEN)
        authority_path = supervise.private_authority_path()
        authority_path.parent.mkdir(parents=True, exist_ok=True)
        authority_path.write_text(
            json.dumps({"spawning": {"token": "abc"}}),
            encoding="utf-8",
        )

        # Without the token, private_authority_path resolves to a different location
        monkeypatch.delenv(SUPERVISOR_STATE_TOKEN_ENV, raising=False)
        fallback_path = supervise.private_authority_path()
        assert fallback_path != authority_path
        assert not fallback_path.exists()

    @staticmethod
    def test_different_tokens_create_isolated_namespaces(
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """Two different tokens resolve to distinct directories."""
        monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))

        token_a = secrets.token_hex(32)
        token_b = secrets.token_hex(32)

        monkeypatch.setenv(SUPERVISOR_STATE_TOKEN_ENV, token_a)
        dir_a = supervise.supervisor_private_dir()

        monkeypatch.setenv(SUPERVISOR_STATE_TOKEN_ENV, token_b)
        dir_b = supervise.supervisor_private_dir()

        assert dir_a != dir_b
        assert dir_a.parent == dir_b.parent  # same parent, different children

    @staticmethod
    def test_state_path_unaffected_by_token(
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """state_path (CLI-readable) never changes with the token."""
        monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
        path_no_token = supervise.state_path()

        monkeypatch.setenv(SUPERVISOR_STATE_TOKEN_ENV, VALID_TOKEN)
        path_with_token = supervise.state_path()

        assert path_no_token == path_with_token

    @staticmethod
    def test_request_surface_unaffected_by_token(
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """desired_path (request surface) never changes with the token."""
        monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
        path_no_token = supervise.desired_path()

        monkeypatch.setenv(SUPERVISOR_STATE_TOKEN_ENV, VALID_TOKEN)
        path_with_token = supervise.desired_path()

        assert path_no_token == path_with_token

    @staticmethod
    def test_observation_surface_unaffected_by_token(
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """status_path (observation surface) never changes with the token."""
        monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
        path_no_token = supervise.status_path()

        monkeypatch.setenv(SUPERVISOR_STATE_TOKEN_ENV, VALID_TOKEN)
        path_with_token = supervise.status_path()

        assert path_no_token == path_with_token


# ---------------------------------------------------------------------------
# Backward compatibility
# ---------------------------------------------------------------------------


class TestBackwardCompatibility:
    """Without a token, all paths resolve to their original untokenized locations."""

    @staticmethod
    def test_all_paths_untokenized_without_token(
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """Every path function returns the untokenized path when no token is set."""
        monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
        monkeypatch.delenv(SUPERVISOR_STATE_TOKEN_ENV, raising=False)

        base = supervise.supervisor_dir()
        assert supervise.state_path() == base / "state.json"
        assert supervise.desired_path() == base / "desired.json"
        assert supervise.status_path() == base / "status.json"
        assert supervise.supervisor_pid_path() == base / "supervisor.pid"
        assert supervise.supervisor_private_dir() == base
        assert supervise.private_authority_path() == base / "authority.json"

    @staticmethod
    def test_read_state_without_token(
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """read_state() works without a token (backward compatible)."""
        monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
        monkeypatch.delenv(SUPERVISOR_STATE_TOKEN_ENV, raising=False)
        state = supervise.read_state()
        assert state.schema_version == supervise.SCHEMA_VERSION
        assert state.child is None
