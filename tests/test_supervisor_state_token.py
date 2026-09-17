"""Supervisor state namespace token invariants.

Tests cover token validation, tokenized path resolution, environment
stripping, namespace isolation (collision-class regression), fail-closed
behavior when absent, and the global singleton gate invariant.
"""

from __future__ import annotations

import fcntl
import json
import os
import secrets
from typing import TYPE_CHECKING

import pytest

from lubko import lifecycle, supervise, supervise_client, supervisor
from lubko.control_socket import bind_abstract_socket
from lubko.durable import write_json_durable
from lubko.state import (
    SUPERVISOR_STATE_TOKEN_ENV,
    SupervisorStateTokenError,
    supervisor_state_token,
    validate_supervisor_state_token,
)
from lubko.supervise import SupervisorDesired as SupervisedDesired

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
# Fail-closed: private paths require the token
# ---------------------------------------------------------------------------


class TestFailClosed:
    """Private authority paths raise when the token is absent."""

    @staticmethod
    def test_private_dir_requires_token(
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """supervisor_private_dir() raises without a token."""
        monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
        monkeypatch.delenv(SUPERVISOR_STATE_TOKEN_ENV, raising=False)
        with pytest.raises(SupervisorStateTokenError):
            supervise.supervisor_private_dir()

    @staticmethod
    def test_state_path_requires_token(
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """state_path() raises without a token."""
        monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
        monkeypatch.delenv(SUPERVISOR_STATE_TOKEN_ENV, raising=False)
        with pytest.raises(SupervisorStateTokenError):
            supervise.state_path()

    @staticmethod
    def test_desired_path_requires_token(
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """desired_path() raises without a token."""
        monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
        monkeypatch.delenv(SUPERVISOR_STATE_TOKEN_ENV, raising=False)
        with pytest.raises(SupervisorStateTokenError):
            supervise.desired_path()

    @staticmethod
    def test_pending_request_path_accessible_without_token(
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """pending_request_path() is accessible without a token."""
        monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
        monkeypatch.delenv(SUPERVISOR_STATE_TOKEN_ENV, raising=False)
        expected = supervise.supervisor_dir() / "pending-request.json"
        assert supervise.pending_request_path() == expected

    @staticmethod
    def test_private_authority_path_requires_token(
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """private_authority_path() raises without a token."""
        monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
        monkeypatch.delenv(SUPERVISOR_STATE_TOKEN_ENV, raising=False)
        with pytest.raises(SupervisorStateTokenError):
            supervise.private_authority_path()


# ---------------------------------------------------------------------------
# Tokenized path resolution
# ---------------------------------------------------------------------------


class TestTokenizedPaths:
    """Tokenized directory and file paths are correct with a valid token."""

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
    def test_state_path_tokenized(
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """state_path lives under the tokenized directory."""
        monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
        monkeypatch.setenv(SUPERVISOR_STATE_TOKEN_ENV, VALID_TOKEN)
        expected = supervise.supervisor_dir() / VALID_TOKEN / "state.json"
        assert supervise.state_path() == expected

    @staticmethod
    def test_desired_path_requires_token(
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """desired_path() raises without a token."""
        monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
        monkeypatch.delenv(SUPERVISOR_STATE_TOKEN_ENV, raising=False)
        with pytest.raises(SupervisorStateTokenError):
            supervise.desired_path()

    @staticmethod
    def test_pending_request_path_without_token(
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """pending_request_path() is always accessible without a token."""
        monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
        monkeypatch.delenv(SUPERVISOR_STATE_TOKEN_ENV, raising=False)
        expected = supervise.supervisor_dir() / "pending-request.json"
        assert supervise.pending_request_path() == expected

    @staticmethod
    def test_private_authority_path_tokenized(
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """authority.json lives under the tokenized directory."""
        monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
        monkeypatch.setenv(SUPERVISOR_STATE_TOKEN_ENV, VALID_TOKEN)
        expected = supervise.supervisor_dir() / VALID_TOKEN / "authority.json"
        assert supervise.private_authority_path() == expected

    @staticmethod
    def test_status_path_untokenized(
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """status_path (observation surface) is always at the untokenized path."""
        monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
        monkeypatch.setenv(SUPERVISOR_STATE_TOKEN_ENV, VALID_TOKEN)
        assert supervise.status_path() == supervise.supervisor_dir() / "status.json"

    @staticmethod
    def test_pid_path_untokenized(
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """supervisor_pid_path is always at the untokenized path."""
        monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
        monkeypatch.setenv(SUPERVISOR_STATE_TOKEN_ENV, VALID_TOKEN)
        assert supervise.supervisor_pid_path() == (supervise.supervisor_dir() / "supervisor.pid")


# ---------------------------------------------------------------------------
# Global singleton gate: lock is NOT tokenized
# ---------------------------------------------------------------------------


class TestGlobalSingletonGate:
    """The supervisor ownership lock is the global singleton gate.

    It must NOT be tokenized so two differently-tokened supervisors
    cannot both become authority.  The lock is a non-authoritative
    runtime mechanism; it does not reveal or select tokens.
    """

    @staticmethod
    def test_lock_path_untokenized(
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """supervisor_lock_path is always at the untokenized path."""
        monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
        monkeypatch.setenv(SUPERVISOR_STATE_TOKEN_ENV, VALID_TOKEN)
        assert supervise.supervisor_lock_path() == (supervise.supervisor_dir() / ".supervisor.lock")

    @staticmethod
    def test_lock_path_independent_of_token(
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """Lock path is identical regardless of which token is set."""
        monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
        token_a = secrets.token_hex(32)
        token_b = secrets.token_hex(32)

        monkeypatch.setenv(SUPERVISOR_STATE_TOKEN_ENV, token_a)
        path_a = supervise.supervisor_lock_path()

        monkeypatch.setenv(SUPERVISOR_STATE_TOKEN_ENV, token_b)
        path_b = supervise.supervisor_lock_path()

        assert path_a == path_b

    @staticmethod
    def test_token_a_owns_gate_token_b_fails(
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """Token A supervisor holds the gate; token B cannot acquire it.

        This is the core singleton invariant: the untokenized flock
        prevents two differently-tokened supervisors from running
        concurrently.
        """
        monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
        lock_path = supervise.supervisor_lock_path()
        lock_path.parent.mkdir(parents=True, exist_ok=True)

        # Token A acquires the global gate
        fd_a = os.open(str(lock_path), os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(fd_a, fcntl.LOCK_EX | fcntl.LOCK_NB)

            # Token B tries to acquire the same gate — must fail
            fd_b = os.open(str(lock_path), os.O_RDWR | os.O_CREAT, 0o600)
            try:
                with pytest.raises(BlockingIOError):
                    fcntl.flock(fd_b, fcntl.LOCK_EX | fcntl.LOCK_NB)
            finally:
                os.close(fd_b)
        finally:
            os.close(fd_a)


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
    def test_authority_files_isolated_by_token(
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """Authority written under token A is not reachable under token B."""
        monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))

        token_a = secrets.token_hex(32)
        token_b = secrets.token_hex(32)

        # Write authority under token A
        monkeypatch.setenv(SUPERVISOR_STATE_TOKEN_ENV, token_a)
        path_a = supervise.private_authority_path()
        path_a.parent.mkdir(parents=True, exist_ok=True)
        path_a.write_text(
            json.dumps({"spawning": {"token": "a"}}),
            encoding="utf-8",
        )

        # Token B resolves to a different path
        monkeypatch.setenv(SUPERVISOR_STATE_TOKEN_ENV, token_b)
        path_b = supervise.private_authority_path()
        assert path_b != path_a
        assert not path_b.exists()

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
# No token leak in public surfaces
# ---------------------------------------------------------------------------


class TestNoTokenLeak:
    """Token values never appear in logs, status, diagnostics, or public paths."""

    @staticmethod
    def test_token_not_in_status_path(
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """status_path contains no trace of the token value."""
        monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
        monkeypatch.setenv(SUPERVISOR_STATE_TOKEN_ENV, VALID_TOKEN)
        assert VALID_TOKEN not in str(supervise.status_path())

    @staticmethod
    def test_token_not_in_pid_path(
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """supervisor_pid_path contains no trace of the token value."""
        monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
        monkeypatch.setenv(SUPERVISOR_STATE_TOKEN_ENV, VALID_TOKEN)
        assert VALID_TOKEN not in str(supervise.supervisor_pid_path())

    @staticmethod
    def test_token_not_in_lock_path(
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """supervisor_lock_path contains no trace of the token value."""
        monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
        monkeypatch.setenv(SUPERVISOR_STATE_TOKEN_ENV, VALID_TOKEN)
        assert VALID_TOKEN not in str(supervise.supervisor_lock_path())

    @staticmethod
    def test_token_not_in_log_path(
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """supervisor_log_path contains no trace of the token value."""
        monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
        monkeypatch.setenv(SUPERVISOR_STATE_TOKEN_ENV, VALID_TOKEN)
        assert VALID_TOKEN not in str(supervise.supervisor_log_path())


# ---------------------------------------------------------------------------
# Direct private authority resolution fails without token
# ---------------------------------------------------------------------------


class TestDirectResolutionFailsClosed:
    """Without the token, direct private authority resolution must fail.

    These tests explicitly REMOVE the token and verify that every private
    authority path function raises ``SupervisorStateTokenError``.  This
    proves that tokenless code cannot accidentally reach live supervisor
    authority through direct file access.
    """

    @staticmethod
    def test_supervisor_private_dir_requires_token(
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """supervisor_private_dir() raises without a token."""
        monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
        monkeypatch.delenv(SUPERVISOR_STATE_TOKEN_ENV, raising=False)
        with pytest.raises(SupervisorStateTokenError):
            supervise.supervisor_private_dir()

    @staticmethod
    def test_desired_path_requires_token(
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """desired_path() raises without a token."""
        monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
        monkeypatch.delenv(SUPERVISOR_STATE_TOKEN_ENV, raising=False)
        with pytest.raises(SupervisorStateTokenError):
            supervise.desired_path()

    @staticmethod
    def test_state_path_requires_token(
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """state_path() raises without a token."""
        monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
        monkeypatch.delenv(SUPERVISOR_STATE_TOKEN_ENV, raising=False)
        with pytest.raises(SupervisorStateTokenError):
            supervise.state_path()

    @staticmethod
    def test_private_authority_path_requires_token(
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """private_authority_path() raises without a token."""
        monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
        monkeypatch.delenv(SUPERVISOR_STATE_TOKEN_ENV, raising=False)
        with pytest.raises(SupervisorStateTokenError):
            supervise.private_authority_path()


# ---------------------------------------------------------------------------
# Control socket: supervisor-dead fails closed
# ---------------------------------------------------------------------------


class TestSupervisorDeadFailsClosed:
    """Without a running supervisor, socket operations fail closed.

    Tokenless CLIs must not fall back to direct file access when the
    supervisor is not running.  They must fail with a clear connection
    error.
    """

    @staticmethod
    def test_ping_fails_when_supervisor_dead(
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """Control socket ping fails when no supervisor is listening."""
        monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
        monkeypatch.delenv(SUPERVISOR_STATE_TOKEN_ENV, raising=False)
        assert supervise_client.supervisor_alive() is False

    @staticmethod
    def test_read_state_via_socket_fails_when_dead(
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """read_state_client raises when supervisor is not running."""
        monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
        monkeypatch.delenv(SUPERVISOR_STATE_TOKEN_ENV, raising=False)
        with pytest.raises(ConnectionRefusedError):
            supervise_client.read_state_client()

    @staticmethod
    def test_write_desired_via_socket_fails_when_dead(
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """write_desired_client raises when supervisor is not running."""
        monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
        monkeypatch.delenv(SUPERVISOR_STATE_TOKEN_ENV, raising=False)
        desired = SupervisedDesired(
            schema_version=1,
            generation=1,
            commit="a" * 40,
            repo=str(tmp_path / "repo"),
            uv_path="/usr/bin/uv",
            worker_id=None,
        )
        with pytest.raises((ConnectionRefusedError, OSError)):
            supervise_client.write_desired_client(desired)


# ---------------------------------------------------------------------------
# Client API: dual-mode behavior
# ---------------------------------------------------------------------------


class TestClientAPIDualMode:
    """Client API uses direct access with token, socket without."""

    @staticmethod
    def test_read_state_with_token_uses_direct(
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """With token, read_state_client reads directly from the file."""
        monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
        monkeypatch.setenv(SUPERVISOR_STATE_TOKEN_ENV, VALID_TOKEN)
        supervise.supervisor_private_dir().mkdir(parents=True, exist_ok=True)
        supervise.write_state(supervise.fresh_state())
        result = supervise_client.read_state_client()
        assert result is not None
        assert result.schema_version == supervise.SCHEMA_VERSION

    @staticmethod
    def test_read_state_without_token_uses_socket(
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """Without token, read_state_client raises when supervisor is dead."""
        monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
        monkeypatch.delenv(SUPERVISOR_STATE_TOKEN_ENV, raising=False)
        with pytest.raises(ConnectionRefusedError):
            supervise_client.read_state_client()

    @staticmethod
    def test_read_status_always_works(
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """read_status_cli reads from untokenized status.json (no token needed)."""
        monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
        monkeypatch.delenv(SUPERVISOR_STATE_TOKEN_ENV, raising=False)
        # No status.json exists, so result is None
        assert supervise_client.read_status_cli() is None


# ---------------------------------------------------------------------------
# supervisor.main fails closed on absent token
# ---------------------------------------------------------------------------


class TestMainFailsClosedWithoutToken:
    """supervisor.main must return exit code 1 when the token is absent."""

    @staticmethod
    def test_main_returns_error_without_token(
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """main() refuses to start when LUBKO_SUPERVISOR_STATE_TOKEN is absent."""
        monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
        monkeypatch.delenv(SUPERVISOR_STATE_TOKEN_ENV, raising=False)
        # No --status flag: falls through to token validation
        assert supervisor.main([]) == 1

    @staticmethod
    def test_main_returns_error_with_invalid_token(
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """main() refuses to start when LUBKO_SUPERVISOR_STATE_TOKEN is invalid."""
        monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
        monkeypatch.setenv(SUPERVISOR_STATE_TOKEN_ENV, "not-valid-hex!")
        assert supervisor.main([]) == 1


# ---------------------------------------------------------------------------
# Nonblocking control socket
# ---------------------------------------------------------------------------


class TestNonblockingControlSocket:
    """The listening socket must be nonblocking so reconciliation never stalls."""

    @staticmethod
    def test_accept_returns_immediately_without_client() -> None:
        """accept() on the bound socket raises BlockingIOError immediately.

        A blocking socket would hang until a client connects, blocking the
        supervisor reconciliation loop.  The socket must be nonblocking so
        accept() returns instantly when no clients are pending.
        """
        sock = bind_abstract_socket()
        try:
            with pytest.raises(BlockingIOError):
                sock.accept()
        finally:
            sock.close()


# ---------------------------------------------------------------------------
# Tokenless write isolation
# ---------------------------------------------------------------------------


class TestTokenlessWriteIsolation:
    """Tokenless desired writes cannot affect live supervisor authority."""

    @staticmethod
    def test_pending_request_does_not_create_desired_authority(
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """Writing to pending_request_path does not create tokenized desired.json.

        A tokenless install writes to the non-authoritative pending request
        surface.  This must never create or mutate the tokenized desired
        authority that the supervisor daemon reads.
        """
        monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
        monkeypatch.setenv(SUPERVISOR_STATE_TOKEN_ENV, VALID_TOKEN)

        # Write a pending request (tokenless path)
        request_path = supervise.pending_request_path()
        request_path.parent.mkdir(parents=True, exist_ok=True)
        write_json_durable(
            request_path,
            {"schema_version": 1, "commit": "a" * 40, "repo": "/r", "uv_path": "uv"},
        )

        # The tokenized desired.json must NOT exist
        desired = supervise.desired_path()
        assert not desired.exists(), f"pending request created undesired authority at {desired}"

    @staticmethod
    def test_promote_pending_request_creates_desired_authority(
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """promote_pending_request() promotes to tokenized desired_path.

        After promotion, the pending request file is removed and the
        tokenized desired authority contains the correct intent.
        """
        monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
        monkeypatch.setenv(SUPERVISOR_STATE_TOKEN_ENV, VALID_TOKEN)

        # Create the private directory
        supervise.supervisor_private_dir().mkdir(parents=True, exist_ok=True)

        # Write a pending request
        commit = "b" * 40
        request_path = supervise.pending_request_path()
        request_path.parent.mkdir(parents=True, exist_ok=True)
        write_json_durable(
            request_path,
            {"schema_version": 1, "commit": commit, "repo": "/r", "uv_path": "uv"},
        )

        # Promote
        result = supervise.promote_pending_request()
        assert result is True

        # Pending request file should be removed
        assert not request_path.exists()

        # Tokenized desired authority should exist with correct commit
        desired = supervise.read_desired_strict()
        assert desired is not None
        assert desired.commit == commit


# ---------------------------------------------------------------------------
# Generation reservation: atomic allocation prevents reuse
# ---------------------------------------------------------------------------


class TestGenerationReservation:
    """Supervisor-owned generation reservation prevents concurrent reuse.

    When a tokenless caller allocates a generation via the control socket,
    the supervisor durably reserves it so concurrent allocators cannot
    reuse the same generation.  The reservation is overwritten by the next
    allocation and does not require explicit clearing.
    """

    @staticmethod
    def test_reserved_generation_path_requires_token(
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """reserved_generation_path() raises without a token."""
        monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
        monkeypatch.delenv(SUPERVISOR_STATE_TOKEN_ENV, raising=False)
        with pytest.raises(SupervisorStateTokenError):
            supervise.reserved_generation_path()

    @staticmethod
    def test_reserved_generation_path_tokenized(
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """reserved_generation_path lives under the tokenized directory."""
        monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
        monkeypatch.setenv(SUPERVISOR_STATE_TOKEN_ENV, VALID_TOKEN)
        expected = supervise.supervisor_dir() / VALID_TOKEN / "reserved_generation.json"
        assert supervise.reserved_generation_path() == expected

    @staticmethod
    def test_reserved_generation_under_private_dir(
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """Reserved generation file lives under the private tokenized directory."""
        monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
        monkeypatch.setenv(SUPERVISOR_STATE_TOKEN_ENV, VALID_TOKEN)
        expected = supervise.supervisor_private_dir() / "reserved_generation.json"
        assert supervise.reserved_generation_path() == expected

    @staticmethod
    def test_next_generation_includes_reservation(
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """next_generation() includes a durable reservation in its max."""
        monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
        monkeypatch.setenv(SUPERVISOR_STATE_TOKEN_ENV, VALID_TOKEN)
        supervise.supervisor_private_dir().mkdir(parents=True, exist_ok=True)
        supervise.write_state(supervise.fresh_state())
        # No reservation: generation is 1 (max(0, 0, 0, 0) + 1)
        gen_no_reservation = supervise.next_generation()
        assert gen_no_reservation == 1
        # Write a reservation for generation 5
        write_json_durable(
            supervise.reserved_generation_path(),
            {"generation": 5},
        )
        gen_with_reservation = supervise.next_generation()
        assert gen_with_reservation == 6  # max(0, 0, 0, 5) + 1

    @staticmethod
    def test_reservation_overwritten_by_next_allocation(
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """Writing a new reservation overwrites the previous one."""
        monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
        monkeypatch.setenv(SUPERVISOR_STATE_TOKEN_ENV, VALID_TOKEN)
        supervise.supervisor_private_dir().mkdir(parents=True, exist_ok=True)
        supervise.write_state(supervise.fresh_state())
        # First reservation
        write_json_durable(
            supervise.reserved_generation_path(),
            {"generation": 5},
        )
        assert supervise.next_generation() == 6
        # Second reservation overwrites
        write_json_durable(
            supervise.reserved_generation_path(),
            {"generation": 10},
        )
        assert supervise.next_generation() == 11

    @staticmethod
    def test_malformed_reservation_ignored(
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """A malformed reservation file is treated as absent."""
        monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
        monkeypatch.setenv(SUPERVISOR_STATE_TOKEN_ENV, VALID_TOKEN)
        supervise.supervisor_private_dir().mkdir(parents=True, exist_ok=True)
        supervise.write_state(supervise.fresh_state())
        supervise.reserved_generation_path().write_text("not json", encoding="utf-8")
        # Malformed reservation is ignored; generation is 1
        assert supervise.next_generation() == 1

    @staticmethod
    def test_concurrent_allocations_get_distinct_generations(
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """Two sequential allocations never return the same generation.

        This simulates the race: allocate generation, don't publish yet,
        allocate again.  The second allocation must return a strictly
        greater generation because the first was durably reserved.
        """
        monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
        monkeypatch.setenv(SUPERVISOR_STATE_TOKEN_ENV, VALID_TOKEN)
        supervise.supervisor_private_dir().mkdir(parents=True, exist_ok=True)
        supervise.write_state(supervise.fresh_state())
        # Simulate first allocation: compute + reserve
        gen1 = supervise.next_generation()
        write_json_durable(
            supervise.reserved_generation_path(),
            {"generation": gen1},
        )
        # Simulate second allocation: sees first reservation
        gen2 = supervise.next_generation()
        assert gen2 > gen1, f"second allocation {gen2} must exceed first {gen1}"
