"""Database driver for the zero-space acceptance probe.

Every operation here rides the real PostgreSQL transport: role/database
setup as superuser, migration plus grants, write-free job submission, and
bounded polling that asserts the exact published stdout, stderr, and result.
The acceptance shell script shells out to this driver so no ``psql`` client
binary is required in the acceptance environment.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import TYPE_CHECKING, Final, cast

import psycopg

from lubko import lifecycle_authority as authority
from lubko.protocol import PROTOCOL_VERSION, build_payload

if TYPE_CHECKING:
    from collections.abc import Sequence

MIGRATION: Final = Path(__file__).resolve().parents[2] / "migrations"
MIGRATION_FILE: Final = MIGRATION / "0001_two_column_protocol.sql"

POLL_INTERVAL_SECONDS: Final = 0.5
AWAIT_TIMEOUT_SECONDS: Final = 240.0
READY_TIMEOUT_SECONDS: Final = 90.0

_JOB_PROCESS_PREFIX: Final = ["sh"]
_JOB_SHELL_FLAG: Final = "-c"


def _connect(settings: dict[str, str]) -> psycopg.Connection[object]:
    """Open a PostgreSQL connection from explicit settings.

    Args:
        settings: Keyword arguments for :func:`psycopg.connect`.

    Returns:
        An open connection.
    """
    return psycopg.connect(**settings)  # type: ignore[arg-type]


def cmd_setup(args: argparse.Namespace) -> int:
    """Create the role and database, then apply the transport contract.

    Args:
        args: Parsed arguments carrying ``app_*`` and ``setup_*`` settings.

    Returns:
        A process exit code.
    """
    setup_settings = {
        "host": args.setup_host,
        "port": args.setup_port,
        "user": args.setup_user,
        "password": args.setup_password,
        "dbname": args.setup_dbname,
    }
    app_role = args.app_user
    app_password = args.app_password
    app_db = args.app_dbname
    with _connect(setup_settings) as conn:
        conn.autocommit = True
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (app_role,))
            if cursor.fetchone() is None:
                # DDL takes no bound parameters: interpolate quoted literals.
                cursor.execute(
                    f"CREATE ROLE {_quote_ident(app_role)} WITH LOGIN "
                    f"PASSWORD {_quote_literal(app_password)}"
                )
            cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (app_db,))
            if cursor.fetchone() is None:
                cursor.execute(
                    f"CREATE DATABASE {_quote_ident(app_db)} OWNER {_quote_ident(app_role)}"
                )
    app_settings = {
        "host": args.setup_host,
        "port": args.setup_port,
        "user": args.setup_user,
        "password": args.setup_password,
        "dbname": app_db,
    }
    with _connect(app_settings) as conn:
        conn.autocommit = True
        migration = MIGRATION_FILE.read_text(encoding="utf-8")
        with conn.cursor() as cursor:
            cursor.execute(migration)
            cursor.execute(f"GRANT USAGE ON SCHEMA lubko TO {_quote_ident(app_role)}")
            cursor.execute(
                "GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE lubko.jobs "
                f"TO {_quote_ident(app_role)}"
            )
    sys.stdout.write(f"setup: database {app_db} owned by {app_role}\n")
    return 0


def _quote_ident(name: str) -> str:
    """Quote an SQL identifier without trusting client quoting.

    Args:
        name: Raw identifier text.

    Returns:
        The double-quoted identifier.

    Raises:
        ValueError: If the identifier contains a NUL byte.
    """
    if "\x00" in name:
        msg = "identifier contains a NUL byte"
        raise ValueError(msg)
    return '"' + name.replace('"', '""') + '"'


def _quote_literal(value: str) -> str:
    """Quote a string literal for the setup DDL statements.

    Args:
        value: Raw literal text.

    Returns:
        The single-quoted literal.

    Raises:
        ValueError: If the value contains a NUL byte.
    """
    if "\x00" in value:
        msg = "literal contains a NUL byte"
        raise ValueError(msg)
    return "'" + value.replace("'", "''") + "'"


def cmd_submit(args: argparse.Namespace) -> int:
    """Submit one filesystem-write-free job and print its UUID.

    Args:
        args: Parsed arguments carrying connection settings, the target
            server, and the output marker.

    Returns:
        A process exit code.
    """
    marker = args.marker
    payload = build_payload(
        server=args.server,
        cwd="/",
        process=[
            *_JOB_PROCESS_PREFIX,
            _JOB_SHELL_FLAG,
            f'printf "%s\\n" "{marker}-stdout"; printf "%s\\n" "{marker}-stderr" >&2',
        ],
        version=PROTOCOL_VERSION,
    )
    settings = {
        "host": args.pg_host,
        "port": args.pg_port,
        "dbname": args.pg_dbname,
        "user": args.pg_user,
        "password": args.pg_password,
    }
    with _connect(settings) as conn, conn.transaction(), conn.cursor() as cursor:
        cursor.execute(
            "INSERT INTO lubko.jobs(payload) VALUES (%s) RETURNING id",
            (json.dumps(payload),),
        )
        row = cursor.fetchone()
    if row is None:
        sys.stderr.write("submit: no job id returned\n")
        return 1
    job_id = cast("tuple[object, ...]", row)[0]
    sys.stdout.write(f"{job_id}\n")
    return 0


def _read_payload(cursor: psycopg.Cursor[object], job_id: str) -> dict[str, object] | None:
    """Read one job payload as a mapping.

    Args:
        cursor: Open cursor on the application database.
        job_id: Job UUID text.

    Returns:
        The decoded payload, or ``None`` when the row is absent.

    Raises:
        TypeError: If the stored payload is not a JSON object.
    """
    cursor.execute("SELECT payload FROM lubko.jobs WHERE id = %s", (job_id,))
    row = cursor.fetchone()
    if row is None:
        return None
    raw = cast("tuple[object, ...]", row)[0]
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    decoded = json.loads(str(raw))
    if not isinstance(decoded, dict):
        msg = "job payload is not a JSON object"
        raise TypeError(msg)
    return decoded


def cmd_ready(args: argparse.Namespace) -> int:
    """Wait until the restarted supervisor owns the lifecycle authority.

    Args:
        args: Parsed arguments carrying connection settings and the
            expected execution-server identity.

    Returns:
        A process exit code, nonzero when no live owner appears in time.
    """
    settings = {
        "host": args.pg_host,
        "port": args.pg_port,
        "dbname": args.pg_dbname,
        "user": args.pg_user,
        "password": args.pg_password,
    }
    row_id = str(authority.authority_row_id(args.server))
    deadline = time.monotonic() + READY_TIMEOUT_SECONDS
    with _connect(settings) as conn:
        while time.monotonic() < deadline:
            with conn.transaction(), conn.cursor() as cursor:
                payload = _read_payload(cursor, row_id)
            if payload is not None:
                try:
                    row = authority.parse_authority_payload(payload, server=args.server)
                except authority.AuthorityError:
                    row = None
                if row is not None and row.owner is not None and _pid_live(row.owner.pid):
                    sys.stdout.write(
                        f"ready: server {args.server} owned by pid "
                        f"{row.owner.pid} at epoch {row.epoch}\n"
                    )
                    return 0
            time.sleep(POLL_INTERVAL_SECONDS)
    sys.stderr.write(f"ready: server {args.server} gained no live owner in time\n")
    return 1


def _pid_live(pid: int) -> bool:
    """Return whether a process identity is currently alive.

    Args:
        pid: Process identifier to probe.

    Returns:
        ``True`` when signalling the process succeeds.
    """
    try:
        os.kill(pid, 0)
    except (OSError, ValueError, OverflowError):
        return False
    return True


def cmd_await(args: argparse.Namespace) -> int:
    """Wait for one job to publish its exact stdout, stderr, and result.

    Args:
        args: Parsed arguments carrying connection settings, the job UUID,
            and the expected output marker.

    Returns:
        A process exit code, nonzero with a diagnostic when the job does
        not publish exactly the expected terminal state in time.
    """
    marker = args.marker
    expected_out = f"{marker}-stdout\n"
    expected_err = f"{marker}-stderr\n"
    settings = {
        "host": args.pg_host,
        "port": args.pg_port,
        "dbname": args.pg_dbname,
        "user": args.pg_user,
        "password": args.pg_password,
    }
    deadline = time.monotonic() + AWAIT_TIMEOUT_SECONDS
    last_status = "<no row>"
    with _connect(settings) as conn:
        while time.monotonic() < deadline:
            with conn.transaction(), conn.cursor() as cursor:
                payload = _read_payload(cursor, args.job_id)
            if payload is not None:
                state = payload.get("state")
                if isinstance(state, dict):
                    last_status = str(state.get("status", last_status))
                    if last_status in {"succeeded", "failed", "cancelled"}:
                        return _check_terminal(
                            args.job_id,
                            last_status,
                            payload,
                            expected_out,
                            expected_err,
                        )
            time.sleep(POLL_INTERVAL_SECONDS)
    sys.stderr.write(
        f"await: job {args.job_id} never reached a terminal state (last status {last_status})\n"
    )
    return 1


def _check_terminal(
    job_id: str,
    status: str,
    payload: dict[str, object],
    expected_out: str,
    expected_err: str,
) -> int:
    """Assert the exact terminal publication of one finished job.

    Args:
        job_id: Job UUID text for diagnostics.
        status: Terminal status string already observed.
        payload: Decoded job payload.
        expected_out: Exact expected stdout text.
        expected_err: Exact expected stderr text.

    Returns:
        A process exit code, nonzero describing the first mismatch.
    """
    if status != "succeeded":
        sys.stderr.write(f"await: job {job_id} terminal status {status}\n")
        return 1
    result = payload.get("result")
    if not isinstance(result, dict):
        sys.stderr.write(f"await: job {job_id} published no result section\n")
        return 1
    failures: list[str] = []
    if result.get("stdout") != expected_out:
        failures.append(f"result.stdout is {result.get('stdout')!r}")
    if result.get("stderr") != expected_err:
        failures.append(f"result.stderr is {result.get('stderr')!r}")
    if result.get("exit_code") != 0:
        failures.append(f"result.exit_code is {result.get('exit_code')!r}")
    output = payload.get("output")
    if isinstance(output, dict):
        for stream, expected in (("stdout", expected_out), ("stderr", expected_err)):
            section = output.get(stream)
            if not isinstance(section, dict) or section.get("tail") != expected:
                failures.append(f"output.{stream}.tail is not exactly {expected!r}")
    else:
        failures.append("no output live-tail section was published")
    if failures:
        sys.stderr.write(f"await: job {job_id} mismatches: {'; '.join(failures)}\n")
        return 1
    sys.stdout.write(f"await: job {job_id} succeeded with exact stdout/stderr/result\n")
    return 0


def build_parser() -> argparse.ArgumentParser:
    """Build the driver command line parser.

    Returns:
        The configured parser.
    """
    parser = argparse.ArgumentParser(prog="zero-space-drive")
    sub = parser.add_subparsers(dest="command", required=True)

    setup = sub.add_parser("setup", help="create role/database and migrate")
    setup.add_argument("--setup-host", default="127.0.0.1")
    setup.add_argument("--setup-port", type=int, default=5432)
    setup.add_argument("--setup-user", default="postgres")
    setup.add_argument("--setup-password", default="postgres")
    setup.add_argument("--setup-dbname", default="postgres")
    setup.add_argument("--app-user", default="lubko_zero_space")
    setup.add_argument("--app-password", default="lubko_zero_space")
    setup.add_argument("--app-dbname", default="lubko_zero_space")
    setup.set_defaults(func=cmd_setup)

    submit = sub.add_parser("submit", help="submit one write-free job")
    submit.add_argument("--pg-host", default="127.0.0.1")
    submit.add_argument("--pg-port", type=int, default=5432)
    submit.add_argument("--pg-dbname", default="lubko_zero_space")
    submit.add_argument("--pg-user", default="lubko_zero_space")
    submit.add_argument("--pg-password", default="lubko_zero_space")
    submit.add_argument("--server", required=True)
    submit.add_argument("--marker", required=True)
    submit.set_defaults(func=cmd_submit)

    await_parser = sub.add_parser("await", help="await exact job publication")
    await_parser.add_argument("--pg-host", default="127.0.0.1")
    await_parser.add_argument("--pg-port", type=int, default=5432)
    await_parser.add_argument("--pg-dbname", default="lubko_zero_space")
    await_parser.add_argument("--pg-user", default="lubko_zero_space")
    await_parser.add_argument("--pg-password", default="lubko_zero_space")
    await_parser.add_argument("--job-id", required=True)
    await_parser.add_argument("--marker", required=True)
    await_parser.set_defaults(func=cmd_await)

    ready = sub.add_parser("ready", help="await a live authority owner")
    ready.add_argument("--pg-host", default="127.0.0.1")
    ready.add_argument("--pg-port", type=int, default=5432)
    ready.add_argument("--pg-dbname", default="lubko_zero_space")
    ready.add_argument("--pg-user", default="lubko_zero_space")
    ready.add_argument("--pg-password", default="lubko_zero_space")
    ready.add_argument("--server", required=True)
    ready.set_defaults(func=cmd_ready)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the driver command.

    Args:
        argv: Command line arguments, or ``None`` to use ``sys.argv``.

    Returns:
        A process exit code.
    """
    args = build_parser().parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
