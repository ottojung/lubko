"""Real PostgreSQL regression for malformed opaque transport rows."""

from __future__ import annotations

import json
import os
from dataclasses import replace
from typing import TYPE_CHECKING

import psycopg
import pytest

from lubko.protocol_versioning import CURRENT_PROTOCOL_VERSION
from lubko.worker import (
    JobResult,
    Settings,
    bulk_refresh_leases,
    claim_jobs,
    collect_transport,
    discover_cancellations,
    finish_job,
    reap_unsupported_jobs,
    recover_stale_jobs,
    request_cancel,
)

if TYPE_CHECKING:
    from uuid import UUID

DSN = os.environ.get("LUBKO_TEST_POSTGRES_DSN")
pytestmark = pytest.mark.skipif(DSN is None, reason="real PostgreSQL DSN not configured")


def _payload(*, server: str, status: str, **state: object) -> str:
    return json.dumps({
        "v": CURRENT_PROTOCOL_VERSION,
        "type": "command",
        "server": server,
        "request": {"cwd": "/", "process": ["true"]},
        "state": {
            "status": status,
            "created_at": "2026-01-01T00:00:00.000000Z",
            **state,
        },
    })


def _insert(conn: psycopg.Connection[object], payload: str) -> UUID:
    with conn.cursor() as cursor:
        cursor.execute("INSERT INTO lubko.jobs(payload) VALUES (%s) RETURNING id", (payload,))
        row = cursor.fetchone()
    assert row is not None
    return row[0]


def test_malformed_rows_do_not_poison_worker_operations() -> None:
    """Malformed/future rows stay inert while valid worker operations complete."""
    assert DSN is not None
    server = "malformed-queue-test"
    with psycopg.connect(DSN, autocommit=True) as conn:
        with conn.cursor() as cursor:
            cursor.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")
            cursor.execute("DROP SCHEMA IF EXISTS lubko CASCADE")
            cursor.execute("CREATE SCHEMA lubko")
            cursor.execute(
                "CREATE TABLE lubko.jobs ("
                "id uuid PRIMARY KEY DEFAULT gen_random_uuid(), payload text NOT NULL)"
            )

        malformed_plain = _insert(conn, "not-json")
        malformed_escape = _insert(conn, '{"broken": "\\q"')
        unrelated = _insert(conn, "[]")
        future = _insert(
            conn,
            json.dumps({
                "v": CURRENT_PROTOCOL_VERSION + 1000,
                "type": "command",
                "server": server,
                "request": {"cwd": "/", "process": ["false"]},
                "state": {
                    "status": "pending",
                    "created_at": "2026-01-01T00:00:00.000001Z",
                },
            }),
        )
        other_server = _insert(conn, _payload(server="other-server", status="pending"))
        pending = _insert(conn, _payload(server=server, status="pending"))

        settings = Settings.from_environment(server=server)
        claimed = claim_jobs(conn, settings, 10)
        assert [job.id for job in claimed] == [pending]
        assert bulk_refresh_leases(conn, settings, [pending]) == [pending]

        with conn.cursor() as cursor:
            cursor.execute(
                "UPDATE lubko.jobs SET payload = jsonb_set("
                "payload::jsonb, '{state,cancel_requested_at}', "
                "to_jsonb('2026-01-01T00:00:01.000000Z'::text))::text WHERE id = %s",
                (pending,),
            )
        assert pending in discover_cancellations(conn, settings)

        result = JobResult(
            status="succeeded",
            exit_code=0,
            stdout="",
            stderr="",
            cancellation_note=None,
        )
        assert finish_job(conn, pending, result, server=server) == "cancelled"

        cancellable = _insert(conn, _payload(server=server, status="pending"))
        assert request_cancel(conn, cancellable, server=server) == "cancelled"

        stale = _insert(
            conn,
            _payload(
                server=server,
                status="running",
                lease_expires_at="2020-01-01T00:00:00.000000Z",
                worker_id="dead-worker",
                worker_incarnation="dead-incarnation",
            ),
        )
        recovered = recover_stale_jobs(conn, server)
        assert stale in {job_id for job_id, _payload_text in recovered}

        assert reap_unsupported_jobs(conn, settings, 100) == []
        gc_settings = replace(settings, gc_retention_seconds=0.0)
        collect_transport(conn, gc_settings)

        with conn.cursor() as cursor:
            for row_id in (malformed_plain, malformed_escape, unrelated, future, other_server):
                cursor.execute("SELECT 1 FROM lubko.jobs WHERE id = %s", (row_id,))
                assert cursor.fetchone() is not None
