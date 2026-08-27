# Per-server PostgreSQL authorization boundary

This document specifies the trust/authority model and operational procedure for
enforcing protocol-v4 per-server queue isolation at the PostgreSQL authorization
boundary (issue #285). The application-layer server predicate (`SERVER_MATCH_SQL`
in `src/lubko/worker.py`) remains as defense in depth; the database boundary is
what makes cross-server access *impossible* even if that predicate were removed,
bypassed, or spoofed.

## Trust / authority model

1. **PostgreSQL is the authoritative trust boundary.** Multiple execution
   servers may share one physical `lubko.jobs` table, but no server's credential
   may read, mutate, delete, or forge rows belonging to another server.
2. **Exactly one server identity per session, bound to credentials.** Each
   execution server has exactly one configured non-empty `server` identity (the
   `server` setting of its restricted worker configuration file). Its worker
   daemon connects as a dedicated login principal `lubko_worker_<server>` that is
   a member of the `lubko_worker_role` group. The server bound to a session is
   derived from the authenticated login role (`current_user`), which is immutable
   for the session and is never granted to another server's principal. A worker
   therefore cannot rebind to, or impersonate, another server.
3. **Output chunks obey the same ownership.** Chunks are rows in the same
   `lubko.jobs` table, so the same boundary covers them: a worker may only see
   and insert chunks whose `server` equals its own.
4. **Orchestrator/admin retain broader privileges.** The orchestrator/admin
   principal (`lubko_worker`) is granted `BYPASSRLS` and so bypasses row-level
   security. It keeps its intended cross-server capabilities (transport GC across
   servers, lifecycle and recovery control). The boundary restricts only the
   per-server worker principals.

## Mechanism (`migrations/0004_server_isolation_boundary.sql`)

- `lubko.server_principals(server, principal)` — authoritative, owner-written
  mapping from execution-server identity to its dedicated login principal. This
  is the root of trust.
- `lubko.session_server()` — `SECURITY DEFINER` function returning the server
  mapped to `current_user`. Because it depends only on the login role and reads
  the owner-owned mapping, the result cannot be forged by the session.
- Row-level security is enabled on `lubko.jobs`.
- Four policies (`jobs_isolation_select/insert/update/delete`) for the
  `lubko_worker_role` group restrict every operation to rows whose
  `server = lubko.session_server()`. The `INSERT` policy uses `WITH CHECK`, so a
  worker cannot spoof rows for another server. A principal with no mapping
  resolves to `NULL` and matches nothing and can insert nothing — fail closed.

The worker additionally verifies, at connect time:

- `verify_server_isolation(conn)` — refuses to run unless RLS is enabled, the
  `session_server()` function exists, and the isolation policies are installed.
- `verify_server_identity(conn, server)` — refuses to run unless the connected
  principal resolves to exactly its configured `server`.

## Bootstrap provisioning (fresh install)

Apply migrations in order (`0001`, `0002`, `0003`, `0004`), then provision each
server's principal and mapping as the schema owner:

```sql
-- One role per execution server; never share a role across servers.
CREATE ROLE lubko_worker_alpha LOGIN PASSWORD '...';
GRANT lubko_worker_role TO lubko_worker_alpha;

INSERT INTO lubko.server_principals (server, principal)
    VALUES ('alpha', 'lubko_worker_alpha');
```

Each worker's `database.conf` then uses `user=lubko_worker_alpha` (and its own
`worker.conf` uses `server=alpha`). The orchestrator/admin `database.conf` uses
`user=lubko_worker` (which the migration marks `BYPASSRLS`).

## Upgrade guidance (existing v4 deployment)

1. Quiesce new submissions and let in-flight work become durably terminal.
2. Apply `migrations/0004_server_isolation_boundary.sql` (idempotent; wraps in a
   single transaction).
3. For each execution server, create its dedicated role and mapping as above; the
   worker `database.conf` `user` must change from the shared `lubko_worker` to
   `lubko_worker_<server>`. The `server` setting in `worker.conf` is unchanged
   and must equal the mapped `server`.
4. Keep the orchestrator/admin `database.conf` on `user=lubko_worker` — the
   migration grants it `BYPASSRLS`, preserving its broader privileges.
5. Restart each worker. It will refuse to start unless its principal resolves to
   its configured server; a misprovisioned or unmapped role fails closed with a
   diagnostic pointing at `lubko.server_principals`.
6. Prove a fresh round trip per server. No `lubko.jobs` truncation is required by
   this migration (it adds access control only; the two-column invariant and v4
   payload shape are untouched).

## Testing

The canonical fast suite (`uv run pytest`) includes deterministic authorization
tests in `tests/test_db_server_isolation.py` covering: the boundary-verification
logic (present, and each missing element failing closed), the session-identity
binding (exact match accepted; mismatch/unmapped rejected), the retained
application server predicate on `claim_jobs`/`recover_stale_jobs`, and the
migration's declared RLS/function/policies. A live PostgreSQL is not required
for these. Operators should additionally validate `0004` against a staging
database: connect as `lubko_worker_<server>`, confirm a row for another server
is invisible and an `INSERT` of a foreign-server chunk is rejected by `WITH
CHECK`.
