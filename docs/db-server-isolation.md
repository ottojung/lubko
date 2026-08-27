# Per-server PostgreSQL authorization boundary

This document specifies the trust/authority model and operational procedure for
enforcing protocol-v4 per-server queue isolation at the PostgreSQL authorization
boundary. The application-layer server predicate (`SERVER_MATCH_SQL` in
`src/lubko/worker.py`) remains as defense in depth; the database boundary is what
makes cross-server access *impossible* even if that predicate were removed,
bypassed, or spoofed.

## Trust / authority model

1. **PostgreSQL is the authoritative trust boundary.** Multiple execution servers
   may share one physical `lubko.jobs` table, but no server's credential may read,
   mutate, delete, or forge rows belonging to another server.
2. **Exactly one server identity per session, bound to credentials.** Each
   execution server has exactly one configured non-empty `server` identity (the
   `server` setting of its restricted worker configuration file). Its worker daemon
   — and every local worker-host process (the outer supervisor, lifecycle probes,
   and deploy control) — connects as a dedicated login principal
   `lubko_worker_<server>` that is a member of the `lubko_worker_role` group. The
   server bound to a session is derived from the immutable authenticated login role
   (`session_user`), which can never be changed into another server's principal, so
   a local process is role-bound to exactly one execution server.
3. **Output chunks obey same-server ownership — both row- and reference-level.**
   Chunks are rows in the same `lubko.jobs` table, so the row-level boundary
   covers them, and additionally a chunk's `thread` must reference a *command root of
   the same server* (enforced by a trigger at the database authority; see below).
4. **The broad credential is external, not local.** A second config file or env var
   on the *same* host is **not** a boundary: a compromised local process could read
   the sibling file or inherited environment. Therefore local worker-host processes
   hold **no** broad (`BYPASSRLS`) credential at all — every local operation is
   server-scoped by RLS. The only broad credential is `lubko_admin`, granted
   `BYPASSRLS` strictly for the **external orchestrator / control plane**, and must
   be provisioned only on a **separate host/account**. It is never present on an
   execution-server host, so the worker cannot read or inherit it. This is a real
   OS/account boundary, not a filename distinction.

## Mechanism (`migrations/0004_server_isolation_boundary.sql`)

- `lubko.server_principals(server, principal)` — authoritative, owner-written
  mapping from execution-server identity to its dedicated login principal. This is
  the root of trust.
- `lubko.session_server()` — `SECURITY DEFINER` function returning the server
  mapped to `session_user` (the authenticated login role). Because it depends only
  on the login role and reads the owner-owned mapping, the result cannot be forged
  by the session. It must read `session_user`, **not** `current_user`: inside
  `SECURITY DEFINER`, `current_user` is the function *definer*, so using it would
  make every caller resolve to the same identity and silently destroy the
  boundary.
- Row-level security is enabled on `lubko.jobs`.
- Four policies (`jobs_isolation_select/insert/update/delete`) for the
  `lubko_worker_role` group restrict every operation to rows whose
  `server = lubko.session_server()`. The `INSERT` policy uses `WITH CHECK`, so a
  worker cannot spoof rows for another server. A principal with no mapping resolves
  to `NULL` and matches nothing and can insert nothing — fail closed.
- `lubko.chunk_root_server(thread)` — `SECURITY DEFINER` function returning the
  `server` of the command root referenced by an output chunk's `thread`. It is
  `SECURITY DEFINER` so the lookup is independent of, and does not recurse through,
  the session's RLS scope.
- `jobs_chunk_root_server` — `BEFORE INSERT OR UPDATE` trigger on `lubko.jobs` that,
  for `output_chunk` rows, raises unless the referenced root's server equals the
  chunk's own `server`. This enforces at the database authority that a chunk cannot
  be attached to a command root owned by a different execution server, regardless
  of the application predicate.

The worker additionally verifies, at connect time:

- `verify_server_isolation(conn)` — refuses to run unless RLS is enabled, the
  `session_server()` and `chunk_root_server()` functions exist, the
  `jobs_chunk_root_server` trigger is installed, and the isolation policies are
  present.
- `verify_server_identity(conn, server)` — refuses to run unless the connected
  principal resolves to exactly its configured `server`.

## Bootstrap provisioning (fresh install)

Apply migrations in order (`0001`, `0002`, `0003`, `0004`), then provision each
server's principal and mapping as the schema owner:

```sql
-- One role per execution server; never share a role across servers.
-- Provision ONLY on the execution-server host.
CREATE ROLE lubko_worker_alpha LOGIN PASSWORD '...';
GRANT lubko_worker_role TO lubko_worker_alpha;

INSERT INTO lubko.server_principals (server, principal)
    VALUES ('alpha', 'lubko_worker_alpha');
```

Each worker's `database.conf` then uses `user=lubko_worker_alpha` (and its own
`worker.conf` uses `server=alpha`). The local supervisor/lifecycle/deployctl paths
use the same per-server `database.conf`; none of them receive a broad credential.

The `lubko_admin` broad credential is created by the migration but its password
must be set and it must be used **only** by the external orchestrator on a separate
host/account:

```sql
-- External orchestrator / control plane ONLY (separate host/account).
ALTER ROLE lubko_admin PASSWORD '...';
```

## Upgrade guidance (existing v4 deployment)

1. Quiesce new submissions and let in-flight work become durably terminal.
2. Apply `migrations/0004_server_isolation_boundary.sql` (idempotent; wraps in a
   single transaction).
3. For each execution server, create its dedicated role and mapping as above; the
   worker `database.conf` `user` must change from the shared `lubko_worker` to
   `lubko_worker_<server>`. The `server` setting in `worker.conf` is unchanged and
   must equal the mapped `server`.
4. Provision `lubko_admin` (password + separate host/account) for the external
   orchestrator only. **Do not** place its credential on execution-server hosts.
5. Restart each worker. It will refuse to start unless its principal resolves to
   its configured server; a misprovisioned or unmapped role fails closed with a
   diagnostic pointing at `lubko.server_principals`.
6. Prove a fresh round trip per server. No `lubko.jobs` truncation is required by
   this migration (it adds access control only; the two-column invariant and v4
   payload shape are untouched).

## Testing

The canonical fast suite (`uv run pytest`) includes deterministic authorization
tests in `tests/test_db_server_isolation.py` covering: the boundary-verification
logic (present, and each missing element — RLS, identity function, same-server
chunk enforcement, policies — failing closed), the session-identity binding (exact
match accepted; mismatch/unmapped rejected), the retained application server
predicate on `claim_jobs`/`recover_stale_jobs`, and the migration's declared
RLS/function/trigger/policies. A live PostgreSQL is not required for these.

A genuine end-to-end test of PostgreSQL RLS/trigger semantics would require a
running server. The repository's test contract forbids slow/heavyweight process
startup and any optional/skip/integration test tier, and no PostgreSQL binary is
available in the canonical environment, so such a test is intentionally not part
of the suite. Operators must additionally validate `0004` against a staging
database: connect as `lubko_worker_<server>`, confirm another server's rows are
invisible, an `INSERT` of a foreign-server chunk is rejected by `WITH CHECK`, and
an `INSERT` of a chunk whose `thread` references a foreign-server root is rejected
by the `jobs_chunk_root_server` trigger.
