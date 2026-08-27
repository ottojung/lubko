-- Lubko per-server PostgreSQL authorization boundary (issue #285).
--
-- TRUST / AUTHORITY MODEL
--
-- The PostgreSQL instance is the authoritative trust boundary for queue
-- isolation, not only the worker's application-layer predicates. Every
-- execution server owns exactly one non-empty server identity, and its worker
-- daemon connects as a dedicated login principal lubko_worker_<server> that is a
-- member of the lubko_worker_role group. The server identity bound to a database
-- session is derived from the authenticated login role (current_user), which is
-- immutable for the session and can never be changed into another server's
-- principal, so a worker credential is cryptographically/role-bound to exactly
-- one execution server and cannot impersonate another.
--
-- Isolation is enforced three ways, all at the database authorization boundary:
--
--   1. Row-level security is enabled on lubko.jobs.
--   2. lubko.session_server() returns the server mapped to the authenticated
--      login principal. It is SECURITY DEFINER so it reads lubko.server_principals
--      regardless of the caller's own grants, and its result cannot be forged by
--      the session. CRITICAL: inside SECURITY DEFINER, current_user is the
--      function *definer*, not the caller, so the function must read session_user
--      (the immutable authenticated login role) to reflect the connecting
--      principal. Using current_user would collapse every caller to the same
--      identity and destroy the isolation boundary.
--   3. Per-server policies restrict the worker group to rows/chunks whose
--      server equals the session's bound identity, both for reads and for
--      writes (INSERT WITH CHECK prevents spoofing another server's rows).
--
-- The orchestrator/admin principal (lubko_worker) is granted BYPASSRLS and
-- retains its intended broader, cross-server privileges (GC across servers,
-- lifecycle/recovery control). The worker application keeps its own exact
-- server-match predicate on every query as defense in depth; the database
-- boundary is what makes cross-server access impossible even if that predicate
-- were removed or bypassed.
--
-- PROVISIONING (operator responsibility, not performed here because server and
-- role names are deployment-specific):
--
--   CREATE ROLE lubko_worker_alpha LOGIN PASSWORD '...';
--   GRANT lubko_worker_role TO lubko_worker_alpha;
--   INSERT INTO lubko.server_principals (server, principal)
--       VALUES ('alpha', 'lubko_worker_alpha');
--
-- The worker refuses to run unless the boundary is present AND its connected
-- principal resolves (via lubko.session_server()) to its configured server,
-- failing closed. See docs/db-server-isolation.md for full bootstrap and
-- upgrade guidance.
--
-- Idempotent: safe to apply more than once; every object is created guarded or
-- replace/IF EXISTS.

begin;

-- Authoritative server -> login-principal mapping. Only the schema owner can
-- write it; the SECURITY DEFINER function reads it on behalf of workers.
create table if not exists lubko.server_principals (
    server text primary key,
    principal text not null unique
);

comment on table lubko.server_principals is
    'Authoritative mapping from an execution-server identity to the dedicated '
    'PostgreSQL login principal that owns its queue. Written only by the schema '
    'owner/admin; read by the SECURITY DEFINER lubko.session_server() to bind a '
    'database session to exactly one execution server. This is the root of trust '
    'for per-server queue isolation.';

-- Trusted session identity: the server bound to the current login principal.
-- SECURITY DEFINER + restricted search_path so the caller cannot influence the
-- lookup. CRITICAL SEMANTIC: it reads session_user (the authenticated login
-- role), NOT current_user, because inside SECURITY DEFINER current_user becomes
-- the function definer and would make every caller resolve to the same identity.
-- session_user is immutable for the session and cannot be changed via SET ROLE,
-- so it is the only non-spoofable per-session login identity.
create or replace function lubko.session_server() returns text
language sql stable security definer set search_path = lubko as $$
    select server from lubko.server_principals where principal = session_user;
$$;

comment on function lubko.session_server() is
    'Return the execution-server identity bound to the authenticated login role '
    '(session_user), or NULL if the principal is unmapped. SECURITY DEFINER: the '
    'result is derived solely from session_user and the admin-owned '
    'server_principals table, so a worker session cannot rebind to another '
    'server. session_user (not current_user) is used on purpose: current_user is '
    'the definer inside SECURITY DEFINER and must never be used here.';

-- Group that holds the table grants; both the per-server worker principals and
-- the orchestrator/admin principal are members.
do $$
begin
    if to_regrole('lubko_worker_role') is null then
        execute 'create role lubko_worker_role nologin';
    end if;
    if to_regrole('lubko_worker') is not null then
        -- Orchestrator/admin keeps its broader, cross-server privileges.
        execute 'grant lubko_worker_role to lubko_worker';
        execute 'alter role lubko_worker bypassrls';
    end if;
end
$$;

grant usage on schema lubko to lubko_worker_role;
grant select, insert, update, delete on table lubko.jobs to lubko_worker_role;
grant execute on function lubko.session_server() to lubko_worker_role;

-- Enforce the boundary: enable RLS and install per-server policies.
alter table lubko.jobs enable row level security;

drop policy if exists jobs_isolation_select on lubko.jobs;
create policy jobs_isolation_select on lubko.jobs
    for select to lubko_worker_role
    using ((payload::jsonb)->>'server' = lubko.session_server());

drop policy if exists jobs_isolation_insert on lubko.jobs;
create policy jobs_isolation_insert on lubko.jobs
    for insert to lubko_worker_role
    with check ((payload::jsonb)->>'server' = lubko.session_server());

drop policy if exists jobs_isolation_update on lubko.jobs;
create policy jobs_isolation_update on lubko.jobs
    for update to lubko_worker_role
    using ((payload::jsonb)->>'server' = lubko.session_server())
    with check ((payload::jsonb)->>'server' = lubko.session_server());

drop policy if exists jobs_isolation_delete on lubko.jobs;
create policy jobs_isolation_delete on lubko.jobs
    for delete to lubko_worker_role
    using ((payload::jsonb)->>'server' = lubko.session_server());

commit;
