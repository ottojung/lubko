-- Lubko per-server PostgreSQL authorization boundary.
--
-- TRUST / AUTHORITY MODEL
--
-- The PostgreSQL instance is the authoritative trust boundary for queue
-- isolation, not only the worker's application-layer predicates. Every
-- execution server owns exactly one non-empty server identity, and its worker
-- daemon (and its local host processes: the outer supervisor, lifecycle
-- probes, and deploy control) connects as a dedicated login principal
-- lubko_worker_<server> that is a member of the lubko_worker_role group. The
-- server identity bound to a database session is derived from the immutable
-- authenticated login role (session_user), which can never be changed into
-- another server's principal, so a local process is role-bound to exactly one
-- execution server and cannot impersonate another.
--
-- CRITICAL: a SECOND config file or env var on the SAME host is NOT a boundary.
-- The local worker-host processes therefore hold NO broad (BYPASSRLS)
-- credential at all; every local operation is server-scoped by RLS. The only
-- broad credential is lubko_admin, which is granted BYPASSRLS strictly for the
-- EXTERNAL orchestrator / control plane and must be provisioned only on a
-- separate host/account. It is never present on an execution-server host, so the
-- worker cannot read or inherit it. That is a real OS/account boundary, not a
-- filename distinction.
--
-- Isolation is enforced at the database authorization boundary three ways:
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
--      server equals the session's bound identity, both for reads and for writes
--      (INSERT WITH CHECK prevents spoofing another server's rows).
--
-- ADDITIONALLY, an output_chunk may only reference a command root of the SAME
-- server: the trigger jobs_chunk_root_server enforces, at the database
-- authority, that chunk.thread points to a root whose server equals the chunk's
-- own server. It uses the SECURITY DEFINER lubko.chunk_root_server() so the
-- root lookup is not subject to (and does not recurse through) the session's RLS.
--
-- The worker application keeps its own exact server-match predicate on every
-- query as defense in depth; the database boundary is what makes cross-server
-- access impossible even if that predicate were removed or bypassed.
--
-- PROVISIONING (operator responsibility, not performed here because server and
-- role names are deployment-specific):
--
--   -- Per execution server (on the server host, restricted to that host only):
--   CREATE ROLE lubko_worker_alpha LOGIN PASSWORD '...';
--   GRANT lubko_worker_role TO lubko_worker_alpha;
--   INSERT INTO lubko.server_principals (server, principal)
--       VALUES ('alpha', 'lubko_worker_alpha');
--
--   -- External orchestrator / control plane (separate host/account ONLY):
--   ALTER ROLE lubko_admin PASSWORD '...';   -- role created by this migration
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

-- Trusted session identity: the server bound to the authenticated login role.
-- SECURITY DEFINER + restricted search_path so the caller cannot influence the
-- lookup. CRITICAL SEMANTIC: it reads session_user (the immutable authenticated
-- login role), NOT current_user, because inside SECURITY DEFINER current_user
-- becomes the function definer and would make every caller resolve to the same
-- identity. session_user cannot be changed via SET ROLE, so it is the only
-- non-spoofable per-session login identity.
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

-- Group that holds the table grants; per-server worker principals are members.
do $$
begin
    if to_regrole('lubko_worker_role') is null then
        execute 'create role lubko_worker_role nologin';
    end if;
    -- lubko_worker (legacy shared login) stays a plain, RLS-scoped member: it
    -- receives NO bypassrls and therefore cannot exceed its own server's rows.
    -- It is provisioned on execution-server hosts only.
    if to_regrole('lubko_worker') is not null then
        execute 'grant lubko_worker_role to lubko_worker';
    end if;
    -- lubko_admin is the EXTERNAL orchestrator/control-plane credential. It is
    -- granted BYPASSRLS but MUST be provisioned only on a separate host/account;
    -- it must never exist on an execution-server host, so the worker cannot read
    -- or inherit it. This is a real OS/account boundary, not a config-file name.
    if to_regrole('lubko_admin') is null then
        execute 'create role lubko_admin login';
    end if;
    execute 'grant lubko_worker_role to lubko_admin';
    execute 'alter role lubko_admin bypassrls';
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

-- Same-server chunk ownership enforced at the database authority. An
-- output_chunk must reference a command root of the SAME server. The root lookup
-- is performed ONLY inside this single SECURITY DEFINER trigger function; there is
-- no standalone, externally callable privileged helper that returns a foreign
-- root's server. SECURITY DEFINER + restricted search_path so the lookup runs as
-- the owner (independent of, and not filtered by, the session RLS) and cannot be
-- influenced by search_path. EXECUTE is revoked from PUBLIC and granted only to the
-- worker/admin roles so the trigger still fires for them, but no worker-facing
-- principal can directly invoke a cross-server lookup.
create or replace function lubko.enforce_chunk_root_server() returns trigger
language plpgsql security definer set search_path = lubko as $$
declare
    root_server text;
begin
    if (new.payload::jsonb)->>'type' = 'output_chunk' then
        select (payload::jsonb)->>'server'
        into root_server
        from lubko.jobs
        where id = ((new.payload::jsonb)->>'thread')::uuid
            and (payload::jsonb)->>'type' = 'command';
        if root_server is distinct from (new.payload::jsonb)->>'server' then
            -- Generic diagnostic only: never reveal the foreign root's server
            -- value, which would turn this trigger into a cross-server metadata
            -- oracle (a worker could probe arbitrary root UUIDs and read their
            -- server from the error text).
            raise exception using
                message = 'output_chunk.thread references a command root owned by '
                    'a different execution server; cross-server chunk ownership '
                    'is forbidden',
                hint = 'ensure the chunk thread references a command root owned '
                    'by the same execution server';
        end if;
    end if;
    return new;
end;
$$;

comment on function lubko.enforce_chunk_root_server() is
    'BEFORE INSERT/UPDATE trigger enforcing that an output_chunk.thread references '
    'a command root of the same server. The root lookup is inlined here and runs '
    'as the function owner (SECURITY DEFINER), so it is not a callable, '
    'data-returning helper: no worker-facing principal can invoke a cross-server '
    'lookup directly. EXECUTE is revoked from PUBLIC.';

-- Remove the default PUBLIC EXECUTE and grant only to the roles that legitimately
-- fire the trigger (workers, and the external orchestrator). The owner retains
-- EXECUTE implicitly, so the trigger body can read the root.
revoke execute on function lubko.enforce_chunk_root_server() from public;
grant execute on function lubko.enforce_chunk_root_server() to lubko_worker_role;
grant execute on function lubko.enforce_chunk_root_server() to lubko_admin;

drop trigger if exists jobs_chunk_root_server on lubko.jobs;
create trigger jobs_chunk_root_server
    before insert or update on lubko.jobs
    for each row execute function lubko.enforce_chunk_root_server();

commit;
