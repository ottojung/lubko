-- Lubko transport queue baseline: the canonical protocol v3 two-column schema.
--
-- THE INVARIANT
--
-- The Lubko transport table lubko.jobs has exactly two columns forever:
--   (1) id      uuid primary key  -- unique random identifier
--   (2) payload text not null     -- one string containing a JSON object
-- All evolving job/request/result/state/cancellation/process-identity/output
-- data lives inside that JSON payload. Never add a third column.
--
-- Protocol evolution happens inside payload (see docs/protocol.md), versioned
-- by the top-level "v" field. SQL casts payload::jsonb only transiently for
-- predicates and atomic jsonb_set updates; every stored value is ::text.
--
-- Protocol v3 distinguishes command rows from immutable output_chunk rows.
-- A command request carries exactly one executable field: request.process,
-- but protocol shape is validated entirely in the payload parser
-- (src/lubko/protocol.py): a command row must carry a request object plus a
-- lifecycle state.status, while output_chunk rows must carry thread ownership
-- and offset/value shape. The parser requires request.process to be a
-- non-empty array of non-empty strings and rejects the legacy protocol v2
-- request.command / request.args keys outright; none of that validation is
-- encoded as SQL. Because the v2 -> v3 change is content-only, the physical
-- two-column table does NOT change between versions. Claim/recovery queries
-- operate only on command rows, and the worker verifies this output-chunk
-- shape at startup.
--
-- This file is the complete current schema for a fresh installation. It is
-- idempotent: every statement is safe to run more than once. The two-column
-- table is the only supported binding: no staging table, no older schema, and
-- no rollback path to maintain.
--
-- The v2 -> v3 cutover is DESTRUCTIVE and needs NO DDL upgrade. Protocol v3
-- does not accept v2 rows and there is no migration, drain, or compatibility
-- path; the physical schema is identical for v2 and v3. The supported cutover
-- is: stop every queue consumer, purge the transport contents with
-- `truncate lubko.jobs` (dropping every old root command row and every
-- output_chunk row), then start a v3 worker against the same table. No v2 row
-- is transformed, migrated, or preserved, and no existing table is altered.

create table if not exists lubko.jobs (
    id uuid primary key default gen_random_uuid(),
    payload text not null
        constraint jobs_payload_is_json_object check (jsonb_typeof(payload::jsonb) = 'object')
        constraint jobs_payload_has_version check ((payload::jsonb) ? 'v')
        constraint jobs_payload_type_shape check (
            case
                when (payload::jsonb)->>'type' = 'command' then
                    jsonb_typeof((payload::jsonb)->'request') = 'object'
                    and (((payload::jsonb)->'state'->>'status') is not null)
                when (payload::jsonb)->>'type' = 'output_chunk' then
                    jsonb_typeof((payload::jsonb)->'value') = 'string'
                    and (((payload::jsonb)->>'thread') is not null)
                    and (((payload::jsonb)->>'stream') in ('stdout', 'stderr'))
                    and (((payload::jsonb)->>'sequence') ~ '^[0-9]+$')
                    and (((payload::jsonb)->>'start') ~ '^[0-9]+$')
                    and (((payload::jsonb)->>'end') ~ '^[0-9]+$')
                else true
            end
        )
);

-- Worker role access is part of the binding: lubko_worker is the stable role
-- the worker connects as (see README configuration). Protocol v3 requires the
-- worker to read and claim jobs (SELECT, UPDATE), to finalize and publish
-- output (UPDATE), and to insert immutable output_chunk rows (INSERT). It also
-- needs USAGE on the lubko schema to reach the table. GRANT is idempotent, and
-- it is guarded by to_regrole so a fresh environment where the role is not yet
-- provisioned does not fail.
do $$
begin
    if to_regrole('lubko_worker') is not null then
        execute 'grant usage on schema lubko to lubko_worker';
        execute 'grant select, insert, update on table lubko.jobs to lubko_worker';
    end if;
end
$$;

comment on table lubko.jobs is
    'Lubko transport queue (two-column protocol). INVARIANT: exactly two '
    'columns forever: id (unique random) and payload (one string containing '
    'a JSON object). All job/request/result/state/cancellation/'
    'process-identity/output data lives inside payload. Never add a third '
    'column.';

-- Pending command queue: only command rows are ever claimed.
create index if not exists jobs_queue_idx
    on lubko.jobs (
        ((payload::jsonb)->'state'->>'status'),
        ((payload::jsonb)->'state'->>'created_at')
    )
    where ((payload::jsonb)->>'type') = 'command';

-- Immutable output chunks: ownership and deterministic ordering reads.
create index if not exists jobs_chunk_owner_idx
    on lubko.jobs (((payload::jsonb)->>'thread'))
    where ((payload::jsonb)->>'type') = 'output_chunk';

create index if not exists jobs_chunk_order_idx
    on lubko.jobs (
        ((payload::jsonb)->>'thread'),
        (((payload::jsonb)->'sequence')::bigint)
    )
    where ((payload::jsonb)->>'type') = 'output_chunk';
