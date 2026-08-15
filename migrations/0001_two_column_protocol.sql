-- Lubko transport queue baseline: the two-column protocol.
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
-- Protocol v2 distinguishes command rows from immutable output_chunk rows.
-- Constraints are type-aware: command rows must carry a request object and a
-- lifecycle state.status, while output_chunk rows must carry thread ownership
-- and offset/value shape. Claim/recovery queries operate only on command rows.
--
-- This file is the complete current schema for a fresh installation. It is
-- idempotent: every statement is safe to run more than once. The two-column
-- table is the only supported binding: no staging table, no older schema, and
-- no rollback path to maintain.

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
-- the worker connects as (see README configuration), and it needs SELECT and
-- UPDATE on the transport table to claim, cancel, poll, and finalize jobs.
-- GRANT is idempotent, and it is guarded by to_regrole so a fresh environment
-- where the role is not yet provisioned does not fail.
do $$
begin
    if to_regrole('lubko_worker') is not null then
        execute 'grant select, update on table lubko.jobs to lubko_worker';
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
