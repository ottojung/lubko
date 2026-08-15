-- Lubko transport upgrade: protocol v2 type-aware constraints and chunk indexes.
--
-- The two-column invariant is unchanged: lubko.jobs keeps exactly two columns
-- forever (id uuid, payload text). This migration upgrades an installation
-- that already applied 0001_two_column_protocol.sql so its constraints become
-- type-aware:
--
--   * the v1 jobs_payload_has_status check required state.status on EVERY row,
--     which an immutable output_chunk row can never satisfy;
--   * protocol v2 therefore replaces it with a type-aware shape check:
--     command rows need a request object and state.status, while
--     output_chunk rows need explicit thread ownership and value/offset shape;
--   * the claim queue index is restricted to command rows;
--   * expression indexes support structured output-chunk ownership/order reads
--     instead of substring matching.
--
-- Every statement is idempotent and safe to apply more than once. Fresh
-- installations already receive this shape from the updated 0001 baseline;
-- this migration only repairs older tables.

alter table lubko.jobs drop constraint if exists jobs_payload_has_status;
alter table lubko.jobs drop constraint if exists jobs_payload_type_shape;

alter table lubko.jobs add constraint jobs_payload_type_shape check (
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
);

drop index if exists jobs_queue_idx;
create index if not exists jobs_queue_idx
    on lubko.jobs (
        ((payload::jsonb)->'state'->>'status'),
        ((payload::jsonb)->'state'->>'created_at')
    )
    where ((payload::jsonb)->>'type') = 'command';

create index if not exists jobs_chunk_owner_idx
    on lubko.jobs (((payload::jsonb)->>'thread'))
    where ((payload::jsonb)->>'type') = 'output_chunk';

create index if not exists jobs_chunk_order_idx
    on lubko.jobs (
        ((payload::jsonb)->>'thread'),
        (((payload::jsonb)->'sequence')::bigint)
    )
    where ((payload::jsonb)->>'type') = 'output_chunk';
