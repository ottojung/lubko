-- Lubko protocol v3 -> v4 cutover DDL: routing-aware payload shape.
--
-- This migration upgrades an EXISTING two-column `lubko.jobs` table from the
-- v3 payload-shape constraint to the canonical protocol v4 shape. Protocol v4
-- requires every valid `command` and `output_chunk` payload to carry a
-- required non-empty top-level `server` field naming the execution server
-- that owns the row (see docs/protocol.md). Fresh installations get this
-- shape directly from migrations/0001_two_column_protocol.sql and do not need
-- this file, but it is idempotent and safe to run on either a fresh or an
-- upgraded table.
--
-- SUPPORTED CUTOVER ORDER (destructive; run only while quiescent):
--
--   1. Quiesce all submitters and daemons (no new submissions, no claims).
--   2. Let any in-flight work become durably terminal and drained.
--   3. TRUNCATE lubko.jobs — the row cutover is destructive: no legacy row is
--      converted and there is NO default server. Every old root command row
--      and its output_chunk history is discarded.
--   4. Apply this migration (preflight check, then drop + recreate + validate
--      the constraint, then rebuild the queue index).
--   5. Start the v4 daemon(s) with their configured LUBKO_SERVER identity and
--      prove a fresh round trip.
--
-- The ENTIRE migration runs inside one explicit transaction whose FIRST
-- statement is the nonconforming-row preflight: applying this file against a
-- table that still holds legacy rows (v3 payloads without a server field)
-- raises before ANY schema-changing DDL has run, so the original v3
-- constraint and index state is left completely intact. There is no possible
-- half-upgraded state. Truncating first is what makes validation trivially
-- succeed; truncating before applying is required, and applying before
-- truncating is refused by design.
--
-- The two-column invariant is untouched: exactly `id uuid` and
-- `payload text not null` forever; no column is added, removed, or altered.

begin;

do $$
declare
    nonconforming bigint;
begin
    select count(*) into nonconforming
    from lubko.jobs
    where ((payload::jsonb)->>'type') in ('command', 'output_chunk')
        and (
            coalesce(jsonb_typeof((payload::jsonb)->'server'), '') <> 'string'
            or (payload::jsonb)->>'server' = ''
            or ((payload::jsonb)->'v') <> '4'::jsonb
        );

    if nonconforming > 0 then
        raise exception using
            message = (
                'lubko.jobs still holds '
                || nonconforming::text
                || ' command/output_chunk payload(s) without a non-empty '
                || 'server string field or without protocol version 4. The '
                || 'protocol v4 cutover is destructive: quiesce, truncate '
                || 'lubko.jobs, then re-run this migration.'
            ),
            hint = 'truncate lubko.jobs while quiescent, then apply migrations/0003_protocol_v4_server_routing.sql';
    end if;
end
$$;

alter table lubko.jobs
    drop constraint if exists jobs_payload_type_shape;

alter table lubko.jobs
    add constraint jobs_payload_type_shape check (
    case
        when (payload::jsonb)->>'type' = 'command' then
            ((payload::jsonb)->'v') = '4'::jsonb
            and jsonb_typeof((payload::jsonb)->'request') = 'object'
            and (((payload::jsonb)->'state'->>'status') is not null)
            and coalesce(jsonb_typeof((payload::jsonb)->'server'), '') = 'string'
            and coalesce((payload::jsonb)->>'server', '') <> ''
        when (payload::jsonb)->>'type' = 'output_chunk' then
            ((payload::jsonb)->'v') = '4'::jsonb
            and jsonb_typeof((payload::jsonb)->'value') = 'string'
                and (((payload::jsonb)->>'thread') is not null)
                and coalesce(jsonb_typeof((payload::jsonb)->'server'), '') = 'string'
                and coalesce((payload::jsonb)->>'server', '') <> ''
                and (((payload::jsonb)->>'stream') in ('stdout', 'stderr'))
                and (((payload::jsonb)->>'sequence') ~ '^[0-9]+$')
                and (((payload::jsonb)->>'start') ~ '^[0-9]+$')
                and (((payload::jsonb)->>'end') ~ '^[0-9]+$')
            else true
        end
    ) not valid;

alter table lubko.jobs
    validate constraint jobs_payload_type_shape;

-- Routing-aware pending-queue index: claims select command rows by server,
-- status, and creation order, so the server identity leads the index.
drop index if exists lubko.jobs_queue_idx;
create index jobs_queue_idx
    on lubko.jobs (
        ((payload::jsonb)->>'server'),
        ((payload::jsonb)->'state'->>'status'),
        ((payload::jsonb)->'state'->>'created_at')
    )
    where ((payload::jsonb)->>'type') = 'command';

comment on table lubko.jobs is
    'Lubko transport queue (two-column protocol). INVARIANT: exactly two '
    'columns forever: id (unique random) and payload (one string containing '
    'a JSON object). All job/request/result/state/cancellation/'
    'process-identity/output data lives inside payload. Never add a third '
    'column. Protocol v4: every command/output_chunk payload carries a '
    'required non-empty top-level server field.';

commit;
