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
-- no rollback path to maintain. The schema also installs one trigger (and its
-- PL/pgSQL function) that emits a `lubko_jobs_changed` NOTIFY for the events
-- an idle worker should wake for; see the trigger block at the end of this
-- file. Notifications are only wakeups: the worker's durable scans remain
-- authoritative.
--
-- The v2 -> v3 cutover is DESTRUCTIVE and needs NO DDL upgrade. Protocol v3
-- does not accept v2 rows and there is no protocol-data drain/migration or compatibility
-- path; the physical schema is identical for v2 and v3. The cutover runs
-- against the live queue: quiesce new submissions, let any in-flight v2 work
-- become durably terminal, bring up and prove the v3 supervisor/worker, then
-- `truncate lubko.jobs` while quiescent (dropping every old root command row
-- and every output_chunk row), and prove a fresh v3 round trip. Truncating
-- before the first v3 start is equally valid; only the end state matters, and
-- that end state is an empty transport. No v2 row is transformed, migrated,
-- or preserved, and no existing table is altered.

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
        execute 'grant select, insert, update, delete on table lubko.jobs to lubko_worker';
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

-- Event-driven worker wakeups (issue #20): a trigger emits a PostgreSQL
-- NOTIFY whenever durable queue state changes that an idle worker should act
-- on promptly. Notifications are only wakeups: every notification wake simply
-- makes the worker re-run its authoritative durable scans, and those scans
-- remain the source of correctness, so a lost, reordered, or coalesced
-- notification is never correctness-critical.
--
-- The trigger only notifies for events produced by the queue's *external*
-- writers and never for the worker's own writes:
--   * a command row inserted in state `pending` (a job was submitted),
--   * an existing command row updated into state `pending` from any
--     non-pending state (a job was requeued externally), and
--   * a freshly introduced `cancel_requested_at` marker on a `running`
--     `command` row (a running job was cancelled).
-- Worker-generated claims (pending -> running), lease heartbeats, output
-- publications, finalization, and `output_chunk` inserts never notify, so a
-- busy worker is not woken in a loop by its own writes. An idle worker
-- therefore blocks waiting for PostgreSQL notifications instead of re-running
-- the claim query on a sleep timer; durable scans still bound worst-case
-- latency when a notification is missed (for example around a reconnect).
create or replace function lubko.notify_jobs_changed() returns trigger
language plpgsql
as $$
begin
    if (new.payload::jsonb)->>'type' <> 'command' then
        return new;
    end if;
    if TG_OP = 'INSERT' then
        if (new.payload::jsonb)->'state'->>'status' = 'pending' then
            perform
                pg_notify('lubko_jobs_changed', new.id::text);
        end if;
    elsif TG_OP = 'UPDATE' then
        if (new.payload::jsonb)->'state'->>'status' = 'pending'
            and (old.payload::jsonb)->'state'->>'status' <> 'pending' then
            perform
                pg_notify('lubko_jobs_changed', new.id::text);
        elsif (new.payload::jsonb)->'state'->>'status' = 'running'
            and (old.payload::jsonb)->'state'->>'cancel_requested_at' is null
            and (new.payload::jsonb)->'state'->>'cancel_requested_at' is not null then
            perform
                pg_notify('lubko_jobs_changed', new.id::text);
        end if;
    end if;
    return new;
end
$$;

comment on function lubko.notify_jobs_changed() is
    'Emit a lubko_jobs_changed NOTIFY wakeup for command rows that enter the '
    'pending state (newly submitted or externally requeued) and for freshly '
    'introduced running-job cancellation markers. Worker-generated claims, '
    'lease heartbeats, output publications, finalization, and output_chunk '
    'inserts never notify, so the worker is not woken by its own writes. '
    'Notifications are wakeups only; durable scans remain authoritative.';

drop trigger if exists lubko_jobs_notify_wakeups on lubko.jobs;
create trigger lubko_jobs_notify_wakeups
    after insert or update on lubko.jobs
    for each row
    execute function lubko.notify_jobs_changed();
