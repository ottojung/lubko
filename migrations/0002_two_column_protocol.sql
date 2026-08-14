-- Two-column Lubko transport protocol: additive preparation.
--
-- THE INVARIANT
--
-- The Lubko transport table lubko.jobs has exactly two columns forever:
--   (1) id      uuid primary key  -- unique random identifier
--   (2) payload text not null     -- one string containing a JSON object
-- All evolving job/request/result/state/cancellation/process-identity data
-- lives inside that JSON payload. Never add a third column.
--
-- Protocol evolution happens inside payload (see docs/protocol.md), versioned
-- by the top-level "v" field. SQL casts payload::jsonb only transiently for
-- predicates and atomic jsonb_set updates; every stored value is ::text.
--
-- This migration is ADDITIVE and safe to apply while the legacy multi-column
-- worker is still running:
--
--   1. creates lubko.jobs_v2, the two-column transport table;
--   2. adds its CHECK constraints, expression indexes, and invariant comment;
--   3. backfills lubko.jobs_v2 from the legacy lubko.jobs, converting each row
--      into a protocol v1 JSON payload (idempotent upsert).
--
-- It never touches lubko.jobs itself, so the running worker is unaffected.
-- The cutover (final incremental sync plus the renames) happens separately in
-- 0003_cutover_two_column_protocol.sql, applied only after the legacy worker
-- is stopped.

create table if not exists lubko.jobs_v2 (
    id uuid primary key default gen_random_uuid(),
    payload text not null
        constraint jobs_v2_payload_is_json_object check (jsonb_typeof(payload::jsonb) = 'object')
        constraint jobs_v2_payload_has_version check ((payload::jsonb) ? 'v')
        constraint jobs_v2_payload_has_status check (((payload::jsonb)->'state'->>'status') is not null)
);

comment on table lubko.jobs_v2 is
    'Lubko transport queue (two-column protocol). INVARIANT: exactly two '
    'columns forever: id (unique random) and payload (one string containing '
    'a JSON object). All job/request/result/state/cancellation/'
    'process-identity data lives inside payload. Never add a third column.';

create index if not exists jobs_queue_idx
    on lubko.jobs_v2 (
        ((payload::jsonb)->'state'->>'status'),
        ((payload::jsonb)->'state'->>'created_at')
    );

do $$
begin
    if to_regclass('lubko.jobs') is not null
       and to_regclass('lubko.jobs_v2') is not null
       and (select count(*)
            from information_schema.columns
            where table_schema = 'lubko' and table_name = 'jobs') > 2 then
        execute $backfill$
            insert into lubko.jobs_v2 (id, payload)
            select
                id,
                jsonb_build_object(
                    'v', 1,
                    'type', 'command',
                    'request', jsonb_build_object(
                        'cwd', cwd,
                        'command', command
                    ),
                    'state', jsonb_build_object(
                        'status', status,
                        'created_at', to_jsonb(to_char(
                            created_at at time zone 'utc',
                            'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')),
                        'updated_at', to_jsonb(to_char(
                            updated_at at time zone 'utc',
                            'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')),
                        'started_at', to_jsonb(to_char(
                            started_at at time zone 'utc',
                            'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')),
                        'finished_at', to_jsonb(to_char(
                            finished_at at time zone 'utc',
                            'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')),
                        'worker_id', worker_id,
                        'process_pid', process_pid,
                        'process_pgid', process_pgid,
                        'cancel_requested_at', to_jsonb(to_char(
                            cancel_requested_at at time zone 'utc',
                            'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'))
                    ),
                    'result', jsonb_build_object(
                        'stdout', stdout,
                        'stderr', stderr,
                        'exit_code', exit_code,
                        'cancellation_note', cancellation_note
                    )
                )::text
            from lubko.jobs
            on conflict (id) do update set payload = excluded.payload
        $backfill$;
    end if;
end
$$;
