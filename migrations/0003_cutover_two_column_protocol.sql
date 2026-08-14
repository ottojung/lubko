-- Two-column Lubko transport protocol: cutover.
--
-- THE INVARIANT
--
-- The Lubko transport table lubko.jobs has exactly two columns forever:
--   (1) id      uuid primary key  -- unique random identifier
--   (2) payload text not null     -- one string containing a JSON object
-- All evolving job/request/result/state/cancellation/process-identity data
-- lives inside that JSON payload. Never add a third column.
--
-- WARNING: this migration retires the legacy multi-column schema. It must be
-- applied only AFTER the legacy worker has been stopped (see the ordered
-- cutover plan in docs/protocol.md), otherwise the running worker breaks.
--
-- It performs, in order:
--
--   1. a final incremental backfill of any legacy rows that changed or were
--      inserted since 0002 was applied (idempotent upsert, guarded to run
--      only while lubko.jobs still has the legacy columns);
--   2. renames lubko.jobs to lubko.jobs_legacy (kept for rollback), guarded
--      so it never clobbers an existing lubko.jobs_legacy;
--   3. renames lubko.jobs_v2 to lubko.jobs, making the two-column table the
--      canonical transport table.
--
-- Every step is guarded and the whole file is safe to apply more than once:
-- once lubko.jobs already has the two-column shape, all steps are skipped.

do $$
begin
    -- 1. Final incremental backfill while lubko.jobs is still the legacy schema.
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

    -- 2. Retire the legacy table only when it still has the legacy columns
    --    and no lubko.jobs_legacy exists yet.
    if to_regclass('lubko.jobs') is not null
       and to_regclass('lubko.jobs_legacy') is null
       and (select count(*)
            from information_schema.columns
            where table_schema = 'lubko' and table_name = 'jobs') > 2 then
        execute 'alter table lubko.jobs rename to lubko.jobs_legacy';
    end if;

    -- 3. Promote the two-column table to the canonical name.
    if to_regclass('lubko.jobs_v2') is not null
       and to_regclass('lubko.jobs') is null then
        execute 'alter table lubko.jobs_v2 rename to lubko.jobs';
    end if;
end
$$;
