-- Grant DELETE to lubko_worker for automatic transport garbage collection.
--
-- The worker role needs DELETE on lubko.jobs to execute collect_transport,
-- which removes terminal roots and their owned output chunks.  The baseline
-- migration 0001 already includes this grant for fresh installs; this
-- incremental migration adds it for existing installations that applied
-- 0001 before the GC feature was introduced.
--
-- This statement is idempotent: GRANT is safe to re-apply.

do $$
begin
    if to_regrole('lubko_worker') is not null then
        execute 'grant delete on table lubko.jobs to lubko_worker';
    end if;
end
$$;
