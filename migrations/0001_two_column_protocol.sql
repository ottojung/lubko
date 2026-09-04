-- Frozen Lubko PostgreSQL transport contract.
--
-- PostgreSQL is deliberately unaware of the application payload format. The
-- catalog contract is fixed: one schema, one two-column queue table, the UUID
-- primary-key/default, payload NOT NULL, and the already-established worker
-- access grants. Protocol evolution happens only in application code and in the
-- opaque payload text; normal Lubko upgrades must not alter PostgreSQL metadata.

create schema if not exists lubko;

create table if not exists lubko.jobs (
    id uuid primary key default gen_random_uuid(),
    payload text not null
);

-- `lubko_worker` is existing frozen access infrastructure. This baseline does
-- not create users or credentials. When the role already exists, make the
-- stable table privileges idempotently explicit.
do $$
begin
    if to_regrole('lubko_worker') is not null then
        execute 'grant usage on schema lubko to lubko_worker';
        execute 'grant select, insert, update, delete on table lubko.jobs to lubko_worker';
    end if;
end
$$;
