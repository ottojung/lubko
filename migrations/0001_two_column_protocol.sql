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

-- One-time convergence for installations created before the PostgreSQL
-- transport contract was frozen. These exact objects were created by older
-- Lubko migrations and made PostgreSQL interpret application payload fields.
-- Dropping only Lubko's known legacy objects is idempotent and deliberately
-- avoids touching operator-owned catalog metadata. Once absent, future normal
-- Lubko upgrades have no protocol-aware PostgreSQL metadata to evolve.
alter table lubko.jobs drop constraint if exists jobs_payload_has_version;
alter table lubko.jobs drop constraint if exists jobs_payload_is_json_object;
alter table lubko.jobs drop constraint if exists jobs_payload_type_shape;

drop index if exists lubko.jobs_queue_idx;
drop index if exists lubko.jobs_chunk_owner_idx;
drop index if exists lubko.jobs_chunk_order_idx;

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
