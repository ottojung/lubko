-- Lubko transport queue baseline.
--
-- PostgreSQL owns only the frozen transport metadata. The payload is opaque
-- text to PostgreSQL; all JSON shape, protocol version, routing, lifecycle,
-- and compatibility semantics are enforced by Lubko application code.

create schema if not exists lubko;

create table if not exists lubko.jobs (
    id uuid primary key default gen_random_uuid(),
    payload text not null
);

-- lubko_worker is the stable transport credential. This access arrangement is
-- frozen infrastructure rather than an application-protocol boundary.
do $$
begin
    if to_regrole('lubko_worker') is not null then
        execute 'grant usage on schema lubko to lubko_worker';
        execute 'grant select, insert, update, delete on table lubko.jobs to lubko_worker';
    end if;
end
$$;
