-- Lubko non-destructive mixed-version protocol window.
--
-- This migration generalizes the protocol-v4 payload-shape constraint so it no
-- longer hard-codes a single version. Instead of demanding `(payload::jsonb)->'v'
-- = '4'`, it admits every version inside a bounded, mutually compatible window
-- `[MIN_PROTOCOL_VERSION, MAX_PROTOCOL_VERSION]`.
--
-- THE MODEL (see docs/protocol_upgrades.md)
--
-- The two-column transport table is immutable across versions; protocol
-- evolution lives inside the payload's top-level integer `v`. A *bounded mixed-
-- version window* lets a fleet run more than one compatible version at once
-- during a staggered upgrade, while the physical schema never changes and no
-- data is destroyed:
--
--   * Every version inside one window is mutually compatible: the two payload
--     kinds (`command`, `output_chunk`) and all required fields are identical,
--     and evolution between window versions is strictly additive. A breaking
--     change is handled by draining the old version out of the window before
--     raising MIN_PROTOCOL_VERSION, never by altering the table or truncating.
--   * The window is bounded (see MAX_VERSION_SPAN in lubko.protocol_versioning);
--     this migration enforces the same bound at the schema level so a row can
--     never enter the table at a version the fleet cannot converge on.
--   * A daemon claims and executes only jobs whose `v` lies inside its own
--     configured window (lubko.worker claim predicate) and fails closed on any
--     version outside it.
--
-- UPGRADING TO A NEW COMPATIBLE VERSION
--
-- To widen the window for the next generation (for example v4 -> v5), edit the
-- two constants below (MAX_PROTOCOL_VERSION = 5, once the v5 parser and builder
-- exist in lubko.protocol_versioning.SUPPORTED_PROTOCOL_VERSIONS) and re-apply
-- this idempotent migration. The preflight refuses to apply against any row
-- whose version is already outside the new window, so the table is left
-- completely intact on a failed cutover -- there is no half-upgraded state.
--
-- For a breaking change, keep MIN_PROTOCOL_VERSION = MAX_PROTOCOL_VERSION =
-- the new generation after the old version has fully drained; the same
-- constraint refuses legacy rows.

do $$
declare
    -- The bounded, mutually compatible version window for this deployment.
    min_version integer := 4;
    max_version integer := 4;
    nonconforming bigint;
    constraint_text text;
begin
    if max_version < min_version then
        raise exception using
            message = format(
                'protocol window is invalid: min %s exceeds max %s',
                min_version, max_version
            );
    end if;

    -- Preflight: refuse to apply if any command/output_chunk row already sits
    -- outside the new window. Raising here leaves the original constraint and
    -- table completely intact; the cutover is non-destructive and fail-closed.
    select count(*) into nonconforming
    from lubko.jobs
    where (payload::jsonb)->>'type' in ('command', 'output_chunk')
        and (
            jsonb_typeof((payload::jsonb)->'v') <> 'number'
            or ((payload::jsonb)->'v')::int not between min_version and max_version
        );

    if nonconforming > 0 then
        raise exception using
            message = format(
                'lubko.jobs still holds %s command/output_chunk payload(s) whose '
                'protocol version is outside the target window [%s, %s]. Widen '
                'the window or drain those jobs before applying; the constraint '
                'is unchanged.',
                nonconforming, min_version, max_version
            );
    end if;

    constraint_text := format(
        'case
            when (payload::jsonb)->>''type'' = ''command'' then
                jsonb_typeof((payload::jsonb)->''v'') = ''number''
                and ((payload::jsonb)->''v'')::int between %L and %L
                and jsonb_typeof((payload::jsonb)->''request'') = ''object''
                and (((payload::jsonb)->''state''->>''status'') is not null)
                and coalesce(jsonb_typeof((payload::jsonb)->''server''), '''') = ''string''
                and coalesce((payload::jsonb)->>''server'', '''') <> ''''
            when (payload::jsonb)->>''type'' = ''output_chunk'' then
                jsonb_typeof((payload::jsonb)->''v'') = ''number''
                and ((payload::jsonb)->''v'')::int between %L and %L
                and jsonb_typeof((payload::jsonb)->''value'') = ''string''
                and (((payload::jsonb)->>''thread'') is not null)
                and coalesce(jsonb_typeof((payload::jsonb)->''server''), '''') = ''string''
                and coalesce((payload::jsonb)->>''server'', '''') <> ''''
                and (((payload::jsonb)->>''stream'') in (''stdout'', ''stderr''))
                and (((payload::jsonb)->>''sequence'') ~ ''^[0-9]+$'')
                and (((payload::jsonb)->>''start'') ~ ''^[0-9]+$'')
                and (((payload::jsonb)->>''end'') ~ ''^[0-9]+$'')
            else true
        end',
        min_version, max_version, min_version, max_version
    );

    alter table lubko.jobs drop constraint if exists jobs_payload_type_shape;
    execute 'alter table lubko.jobs add constraint jobs_payload_type_shape '
        || 'check (' || constraint_text || ')';
end
$$;
