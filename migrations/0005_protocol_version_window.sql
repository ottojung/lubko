-- Lubko non-destructive mixed-version protocol window: RETAINED-HISTORY range.
--
-- This migration generalizes the protocol-v4 payload-shape constraint so it no
-- longer hard-codes a single version. Instead of demanding `(payload::jsonb)->'v'
-- = '4'`, it admits every version inside a *retained-history* range
-- `[RETAINED_MIN, RETAINED_MAX]`.
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
--     raising the execution floor, never by altering the table or truncating.
--   * The daemon's EXECUTION window (which versions it will claim, parse, and
--     run) is a runtime property of each daemon -- `Settings.supported_protocol_range`
--     in lubko.protocol_versioning -- applied through the claim predicate and the
--     fail-closed reaper. It is NEVER the table constraint. The execution floor
--     can move forward (for example `[4,4]` -> `[5,5]`) while old terminal `v=4`
--     command rows and their `output_chunk` history stay stored and queryable.
--   * This migration's CHECK therefore validates `v` against the RETAINED-HISTORY
--     range, which is deliberately broader than any single daemon's execution
--     window. It covers every protocol version the fleet has ever written and
--     must keep: raising the execution floor never invalidates that history.
--
-- RETAINED-HISTORY RANGE vs EXECUTION WINDOW
--
--   * RETAINED_MIN / RETAINED_MAX are the bounds of versions the table may store.
--     RETAINED_MIN is the oldest version this build still treats as valid
--     retained history (v4 in the current generation; v1-v3 were never valid
--     post-cutover v4+ history and are rejected to keep the fail-closed DB
--     boundary tight). RETAINED_MAX is the highest version THIS BUILD of the code
--     can parse and store. Bump RETAINED_MAX only when a new compatible version
--     becomes writable (for example v4 -> v5, once the v5 parser and builder
--     exist in lubko.protocol_versioning.SUPPORTED_PROTOCOL_VERSIONS). The
--     execution window is configured separately, per daemon, at runtime.
--   * Because the retained range is the superset, a row at an older version such
--     as `v=4` remains valid even after every daemon's execution floor has risen
--     to `[5,5]`. The constraint rejects only malformed, fractional,
--     out-of-retained-range, or future/unrepresentable `v` values -- never
--     legitimate historical versions.
--
-- TOTAL, FAIL-CLOSED VERSION VALIDATION
--
-- The version check below admits a row only when its `v` is a JSON number that
-- is integral and lies inside the retained-history range. It is written so it
-- can NEVER return SQL NULL and NEVER raises on a malformed value, so a bad row
-- is always rejected rather than silently admitted (a NULL/TRUE in a CHECK would
-- pass, and an unguarded ::int cast would raise on an oversized value):
--
--   * a missing `v` key            (jsonb extraction yields SQL NULL),
--   * a JSON `null` `v`           (jsonb_typeof reports 'null', not 'number'),
--   * a non-number `v`            (string / boolean / object / array),
--   * a fractional `v`            (e.g. 4.9 would be rounded by a bare ::int),
--   * an out-of-retained-range `v` (below RETAINED_MIN or above RETAINED_MAX),
--   * an oversized `v`            (far larger than INT4; compared as numeric,
--                                  never cast to int, so it cannot raise or
--                                  slip through as a wrapped small integer).
-- The numeric comparison is performed with ::numeric (safe for every JSON
-- number) and the type test uses `is not distinct from 'number'` so missing
-- keys and JSON null are treated as decisively NOT a number.
--
-- COMMAND STATUS VALIDATION
--
-- A command row is lifecycle state, so its status discriminator must also be
-- total and fail-closed at the database boundary.  `->>` alone is not a type
-- check: PostgreSQL stringifies booleans, numbers, arrays, and objects.  The
-- command status gate therefore first requires a JSON string and only then
-- admits one of the five protocol lifecycle states.  This prevents malformed
-- direct writes from becoming permanently stranded rows that are neither
-- claimable (`pending`), recoverable (`running`), nor terminal-GC eligible.
-- As with protocol versions, migration preflight refuses to tighten the
-- constraint while malformed historical command status exists; it never
-- rewrites or deletes such authority.

do $$
declare
    -- Retained-history range: every version the table may store as immutable
    -- terminal history. This is broader than any daemon's execution window, so
    -- advancing the execution floor never rejects old command/output_chunk rows.
    -- RETAINED_MIN is the oldest version this build still treats as valid
    -- retained history (v4 in the current generation): v1-v3 were never valid
    -- post-cutover v4+ history, and admitting shape-compatible v1-v3 direct
    -- writes would weaken the fail-closed DB boundary, so they are rejected.
    -- RETAINED_MAX is the highest version this build can parse and store; widen
    -- it only when a new compatible version becomes writable.
    retained_min integer := 4;
    retained_max integer := 5;
    nonconforming bigint;
    constraint_text text;
begin
    if retained_max < retained_min then
        raise exception using
            message = format(
                'retained protocol range is invalid: min %s exceeds max %s',
                retained_min, retained_max
            );
    end if;

    -- Preflight: refuse to apply if any command/output_chunk row already sits
    -- outside the retained-history range (malformed, fractional, future, or
    -- unrepresentable). Raising here leaves the original constraint and table
    -- completely intact; the cutover is non-destructive and fail-closed. Valid
    -- historical rows (for example a `v=4` row once the execution floor is `5`)
    -- are inside the retained range and are NOT rejected.
    --
    -- The version test is a single boolean expression that can never be NULL and
    -- never casts a non-number: the numeric comparisons live inside a CASE that
    -- only runs them when the value is provably a JSON number, so a
    -- string/object/null/boolean `v` short-circuits to `false` without ever
    -- touching ::numeric. A missing key yields SQL NULL from jsonb_typeof, which
    -- is `not distinct from 'number'` => false; a JSON null yields 'null', also
    -- false.
    select count(*) into nonconforming
    from lubko.jobs
    where not (
        case
            when jsonb_typeof((payload::jsonb)->'type')
                 is not distinct from 'string'
                 and (payload::jsonb)->>'type' in ('command', 'output_chunk')
            then (
                case
                    when jsonb_typeof((payload::jsonb)->'v')
                     is not distinct from 'number'
                then ((payload::jsonb)->'v')::numeric
                         = floor(((payload::jsonb)->'v')::numeric)
                     and ((payload::jsonb)->'v')::numeric
                         between retained_min and retained_max
                    else false
                end
                and (
                    case
                        when (payload::jsonb)->>'type' = 'command' then
                        case
                            when jsonb_typeof(
                                (payload::jsonb)->'state'->'status'
                            ) is not distinct from 'string'
                            then (payload::jsonb)->'state'->>'status'
                                in ('pending', 'running', 'succeeded', 'failed', 'cancelled')
                            else false
                        end
                        else true
                    end
                )
            )
            else false
        end
    );

    if nonconforming > 0 then
        raise exception using
            message = format(
                'lubko.jobs still holds %s command/output_chunk payload(s) whose '
                'protocol version is outside the retained range [%s, %s] or whose '
                'command status is malformed/unsupported. Fix the malformed/future '
                'row or widen RETAINED_MAX when appropriate before applying; the '
                'constraint is unchanged.',
                nonconforming, retained_min, retained_max
            );
    end if;

    constraint_text := format(
        'case
            when jsonb_typeof((payload::jsonb)->''type'')
                 is not distinct from ''string''
                 and (payload::jsonb)->>''type'' = ''command'' then
                (case
                    when jsonb_typeof((payload::jsonb)->''v'')
                         is not distinct from ''number''
                    then ((payload::jsonb)->''v'')::numeric
                             = floor(((payload::jsonb)->''v'')::numeric)
                         and ((payload::jsonb)->''v'')::numeric between %L and %L
                    else false
                 end)
                and coalesce(jsonb_typeof((payload::jsonb)->''request''), '''')
                    = ''object''
                and (case
                    when jsonb_typeof((payload::jsonb)->''state''->''status'')
                         is not distinct from ''string''
                    then ((payload::jsonb)->''state''->>''status'')
                         in (''pending'', ''running'', ''succeeded'', ''failed'', ''cancelled'')
                    else false
                 end)
                and coalesce(jsonb_typeof((payload::jsonb)->''server''), '''')
                    = ''string''
                and coalesce((payload::jsonb)->>''server'', '''') <> ''''
            when jsonb_typeof((payload::jsonb)->''type'')
                 is not distinct from ''string''
                 and (payload::jsonb)->>''type'' = ''output_chunk'' then
                (case
                    when jsonb_typeof((payload::jsonb)->''v'')
                         is not distinct from ''number''
                    then ((payload::jsonb)->''v'')::numeric
                             = floor(((payload::jsonb)->''v'')::numeric)
                         and ((payload::jsonb)->''v'')::numeric between %L and %L
                    else false
                 end)
                and coalesce(jsonb_typeof((payload::jsonb)->''value''), '''')
                    = ''string''
                and (((payload::jsonb)->>''thread'') is not null)
                and coalesce(jsonb_typeof((payload::jsonb)->''server''), '''')
                    = ''string''
                and coalesce((payload::jsonb)->>''server'', '''') <> ''''
                and (((payload::jsonb)->>''stream'') in (''stdout'', ''stderr''))
                and (((payload::jsonb)->>''sequence'') ~ ''^[0-9]+$'')
                and (((payload::jsonb)->>''start'') ~ ''^[0-9]+$'')
                and (((payload::jsonb)->>''end'') ~ ''^[0-9]+$'')
            else false
        end',
        retained_min, retained_max, retained_min, retained_max
    );

    alter table lubko.jobs drop constraint if exists jobs_payload_type_shape;
    execute 'alter table lubko.jobs add constraint jobs_payload_type_shape '
        || 'check (' || constraint_text || ')';
end
$$;
