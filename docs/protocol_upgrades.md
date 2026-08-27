# Lubko protocol upgrades: the bounded mixed-version model

Status: authoritative for non-destructive protocol upgrades (compatible versions).
Companion to `docs/protocol.md` (the v4 payload binding) and
`docs/issue21-deploy-protocol.md` (deploy-time readiness).

## Problem

The v3 → v4 cutover described in `docs/protocol.md` is **destructive**: it
`truncate`s `lubko.jobs`, discarding every in-flight root command and its entire
immutable `output_chunk` history, because v4 parsers reject every v3 row. That
was acceptable exactly once, for a single breaking change (the introduction of
the required top-level `server` routing identity, which v3 rows cannot carry).

A repeated destructive cutover is unacceptable for a running fleet: it throws
away in-flight jobs and output history, and it forbids staggered (one-server-at-
a-time) rollout because there is no moment where old and new daemons can share a
queue. The goal of issue #286 is a **clean, reusable, non-destructive** upgrade
mechanism instead of a second one-off v4 hack.

## Principles

1. **The two-column table is immutable across versions.** `id` and `payload`
   forever; protocol evolution lives inside `payload` via the top-level integer
   `v`. No new column, no staging table, no `truncate`.
2. **A version window is bounded.** A daemon advertises an inclusive
   `[min, max]` window of *mutually compatible* versions. The window width is
   capped (`MAX_VERSION_SPAN`, currently 1) so the compatibility surface —
   parsers, builders, and the SQL shape constraint — stays finite and reviewable.
   There is deliberately no unbounded backwards-compatibility ladder.
3. **Versions inside one window are mutually compatible.** They share the same
   two payload kinds (`command`, `output_chunk`) and the same required fields;
   evolution between them is strictly additive (new optional fields may appear).
   A *breaking* change is not admitted inside a window.
4. **Upgrades are non-destructive and preserve history.** Because the physical
   schema is unchanged and versions are compatible, an in-flight job submitted
   at the old version keeps running and every `output_chunk` it has published
   stays exactly where it is. Nothing is truncated and nothing is migrated.
5. **A daemon fails closed on any version it cannot understand.** It claims and
   executes only jobs whose `v` lies inside its own window. A job outside the
   window is never run, never silently ignored, and (under the reaper below) is
   failed with a diagnostic rather than stranded.
6. **Fresh install stays simple.** A brand-new database applies the single
   baseline `0001_two_column_protocol.sql` and runs at exactly the current
   version. No window arithmetic, no extra migration, no default server. The
   window machinery exists only for *upgrading an existing fleet*.

## The mechanism

All of the reusable logic lives in `src/lubko/protocol_versioning.py`.

- `ProtocolVersionRange(min, max)` — the bounded window. Construction validates
  `min >= 1`, `max >= min`, and `max - min <= MAX_VERSION_SPAN`, failing closed
  otherwise. This is the single source of truth for "what versions can this
  daemon touch".
- `negotiate_version(client_min, client_max, server_range)` — a submitter
  proposes the range it can speak; the target server advertises its window; the
  function returns the highest version in both. New submissions therefore
  converge onto the newest version the whole fleet understands, while older
  in-flight jobs (submitted at the old version) remain claimable by daemons that
  still advertise the old version.
- `unsupported_version_diagnostic(version, supported)` /
  `classify_job_version(version, supported)` — the fail-closed primitives. A
  version below `min` names a retired generation; a version above `max` names a
  generation this daemon cannot parse. Both yield a clear diagnostic and a
  `FAIL_CLOSED` disposition.
- `claim_version_predicate(supported)` — emits the SQL `AND` clause and bound
  parameters that gate the claim query to `v BETWEEN min AND max`, so a daemon
  never locks a row it cannot parse or execute.

`src/lubko/protocol.py` consumes the window through `parse_payload` /
`parse_chunk_payload`, which now accept a `supported` window (defaulting to the
current single-version window) and reject any `v` outside it via
`unsupported_version_diagnostic`. The builders `build_payload` /
`build_output_chunk_payload` take the negotiated `version` and stamp it on the
payload. Because window versions are mutually compatible, the *same* parser
handles every version in the window; a future breaking generation would register
its own parser and raise `min` only after the prior version has drained.

The SQL side is generalized by `migrations/0005_protocol_version_window.sql`,
which replaces the hard-coded `(payload::jsonb)->'v' = '4'` check with a
**retained-history** range check: `v` must be a well-formed integer in
`[RETAINED_MIN, RETAINED_MAX]`. This range is deliberately broader than any
single daemon's execution window — it covers every protocol version the fleet
has ever written and must keep queryable as immutable terminal history, so
raising the execution floor never invalidates old `command` rows or
`output_chunk` history. The daemon's *execution* window (which versions it will
claim, parse, and run) is a runtime property of each daemon
(`Settings.supported_protocol_range` in `lubko.protocol_versioning`), applied
through the claim predicate and the fail-closed reaper; it is never the table
constraint. The two bounds are plain constants at the top of the migration:
`RETAINED_MIN` is the oldest version this build still treats as valid retained
history (v4 in the current generation; v1-v3 were never valid post-cutover v4+
history and are rejected to keep the fail-closed DB boundary tight), and
`RETAINED_MAX` is the highest version this build of the code can parse and store.
Bump `RETAINED_MAX` when a new compatible version becomes writable and re-apply
the idempotent migration. A preflight refuses to apply against any row whose `v`
is malformed, fractional, out of the retained range, or a future/unrepresentable
value, so a failed cutover leaves the table and its constraint completely intact
— there is no half-upgraded state.

## Deterministic staggered server upgrade procedure

The window makes a one-server-at-a-time rollout safe and deterministic:

1. **Quiesce nothing.** New submissions continue; the submitter negotiates the
   highest version common to it and each target server's window. While every
   server still advertises only `[C, C]`, new jobs are submitted at `C`.
2. **Widen the DB retained/admission max first (non-destructive).** While every
   daemon still advertises only `[C, C]`, apply `0005` with `RETAINED_MAX = C+1`
   so the table's stored-history (admission) range already accepts both `C` and
   `C+1` writes. No daemon writes `C+1` yet, because none advertises it, so
   in-flight `C` jobs keep running untouched. **This must precede any daemon
   advertising `C+1`:** the DB admission max is the ceiling the daemon execution
   max may not exceed, so widening it first guarantees a daemon never advertises
   an execution window whose max the DB would reject.
3. **Widen the daemon execution window.** Only now roll out daemons that
   advertise `[C, C+1]` (with the `C+1` parser in place). Because the DB
   admission max already admits `C+1`, the execution max a daemon advertises can
   never exceed the DB-admitted max. `C` jobs keep running on daemons that still
   advertise `[C, C]`.
4. **Converge new work.** Submitters now negotiate `C+1` against the widened
   daemon windows, so every *new* job is stamped `v = C+1`, while old `C` jobs
   drain naturally as they finish.
5. **Drain the old version and raise the execution floor (non-destructive).**
   Once the queue contains no *pending* `command` rows at `C` (they are all
   terminal and collected, or have finished), roll out daemons that advertise
   `[C+1, C+1]`. The execution floor moves at the daemon level only; the table
   constraint's retained range already spans `C` through `C+1`, so the terminal
   `C` rows and their `output_chunk` history remain valid and queryable. No
   migration re-application is required to raise the floor, and old history is
   never rejected.
6. **Breaking change?** A breaking generation `C+2` is handled the same way, but
   `C+1` and `C+2` are *not* mutually compatible, so they are never in the same
   window; you drain `C+1` completely (step 5) before opening `[C+2, C+2]`.

Determinism comes from the explicit integer `v` on every payload and the strict
claim gate: at every moment, the set of claimable versions for a given daemon is
exactly its window, so rollout order cannot produce a job claimed by a daemon
that cannot execute it.

## Fail-closed on unsupported versions

- **Per daemon (always on):** the claim predicate excludes every `v` outside the
  daemon's window, and `parse_payload`/`parse_chunk_payload` reject any such `v`
  with a diagnostic. A daemon therefore never executes, mutates, or collects a
  payload it does not understand.
- **Fleet-wide reaper (recommended):** a periodic pass scans `pending`
  `command` rows and, for any whose `v` is `classify_job_version(...) ==
  FAIL_CLOSED` against the *deployment's* supported window, fails the job closed
  with the diagnostic from `unsupported_version_diagnostic` instead of letting it
  sit pending forever. This is the safety net for a job submitted at a version no
  running daemon accepts (for example a `C+1` job that arrives after every daemon
  has already moved to `[C+2, C+2]`). It is fail-closed, not fail-open: such a
  job is rejected loudly, never executed by a daemon that would misinterpret it.

## What this replaces

The destructive v3 → v4 cutover remains documented in `docs/protocol.md` as a
one-time legacy event (v3 rows carried no `server` identity and cannot be made
compatible). Every *future* upgrade — compatible or breaking — uses the bounded
mixed-version window instead of another `truncate`. The mechanism is version-
agnostic: it never names v4 specifically, so it applies equally to v5, v6, and
beyond.
