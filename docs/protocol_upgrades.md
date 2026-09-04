# Protocol upgrades with frozen PostgreSQL metadata

PostgreSQL is a durable transport store, not a protocol validator. Its catalog is
frozen: `lubko.jobs` has `id uuid primary key default gen_random_uuid()` and
`payload text not null`, and PostgreSQL treats `payload` as opaque text. Protocol
upgrades never add or change CHECK constraints, payload expression indexes,
roles, policies, functions, triggers, or other catalog objects.

## Application version window

The top-level payload `v` is interpreted only by Lubko application code. A worker
has a bounded supported execution window and:

- claims only command rows whose `v` is supported by that worker;
- parses claimed payloads fail-closed in application code;
- preserves the root version when producing output chunks;
- leaves payloads from a newer binary generation untouched rather than
  destructively misclassifying them;
- may retire older application versions only according to application rollout policy.

The database neither knows the supported window nor rejects a row because of its
version.

## Compatible upgrades

For mutually compatible generations, widen the application execution window,
roll out newer workers, let new submissions negotiate/use the newer version, then
raise the application execution floor after old work has drained. Stored terminal
history may retain older versions indefinitely because PostgreSQL does not inspect
it.

## Breaking upgrades

A breaking payload change is also an application concern. Quiesce or otherwise
coordinate producers and consumers so no worker executes a payload it cannot
parse. Use explicit application versioning and bounded compatibility windows where
possible. If queued old work cannot remain supported, drain or cancel it according
to product policy.

A breaking upgrade still must not require PostgreSQL DDL, catalog migration, role
changes, or credential changes.

## Permanent rule

New Lubko functionality must adapt to the frozen PostgreSQL transport contract.
Changing the database to accommodate a new payload format is not a supported
design option.
