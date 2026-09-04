# Protocol upgrades with frozen PostgreSQL metadata

PostgreSQL stores `payload` as opaque text. Protocol changes never require database metadata changes.

## Current policy

This release implements exactly protocol v4. There is no speculative v5 and no configurable protocol-version window. Submitters stamp v4, workers claim and execute only v4, and application parsers reject every other version.

A worker may fail closed pending rows from versions lower than v4 because those generations are retired. Rows from versions higher than v4 stay pending: an old worker cannot prove that a newer worker in the fleet cannot serve them.

## Future protocol changes

Introduce a new top-level `v` only when there is a real application-protocol change. At that time, design the smallest compatibility/rollout mechanism required by that actual change. Do not pre-create placeholder versions or compatibility windows.

Compatible and breaking upgrades alike must preserve the frozen PostgreSQL contract: no payload CHECK constraints, expression indexes, protocol-specific roles, RLS, policies, functions, triggers, or other catalog changes.
