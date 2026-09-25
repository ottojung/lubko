# Lubko scheduled-work itinerary

This itinerary is the entry point for scheduled ChatGPT tasks that maintain the Lubko repository.

Before acting, study and obey:

- `AGENTS.md`;
- `docs/GOVERNANCE.md`;
- `docs/SKILL.md`;
- `docs/TESTING.md`;
- `docs/TOOLCHAIN.md`;
- the current intent records under `docs/intent-records/`;
- the protocol, lifecycle, startup, and deployment documents relevant to the selected work.

## Work selection

Recover useful abandoned work before inventing duplicate work. Prefer, in order:

1. an existing open pull request that can be advanced or repaired;
2. an actionable open issue whose dependencies are satisfied and which is not actively owned;
3. repository-wide audit or cleanup work only when it addresses a concrete current risk or inconsistency.

Do not let one blocked issue terminate the recurring workflow. Record durable recovery state and continue with another useful item.

## Integration

Scheduled Lubko development integrates through the current active `release/*` branch according to `docs/GOVERNANCE.md`.

If no active release branch exists, create one from current `main`. After bootstrap, advance it through pull requests and canonical verification. Do not bypass the repository's PR-and-CI workflow merely because GitHub permissions would permit a direct write.

Do not deploy or publish Lubko as part of ordinary scheduled development. Deployment remains a separate explicit action rather than an implied completion step.

## Verification and completion

Use the validation requirements from `AGENTS.md`, `docs/GOVERNANCE.md`, `docs/TESTING.md`, and `docs/TOOLCHAIN.md`.

In particular, preserve the single canonical `uv run pytest` suite and its under-ten-second requirement, together with formatting, lint, strict typing, and frozen dependency validation.

A scheduled work item is complete when:

- its requested repository result is implemented;
- required review has been completed;
- the exact proposed integration has the required canonical verification;
- no unresolved correctness or review blocker remains;
- the work is integrated into the active release branch, or deliberately left in a clearly recoverable reviewed PR state when there is a concrete reason not to merge yet.

After completion, continue future scheduled invocations with the next useful Lubko work item.
