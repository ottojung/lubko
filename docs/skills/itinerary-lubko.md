# Lubko scheduled-work itinerary

This itinerary is the Lubko-specific execution, integration, and completion policy for work selected through the Antonina board.

Before acting, study and obey:

- Antonina's canonical board-orchestrator skill: <https://github.com/ottojung/antonina/blob/main/docs/skills/orchestrator.md>;
- `AGENTS.md`;
- `docs/GOVERNANCE.md`;
- `docs/SKILL.md`;
- `docs/TESTING.md`;
- `docs/TOOLCHAIN.md`;
- the current intent records under `docs/intent-records/`;
- the protocol, lifecycle, startup, and deployment documents relevant to the selected work.

The Antonina board is the coordination authority. Queue selection, claims, recovery, progress, blockers, and handoff are governed by the canonical orchestrator skill. GitHub issues and pull requests are execution/specification artifacts, not a second scheduler.

## Work selection

Use the Antonina board selection algorithm. For Lubko work, prefer recoverable ongoing work before starting duplicate work; otherwise the highest-priority actionable Lubko board issue wins.

An existing open pull request may be the objective durable state of the selected board issue and should be advanced when useful. Do not create repository-wide audit or cleanup work merely to keep the scheduler busy; such work needs a concrete board issue or a concrete current risk worth recording as one.

Do not let one blocked issue terminate recurring orchestration. Append the blocker to the board and continue with another actionable issue.

## Integration

Scheduled Lubko development integrates through the current active `release/*` branch according to `docs/GOVERNANCE.md`.

If no active release branch exists, create one from current `main`. After bootstrap, advance it through pull requests and canonical verification. Do not bypass the repository's PR-and-CI workflow merely because GitHub permissions would permit a direct write.

Do not deploy or publish Lubko as part of ordinary scheduled development. Deployment remains a separate explicit action rather than an implied completion step.

## Verification and completion

Use the validation requirements from `AGENTS.md`, `docs/GOVERNANCE.md`, `docs/TESTING.md`, and `docs/TOOLCHAIN.md`.

In particular, preserve the single canonical `uv run pytest` suite and its under-ten-second requirement, together with formatting, lint, strict typing, and frozen dependency validation.

A Lubko board issue is complete when:

- its requested repository result is implemented;
- required review has been completed;
- the exact proposed integration has the required canonical verification;
- no unresolved correctness or review blocker remains;
- the work is integrated into the active release branch, or deliberately left in a clearly recoverable reviewed PR state when there is a concrete reason not to merge yet.

When the completion predicate is actually satisfied, append the completed board comment, close the Antonina board issue, and verify that it has left the queue.
