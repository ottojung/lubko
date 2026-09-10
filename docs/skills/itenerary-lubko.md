# Lubko scheduled-work itinerary

## Scope

This is the **sole entry point** for scheduled ChatGPT tasks that maintain the Lubko repository itself.

Target repository:

<https://github.com/ottojung/lubko>

Before acting, study and obey:

- <https://github.com/ottojung/lubko/blob/main/docs/SKILL.md>
- <https://github.com/ottojung/lubko/blob/main/docs/skills/scheduled.md>

`docs/skills/scheduled.md` owns the reusable scheduled-orchestrator mechanics. This itinerary contains only Lubko-specific work-selection, integration, and completion policy; do not restate the shared mechanics here.

**Never stop or disable this scheduled task.** Treat errors and blockers in an individual work item as recoverable. Preserve useful state, recover or choose other actionable work as appropriate, and continue producing useful progress rather than terminating the recurring orchestrator.

Repository-hosting configuration is outside development scope. Do not create, modify, require, audit, or treat as acceptance criteria GitHub rulesets, branch protection, required-status-check settings, merge-policy settings, repository permissions, GitHub Actions repository settings, or other out-of-band Git/GitHub configuration. See `docs/intent-records/repository-configuration-out-of-scope.md`.

## Work selection

Apply `docs/skills/scheduled.md`, with these Lubko-specific choices:

- Prefer inheriting abandoned issue-tracked work over selecting a new issue.
- If there is no abandoned work to inherit, choose an actionable open Lubko issue that is neither actively owned nor already completed under the shared scheduled-work protocol.
- Once selected, drive the issue to the Lubko-specific completion condition below.
- If the selected issue has a genuine external blocker, record enough durable state for later recovery and select other actionable Lubko work. A blocked issue is not a reason to stop or disable the scheduled task.
