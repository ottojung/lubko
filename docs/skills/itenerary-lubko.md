# Lubko scheduled-work itinerary

## Scope

This is the **sole entry point** for scheduled ChatGPT tasks that maintain the Lubko repository itself.

Target repository:

<https://github.com/ottojung/lubko>

Before acting, study and obey:

- <https://github.com/ottojung/lubko/blob/main/docs/SKILL.md>
- <https://github.com/ottojung/lubko/blob/main/docs/skills/scheduled.md>

`docs/skills/scheduled.md` owns the reusable scheduled-orchestrator mechanics. This itinerary contains only Lubko-specific work-selection, integration, and completion policy; do not restate the shared mechanics here.

## Work selection

Apply `docs/skills/scheduled.md`, with these Lubko-specific choices:

- Prefer inheriting abandoned issue-tracked work over selecting a new issue.
- If there is no abandoned work to inherit, choose an actionable open Lubko issue that is neither actively owned nor already completed under the shared scheduled-work protocol.
- Once selected, drive the issue to the Lubko-specific completion condition below.

## Release integration

Scheduled Lubko work accumulates in one current active `release/*` branch. A human promotes that release branch into `main`.

- The active release branch is the latest `release/*` branch that has **never** been promoted into `main`.
- A release branch is permanently retired after its first promotion into `main`, even if commits are accidentally added to it later.
- If no active release branch exists, create one from current `main`.
- Reuse the same active release branch across scheduled issues; do not create one release branch per issue.
- Before starting issue work, merge current `main` into the active release branch via a pull request. The `release1` ruleset is the required enforcement target for this rule; operators must verify it is live.
- Start each issue branch from the active release branch in an isolated worktree.
- Open the issue PR against the active release branch, not `main`.
- After required implementation, verification, and orchestrator review, merge the issue PR into the active release branch.
- After that merge, merge the latest `main` into the active release branch via a pull request and verify the exact resulting release head.
- If work is accidentally added to a retired release branch, preserve any unique work by moving it onto the active release branch, then stop using the retired branch.
- There must be at most one open release-promotion PR targeting `main`, and it must come from the current active release branch. Close stale or redundant promotion PRs after verifying that the active release contains any needed work.

Scheduled orchestrators must not merge issue/task PRs into `main` and must not merge `release/*` into `main`. Promotion into `main` is the human review boundary.

## Completion

A scheduled Lubko issue is complete when:

- its reviewed work is merged into the current active release branch;
- the release branch is reconciled with current `main`;
- the repository-required verification passes on the exact resulting release head;
- no unresolved review blocker remains.

After those conditions hold, complete the shared orchestrator workflow according to `docs/skills/scheduled.md`.
