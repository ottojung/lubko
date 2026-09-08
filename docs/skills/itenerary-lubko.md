# Lubko scheduled-work itinerary

## What this document is

This is the **sole entry point** for scheduled ChatGPT tasks that maintain the Lubko repository itself.

The target repository is:

<https://github.com/ottojung/lubko>

A scheduled task should point only to this document. This document supplies the Lubko-specific work-selection and release-flow policy, then delegates shared scheduled-orchestrator mechanics and Lubko operation to the canonical reusable documents.

## Start every scheduled run here

At the start of every run:

1. Study <https://github.com/ottojung/lubko/blob/main/docs/SKILL.md> and obey it for normal Lubko operation.
2. Study <https://github.com/ottojung/lubko/blob/main/docs/skills/scheduled.md> and obey its scheduled-run, ownership, abandonment, recovery, and liveness rules.
3. Treat <https://github.com/ottojung/lubko> as the target repository.
4. Inspect the target repository's own operating instructions and current GitHub state before acting.
5. Determine the concrete work to do according to the policy below.
6. Continue until that work reaches the completion condition in this document; do not stop after merely inspecting or reporting what could be done.

## Choosing which work to do

Inspect the Lubko repository's open GitHub issues and their canonical orchestrator status comments before choosing work.

Apply the ownership rules from `docs/skills/scheduled.md`, with this Lubko-specific selection policy:

- A `working` status whose canonical status comment was updated less than 10 minutes ago is actively owned by another orchestrator. Do not intentionally work on that issue.
- A `working` status whose canonical status comment was updated at least 10 minutes ago is abandoned and inheritable.
- **Prefer inheriting abandoned work over selecting a new issue.**
- A `completed` orchestrator status is finished scheduled work. Do not select it as new work merely because the GitHub issue remains open.
- If there is no abandoned work to inherit, choose an actionable open Lubko issue that has neither an active `working` status nor a `completed` orchestrator status.

Immediately after choosing an issue, claim or inherit it through the canonical orchestrator status comment exactly as `docs/skills/scheduled.md` requires, then re-read the comment to confirm ownership before starting substantial work.

When inheriting, reconstruct the existing work from objective state: the status comment's recorded resources, Lubko jobs and agents, worktrees, branches, PRs, issue discussion, CI, and any other durable state.

Once an issue is selected, **drive it until completion**. Do not substitute a status report, partial implementation, or recommendation for completion when the remaining work is actionable.

## Lubko release-branch workflow

Scheduled/unattended Lubko work must not be merged directly into `main`. Completed scheduled work accumulates in the latest active unmerged `release/*` branch. A human periodically reviews that release branch and promotes it into `main`.

### Release branch lifecycle

- Find the latest active unmerged `release/*` branch.
- If none exists, create one from the current `main`.
- Do not create one release branch per issue.
- Reuse the latest active release branch across scheduled runs and across multiple completed issues.
- Once that release branch has been merged into `main`, stop using it and create a fresh release branch from the new `main` head.
- A date/timestamp-based name such as `release/2026-08-15` is acceptable.

### Keep the release branch reconciled with `main`

Whenever a scheduled invocation begins operating on the current release branch, first update `main` and merge it into the release branch:

```sh
git checkout <release-branch>
git merge origin/main
```

Resolve conflicts and verify the release branch before starting new work.

After merging a completed task PR into the release branch, merge the latest `main` into the release branch again and run the required verification.

This does not guarantee that a future `main` commit cannot conflict, but it ensures the unattended release branch is reconciled with the latest known `main` whenever the scheduled orchestrator operates.

### Issue branches and PRs

For each Lubko issue:

- start from the current `release/*` branch;
- create a normal issue/task branch in an isolated worktree as required by `docs/SKILL.md`;
- implement, test, and independently review the work;
- push the task branch normally;
- open the task PR against the **current release branch**, not `main`;
- merge the completed/reviewed task PR into the release branch;
- use the updated release branch as the base for subsequent Lubko work.

Hard rules:

```text
Scheduled orchestrator MAY:
    create release/* from main
    merge main -> release/*
    create task branches from release/*
    merge reviewed task PRs -> release/*

Scheduled orchestrator MUST NOT:
    merge unattended task PRs -> main
    merge release/* -> main
```

Promotion of `release/*` into `main` is the human review boundary.
