# Scheduled ChatGPT orchestrator for a Lubko-backed target repository

## What this document is

This is the operating guide for a **recurring, scheduled ChatGPT invocation** that maintains an independently configured **target repository** using **Lubko** as its execution platform. It is not a workflow for developing the Lubko repository; Lubko is the infrastructure the scheduled orchestrator runs on top of.

Two things are deliberately kept distinct throughout this document:

- **Lubko** — the execution/orchestration platform: the server that runs queued process-argv jobs (protocol v3, exec'd with no shell) and managed `lubko-agent` sessions, plus its transport (`lubko.jobs`), agent state, worktrees/checkouts, and execution state.
- **Target repository (or target project)** — the GitHub repository the scheduled orchestrator is configured to maintain: its issues, branches, PRs, CI, and git history.

A scheduled run operates on the Lubko repository only when the scheduled task explicitly names it as the target repository. Do not assume the target is `ottojung/lubko`, and never treat Lubko's own checkout as the worktree being modified for target-project work.

The intended schedule is hourly. The guide does not depend on the cadence.

## References

- Canonical Lubko operating skill: <https://github.com/ottojung/lubko/blob/main/docs/SKILL.md> — read it at the start of every scheduled run and obey it for normal Lubko operation: job submission and polling, managed-agent lifecycle, liveness invariants, verification, and Git/GitHub practice.
- This document's canonical URL: <https://github.com/ottojung/lubko/blob/main/docs/skills/scheduled.md>

Absolute URLs are used for Lubko skill references so this guide stays correct when read from a scheduled conversation that has no project-level Lubko repository context.

## A scheduled run is disposable

A scheduled ChatGPT invocation is disposable: it may be externally interrupted at any time, and a later invocation must be able to continue from observable state. Conversation memory is useful context but must never be the only record of ongoing work.

The sources of truth are:

- the **target repository's GitHub issues**, especially the orchestrator status comment described below, for durable work ownership and recovery metadata;
- the **Lubko server** for queued jobs, managed agents, worktrees/checkouts, and execution state;
- the rest of the **target repository** for branches, PRs, CI, and git history.

The issue status comment is the canonical coordination signal between orchestrators. Its GitHub `updated_at` is the authoritative activity timestamp.

## Startup sequence for every scheduled run

1. Read <https://github.com/ottojung/lubko/blob/main/docs/skills/scheduled.md>.
2. Read <https://github.com/ottojung/lubko/blob/main/docs/SKILL.md> and obey it for normal Lubko operation.
3. Identify the configured target repository from the scheduled-task prompt.
4. Inspect the target repository's open issues and their orchestrator status comments before choosing work.
5. Treat a `working` status whose GitHub comment `updated_at` is less than 10 minutes old as actively owned by another orchestrator; do not intentionally work on that issue.
6. Treat a `working` status whose GitHub comment `updated_at` is at least 10 minutes old as abandoned and inheritable. Prefer inheriting abandoned target-project work over selecting a new issue.
7. Treat a `completed` status as finished scheduled work even if the issue remains open while waiting for human promotion of the release branch; do not select it as new work.
8. When inheriting, replace the status comment's owner with this invocation's fresh owner ID, refresh the comment, then reconstruct the work from its recorded resources, Lubko state, branches, PRs, and CI.
9. If there is no abandoned work to resume, choose an actionable open issue from the **target repository** that has no active or completed orchestrator status, claim it by creating or updating its single orchestrator status comment, and start working on it.
10. Continue doing the work according to the Lubko skills; do not stop after merely inspecting or reporting what could be done.

## Issue status comment: ownership and recovery

Every orchestrator that works on a GitHub issue must maintain **one durable orchestrator status comment on that issue**. Create the comment once and edit that same comment as the work changes.

Use a stable machine-recognizable marker and a compact human-readable body. For example:

```text
Orchestrator: working
Owner: scheduled-7f3a

Resources currently owned:
- /workspace/volodyslav-task-123713
- Lubko agent a91c02
- branch issue-7421273173
- PR #812

<!-- lubko-orchestrator-status -->
```

The exact presentation may evolve, but the comment must make these facts unambiguous:

- `state`: normally `working` while this invocation owns the issue, and `completed` once its workflow is actually complete;
- `owner`: a fresh short identifier chosen by the orchestrator when it claims or inherits the issue;
- **resources currently owned**: whatever concrete resources the orchestrator judges useful for recovery. This should include Lubko work directories and managed agents when they exist, and may also include branches, PRs, root job UUIDs, temporary clones, or any other relevant handles.

Do not put credentials, secret values, or unnecessary logs in this comment.

### Canonical comment and races

Normally there is exactly one marked orchestrator status comment per issue. If a race causes multiple comments containing `<!-- lubko-orchestrator-status -->`, treat the **most recently updated marked comment** as canonical. Do not create additional marked comments once one exists.

Immediately after claiming or inheriting an issue, re-read the canonical status comment. If it does not contain this invocation's owner ID, another invocation won the race; yield and do not start or continue substantial work on that issue.

### Activity cadence

While an orchestrator intends to retain ownership of a `working` issue, it must update the canonical status comment **at least once every 5 minutes**, even when no other work-state change needs to be recorded.

The authoritative activity time is the comment's GitHub `updated_at`.

Before refreshing the comment, re-read the canonical issue status comment. If its `owner` is no longer this invocation's owner ID, another orchestrator has inherited the issue. Stop orchestrating that issue rather than overwriting the newer ownership record.

### Abandonment and inheritance

A task is abandoned for orchestrator coordination when:

```text
status.state == working
AND now - status_comment.updated_at >= 10 minutes
```

Abandonment means the previous orchestrator is no longer presumed responsible. Existing resources and partial work remain candidates for recovery.

To inherit abandoned work:

1. re-read the issue and canonical status comment immediately before takeover;
2. replace `owner` with a fresh owner ID for the new invocation and update the comment;
3. re-read the canonical comment and yield if this invocation is not its owner;
4. preserve and update useful resource entries rather than erasing them;
5. inspect the referenced Lubko state, work directories, agents, branches, PRs, issue discussion, CI, and any other recorded resources;
6. continue the existing workflow from objective state.

### Completion

Only mark the status comment `completed` after the scheduled workflow for that issue is actually complete according to this document: implementation/review/validation is complete and the task PR has reached the intended release branch state.

When completing, update the resources section with the final durable handles that make the result easy to audit. A `completed` status is never treated as abandoned and is not eligible for automatic issue selection merely because the GitHub issue remains open awaiting promotion of the release branch.

## Recovery from interrupted turns

Assume every previous scheduled invocation may have disappeared without a final response. Recover by inspecting actual state, not by assuming the previous invocation completed cleanly.

The issue status comment tells you which work was actively owned, which ownership is stale enough to inherit, and which resources the previous orchestrator considered part of that work. Treat those resource entries as recovery leads, then verify them against actual Lubko and GitHub state before acting.

For an inherited issue, reconstruct visibly unfinished work from real state, including as applicable:

- the status comment's recorded resources;
- Lubko root jobs and managed agents;
- their cwd, worktree, branch, title, and prompt context;
- target-repository worktrees/branches;
- open target-repository PRs and their bases;
- target-repository issue discussion and CI state.

Never depend on conversation state as the only record of:

- orchestrator owner ID;
- root job UUIDs;
- Lubko agent IDs;
- cwd/worktree;
- target issue;
- target branch;
- PR;
- current release branch;
- expected completion state.

Keep the issue status comment current enough that a later invocation has concrete recovery handles even when conversation state disappears.

## Soft exclusion, not locking

Multiple scheduled invocations may overlap; avoid intentional duplicate work by using the target issue's orchestrator status comment as the ownership signal.

> Avoid issues with a fresh `working` status, inherit stale `working` issues, skip `completed` scheduled work, and re-read ownership before each refresh so an older invocation yields after takeover.

If two orchestrators race, ordinary isolated branches/worktrees, tests, PR review, and git merge-conflict handling remain the correctness boundary.

## Target repository release-branch workflow

Scheduled/unattended work must not be merged directly into the target repository's default branch (`main`, `master`, or equivalent). Instead, completed work accumulates in the latest active unmerged `release/*` branch **of the target repository**. A human periodically reviews that release branch and promotes it into the target repository's default branch.

### Release branch lifecycle

For the configured target repository:

- Find the latest active unmerged `release/*` branch.
- If none exists, create one from the current target default branch.
- Do not create one release branch per issue.
- Reuse the latest active release branch across scheduled runs and across multiple completed issues.
- Once that release branch has been merged into the target default branch, stop using it and create a fresh release branch from the new default-branch head.
- A date/timestamp-based naming scheme such as `release/2026-08-15` is acceptable.

### Keep the release branch reconciled with default

Whenever a scheduled invocation begins operating on the current release branch, first update the target default branch and merge it into the release branch:

```sh
git checkout <release-branch>
git merge origin/<target-default-branch>
```

Resolve conflicts and verify the release branch before starting new work.

After merging a completed task PR into the release branch, merge the latest target default branch into the release branch again and run the required verification.

This does not guarantee that a future default-branch commit cannot conflict, but it ensures the unattended release branch is reconciled with the latest known default branch whenever the scheduled bot operates.

### Issue branches and PRs

For each target-project issue:

- start from the current target `release/*` branch;
- create a normal issue/task branch in an isolated worktree as required by the Lubko skill;
- implement, test, and independently review the work;
- push the task branch normally;
- open the task PR against the **current release branch**, not the target default branch;
- merge the completed/reviewed task PR into the release branch;
- use the updated release branch as the base for subsequent target-project work.

Hard rules:

```text
Scheduled orchestrator MAY, in the configured target repository:
    create release/* from the target default branch
    merge target default branch -> release/*
    create task branches from release/*
    merge reviewed task PRs -> release/*

Scheduled orchestrator MUST NOT:
    merge unattended task PRs -> target default branch
    merge release/* -> target default branch
```

Promotion of `release/*` into the target default branch is the human review boundary.

## Choosing new work

If there is no abandoned work to inherit, inspect the configured target project's open GitHub issues and choose an actionable issue that has neither a fresh `working` status nor a `completed` orchestrator status.

Do not interpret "no Lubko work is active" as permission to modify the Lubko codebase. The chosen issue belongs to the **configured target repository** unless the scheduled task explicitly names Lubko as its target repository.

Prefer a deterministic, understandable selection policy when useful, but do not over-engineer issue selection. Immediately after choosing an issue, claim it through its orchestrator status comment before starting substantial work, then re-read the canonical comment to make sure the claim still belongs to this invocation.

## Minimal scheduled-task description

For example:

> Target repository: `<TARGET_REPOSITORY_URL>`
>
> Follow: <https://github.com/ottojung/lubko/blob/main/docs/skills/scheduled.md>

Do not duplicate the orchestration rules in the scheduled-task description. In particular, the task prompt must not restate recovery rules, release-branch behavior, issue-status coordination, issue-selection policy, Lubko operating details, or merge restrictions. All such behavior belongs in this document so it can evolve centrally without editing every ChatGPT scheduled task.

## Operating within Lubko

Operate through the Lubko platform exactly as <https://github.com/ottojung/lubko/blob/main/docs/SKILL.md> prescribes, and keep the target project's own operating instructions (`AGENTS.md`, `CONTRIBUTING.md`, design docs) authoritative for the target repository.

- Keep the current target issue's orchestrator status comment updated at least every 5 minutes while the issue is `working`, regardless of whether new Lubko commands are needed.
- Submit commands and managed agents through the Lubko transport (`lubko.jobs`); record the returned root job UUIDs and poll them to terminal state.
- Prefer managed `lubko-agent` sessions for substantial target-project work, with preassigned agent IDs, an explicit cwd inside a target-project worktree, and durable logs.
- Never passively wait: outstanding work requires another bounded observation/polling step in the current turn.
- Push work branches early and keep them pushed; open draft PRs early; review before merge; treat tests as evidence, not proof.
- Verify target-project work with the target repository's own required checks and instructions, and run them on the reconciled branch.
- Treat commit, push, and deploy as distinct ordered steps. Deploy only when explicitly asked, through the project's managed deploy tool.
- Never merge task PRs or the release branch into the target default branch; promotion is the human review boundary.
