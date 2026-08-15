# Scheduled ChatGPT orchestrator for a Lubko-backed target repository

## What this document is

This is the operating guide for a **recurring, scheduled ChatGPT invocation** that maintains an independently configured **target repository** using **Lubko** as its execution platform. It is not a workflow for developing the Lubko repository; Lubko is the infrastructure the scheduled orchestrator runs on top of.

Two things are deliberately kept distinct throughout this document:

- **Lubko** — the execution/orchestration platform: the server that runs queued shell jobs and managed `lubko-agent` sessions, plus its transport (`lubko.jobs`), agent state, worktrees/checkouts, and execution state.
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

- the **Lubko server** for queued jobs, managed agents, worktrees/checkouts, and execution state;
- the **target repository** for issues, branches, PRs, CI, and git history.

## Startup sequence for every scheduled run

1. Read <https://github.com/ottojung/lubko/blob/main/docs/skills/scheduled.md>.
2. Read <https://github.com/ottojung/lubko/blob/main/docs/SKILL.md> and obey it for normal Lubko operation.
3. Identify the configured target repository from the scheduled-task prompt (its URL; see [Minimal scheduled-task description](#minimal-scheduled-task-description)).
4. Inspect the Lubko server and the target repository before choosing new work.
5. Reconstruct visibly unfinished work for the target project from real state (see [Recovery from interrupted turns](#recovery-from-interrupted-turns)).
6. Prefer resuming unfinished work over starting another issue.
7. Apply [soft exclusion](#soft-exclusion-not-locking) before selecting work.
8. If there is no unfinished target-project work to resume, choose an actionable open issue from the **target repository** and start working on it (see [Choosing new work](#choosing-new-work)).
9. Continue doing the work according to the Lubko skills; do not stop after merely inspecting or reporting what could be done.

## Recovery from interrupted turns

Assume every previous scheduled invocation may have disappeared without a final response. Recover by inspecting actual state, not by assuming the previous invocation completed cleanly.

If Lubko still has managed agents or root jobs whose cwd, worktree, branch, title, or prompt clearly belongs to the configured target project, inspect those exact agents/jobs and continue the workflow.

Reconstruct visibly unfinished work for the target project from real state, including as applicable:

- Lubko root jobs and managed agents;
- their cwd, worktree, branch, title, and prompt context;
- target-repository worktrees/branches;
- open target-repository PRs and their bases;
- target-repository issues and CI state.

Never depend on conversation state as the only record of:

- root job UUIDs;
- Lubko agent IDs;
- cwd/worktree;
- target issue;
- target branch;
- PR;
- current release branch;
- expected completion state.

When conversational bookkeeping is missing, reconcile from Lubko + GitHub evidence.

## Soft exclusion, not locking

Do not add correctness-critical distributed locking merely to coordinate scheduled ChatGPT invocations. Multiple scheduled invocations may overlap; this is primarily an efficiency problem.

> Multiple scheduled ChatGPT invocations may overlap. Before choosing work, inspect active Lubko jobs/agents and the target repository's branches/PRs. Do not intentionally work on an issue or workstream that another active invocation appears to be handling. Perfect exclusion is not required; this is an efficiency rule, not a correctness invariant.

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

If there is no unfinished work for the configured target project, inspect that project's open GitHub issues and choose an actionable issue.

Do not interpret "no Lubko work is active" as permission to modify the Lubko codebase. The chosen issue belongs to the **configured target repository** unless the scheduled task explicitly names Lubko as its target repository.

Prefer a deterministic, understandable selection policy when useful, but do not over-engineer issue claiming/locking.

## Minimal scheduled-task description

The actual ChatGPT scheduled-task prompt/description must contain only the minimum bootstrap information needed to locate the target and the instructions. Broadly, it should contain only:

1. the **target repository URL**;
2. the absolute URL of this scheduled skill file.

For example:

> Target repository: `<TARGET_REPOSITORY_URL>`
>
> Follow: <https://github.com/ottojung/lubko/blob/main/docs/skills/scheduled.md>

Do not duplicate the orchestration rules in the scheduled-task description. In particular, the task prompt must not restate recovery rules, release-branch behavior, soft exclusion, issue-selection policy, Lubko operating details, or merge restrictions. All such behavior belongs in this document so it can evolve centrally without editing every ChatGPT scheduled task.

## Operating within Lubko

Operate through the Lubko platform exactly as <https://github.com/ottojung/lubko/blob/main/docs/SKILL.md> prescribes, and keep the target project's own operating instructions (`AGENTS.md`, `CONTRIBUTING.md`, design docs) authoritative for the target repository.

- Submit commands and managed agents through the Lubko transport (`lubko.jobs`); record the returned root job UUIDs and poll them to terminal state.
- Prefer managed `lubko-agent` sessions for substantial target-project work, with preassigned agent IDs, an explicit cwd inside a target-project worktree, and durable logs.
- Never passively wait: outstanding work requires another bounded observation/polling step in the current turn.
- Push work branches early and keep them pushed; open draft PRs early; review before merge; treat tests as evidence, not proof.
- Verify target-project work with the target repository's own required checks and instructions, and run them on the reconciled branch.
- Treat commit, push, and deploy as distinct ordered steps. Deploy only when explicitly asked, through the project's managed deploy tool.
- Never merge task PRs or the release branch into the target default branch; promotion is the human review boundary.
