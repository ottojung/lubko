# Scheduled ChatGPT orchestrator on Lubko

## What this document is

This is the reusable operating guide for a **recurring, scheduled ChatGPT invocation** that uses **Lubko** as its execution platform.

It defines the mechanics that scheduled orchestrators share across projects: disposable invocations, durable ownership, abandonment and inheritance, recovery, liveness, and normal Lubko operation.

It deliberately does **not** decide:

- which repository or project is being maintained;
- which specific work item should be selected;
- whether abandoned work should be preferred over new work;
- which branch or integration strategy the target project uses;
- whether completed work may merge to the default branch;
- what project-specific state counts as completion.

Those decisions belong in a project-specific **itinerary** that calls this document.

Two things are deliberately kept distinct throughout this document:

- **Lubko** — the execution/orchestration platform: the server that runs queued development jobs and managed `lubko-agent` sessions, plus its transport (`lubko.jobs`), agent state, worktrees/checkouts, and execution state.
- **Target repository (or target project)** — the repository or project identified by the calling itinerary: its issues or other work records, branches, PRs, CI, and history.

The guide does not depend on a particular schedule cadence.

## References

- Canonical Lubko operating skill: <https://github.com/ottojung/lubko/blob/main/docs/SKILL.md> — obey it for normal Lubko operation: job submission and polling, managed-agent lifecycle, liveness invariants, verification, and Git/GitHub practice.
- This document's canonical URL: <https://github.com/ottojung/lubko/blob/main/docs/skills/scheduled.md>

A project-specific itinerary should be the scheduled task's entry point and should direct the orchestrator to study both this document and the canonical Lubko operating skill.

## Contract with the calling itinerary

The calling itinerary supplies the project-specific policy. At minimum, it must make clear:

- the target repository or project;
- how the orchestrator determines which concrete work to do;
- the project's branch, PR, merge, and integration policy;
- the project's completion condition.

This document supplies the shared coordination and execution mechanics around that policy. When this document says to continue or complete work, interpret that according to the calling itinerary.

## A scheduled run is disposable

A scheduled ChatGPT invocation is disposable: it may be externally interrupted at any time, and a later invocation must be able to continue from observable state. Conversation memory is useful context but must never be the only record of ongoing work.

The sources of truth are:

- the target project's durable work record, including the orchestrator status comment described below when work is tracked by GitHub issue;
- the **Lubko server** for queued jobs, managed agents, worktrees/checkouts, and execution state;
- the rest of the target repository for branches, PRs, CI, and git history.

When GitHub issues are used for scheduled work, the issue status comment is the canonical coordination signal between orchestrators. Its GitHub `updated_at` is the authoritative activity timestamp.

## Startup sequence for every scheduled run

1. Read and obey the project-specific itinerary that was used as the scheduled task entry point.
2. Read <https://github.com/ottojung/lubko/blob/main/docs/skills/scheduled.md>.
3. Read <https://github.com/ottojung/lubko/blob/main/docs/SKILL.md> and obey it for normal Lubko operation.
4. Identify the concrete work according to the calling itinerary.
5. Before taking over issue-tracked work, apply the ownership and recovery protocol below.
6. Continue doing the work according to the itinerary and Lubko skills; do not stop after merely inspecting or reporting what could be done.

## Issue status comment: ownership and recovery

Every orchestrator that works on a GitHub issue must maintain **one durable orchestrator status comment on that issue**. Create the comment once and edit that same comment as the work changes.

Use a stable machine-recognizable marker and a compact human-readable body. For example:

```text
Orchestrator: working
Owner: scheduled-7f3a

Resources currently owned:
- /workspace/task-123713
- Lubko agent a91c02
- branch issue-7421273173
- PR #812

<!-- lubko-orchestrator-status -->
```

The exact presentation may evolve, but the comment must make these facts unambiguous:

- `state`: normally `working` while this invocation owns the issue, and `completed` once its workflow is actually complete according to the calling itinerary;
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

Whether abandoned work should be selected before other work is a project-specific decision for the calling itinerary. Once an itinerary chooses to inherit abandoned issue-tracked work:

1. re-read the issue and canonical status comment immediately before takeover;
2. replace `owner` with a fresh owner ID for the new invocation and update the comment;
3. re-read the canonical comment and yield if this invocation is not its owner;
4. preserve and update useful resource entries rather than erasing them;
5. inspect the referenced Lubko state, work directories, agents, branches, PRs, issue discussion, CI, and any other recorded resources;
6. continue the existing workflow from objective state.

### Completion

Only mark the status comment `completed` after the workflow for that issue is actually complete according to the calling itinerary.

When completing, update the resources section with the final durable handles that make the result easy to audit. A `completed` status is never treated as abandoned.

## Recovery from interrupted turns

Assume every previous scheduled invocation may have disappeared without a final response. Recover by inspecting actual state, not by assuming the previous invocation completed cleanly.

The issue status comment tells you which work was actively owned, which ownership is stale enough to inherit, and which resources the previous orchestrator considered part of that work. Treat those resource entries as recovery leads, then verify them against actual Lubko and repository state before acting.

For inherited work, reconstruct visibly unfinished work from real state, including as applicable:

- the status comment's recorded resources;
- Lubko root jobs and managed agents;
- their cwd, worktree, branch, title, and prompt context;
- target-repository worktrees/branches;
- open target-repository PRs and their bases;
- target-repository issue discussion and CI state;
- any project-specific integration branch or other durable state named by the itinerary.

Never depend on conversation state as the only record of:

- orchestrator owner ID;
- root job UUIDs;
- Lubko agent IDs;
- cwd/worktree;
- target work item;
- target branch;
- PR;
- project-specific integration state;
- expected completion state.

Keep the issue status comment current enough that a later invocation has concrete recovery handles even when conversation state disappears.

## Soft exclusion, not locking

Multiple scheduled invocations may overlap; avoid intentional duplicate work by using the target issue's orchestrator status comment as the ownership signal when the project tracks work through issues.

> Avoid work with a fresh `working` owner, permit takeover only after the abandonment threshold, and re-read ownership before each refresh so an older invocation yields after takeover.

If two orchestrators race, ordinary isolated branches/worktrees, tests, PR review, and conflict handling remain the correctness boundary.

## Project-specific branching and work selection are out of scope here

This document intentionally contains no policy for choosing the next issue or other work item and no policy requiring a particular integration branch, release branch, or merge target.

The calling itinerary must define those rules. This separation is what makes `scheduled.md` reusable by projects with different work-selection and release flows.

## Operating within Lubko

Operate through the Lubko platform exactly as <https://github.com/ottojung/lubko/blob/main/docs/SKILL.md> prescribes, and keep the target project's own operating instructions (`AGENTS.md`, `CONTRIBUTING.md`, design docs) authoritative for the target repository.

- Keep the current issue's orchestrator status comment updated at least every 5 minutes while the issue is `working`, when issue-based coordination is in use.
- Submit commands and managed agents through the Lubko transport (`lubko.jobs`); record the returned root job UUIDs and poll them to terminal state.
- Prefer managed `lubko-agent` sessions for substantial target-project work, with preassigned agent IDs, an explicit cwd inside a target-project worktree, and durable logs.
- Never passively wait: outstanding work requires another bounded observation/polling step in the current turn.
- Push work branches early and keep them pushed; open draft PRs early; review before merge; treat tests as evidence, not proof.
- Verify target-project work with the target repository's own required checks and instructions.
- Treat commit, push, merge, and deploy as distinct ordered steps and obey the calling itinerary's integration policy.
