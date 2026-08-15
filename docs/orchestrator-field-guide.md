# Orchestrator Field Guide

Orchestrating software-development work on an agent-capable development server.

Status: living document. It records what has empirically worked, what has
empirically failed, and the working rules that follow, from the point of view
of an orchestrator that has driven real development, review, deployment, and
acceptance work through an agent-capable development server — a server that
runs development commands and hosts managed agent sessions on the user's
behalf.

This document is guidance for the **orchestrator**, not for repository agents.
It complements, and is subordinate to, the repository's own operating
instructions (for example `AGENTS.md`, `CONTRIBUTING.md`, and the project's
design docs). Where this guide contradicts an earlier habit, this guide is the
correction.

## How to read this guide

Claims are tagged so you can tell hard-won observation from opinion:

- **[Observed]** — a pattern that has been confirmed in practice, through
  command results, agent logs, git history, or live server state.
- **[Recommended]** — a rule derived from those observations. Treat these as
  defaults, not laws; every rule has a legitimate exception you must be able to
  name.

Sections 1–18 are principles. Section 19 is the practical guide to Git/GitHub
branch and PR management. Section 20 lists failure modes that have each been
observed at least once. Section 21 is the executable workflow/checklist.
Section 22 is the quick reference.

---

## 1. Delegate substantial work to agents

**[Observed]** Substantial multi-step work — implementing an issue, refactoring,
investigating a test failure, writing a migration, reviewing a subsystem — has
reliably produced better results through a managed agent session than through
the orchestrator composing long shell command chains by hand.

**[Observed]** The sharpest failures have come from work that needed reasoning
but was executed as a series of short, stateless shell commands. Each command
re-inspects the world from zero, accumulates no context, and cannot iterate.

**[Recommended]** Default to an agent for any task that requires judgment,
context, iteration, or more than a couple of obvious shell commands. Use the
rule: **direct shell for observation, agents for work.** Give the agent an
explicit working directory, a title, and a detailed prompt (see Section 11).

**[Recommended]** Keep the orchestrator role as: decide *what* should happen,
specify *constraints*, delegate the *how*, then verify the *result*
independently. Do not ask the user to run commands the development server can
run, and do not turn repository work back into instructions to the user.

---

## 2. Let agents think without arbitrary time pressure

**[Observed]** Agents that were stopped or killed because the orchestrator
judged them "slow" had, in several cases, just spent that time on exactly the
reasoning the task required — reading the real code before editing it. Stopping
them forced the orchestrator to redo or re-verify the work later.

**[Observed]** Interrupted sessions were not resumable as-is: when those tasks
were resumed, a fresh agent re-derived context that the interrupted agent had
already built.

**[Recommended]** Do not impose deadlines on thinking. When an agent appears to
be taking long, first check its status and a log tail. Ask "is it making
progress?" not "is it done yet?" An agent that is reading files, running tests,
and converging is working; an agent that is looping on one failing action is
stuck.

**[Recommended]** Use a blocking wait only when you are confident no
intermediate steering is useful, and remember the timeout stops *waiting*, not
the agent. For genuinely long or uncertain tasks, poll status and occasionally
read the log instead of blocking.

**[Recommended]** Stopping is a decision that the task is no longer wanted, not
a pause button. Prefer a steering prompt for course correction and reserve
stop/kill for abandoned tasks.

---

## 3. Inspect status and process activity rather than repeatedly steering

**[Observed]** The most over-orchestrated agents are the ones whose orchestrator
sent frequent prompts ("now do X", "are you done?") without first reading status
or the log. Each such prompt interrupts the agent's reasoning and can push it to
declare premature completion.

**[Recommended]** Before any prompt, read the evidence: the agent's status, then
a focused log tail when more detail is needed. Only prompt when the evidence
shows a concrete problem or a new requirement.

**[Recommended]** Steer with *constraints and acceptance criteria*, not with
play-by-play instructions. One precise follow-up that says what is wrong and
what "done" means is worth ten that say what to type next.

**[Recommended]** When you do not know an agent's ID, recover it from the agent
management interface, and record the ID in your own state immediately after
every launch. Never rely on a "most recent" shortcut when multiple agents
exist.

---

## 4. Use parallel agents on isolated repo clones and branches

**[Observed]** The most productive work on this system ran **multiple agents in
parallel**, each in its own clone with its own branch:

- the core implementation branch at a dedicated clone path;
- a docs branch in a separate clone;
- an acceptance branch in a separate clone;
- an integration branch in a separate clone, plus a final read-only review
  agent.

Each clone isolated the agents from each other's uncommitted changes and from
the live checkout.

**[Observed]** The single most common source of cross-agent corruption has been
**two write-heavy agents in the same working tree.** One agent's `git checkout`,
`git reset`, or uncommitted edit silently destroys or masks another's.

**[Recommended]** For parallel write work, always give each agent its own clone
(`git clone` to a distinct path) and its own branch. Never point two
write-capable agents at the same tree. An independent reviewer may share the
tree only if it is read-only and the tree is committed first. Where a shared
repository is preferable, `git worktree` gives each branch its own directory
with the same isolation (Section 19).

**[Recommended]** Treat each clone as disposable. The durable artifact is the
branch you push and reconcile; the working tree is scratch space.

---

## 5. Give independent agents separate responsibilities

**[Observed]** Separating concerns across parallel agents has been the pattern
behind the cleanest outcomes:

- one **implementation** agent that owns the production code change;
- one **acceptance** agent that independently designs black-box tests against
  the required contract, without reading the implementation;
- one **docs/review** agent that updates documentation and/or performs a
  read-only review focused on soundness;
- the **orchestrator**, which reconciles branches and verifies invariants.

**[Recommended]** Assign disjoint filesystems and disjoint responsibilities. An
acceptance agent should not be told "verify the implementation"; it should be
given the *contract* and asked to test the *behavior*. A reviewer should be told
to review, not to fix — or told to fix only concrete bugs it finds, never to
"improve" freely.

**[Recommended]** Put every agent's mandate in the initial prompt, including
what it must *not* do (see Section 11). The cost of a wrong responsibility split
is usually only discovered at reconciliation, which is the most expensive time
to find it.

---

## 6. Reconcile branches deliberately

**[Observed]** Parallel branches do not merge themselves. Reconciliation is a
separate, deliberate step: take the reviewed core, layer on the cherry-picked
docs, add the acceptance tests, then fix the integration fallout. In practice
the fastest path has been a **dedicated integration session on a dedicated
branch** that cherry-picks the accepted work and runs the full checks — not an
impatient `git merge` into the main branch.

**[Recommended]** Reconcile in this order:

1. Verify each contributing branch is committed and pushed, with a clean tree.
2. Identify the known base commit shared by all branches.
3. Build an integration branch from the most trusted component.
4. Cherry-pick or merge the other components one at a time, running the full
   checks after each addition so you can attribute any breakage.
5. Resolve conflicts explicitly; never resolve with a blind `git checkout
   --theirs` or a forced overwrite.
6. Only after the integrated branch is green do you consider the main branch.

**[Recommended]** Do not reconcile two branches by letting one agent operate in
the other's clone. Reconcile by commits and branches, in the orchestrator's
controlled order.

---

## 7. Keep independent acceptance independent

**[Observed]** Acceptance tests written against the implementation agent's own
branch have a systematic blind spot: they encode the same assumptions the
implementation encoded. The acceptance agent that produced the best findings
was explicitly told to *"independently design and implement black-box/
acceptance tests ... without relying on another agent's implementation."*

**[Recommended]** Contract tests must be written from the **contract** — the
issue, the protocol, the documented behavior — not from the implementation. Give
the acceptance agent its own clone, its own branch, the spec, and no access to
the implementation branch until the tests are written.

**[Recommended]** When you run the acceptance suite against the reconciled
branch, treat failures as first-class evidence about the implementation, not as
a test bug to suppress. If a test encodes an assumption you actually want to
reject, change the *test* deliberately and document why — do not silently mark
it to skip.

---

## 8. The orchestrator itself reads code and reviews invariants

**[Observed]** The orchestrator has repeatedly found hard bugs that passing
tests did not catch: concurrency races in job claiming, leasing, and recovery;
wrong process-group handling; a "fixed" deadline race that the tests' timing
happened to mask; and accidental test-only production knobs. In each case the
finding came from *reading the code and the diff* against the system's stated
invariants — not from running tests.

**[Observed]** A review agent told to treat *"automated tests as evidence, not
proof"* and to prioritize *"hard soundness/concurrency/state-machine/process-
lifecycle bugs"* produced findings the implementation and acceptance agents had
both missed.

**[Recommended]** After an agent reports success, do not merely relay its
summary. Read the diff. Check the invariants that matter to this codebase:
atomic and exactly-once state transitions, precise process signaling, no
credentials in environments or logs, and no destructive action before durable
state exists. Tests passing is necessary, not sufficient.

**[Recommended]** Read the project's own checklist that says what "done" means
for the subsystem. For hard concurrency or lifecycle work, run a dedicated
read-only review pass before merging even when the implementation agent says
everything is green.

**[Recommended]** When you read the diff and find a discrepancy with an
invariant, that is a bug until proven otherwise — even if the tests pass.
Investigate to closure before reconciliation.

---

## 9. Share partial findings early

**[Observed]** The most useful findings arrived *before* the task completed:
a reviewer flagging a soundness concern while the implementation was still in
flight, an acceptance agent reporting a contract ambiguity mid-way, an
orchestrator noticing a base-commit mismatch between branches while both were
still running. Early findings changed direction cheaply.

**[Recommended]** Ask agents to report early, risky findings in their prompt:
*"If you find a blocker, a violated invariant, or a changed understanding of the
task, surface it now rather than continuing to the end."* Do not require agents
to finish before communicating.

**[Recommended]** When the orchestrator spots something mid-flight, share it
immediately with the affected agent via a steering prompt, even if it means the
agent re-plans. A stopped-wrong task is cheaper than a finished-wrong task.

**[Recommended]** Keep partial progress durable: ask agents to commit
incrementally on their branch, not only at the end. A branch with frequent,
logical commits is far easier to reconcile and to salvage than one
last-minute commit.

---

## 10. Avoid low-level direct shell except for tiny deterministic observations and agent lifecycle

**[Observed]** Long, improvised shell pipelines have been a recurring source of
bloat and confusion: quoting errors, working-directory drift, truncated output,
and state lost between commands. The reliable fast path has been managed agent
sessions for work and short, deterministic shell commands for observation.

**[Recommended]** Reserve direct shell commands for tiny, deterministic
observations and for agent lifecycle:

```text
pwd
git status --short
git diff --stat
git diff
ls a directory
git log --oneline -5
git branch -vv
cat one short file
print a tool version
check whether a process exists
agent new / status / list / prompt / log / wait / stop / kill / result
```

**[Recommended]** If a shell command needs quoting, conditionals, loops, or
coordination between several files, it is no longer an observation — delegate it
to an agent. Also keep output small: prefer `git status --short`, log tails of
100–200 lines, and focused `sed` ranges over full dumps.

---

## 11. Use precise prompts with invariants and negative requirements

**[Observed]** The prompts that produced the best work were long on *constraint*
and short on *how-to*. They named the objective, the architectural context, the
repository's own instructions (`AGENTS.md`), the required checks, and the
**negative requirements** explicitly: *"do not deploy,"* *"do not push,"*
*"do not expose credentials,"* *"do not modify the schema,"* *"do not close the
issue yourself."*

**[Observed]** Every deployment-capable prompt that lacked an explicit
"do not deploy" created risk and, at least once, required a corrective follow-up
because the agent's own judgment about authority disagreed with the
orchestrator's.

**[Recommended]** A strong agent prompt contains:

1. **Objective** — what must be true when done.
2. **Context** — the relevant history and architecture, including invariants.
3. **Location** — the working directory.
4. **Constraints** — what may and may not change.
5. **Local instructions** — "read and obey AGENTS.md / docs/*.md first."
6. **Validation** — the exact checks to run.
7. **Completion criteria** — the objective evidence of done.
8. **Negative requirements** — explicitly what the agent must not do (deploy,
   push, expose secrets, touch unrelated files, expand scope into a sibling
   issue, edit a file another agent owns).

**[Recommended]** Name the invariants that the agent must preserve, drawn from
the project's own design docs: for example atomic, exactly-once state
transitions; precise process signaling; no credentials in logs, commits, or
process environments; and git state changed only on the agent's own branch.

---

## 12. Preserve clean working trees and known base commits

**[Observed]** Reconciling parallel branches has repeatedly turned on knowing
the exact base commit each branch was cut from. When a branch was cut from a
drifted tree, cherry-picks produced double-applied or missing changes that took
more effort to untangle than the original work.

**[Observed]** Agents that started with a dirty tree wasted early effort
stabilizing state the orchestrator should have guaranteed, and sometimes
committed unrelated files.

**[Recommended]** Before launching an implementation agent, ensure its clone is
on a known commit with a clean tree, and put the base commit in the prompt:
*"Baseline is <sha>, tests green; reconcile from there."* Record the base in
your own state so reconciliation can verify it.

**[Recommended]** After an agent finishes, verify `git status --short` shows
only intended changes, the intended commits exist on the branch, and the tree
was not force-reset or squashed without your knowledge. A clean, committed
branch is the contract your acceptance and review steps depend on.

---

## 13. Run the full checks

**[Observed]** Agents have repeatedly reported success on the subset of checks
they happened to run, while the full suite failed. The complete set is fixed by
the project's `AGENTS.md` / `CONTRIBUTING.md` and must not be shortened. On a
typical Python/`uv` project it looks like:

```sh
uv run ruff format --check .
uv run ruff check .
uv run mypy .
uv run pytest
```

**[Observed]** Validation-failing changes reached reconciliation more than once
because "it passed mypy" was treated as "it passed."

**[Recommended]** Put the exact full command list in every implementation and
acceptance prompt. Independently re-run the full suite after reconciliation, on
the integrated branch, before considering a merge — the orchestrator should not
trust a report of green that it did not observe. When a check fails after
reconciliation, attribute the failure to the most recently integrated change
and fix it there.

---

## 14. Do not confuse commit / push / deploy

**[Observed]** These three operations have different blast radius and different
authorities, and conflating them has caused real incidents: a change committed
and pushed to the remote main branch was treated as "deployed," and an agent
that was told to deploy performed its own push without the orchestrator
reviewing the committed state first.

**[Recommended]** Treat them as strictly ordered, separable steps:

1. **Commit** — durable local history on a branch. Cheap, reversible, safe.
2. **Push** — publishes commits to a remote. Visible to other collaborators;
   only do this on an explicit instruction and after the committed state has
   been reviewed.
3. **Deploy** — replaces the running service/worker/daemon. Highest blast
   radius; only via the project's managed deploy tool, only on explicit
   instruction, and only from a reviewed, validated checkout at an exact
   commit.

**[Recommended]** Name the target of each action in prompts. "Commit and push
the change" is one instruction. "Deploy the already-pushed commit" is a
*different* instruction — and in practice it has been given as a separate agent
session whose sole job was to verify the checkout at the exact commit and run
the managed deployment. Match that separation.

---

## 15. Never deploy implicitly

**[Observed]** A user request to "change the code" or "fix the bug" does not
mean "replace the running service." Agents given a deployment-capable
environment have, absent an explicit negative, occasionally inferred that
deployment was part of "do the right thing." The fix was always cheaper as a
prevention than as a post-hoc explain.

**[Recommended]** When the user asks only for code changes: modify, run checks,
inspect the diff, summarize, stop. Deploy only when the user explicitly asks to
deploy or upgrade. Put `do not deploy` in every agent prompt where the
distinction matters, and never allow an agent to decide for itself that a
deployment is warranted.

**[Recommended]** The orchestrator must also not deploy implicitly. Deploying is
its own explicit step, performed through the project's managed deploy tool
(never through manual process-tree manipulation), from a checkout that passed
the full validation, and after verifying the target commit is exactly the
commit you intend to run.

---

## 16. Use a real end-to-end round trip for verification

**[Observed]** The strongest end-to-end evidence on this system came from
submitting a real job through the production execution path and watching it run
in the live environment, then reading back its status and output: the job must
report success, a zero exit code, the expected stdout, and an identity for the
live executor that handled it.

**[Observed]** Simulated or mocked round trips have, more than once, passed
while the real execution path failed — for example a runtime started with the
wrong working directory, or a deployment that verified "the process started" but
not that it could reach its database.

**[Recommended]** For any change to the execution transport, the worker/runtime,
or a deployment lifecycle, verify with a real round trip after deployment:

1. Submit a job with a distinctive sentinel output.
2. Poll the job to a terminal state.
3. Assert success, exit code 0, exact expected stdout, and an executor
   identity consistent with the newly deployed runtime.

**[Recommended]** Remember that two lifecycles are distinct: the transport job
that launched an agent may finish while the agent continues. Verify agent work
through the **agent ID**, and verify runtime behavior through the **job**.

---

## 17. Design around secrets

**[Observed]** The runtime environment has been scrubbed of credential-bearing
variables; connection settings and credentials live in a permission-restricted
file, and the deploy tool strips credential-bearing variables from the
environment it hands to a deployed worker. This was a deliberate, tested design
— not an accident.

**[Observed]** Agents that printed, echoed, or dumped environment variables
while debugging have been a persistent risk. The prompt discipline that worked
was: never emit secret *values*; inspect variable *names*, permissions, and
counts only.

**[Recommended]** State the secret contract in prompts: no values in logs, no
values in commit content, no `echo $VAR`, no `env | ...`, no connection strings
in output. Verify secret handling by checking *absence*: assert the live
environment contains no credential variable names (check names only), the
config file mode is correct, and git history contains no secret. A secrets leak
discovered in git history is near-permanent; treat it as the worst class of
failure.

**[Recommended]** Do not put credentials or server identifiers in this guide,
in prompts, or in commits. Design the system so that secrets are never needed
in an instruction.

---

## 18. Parallelism rules that have held

**[Observed]** Parallelism works when isolation is real and mandates are
disjoint; it fails when agents share state. The rules below have held
repeatedly:

- Separate clones (or worktrees) for separate writers; a reviewer may share
  only a committed, read-only tree.
- One agent per responsibility: implementation, acceptance, docs/review.
- Every agent has a recorded ID, a title, an explicit working directory, and a
  single-mandate prompt.
- The orchestrator keeps the map: which branch, which base commit, which
  responsibility, which ID.
- No agent relies on a "most recent" shortcut; no two agents assume the same
  exclusive tree.
- Reconciliation is deliberate and done by the orchestrator (Section 6).

---

## 19. Git/GitHub branch and PR management

The mechanics of parallel agent work (Sections 4–6) only pay off if the Git
history they produce is easy to reconcile, review, and share. GitHub pull
requests are the primary observability tool for the human owner: every open,
update, and merge of a PR is a durable, human-visible record of what happened.
Use them by default for substantial changes; only trivial emergency fixes may
skip the ceremony (Section 19.18).

### 19.1 One task, one agent, one branch

**[Recommended]** Give every task its own branch, and give each agent exactly
one branch to own. A branch is a unit of accountability: its history should
tell the story of one task. Two tasks on one branch make review, acceptance,
and reversion harder; two agents on one branch invite the cross-agent
corruption described in Section 4.

**[Recommended]** Name branches after the task (`fix/lease-recovery`,
`feature/search-index`, `docs/orchestrator-guide`), not after the agent. A
name that says what the branch is *for* stays meaningful after the agent is
gone; a name that says who wrote it does not.

### 19.2 Isolate clones and worktrees

**[Recommended]** Give each write agent an exclusive working tree. For
independent parallel work use separate clones (Section 4); when agents share a
repository checkout, use `git worktree add` so each branch gets its own
directory while all work still lives in one repository. Never let two writers
into the same tree.

### 19.3 Start from a known base commit

**[Recommended]** Cut every branch from a known, clean, tested base — a real
commit SHA, not "whatever the tree looked like." Record the base commit in the
agent prompt and in your own state. When branches were cut from a drifted tree,
cherry-picks double-applied or lost changes (Section 12).

### 19.4 Commit incrementally

**[Recommended]** Ask agents to commit in small, logical, self-contained
commits as they go, not in one lump at the end. Incremental commits make it
possible to salvage partial work, to attribute breakage to a specific change,
and to reorder history when reconciliation demands it. A branch with frequent,
logical commits is far easier to reconcile than one with a single final commit.

### 19.5 Keep branches pushed

**[Recommended]** Push the branch as soon as there is something to see, and
keep it pushed as work proceeds. A pushed branch survives a lost or recycled
clone; an unpushed branch exists only in one disposable working tree. Never
treat the clone as the durable copy — the remote branch is the durable copy.

### 19.6 Open draft PRs early for observability

**[Recommended]** Open a **draft PR** as soon as the branch exists, even if it
is mostly empty. This makes the work visible to the human owner from day one:
they can watch progress, object early to a wrong direction, and see which
branches are active. A draft PR is cheap to update and costs nothing to leave
open; discovering a wrong direction after two weeks of agent work is expensive.

### 19.7 PRs as the human-visible activity log

**[Recommended]** Treat the PR as the human-visible record of the work. The
commit history, the diff, and the comments on the PR are what the human owner
(and any later collaborator) will read to understand what happened. Write PR
descriptions that say what changed and why, keep discussion on the PR rather
than in private agent state, and let the PR tell the story of the task.

### 19.8 Keep implementation, acceptance, and docs branches separate

**[Recommended]** Do not fold acceptance tests and documentation into the
implementation branch by default. Keep the implementation branch, the
acceptance branch, and the docs branch separate (Section 5), each opened as its
own PR or combined into one integration PR, so each part is independently
reviewable and mergeable. Folding everything into one branch makes it
impossible to review the code change without the noise of the docs change, and
vice versa.

### 19.9 Reconcile into an integration branch

**[Recommended]** When several branches contribute to one change, reconcile
them deliberately on a dedicated integration branch (Section 6): apply the
trusted components one at a time, run the full checks after each step, and only
then propose the integrated result for merge — via a PR, never as a direct push
to the default branch.

### 19.10 Cherry-pick vs merge vs rebase

**[Recommended]** Prefer **cherry-pick** when you are assembling a single
coherent change from multiple branches and you want only the accepted commits —
for example layering docs commits onto an implementation branch while leaving
the docs branch untouched. Prefer **merge** when you want to preserve the full,
branch-shaped history of two long-lived lines of work. Prefer **rebase** when
the branch's history needs to be linearized onto a newer base for review — but
rebase rewrites history, so only do it on branches that are not yet
shared/reviewed (or via a PR's squash/rebase merge on GitHub). Do not rebase a
branch that other agents or the human owner are already reading.

### 19.11 Updating PRs after review

**[Recommended]** Respond to review comments by pushing new commits to the same
PR branch, not by closing and reopening. Keep each review cycle as additional
commits (amend only pre-review commits); this lets reviewers see exactly what
changed in response to their feedback. Never force-push away the reviewed
history while review is in flight.

### 19.12 Resolve conflicts semantically

**[Recommended]** Resolve merge conflicts by reading both sides and deciding
what the merged result *should* be — never with a blind `git checkout --ours`/
`--theirs` or a forced overwrite (Section 6). After resolving, rerun the full
checks on the merged state.

### 19.13 Review before merge

**[Recommended]** Do not merge a branch that has not been reviewed, even if the
checks pass. Review is the cheapest place to catch the class of bugs tests miss
(Section 8). For hard concurrency/lifecycle/soundness work, run a dedicated
read-only review pass — by a reviewer agent and, where possible, by the human
owner — before merging.

### 19.14 Merge regular changes

**[Recommended]** Once a PR has been reviewed and the checks are green on the
integrated branch, merge it — do not leave finished branches dangling forever.
A merged PR closes the loop and is the cleanest possible record: "this change
was reviewed and landed." This is exactly the observability the human owner
needs; hiding a completed change in an unmerged branch buries it.

### 19.15 Keep experimental and "wisdom" PRs separate

**[Recommended]** Keep experimental, exploratory, or "capture the lesson"
changes in their own PRs, clearly labeled as such, and do not merge them into a
delivery branch. A findings report or a practice note can live in its own docs
PR; mixing it into a feature branch changes the meaning of the merge. Label
experimental PRs as drafts and close them when the experiment is over.

### 19.16 Delete stale branches and clones after merge

**[Recommended]** After a PR is merged, delete the branch on the remote and
locally, and clean up the disposable clones/worktrees. Stale branches and
clones are how two writers later collide in one tree (Section 4) and how the
human owner loses track of what is live. Keep nothing around that is not either
active work or a preserved record.

### 19.17 Never bypass reviewed PR history with force-pushes

**[Recommended]** The reviewed PR history is the record both the human owner
and later reviewers depend on. Never force-push over it — rewriting or deleting
commits that reviewers (or the human owner) have already seen silently
invalidates their review and corrupts the activity log. Force-push only in the
rare, genuinely necessary cases: fixing a branch that leaked a secret, or
rewinding an accidental push to the wrong branch — and always say so on the PR
first. When the default branch is protected (it should be), a force-push is not
even possible; rely on the PR's normal merge instead.

### 19.18 Opening and merging PRs is the default

**[Recommended]** Opening and merging GitHub PRs is the default for substantial
changes because it is the observability mechanism the human owner relies on:
every open, push, review comment, and merge is durable and human-visible, and
nothing real happens to the repository history outside a PR. Trivial emergency
fixes — a one-line hotfix to a breaking typo, a reverted bad merge — may go
directly to the default branch when speed matters more than ceremony. Everything
else flows through a PR. If a change is substantial enough that it could go
wrong, it is substantial enough for a PR.

---

## 20. Common failure modes observed

Each of these has happened. Name the failure mode when you see it forming.

### 20.1 Two write agents on a shared tree

**[Observed]** A second agent's `git checkout`, `git reset --hard`, or broad
edit destroyed another agent's in-flight work. The fixes were lost or silently
overwritten.

**Avoid:** always give writers separate clones and branches (Section 4). Before
launching any agent, know which trees are exclusively owned by whom.

### 20.2 Rushing or stopping active agents

**[Observed]** Agents stopped because they seemed slow had often been doing
exactly the right reading. The orchestrator then had to re-create their context
at a higher total cost. Repeatedly prompting a healthy agent pushed it toward
premature completion instead of completion.

**Avoid:** inspect status and the log before touching an agent; distinguish
progress from stuck; prefer a steering prompt with acceptance criteria; reserve
stop/kill for abandoned work (Sections 2–3).

### 20.3 Self-referential tests

**[Observed]** Acceptance tests written from the implementation, or by the
implementation agent, encoded its assumptions and passed while behavior
violated the contract. The independent acceptance agent caught what the
self-referential suite could not.

**Avoid:** write tests from the contract, not the code (Section 7).

### 20.4 Test-only production knobs

**[Observed]** Sub-second timing, fake output paths, and confirmation timeouts
have repeatedly crept into production code as environment variables "for the
tests." This is how real systems ship with hidden behavior no documentation
covers. Review passes have explicitly hunted for *"accidental test-only
production knobs"* and found them.

**Avoid:** in review, ask whether every knob and branch is reachable and
meaningful in production. Tests should influence timing through documented,
production-justified knobs, or by injecting at seams — not by adding hidden
production behavior that only tests use. If a knob exists for tests, reconcile
it with the real control before merging.

### 20.5 Stale docs

**[Observed]** Documentation drifted from behavior after refactors; an agent
then built on the stale text and produced code that matched the docs but not
the actual contract. The docs that were explicitly marked *authoritative*
drifted the least, precisely because drift was a recurring cost.

**Avoid:** treat docs as a deliverable in the same change that changes
behavior; update them in the same reconciliation pass; when docs and code
disagree, code is not automatically right — resolve the discrepancy
deliberately (Section 8).

### 20.6 Multiple deployment authorities

**[Observed]** More than one actor believing it can deploy is a latent
accident: a prompt that implied deploy capability, a legacy daemon outside the
managed lifecycle, or a deploy performed from a tree that had not passed
validation. The managed deploy lifecycle was introduced to make the runtime
authority single and identity-based.

**Avoid:** exactly one authority — the orchestrator — decides to deploy, and
only via the managed deploy tool from a validated checkout. No agent deploys
unless its prompt explicitly says so. Never bypass the lifecycle with manual
process signals (Sections 14–15).

### 20.7 Destructive actions before durable rollback state

**[Observed]** The worst-case sequence is: delete/stop/overwrite *first*, then
discover the replacement is broken with no recorded previous state. The fix was
a rollback design: a new deployment must be confirmed within a bounded window
or the system returns to the recorded previous known-good version — never
`rm -rf` and hope.

**Avoid:** any destructive action (replacing a worker, deleting a branch,
resetting a tree, dropping schema) is only safe when durable rollback state
exists first and you can restore it. Before destroying, record the exact
previous identity/commit/state. If you cannot say what will restore the old
state, do not destroy (Section 15).

### 20.8 Output bloat and truncated evidence

**[Observed]** Full logs and full dumps were truncated by an output limit, or
were simply too large to read, so conclusions were drawn from incomplete
output.

**Avoid:** keep commands focused; prefer log tails and `git diff --stat`;
when output is truncated, run a narrower follow-up rather than guessing
(Section 10).

### 20.9 Trusting a report of green

**[Observed]** "Tests passed" has been reported for subsets, for the wrong
branch, or for stale trees. Independent re-run on the reconciled branch is the
only trustworthy green (Section 13).

---

## 21. Reference workflow and checklist

Use this as the default for substantial repository work. Deviate only when you
can name why.

### Phase A — Prepare

1. Confirm the base commit and that the canonical tree is clean.
2. Decide responsibilities: implementation, acceptance, docs/review — separate.
3. For parallel work, create one clone (or worktree) per writer and one branch
   per agent. Record: agent title, working directory, branch, base commit, and
   intended ID. Open a draft PR for each branch (Section 19).

### Phase B — Launch

4. Launch each agent through the agent-management interface: explicit working
   directory, title, and prompt. The prompt must contain: objective,
   context/invariants, constraints, "read AGENTS.md", exact validation
   commands, completion criteria, and negative requirements (do not deploy/
   push/expose secrets/expand scope).
5. Record every returned agent ID immediately.

### Phase C — Observe (not steer)

6. Poll the agent's status; read log tails only when you need detail.
7. Do not impose time pressure on reasoning. Intervene only on evidence of
   stuckness or a changed requirement, using a precise steering prompt with
   acceptance criteria.
8. Ask agents to surface blockers/invariant violations early and to commit
   incrementally on their branch. Keep the branch pushed (Section 19).

### Phase D — Independently verify

9. When an agent finishes, read its final result.
10. Do not stop there. Read the diff yourself (`git status --short`,
    `git diff --stat`, `git diff`). Review the invariants relevant to the
    change (Section 8).
11. Run the full checks yourself on the branch (Section 13).
12. Run a read-only review pass for hard concurrency/lifecycle/soundness work.
13. If acceptance is separate, run the acceptance suite against this branch and
    attribute every failure.

### Phase E — Reconcile, review, and merge

14. Verify each contributing branch is committed, pushed, clean, and based on
    the known base commit.
15. Build an integration branch: apply trusted components in order, running the
    full checks after each addition.
16. Resolve conflicts explicitly; do not force-resolve (Sections 6, 19).
17. Run the full checks on the integrated branch. Green here is the only green
    that counts.
18. Get the change reviewed — the PR is the review surface — and only then
    merge it. Opening and merging a PR is the default (Section 19).

### Phase F — Ship (only when explicitly asked)

19. **Commit** and, only if asked, **push** — after the committed state has
    been reviewed.
20. **Deploy**, only if explicitly asked, via the project's managed deploy
    tool from the validated checkout at the exact commit.
21. Verify deployment end-to-end with a real round trip through the execution
    path: submit a sentinel job, poll to terminal, assert success/exit code
    0/exact output/executor identity (Section 16).
22. Verify the live environment contains no credential variable names (names
    only), and the config file permissions are correct (Section 17).

### Post-task

23. Keep recent sessions for follow-up (a steering continuation beats a fresh
    agent). Run a dry-run cleanup for housekeeping; never delete a running
    agent's state out from under it. Delete merged branches and disposable
    clones (Section 19).

---

## 22. Quick reference

| Situation | Do | Avoid |
| --------- | -- | ----- |
| Substantial work | launch an agent with a precise prompt | long improvised shell scripts |
| Agent looks slow | check status, then a log tail | stopping or nagging it |
| Course correction | a steering prompt with acceptance criteria | frequent steering |
| Parallel work | separate clones/worktrees + branches + mandates | two writers in one tree |
| Acceptance | contract-based tests, independent agent | tests derived from the implementation |
| Verification | read the diff, review invariants, full checks | trusting a report of green |
| Reconcile | deliberate integration branch, checks after each step | blind merge, forced resolves |
| Branches/PRs | one branch per task, draft PR early, review before merge, merge when green | unmerged dangling branches, force-pushed review history |
| Commit/push/deploy | separate, explicit, in order | conflating any two |
| Deployment | only when asked, via the managed tool, then a real smoke | implicit deploy, manual signals |
| Secrets | design them out; verify by absence | printing/dumping values |
| Destructive action | only after durable rollback state exists | delete-then-hope |
