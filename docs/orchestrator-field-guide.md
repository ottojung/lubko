# Lubko Orchestrator Field Guide

Status: living document, from the point of view of the ChatGPT/Lubko orchestrator
that has driven real development, review, deployment, and acceptance work through
Lubko. It records what has empirically worked, what has empirically failed, and
the working rules that follow.

This document is guidance for the **orchestrator**, not for repository agents.
It complements `docs/SKILL.md` (the operating manual) and `docs/protocol.md`
(the transport binding). Where this guide contradicts an earlier habit, this
guide is the correction.

## How to read this guide

Claims are tagged so you can tell hard-won observation from opinion:

- **[Observed]** — something actually happened on the Lubko system and was
  confirmed by queue results, agent logs, git history, or live worker state.
- **[Recommended]** — a rule derived from those observations. Treat these as
  defaults, not laws; every rule has a legitimate exception you must be able to
  name.

Sections 1–18 are principles. Section 19 lists failure modes that were each
observed at least once. Section 20 is the executable workflow/checklist.

---

## 1. Delegate substantial work to agents

**[Observed]** Substantial multi-step work — implementing an issue, refactoring,
investigating a test failure, writing a migration, reviewing a subsystem — has
reliably produced better results through a managed `lubko-agent` session than
through the orchestrator composing long shell command chains over the queue.

**[Observed]** The sharpest failures have come from work that needed reasoning
but was executed as a series of short, stateless queue commands. Each command
re-inspects the world from zero, accumulates no context, and cannot iterate.

**[Recommended]** Default to an agent for any task that requires judgment,
context, iteration, or more than a couple of obvious shell commands. Use the
rule: **direct shell for observation, agents for work.** Give the agent an
explicit `--cwd`, a title, and a detailed prompt (see Section 11).

**[Recommended]** Keep the orchestrator role as: decide *what* should happen,
specify *constraints*, delegate the *how*, then verify the *result* independently.
Do not ask the user to run commands Lubko can run, and do not turn repository
work back into instructions to the user.

---

## 2. Let agents think without arbitrary time pressure

**[Observed]** Agents that were `stop`ped or `kill`ed because the orchestrator
judged them "slow" had, in several cases, just spent that time on exactly the
reasoning the task required — reading the real code before editing it. Stopping
them forced the orchestrator to redo or re-verify the work later.

**[Observed]** `exit_code: -15` (SIGTERM) in finished agent metadata is the
signature of a `stop`, and it appears on sessions that were interrupted rather
than completed. When those tasks were resumed, a fresh agent re-derived context
that the interrupted agent had already built.

**[Recommended]** Do not impose deadlines on thinking. When an agent appears to
be taking long, first check `lubko-agent status <id>` and a log tail
(`lubko-agent log <id> --lines 100`). Ask "is it making progress?" not "is it
done yet?" An agent that is reading files, running tests, and converging is
working; an agent that is looping on one failing action is stuck.

**[Recommended]** Use `lubko-agent wait <id> --timeout SEC` only when you are
confident no intermediate steering is useful, and remember the timeout stops
*waiting*, not the agent. For genuinely long or uncertain tasks, poll `status`
and occasionally read the log instead of blocking.

**[Recommended]** `stop` is a decision that the task is no longer wanted, not a
pause button. Prefer `prompt` for course correction and reserve `stop`/`kill`
for abandoned tasks.

---

## 3. Inspect status and process activity rather than repeatedly steering

**[Observed]** The most over-orchestrated agents in the history are the ones
whose orchestrator sent frequent prompts ("now do X", "are you done?") without
first reading `status` or the log. Each such prompt interrupts the agent's
reasoning and can push it to declare premature completion.

**[Recommended]** Before any prompt, read the evidence:
`lubko-agent status <id>`, then `lubko-agent log <id> --lines 100` (or a
focused tail of a specific log file) when more detail is needed. Only prompt
when the evidence shows a concrete problem or a new requirement.

**[Recommended]** Steer with *constraints and acceptance criteria*, not with
play-by-play instructions. One precise follow-up that says what is wrong and
what "done" means is worth ten that say what to type next.

**[Recommended]** When you do not know an agent's ID, recover it with
`lubko-agent list` or `lubko-agent last`, and record the ID in your own state
immediately after every `new`. Never rely on `last` when multiple agents exist.

---

## 4. Use parallel agents on isolated repo clones and branches

**[Observed]** The most productive work on this system ran **multiple agents in
parallel**, each in its own clone with its own branch:

- `issue21-core` at `/tmp/lubko-issue21-core` on branch `issue21-core`;
- `issue21-docs` at `/tmp/lubko-issue21-docs` on branch `issue21-docs`;
- `issue21-acceptance` at `/tmp/lubko-issue21-acceptance` on branch
  `issue21-acceptance`;
- `issue21-integration` at `/tmp/lubko-issue21-integration` on branch
  `issue21-integration`, plus a final read-only review agent.

Each clone isolated the agents from each other's uncommitted changes and from
the live checkout.

**[Observed]** The single most common source of cross-agent corruption has been
**two write-heavy agents in the same working tree.** One agent's `git checkout`,
`git reset`, or uncommitted edit silently destroys or masks another's.

**[Recommended]** For parallel write work, always give each agent its own clone
(`git clone` to a distinct path) and its own branch. Never point two
write-capable agents at the same tree. An independent reviewer may share the
tree only if it is read-only and the tree is committed first.

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
separate, deliberate step: take the reviewed core, layer on the 
cherry-picked docs, add the acceptance tests, then fix the integration
fallout. In practice the fastest path has been a **dedicated integration
session on a dedicated branch** that cherry-picks the accepted work and runs
the full checks — not an impatient `git merge` into main.

**[Recommended]** Reconcile in this order:

1. Verify each contributing branch is committed and pushed, with a clean tree.
2. Identify the known base commit shared by all branches.
3. Build an integration branch from the most trusted component.
4. Cherry-pick or merge the other components one at a time, running the full
   checks after each addition so you can attribute any breakage.
5. Resolve conflicts explicitly; never resolve with a blind `git checkout
   --theirs` or a forced overwrite.
6. Only after the integrated branch is green do you consider main.

**[Recommended]** Do not reconcile two branches by letting one agent operate in
the other's clone. Reconcile by commits and branches, in the orchestrator's
controlled order.

---

## 7. Keep independent acceptance independent

**[Observed]** Acceptance tests written against the implementation agent's own
branch have a systematic blind spot: they encode the same assumptions the
implementation encoded. On this system the acceptance agent that produced the
best findings was explicitly told to *"independently design and implement
black-box/acceptance tests ... without relying on another agent implementation."*

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
tests did not catch: concurrency races in claim/lease/recovery, wrong process
group handling, a "fixed" deadline race that the tests' timing happened to mask,
and accidental test-only production knobs. In each case the finding came from
*reading the code and the diff* against the system's stated invariants — not
from running tests.

**[Observed]** The review agent that was told to treat *"automated tests as
evidence, not proof"* and to prioritize *"hard soundness/concurrency/state-machine/
process-lifecycle bugs"* produced findings the implementation and acceptance
agents had both missed.

**[Recommended]** After an agent reports success, do not merely relay its
summary. Read the diff. Check the invariants that matter to this codebase: for
Lubko, that means the two-column table invariant, atomic claim/finalize with
CAS, lease/heartbeat/recovery never re-executing a job, exact process-group
signals, no credentials in the worker environment, and no destructive action
before durable state exists. Tests passing is necessary, not sufficient.

**[Recommended]** Read the review checklist that says what "done" means for the
subsystem. For hard concurrency or lifecycle work, run a dedicated read-only
review pass before merging even when the implementation agent says everything
is green.

**[Recommended]** When you read the diff and find a discrepancy with an
invariant, that is a bug until proven otherwise — even if the tests pass.
Investigate to closure before reconciliation.

---

## 9. Share partial findings early

**[Observed]** The most useful findings arrived *before* the task completed:
a reviewer flagging a soundness concern while the implementation was still
in flight, an acceptance agent reporting a contract ambiguity mid-way, an
orchestrator noticing a base-commit mismatch between branches while both were
still running. Early findings changed direction cheaply.

**[Recommended]** Ask agents to report early, risky findings in their prompt:
*"If you find a blocker, a violated invariant, or a changed understanding of the
task, surface it now rather than continuing to the end."* Do not require agents
to finish before communicating.

**[Recommended]** When the orchestrator spots something mid-flight, share it
immediately with the affected agent via `prompt`, even if it means the agent
re-plans. A stopped-wrong task is cheaper than a finished-wrong task.

**[Recommended]** Keep partial progress durable: ask agents to commit
incrementally on their branch, not only at the end. A branch with frequent,
logical commits is far easier to reconcile and to salvage than one
last-minute commit.

---

## 10. Avoid low-level direct shell except for tiny deterministic observations and agent lifecycle

**[Observed]** Long, improvised shell pipelines over the queue have been a
recurring source of bloat and confusion: quoting errors, working-directory
drift, truncated output, and state lost between commands. The reliable fast
path has been `lubko-agent` for work and short, deterministic shell commands
for observation.

**[Observed]** The queue round-trip for an agent launch is itself a shell job;
the job command should be a single high-level `lubko-agent ...` invocation, not
a script.

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
lubko-agent new / status / list / prompt / log / wait / stop / kill / result
```

**[Recommended]** If a shell command needs quoting, conditionals, loops, or
coordination between several files, it is no longer an observation — delegate it
to an agent or to `lubko-agent`. Also keep output small: prefer `git status
--short`, log tails of 100–200 lines, and `sed -n '1,200p'` over full dumps.

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
3. **Location** — the working directory (also set with `--cwd`).
4. **Constraints** — what may and may not change.
5. **Local instructions** — "read and obey AGENTS.md / docs/*.md first."
6. **Validation** — the exact checks to run.
7. **Completion criteria** — the objective evidence of done.
8. **Negative requirements** — explicitly what the agent must not do (deploy,
   push, expose secrets, touch unrelated files, expand scope into a sibling
   issue, seek DB privileges, edit a file the acceptance agent owns).

**[Recommended]** Name the invariants that the agent must preserve. For Lubko,
typical invariants to state: the `lubko.jobs` two-column shape, atomic
compare-and-swap updates, never re-executing an abandoned job, exact
process-group signaling, no credentials in the process environment, and git
state only ever changed by the agent's own branch.

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
*"Baseline is core <sha> + docs <sha>, tests green; reconcile from there."*
Record the base in your own state so reconciliation can verify it.

**[Recommended]** After an agent finishes, verify `git status --short` shows
only intended changes, the intended commits exist on the branch, and the tree
was not force-reset or squashed without your knowledge. A clean, committed
branch is the contract your acceptance and review steps depend on.

---

## 13. Run the full checks

**[Observed]** Agents have repeatedly reported success on the subset of checks
they happened to run, while the full suite failed. For the Lubko repository the
complete set is fixed by `AGENTS.md` and must not be shortened:

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
and pushed to `origin/main` was treated as "deployed," and an agent that was
told to deploy performed its own push without the orchestrator reviewing the
committed state first.

**[Recommended]** Treat them as strictly ordered, separable steps:

1. **Commit** — durable local history on a branch. Cheap, reversible, safe.
2. **Push** — publishes commits to a remote (e.g. `origin/main`). Visible to
   other collaborators; only do this on an explicit instruction and after the
   committed state has been reviewed.
3. **Deploy** — replaces the running worker/daemon. Highest blast radius; only
   via `lubko-deploy`, only on explicit instruction, and only from a reviewed,
   validated checkout at an exact commit.

**[Recommended]** Name the target of each action in prompts. "Commit and push
the change" is one instruction. "Deploy the already-pushed commit" is a
*different* instruction — and in practice it has been given as a separate agent
session whose sole job was to verify the checkout at the exact commit and run
the managed deployment. Match that separation.

---

## 15. Never deploy implicitly

**[Observed]** A user request to "change the code" or "fix the bug" does not
mean "replace the running worker." Agents given a deployment-capable
environment have, absent an explicit negative, occasionally inferred that
deployment was part of "do the right thing." The fix was always cheaper as a
prevention than as a post-hoc explain.

**[Recommended]** When the user asks only for code changes: modify, run checks,
inspect the diff, summarize, stop. Deploy only when the user explicitly asks to
deploy or upgrade. Put `do not deploy` in every agent prompt where the
distinction matters, and never allow an agent to decide for itself that a
deployment is warranted.

**[Recommended]** The orchestrator must also not deploy implicitly. Deploying is
its own explicit step, performed through `lubko-deploy` (never through manual
process-tree manipulation), from a checkout that passed the full validation,
and after verifying the target commit is exactly the commit you intend to run.

---

## 16. Use queue round trips for real end-to-end verification

**[Observed]** The strongest end-to-end evidence on this system came from
inserting a **real queue job** and watching it execute in the live worker, then
reading back its status and output. The canonical smoke job is a `command` job
whose `cwd` is the repository root and whose command is a fixed sentinel; the
result must show `state.status = succeeded`, `exit_code = 0`, the expected
`stdout`, and a `worker_id` identifying the live worker.

**[Observed]** Simulated or mocked round trips have, more than once, passed
while the real queue path failed — for example a worker started with the wrong
working directory, or a deployment that verified "the process started" but not
that it could reach the database.

**[Recommended]** For any change to the worker, queue behavior, or deployment
lifecycle, verify with a real queue round trip after deployment:

1. Insert a `command` job with a distinctive sentinel output.
2. Poll the job to terminal state.
3. Assert `succeeded`, `exit_code 0`, exact expected stdout, and a
   `worker_id`/`process_pid`/`process_pgid` consistent with the newly deployed
   worker.

**[Recommended]** Remember the two lifecycles are distinct: the Supabase job
that launched an agent may finish while the agent continues. Verify agent work
through the **agent ID**, and verify worker behavior through the **queue job**.

---

## 17. Design around secrets

**[Observed]** The worker environment has been scrubbed of `PG*` variables and
`DATABASE_URL`; connection settings and credentials live in a
permission-restricted file (`mode 0600`), and `lubko-deploy` strips
credential-bearing variables from the environment it hands to a deployed
worker. This was a deliberate, tested design — not an accident.

**[Observed]** Agents that printed, echoed, or dumped environment variables
while debugging have been a persistent risk. The prompt discipline that worked
was: never emit secret *values*; inspect variable *names*, permissions, and
counts only. Verification of the file was done "using metadata/counts only" and
credentials were migrated "without ever emitting secret contents."

**[Recommended]** State the secret contract in prompts: no values in logs, no
values in commit content, no `echo $VAR`, no `env | ...`, no connection strings
in output. Verify secret handling by checking *absence*: assert the live worker
environment contains no credential variable names (check names only), the
config file mode is correct, and git history contains no secret. A secrets
leak discovered in git history is near-permanent; treat it as the worst class of
failure.

**[Recommended]** Do not put credentials or server identifiers in this guide,
in prompts, or in commits. Design the system so that secrets are never needed
in an instruction.

---

## 18. Parallelism rules that have held

**[Observed]** Parallelism works when isolation is real and mandates are
disjoint; it fails when agents share state. The rules below have held
repeatedly:

- Separate clones for separate writers; a reviewer may share only a
  committed, read-only tree.
- One agent per responsibility: implementation, acceptance, docs/review.
- Every agent has a recorded ID, a title, an explicit `--cwd`, and a
  single-mandate prompt.
- The orchestrator keeps the map: which branch, which base commit, which
  responsibility, which ID.
- No agent relies on `last`; no two agents assume the same exclusive tree.
- Reconciliation is deliberate and done by the orchestrator (Section 6).

---

## 19. Common failure modes observed

Each of these has happened. Name the failure mode when you see it forming.

### 19.1 Two write agents on a shared tree

**[Observed]** A second agent's `git checkout`, `git reset --hard`, or broad
edit destroyed another agent's in-flight work. The fixes were lost or silently
overwritten.

**Avoid:** always give writers separate clones and branches (Section 4). Before
launching any agent, know which trees are exclusively owned by whom.

### 19.2 Rushing or stopping active agents

**[Observed]** Agents `stop`ped (exit `-15`) because they seemed slow had often
been doing exactly the right reading. The orchestrator then had to re-create
their context at a higher total cost. Repeatedly `prompt`ing a healthy agent
pushed it toward premature completion instead of completion.

**Avoid:** inspect `status` and the log before touching an agent; distinguish
progress from stuck; prefer `prompt` with acceptance criteria; reserve
`stop`/`kill` for abandoned work (Sections 2–3).

### 19.3 Self-referential tests

**[Observed]** Acceptance tests written from the implementation, or by the
implementation agent, encoded its assumptions and passed while behavior
violated the contract. The independent acceptance agent caught what the
self-referential suite could not.

**Avoid:** write tests from the contract, not the code (Section 7).

### 19.4 Test-only production knobs

**[Observed]** Sub-second timing, fake-worker output paths, and confirmation
timeouts have repeatedly crept into production code as environment variables
"for the tests." This is how real systems ship with hidden behavior no
documentation covers. Review passes have explicitly hunted for *"accidental
test-only production knobs"* and found them.

**Avoid:** in review, ask whether every knob and branch is reachable and
meaningful in production. Tests should influence timing through documented,
production-justified knobs, or by injecting at seams — not by adding
hidden production behavior that only tests use. If a knob exists for tests,
reconcile it with the real control before merging.

### 19.5 Stale docs

**[Observed]** Documentation drifted from behavior after refactors; an agent
then built on the stale text and produced code that matched the docs but not
the actual contract. The protocol document is marked *authoritative* precisely
because drift was a recurring cost.

**Avoid:** treat docs as a deliverable in the same change that changes
behavior; update them in the same reconciliation pass; when docs and code
disagree, code is not automatically right — resolve the discrepancy
deliberately (Section 8).

### 19.6 Multiple deployment authorities

**[Observed]** More than one actor believing it can deploy is a latent
accident: a prompt that implied deploy capability, a legacy daemon outside the
managed lifecycle, or a deploy performed from a tree that had not passed
validation. The managed lifecycle (`lubko-deploy`) was introduced to make the
worker authority single and identity-based.

**Avoid:** exactly one authority — the orchestrator — decides to deploy, and
only via `lubko-deploy` from a validated checkout. No agent deploys unless its
prompt explicitly says so. Never bypass the lifecycle with manual process
signals (Sections 14–15).

### 19.7 Destructive actions before durable rollback state

**[Observed]** The worst-case sequence is: delete/stop/overwrite *first*, then
discover the replacement is broken with no recorded previous state. This
motivated rollback design where a new deployment must be confirmed within a
bounded window or the system automatically returns to the recorded previous
known-good version — never `rm -rf` and hope.

**Avoid:** any destructive action (replacing a worker, deleting a branch,
resetting a tree, dropping schema) is only safe when durable rollback state
exists first and you can restore it. Before destroying, record the exact
previous identity/commit/state. If you cannot say what will restore the old
state, do not destroy (Section 15).

### 19.8 Output bloat and truncated evidence

**[Observed]** Full logs and full dumps were truncated by the worker's output
limit, or were simply too large to read, so conclusions were drawn from
incomplete output.

**Avoid:** keep commands focused; prefer log tails and `git diff --stat`;
when output is truncated, run a narrower follow-up rather than guessing
(Section 10).

### 19.9 Trusting a report of green

**[Observed]** "Tests passed" has been reported for subsets, for the wrong
branch, or for stale trees. Independent re-run on the reconciled branch is the
only trustworthy green (Section 13).

---

## 20. Reference workflow and checklist

Use this as the default for substantial repository work. Deviate only when you
can name why.

### Phase A — Prepare

1. Confirm the base commit and that the canonical tree is clean.
2. Decide responsibilities: implementation, acceptance, docs/review — separate.
3. For parallel work, create one clone per writer and one branch per agent.
   Record: agent title, `--cwd`, branch, base commit, and intended ID.

### Phase B — Launch

4. Launch each agent with `lubko-agent new --cwd ... --title ... --prompt ...`.
   The prompt must contain: objective, context/invariants, constraints,
   "read AGENTS.md", exact validation commands, completion criteria, and
   negative requirements (do not deploy/push/expose secrets/expand scope).
5. Record every returned agent ID immediately.

### Phase C — Observe (not steer)

6. Poll `lubko-agent status <id>`; read log tails only when you need detail.
7. Do not impose time pressure on reasoning. Intervene only on evidence of
   stuckness or a changed requirement, using precise `prompt` with acceptance
   criteria.
8. Ask agents to surface blockers/invariant violations early and to commit
   incrementally on their branch.

### Phase D — Independently verify

9. When an agent finishes, read `lubko-agent result <id>`.
10. Do not stop there. Read the diff yourself (`git status --short`,
    `git diff --stat`, `git diff`). Review the invariants relevant to the
    change (Section 8).
11. Run the full checks yourself on the branch (`ruff format --check`,
    `ruff check`, `mypy`, `pytest` for this repo).
12. Run a read-only review pass for hard concurrency/lifecycle/soundness work.
13. If acceptance is separate, run the acceptance suite against this branch and
    attribute every failure.

### Phase E — Reconcile

14. Verify each contributing branch is committed, pushed, clean, and based on
    the known base commit.
15. Build an integration branch: apply trusted components in order, running the
    full checks after each addition.
16. Resolve conflicts explicitly; do not force-resolve.
17. Run the full checks on the integrated branch. Green here is the only green
    that counts.

### Phase F — Ship (only when explicitly asked)

18. **Commit** and, only if asked, **push** — after the committed state has been
    reviewed.
19. **Deploy**, only if explicitly asked, via `lubko-deploy` from the validated
    checkout at the exact commit.
20. Verify deployment end-to-end with a real queue round trip: insert a
    sentinel `command` job, poll to terminal, assert `succeeded`/`exit_code 0`/
    exact stdout/`worker_id` (Section 16).
21. Verify the live worker environment contains no credential variable names
    (names only), and the config file permissions are correct (Section 17).

### Post-task

22. Keep recent sessions for follow-up (`prompt` continuation beats a fresh
    agent). Run `lubko-agent clean --dry-run` for housekeeping; never delete a
    running agent's state out from under it.

---

## 21. Quick reference

| Situation | Do | Avoid |
| --------- | -- | ----- |
| Substantial work | `lubko-agent new` with a precise prompt | long improvised shell scripts |
| Agent looks slow | `status`, then a log tail | stopping or nagging it |
| Course correction | `prompt <id>` with criteria | frequent steering |
| Parallel work | separate clones + branches + mandates | two writers in one tree |
| Acceptance | contract-based tests, independent agent | tests derived from the implementation |
| Verification | read the diff, review invariants, full checks | trusting a report of green |
| Reconcile | deliberate integration branch, checks after each step | blind merge, forced resolves |
| Commit/push/deploy | separate, explicit, in order | conflating any two |
| Deployment | only when asked, via `lubko-deploy`, then a real queue smoke | implicit deploy, manual signals |
| Secrets | design them out; verify by absence | printing/dumping values |
| Destructive action | only after durable rollback state exists | delete-then-hope |
