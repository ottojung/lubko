# Code Review and Quality

## Purpose

Review the resulting implementation, not merely whether automation accepts it. Find concrete defects, missing requirements, unintended regressions, avoidable complexity, legacy compatibility machinery, and meaningful performance problems.

The review is read-only. Report findings and proposed remedies; do not modify the code unless the user separately asks for fixes.

## Operating Assumptions

Assume the submitted revision passes all automated tests and every CI check, including build, lint, formatting, type checking, and static analysis.

- Do not rerun automated checks.
- Do not inspect or report CI status.
- Do not use passing automation as proof that the implementation is correct or complete.
- Read tests as code and as evidence about intended behaviour. Assess whether they test the right behaviour, but do not execute them.

Security is outside the scope of this skill. Do not perform or report a security review unless the user explicitly requests one separately.

## Review Dimensions

Every review evaluates the change across five dimensions.

### 1. Soundness

For the behaviour that is implemented, is the implementation correct in principle?

Check:

- Does each execution path produce the intended result?
- Are invariants established, preserved, and consumed consistently?
- Are boundary cases, empty inputs, exceptional states, and error paths handled coherently?
- Are state transitions valid and complete?
- Are ordering, lifetime, concurrency, ownership, and resource assumptions valid?
- Are types and representations used according to their actual contracts?
- Does the implementation rely on an assumption that its callers or dependencies do not guarantee?
- Would a concrete input or sequence of events produce incorrect behaviour?

A passing test suite does not establish soundness. Trace the relevant behaviour through the implementation and its surrounding code.

### 2. Completeness

Compared with the requested implementation, is anything missing?

Check:

- Is every explicit requirement implemented?
- Are all required cases, states, entry points, consumers, and integrations covered?
- Did the change update every representation of the affected concept?
- Are required migrations, documentation, configuration, or tests missing?
- Do the tests demonstrate all important requested behaviours, rather than merely exercising the implementation?
- Did the implementation silently narrow or reinterpret the request?

Assess completeness against the actual task, issue, specification, or pull-request description. Tests supplement that contract; they do not replace it.

If no reliable specification is available, state that completeness could not be assessed. Do not invent requirements and report their absence as defects.

### 3. Regression Safety

Does the change unintentionally damage currently supported behaviour?

Check:

- Does it alter a current public contract outside the requested scope?
- Does it break a current caller, consumer, stored representation, or integration that remains supported?
- Does it change defaults, ordering, error semantics, or side effects unintentionally?
- Does a refactor preserve the intended semantics of the code it replaces?
- Does new behaviour leak into unrelated paths or modes?
- Does the change include unrelated behaviour or scope creep?

Regression safety protects the current intended contract. It does **not** justify preserving obsolete behaviour. Backward compatibility is governed by the clean-break policy below.

### 4. Design and Maintainability

Is the resulting code the clearest coherent design for the requested behaviour?

Check:

- Is control flow direct and easy to follow?
- Do names express domain concepts and invariants precisely?
- Are module boundaries and dependency directions coherent?
- Are abstractions earning their complexity?
- Does a refactor remove concepts and branches rather than merely move them?
- Are repeated conditionals exposing a missing model, state, or dispatcher?
- Is feature-specific logic placed in the component that owns the concept?
- Is a new helper duplicating an existing canonical operation?
- Are type boundaries explicit, without casts, optionality, or silent fallbacks hiding unclear invariants?
- Is there dead, unreachable, superseded, or pass-through code?
- Is the design less elegant only because of historical decisions?

Codebase consistency is not a defence for avoidable complexity. When an existing pattern is historically accidental, awkward, or needlessly indirect, require the touched design to be simplified rather than reproducing the pattern.

Do not report formatting, stylistic preference, or matters already enforced mechanically unless they create genuine ambiguity or maintenance cost.

### 5. Performance

Does the change introduce a meaningful scalability, latency, memory, I/O, or resource-usage regression?

Check:

- Does work grow unexpectedly with input size?
- Are there N+1 operations, repeated traversals, or redundant computation?
- Are operations unbounded where the supported domain requires a bound?
- Is expensive work introduced into a hot path?
- Are large objects, copies, queries, renders, or network operations performed unnecessarily?
- Does the design prevent streaming, batching, caching, or incremental work where those properties are required?

Report performance findings only when the affected path and likely impact are concrete. Do not report speculative micro-optimizations.

## Clean-Break Policy: Reject Backward Compatibility by Default

Cleanliness, simplicity, and a single canonical design take priority over legacy support.

Unless the task clearly and explicitly requires backward compatibility, treat every compatibility mechanism as an **Error** that must be removed. Do not infer a compatibility requirement from repository history, existing data, old tests, comments, version numbers, or the mere existence of old callers.

Flag all of the following aggressively:

- Legacy formats, schemas, protocols, algorithms, or configuration shapes
- Old and new implementations kept side by side
- Compatibility shims, adapters, aliases, deprecated entry points, and forwarding wrappers
- Fallback parsing or tolerant acceptance of obsolete inputs
- Dual reads, dual writes, format detection, and version-dependent branches
- Silent defaults or exception handling that preserve historical behaviour
- Feature flags whose purpose is to retain the replaced implementation
- Parameters, types, states, or branches that exist only for older callers
- Comments such as “for backward compatibility,” “legacy,” “temporary,” or “remove later”
- Less elegant designs justified only by how the system used to work

Treat compatibility machinery in the affected execution path as endorsed by the change even when it predates the diff. If the change touches that path, require the obsolete machinery to be deleted and the design beautified.

The desired result is one representation, one algorithm, one path, and one explicit contract. Prefer a clean break over negotiation between generations of the system.

Backward compatibility is allowed only when the request unambiguously identifies the legacy contract that must remain supported. When it is allowed:

- Keep it isolated from the canonical implementation.
- Minimize its surface and lifetime.
- Do not let it distort the main model or control flow.
- If conversion is required, prefer an explicit one-time migration over permanent runtime compatibility.

## Review Procedure

### 1. Establish the Contract

Read the task, issue, specification, pull-request description, and relevant project documentation. Identify:

- Requested behaviour
- Explicit non-goals
- Current behaviour intended to remain supported
- Any explicitly required compatibility contract

If information is missing, distinguish an open question from a defect. Never manufacture a requirement to justify a finding.

### 2. Map the Change

Use the diff to identify the affected surface, then inspect the resulting files and enough surrounding code to understand the real execution paths, callers, data flow, and invariants.

The diff is an index, not a scope wall. A finding may depend on unchanged code when the change interacts with it. Do not judge a hunk in isolation.

### 3. Read Tests as Evidence

Read relevant tests before or alongside the implementation to understand claimed behaviour.

Ask:

- Do the assertions express the requested external behaviour?
- Would they fail if the implementation contained the suspected defect?
- Are important requirements or cases absent?
- Are tests coupled to implementation details in a way that permits incorrect behaviour?

Do not run the tests. Their passing status is assumed.

### 4. Trace the Implementation

Walk the affected behaviour through all five dimensions. Inspect callers and consumers when necessary. For each potential finding, identify the concrete trigger and follow it to the observable result.

### 5. Perform the Legacy Sweep

Search the affected path for compatibility mechanisms, legacy representations, fallbacks, historical branches, deprecated APIs, and duplicated old/new algorithms. Apply the clean-break policy even when the machinery is pre-existing.

### 6. Validate Every Finding

Before reporting a finding, verify all of the following:

1. The relevant code exists in the reviewed revision.
2. The finding is caused, exposed, perpetuated, or left unnecessarily in place by the change.
3. A concrete input, state, caller, or execution path demonstrates the problem.
4. Surrounding code does not already prevent or handle it.
5. The impact is material enough to require a code change.
6. The proposed remedy addresses the cause rather than masking the symptom.

Suppress the finding if these conditions cannot be established. “This might fail” is not a finding without a plausible failure path.

Legacy compatibility is itself a concrete design error under this skill; it does not require demonstrating a runtime failure.

### 7. Report, Do Not Fix

Present the findings first. Do not edit code, delete files, post review comments, or implement remedies unless the user explicitly asks for a separate fixing pass.

## Finding Classification

Use only these classifications:

| Classification | Meaning | Merge consequence |
|---|---|---|
| **Error** | Incorrect, incomplete, regressive, materially inefficient, unnecessarily complex, or legacy-compatible behaviour | Must be fixed before approval |
| **Suggestion** | A genuinely optional improvement with a clear benefit | Does not block approval |

Do not produce nits. If a comment does not identify a concrete defect or a worthwhile optional improvement, omit it.

Compatibility mechanisms forbidden by the clean-break policy are always **Error**, never Suggestion.

## Evidence Required for a Finding

Every finding must include:

- The smallest relevant file and line or line range
- Its review dimension
- The concrete input, state, caller, or execution path that triggers it
- The resulting incorrect, missing, regressive, complicated, or inefficient behaviour
- Why surrounding code or tests do not already address it
- A specific required change or structural remedy

Prefer a few high-confidence findings over a long speculative list.

## Structural Remedies

When reporting a design problem, propose the simplifying move:

- Delete the legacy representation, parser, algorithm, branch, or adapter.
- Replace parallel old/new paths with one canonical path.
- Collapse duplicated branches into a single flow.
- Replace repeated conditionals with an explicit model or dispatcher.
- Separate orchestration from domain logic.
- Move feature logic into the module that owns the concept.
- Reuse the canonical operation instead of adding a near-duplicate.
- Make an invariant or type boundary explicit so downstream branching disappears.
- Delete pass-through wrappers and obsolete abstractions.
- Extract a focused component when it removes concepts from the caller.

Prefer the remedy that deletes moving pieces. Do not accept “historical reasons,” “existing callers might rely on it,” or “we can clean it up later” unless preserving the identified legacy behaviour is an explicit requirement.

## Output Format

Start directly with findings. Do not begin with praise, a summary of the diff, or a narration of the review process.

```markdown
## Findings

1. **Error — Short descriptive title**
   - Location: `path/to/file.ext:LINE`
   - Dimension: Soundness | Completeness | Regression safety | Design and maintainability | Performance
   - Trigger: Concrete input, state, caller, or execution path
   - Result: Observable defect or unwanted complexity
   - Evidence: Why the current implementation permits it
   - Required change: Specific correction or simplification

2. **Suggestion — Short descriptive title**
   - Location: `path/to/file.ext:LINE`
   - Dimension: ...
   - Benefit: Concrete optional improvement
   - Suggested change: Specific remedy


## Open Questions

- Include only questions whose answers are necessary to assess soundness or completeness.

## Verdict

**Request changes** | **Approve**

## Agent prompt

- Write a prompt for an AI agent that instructs it how to address the review.
- Assume that the agent tends to be lazy and is not very intelligent, so try to design the prompt with that in mind.

```

If there are no findings, write:

```markdown
## Findings

No findings.

## Verdict

**Approve**
```

If completeness could not be assessed, state that immediately after the findings and name the missing specification source. Do not treat the uncertainty itself as an Error.

Any Error requires **Request changes**. Suggestions alone do not block approval.

## Out of scope

For the review, ignore and do not inspect these things:
- PR description
- Commit messages
- CI status

The reason is simple: these are not reliably accessible and/or fixable by the agent.
So just save time and not consider them at all.

## Review Standard

- Findings are about code, never people.
- Technical evidence overrides convention and historical precedent.
- Existing architecture may be criticized when the change perpetuates avoidable complexity.
- Passing tests and CI are premises, not proof.
- Current supported behaviour deserves regression protection; obsolete behaviour does not.
- Clean design is the default. Legacy support must justify its existence explicitly.
- A clean review with no findings is better than speculative feedback.

## Lifecycle/supervisor state safety

Lubko's committed test suite redirects every XDG-backed state root to pytest-owned
temporary directories before any test runs, and asserts the whole session never
touches ambient (production-like) state or processes. Reviews must preserve and
rely on that isolation, and must treat any lifecycle/supervisor test or helper
that could write ambient user state as an Error.

The same rule applies to manual orchestration and review experiments: never
mutate the ambient Lubko lifecycle/supervisor state tree. Durable
state-mutating experiments against `lubko-deploy`, the `lubko-supervisor`
daemon, `worker/meta.json`, `worker/rollback.json`, or `supervisor/*.json` must
run with explicit temporary XDG roots (for example `XDG_STATE_HOME=$(mktemp -d)`
or a dedicated scratch container), never against the live environment's state.
Ordinary worker restarts and probes run from the sealed per-commit runtime and
must remain functional even when the source checkout is modified or deleted;
there is no supported in-environment stopped state, and the only supported way
to fully stop Lubko is to stop its container/environment.
