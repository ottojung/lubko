# Repository governance contract

This document is the change-integrity contract for `main` and active `release/*`
branches. It is the source of truth for how a commit becomes the tip of `main`
or of an active release branch; every other place that describes branch
protection must stay consistent with it.

Lubko validates one canonical pipeline. That pipeline is the only thing that
may authorize `main` to advance, and it is enforced by a GitHub ruleset, not by
convention or by CI merely reporting a failure after `main` has already moved.

## The contract

For the default branch (`main`, `~DEFAULT_BRANCH` in the ruleset condition):

1. **No direct pushes.** Every change reaches `main` through a pull request.
   The ruleset's `pull_request` rule blocks updates that are not made via a
   merged PR.
2. **Canonical CI must pass on an up-to-date integration.** The single
   canonical check, the `test` job in `.github/workflows/ci.yml`, must complete
   successfully, and the pull request's integration must be up to date with the
   base branch before merge. The ruleset's `required_status_checks` rule (with
   strict policy enabled) blocks a merge unless a passing `test` check exists on
   an integration that includes the latest base-branch state.
3. **No silent bypass.** The ruleset has no bypass actors. Administrators and
   bots cannot override the contract through a normal path. The only way around
   it is the explicit emergency procedure below, which is itself a visible,
   reviewed ruleset change.
4. **One test-suite command.** The governance rule does not introduce a second
   test command. The `test` job additionally gates frozen-sync, formatting,
   lint, and strict types, but `uv run pytest` remains the single, complete test
   suite command — the same command developers run locally.

This is deliberately fail-closed: if the required `test` check cannot be found
on a commit (because the job was renamed or removed), no commit can satisfy the
rule, so merges are blocked rather than silently allowed.

## What the canonical check covers

The `test` job runs, in order, the same checks developers run locally:

- `uv sync --frozen` — frozen dependency lock.
- `uv run ruff format --check .` — formatting.
- `uv run ruff check .` — linting (Ruff `ALL`, preview).
- `uv run mypy .` — strict type checking.
- `uv run pytest` — the complete test suite.

## Why the PR path

The contract requires a pull request rather than permitting unrestricted direct
pushes. A pull request is the only supported update path that guarantees the
canonical CI runs on the integration and that the result is visible before the
merge. With strict status-check policy, the integration must also be up to date
with the base branch, so a PR whose base advanced after its last green check
cannot merge until CI passes again on current state. Direct pushes would let
`main` move before CI reports, which is exactly the gap this contract closes.
Reliability, not convenience, drives the choice.

Note that GitHub may create a fresh merge or squash commit when the PR is
merged; the literal final commit SHA is not necessarily the commit that CI
executed. The enforced invariant is therefore that the PR/integration was up to
date with the base branch and that canonical CI passed on it before merge — not
that the merged commit's exact SHA itself ran CI.

## Required check name is part of the contract

The rule references the check by the literal name `test`. Renaming the job in
`.github/workflows/ci.yml` is a governance change: it must be paired with an
update to the ruleset's `required_status_checks` entry, or enforcement will
break fail-closed. Do not rename the job casually.

## Emergency procedure

The contract may be suspended only through an explicit, visible ruleset edit,
never through a hidden override:

1. Set the `main1` ruleset enforcement to `disabled` (or temporarily remove the
   `required_status_checks` / `pull_request` rules) via the repository ruleset
   API or settings UI.
2. Make the urgent change through whatever path the suspension permits.
3. Immediately restore the `main1` ruleset to `active` with the full rule set
   before `main` is next advanced through any non-emergency change.

The suspension and its reversal must each be their own auditable change. The
contract is never bypassed silently; a bypass is always a deliberate,
recoverable ruleset modification.

## Where it lives

The enforcement is configured in the active `main1` repository ruleset
(target `branch`, default-branch ref condition). It carries the rules
`deletion`, `non_fast_forward`, `pull_request`, and `required_status_checks`.

# Release-branch integrity contract

This section extends the change-integrity contract to active `release/*`
branches. The same class of protection that guards `main` must also guard the
branches where release integration occurs.

## The contract

For branches matching `release/*`:

1. **No direct pushes.** Every change reaches a release branch through a pull
   request. The `release1` repository ruleset's `pull_request` rule blocks
   updates that are not made via a merged PR. Direct pushes are prohibited
   entirely — they are not validated after the fact, they are rejected.
2. **Canonical CI must pass on an up-to-date integration.** The same `test` job
   in `.github/workflows/ci.yml` that gates `main` must complete successfully
   on the release-branch integration. The ruleset's `required_status_checks`
   rule (with strict policy enabled) blocks a merge unless a passing `test`
   check exists on an integration that includes the latest base-branch state.
3. **No silent bypass.** The ruleset has no bypass actors. Administrators and
   bots cannot override the contract through a normal path.
4. **Same test command.** The release branch uses the identical canonical
   validation pipeline as `main`: frozen sync, formatting, lint, strict types,
   and the complete test suite.
5. **Branch creation is not blocked.** The ruleset is created with
   `do_not_enforce_on_create: true`. A new release branch can be created from
   `main` without passing CI first or requiring a PR; once the branch exists,
   all subsequent advancement requires the PR + CI path.

## Why a separate ruleset

A dedicated `release1` ruleset (rather than extending `main1`) keeps the
release-branch contract auditable as its own ruleset. The `release/*` ref
pattern targets all active release branches, and `do_not_enforce_on_create`
preserves the branch-creation bootstrap path without creating a loophole for
subsequent unvalidated advancement.

## Required check name

The ruleset references the same `test` check name as the `main1` ruleset.
Renaming the job in `.github/workflows/ci.yml` is a governance change that
must be paired with an update to both `main1` and `release1` rulesets.

## Where it lives

The enforcement is configured in the active `release1` repository ruleset
(target `branch`, ref pattern `release/*`). It carries the rules `pull_request`
and `required_status_checks` (strict, with `do_not_enforce_on_create: true`).
