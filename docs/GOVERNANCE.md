# Repository governance contract

This document is the change-integrity contract for the default branch. It is
the source of truth for how a commit becomes the tip of `main`; every other
place that describes branch protection must stay consistent with it.

Lubko validates one canonical pipeline. That pipeline is the only thing that
may authorize `main` to advance, and it is enforced by a GitHub ruleset, not by
convention or by CI merely reporting a failure after `main` has already moved.

## The contract

For the default branch (`main`, `~DEFAULT_BRANCH` in the ruleset condition):

1. **No direct pushes.** Every change reaches `main` through a pull request.
   The ruleset's `pull_request` rule blocks updates that are not made via a
   merged PR.
2. **Canonical CI must pass.** The single canonical check, the `test` job in
   `.github/workflows/ci.yml`, must complete successfully on the commit that is
   merged into `main`. The ruleset's `required_status_checks` rule blocks a
   merge whose head commit lacks a passing `test` check.
3. **No silent bypass.** The ruleset has no bypass actors. Administrators and
   bots cannot override the contract through a normal path. The only way around
   it is the explicit emergency procedure below, which is itself a visible,
   reviewed ruleset change.
4. **One test command.** The governance rule does not introduce a second test
   command. The `test` job runs exactly `uv run pytest`, which is the complete
   suite; the same command developers run locally.

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
canonical CI runs on the exact commit that will become `main`'s tip and that
the result is visible before the merge. Direct pushes would let `main` move
before CI reports, which is exactly the gap this contract closes. Reliability,
not convenience, drives the choice.

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
