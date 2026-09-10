# Repository governance contract

This document is the version-controlled change-integrity contract for `main` and active `release/*` branches. It defines the development workflow operators and orchestrators must follow.

Repository-hosting configuration is deliberately outside this contract. Development must not create, modify, require, audit, or treat as acceptance criteria GitHub rulesets, branch protection, required-status-check settings, merge-policy settings, repository permissions, GitHub Actions repository settings, or other out-of-band Git/GitHub configuration. See `docs/intent-records/repository-configuration-out-of-scope.md`.

External repository configuration may exist and may independently reinforce this workflow, but Lubko development must neither depend on nor inspect it for correctness or completion.

## `main` integrity contract

For the default branch `main`:

1. **Use pull requests.** Ordinary development changes reach `main` through a pull request so the diff, review, and integration evidence are visible before merge.
2. **Canonical CI must pass on current integration.** The single canonical check is the `test` job in `.github/workflows/ci.yml`. Before merge, the pull request must include the current base-branch state and the canonical check must succeed on that current integration.
3. **No procedural bypass.** The ability of a user, bot, API client, or repository host to perform some update does not authorize bypassing this version-controlled workflow. Exceptions require an explicit user instruction for the particular operation; they are not inferred from repository-hosting capabilities or settings.
4. **One test-suite command.** The `test` job gates frozen sync, formatting, lint, strict types, and the complete pytest suite. `uv run pytest` remains the single complete test-suite command developers run locally. Installation and environment acceptance checks are separate and are not part of the pytest wall-clock budget.

## What the canonical check covers

The `test` job runs, in order, the same checks developers run locally:

- `uv sync --frozen --extra dev` — frozen dependency lock (runtime + development).
- `uv run ruff format --check .` — formatting.
- `uv run ruff check .` — linting.
- `uv run mypy .` — strict type checking.
- `uv run pytest` — the complete test suite (subject to the testing requirements in `AGENTS.md`).

Installation, environment provisioning, dependency installation, image construction, and similar acceptance checks are outside the pytest budget and may take longer. They are not part of the canonical `uv run pytest` suite.

## Why the PR path

A pull request is the canonical review and integration surface. It lets the orchestrator inspect the exact diff through the GitHub plugin, makes CI evidence visible before integration, and provides a durable record of review and merge decisions.

The workflow must establish that the integration being accepted includes the current base branch and has passing canonical verification. Do not substitute assumptions about branch protection or other repository-hosting configuration for that verification.

GitHub may create a fresh merge or squash commit when a PR is merged, so the literal final commit SHA need not be the exact SHA on which CI ran. The invariant is that the reviewed integration included current base state and passed the canonical check before merge.

## Canonical check name

The canonical CI job is named `test`. Renaming it is a version-controlled governance change and must update any repository documentation or tooling that refers to that name. It does not require or imply any out-of-band repository-setting change as part of Lubko development.

# Release-branch integrity contract

Scheduled Lubko development integrates through the active `release/*` branch as described by `docs/skills/itenerary-lubko.md`. The release branch follows the same version-controlled integrity policy as `main`.

1. **Use pull requests for advancement.** After an active release branch has been created from `main`, subsequent changes reach it through pull requests.
2. **Canonical CI must pass on current integration.** The same `test` job must succeed on an integration that includes the current release-branch state before an issue PR is merged.
3. **Use the same validation pipeline.** Release integration uses frozen sync, formatting, lint, strict types, and the complete pytest suite described above.
4. **Branch creation is the bootstrap step.** If no active release branch exists, it may be created directly from current `main`; after creation, advancement follows the PR + canonical-verification path.
5. **Do not depend on hosting configuration.** Never inspect, create, require, or wait for a ruleset, branch-protection rule, repository permission, required-check setting, or other external GitHub configuration in order to start, continue, or complete release work.

These requirements are development procedure expressed in version-controlled repository contents. They remain applicable regardless of what repository-hosting configuration happens to exist outside the repository.
