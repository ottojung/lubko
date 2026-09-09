$id-3816759021448763
title: Repository-hosting configuration is outside development scope
date: 2026/09/08
source: @ottojung
kind: constraint

Development of Lubko is limited to version-controlled repository contents and the behavior of software built from them. Git and GitHub repository administration outside those contents is out of scope. Development work must not create, modify, require, audit, or treat as acceptance criteria any out-of-band repository configuration, including GitHub rulesets, branch protection, required-status-check settings, merge policies, repository permissions, GitHub Actions repository settings, or local/server-side Git configuration.

Such external configuration may exist and may be managed by repository owners independently, but Lubko development must not depend on it for correctness or completion. In particular, a development task must not require creation of a GitHub ruleset or branch-protection rule as a condition of completion. If an invariant matters to Lubko development, it must be expressed and enforced through version-controlled artifacts or application behavior within repository scope, or else explicitly remain an external operational concern.
