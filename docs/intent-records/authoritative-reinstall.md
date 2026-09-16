$id-7092185346721184
title: Reinstallation is authoritative over prior Lubko runtime state
date: 2026/09/15
source: @ottojung
kind: requirement

`lubko-install --repo <checkout>` is expected to be safe and normal to run on every Lubko container start. The checkout supplied to `--repo` is the cold-start source of truth for the Lubko version that should be installed and started.

Installation must reconcile Lubko-managed installed/runtime state to that checkout even when durable state from a previous run names a different commit. A stale `cli/current`, supervisor desired/applied state, previous deployment state, startup definition, launcher installation, or other Lubko-managed installation artifact must not by itself cause `lubko-install` to refuse a version-changing reinstall. If the previous persisted state disagrees with the supplied checkout, successful installation supersedes or repairs that managed state so startup can continue from the supplied checkout.

This is intentional even when it changes version in either direction. A live deployment may therefore be superseded on a later cold start if the boot-time checkout still names another commit. The component that chooses the boot-time checkout is responsible for choosing the version to recover to after restart.

`lubko-install` should be restorative and repeatable rather than incremental: every invocation must recreate, overwrite, verify, or otherwise deterministically reconcile all Lubko-managed installation artifacts whose contents or authority are derived from the selected checkout. It must not rely on an earlier successful install having left those artifacts intact.

The source Git checkout itself is input and must not be rewritten by installation. Likewise, unrelated user-owned data such as credentials, workspaces, project repositories, and logs are not installation targets merely because they share the same persistent home or state volume.

An actually live concurrent lifecycle authority may require serialization or coordinated replacement for process-safety. That is a concurrency requirement, not permission for stale durable state from a dead previous run to veto reinstalling the selected checkout.
