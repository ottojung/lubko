# Supervised self-deployment protocol

`lubko-deploy-ctl` is the stable control plane for changing the running Lubko commit after a maintained worker already exists. A fresh installation can still establish its first maintained worker with `lubko-deploy`; later version changes should use this supervised protocol.

## Checkout

Submit one request through the queue:

```json
{"type":"checkout","commit":"<exact 40-character commit>"}
```

The controller requires the exact commit to exist locally and uses the commit recorded in the currently running maintained-worker metadata as the rollback baseline. Repository `HEAD` is not a rollback authority.

The candidate is first spawned behind a stable pipe gate. It cannot consume `lubko.jobs` yet. Before the old worker is stopped or the candidate is released, the controller:

1. persists the rollback mission;
2. builds the provisional candidate CLI environment (the immutable per-commit environment the global commands will resolve to once confirmed);
3. forks a watchdog from the already-loaded stable controller process image;
4. verifies the exact process identity of the gated candidate.

Only then does it stop the previous worker and release the candidate. Therefore there is never an intentional interval with two queue consumers, and a controller crash before release closes the gate rather than orphaning an unconfirmed consumer.

The checkout remains provisional. The watchdog restores the previous exact commit if the candidate dies or the confirmation deadline expires. The global CLIs are untouched during the provisional phase: the `current` symlink under `$XDG_STATE_HOME/lubko/cli/` still selects the previous confirmed commit, so a rollback can never strand them on candidate code. If the candidate CLI environment cannot be built, the checkout is aborted and the previous checkout is restored.

When checkout itself was invoked as a Lubko queue job, the stable controller finalizes that job before returning because the worker that originally claimed it has intentionally been replaced.

## Confirmation handshake

The orchestrator must confirm the exact proposed commit twice. Both requests travel through the replacement worker, so the handshake itself is the end-to-end proof that the candidate can consume `lubko.jobs` and return responses.

First request:

```json
{"type":"confirm","commit":"<exact proposed commit>"}
```

The controller returns a fresh random challenge and stores only its digest.

Second request:

```json
{
  "type":"confirm",
  "commit":"<exact proposed commit>",
  "challenge":"<the first challenge reversed>"
}
```

The controller rechecks the deadline and candidate process identity while holding the deployment lock. Only after the response is valid does it write the candidate as maintained-worker metadata and persist terminal `confirmed` state.

A wrong commit, missing or wrong challenge, candidate failure, or timeout triggers rollback rather than leaving the candidate accepted ambiguously.

## Global CLI coherence

The maintained commands on PATH (`lubko-agent`, `lubko-worker`, `lubko-deploy`,
`lubko-deploy-ctl`, `lubko-install`, `my-lubko-agent`) are stable launchers that
resolve one `current` symlink (`$XDG_STATE_HOME/lubko/cli/current`) to the
immutable per-commit CLI environment of the commit they should run. The
supervised protocol keeps that pointer coherent with the *confirmed* worker:

- the candidate environment is built during checkout, while the CLIs still
  resolve to the previous confirmed commit;
- the second confirmation durably records `confirmed` state and candidate
  metadata first, and only then atomically switches the `current` symlink to
  the candidate commit;
- any rollback path removes the candidate environment and never moves the
  `current` symlink, so the prior confirmed CLI version is preserved by
  construction.

A crash between recording `confirmed` state and switching the symlink leaves
the CLIs stale (the previous confirmed version), never stranded on candidate
code; no rollback can fire after `confirmed` is durable. That window is
repaired idempotently: every `status` or `checkout` request reconciles the CLI
pointer to the confirmed maintained commit (`$XDG_STATE_HOME/lubko/cli/current`
is switched only when the confirmed commit's environment is already usable),
so a crash can never leave a permanently confirmed worker with stale CLIs. The
reconciliation never points the CLIs at a provisional candidate: while a
mission is `pending` the pointer stays on the previous confirmed commit.
`lubko-deploy status` reports the mismatch as a warning when it observes it.

## Rollback

Rollback authority does not execute candidate code. The forked stable watchdog:

1. stops the candidate by its recorded exact process identity;
2. force-checks out the exact previously maintained commit;
3. reuses the previous worker if it is still alive, otherwise starts that previous commit;
4. verifies the restored worker and PostgreSQL connectivity;
5. writes restored maintained-worker metadata;
6. removes the provisional candidate CLI environment (the `current` symlink was never moved);
7. records terminal `rolled_back` state.

If restoration cannot complete, the state remains pending and the watchdog retries. A later checkout must not silently supersede an unresolved rollback mission.

## Status

```json
{"type":"status"}
```

Status reports one of the externally relevant states: idle, awaiting the first confirmation, or awaiting the reversed challenge. Reading status also enforces an already-expired/dead candidate by attempting rollback under the deployment lock.
