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

Just before the previous worker is stopped, the controller durably records that
its retirement has begun (`previous_retiring` in the rollback state file). The
watchdog reads that marker after any controller crash, so it always knows
whether the previous worker may already be mid-shutdown.

The checkout remains provisional. The watchdog restores the previous exact commit if the candidate dies or the confirmation deadline expires. The global CLIs are untouched during the provisional phase: the `current` symlink under `$XDG_STATE_HOME/lubko/cli/` still selects the previous confirmed commit, so a rollback can never strand them on candidate code. If the candidate CLI environment cannot be built, the checkout is aborted and the previous checkout is restored.

### Queue-invoked checkout (the control job survives the old worker's shutdown)

When checkout itself is submitted through the Lubko queue, the worker that claims it is the very worker about to be replaced. The worker's correct general shutdown invariant terminates every tracked active job group, including the group running the controller — so the controller must hand off before any destructive step. It does so without any worker-side special/exempt argv, process group, environment, or name: an ordinary job remains ordinary and is terminated/reaped like every other active job.

The queue-invoked controller forks a detached handoff helper (its own session and process group) over a pipe:

1. The helper acquires the deployment lock and performs only reversible preparation: it persists the pending rollback mission with `previous_retiring=false`, arms the watchdog, and spawns the gated candidate.
2. It delivers the normal candidate (or an error) response to the controller parent over the pipe and stops there — the previous worker is not yet touched.
3. The controller parent prints the response and exits zero, so the owning worker finalizes the checkout row as durably `succeeded`. The controller never rewrites a terminal queue result itself.
4. The helper polls that exact captured row until PostgreSQL durably reports `succeeded` with no cancellation marker. A `failed`/`cancelled`/deleted row or an expired handoff deadline aborts the mission.
5. Only then — still holding the deployment lock — it durably records `previous_retiring=true` before stopping the old worker, releases the gate, verifies the candidate, extends the confirmation deadline, writes the live pending state, and exits. The old worker's shutdown still terminates every ordinary active job group; the detached helper is simply no longer one of them.
6. If preparation, the initiating row, or the handoff deadline fails before durable success, the helper closes the gated candidate, restores the previous checkout, and leaves the previous worker running — it never crosses the destructive boundary, and the armed watchdog completes the rollback.

A manual (non-queue) invocation retains the synchronous safe path: preparation is immediately followed by the destructive handoff under the deployment lock.

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
repaired idempotently on the next controller invocation: every `status` or
`checkout` request reconciles the CLI pointer to the confirmed maintained
commit (`$XDG_STATE_HOME/lubko/cli/current` is switched only when the confirmed
commit's environment is already usable), so a permanently confirmed worker
cannot remain with stale CLIs past the next status/checkout. The
reconciliation never points the CLIs at a provisional candidate: while a
mission is `pending` the pointer stays on the previous confirmed commit.
`lubko-deploy status` reports the mismatch as a warning when it observes it.

## Rollback

Rollback authority does not execute candidate code. The forked stable watchdog:

1. stops the candidate by its recorded exact process identity;
2. force-checks out the exact previously maintained commit;
3. restores the previous worker: if retirement had not yet begun and the
   recorded previous worker is still alive, its exact identity is reused;
   otherwise the old identity is deterministically stopped and awaited dead and
   a fresh previous-commit worker is spawned, so a worker that is merely in the
   middle of shutting down is never accepted as the restored consumer;
4. verifies the restored worker and PostgreSQL connectivity;
5. writes restored maintained-worker metadata;
6. removes the provisional candidate CLI environment (the `current` symlink was never moved);
7. records terminal `rolled_back` state.

Terminal `rolled_back` therefore always means the checkout is the previous
commit and a genuinely restored, verified maintained worker exists.

If restoration cannot complete, the state remains pending and the watchdog retries. A later checkout must not silently supersede an unresolved rollback mission.

## Status

```json
{"type":"status"}
```

Status reports one of the externally relevant states: idle, awaiting the first confirmation, or awaiting the reversed challenge. Reading status also enforces an already-expired/dead candidate by attempting rollback under the deployment lock.
