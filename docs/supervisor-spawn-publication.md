# Supervisor spawn publication protocol

The supervisor publishes a newly spawned maintained worker through three ordered
durable steps so a concurrent manual recovery (`lubko-deploy recover` / repair)
can never authorize a second queue consumer while the first may still be live.

## Durable authorities

- `state.spawning` — the exact pid-bearing **pre-spawn recovery obligation**.
  Written crash-durably *before* `Popen`, then upgraded with the exact child
  identity (PID + start-time ticks + lifecycle token) immediately after a
  successful spawn. While present it is **replacement-blocking**: manual
  recovery's `_pre_adoption_authority_error` refuses to adopt any worker that
  does not match the recorded obligation instance.
- `state.child` — the published exact child identity. Manual recovery does **not**
  consult `state.child` to authorize a consumer; it consults `state.spawning`,
  so `state.child` alone never opens the adoption gate.
- `worker/meta.json` — the lifecycle metadata manual recovery and the supervisor
  both reconcile against. It is the authoritative record of the queue-visible
  worker.

## Publication order (the fix for issue #282)

A successful spawn now proceeds in this exact order, all inside the shared
consumer-establishment lock:

1. **Publish `state.child` while `state.spawning` stays durable.** The exact
   pid-bearing obligation remains set, matching the just-published child by
   token, PID, and start-time ticks. A concurrent manual recovery therefore
   observes a blocking authority and cannot adopt a second consumer.
2. **Write `worker/meta.json`** for the published child.
3. **Clear `state.spawning`** in a final durable state write, only after the
   meta write has durably succeeded.

### Crash/failure semantics

- A crash or a meta write failure *after* step 1 leaves `state.child` published
  **and** `state.spawning` durable. The obligation is replacement-blocking, so
  manual recovery stays refused and no second consumer is authorized.
- A later supervisor tick (or a restart) re-reads `child + spawning`. The
  obligation's identity matches the published child exactly, which is recognized
  as an **in-progress publication** (not a pending convergence). The supervisor
  retries the meta write and then clears the obligation — it never converges or
  duplicates the worker.
- A meta write failure keeps the obligation durable and is retried on the next
  tick; the worker is never silently duplicated and the daemon never permanently
  holds while a live child exists.

## Exact-identity and lock invariants preserved

- The whole gate-to-spawn-to-publication critical section still runs under the
  shared cross-process `consumer_lock`, so from one initially consumer-free
  state exactly one path can authorize a spawn.
- Every resolution path still uses exact identity (PID, process group, session,
  start time, lifecycle token) and never process-name matching or broad numeric
  signalling.
- The obligation survives its own malformation as a durable blocking hold that
  only explicit operator repair clears.
