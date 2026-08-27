# Supervisor startup contract

Lubko's production reliability guarantee depends on `lubko-supervisor` being the
container's long-lived process owner, restarted by the init process (Tini)
after every container or host restart. The repository owns this contract so it
cannot silently drift away from the code that depends on it.

## Supported topology

```
tini-static -- lubko-supervisor
```

- Tini is PID 1 and launches the supervisor as its **direct child**.
- The supervisor owns the maintained worker as its **direct child**.
- Tini restarts the supervisor whenever it exits, so a container/host restart
  restores exactly one worker from durable supervisor state.

Any other topology is unsupported, including the legacy placeholder
`tini-static -- sleep infinity`: with `sleep infinity` there is no supervisor,
so a worker crash or container restart has no automatic restart authority and
requires a human.

## Versioned contract artifact

`lubko-install` and `lubko-deploy bootstrap` publish the current contract under
`$XDG_STATE_HOME/lubko/deploy/startup-contract.json`. The artifact carries a
`schema_version`; a version change bumps it so an installation built against an
older contract fails closed rather than trusting an obsolete startup
definition. `lubko-deploy startup-contract --write` re-publishes the artifact on
demand.

## Proving the contract is active

`lubko-deploy status` and `lubko-deploy startup-contract` both prove the **live**
process topology, not merely worker liveness:

- the init process (PID 1) is a supported Tini;
- the supervisor is that init's live direct child running `lubko-supervisor`
  (explicitly **not** `sleep infinity`);
- the running worker, when present, is the supervisor's direct child.

`lubko-deploy startup-contract` exits non-zero when the supported chain is not
satisfied, so it can gate a deploy or alert. These checks signal nothing and
infer nothing from queue state: the proof is exact parent/child identity read
from `/proc`.

## Migrating from an older deployment

1. Stop the legacy unmanaged worker manually once (there is no supervisor
   identity to hand it to yet):
   `lubko-deploy deploy --bootstrap` records the first maintained worker.
2. Switch the container command from whatever launched `sleep infinity` (or the
   old launcher) to `tini-static -- lubko-supervisor`. This is the only
   image/container change required; all supervisor-side pieces already live in
   this repository.
3. `lubko-install` (or `lubko-deploy bootstrap`) records the current startup
   contract version.
4. Restart the container. Tini starts the supervisor, which reconstructs the
   confirmed worker from durable state.
5. Run `lubko-deploy startup-contract` to prove the live chain
   `tini -> supervisor -> worker` before trusting the deployment.
