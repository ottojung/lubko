# Supervisor startup contract

Lubko's production reliability guarantee depends on `lubko-supervisor` being the
container's long-lived process owner, restored after every container or host
restart. The repository owns this contract so it cannot silently drift away from
the code that depends on it.

## Supported topology

```
tini-static -- lubko-supervisor
```

- Tini is PID 1 and launches the supervisor as its **direct child**.
- The supervisor owns the maintained worker as its **direct child**.
- Tini **only** reaps zombies and forwards signals. It does **not** restart the
  supervisor. The container/service restart policy (for example a container
  `restart: always` or a systemd `Restart=always`) is the restart authority: it
  restarts the container (and therefore the supervisor) after a crash or host
  restart, which restores exactly one worker from durable supervisor state.

Any other topology is unsupported, including the legacy placeholder
`tini-static -- sleep infinity`: with `sleep infinity` there is no supervisor,
so a worker crash or container restart has no automatic restart authority and
requires a human.

## Versioned contract artifact and startup definition

`lubko-install` and `lubko-deploy bootstrap` publish two repository-owned
artifacts under `$XDG_STATE_HOME/lubko/deploy/`:

- `startup-contract.json` — the versioned contract (schema version, supported
  command, external restart authority, required state directories, and private
  config path expectations).
- `lubko-startup-definition.json` — the concrete, versioned container/service
  startup definition (exact `tini-static -- lubko-supervisor` command, required
  restart policy, required state mounts, and private config path expectations).

The contract carries a `schema_version`; a version change bumps it so an
installation built against an older contract fails closed rather than trusting an
obsolete startup definition. `lubko-deploy startup-contract --write` re-publishes
both artifacts on demand. `lubko-install`/`lubko-deploy bootstrap` install and
validate these artifacts (and the versioned `lubko-startup` launcher) as part of
the supported deployment path; they cannot mutate the outer container manager, so
the container must actually run `lubko-startup` and the deployment seam must
supply restart-policy evidence (see below).

## Proving the contract is active

`lubko-deploy status` and `lubko-deploy startup-contract` prove the **live**
process topology and the deployment seams, not merely worker liveness:

- the recorded contract exactly matches the code's current contract (fail closed
  on missing/malformed/unsupported/mismatch);
- the installed `lubko-startup` launcher matches the versioned source;
- the installed startup definition matches the current contract exactly;
- the required private state directories exist with the exact safe (`0700`) mode;
- the private config files exist with no group/world access;
- the init process (PID 1) is a supported Tini;
- the supervisor is that init's live direct child running `lubko-supervisor`
  (explicitly **not** `sleep infinity`);
- the running worker, when present, is the supervisor's direct child;
- **concrete, configured restart-authority evidence is present** (the contract of
  record alone is never treated as proof — `lubko-deploy startup-contract` exits
  non-zero until the deployment seam supplies `LUBKO_SUPERVISOR_RESTART_POLICY`).

`lubko-deploy startup-contract` exits non-zero when any boundary is not satisfied,
so it can gate a deploy or alert. These checks signal nothing and infer nothing
from queue state: the topology proof is exact parent/child identity read from
`/proc`, and the start-tick identity is bound after every read so a PID reuse
cannot satisfy it.

## Migrating from an older deployment

1. Stop the legacy unmanaged worker manually once (there is no supervisor
   identity to hand it to yet):
   `lubko-deploy deploy --bootstrap` records the first maintained worker.
2. Point the container command at the repository-owned `lubko-startup` launcher
   (which execs `tini-static -- lubko-supervisor`) and configure the container/service
   restart policy to `always` (Tini does not restart the supervisor — the restart
   policy does). These are the only image/container changes required; all
   supervisor-side pieces already live in this repository.
3. `lubko-install` (or `lubko-deploy bootstrap`) installs and validates the current
   startup contract, launcher, and startup definition.
4. Restart the container. Tini starts the supervisor, which reconstructs the
   confirmed worker from durable state.
5. Supply `LUBKO_SUPERVISOR_RESTART_POLICY=always` to the deployment seam and run
   `lubko-deploy startup-contract` to prove the full chain
   `tini -> supervisor -> worker` (including restart authority) before trusting the
   deployment. Without that evidence the command fails closed.
