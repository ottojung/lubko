# Supervisor startup contract

Lubko requires a simple observable process topology:

```
tini-static -- lubko-supervisor
```

Tini is PID 1 and launches `lubko-supervisor` as its direct child. The supervisor owns the maintained worker as its direct child. Lubko verifies this process topology, exact process identities, the installed startup launcher/definition, required private state directories, and private config permissions.

The outer host/container/service environment is **trusted** to restart Lubko appropriately. Lubko does not declare, inspect, infer, or verify Docker, Podman, systemd, or any host restart policy. There is no runtime proof seam for outer host/service-manager behavior.

## Rolling-upgrade readiness compatibility

The supervisor intentionally outlives the maintained worker during a version-changing deployment. The per-incarnation worker-health file used for readiness is therefore a stable, additive **schema-v1 compatibility envelope**. New workers may add bounded observability fields, but they must keep the v1 identity/liveness/readiness fields and schema marker readable by the immediately previous supervisor. New supervisors likewise accept minimal legacy v1 snapshots with safe defaults for observability fields that did not exist yet. Current rich snapshots carry an additive `observability_version` marker so current readers can still fail closed on truncated/malformed rich metrics while previous supervisors safely ignore the marker. Additive health metrics do not justify a readiness-envelope version bump.

## Confirmed runtime recovery anchor

Every maintained entry point, including `lubko-supervisor`, resolves through the single crash-durable `cli/current` symlink. There is no secondary supervisor runtime selector or deployment override. `cli/current` names the last confirmed sealed exact-commit runtime and is not advanced to a candidate until confirmation succeeds.

Candidate deployment state may select a provisional worker while it is valid, but it cannot invalidate the independently confirmed runtime. If mutable desired/mission state is unreadable, malformed, unsupported, truncated, or contradictory, the supervisor converges back to the usable runtime named by `cli/current`, unless exact live-consumer authority requires a temporary hold to preserve the one-consumer invariant.

Supervisor-owned deployment missions also use a stable backward-readable schema-3 wire envelope while a candidate is unconfirmed. The compatibility `new_meta` field is only a non-process candidate descriptor: all PID/session/start-time/token/worker-identity fields are null, and real candidate process authority remains exclusively in the supervisor durable child state. Newer controllers may add fields that older trusted supervisors ignore, but they must not publish an envelope the already-running recovery supervisor cannot parse.

## Versioned artifacts

`lubko-install` publishes repository-owned artifacts under `$XDG_STATE_HOME/lubko/deploy/`:

- `startup-contract.json` — the versioned observable startup contract;
- `lubko-startup-definition.json` — the exact `tini-static -- lubko-supervisor` startup definition plus required state/config paths;
- `lubko-startup` — the generated launcher.

`lubko-deploy startup-contract` validates those artifacts and the live process topology. It does not inspect anything outside the Lubko environment.

## Migrating from the legacy placeholder

Replace `tini-static -- sleep infinity` with the repository-owned `lubko-startup` launcher, install the current artifacts, and restart the environment. The outer environment's restart behavior is an operational prerequisite and is trusted rather than verified by Lubko.
