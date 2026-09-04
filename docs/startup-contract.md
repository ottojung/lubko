# Supervisor startup contract

Lubko requires a simple observable process topology:

```
tini-static -- lubko-supervisor
```

Tini is PID 1 and launches `lubko-supervisor` as its direct child. The supervisor owns the maintained worker as its direct child. Lubko verifies this process topology, exact process identities, the installed startup launcher/definition, required private state directories, and private config permissions.

The outer host/container/service environment is **trusted** to restart Lubko appropriately. Lubko does not declare, inspect, infer, or verify Docker, Podman, systemd, or any host restart policy. There is no runtime proof seam for outer host/service-manager behavior.

## Rolling-upgrade readiness compatibility

The supervisor intentionally outlives the maintained worker during a version-changing deployment. The per-incarnation worker-health file used for readiness is therefore a stable, additive **schema-v1 compatibility envelope**. New workers may add bounded observability fields, but they must keep the v1 identity/liveness/readiness fields and schema marker readable by the immediately previous supervisor. New supervisors likewise accept minimal legacy v1 snapshots with safe defaults for observability fields that did not exist yet. Current rich snapshots carry an additive `observability_version` marker so current readers can still fail closed on truncated/malformed rich metrics while previous supervisors safely ignore the marker. Additive health metrics do not justify a readiness-envelope version bump.

## Versioned artifacts

`lubko-install` and `lubko-deploy bootstrap` publish repository-owned artifacts under `$XDG_STATE_HOME/lubko/deploy/`:

- `startup-contract.json` — the versioned observable startup contract;
- `lubko-startup-definition.json` — the exact `tini-static -- lubko-supervisor` startup definition plus required state/config paths;
- `lubko-startup` — the generated launcher.

`lubko-deploy startup-contract` validates those artifacts and the live process topology. It does not inspect anything outside the Lubko environment.

## Migrating from the legacy placeholder

Replace `tini-static -- sleep infinity` with the repository-owned `lubko-startup` launcher, install/bootstrap the current artifacts, and restart the environment. The outer environment's restart behavior is an operational prerequisite and is trusted rather than verified by Lubko.
