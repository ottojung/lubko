# Supervisor startup contract

Lubko requires a simple supervised service command:

```
lubko-supervisor
```

Lubko does not require any particular PID, init system, service manager, or
restart topology. `lubko-supervisor` may be launched directly or by any external
supervisor chosen by the deployment. Lubko deliberately does not prescribe or
inspect that topology.

Lubko validates only its repository-owned startup artifacts, state directories,
private config permissions, and required environment variable names. The outer
environment must supply the stable environment variables named by the versioned
contract, including `LUBKO_SUPERVISOR_STATE_TOKEN`. Lubko records only required
variable names, never their values.

## Rolling-upgrade readiness compatibility

The supervisor intentionally outlives the maintained worker during a
version-changing deployment. The per-incarnation worker-health file used for
readiness is therefore a stable, additive **schema-v1 compatibility envelope**.
New workers may add bounded observability fields, but they must keep the v1
identity/liveness/readiness fields and schema marker readable by the immediately
previous supervisor. New supervisors likewise accept minimal legacy v1 snapshots
with safe defaults for observability fields that did not exist yet. Current rich
snapshots carry an additive `observability_version` marker so current readers can
still fail closed on truncated/malformed rich metrics while previous supervisors
safely ignore the marker. Additive health metrics do not justify a
readiness-envelope version bump.

## Confirmed runtime recovery anchor

Every maintained entry point, including `lubko-supervisor`, resolves through the
single crash-durable `cli/current` symlink. There is no secondary supervisor
runtime selector or deployment override. `cli/current` names the last confirmed
sealed exact-commit runtime and is not advanced to a candidate until confirmation
succeeds.

Candidate deployment state may select a provisional worker while it is valid, but
it cannot invalidate the independently confirmed runtime. If mutable
desired/mission state is unreadable, malformed, unsupported, truncated, or
contradictory, the supervisor converges back to the usable runtime named by
`cli/current`, unless exact live-consumer authority requires a temporary hold to
preserve the one-consumer invariant.

Supervisor-owned deployment missions also use a stable backward-readable schema-3
wire envelope while a candidate is unconfirmed. The compatibility `new_meta`
field is only a non-process candidate descriptor: all
PID/session/start-time/token/worker-identity fields are null, and real candidate
process authority remains exclusively in the supervisor durable child state.
Newer controllers may add fields that older trusted supervisors ignore, but they
must not publish an envelope the already-running recovery supervisor cannot parse.

## Versioned artifacts

`lubko-install` publishes repository-owned artifacts under
`$XDG_STATE_HOME/lubko/deploy/`:

- `startup-contract.json` — the versioned observable startup contract;
- `lubko-startup-definition.json` — the exact `lubko-supervisor` service
  command plus required state/config paths and required external environment
  variable names;
- `lubko-startup` — the generated launcher that execs
  `lubko-supervisor`.

`lubko-deploy startup-contract` validates those artifacts. It does not inspect
the live process topology or anything outside the Lubko environment.

## External supervision

`docker/lubko-base.Dockerfile` intentionally has no init-system entrypoint.
Deployments choose how to launch the image and whether to supervise
`lubko-supervisor`. Tini, s6, Docker `--init`, systemd, and direct execution
are all external deployment choices rather than Lubko requirements.

Automatic restart after an external process/container failure can be provided by
the deployment when desired, but that mechanism is outside Lubko's startup
contract and is never inspected by Lubko.
