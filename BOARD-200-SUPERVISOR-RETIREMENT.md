# Board 200 — Lubko supervisor worker retirement: investigation report

Front: `200c1`. Repo: `/workspace/lubko-200-supervisor`, detached at `origin/main` `91e4547`.
Host: `marceline-dev` (this container is the host's Lubko container; supervisor and worker
state/logs are directly readable). Read-only investigation: no tracked file was modified,
nothing was committed, pushed, or merged, and the board issue was not touched.

> **Update 2026/10/09 — the blocked human decision is resolved.** `chatgpt@delegated-operator`
> delegated the supervisor-contract decision on this issue, so section 6 is no longer a
> blocking question. The remediation recorded below is implemented (degraded readiness never
> signals a live queue consumer; sustained, independently verified absent forward progress
> with a 60 s grace period and 3 consecutive probes; crash-style bounded backoff; ≥48 h
> bounded retention of superseded-incarnation evidence). The investigation findings, the
> causal path, and the enumeration of retiring conditions are kept unchanged below as the
> historical record of the defect.

> **Terminal verdict B200-REMEDIATION-LANDED 2026/10/09 — the delegated policy is implemented,
> tested, mutation-pinned, and pushed. The issue stays OPEN for the human integration
> decision (review / merge / promotion); per its completion rule it is not closed here.**
>
> **What landed** — branch `fix/issue-200-supervisor-retirement-policy` (based on `main` @
> `72910f6` via `4155d5d`), commit `234811d` (policy enforcement across code, intent records,
> protocol, and tests) followed by this verdict commit. No merge to `main`, no PR, no
> promotion.
>
> **Measured gates** (each run with `XDG_STATE_HOME` / `XDG_CONFIG_HOME` / `XDG_CACHE_HOME` /
> `HOME` fenced in fresh temp dirs; real exit codes captured):
>
> | Gate | Result | Exit code |
> |---|---|---|
> | `uv run ruff format --check .` | 145 files already formatted | 0 |
> | `uv run ruff check .` | All checks passed | 0 |
> | `uv run mypy .` | Success: no issues in 119 source files | 0 |
> | `uv run pytest` | 1225 passed in ~2.3 s (budget 10 s) | 0 |
>
> **Mutation evidence** — 13 guard reverts were applied one at a time on top of the landed
> tree; the full suite went RED for every one, so each new assertion pins its guard
> non-vacuously:
>
> | Mutation (guard reverted) | Verdict |
> |---|---|
> | safety predicate accepts `any_scan_overdue` again | RED |
> | 60 s grace period removed | RED |
> | 3-probe corroboration removed | RED |
> | inconclusive probe counted as no-progress evidence | RED |
> | queue consumption no longer resets the evidence | RED |
> | health retirement bypasses the crash backoff counter | RED |
> | 48 h evidence-retention window removed | RED |
> | missing DB config treated as conclusive non-consumption | RED |
> | unreachable DB treated as conclusive non-consumption | RED |
> | unproven probe insert treated as conclusive non-consumption | RED |
> | no-progress evidence not bound to one incarnation | RED |
> | healthy recovery no longer resets the evidence | RED |
> | failed retirement discards the evidence | RED |
>
> Three guards (3-probe corroboration, per-incarnation evidence binding, healthy-recovery
> reset) were first found **GREEN** — vacuous — and were pinned by four new tests in
> `tests/test_health_retirement_policy.py` before re-running the suite; one redundant
> token check in `_note_progress_observation` was removed so each guard has exactly one
> enforced home.
>
> **Scope reconciliation** — `ANTONINA_ORCHESTRATOR_INTERVAL_SECONDS` does not exist anywhere
> in this repository (no occurrence of `ANTONINA` at all); the Antonina orchestrator is a
> separate external system, so those interval settings do not belong to this contract and
> were left untouched.
>
> **Remaining human-only residual:**
> 1. Review, merge, and promotion of the branch are human-gated by design (no PR was opened).
> 2. The underlying remote-database stalls (`db_deadline_breach`, `gc_batch_bound_hit` against
>    the Supabase pooler) remain an infrastructure investigation; Lubko now survives them
>    instead of churning workers, but nothing here fixes the database itself.
> 3. The issue remains **open**: the human operator must decide integration and confirm the
>    production behaviour under the next real database stall.

## 1. Conclusion

**Proven cause.** The 2026-10-05 ~16:12 UTC worker replacement was a **health-driven
retirement of a live, queue-ready worker**, not a crash, not OOM, not a desired-config
change, and not a deploy.

Exact causal path:

1. Supervisor tick `reconcile()` (`src/lubko/supervisor.py:912`) reaches
   `_probe_readiness()` (`supervisor.py:1913`).
2. The recorded child is alive and `state.ready` is true, so the ready-worker branch runs
   `_check_worker_health()` (`supervisor.py:2046`, called at `supervisor.py:1945`).
3. `interpret_worker_health()` (`health.py:1309`, operational block `health.py:1164-1196`)
   derives `operational.ready = False` with reason
   `overdue scans: cancellation, recovery, gc; unrecovered DB deadline breach`.
4. The supervisor does **not** merely withdraw readiness: because the reason starts with
   `worker operational not ready:` **and** `_worker_health_requires_retirement(child)`
   (`supervisor.py:2020-2043`) returns `True` — it returns `True` whenever
   `operational.lease_safety_negative or operational.any_scan_overdue`
   (`supervisor.py:2043`) — it sets `self._message`, logs
   `ready worker pid=… became operationally unsafe: …; retiring the exact incarnation`
   (`supervisor.py:1956-1960`) and calls `_retire_child()` (`supervisor.py:1961`).
5. `_retire_child()` (`supervisor.py:2113`) gates on canonical DB authority
   (`supervisor.py:2147-2156`) and `lifecycle_state.authorize_retirement`
   (`supervisor.py:2170`), then calls `lifecycle.stop_worker(meta, stop_grace_seconds)`
   (`supervisor.py:2177`).
6. `stop_worker()` (`lifecycle.py:1471`) → `_stop_pinned()` (`lifecycle.py:1544`) sends
   `SIGTERM` to the exact worker process group per member via pidfd
   (`lifecycle.py:1595`), waits for the worker's drain sentinel (`lifecycle.py:1597`),
   and would escalate to `SIGKILL` (`lifecycle.py:1610`) only after the cancel-grace floor.
7. The worker handles `SIGTERM` (`worker.py:7263`), runs `Supervisor._shutdown()`
   (`worker.py:6831`), whose first action is `_cleanup_pending_starts()`
   (`worker.py:6847`). That method (`worker.py:6261`) fails **every in-flight spawn
   attempt** with `stderr="worker shutting down before spawn completed"`
   (`worker.py:6284`) — this is the exact error text reported in the incident. It then
   writes the drain sentinel, which is why `drain/42440b27….drained` exists with mtime
   16:11:58.
8. Because the drain sentinel matched, `_retire_child` skipped owned-group recovery
   (`supervisor.py:2197-2199`), cleared the child, and logged
   `retired worker child pid=545885` (`supervisor.py:2225`).
9. The same tick's `_ensure_worker(commit)` (`supervisor.py:966` → `supervisor.py:1569`)
   spawned the replacement, logging `started worker child pid=%d for commit %s`
   (`supervisor.py:1911`) and, durably, `supervisor started worker pid=… commit=… incarnation=…`
   (`supervisor.py:1907-1909` → `deploy.log`).

**The command that failed is a downstream consequence, not the cause.** Any job whose
gated spawn was still pending when `SIGTERM` arrived was failed with that text. The
supervisor, not the worker, decided to stop.

### Why the health predicate fired (the real defect locus)

`_scan_schedule_overdue()` (`worker.py:353`, used at `worker.py:6690-6707`) marks a
maintenance scan overdue only when it is later than
`db_operation_timeout_seconds + process_poll_interval_seconds`
(`worker.py:386-387`) — on this host `15.0 + 0.1 = 15.1 s`
(`worker.py:435` `DEFAULT_DB_OPERATION_TIMEOUT_SECONDS`, `worker.py:421`
`DEFAULT_PROCESS_POLL_INTERVAL_SECONDS`, confirmed in the worker's own startup log line).
So each of the 122 retirements required a **>15 s stall of the single cooperative worker
loop**, not a momentary scheduling glitch.

Supporting measured facts on the live worker (`health/health-043834e5….json`,
`status.json`): `db_deadline_breach_count: 6`, `db_deadline_breached_at` set,
`gc_batch_bound_hit: true` — i.e. database operations really were exceeding the 15 s
client deadline and GC was saturating its 100-row batch. The PostgreSQL server is remote
and managed (`~/.config/lubko/database.conf`: `aws-0-us-east-2.pooler.supabase.com`,
Supabase pooler), so a slow/starved round trip plausibly produces exactly this stall.

**Judgement:** the retirement *mechanism* is behaving as designed (fail-closed, exact
identity, no false signal, clean drain). The **predicate is defective in effect**: it
converts a bounded, self-recovering database stall into a supervised kill of a
queue-ready worker, with **no backoff and no stability threshold**, because
`_worker_health_requires_retirement` deliberately excludes DB breach/error and keys only
on `any_scan_overdue`. `state.json` shows `restart_count: 0`, `last_exit: null`,
`next_attempt_at: null` — health-driven retirement bypasses the crash backoff entirely, so
the supervisor can churn workers indefinitely.

### Churn is chronic, not a one-off

From `~/.local/state/lubko/supervisor/supervisor.log` (3390 lines, append-only across
supervisor lifetimes):

- `ready worker pid=… became operationally unsafe` — **122** occurrences, each
  immediately followed by `retired worker child pid=…` (verified 122/122).
- `retired worker child pid=…` — **131** occurrences total; the remaining 9 are all
  pre-2026-10-03 supervisor-shutdown retirements or the single desired-generation-11 apply.
- First health-driven retirement: **2026-10-03 02:46:59**, i.e. after the current
  supervisor (pid 240442, `supervisor.pid`, started 2026-10-02 17:09) began running
  `91e4547`. Zero occurred before that.
- Per-day: 21 (10-03), 61 (10-04), 40 (10-05 up to 17:19).
- `maintained worker pid=… exited unexpectedly` (crash path) — 12 occurrences, all
  before 2026-10-03 except one at 2026-10-04 23:25:25. **None** at 16:12.

This matches the intent of the commit that introduced the behaviour: `98a764a`
("Fix worker starvation on large command output", #858) added
`_worker_health_requires_retirement` and, per its own message, "retire only on hard
safety breaches". The predicate has proven far more trigger-happy in production than in
its unit test (`tests/test_operational_readiness.py:382` monkeypatches the predicate to
`True`, so the real predicate's sensitivity to a 15 s stall is never exercised).

## 2. Complete enumeration of worker-retiring conditions

All paths verified by reading the cited lines. `SIGTERM`/`SIGKILL` are always delivered
per-member through a freshly opened pidfd after re-proving PID/start-ticks/PGID/token
(`_signal_exact_group`, `lifecycle.py:1363-1408`); there is no broad `killpg` anywhere.

### (a) Intentional restart / deploy on a desired-config change

| Condition | Evidence | Signal |
|---|---|---|
| Newer desired generation for the same commit (`desired.generation > state.applied_generation`) | decision `supervisor.py:1017-1023`; `_apply_desired` `supervisor.py:1425`; `desired.json` path `supervise.py:1008-1017`, write `supervise.py:1537`, read `supervise.py:1568` | — |
| `restart: true` intent forces replacement even at the same commit | `supervisor.py:1445` (`already_running = db_running and not desired.restart`), `supervisor.py:1475` → `_retire_child()` | `stop_worker` |
| Retirement before publishing the new generation | `supervisor.py:1475-1477`; refusal `supervisor.py:1478-1501` | `stop_worker` |
| Success record | `supervisor.py:1535` `applied supervisor run intent (generation %d)`; deploy.log `supervisor.py:1911` | — |
| `lubko-deploy restart` request ingress | `lifecycle.py:4357` (`_restart_intent_locked`), `supervise.py:2442` (`request_restart`) | — |
| Control-socket deploy with `restart` | `supervisor.py:4533` | — |
| Direct deploy (`lubko-deploy`, deployctl) replacing a running worker | `lifecycle.py:2446-2452` (`stop_worker(previous)`; rollback `lifecycle.py:2450`) | `stop_worker` |
| Direct deploy aborting an unproven replacement spawn | `lifecycle.py:2410-2425` (`_converge_unproven_spawn`) | TERM→KILL (`lifecycle.py:1315/1323/1335`) |
| Direct deploy replacement not alive / cannot reach PostgreSQL | `lifecycle.py:2209-2220` (`_verify_replacement`) | `stop_worker` on the **new** worker only |
| Legacy gated candidate: stop known-good before release | `deployctl.py:1516-1518` | `stop_worker` |
| Legacy rollback restart of previous worker | `deployctl.py:1798`, `deployctl.py:1712-1725` | `stop_worker` |
| Supervised rollback settling previous commit / candidate stop | `deployctl.py:2425-2428`, `deployctl.py:1821-1826` | `stop_worker` |
| Crash repair of a stale maintained CLI pointer (no process action) | `deployctl.py:3100-3102` `reconciled maintained CLI pointer to commit …` | none |

**Not what happened at 16:12:** `desired.json` mtime is **2026-10-02 17:09** and its content
is `{"commit":"91e4547…","generation":13,"migration":false,"restart":false,…}`;
`state.json` has `applied_generation: 13`, `intent: "run"`, `restart_count: 0`,
`last_exit: null`. No `applied supervisor run intent` and no `cold migration` line exists
after 2026-10-02 17:09:51.

### (b) Unexpected worker exit / crash

| Condition | Evidence |
|---|---|
| Recorded child no longer our direct child (reparented) → exact retirement | `supervisor.py:944-948` (`worker pid=%d alive but reparented (not our direct child); proceeding to exact retirement`) |
| Child dead, intent not `run` → state-only clear, no signal | `supervisor.py:951`, `_clear_child` `supervisor.py:2228` |
| Child dead, intent `run` → crash path | `supervisor.py:952`, `_handle_crash` `supervisor.py:2273` |
| Crash log + restart scheduling (with backoff) | `supervisor.py:2291` `maintained worker pid=%d exited unexpectedly with returncode %r; scheduling restart` |
| Crashed worker with no drain sentinel → owned-group recovery by exact `process_pgid` | `supervisor.py:2294-2310`, `recover_owned_groups` `supervisor.py:651` |
| Owned-group recovery incomplete → withhold replacement | `supervisor.py:2300-2306` |
| Crash recorded durably | `supervisor.py:2345-2348` `supervisor detected unexpected worker exit pid=… returncode=… restart=…` → `deploy.log` |
| DB-published worker dead | `_db_crash_blocks` `supervisor.py:972`, `_handle_db_crash` `supervisor.py:3045`, log `supervisor.py:3095` |
| Worker exited during startup | `_log_worker_startup_exit` `supervisor.py:3435-3449` |
| Spawn live without acceptable identity → converge | `_settle_unproven_spawn` `supervisor.py:3497-3502`, `_converge_direct_child` `supervisor.py:4199-4211` (`terminate` `4200`, `kill` `4211`) |

**Not what happened at 16:12:** no `exited unexpectedly` and no `detected unexpected worker
exit` line anywhere on 2026-10-05; `last_exit` is `null`; the retired pid 545885 produced a
clean drain sentinel, which only happens on the `SIGTERM` shutdown path.

### (c) Health / liveness failure or lease expiry — **this is the 16:12 path**

| Condition | Evidence |
|---|---|
| Ready worker, `operational.ready == False`, and `_worker_health_requires_retirement` true → **retire the incarnation** | `supervisor.py:1944-1962`; predicate `supervisor.py:2020-2043` (return `supervisor.py:2043`) |
| Predicate true iff `lease_safety_negative or any_scan_overdue` | `supervisor.py:2043`; derived in `health.py:1164-1166` and reported at `health.py:1177-1185` |
| Same, but predicate false (e.g. transient DB error only) → readiness withdrawn, worker kept | `supervisor.py:1965-1968`, `_record_not_ready` `supervisor.py:2080`, log `supervisor.py:2106-2110` |
| Missing / stale / PID- or ticks-mismatched snapshot (`supervisor.py:2056-2078`), or `not live` (`supervisor.py:2074-2075`), or `operational not ready` (`supervisor.py:2076-2077`) | none (readiness only) |
| Overdue-scan derivation in the worker | `worker.py:353-388`, used `worker.py:6690-6707` |
| Lateness budget = `db_operation_timeout + process_poll` | `worker.py:387-388` |
| Unrecovered DB deadline breach / DB error (revoke readiness, **not** retire) | `health.py:1173-1174`, reason text `health.py:1187-1190`; `_db_deadline_breach_recovered` / `_db_error_recovered` invoked at `health.py:1173-1174` |
| Retirement itself | `_retire_child` `supervisor.py:2113`; `stop_worker` call `supervisor.py:2177`; log `supervisor.py:2225` |
| Worker-side consequence of the `SIGTERM` | `worker.py:7263` → `worker.py:6831` → `worker.py:6847` → `worker.py:6261` → text at `worker.py:6284` |
| Retirement refusals that correctly hold instead of signalling | `supervisor.py:2139-2155`, `supervisor.py:2170-2173`, `supervisor.py:2185-2189`, `supervisor.py:2213-2222`; `lifecycle_state.authorize_retirement` `lifecycle_state.py:596`; `lifecycle.py:1573-1578`, `lifecycle.py:1586-1587` |

Note: the worker also has a *lease-expiry* path of its own
(`_enforce_lease_safety`, `worker.py:6540`, invoked at `worker.py:4891`), but that
cancels **jobs**, not the worker process. There is no lease-expiry condition in the supervisor that retires the worker; the
supervisor-side lease signal is `lease_safety_negative` inside the health predicate.

### (d) Crash recovery / startup recovery

| Condition | Evidence |
|---|---|
| Identity publication failed after spawn → converge the live child | `supervisor.py:3400-3434` |
| Live spawn with no durable pre-`Popen` obligation → converge + recover groups | `supervisor.py:1711-1779` |
| Durable unresolved-child hold still alive → pinned TERM→KILL | `supervisor.py:3742`, `_converge_unresolved` `supervisor.py:3785/3789`; re-proof refusals `supervisor.py:3769-3774` |
| Unresolved hold not convergable → hold without starting a worker | `supervisor.py:4173-4176` |
| Previously spawned pid-bearing obligation still live → converge | `supervisor.py:4100-4118` |
| Creator dead, kernel `PR_SET_PDEATHSIG` already killed it → resolve without signalling | `supervisor.py:4046-4051`; `pdeathsig` installed `supervisor.py:385`, parentage re-check `supervisor.py:393` |
| Pid-less obligation without parent-death guarantee → fail closed, demand `lubko-deploy recover` | `supervisor.py:3996-4042` |
| Recovery worker with unproven spawn converged | `lifecycle.py:3896-3904`; `_converge_unproven_spawn` `lifecycle.py:1300-1335` |

### (e) Convergence / reconciliation of desired vs actual state

| Condition | Evidence |
|---|---|
| Canonical DB record names a worker that must not run → stop by exact identity | `_retire_db_worker` `supervisor.py:2928`, stop at `supervisor.py:2944`, log `supervisor.py:2945-2952` |
| Published worker runs a superseded commit | `supervisor.py:3002-3008` `published worker pid=%d runs superseded commit %s; retiring by exact identity…` |
| Published worker is no longer our live direct child | `supervisor.py:3010-3015` |
| Dead published record still owning command groups → recover | `supervisor.py:2954-2961` |
| Derived action is `hold` (explicit stop, unmanaged record, unsettled mission) | `supervisor.py:958-960`, `_ensure_held` `supervisor.py:4275` (retire at `supervisor.py:4282-4283`) |
| Corrupt/unreadable durable worker-ownership state → hold, never start | `supervisor.py:922-936` |
| Cold migration convergence (pointer + durable rollback state, no process action) | `supervisor.py:1077-1150`, logs `supervisor.py:1113-1114` and `supervisor.py:1145-1150` |
| Startup artifact convergence (no process action) | `supervisor.py:1151-1190` |
| Runtime/confirmed commit version skew → exec-in-place or handoff; probe process killed, worker child preserved | `supervisor.py:4869`, `4960`, probe kill `supervisor.py:5061/5082` |
| Consumer-establishment lock contention → hold, no signal | `supervisor.py:1590-1600` (message at `supervisor.py:1596-1600`) |

### (f) Graceful drain and shutdown

| Condition | Evidence |
|---|---|
| Container runtime `SIGTERM`/`SIGINT` to the supervisor | `supervisor.py:4296-4302`, log text at `supervisor.py:4298` |
| Run-loop exit → retire child, then published record / unresolved obligation checks | `supervisor.py:4717-4768` (`_shutdown_locked`; retire at `4744`, published-record check `4746-4751`, unresolved-obligation check `4752-4766`), logs at `supervisor.py:4728/4740/4767` |
| Drain/escalate ladder used by every retirement above | `lifecycle.py:1471` `stop_worker` → `lifecycle.py:1544` `_stop_pinned`: kill floor `lifecycle.py:1589-1591`, `SIGTERM` `lifecycle.py:1595`, drain wait `lifecycle.py:1597`, `SIGKILL` `lifecycle.py:1610`, bounded retirement wait `lifecycle.py:1611` |
| Old incarnation health/log artifacts pruned after a successful readiness proof | `health.py:973-986` `prune_old_incarnation_artifacts` (called `supervisor.py:1993`) |

## 3. Artifacts read, and what each does / does not establish

Everything below was read directly on `marceline-dev` inside the Lubko container.

| Artifact | Establishes | Does **not** establish |
|---|---|---|
| `~/.local/state/lubko/supervisor/supervisor.log` (3390 lines, append-only) | **Decisive.** The exact 16:11:56.865 `became operationally unsafe` message, its reason string, the 16:11:58.861 `retired worker child pid=545885`, the 16:12:03.156 `started worker child pid=800098`, and the 122-occurrence churn with zero crash-path events on 10-05 | Nothing about *why* the worker loop stalled >15.1 s on that tick; the retired worker's own log was already pruned |
| `…/supervisor/f7ce07e7…/state.json` | `applied_generation 13 == desired.generation 13`, `intent run`, `restart_count 0`, `last_exit null`, `next_attempt_at null`, `unresolved_child null`, `spawning null`; child = pid 800098 token `043834e5…` | No historical retirement record — `state.json` is rewritten in place and retains no counter for health-driven retirement |
| `…/supervisor/f7ce07e7…/desired.json` (mtime 2026-10-02 17:09) | Desired config unchanged since 2026-10-02; `restart: false`, `migration: false`, generation 13 — **rules out (a) and (e)** for this event | n/a |
| `…/supervisor/status.json`, `supervisor.pid` | Supervisor pid 240442 with `start_time_ticks 749826767`, i.e. the same supervisor process before and after 16:12 — **rules out supervisor exit/restart**; live health snapshot with `db_deadline_breach_count 6`, `gc_batch_bound_hit true` | Historical health snapshots (overwritten every second) |
| `…/worker/deploy.log` (486 lines) | `supervisor started worker pid=800098 … incarnation=marceline-dev` at 16:12:03, `supervisor verified worker pid=800098 consumes the queue` at 16:12:13, and the full per-restart history with unchanged commit `91e4547` — **no deploy or generation event on 10-05** | Which trigger preceded each start (that is only in `supervisor.log`) |
| `…/worker/meta.json` | Replacement worker identity: pid/pgid 800098, token `043834e5…`, `state: running`, `started_at 1791216722.49` = 16:12:02 UTC | — |
| `…/worker/health/health-043834e5….json` | Live snapshot: `db_deadline_breach_count 6`, `db_deadline_breached_at` set, `gc_batch_bound_hit true`, `shutting_down false` — the same signal family that drove the retirements | The 16:11 snapshot (overwritten at 1 Hz) |
| `…/worker/drain/42440b27c69ee268037492d82f2e287d.drained` (mtime **16:11:58**) | The retired incarnation `42440b27…` completed a **clean local drain** on `SIGTERM`, i.e. the graceful path — this is what makes `_stop_pinned` return at `lifecycle.py:1597` without `SIGKILL` and makes `_retire_child` skip owned-group recovery. 131 drain sentinels match the 131 retirements exactly | Which job rows were failed |
| `…/worker/logs/worker-043834e5….log` (only file present) | Current worker's startup parameters: `poll=1.0s process_poll=0.1s lease=30.0s lease_refresh=5.0s lease_recovery=10.0s output_pub=1.0s claim_batch=8 health_pub=1.0s` | **The retired worker's log was deleted** by `prune_old_incarnation_artifacts` (`health.py:973-986`) after the replacement reached readiness, so the loop-stall trace is gone |
| Read-only SQL against `lubko.jobs` (`default_transaction_read_only=on`) | Job rows are plain JSON with `state.worker_incarnation`; terminal rows are **GC-reaped within ~30 min** — the live table spans only `created_at` 2026-10-05T16:22:26 → 16:52:32. **No row anywhere contains `shutting down before spawn`, `abandoning pending`, or `spawn timed out`.** | The identity, payload, or existence of the specific failed command |
| `~/.local/state/antonina/orchestrator-turn.log` | The 4 occurrences of the phrase `worker shutting down before spawn completed` are all quotations of *this investigation brief*, not incident records | Any independent corroboration of the reported failure |
| `ps` process table | Supervisor pid 240442 started Oct 2; worker pid 800098 started **16:12**; `~/.config/lubko/database.conf` shows a remote Supabase pooler | Any pre-16:12 process state |

**Net evidentiary status: the cause is proven, not merely narrowed.** The supervisor's own
append-only log names the exact condition, the exact predicate that escalated it, the
target pid, and the replacement. The only thing not recoverable is the retired worker's
internal trace of the >15.1 s stall.

**Honest limitation on the reported symptom.** The claim "one command failed with
`worker shutting down before spawn completed`" is *consistent* with the proven path — the
string exists at exactly one place in the codebase (`worker.py:6284`) and is reachable only
via `_cleanup_pending_starts()` on the `SIGTERM` shutdown path, which the 16:11:58 drain
sentinel proves was taken. But no surviving artifact on this host records that specific
job: its DB row was GC-reaped and the retired worker's log was pruned. So the *mechanism*
is proven; the *specific command* is not independently corroborated here.

## 4. Intended behaviour vs defect, per retiring condition

| Condition | Intended? | Judgement |
|---|---|---|
| (a) desired-generation change / `restart: true` / deploy | Yes, by design (`docs/issue21-deploy-protocol.md`, `docs/protocol.md`) | Correct. Not involved here. |
| (b) crash detection + owned-group recovery + backoff | Yes | Correct. Not involved here. |
| (c) health/liveness: `lease_safety_negative` | Yes | Correct — negative remaining lease safety is a real safety breach. |
| (c) health/liveness: `any_scan_overdue` → retire a **queue-ready** worker | Design intent of `98a764a` ("retire only on hard safety breaches") | **Defect in effect.** A >15.1 s maintenance-schedule lateness under a remote, stalling database is a *self-recovering* condition. Retiring re-establishes nothing the loop could not resume; it discards all in-flight spawns and warm state. Combined with the deliberate exclusion of DB breach/error from the predicate and the total absence of backoff on this path (`restart_count` stayed 0 across 122 retirements), it converts a transient database stall into an unbounded retire/replace loop. The unit coverage (`tests/test_operational_readiness.py:382`) monkeypatches the predicate, so this sensitivity is untested. |
| (c) `shutting_down` reported in operational reason | Yes | Correct as a readiness signal; not a retirement trigger on its own. |
| (d) crash/startup recovery convergence | Yes | Correct. |
| (e) superseded-commit / not-our-child convergence | Yes | Correct. |
| (f) graceful drain and SIGTERM shutdown | Yes | Correct; proven clean by the drain sentinel. |
| Failure mode: previous incarnation's log pruned before any post-mortem window | Not obviously intended | **Secondary defect.** `prune_old_incarnation_artifacts` (`health.py:973-986`) runs on every successful readiness proof, so the log of a worker retired for a health breach is deleted within seconds. Every retirement in this class is therefore undiagnosable after the fact. |

## 5. Remediation options and trade-offs

Presented for the human decision; **nothing has been changed.**

1. **Do not retire on `any_scan_overdue`; require persistence.** Keep the readiness
   withdrawal (`supervisor.py:1965-1968`) and retire only when the health snapshot shows
   the same hard breach on N consecutive supervisor ticks (or for longer than a multiple
   of the lateness budget), or when `lease_safety_negative` is present at all.
   *Trade-off:* a genuinely wedged loop takes longer to be replaced (N ticks × 1 s poll);
   requires choosing N, which is a policy decision. Cheapest change, no new state.
2. **Add crash-style backoff to the health-retirement path.** Route health-driven
   retirement through the existing `restart_count` / `next_attempt_at` machinery so a
   repeated trigger escalates the delay instead of firing every few minutes.
   *Trade-off:* a real wedge then backs off too; needs a reset condition (the existing
   "worker is stable; resetting restart counter" behaviour already provides one).
3. **Widen the worker's lateness budget for a saturated database.** Derive the overdue
   budget from observed DB behaviour (e.g. `k × db_operation_timeout` or an EWMA of
   observed round-trip time) instead of a single hard deadline.
   *Trade-off:* moves the judgement into the worker, weakens a fail-closed signal, and
   could mask a genuinely stuck loop. Weakest option on its own; a good complement to (1).
4. **Investigate the database before changing Lubko.** The measured `db_deadline_breach`
   and `gc_batch_bound_hit` signals point at the remote Supabase pooler. Moving to a
   local/dedicated PostgreSQL, or reducing concurrent agent load per host, may remove the
   trigger without any code change.
   *Trade-off:* infrastructure cost; does not fix the fail-fast-to-kill predicate.
5. **Retain one previous incarnation's log/health snapshot** (defer
   `prune_old_incarnation_artifacts` by one generation, or keep the last N).
   *Trade-off:* small unbounded-ish disk growth under churn — which, given 122 retirements
   in 2.5 days, is exactly the case that must be handled. Without this, no future
   occurrence of this class is diagnosable.

Recommended combination if a code fix is authorised: **(1) + (2) + (5)**, with **(4)**
pursued in parallel as the likely root-cause mitigation.

## 6. The decision that was required — resolved by delegated operator

Once the cause is accepted, one decision was needed and it cannot be made from code or
tests:

> **Should a queue-ready `lubko-worker` ever be retired — killed and replaced — for a
> bounded, self-recovering degradation (a >15 s maintenance-scan lapse caused by slow
> remote database round trips), or is "the worker is alive and consuming the queue" a
> stronger condition than "the worker's maintenance schedule slipped"?**

`chatgpt@delegated-operator` answered on 2026/10/09 by delegation; no further human ruling
is required. The four questions are settled as follows, and the current intent lives in
`docs/intent-records/worker-retirement.md`:

1. **The invariant** — `any_scan_overdue` (and a transient database stall) is
   **degraded readiness**, never a safety breach. "The worker is alive and consuming the
   queue" is the stronger condition: readiness is withdrawn when appropriate, in-flight
   jobs and spawns are kept alive, and the process is never signalled. Immediate
   retirement is preserved for `lease_safety_negative`, invalid ownership/desired
   generation, and real process death.
2. **The threshold** — a health-driven retirement requires independently verified absent
   forward progress for **≥60 s** and **≥3 consecutive supervisor probes**, with evidence
   reset on recovery. Database latency is a diagnostic trigger, not proof that work
   stopped; the evidence comes from an independent queue roundtrip, and a probe that could
   not be performed is not evidence about the worker.
3. **The backoff policy** — repeated health-driven retirements share the existing
   crash-style exponential backoff with its existing cap, and stability resets the
   counter.
4. **The observability policy** — superseded-incarnation health and log evidence is
   retained for **≥48 h**, bounded by file count and total bytes.

## 7. Boundaries observed

- No tracked file was created, modified, or deleted; `git status` shows only this
  untracked report.
- No commit, push, merge, or branch operation.
- No board operation; the issue was not closed or transitioned.
- No repair was attempted. Read-only SQL was used with
  `options='-c default_transaction_read_only=on'`.
