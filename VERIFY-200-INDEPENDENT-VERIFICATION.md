# Board 200 — independent verification pass by agent f1707755dde0 (2026-10-13)

Front: `f1707755dde0`. Repo: `/workspace/agent-f1707755dde0-issue200/lubko` (my own clone of
`git@github.com:ottojung/lubko.git`; the shared checkout `/workspace/lubko-200-supervisor`
was not touched). This pass re-verifies the investigation end-to-end by execution and
records the human-blocked handoff required by the issue completion rule. The full report
with the complete enumeration of every worker-retiring condition remains
`BOARD-200-SUPERVISOR-RETIREMENT.md` on branch `docs/board-200-supervisor-retirement`
(commit `4159d5d`); it is unchanged and its citations were spot-checked accurate.

## 1. What this pass verified by execution

- **Causal-path code citations at the incident revision `91e4547`** (each re-read in my
  clone): `supervisor.py:2043` returns
  `operational.lease_safety_negative or operational.any_scan_overdue`; the ready-worker
  branch calls `_check_worker_health` (`supervisor.py:1945`), logs
  `ready worker pid=… became operationally unsafe: …; retiring the exact incarnation` and
  calls `_retire_child()` (`supervisor.py:1956-1961`); `_retire_child` calls
  `lifecycle.stop_worker` (`supervisor.py:2177`); `_stop_pinned` sends `SIGTERM` to the
  exact process group via pidfd (`lifecycle.py:1595`); the worker's shutdown fails every
  in-flight spawn with `stderr="worker shutting down before spawn completed"`
  (`worker.py:6284`, reachable only via `_cleanup_pending_starts` on the SIGTERM path);
  the overdue-scan lateness budget is `db_operation_timeout + process_poll`
  (`worker.py:386-388`) = 15.0 s + 0.1 s (`worker.py:435`, `worker.py:421`);
  `prune_old_incarnation_artifacts` (`health.py:973-986`) deletes the retired
  incarnation's log after a successful readiness proof.
- **Durable state and logs on the host** (read-only): `supervisor.log` now holds **168**
  `became operationally unsafe` events and **176** `retired worker child` events; the
  latest retirement is **2026-10-12 23:34:44** (pid 3824963, reason
  `overdue scans: recovery`), retired 23:34:57, replacement pid 243634 started 23:35:06 —
  the identical pattern to the 2026-10-05 16:12 incident. The current worker's live
  health snapshot still shows the stall signal family (`db_deadline_breach_count: 1`,
  `db_error_at` set). `desired.json` is unchanged since 2026-10-06 22:02 (generation 15,
  `restart: false`).
- **Remediation state**: the fix branch `fix/issue-200-supervisor-retirement-policy`
  (`9953f57`) is **merged to `main`** via PR #862, merge commit `b7f8f2e`
  (2026-10-09 22:52 UTC). On `main` the supervisor safety predicate no longer references
  `any_scan_overdue` at all: immediate retirement requires
  `_worker_health_proves_immediate_safety_breach` (lease safety negative only); any other
  retirement requires `_no_progress_evidence_confirms` — independently verified absent
  forward progress for **>=60 s** AND **>=3 consecutive probes**
  (`DEFAULT_NO_PROGRESS_GRACE_SECONDS=60.0`, `DEFAULT_NO_PROGRESS_REQUIRED_PROBES=3`),
  evidence bound to one incarnation and reset on observed consumption; all other
  degradation (overdue scans, DB stall) only withdraws readiness and never signals a
  live queue consumer. Intent recorded in `docs/intent-records/worker-retirement.md`.
- **Gates on `main` in my clone** (each run with the canonical commands):
  `uv run ruff format --check .` → 147 files already formatted (exit 0);
  `uv run ruff check .` → all checks passed (exit 0);
  `uv run mypy .` → success, no issues in 120 source files (exit 0);
  `uv run pytest` → all passed, 2.36 s of the 10.0 s budget (exit 0; an initial run
  measured 21.14 s purely under the host's ~40/32-CPU load average from concurrent
  agents and passed immediately on re-run).

## 2. Production status — the defect is still live on the host

`marceline-dev` still runs pre-remediation commit **`5a0a558`** (host `meta.json`,
`state.json`, `desired.json` all name it; `supervisor_runtime_commit` agrees). The merge
to `main` has **not** been promoted to the host, and the churn continued after it:
retirements at 2026-10-12 18:47:40, 19:03:13, and 23:34:44 with zero crash-path events.
The host's worker still stalls against the remote Supabase pooler
(`db_deadline_breach`/`db_error` signals present in the live snapshot).

## 3. Proven cause (unchanged, re-verified)

The 2026-10-05 ~16:12 UTC worker replacement was a **health-driven retirement of a
live, queue-ready worker** — not a crash, not OOM, not a desired-config change, not a
deploy. `reconcile()` → `_probe_readiness` → `_check_worker_health` →
`interpret_worker_health` derived `operational.ready=False` with reason
`overdue scans: …; unrecovered DB deadline breach` →
`_worker_health_requires_retirement` returned `True` on `any_scan_overdue`
(`supervisor.py:2043`) → `_retire_child` → `stop_worker` → `SIGTERM` →
`_cleanup_pending_starts` failed in-flight spawns with the exact reported string
(`worker.py:6284`). The predicate fires only after a **>15.1 s** stall of the single
cooperative worker loop, caused by slow remote Supabase round trips. The failing
command is a downstream consequence, not the cause.

## 4. Intended vs defect (unchanged)

Intended and correct: deploy/restart on desired-generation change; crash detection with
owned-group recovery and backoff; immediate retirement on `lease_safety_negative`;
crash/startup recovery; convergence on superseded-commit/not-our-child; graceful drain.
**Defect**: retiring a queue-ready worker on `any_scan_overdue` — a bounded,
self-recovering maintenance lapse — with no backoff (`restart_count` stayed 0 across all
168 events), converting a transient DB stall into unbounded retire/replace churn.
Secondary defect: the retired incarnation's log is pruned within seconds, so every
recurrence is undiagnosable after the fact.

## 5. Remediation — landed and merged; what remains

Landed on `fix/issue-200-supervisor-retirement-policy` (`9953f57`) and **merged to `main`**
(`b7f8f2e`, PR #862): degraded readiness never signals a live queue consumer; retirement
only on `lease_safety_negative`, invalid ownership/generation, real death, or sustained
(>=60 s, >=3 probes) independently verified absent forward progress; health-driven
retirements share the bounded crash-style backoff; superseded-incarnation evidence
retained >=48 h, bounded by file count and bytes; retirement durably clears readiness.
Full trade-off analysis and the complete retiring-condition enumeration: section 5 and
section 2 of `BOARD-200-SUPERVISOR-RETIREMENT.md`.

## 6. Human decision required (blocking)

The policy question was settled by delegated operator on 2026-10-09 and the fix was
merged to `main` the same day. The remaining decision is **promotion**:

> **Deploy the merged `main` (`b7f8f2e`, containing the Board-200 retirement policy) to
> the production `marceline-dev` host — which still runs `5a0a558` and churned workers
> again on 2026-10-12 — and confirm production behaviour under the next real database
> stall.** In parallel, decide whether to open a separate infrastructure investigation
> of the Supabase pooler stalls (`db_deadline_breach`, `gc_batch_bound_hit`) that trigger
> the underlying condition.

Until promotion is confirmed, this issue remains **OPEN — BLOCKED ON HUMAN DECISION**.

## 7. Boundaries observed

- No tracked file in any shared checkout modified; my clone is
  `/workspace/agent-f1707755dde0-issue200/lubko` on branch
  `verify/issue-200-independent-verification`.
- Host state and logs were read read-only; no process was signalled, no state file
  written, no deploy performed.
- No code change beyond this verification report; no merge or promotion performed
  (human-gated).
