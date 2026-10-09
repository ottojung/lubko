$id-6241898657924809
title: Live queue consumption outranks degraded maintenance readiness
date: 2026/10/09
source: issue-200
kind: requirement

A `lubko-worker` that demonstrably consumes the queue is never retired merely
because its maintenance schedule slipped. Overdue maintenance scans
(`any_scan_overdue`) caused by a slow or stalled database are a degraded
readiness condition: the supervisor withdraws readiness when appropriate and
lets the same worker recover in place, and the worker's in-flight jobs — and
the spawns they have already started — are kept alive.

Immediate retirement of a live incarnation is reserved for genuine safety and
ownership breaches: negative lease safety (`lease_safety_negative`), invalid
worker/command ownership or desired generation, and real process death. Those
paths are unchanged by this policy.

$id-9613421760004305
title: Health-driven retirement requires sustained, independently verified absent progress
date: 2026/10/09
source: issue-200
kind: constraint

When health is degraded but no immediate safety breach is present, a live
worker may only be retired after forward progress is independently verified
absent for at least 60 seconds **and** at least 3 consecutive supervisor
probes corroborate it. Both conditions must hold; neither alone is authority
to signal a process.

The evidence must come from an independent observation of the worker, not
from the worker's own late maintenance signals: a probe the supervisor could
not perform at all (missing configuration, unreachable database, unproven
insert) is not evidence about the worker, and database latency is a
diagnostic trigger rather than proof that work stopped. Evidence belongs to
one exact incarnation and is discarded as soon as progress is observed.

$id-6701431978473765
title: Health-driven retirements share the bounded crash backoff
date: 2026/10/09
source: issue-200
kind: constraint

A retirement for sustained absent forward progress is published exactly like
an unexpected worker exit: the durable restart counter advances and the
replacement is scheduled behind the existing bounded exponential backoff with
its existing cap, so a repeatedly failing worker backs off instead of being
replaced in a tight loop. Stability resets that counter through the same
rule that resets crash backoff.

$id-5139943419042654
title: Retirement evidence stays diagnosable for at least 48 hours
date: 2026/10/09
source: issue-200
kind: requirement

Confirming a replacement worker must not delete the retired incarnation's
health snapshot and operational log. Superseded-incarnation evidence is
retained for at least 48 hours, bounded by both a maximum file count and a
maximum total byte budget, so any retirement remains diagnosable after the
fact and a retirement storm cannot grow the retained set without limit.

$id-3262247267270579
title: Database latency is investigated separately from the scheduler
date: 2026/10/09
source: issue-200
kind: rejected-concern

Slow remote database round trips are a separate infrastructure investigation.
They are not a reason to make Lubko's scheduler resource-aware or
agent-count-aware, and no scheduling decision may be derived from observed
agent counts, host resources, or queue pressure.
