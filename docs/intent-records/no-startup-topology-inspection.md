$id-7302946158037241
title: Never inspect runtime startup-process topology
date: 2026/09/08
source: @ottojung
kind: constraint

Lubko must never inspect, validate, infer, report, or depend on the runtime startup process topology. In particular, the Tini → supervisor → worker parent/child chain is not part of Lubko's runtime contract. The external host/container startup environment is trusted, opaque infrastructure and is out of scope.

No deployment, restart, reset, recovery, readiness, health, status, installation, migration, or test path may require or perform a runtime process-topology proof. A topology mismatch must never block starting or recovering a worker, and topology observations must not appear as health or status failures.

Tests must not encode startup parentage as a correctness invariant. The observational benefit of proving the live parent-child chain is too small relative to the fragility, complexity, and `/proc` parsing surface it introduces.

Static startup artifact and configuration validation that does not inspect runtime process topology remains allowed: launcher presence, installed-definition match, state-directory permissions, and private-config permissions.

Lifecycle-safety mechanisms that inspect the exact identity of a process for safe mutation, signaling, convergence, or ownership are not topology inspection and must be preserved. This includes PID plus start-time identity, lifecycle tokens, pidfds, and direct ownership of a child process that Lubko itself spawned. These mechanisms may answer "is this the exact process I am authorized to mutate?"; they must not be used to reconstruct or validate the host/container startup tree.
