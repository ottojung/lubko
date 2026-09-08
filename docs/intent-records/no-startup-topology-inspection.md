$id-7302946158037241
title: No runtime startup-process-topology inspection
date: 2026/09/08
source: @ottojung
kind: constraint

Lubko must not inspect, validate, infer, or report the runtime startup process topology — specifically the Tini → supervisor → worker direct-parent chain — at any point. The external host/container startup environment is trusted and out of scope. The observational benefit of proving the live parent-child chain is too small relative to the fragility, complexity, and /proc parsing surface it introduces. Static startup artifact and config validation (launcher presence, definition match, state-directory permissions, private-config permissions) that do not inspect runtime process topology are retained.

Lifecycle-safety mechanisms that inspect process identity for safe process mutation, signaling, convergence, or direct child control (process identity, lifecycle-token, pidfd, child ownership) are not topology inspection and must be preserved.
