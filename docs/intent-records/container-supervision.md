$id-9992381141995964
title: Use s6 as the Lubko container supervisor
date: 2026/09/26
source: @ottojung
kind: requirement

The Lubko container image must use s6 as its PID 1 and process supervisor rather
than relying on Docker `--init` / Tini. Lubko itself is one supervised service:
if `lubko-supervisor` exits unexpectedly, s6 is responsible for starting it
again.

The outer s6 service topology remains infrastructure rather than Lubko runtime
authority. Lubko's own startup contract should name the `lubko-supervisor`
service command and must not inspect or require a particular live parent/child
process topology.
