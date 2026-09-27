$id-9992381141995964
title: Restart Lubko whenever it dies
date: 2026/09/26
source: @ottojung
kind: requirement

Every Lubko deployment must arrange for `lubko-supervisor` to be restarted
whenever it dies or exits, unless an operator has intentionally disabled or
stopped the deployment.

The restart mechanism is an infrastructure choice, not part of Lubko's runtime
contract. A deployment may use s6, systemd, runit, Docker or container restart
policy, another service manager, or any other mechanism that provides the same
restart guarantee. Lubko may run inside or outside a container. No particular
init system is required, and `lubko-supervisor` does not need to be PID 1.

Lubko must remain agnostic to that external supervision topology: it should not
inspect which restart mechanism is present or depend on a particular parent
process arrangement for correctness.
