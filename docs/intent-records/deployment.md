$id-5831047296612840
title: Deployment excludes lubko-agent health
date: 2026/09/08
source: @ottojung
kind: constraint

`lubko-agent` health must not be verified as part of deployment. Deployment confirmation and deployment health must not depend on a `lubko-agent` prompt, the configured agent model, OpenCode model availability, or external agent-provider health. Agent health may be investigated or verified separately from deployment.
