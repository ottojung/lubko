$id-9448585901481383
title: lubko-agent uses OpenCode Space Bunny Free
date: 2026/09/23
source: @ottojung
kind: constraint

`lubko-agent` must use Space Bunny Free through OpenCode, identified as `opencode/space-bunny-free`. This is the configured `lubko-agent` model and supersedes the previous Muse Spark 1.3 Contributor requirement.

$id-8612645784701677
title: Agent IDs are case-insensitive and use --id uniformly
date: 2026/09/15
source: issue-777
kind: requirement

Agent IDs entering `lubko-agent` at every input boundary are canonicalized to lowercase via `normalize_agent_id()`. All subcommands that accept an agent ID use the `--id <ID>` option; no command accepts the ID positionally. The canonical form is stored, compared, and dispatched in lowercase. Mixed-case spellings such as `ABCD1234` and `abcd1234` identify the same agent.
