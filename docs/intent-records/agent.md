$id-9448585901481383
title: lubko-agent uses paid OpenCode Go MiMo-V2.5
date: 2026/09/08
source: @ottojung
kind: constraint

`lubko-agent` must use MiMo-V2.5 through the paid OpenCode Go provider, identified in OpenCode as `opencode-go/mimo-v2.5`. Free model variants, including model identifiers ending in `-free`, must never be used as the configured `lubko-agent` model or as a fallback.

$id-8612645784701677
title: Agent IDs are case-insensitive and use --id uniformly
date: 2026/09/15
source: issue-777
kind: requirement

Agent IDs entering `lubko-agent` at every input boundary are canonicalized to lowercase via `normalize_agent_id()`. All subcommands that accept an agent ID use the `--id <ID>` option; no command accepts the ID positionally. The canonical form is stored, compared, and dispatched in lowercase. Mixed-case spellings such as `ABCD1234` and `abcd1234` identify the same agent.
