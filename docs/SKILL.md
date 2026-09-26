"fname        | lubko                                                                            |
| ----------- | ------------------------------------------------------------------------------- |
| description | Submit and observe commands through the Lubko connector and execution transport. |

# Lubko

Lubko is the connector and execution transport. An AI agent submits a versioned command row to the `lubko.jobs` queue at its operator's request; a worker claims the row, executes the requested process in the selected working directory, and publishes output and a terminal result to the same row.

Jobs only run on hosts where a worker has been explicitly deployed and configured to poll the queue for a given server name. `marceline-dev` and `phoebe-dev` are the two hosts currently configured this way — submitting a job addressed to an unconfigured server name simply leaves it unclaimed. Job submission follows the operator's request in the live conversation.

## Servers

- `marceline-dev` (default — use this when no server is specified)
- `phoebe-dev`

## Transport flow

```
Agent -> Supabase connector -> lubko.jobs -> worker -> command result -> Agent
```

Insert a protocol-v4 command addressed to the execution server and retain the returned UUID:

```
insert into lubko.jobs (payload)
values ('{"v":4,"type":"command","server":"marceline-dev","request":{"cwd":"<working-directory>","process":["git","status","--short"]},"state":{"status":"pending"}}')
returning id;
```

Poll the same root row until terminal:

```
select id, payload from lubko.jobs where id = '<root-job-uuid>';
```

The worker executes `request.process` directly, without a shell. Select a shell explicitly when needed, for example `["/bin/sh", "-c", "..."]`. Read `state.status`, `result.exit_code`, and bounded `output.stdout.tail` and `output.stderr.tail` values. Success is `status = 'succeeded'` and exit code `0`; stderr alone is not failure.

## Operational rules

- Use the queue as the only route to the execution container.
- Record each root UUID and poll outstanding UUIDs together.
- Continue bounded polling while work is nonterminal; never silently end with outstanding work.
- Cancel only pending or running jobs, then poll until cancellation reaches a terminal state.
- Do not infer one job's result from another similar-looking row.
- Keep higher-level orchestration policy, including work selection and repository review, outside Lubko.

## Cancellation

Set the cancellation marker inside the JSON payload for a pending or running job. The worker terminates the exact recorded process group with `SIGTERM`, then `SIGKILL` after the bounded grace period, and publishes `cancelled` with accumulated output.

## Temporary tools

If a job needs a utility missing from the Lubko host and `guix` is available, run the job with an ephemeral Guix environment, for example `guix environment --ad-hoc curl -- curl ...`. Do not treat a missing utility as a blocker when Guix can supply it. If Guix is unavailable, report the missing utility explicitly.

## Protocol

The complete protocol and compatibility rules are in [`protocol.md`](https://github.com/ottojung/lubko/blob/main/docs/protocol.md), with deployment and lifecycle details in the other documents under `docs/`. PostgreSQL table metadata remains frozen; protocol semantics live in the payload and application code.
