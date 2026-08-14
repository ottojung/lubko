-- Add cancellation tracking columns to lubko.jobs.
--
-- Idempotent: every statement is safe to run more than once.
--
-- process_pid / process_pgid:
--     Exact identity of the running shell process and its process group,
--     recorded by the worker after the job is spawned.
-- cancel_requested_at:
--     Set by the orchestrator to request cancellation of a pending or running
--     job.
-- cancellation_note:
--     Human-readable diagnostic recorded when a job is cancelled.

alter table lubko.jobs
    add column if not exists process_pid integer,
    add column if not exists process_pgid integer,
    add column if not exists cancel_requested_at timestamptz,
    add column if not exists cancellation_note text;
