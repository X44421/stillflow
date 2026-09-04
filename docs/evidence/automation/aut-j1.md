# AUT-J1 — Scheduler runtime evidence

Issue: #269 — `[AUT-J1] Scheduler runtime`
Scope: bounded durable schedule coordination on top of the existing E5
control plane and `JobRuntime`.

## Boundary

AUT-J1 is a trigger coordinator. It persists schedule timing, claim leases,
retry state, and the handoff result needed to recover a trigger after a
restart. It builds an existing `JobSubmission` and delegates it to the E5
submission seam. It does not execute plans, create a second queue, own Job or
Run lifecycle state, calculate request digests, or publish artifacts.

The governing AUT-C0 law is:

> Automation may submit an existing E5 Job, but it must not become a second
> execution engine, queue authority, persistence authority, or digest
> authority.

## Durable state and boundedness

Storage schema v11 adds `aut_schedules`, keyed by workspace and schedule ID.
The row contains the versioned schedule value, IANA timezone, redaction-safe
run template, active/paused/failed/deleted state, next UTC occurrence, the
last accepted occurrence, an in-flight claim lease, a compare-and-set
revision, bounded attempt state, and a safe failure message. The migration is
forward-only and is covered by the existing migration-chain tests.

The scheduler wake channel is bounded to 16 notifications. Each tick scans at
most 64 due schedules (and accepts a stricter configured limit). Schedule
templates are capped at 256 KiB, submission attempts are finite and capped at
8, and claims expire after a bounded lease (60 seconds by default, never over
one hour). A backward wall-clock observation is clamped to the last observed
instant; a tick never scans an unbounded historical interval.

## Schedule and time semantics

The v1 core schedule set is deliberately small and versioned:

- `Interval` uses UTC elapsed seconds, bounded to one through 366 days.
- `Daily` uses an explicit local hour, minute, and second plus an IANA
  timezone; the host timezone is never inferred.
- A nonexistent spring-forward local time is skipped to the next valid
  calendar occurrence.
- An ambiguous fall-back local time selects the earlier instant, so one local
  wall-clock event is not emitted twice.

The next occurrence is stored and ordered as a UTC instant. Its deterministic
deduplication identity is:

```text
automation:{schedule_id}:{occurrence_rfc3339_nanos}
```

## Claim, handoff, and recovery

1. A bounded tick lists active due schedule IDs.
2. Storage claims one occurrence with a durable lease and attempt number. An
   unexpired claim is not claimed again; an expired claim can be recovered.
3. The factory converts the trigger into one already-defined E5
   `JobSubmission`. The scheduler rejects a factory result whose workspace or
   idempotency key does not match the claimed trigger.
4. The existing E5 submitter receives the submission. A replay is reported as
   a replay, not as a second logical trigger.
5. On success, storage atomically acknowledges the claim and advances the
   next occurrence. On failure, it records a bounded message and retries only
   up to the finite policy; exhaustion moves the schedule to `failed`.

The occurrence key is stable across process restart and lease recovery. The
E5 idempotency contract therefore remains the duplicate-suppression authority
when a process crashes around the submission boundary. Pause and resume use
revision compare-and-set transitions: a pause prevents new claims while an
already claimed handoff remains explicitly represented and recoverable.

## Acceptance evidence

The implementation and independent acceptance must cover:

| Requirement | Evidence |
| --- | --- |
| Durable schedule row and migration | `aut_schedules` schema v11 and storage migration tests |
| Interval/local next-run and DST behavior | core automation test `dst_forward_skips_gap_and_dst_backward_emits_one_earlier_instant` |
| Durable claim, retry, and pause CAS | storage tests `claim_is_durable_idempotent_and_pause_is_cas_guarded` and `retry_exhaustion_is_terminal_and_bounded` |
| Bounded tick and E5-only delegation | engine test `scheduler_submits_only_existing_e5_job_with_bounded_tick` |
| Restart/crash/idempotency boundary | durable claim lease, stable occurrence key, and canonical E5 submitter seam |

The PR acceptance matrix is: targeted core, storage, and engine tests; format
and whitespace checks; exact-head independent review; PR CI; and post-merge
main CI. Release gates H2/H3 remain responsible for the full crash/recovery,
tenant-isolation, audit, compatibility, and packaging matrix.

## Non-goals

AUT-J1 does not add Automation CRUD/API routes, authorization middleware,
audit/lineage projections, connector execution, a new Job/Run state machine,
daemon/service packaging, IPC/HTTP transport, notifications, or frontend UI.
Those boundaries belong to AUT-A1, OPS-O4, H2, and H3.
