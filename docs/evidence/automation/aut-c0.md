# AUT-C0 — Automation contract

Issue: #267 — `[AUT-C0] Automation contract`
Predecessor: OPS-O2, merged at `1b96653c0616c2be08a9b312f1c1dda1170d2889`
Scope: provider- and transport-neutral contract/evidence only; no runtime,
schema, worker, API, transport, credential provider, packaging, or UI.

## Contract purpose

This document freezes the Automation vocabulary and invariants consumed by
AUT-J1, AUT-A1, OPS-O4, H2, and H3. Automation is a durable description of
when and why an existing StillFlow Job may be submitted. It is not a second
execution system.

The contract applies to Desktop local mode and Workspace server mode. The
request context, workspace, actor, capability set, and managed-root/trust
boundary come from SEC-C0 and SEC-A1. Audit identity, redaction, causal links,
and retention behavior come from AUD-C0 and AUD-A1. The existing E5 JobRuntime
remains the only execution and queue authority.

> Automation may submit an existing E5 Job, but it must not become a second
> execution engine, queue authority, persistence authority, or digest
> authority.

## Versioned v1 object model

The logical field names below are transport-neutral. Adapters may choose a
serialization casing, but they must preserve the meaning, bounds, versioning,
and required/optional distinction.

| Object | Required identity and fields | Contract rule |
| --- | --- | --- |
| `Automation` | `automationId`, `automationVersion`, `workspaceId`, `name`, `state`, `trigger`, `schedule`, `runTemplate`, `failurePolicy`, `createdAt`, `updatedAt` | Workspace-scoped durable definition. An update creates a new immutable version; it never rewrites an accepted execution identity. |
| `Trigger` | `triggerKind`, optional event source/filter, optional manual authorization context | Exactly one of `schedule`, `event`, or `manual`. A manual trigger is an explicit API action, not an implicit scheduler tick. |
| `Schedule` | `scheduleVersion`, canonical expression, IANA timezone, misfire policy, bounded catch-up window | Describes wall-clock intent. The authoritative due value is a canonical UTC instant plus an occurrence key. Local machine timezone is never used implicitly. |
| `RunTemplate` | immutable `runTemplateId`, template version, target existing Job operation, bounded parameters, opaque credential references | Describes an E5 Job submission request. It cannot contain executable code, a secret value, an alternate executor, or an unbounded payload. |
| `AutomationExecution` | `executionId`, automation/version identity, trigger identity, occurrence key, accepted-at time, E5 Job identity, terminal state | Durable join between one accepted trigger and one E5 Job submission. Same idempotency identity returns the same result; a conflicting request fails closed. |

The v1 `Automation` state is one of `draft`, `active`, `paused`, `failed`,
`disabled`, or `deleted`. `deleted` is a tombstone/retention state, not a
request to erase audit history or an already accepted Job. State transitions
are explicit and auditable:

```text
draft -> active -> paused -> active
   |       |         |
   v       v         v
 deleted  failed   deleted
                    |
                    v
                 disabled
```

The diagram is a contract summary: an implementation may expose compatible
administrative transitions, but it may not reactivate a deleted or disabled
definition without a new version and explicit authorization. A failed
execution does not automatically change the Automation state unless its
declared failure policy says `pause_automation`.

All identifiers are opaque and workspace-scoped. Names are display metadata,
not identity. Unknown or future object versions fail closed; readers select an
explicit supported version rather than guessing.

## Boundedness and safe input

The following limits are contractual minimums; a deployment may configure
stricter limits but may not make them unbounded:

| Input | v1 bound and rule |
| --- | --- |
| Automation name | 1–128 UTF-8 bytes after safe-text validation |
| Template/filter/metadata JSON | maximum 64 KiB encoded, bounded nesting and collection cardinality, canonical key ordering |
| Parameter count | maximum 128 entries per template, with per-key/value bounds inherited from the Job API |
| Credential references | opaque references only; maximum 32 per template and no provider payload |
| Event filter | typed allowlisted fields only; no arbitrary code or unbounded query |
| Schedule catch-up | finite, explicit window and count; never replay all history by default |
| Retry attempts | finite positive integer, bounded by the deployment policy |
| Pagination/history | bounded page size, time span, and scan budget; cursor binds workspace, automation/version, filter digest, and last sequence |

Validation happens before a visible state transition, event append, or Job
submission. Rejected input does not echo raw parameters, credential data,
provider responses, or untrusted filter text.

## Trigger contract

### Schedule triggers

A schedule trigger produces a candidate occurrence only when its versioned
schedule resolves to a due UTC instant. The candidate carries:

- the immutable `automationVersion` and `scheduleVersion`;
- the canonical UTC `scheduledFor` instant;
- the canonical local representation used for display only;
- a deterministic `occurrenceKey` derived from the schedule identity and
  scheduled instant;
- the evaluation clock observation and a bounded misfire classification.

The occurrence key, not a worker process or an in-memory timer, is the
deduplication identity. Re-evaluating a schedule after restart, a clock jump,
or a lease handoff must not create a second accepted execution.

### Event triggers

An event trigger consumes an existing, authorized event source. The source
event must provide a stable identity, workspace, event type, occurred-at time,
and bounded redaction-safe payload. The trigger stores only the minimum event
identity, type, digest, and causal references needed to explain the decision.

The v1 event deduplication identity is:

```text
(workspaceId, automationId, automationVersion, sourceKind, sourceEventId)
```

If a source cannot provide a stable event ID, it is not eligible for a v1
event trigger. A payload digest may assist diagnostics, but it is not a
replacement for source identity and may not turn arbitrary content into an
unbounded deduplication key. A duplicate event returns the original
`AutomationExecution`/E5 Job result. The same identity with a different
canonical request digest is a conflict and makes no new mutation.

### Manual triggers

Manual triggering is an explicit authorized action against a specific
Automation version. It carries a caller-provided idempotency key that is
validated under the existing Job/API bounds. It may bypass a wall-clock due
check, but it may not bypass workspace authorization, state checks, parameter
validation, credential-reference checks, or the E5 Job submission boundary.

## Time, timezone, DST, and next-run semantics

1. A persisted schedule contains an IANA timezone identifier. A missing
   timezone is normalized to UTC only as an explicit contract operation; the
   host, browser, client, or worker timezone is never authoritative.
2. The next-run result is a canonical UTC RFC 3339 instant and an opaque
   occurrence key. Local wall-clock text is a rendering and is never used as a
   storage or ordering key.
3. Calculation is deterministic for the tuple `(scheduleVersion,
   evaluationInstantUtc, timezone database version, schedule expression)`.
   The implementation must record the timezone database version or an
   equivalent compatibility marker when it affects a persisted result.
4. A forward DST transition can create a nonexistent local time. The v1
   schedule must declare one of `skip`, `shift_to_next_valid`, or
   `fire_at_transition`. The default for a schedule that does not opt into a
   different behavior is `shift_to_next_valid`, recorded as a misfire-class
   decision.
5. A backward DST transition can create a repeated local time. The v1
   schedule must declare `earliest`, `latest`, or `both`. The default is
   `earliest`. When `both` is selected, the two distinct UTC instants have
   distinct occurrence keys; the deduplication rule still prevents a repeated
   delivery of either instant.
6. A clock moving backward must not make an already accepted occurrence due a
   second time. A clock moving forward may classify a finite set of missed
   occurrences under the misfire policy, subject to the explicit catch-up
   window/count. Monotonic process time may help pacing, but UTC schedule
   instants and durable state remain the authority.
7. A next-run query is a bounded read and does not enqueue a Job, change
   durable schedule state, or create an audit event merely by being queried.

The exact parser and timezone library remain implementation decisions. They
must implement these semantics and provide deterministic test vectors for
normal dates, DST forward/backward transitions, timezone changes, and clock
jumps.

## Misfire policy

Misfire handling is explicit per schedule and is applied when a due occurrence
was not accepted within its allowed lateness window. v1 policies are:

| Policy | Meaning |
| --- | --- |
| `skip` | Mark the bounded occurrence as skipped; do not submit a Job. |
| `fire_once` | Submit one Job for the missed window, using the original occurrence identity. |
| `coalesce` | Collapse all missed occurrences in the bounded window into one Job with a deterministic range/count summary. |
| `pause_automation` | Record the misfire and pause the Automation without submitting a Job. |

Catch-up never scans without a finite bound. A restart may resume evaluation
from durable schedule state, but it must not infer an unlimited backlog from
the wall clock. Misfire decisions are durable, redaction-safe, and auditable.

## Idempotency and execution handoff

Before submitting a Job, AUT-J1/AUT-A1 must resolve an execution identity:

```text
(workspaceId, automationId, automationVersion, triggerIdentity, occurrenceKey)
```

The canonical RunTemplate and resolved parameter digest are part of the
request preimage. The same identity and digest return the existing execution
and E5 Job identity. The same identity with a different digest is rejected as
a conflict. A worker crash between durable acceptance and E5 submission must
be recoverable through a bounded, durable handoff state; it must not blindly
submit a second Job.

The execution state is a finite state machine:

```text
candidate -> accepted -> submitted -> running -> succeeded
                    \-> blocked/failed
candidate -> skipped
accepted  -> recovery_required -> submitted | failed
```

Only the existing E5 Job API/runtime may create or run the Job. Automation may
store the E5 Job ID and observe the existing lifecycle events, but it does not
own Job queue fairness, engine memory limits, execution cancellation
semantics, artifact publication, identity, or digest calculation.

## Retry, backoff, and failure policy

Retries are bounded orchestration decisions around the same RunTemplate and
the existing E5 Job submission contract. The policy must declare:

- a finite maximum attempt count;
- which typed failures are retryable and which are terminal;
- a finite initial delay, maximum delay, and backoff formula;
- whether a retry reuses the same execution identity with a new bounded
  attempt marker or creates a new explicitly linked E5 Job according to the
  Job API contract;
- a terminal action: `record_failed`, `pause_automation`, or
  `disable_automation`.

Backoff uses deterministic jitter or no jitter under a recorded policy; it
must not become an unbounded retry loop. Authorization failures, invalid
templates, revoked/expired CredentialRefs, cross-workspace references, and
unsupported versions are non-retryable unless a later explicit policy says
otherwise. Every retry and terminal outcome has an audit/correlation link and
is safe to repeat after restart.

## Pause, resume, and race semantics

Pause is a compare-and-set transition on the durable Automation version. Once
the pause commit is visible, new schedule/event candidates are rejected or
recorded as paused; an already accepted E5 Job is not silently cancelled.
Cancellation is a separate authorized Job operation. If pause races with a
candidate, the durable commit order decides: a candidate accepted before the
pause may proceed, and one observed after the pause may not. The result is
recorded with both causal references.

Resume is also an authorized compare-and-set transition. It recomputes the
next due occurrence from durable state and the declared misfire policy. It does
not replay an unlimited paused backlog. An implementation may offer an
explicit bounded catch-up action, but that action has its own idempotency key,
authorization, and audit record.

## RunTemplate and parameter contract

RunTemplate is an immutable, versioned description of an existing E5 Job
submission. It references an allowed Job operation and plan/input identities;
it does not contain executable code, a second queue, a scheduler callback, a
filesystem path, or a provider-specific command. Parameters are typed,
allowlisted, canonicalized values subject to the same size/nesting bounds as
the Job API. Template substitution is deterministic and rejects missing,
unknown, cyclic, or over-limit parameters before submission.

`CredentialRef` is an opaque reference to provider-owned secret material as
defined by SEC-C0. Plaintext secrets, tokens, private keys, provider payloads,
reversible encodings, and useful secret fingerprints are forbidden in
Automation records, templates, events, logs, metrics, artifacts, API
responses, and checked-in fixtures. Credential ownership and capability are
checked in the resolved workspace context; a reference alone is never proof
of permission.

## Authorization and tenant isolation

Automation operations consume SEC-A1 and are evaluated in this order:

1. authenticate and resolve the principal;
2. resolve exactly one authorized workspace;
3. check the operation capability;
4. check Automation/version visibility and state;
5. check referenced Plan, Dataset, Job, CredentialRef, and target objects;
6. validate the state transition, idempotency identity, and bounded payload;
7. append the audit decision before or atomically with the domain mutation as
   required by the existing audit contract.

The v1 capability names are `automation.read`, `automation.create`,
`automation.update`, `automation.pause`, `automation.resume`,
`automation.delete`, and `automation.trigger`. A service account must have
the explicit capability and own the referenced workspace; it cannot inherit a
human member's authority. Clients may hide unavailable actions but never make
the authorization decision. Missing, revoked, cross-workspace, unknown, or
expired inputs fail closed using the SEC-C0 hidden-resource semantics.

## Audit and lineage semantics

Automation creates redaction-safe AUD-C0/AUD-A1 records for at least:

- create, update/version publication, pause, resume, delete/disable;
- schedule/event/manual trigger accepted, skipped, deduplicated, or rejected;
- misfire decisions, retries, terminal failure, recovery, and E5 Job handoff.

The audit actor is the authenticated user, owning service account, or explicit
trusted system actor. Each record includes workspace, request/correlation
links, Automation/version reference, trigger/occurrence identity, and the E5
Job reference once accepted. Schedule expressions, parameter values, event
payloads, and CredentialRefs are included only as bounded redacted metadata or
digests permitted by AUD-C0; raw sensitive content is never copied.

Automation history is a projection of durable AutomationExecution and existing
Job/Run lifecycle records. It is not a second event stream or a replacement
for `cp_events`/AUD records. Queries and cursors are workspace-, version-,
filter-, and sequence-bound, with bounded page/time/scan budgets. Retention
and tombstone processing from OPS-O2 may make old execution metadata
ineligible for ordinary reads, but must preserve active definitions,
unresolved references, audit obligations, and the ability to explain a
surviving E5 Job.

## Downstream implementation obligations

| Node | Must consume this contract |
| --- | --- |
| `AUT-J1` | Durable finite state, bounded queue, restart recovery, finite catch-up/misfire, event/occurrence deduplication, clock/DST behavior, pause/resume races, retry bounds, and E5-only handoff. |
| `AUT-A1` | CRUD/versioning, pause/resume/delete, next-run/history/manual trigger, capability enforcement, bounded pagination, idempotency, audit projection, and OpenAPI consistency. |
| `OPS-O4` | Process/service lifecycle, local/server trust boundary, restart/health behavior, configuration bounds, and no hidden executor. |
| `H2` | Tenant isolation, secret-free output, crash/recovery matrix, DST/clock correctness, duplicate suppression, finite retry, and audit integrity. |
| `H3` | Version compatibility, migration, rollback, observability, release evidence, and the user-visible failure/recovery behavior. |
| `OPS-O2` | Retention must remain reference-safe: active Automation/RunTemplate definitions and unresolved execution/audit references are not collected merely because they are old. |

Downstream nodes may choose storage tables, parser libraries, worker topology,
transport paths, and exact API shapes, but they may not introduce a second
execution engine, queue authority, identity/digest authority, or event
ordering authority.

## Acceptance matrix

The AUT-C0 checklist is frozen only when each item has an explicit contract
rule and a downstream owner:

| Checklist item | Frozen here |
| --- | --- |
| Automation / Trigger / Schedule / RunTemplate | Versioned object model and trigger sections |
| timezone / DST / next-run calculation | Time, timezone, DST, and next-run section |
| misfire / event trigger / trigger deduplication | Misfire and trigger sections |
| idempotency | Execution handoff and event sections |
| retry policy / backoff / failure policy | Retry, backoff, and failure section |
| pause / resume | Pause, resume, and race section |
| parameter template / CredentialRef | RunTemplate and parameter section |
| authorization | Tenant isolation and SEC-A1 capability section |
| audit semantics | Audit and lineage section |

Acceptance is contract/evidence only. It must not be represented as runtime
coverage before AUT-J1/AUT-A1 implement the downstream obligations and H2/H3
independently verify them.

## Non-goals and unresolved implementation decisions

This node does not create an Automation table, migration, scheduler worker,
clock service, cron parser, event subscription, E5 Job adapter, API/OpenAPI
route, credential provider, notification channel, retention worker, daemon,
IPC/HTTP transport, or UI. Storage placement, timezone database upgrade policy,
parser/library selection, durable lease mechanism, service lifecycle, and
transport mapping remain downstream implementation decisions subject to this
contract, exact-head review, independent acceptance, and H2/H3 release gates.
